// Copyright 2020 Teratide B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <thread>
#include <memory>

#include <blockingconcurrentqueue.h>
#include <putong/timer.h>
#include <illex/arrow.h>

#include "bolson/bench.h"
#include "bolson/utils.h"
#include "bolson/status.h"
#include "bolson/pulsar.h"
#include "bolson/buffer/allocator.h"
#include "bolson/buffer/opae_allocator.h"
#include "bolson/parse/parser.h"
#include "bolson/parse/opae_battery_impl.h"
#include "bolson/parse/arrow_impl.h"
#include "bolson/convert/converter.h"
#include "bolson/convert/stats.h"

namespace bolson {

static auto GenerateJSONs(size_t num_jsons,
                          const arrow::Schema &schema,
                          const illex::GenerateOptions &gen_opts,
                          std::vector<illex::JSONQueueItem> *items)
-> std::pair<size_t, size_t> {
  size_t largest = 0;
  // Generate a message with tweets in JSON format.
  auto gen = illex::FromArrowSchema(schema, gen_opts);
  // Generate the JSONs.
  items->reserve(num_jsons);
  size_t raw_chars = 0;
  for (size_t i = 0; i < num_jsons; i++) {
    auto json = gen.GetString();
    if (json.size() > largest) { largest = json.size(); }
    raw_chars += json.size();
    items->push_back(illex::JSONQueueItem{i, json});
  }
  return {raw_chars, largest};
}

/**
 * \brief Prepare input buffers for benchmarking.
 * \param buffers   The buffers to fill.
 * \param jsons     The JSONs to copy into the buffers.
 */
static void FillBuffers(std::vector<illex::RawJSONBuffer *> buffers,
                        const std::vector<illex::JSONQueueItem> &jsons) {
  auto items_per_buffer = jsons.size() / buffers.size();
  auto items_first_buf = jsons.size() % buffers.size();
  size_t item = 0;
  for (size_t b = 0; b < buffers.size(); b++) {
    // Fill the buffer.
    size_t offset = 0;
    auto buffer_num_items = items_per_buffer + (b == 0 ? items_first_buf : 0);
    auto first = item;
    for (size_t j = 0; j < buffer_num_items; j++) {
      std::memcpy(buffers[b]->mutable_data() + offset,
                  jsons[item].string.data(),
                  jsons[item].string.length());
      offset += jsons[item].string.length();
      *(buffers[b]->mutable_data() + offset) = static_cast<std::byte>('\n');
      offset++;
      item++;
    }
    buffers[b]->SetSize(offset);
    buffers[b]->SetRange({first, item - 1});
  }
}

auto BenchConvert(const ConvertBenchOptions &opt) -> Status {
  putong::Timer<> t_gen, t_init, t_conv;

  if (!opt.csv) {
    spdlog::info("Converting {} JSONs to Arrow IPC", opt.num_jsons);
    spdlog::info("  Arrow Schema: {}", opt.schema->ToString());
  } else {
    std::cout << opt.num_jsons << ",";
  }
  // Generate JSONs
  spdlog::info("Generating JSONs...");
  t_gen.Start();
  std::vector<illex::JSONQueueItem> items;
  auto bytes_largest = GenerateJSONs(opt.num_jsons, *opt.schema, opt.generate, &items);
  auto gen_bytes = bytes_largest.first;
  auto max_json = bytes_largest.second + 1; // + 1 for newline.
  t_gen.Stop();

  spdlog::info("Initializing converter...");
  t_init.Start();
  // Set up output queue.
  IpcQueue ipc_queue;
  IpcQueueItem ipc_item;

  // Select allocator.
  std::shared_ptr<buffer::Allocator> allocator;
  switch (opt.converter.implementation) {
    case parse::Impl::ARROW:allocator = std::make_shared<buffer::Allocator>();
      break;
    case parse::Impl::OPAE_BATTERY:allocator = std::make_shared<buffer::OpaeAllocator>();
      break;
  }

  // Set up the Converter.
  size_t num_buffers = opt.converter.num_buffers.value_or(opt.converter.num_threads);

  auto converter = convert::Converter(&ipc_queue,
                                      allocator.get(),
                                      num_buffers,
                                      opt.converter.num_threads);

  // Allocate and fill buffers.
  // Calculate generous buffer size, +1 for newline
  auto buf_cap = num_buffers * max_json + (opt.num_jsons * max_json) / num_buffers;
  // Temporary work-around for opae
  if (opt.converter.implementation == parse::Impl::OPAE_BATTERY) {
    buf_cap = buffer::OpaeAllocator::opae_fixed_capacity;
  }
  converter.AllocateBuffers(buf_cap);

  std::shared_ptr<parse::OpaeBatteryParserManager> opae_battery_manager;

  // Set up the parsers.
  switch (opt.converter.implementation) {
    case parse::Impl::ARROW: {
      for (size_t t = 0; t < opt.converter.num_threads; t++) {
        auto parser = std::make_shared<parse::ArrowParser>(opt.converter.arrow);
        converter.parsers.push_back(parser);
      }
      break;
    }
    case parse::Impl::OPAE_BATTERY: {
      BOLSON_ROE(parse::OpaeBatteryParserManager::Make(parse::OpaeBatteryOptions(),
                                                       ToPointers(converter.buffers),
                                                       opt.converter.num_threads,
                                                       &opae_battery_manager));
      converter.parsers = CastPtrs<parse::Parser>(opae_battery_manager->parsers());
    }
  }

  FillBuffers(ToPointers(converter.buffers), items);

  // Lock all buffers.
  for (size_t m = 0; m < opt.converter.num_buffers; m++) {
    converter.mutexes[m].lock();
  }

  // Set up Resizers and Serializers.
  for (size_t t = 0; t < opt.converter.num_threads; t++) {
    converter.resizers.emplace_back(opt.converter.max_batch_rows);
    converter.serializers.emplace_back(opt.converter.max_ipc_size);
  }

  // Start converter threads.
  spdlog::info("Converting...");

  std::atomic<bool> shutdown = false;
  size_t num_rows = 0;
  size_t ipc_size = 0;
  size_t num_ipc = 0;

  converter.Start(&shutdown);
  t_init.Stop();

  // Start conversion by unlocking the buffers.
  t_conv.Start();
  // Unlock all buffers.
  for (size_t m = 0; m < opt.converter.num_buffers; m++) {
    converter.mutexes[m].unlock();
  }

  // Pull JSON ipc items from the queue to check when we are done.
  while (num_rows != opt.num_jsons) {
    ipc_queue.wait_dequeue(ipc_item);
    num_rows += ipc_item.num_rows;
    ipc_size += ipc_item.ipc->size();
    num_ipc++;
  }

  // Stop converting.
  shutdown.store(true);
  converter.Stop();
  t_conv.Stop();

  // Free buffers.
  BOLSON_ROE(converter.FreeBuffers());

  // Print all statistics:
  auto json_MB = static_cast<double>(gen_bytes) / (1e6);
  auto json_M = static_cast<double>(opt.num_jsons) / (1e6);
  auto ipc_MB = static_cast<double>(ipc_size / (1e6));

  spdlog::info("JSON Generation:");
  spdlog::info("  Bytes (no newlines) : {} B", gen_bytes);
  spdlog::info("  Bytes (w/ newlines) : {} B", gen_bytes + opt.num_jsons);
  spdlog::info("  Time                : {} s", t_gen.seconds());
  spdlog::info("  Throughput          : {} MB/s", json_MB / t_gen.seconds());
  spdlog::info("  Throughput          : {} MJ/s", json_M / t_gen.seconds());

  spdlog::info("End-to-end conversion:");
  spdlog::info("  IPC messages      : {}", num_ipc);
  spdlog::info("  Time              : {} s", t_conv.seconds());
  spdlog::info("  Throughput (in)   : {} MB/s", json_MB / t_conv.seconds());
  spdlog::info("  Throughput (out)  : {} MB/s", ipc_MB / t_conv.seconds());
  spdlog::info("  Throughput        : {} MJ/s", json_M / t_conv.seconds());

  auto a = convert::AggrStats(converter.stats);
  spdlog::info("Details:");
  LogConvertStats(a, opt.converter.num_threads, "  ");

  return Status::OK();
}

using Queue = moodycamel::BlockingConcurrentQueue<uint8_t>;
using QueueTimers = std::vector<putong::SplitTimer<2>>;

// Thread to dequeue
static void Dequeue(const QueueBenchOptions &opt, Queue *queue, QueueTimers *timers) {
  uint64_t o = 0;
  for (size_t i = 0; i < opt.num_items; i++) {
    queue->wait_dequeue(o);
    (*timers)[i].Split();
  }
}

auto BenchQueue(const QueueBenchOptions &opt) -> Status {
  // Make a queue
  Queue queue;
  // Make timers.
  std::vector<putong::SplitTimer<2>> timers(opt.num_items);

  auto deq_thread = std::thread(Dequeue, opt, &queue, &timers);

  // Wait for the thread to spawn.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  for (size_t i = 0; i < opt.num_items; i++) {
    timers[i].Start();
    queue.enqueue(static_cast<uint64_t>(i));
    timers[i].Split();
  }

  deq_thread.join();

  size_t i = 0;
  std::cout << "Item,Enqueue,Dequeue" << std::endl;
  for (const auto &t: timers) {
    std::cout << i << ",";
    i++;
    std::cout << std::setprecision(9) << std::fixed << t.seconds()[0] << ",";
    std::cout << std::setprecision(9) << std::fixed << t.seconds()[1];
    std::cout << std::endl;
  }

  return Status::OK();
}

auto BenchPulsar(const PulsarBenchOptions &opt) -> Status {
  if (!opt.csv) {
    spdlog::info("Number of messages : {}", opt.num_messages);
    spdlog::info("Message size       : {} bytes", opt.message_size);
    spdlog::info("Pulsar URL         : {}", opt.pulsar.url);
    spdlog::info("Pulsar topic       : {}", opt.pulsar.topic);
  }
  // Setup Pulsar context
  PulsarContext pulsar;
  BOLSON_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &pulsar));

  putong::Timer<> t;

  // Allocate some buffer.
  auto *junk = static_cast<uint8_t *>(std::malloc(opt.message_size));
  // Clear the buffer.
  std::memset(junk, 0, opt.message_size);

  // Start a timer.
  t.Start();
  for (int i = 0; i < opt.num_messages; i++) {
    BOLSON_ROE(Publish(pulsar.producer.get(), junk, opt.message_size));
  }
  t.Stop();

  // Free buffer.
  free(junk);

  if (opt.csv) {
    std::cout << opt.num_messages << "," << opt.message_size << "," << t.seconds()
              << std::endl;
  } else {
    // Print stats.
    spdlog::info("Time               : {} s", t.seconds());
    spdlog::info("Goodput            : {} MB/s",
                 1E-6 * static_cast<double>(opt.num_messages * opt.message_size)
                     / t.seconds());
  }

  return Status::OK();
}

auto BenchClient(const ClientBenchOptions &opt) -> Status {
  return Status(Error::GenericError, "Not yet implemented.");
}

auto RunBench(const BenchOptions &opt) -> Status {
  switch (opt.bench) {
    case Bench::CLIENT: return BenchClient(opt.client);
    case Bench::CONVERT: return BenchConvert(opt.convert);
    case Bench::PULSAR: return BenchPulsar(opt.pulsar);
    case Bench::QUEUE: return BenchQueue(opt.queue);
  }
  return Status::OK();
}

}
