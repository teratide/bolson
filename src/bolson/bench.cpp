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

#include "bolson/bench.h"

#include <blockingconcurrentqueue.h>
#include <illex/arrow.h>
#include <putong/timer.h>

#include <iostream>
#include <memory>
#include <thread>

#include "bolson/buffer/allocator.h"
#include "bolson/convert/converter.h"
#include "bolson/convert/stats.h"
#include "bolson/parse/parser.h"
#include "bolson/pulsar.h"
#include "bolson/status.h"
#include "bolson/utils.h"

namespace bolson {

auto GenerateJSONs(size_t num_jsons, const arrow::Schema& schema,
                   const illex::GenerateOptions& gen_opts,
                   std::vector<illex::JSONItem>* items) -> std::pair<size_t, size_t> {
  size_t largest = 0;
  // Generate a message with tweets in JSON format.
  auto gen = illex::FromArrowSchema(schema, gen_opts);
  // Generate the JSONs.
  items->reserve(num_jsons);
  size_t raw_chars = 0;
  for (size_t i = 0; i < num_jsons; i++) {
    auto json = gen.GetString();
    if (json.size() > largest) {
      largest = json.size();
    }
    raw_chars += json.size();
    items->push_back(illex::JSONItem{i, json});
  }
  return {raw_chars, largest};
}

void FillBuffers(std::vector<illex::JSONBuffer*> buffers,
                 const std::vector<illex::JSONItem>& jsons) {
  auto items_per_buffer = jsons.size() / buffers.size();
  auto items_first_buf = jsons.size() % buffers.size();
  size_t item = 0;
  for (size_t b = 0; b < buffers.size(); b++) {
    // Fill the buffer.
    size_t offset = 0;
    auto buffer_num_items = items_per_buffer + (b == 0 ? items_first_buf : 0);
    auto first = item;
    for (size_t j = 0; j < buffer_num_items; j++) {
      std::memcpy(buffers[b]->mutable_data() + offset, jsons[item].string.data(),
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

auto BenchConvert(ConvertBenchOptions opt) -> Status {
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
  std::vector<illex::JSONItem> items;
  auto bytes_largest = GenerateJSONs(opt.num_jsons, *opt.schema, opt.generate, &items);
  t_gen.Stop();

  auto gen_bytes = bytes_largest.first;
  auto max_json = bytes_largest.second + 1;  // + 1 for newline.

  spdlog::info("Initializing converter...");
  t_init.Start();

  // Fix options if not supplied.
  if (!opt.converter.num_buffers) {
    opt.converter.num_buffers = opt.converter.num_threads;
  }

  if (opt.converter.buf_capacity == 0) {
    // Calculate buffer capacity.
    opt.converter.buf_capacity = opt.converter.num_threads * max_json +
                                 (opt.num_jsons * max_json) / opt.converter.num_threads;
  }

  // Set up output queue.
  IpcQueue ipc_queue;
  IpcQueueItem ipc_item;

  // Construct converter.
  std::shared_ptr<convert::Converter> converter;
  BOLSON_ROE(convert::Converter::Make(opt.converter, &ipc_queue, &converter));

  // Fill buffers.
  FillBuffers(converter->mutable_buffers(), items);

  // Lock all buffers, so the threads don't start parsing until we unlock all buffers
  // at the same time.
  converter->LockBuffers();

  // Start converter threads.
  std::atomic<bool> shutdown = false;
  size_t num_rows = 0;
  size_t ipc_size = 0;
  size_t num_ipc = 0;

  converter->Start(&shutdown);
  t_init.Stop();

  spdlog::info("All threads spawned. Unlocking buffers and start converting...");

  // Start conversion by unlocking the buffers.
  t_conv.Start();
  converter->UnlockBuffers();

  // Pull JSON ipc items from the queue to check when we are done.
  while (num_rows != opt.num_jsons) {
    ipc_queue.wait_dequeue(ipc_item);
    SPDLOG_DEBUG("Popped IPC item of {} rows. Progress: {}/{}", RecordSizeOf(ipc_item),
                 num_rows, opt.num_jsons);
    num_rows += RecordSizeOf(ipc_item);
    ipc_size += ipc_item.message->size();
    num_ipc++;
  }

  // Stop converting.
  shutdown.store(true);
  converter->Finish();
  t_conv.Stop();

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
  spdlog::info("  IPC messages        : {}", num_ipc);
  spdlog::info("  Time                : {} s", t_conv.seconds());
  spdlog::info("  Throughput (in)     : {} MB/s", json_MB / t_conv.seconds());
  spdlog::info("  Throughput (out)    : {} MB/s", ipc_MB / t_conv.seconds());
  spdlog::info("  Throughput          : {} MJ/s", json_M / t_conv.seconds());

  auto a = convert::AggrStats(converter->statistics());
  spdlog::info("Details:");
  LogConvertStats(a, opt.converter.num_threads, "  ");

  return Status::OK();
}

using Queue = moodycamel::BlockingConcurrentQueue<uint8_t>;
using QueueTimers = std::vector<putong::SplitTimer<2>>;

// Thread to dequeue
static void Dequeue(const QueueBenchOptions& opt, Queue* queue, QueueTimers* timers) {
  uint64_t o = 0;
  for (size_t i = 0; i < opt.num_items; i++) {
    queue->wait_dequeue(o);
    (*timers)[i].Split();
  }
}

auto BenchQueue(const QueueBenchOptions& opt) -> Status {
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
  for (const auto& t : timers) {
    std::cout << i << ",";
    i++;
    std::cout << std::setprecision(9) << std::fixed << t.seconds()[0] << ",";
    std::cout << std::setprecision(9) << std::fixed << t.seconds()[1];
    std::cout << std::endl;
  }

  return Status::OK();
}

auto BenchPulsar(const PulsarBenchOptions& opt) -> Status {
  if (!opt.csv) {
    spdlog::info("Number of messages : {}", opt.num_messages);
    spdlog::info("Message size       : {} bytes", opt.message_size);
    spdlog::info("Pulsar URL         : {}", opt.pulsar.url);
    spdlog::info("Pulsar topic       : {}", opt.pulsar.topic);
  }
  // Setup Pulsar context
  PulsarConsumerContext pulsar;
  BOLSON_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &pulsar));

  putong::Timer<> t;

  // Allocate some buffer.
  auto* junk = static_cast<uint8_t*>(std::malloc(opt.message_size));
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
    spdlog::info(
        "Goodput            : {} MB/s",
        1E-6 * static_cast<double>(opt.num_messages * opt.message_size) / t.seconds());
  }

  return Status::OK();
}

auto BenchClient(const illex::ClientOptions& opt) -> Status {
  return Status(Error::GenericError, "Not yet implemented.");
}

auto RunBench(const BenchOptions& opt) -> Status {
  switch (opt.bench) {
    case Bench::CLIENT:
      return BenchClient(opt.client);
    case Bench::CONVERT:
      return BenchConvert(opt.convert);
    case Bench::PULSAR:
      return BenchPulsar(opt.pulsar);
    case Bench::QUEUE:
      return BenchQueue(opt.queue);
  }
  return Status::OK();
}

}  // namespace bolson
