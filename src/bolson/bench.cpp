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
#include "bolson/convert/metrics.h"
#include "bolson/parse/parser.h"
#include "bolson/publish/bench.h"
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

auto FillBuffers(std::vector<illex::JSONBuffer*> buffers,
                 const std::vector<illex::JSONItem>& jsons) -> Status {
  auto items_per_buffer = jsons.size() / buffers.size();
  auto items_first_buf = jsons.size() % buffers.size();
  size_t item = 0;
  for (size_t b = 0; b < buffers.size(); b++) {
    // Fill the buffer.
    size_t offset = 0;
    auto buffer_num_items = items_per_buffer + (b == 0 ? items_first_buf : 0);
    auto first = item;
    for (size_t j = 0; j < buffer_num_items; j++) {
      if (offset + jsons[item].string.length() > buffers[b]->capacity()) {
        return Status(Error::GenericError,
                      "JSONs do not fit in buffers. Increase buffer capacity.");
      }
      std::memcpy(buffers[b]->mutable_data() + offset, jsons[item].string.data(),
                  jsons[item].string.length());
      offset += jsons[item].string.length();
      *(buffers[b]->mutable_data() + offset) = static_cast<std::byte>('\n');
      offset++;
      item++;
    }
    BILLEX_ROE(buffers[b]->SetSize(offset));
    buffers[b]->SetRange({first, item - 1});
  }
  return Status::OK();
}

auto BenchConvert(const ConvertBenchOptions& opts) -> Status {
  putong::Timer<> t_gen, t_init, t_conv;
  auto o = opts;

  BOLSON_ROE(o.converter.parser.arrow.ReadSchema());

  if (!o.csv) {
    spdlog::info("Converting {} JSONs to Arrow IPC messages...", o.num_jsons);
  } else {
    std::cout << o.num_jsons << ",";
  }
  // Generate JSONs
  spdlog::info("Generating JSONs...");

  t_gen.Start();
  std::vector<illex::JSONItem> items;
  auto bytes_largest =
      GenerateJSONs(o.num_jsons, *o.converter.parser.arrow.schema, o.generate, &items);
  t_gen.Stop();

  auto gen_bytes = bytes_largest.first;

  spdlog::info("Initializing converter...");
  t_init.Start();

  // Fix options if not supplied.
  convert::ConverterOptions conv_opts = o.converter;

  // Set up output queue.
  publish::IpcQueue ipc_queue;
  publish::IpcQueueItem ipc_item;

  // Construct converter.
  std::shared_ptr<convert::Converter> converter;
  BOLSON_ROE(convert::Converter::Make(conv_opts, &ipc_queue, &converter));

  spdlog::info("Converter schema:\n{}",
               converter->parser_context()->schema()->ToString());

  // Fill buffers.
  BOLSON_ROE(FillBuffers(converter->parser_context()->mutable_buffers(), items));

  // Lock all buffers, so the threads don't start parsing until we unlock all buffers
  // at the same time.
  converter->parser_context()->LockBuffers();

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
  converter->parser_context()->UnlockBuffers();

  // Pull JSON ipc items from the queue to check when we are done.
  while (num_rows != o.num_jsons) {
    ipc_queue.wait_dequeue(ipc_item);
    num_rows += RecordSizeOf(ipc_item);
    SPDLOG_DEBUG("Popped IPC item of {} rows. Progress: {}/{}", RecordSizeOf(ipc_item),
                 num_rows, o.num_jsons);
    ipc_size += ipc_item.message->size();
    num_ipc++;
  }

  // Stop converting.
  shutdown.store(true);
  converter->Finish();
  t_conv.Stop();

  // Print all statistics:
  auto json_MB = static_cast<double>(gen_bytes) / (1e6);
  auto json_M = static_cast<double>(opts.num_jsons) / (1e6);
  auto ipc_MB = static_cast<double>(ipc_size / (1e6));

  spdlog::info("JSON Generation:");
  spdlog::info("  Bytes (no newlines) : {} B", gen_bytes);
  spdlog::info("  Bytes (w/ newlines) : {} B", gen_bytes + opts.num_jsons);
  spdlog::info("  Time                : {} s", t_gen.seconds());
  spdlog::info("  Throughput          : {} MB/s", json_MB / t_gen.seconds());
  spdlog::info("  Throughput          : {} MJ/s", json_M / t_gen.seconds());

  spdlog::info("End-to-end conversion:");
  spdlog::info("  IPC messages        : {}", num_ipc);
  spdlog::info("  Time                : {} s", t_conv.seconds());
  spdlog::info("  Throughput (in)     : {} MB/s", json_MB / t_conv.seconds());
  spdlog::info("  Throughput (out)    : {} MB/s", ipc_MB / t_conv.seconds());
  spdlog::info("  Throughput          : {} MJ/s", json_M / t_conv.seconds());

  auto a = Aggregate(converter->metrics());
  spdlog::info("Details:");
  LogConvertMetrics(a, opts.converter.num_threads, "  ");

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

auto BenchClient(const illex::ClientOptions& opt) -> Status {
  return Status(Error::GenericError, "Not implemented.");
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
