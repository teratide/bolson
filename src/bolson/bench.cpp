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

  spdlog::info("Converting {} randomly generated JSONs to Arrow IPC messages...",
               o.num_jsons);

  BOLSON_ROE(o.converter.parser.arrow.ReadSchema());

  // Generate JSONs
  spdlog::info("Generating JSONs...");

  t_gen.Start();
  std::vector<illex::JSONItem> input_items;
  auto bytes_largest = GenerateJSONs(o.num_jsons, *o.converter.parser.arrow.schema,
                                     o.generate, &input_items);
  t_gen.Stop();
  auto gen_bytes = bytes_largest.first;

  spdlog::info("Initializing converter...");
  t_init.Start();

  // Fix options if not supplied.
  convert::ConverterOptions conv_opts = o.converter;

  // Set up output queue & vector.
  publish::IpcQueue ipc_queue;

  // Construct converter.
  std::shared_ptr<convert::Converter> converter;
  BOLSON_ROE(convert::Converter::Make(conv_opts, &ipc_queue, &converter));

  // Grab the input buffers from the parser context.
  auto buffers = converter->parser_context()->mutable_buffers();

  spdlog::info("Converter schema:\n{}",
               converter->parser_context()->output_schema()->ToString());

  // Fill buffers.
  BOLSON_ROE(FillBuffers(buffers, input_items));

  // Remember how much data is in the buffers so we can quickly reset.
  std::vector<size_t> buffer_sizes;
  std::vector<illex::SeqRange> buffer_seq_ranges;
  for (const auto& buf : buffers) {
    buffer_sizes.push_back(buf->size());
    buffer_seq_ranges.push_back(buf->range());
  }

  // Reserve a vector for latency measurements.
  LatencyMeasurements latencies;
  latencies.reserve(opts.repeats * buffers.size());

  // Other metrics
  size_t total_records_dequeued = 0;
  size_t total_bytes_dequeued = 0;
  size_t total_messages_dequeued = 0;

  // Start converter threads.
  std::atomic<bool> shutdown = false;

  // Lock all buffers, so the threads don't start parsing until we unlock all buffers
  // at the same time.
  converter->parser_context()->LockBuffers();

  converter->Start(&shutdown);
  t_init.Stop();

  spdlog::info("All threads spawned. Unlocking buffers and start converting...");

  t_conv.Start();
  // Repeat measurement.
  for (size_t r = 0; r < opts.repeats; r++) {
    size_t num_records_dequeued = 0;
    size_t num_bytes_dequeued = 0;
    size_t num_messages_dequeued = 0;

    for (size_t b = 0; b < buffers.size(); b++) {
      // Set the size/seq range of the buffer in case we repeat.
      buffers[b]->SetSize(buffer_sizes[b]);
      buffers[b]->SetRange(buffer_seq_ranges[b]);
      // Mark "receive time" point for buffer to be converted, just before we unlock.
      buffers[b]->SetRecvTime(illex::Timer::now());
    }
    // Start conversion by unlocking the buffers.
    converter->parser_context()->UnlockBuffers();

    // Pull JSON ipc items from the queue to check when we are done.
    while ((num_records_dequeued != o.num_jsons) && !shutdown.load()) {
      publish::IpcQueueItem ipc_item;
      // Wait for an IPC message to appear.
      if (ipc_queue.wait_dequeue_timed(ipc_item,
                                       std::chrono::microseconds(BOLSON_QUEUE_WAIT_US))) {
        // Mark time point popped from IPC message queue.
        ipc_item.time_points[TimePoints::popped] = illex::Timer::now();
        // Update some metrics.
        num_records_dequeued += RecordSizeOf(ipc_item);
        num_bytes_dequeued += ipc_item.message->size();
        num_messages_dequeued++;
        latencies.emplace_back(
            LatencyMeasurement{ipc_item.seq_range, ipc_item.time_points});
      }
    }
    converter->parser_context()->LockBuffers();
    total_bytes_dequeued += num_bytes_dequeued;
    total_messages_dequeued += num_messages_dequeued;
    total_records_dequeued += num_records_dequeued;
  }
  converter->parser_context()->UnlockBuffers();

  t_conv.Stop();

  // Stop converting.
  shutdown.store(true);
  converter->Finish();

  // Print all statistics:
  auto json_MB = static_cast<double>(opts.repeats * gen_bytes) / (1e6);
  auto json_M = static_cast<double>(opts.num_jsons) / (1e6);
  auto ipc_MB = static_cast<double>(total_bytes_dequeued) / 1e6;

  spdlog::info("JSON Generation:");
  spdlog::info("  Bytes (no newlines) : {} B", gen_bytes);
  spdlog::info("  Bytes (w/ newlines) : {} B", gen_bytes + opts.num_jsons);
  spdlog::info("  Time                : {} s", t_gen.seconds());
  spdlog::info("  Throughput          : {} MB/s",
               static_cast<double>(gen_bytes) / t_gen.seconds());
  spdlog::info("  Throughput          : {} MJ/s", json_M / t_gen.seconds());

  spdlog::info("End-to-end conversion:");
  spdlog::info("  JSONs (in)          : {}", total_records_dequeued);
  spdlog::info("  IPC messages (out)  : {}", total_messages_dequeued);
  spdlog::info("  Time                : {} s", t_conv.seconds());
  spdlog::info("  Throughput (in)     : {} MB/s", json_MB / t_conv.seconds());
  spdlog::info("  Throughput (out)    : {} MB/s", ipc_MB / t_conv.seconds());
  spdlog::info("  Throughput          : {} MJ/s",
               static_cast<double>(total_records_dequeued) / t_conv.seconds() * 1e-6);

  auto a = Aggregate(converter->metrics());
  spdlog::info("Details:");
  LogConvertMetrics(a, "  ");
  SaveLatencyMetrics(latencies, opts.latency_file, TimePoints::parsed,
                     TimePoints::popped);
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
