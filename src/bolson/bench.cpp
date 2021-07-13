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

#include "bolson/convert/converter.h"
#include "bolson/convert/metrics.h"
#include "bolson/parse/parser.h"
#include "bolson/publish/bench.h"
#include "bolson/status.h"
#include "bolson/utils.h"

namespace bolson {

static auto FillBuffer(const arrow::Schema& schema,
                       const illex::GenerateOptions& gen_opts, illex::JSONBuffer* buffer,
                       size_t max_json_bytes = 0, size_t start_seq = 0) -> Status {
  // Set up generator.
  auto gen = illex::FromArrowSchema(schema, gen_opts);
  // Get start and end pointers.
  auto pos = reinterpret_cast<char*>(buffer->mutable_data());
  auto end = pos + buffer->capacity();
  size_t num_jsons = 0;
  size_t num_bytes = 0;
  // Keep filling until we reach the end or the max number of bytes if enabled.
  while ((pos < end) && (((max_json_bytes > 0) && (num_bytes < max_json_bytes)) ||
                         (max_json_bytes == 0))) {
    std::string json = gen.GetString();
    // Copy the string if it fits.
    if ((pos + json.size() + 1) < end) {
      // Place the JSON document string.
      std::memcpy(pos, json.data(), json.size());
      pos += json.size();
      num_bytes += json.size();
      // Place a newline.
      *pos = '\n';
      pos++;
      num_bytes++;
      // Increase number of generated JSONs.
      num_jsons++;
    } else {
      // Fill the remaining bytes with whitespace if needed.
      if (pos < end) {
        std::memset(pos, ' ', end - pos);
      }
      pos = end;
    }
  }

  // Update buffer properties
  auto ret = buffer->SetSize(num_bytes);
  if (!ret.ok()) {
    return Status(Error::IllexError, ret.msg());
  }
  buffer->SetRange({start_seq, start_seq + num_jsons - 1});

  return Status::OK();
}

static auto FillBuffers(const arrow::Schema& schema,
                        const illex::GenerateOptions& gen_opts,
                        const std::vector<illex::JSONBuffer*>& buffers,
                        size_t approx_total_json_bytes, size_t* gen_bytes,
                        size_t* gen_jsons) -> Status {
  assert(!buffers.empty());
  size_t approx_bytes_per_buffer = 0;

  // Calculate the approximate bytes to fill per buffer if an approximate total is given.
  if (approx_total_json_bytes > 0) {
    approx_bytes_per_buffer = DivideCeil(approx_total_json_bytes, buffers.size());
    // Issue a warning when the approximate total would overflow a buffer.
    if (approx_bytes_per_buffer > buffers[0]->capacity()) {
      spdlog::warn(
          "Approximate total JSON bytes {} divided over {} input buffers of capacity {} "
          "would overflow. Reverting to filling buffers as much as possible.",
          approx_total_json_bytes, buffers.size(), buffers[0]->capacity());
    }
  }

  int seed = gen_opts.seed;
  size_t next_seq = 0;
  *gen_bytes = 0;
  *gen_jsons = 0;
  for (size_t i = 0; i < buffers.size(); i++) {
    auto& buf = buffers[i];
    // Copy the options to modify the seed for each buffer.
    illex::GenerateOptions bfo = gen_opts;
    bfo.seed = seed;
    seed += 42;
    // Fill the buffer.
    BOLSON_ROE(FillBuffer(schema, bfo, buf, approx_bytes_per_buffer, next_seq));
    // Update the number of bytes and JSONs.
    *gen_bytes += buf->size();
    *gen_jsons += buf->num_jsons();
    SPDLOG_DEBUG("Filled buffer {} with {} JSONs, {} bytes.", i, buf->num_jsons(),
                 buf->size());
    // Determine the sequence number for the next buffer.
    next_seq = buf->range().last + 1;
  }

  return Status::OK();
}

auto BenchConvert(const ConvertBenchOptions& opts) -> Status {
  putong::Timer<> t_gen, t_init, t_conv;
  auto o = opts;

  BOLSON_ROE(o.converter.parser.arrow.ReadSchema());

  spdlog::info("Initializing converter...");
  t_init.Start();

  // Fix options if not supplied.
  convert::ConverterOptions conv_opts = o.converter;

  // Set up output queue & vector.
  publish::IpcQueue ipc_queue;

  // Construct converter.
  std::shared_ptr<convert::Converter> converter;
  BOLSON_ROE(convert::Converter::Make(conv_opts, &ipc_queue, &converter));

  spdlog::info("Converter schema:\n{}",
               converter->parser_context()->output_schema()->ToString());

  // Grab the input buffers from the parser context.
  auto buffers = converter->parser_context()->mutable_buffers();

  spdlog::info("Filling buffers...");
  t_gen.Start();
  size_t gen_bytes = 0;
  size_t gen_jsons = 0;
  FillBuffers(*o.converter.parser.arrow.schema, o.generate, buffers, o.approx_total_bytes,
              &gen_bytes, &gen_jsons);
  t_gen.Stop();
  spdlog::info("Generated {} JSONs", gen_jsons);

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
    while ((num_records_dequeued != gen_jsons) && !shutdown.load()) {
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

  // Derive human-readible statistics.

  // Generate bytes and JSONs
  auto gen_MiB = static_cast<double>(gen_bytes) / static_cast<double>(1 << 20);
  auto gen_MB = static_cast<double>(gen_bytes) / 1e6;
  auto gen_MJ = static_cast<double>(gen_jsons) / 1e6;
  // Converted JSON bytes:
  auto json_MB = static_cast<double>(opts.repeats * gen_bytes) / 1e6;
  // IPC messages:
  auto ipc_MB = static_cast<double>(total_bytes_dequeued) / 1e6;

  spdlog::info("JSON Generation:");
  spdlog::info("  JSONs               : {} JSON", gen_jsons);
  spdlog::info("  Bytes               : {} B, {:.3f} MiB", gen_bytes, gen_MiB);
  spdlog::info("  Time                : {} s", t_gen.seconds());
  spdlog::info("  Throughput          : {:.3f} MB/s", gen_MB / t_gen.seconds());
  spdlog::info("  Throughput          : {:.3f} MJSON/s", gen_MJ / t_gen.seconds());

  spdlog::info("End-to-end conversion:");
  spdlog::info("  IPC messages        : {}", total_messages_dequeued);
  spdlog::info("  Time                : {} s", t_conv.seconds());
  spdlog::info("  Throughput (in)     : {:.3f} MB/s", json_MB / t_conv.seconds());
  spdlog::info("  Throughput (out)    : {:.3f} MB/s", ipc_MB / t_conv.seconds());
  spdlog::info("  Throughput          : {:.3f} MJSON/s", gen_MJ / t_conv.seconds());

  auto a = Aggregate(converter->metrics());
  spdlog::info("Details:");
  LogConvertMetrics(a, "  ");
  if (!o.latency_file.empty()) {
    BOLSON_ROE(SaveLatencyMetrics(latencies, opts.latency_file, TimePoints::parsed,
                                  TimePoints::popped));
  }
  if (!o.metrics_file.empty()) {
    BOLSON_ROE(SaveConvertMetrics(converter->metrics(), o.metrics_file));
  }
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

auto ConvertBenchOptions::ParseInput() -> Status {
  // Propagate parse only down to converter options
  this->converter.mock_serialize = this->parse_only;
  this->converter.mock_resize = this->parse_only;

  BOLSON_ROE(ParseWithScale(this->approx_total_bytes_str, &this->approx_total_bytes));

  // Parse converter options.
  BOLSON_ROE(this->converter.ParseInput());

  return Status::OK();
}

}  // namespace bolson
