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

#include <thread>
#include <memory>
#include <arrow/json/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <illex/queue.h>
#include <putong/putong.h>

#include "flitter/converter.h"
#include "flitter/log.h"

namespace flitter {

void ConversionDroneThread(size_t id,
                           illex::Queue *in,
                           IpcQueue *out,
                           std::atomic<bool> *shutdown,
                           std::promise<ConversionStats> &&stats_promise) {
  putong::Timer thread_timer(true);
  ConversionStats stats;

  SPDLOG_DEBUG("[conversion] [drone {}] Spawned.", id);

  auto parse_options = arrow::json::ParseOptions::Defaults();
  putong::Timer convert_timer;
  std::string raw_json;
  // TODO(johanpel): Use a safer mechanism to stop the thread
  while (!shutdown->load()) {
    if (in->try_dequeue(raw_json)) {
      convert_timer.Start();
      SPDLOG_DEBUG("Drone {} popping JSON: {}.", id, raw_json);
      // Wrap Arrow buffer around the JSON string.
      auto buffer_wrapper =
          std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(raw_json.data()), raw_json.length());

      // The following code could be better because it seems like:
      // - it always delivers a chunked table
      // - we cannot reuse the internal builders for new JSONs that are being dequeued, we can only read once.
      // - we could try to work-around previous by buffering JSONs, but that would increase latency.
      // - we wouldn't know how long to buffer because the i/o size ratio is not known,
      //   - preferably we would append jsons until some threshold is reached, then construct the ipc message.

      /*
      // Wrap a random access file abstraction around the buffer.
      auto ra_buffer = arrow::Buffer::GetReader(buffer_wrapper).ValueOrDie();
      // Construct an arrow json TableReader.
      auto reader = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                                   ra_buffer,
                                                   arrow::json::ReadOptions::Defaults(),
                                                   arrow::json::ParseOptions::Defaults()).ValueOrDie();
      auto table = reader->Read().ValueOrDie();
      auto batch = table->CombineChunks().ValueOrDie();
      */

      // The following code could be better because it seems like:
      // - we cannot reuse the internal builders for new JSONs that are being dequeued, we can only read once.
      // - we could try to work-around previous by buffering JSONs, but that would increase latency.
      // - we wouldn't know how long to buffer because the i/o size ratio is not known,
      //   - preferably we would append jsons until some threshold is reached, then construct the ipc message.

      // TODO(johanpel): come up with a solution for all of this. Also don't use ValueOrDie();
      // Construct a RecordBatch from the JSON string.
      auto batch = arrow::json::ParseOne(parse_options, buffer_wrapper).ValueOrDie();
      // Convert to Arrow IPC message.
      auto ipc = arrow::ipc::SerializeRecordBatch(*batch, arrow::ipc::IpcWriteOptions::Defaults()).ValueOrDie();
      convert_timer.Stop();
      stats.convert_time += convert_timer.seconds();
      stats.num_jsons++;
      stats.num_ipc++;
      stats.ipc_bytes += ipc->size();
      // Move the ipc buffer into the queue.
      while (!out->try_enqueue(std::move(ipc))) {
#ifndef NDEBUG
        // Slow this down a bit in debug.
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        SPDLOG_DEBUG("Drone {} attempt to enqueue IPC message failed.", id);
#endif
      }
      SPDLOG_DEBUG("Drone {} enqueued IPC message.", id);
    }
  }
  thread_timer.Stop();
  stats.thread_time = thread_timer.seconds();
  stats_promise.set_value(stats);
  SPDLOG_DEBUG("Drone {} Terminating.", id);
}

void ConversionHiveThread(illex::Queue *in,
                          IpcQueue *out,
                          std::atomic<bool> *shutdown,
                          size_t num_drones,
                          std::promise<std::vector<ConversionStats>> &&stats) {
  // Reserve some space for the thread handles and futures.
  std::vector<std::thread> threads;
  std::vector<std::future<ConversionStats>> futures;
  threads.reserve(num_drones);
  futures.reserve(num_drones);

  SPDLOG_DEBUG("Starting {} JSON conversion drones.", num_drones);

  // Start the conversion threads.
  for (int thread = 0; thread < num_drones; thread++) {
    // Prepare the future/promise.
    std::promise<ConversionStats> promise_stats;
    futures.push_back(promise_stats.get_future());
    threads.emplace_back(ConversionDroneThread, thread, in, out, shutdown, std::move(promise_stats));
  }

  // Wait for the caller to shut conversion down.
  while (!shutdown->load()) {
    for (auto &thread : threads) {
      thread.join();
    }
  }

  // Gather the statistics for each thread.
  std::vector<ConversionStats> threads_stats;
  threads_stats.reserve(num_drones);
  for (auto &f : futures) {
    threads_stats.push_back(f.get());
  }

  stats.set_value(threads_stats);
}

}
