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

#include "bolson/convert/cpu.h"
#include "bolson/log.h"
#include "bolson/utils.h"

namespace bolson::convert {

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

auto ArrowIPCBuilder::FlushBuffered(putong::Timer<> *t,
                                    illex::LatencyTracker *lat_tracker) -> Status {
  // Check if there is anything to flush.
  if (str_buffer->size() > 0) {

    // Mark time point buffer is flushed into table reader.
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BUFFER_FLUSH, illex::Timer::now());
    }

    auto br = std::make_shared<arrow::io::BufferReader>(str_buffer);
    auto tr = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                             br,
                                             read_options,
                                             parse_options).ValueOrDie();
    t->Start();
    auto table = tr->Read().ValueOrDie();
    t->Stop();

    // Mark time point buffer is parsed
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BUFFER_PARSED, illex::Timer::now());
    }

    // Construct the column for the sequence number.
    std::shared_ptr<arrow::Array> seq;
    ARROW_ROE(seq_builder->Finish(&seq));
    auto chunked_seq = std::make_shared<arrow::ChunkedArray>(seq);

    // Add the column to the batch.
    auto tab_with_seq_result = table->AddColumn(0, SeqField(), chunked_seq);
    ARROW_ROE(tab_with_seq_result.status());
    auto table_with_seq = tab_with_seq_result.ValueOrDie();

    auto combined_table = table_with_seq->CombineChunks();
    auto table_reader = arrow::TableBatchReader(*combined_table.ValueOrDie());
    auto combined_batch = table_reader.Next().ValueOrDie();

    this->size_ += GetBatchSize(combined_batch);
    this->batches.push_back(std::move(combined_batch));

    // Mark time point sequence numbers are added.
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BATCH_CONSTRUCTED, illex::Timer::now());
    }

    // Clear the buffers.
    this->lat_tracked_seq_in_buffer.clear();
    ARROW_ROE(this->str_buffer->Resize(0));
  }
  return Status::OK();
}

void ConvertWithCPU(illex::JSONQueue *in,
                    IpcQueue *out,
                    std::atomic<bool> *shutdown,
                    size_t num_drones,
                    const arrow::json::ParseOptions &parse_options,
                    const arrow::json::ReadOptions &read_options,
                    size_t json_buffer_threshold,
                    size_t batch_size_threshold,
                    illex::LatencyTracker *lat_tracker,
                    std::promise<std::vector<Stats>> &&stats) {
  // Reserve some space for the thread handles and futures.
  std::vector<std::thread> threads;
  std::vector<std::future<Stats>> thread_stats_futures;
  std::vector<Stats> threads_stats(num_drones);
  threads.reserve(num_drones);
  thread_stats_futures.reserve(num_drones);

  SPDLOG_DEBUG("Starting {} JSON conversion drones.", num_drones);
  SPDLOG_DEBUG("  with JSON threshold: {}", json_buffer_threshold);

  // Start the conversion threads.
  for (int thread_id = 0; thread_id < num_drones; thread_id++) {
    // Prepare the future/promise.
    std::promise<Stats> thread_stats_promise;
    thread_stats_futures.push_back(thread_stats_promise.get_future());
    // Create builders based on Arrow's CPU implementation.
    auto builder = std::make_unique<ArrowIPCBuilder>(parse_options,
                                                     read_options,
                                                     json_buffer_threshold,
                                                     batch_size_threshold);
    threads.emplace_back(Convert,
                         thread_id,
                         std::move(builder),
                         in,
                         out,
                         lat_tracker,
                         shutdown,
                         std::move(thread_stats_promise));
  }

  // Wait for the drones to get shut down by the caller of this function.
  // Also gather the statistics and see if there was any error.
  for (size_t t = 0; t < num_drones; t++) {
    if (threads[t].joinable()) {
      threads[t].join();
      threads_stats[t] = thread_stats_futures[t].get();
      if (!threads_stats[t].status.ok()) {
        // If a thread returned some error status, shut everything down without waiting
        // for the caller to do so.
        shutdown->store(true);
      }
    }
  }

  stats.set_value(threads_stats);
}

}
