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

#include <putong/putong.h>

#include "bolson/convert/convert_buffered.h"
#include "bolson/convert/cpu.h"
#include "bolson/log.h"
#include "bolson/utils.h"

namespace bolson::convert {

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

auto QueuedArrowIPCBuilder::FlushBuffered(putong::Timer<> *parse,
                                          putong::Timer<> *seq,
                                          illex::LatencyTracker *lat_tracker) -> Status {
  // Check if there is anything to flush.
  if (str_buffer->size() > 0) {

    // Mark time point buffer is flushed into table reader.
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BUFFER_FLUSH, illex::Timer::now());
    }

    parse->Start();
    auto br = std::make_shared<arrow::io::BufferReader>(str_buffer);
    auto tr = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                             br,
                                             read_options,
                                             parse_options).ValueOrDie();
    auto table = tr->Read().ValueOrDie();
    parse->Stop();

    // Mark time point buffer is parsed
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BUFFER_PARSED, illex::Timer::now());
    }

    seq->Start(); // Measure throughput of adding sequence numbers.
    // Construct the column for the sequence number.
    // TODO: Arrows JSON parser outputs a Table that may be chunked.
    std::shared_ptr<arrow::Array> seq_no;
    ARROW_ROE(seq_builder->Finish(&seq_no));
    auto chunked_seq = std::make_shared<arrow::ChunkedArray>(seq_no);

    // Add the column to the batch.
    auto tab_with_seq_result = table->AddColumn(0, SeqField(), chunked_seq);
    ARROW_ROE(tab_with_seq_result.status());
    auto table_with_seq = tab_with_seq_result.ValueOrDie();

    auto combined_table = table_with_seq->CombineChunks();
    auto table_reader = arrow::TableBatchReader(*combined_table.ValueOrDie());
    auto combined_batch = table_reader.Next().ValueOrDie();
    seq->Stop();

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

void ConvertFromQueueWithCPU(illex::JSONQueue *in,
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
    auto builder = std::make_unique<QueuedArrowIPCBuilder>(parse_options,
                                                           read_options,
                                                           json_buffer_threshold,
                                                           batch_size_threshold);
    threads.emplace_back(ConvertFromQueue,
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

void ConvertFromBuffersWithCPU(const std::vector<illex::RawJSONBuffer *> &buffers,
                               const std::vector<std::mutex *> &mutexes,
                               IpcQueue *out,
                               std::atomic<bool> *shutdown,
                               size_t num_drones,
                               const arrow::json::ParseOptions &parse_options,
                               const arrow::json::ReadOptions &read_options,
                               size_t json_buffer_threshold,
                               size_t batch_size_threshold,
                               illex::LatencyTracker *lat_tracker,
                               std::promise<std::vector<Stats>> &&stats) {
  std::vector<std::thread> threads;
  std::vector<std::future<Stats>> thread_stats_futures;
  std::vector<Stats> threads_stats(num_drones);
  threads.reserve(num_drones);
  thread_stats_futures.reserve(num_drones);

  // Start the conversion threads.
  for (int thread_id = 0; thread_id < num_drones; thread_id++) {
    // Prepare the future/promise.
    std::promise<Stats> thread_stats_promise;
    thread_stats_futures.push_back(thread_stats_promise.get_future());
    // Create builders based on Arrow's CPU implementation.
    auto builder = std::make_unique<ArrowBufferedIPCBuilder>(parse_options,
                                                             read_options,
                                                             batch_size_threshold);

    threads.emplace_back(ConvertFromBuffers,
                         thread_id,
                         std::move(builder),
                         buffers,
                         mutexes,
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

auto ArrowBufferedIPCBuilder::ConvertBuffer(illex::RawJSONBuffer *in,
                                            putong::Timer<> *parse,
                                            illex::LatencyTracker *lat_tracker) -> Status {
  // Mark time point buffered JSONs start parsing.
  // Also remember seq. no's tracked in this builder.
  for (auto s : in->seq_tracked()) {
    lat_tracker->Put(s, BOLSON_LAT_BUFFER_FLUSH, illex::Timer::now());
    this->lat_tracked_seq_in_batches.push_back(s);
  }

  // Measure parsing time for throughput numbers.
  parse->Start();
  auto buffer = arrow::Buffer::Wrap(in->data(), in->size());
  auto br = std::make_shared<arrow::io::BufferReader>(buffer);
  auto tr = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                           br,
                                           read_options,
                                           parse_options);
  if (!tr.ok()) {
    return Status(Error::ArrowError, tr.status().message());
  }
  auto table = tr.ValueOrDie()->Read();
  if (!table.ok()) {
    return Status(Error::ArrowError, table.status().message());
  }
  // Combine potential chunks in this table and read the first batch.
  auto combined_table = table.ValueOrDie()->CombineChunks();
  if (!combined_table.ok()) {
    return Status(Error::ArrowError, combined_table.status().message());
  }
  auto table_reader = arrow::TableBatchReader(*combined_table.ValueOrDie());
  auto combined_batch = table_reader.Next();
  if (!combined_batch.ok()) {
    return Status(Error::ArrowError, combined_batch.status().message());
  }

  auto final_batch = combined_batch.ValueOrDie();
  parse->Stop();

  // Append sequence numbers.
  // Make some room and use faster impl to append.
  auto status = seq_builder->Resize(seq_builder->length() + in->num_jsons());
  if (!status.ok()) {
    return Status(Error::ArrowError, "Couldn't resize seq builder: " + status.message());
  }
  for (uint64_t s = in->first(); s <= in->last(); s++) {
    seq_builder->UnsafeAppend(s);
  }

  this->size_ += GetBatchSize(final_batch);
  this->batches.push_back(std::move(final_batch));

  // Reset the buffer so it can be reused.
  in->Reset();

  // Mark time point buffer is parsed
  for (auto s : in->seq_tracked()) {
    lat_tracker->Put(s, BOLSON_LAT_BUFFER_PARSED, illex::Timer::now());
  }

  return Status::OK();
}

}
