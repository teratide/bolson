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

#include "bolson/converter.h"
#include "bolson/log.h"
#include "bolson/utils.h"

/// Convert Arrow status and return on error.
#define ARROW_ROE(s) {                                                  \
  const auto& status = s;                                               \
  if (!status.ok()) return Status(Error::ArrowError, status.ToString());\
}                                                                       \
void()

namespace bolson {

static void ConversionDroneThread(size_t id,
                                  std::atomic<bool> *shutdown,
                                  std::promise<ConversionStats> &&stats_promise,
                                  illex::JSONQueue *in,
                                  IpcQueue *out,
                                  const arrow::json::ParseOptions &parse_options,
                                  size_t batch_threshold) {
  // Prepare some timers
  putong::Timer thread_timer(true);
  putong::Timer convert_timer;
  putong::Timer ipc_construct_timer;

  SPDLOG_DEBUG("[conversion] [drone {}] Spawned.", id);

  // Prepare BatchBuilder
  BatchBuilder builder(parse_options);

  // Prepare statistics
  ConversionStats stats;

  // Reserve a queue item
  illex::JSONQueueItem json_item;

  // Triggers for IPC construction
  size_t batches_size = 0;
  bool attempt_dequeue = true;

  // Loop until thread is stopped.
  while (!shutdown->load()) {
    // Attempt to dequeue an item if the size of the current RecordBatches is not larger than the limit.
    if (attempt_dequeue && in->wait_dequeue_timed(json_item, std::chrono::microseconds(1))) {
      // There is a JSON.
      SPDLOG_DEBUG("Drone {} popping JSON: {}.", id, json_item.string);
      // Convert the JSON.
      convert_timer.Start();
      stats.status = builder.Append(json_item);
      if (!stats.status.ok()) {
        break;
      }
      convert_timer.Stop();
      // Check if threshold was reached.
      if (builder.size() >= batch_threshold) {
        SPDLOG_DEBUG("Size of batches has reached threshold. Current size: {} bytes", builder.size());
        // We reached the threshold, so in the next iteration, just send off the RecordBatch.
        attempt_dequeue = false;
      }
      // Update stats.
      stats.convert_time += convert_timer.seconds();
      stats.num_jsons++;
    } else {
      // If there is nothing in the queue or if the RecordBatch size threshold has been reached, construct the IPC
      // message to have this batch sent off. When the queue is empty, this causes the latency to be low. When it is
      // full, it still provides good throughput.

      // There has to be at least one batch.
      if (!builder.empty()) {
        // Finish the builder, delivering an IPC item.
        ipc_construct_timer.Start();
        auto ipc_item = builder.Finish();
        ipc_construct_timer.Stop();

        // Enqueue the IPC item.
        out->enqueue(ipc_item);

        SPDLOG_DEBUG("Drone {} enqueued IPC message.", id);

        // Attempt to dequeue again.
        attempt_dequeue = true;

        // Update stats.
        stats.ipc_construct_time += ipc_construct_timer.seconds();
        stats.total_ipc_bytes += ipc_item.ipc->size();
        stats.num_ipc++;
      }
    }
  }
  thread_timer.Stop();
  stats.thread_time = thread_timer.seconds();
  stats_promise.set_value(stats);
  SPDLOG_DEBUG("Drone {} Terminating.", id);
}

void ConversionHiveThread(illex::JSONQueue *in,
                          IpcQueue *out,
                          std::atomic<bool> *shutdown,
                          size_t num_drones,
                          const arrow::json::ParseOptions &parse_options,
                          size_t batch_threshold,
                          std::promise<std::vector<ConversionStats>> &&stats) {
  // Reserve some space for the thread handles and futures.
  std::vector<std::thread> threads;
  std::vector<std::future<ConversionStats>> futures;
  std::vector<ConversionStats> threads_stats(num_drones);
  threads.reserve(num_drones);
  futures.reserve(num_drones);

  SPDLOG_DEBUG("Starting {} JSON conversion drones.", num_drones);

  // Start the conversion threads.
  for (int thread = 0; thread < num_drones; thread++) {
    // Prepare the future/promise.
    std::promise<ConversionStats> promise_stats;
    futures.push_back(promise_stats.get_future());
    threads.emplace_back(ConversionDroneThread,
                         thread,
                         shutdown,
                         std::move(promise_stats),
                         in,
                         out,
                         parse_options,
                         batch_threshold);
  }

  // Wait for the drones to get shut down by the caller of this function.
  // Also gather the statistics and see if there was any error.
  for (size_t t = 0; t < num_drones; t++) {
    if (threads[t].joinable()) {
      threads[t].join();
      threads_stats[t] = futures[t].get();
      if (!threads_stats[t].status.ok()) {
        // If a thread returned some error status, shut everything down without waiting for the caller to do so.
        shutdown->store(true);
      }
    }
  }

  stats.set_value(threads_stats);
}

auto BatchBuilder::Append(const illex::JSONQueueItem &item) -> Status {
  // Wrap Arrow buffer around the JSON string so we can parse it using Arrow.
  auto buffer_wrapper =
      std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(item.string.data()), item.string.length());
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
  auto batch_result = arrow::json::ParseOne(parse_options, buffer_wrapper);
  ARROW_ROE(batch_result.status());

  // Construct the column for the sequence number.
  std::shared_ptr<arrow::Array> seq;
  arrow::UInt64Builder seq_builder;
  ARROW_ROE(seq_builder.Append(item.seq));
  ARROW_ROE(seq_builder.Finish(&seq));

  // Add the column to the batch.
  auto batch_with_seq_result = batch_result.ValueOrDie()->AddColumn(0, SeqField(), seq);
  ARROW_ROE(batch_with_seq_result.status());
  auto batch_with_seq = batch_with_seq_result.ValueOrDie();

  this->size_ += GetBatchSize(batch_with_seq);
  this->batches.push_back(std::move(batch_with_seq));

  return Status::OK();
}

void BatchBuilder::Reset() {
  this->batches.clear();
  this->size_ = 0;
}

auto BatchBuilder::Finish() -> IpcQueueItem {
  // Set up a pointer for the combined batch.
  std::shared_ptr<arrow::RecordBatch> combined_batch;

  // Only wrap a table and combine chunks if there is more than one RecordBatch.
  if (batches.size() == 1) {
    combined_batch = batches[0];
  } else {
    auto packed_table = arrow::Table::FromRecordBatches(batches).ValueOrDie()->CombineChunks().ValueOrDie();
    auto table_reader = arrow::TableBatchReader(*packed_table);
    combined_batch = table_reader.Next().ValueOrDie();
  }
  SPDLOG_DEBUG("Packed IPC batch: {}", combined_batch->ToString());

  // Convert to Arrow IPC message.
  auto ipc_item = IpcQueueItem{
      static_cast<size_t>(combined_batch->num_rows()),
      arrow::ipc::SerializeRecordBatch(*combined_batch, arrow::ipc::IpcWriteOptions::Defaults()).ValueOrDie()
  };

  this->Reset();

  return ipc_item;
}

}
