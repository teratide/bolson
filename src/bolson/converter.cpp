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

namespace bolson {

static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

static inline auto GetBatchesSize(const std::vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
  size_t result = 0;
  for (const auto &batch : batches) {
    result += GetBatchSize(batch);
  }
  return result;
}

/**
 * \brief Take a JSONQueueItem and convert it to an Arrow RecordBatch
 * \param item          The item to convert.
 * \param parse_options Options to parse the JSON.
 * \return A RecordBatch with the parsed JSON represented as single RecordBatch row.
 */
static inline auto ConvertJSON(const illex::JSONQueueItem &item,
                               const arrow::json::ParseOptions &parse_options) -> std::shared_ptr<arrow::RecordBatch> {
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
#ifndef DEBUG
  if (!batch_result.ok()) {
    SPDLOG_DEBUG("Failed to parse: {}", item.string);
  }
#endif
  // Construct the column for the sequence number and append it.
  std::shared_ptr<arrow::Array> seq;
  arrow::UInt64Builder seq_builder;
  seq_builder.Append(item.seq); // todo: check status
  seq_builder.Finish(&seq); // todo: check status
  auto batch_with_seq = batch_result.ValueOrDie()->AddColumn(0, SeqField(), seq).ValueOrDie();

  return batch_with_seq;
}

static void ConversionDroneThread(size_t id,
                                  std::atomic<bool> *shutdown,
                                  std::promise<ConversionStats> &&stats_promise,
                                  illex::JSONQueue *in,
                                  IpcQueue *out,
                                  const arrow::json::ParseOptions &parse_options,
                                  size_t batch_threshold) {
  SPDLOG_DEBUG("[conversion] [drone {}] Spawned.", id);

  // Prepare some timers
  putong::Timer thread_timer(true);
  putong::Timer convert_timer;

  // Prepare statistics
  ConversionStats stats;

  // Prepare a vector to hold RecordBatches that we collapse into a single RecordBatch at the end.
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

  // Reserve a queue item
  illex::JSONQueueItem json_item;

  // Loop until thread is stopped.
  while (!shutdown->load()) {

    auto batches_size = GetBatchesSize(batches);
    bool attempt_dequeue = true;

    // First, check if the RecordBatches reached their threshold size.
    if (batches_size >= batch_threshold) {
      SPDLOG_DEBUG("Size of batches has reached threshold. Current size: {} bytes", batches_size);
      attempt_dequeue = false;
    }

    // Attempt to dequeue an item if the size of the current RecordBatches is not larger than the limit.
    if (attempt_dequeue && in->try_dequeue(json_item)) {
      // There is a JSON.
      SPDLOG_DEBUG("Drone {} popping JSON: {}.", id, json_item.string);

      // Convert the JSON
      convert_timer.Start();
      auto new_batch = ConvertJSON(json_item, parse_options);
      convert_timer.Stop();

      // Append the new batch containing just one row to the message batch.
      batches.push_back(new_batch);

      stats.convert_time += convert_timer.seconds();
      stats.num_jsons++;
    } else {
      // If there is nothing in the queue, construct the IPC message to have this batch sent off with lowest possible
      // latency.

      // Of course, there has to be at least one batch.
      if (!batches.empty()) {
        std::shared_ptr<arrow::RecordBatch> packed_batch;

        // Only wrap a table and combine chunks if there is more than one RecordBatch.
        if (batches.size() == 1) {
          packed_batch = batches[0];
        } else {
          auto packed_table = arrow::Table::FromRecordBatches(batches).ValueOrDie()->CombineChunks().ValueOrDie();
          auto table_reader = arrow::TableBatchReader(*packed_table);
          packed_batch = table_reader.Next().ValueOrDie();
        }
        SPDLOG_DEBUG("Packed IPC batch: {}", packed_batch->ToString());

        // Convert to Arrow IPC message.
        auto ipc_item = IpcQueueItem{
            static_cast<size_t>(packed_batch->num_rows()),
            arrow::ipc::SerializeRecordBatch(*packed_batch, arrow::ipc::IpcWriteOptions::Defaults()).ValueOrDie()
        };

        stats.total_ipc_bytes += ipc_item.ipc->size();
        stats.num_ipc++;

        // Keep trying to enqueue by copy until successful.
        // (This only copies a shared ptr to the data, but not the data itself.)
        while (!out->try_enqueue(ipc_item)) {
          //todo: maybe use condition_variable here
#ifndef NDEBUG
          // Slow this down a bit in debug.
          std::this_thread::sleep_for(std::chrono::milliseconds(500));
          SPDLOG_DEBUG("Drone {} attempt to enqueue IPC message failed.", id);
#endif
        }
        SPDLOG_DEBUG("Drone {} enqueued IPC message.", id);

        // Clear the current batches
        batches.clear();
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
