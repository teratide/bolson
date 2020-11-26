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

BatchBuilder::BatchBuilder(size_t seq_buf_init_size,
                           size_t str_buf_init_size) {
  str_buffer = std::shared_ptr(std::move(
      arrow::AllocateResizableBuffer(0)
          .ValueOrDie()));
  str_buffer->Reserve(str_buf_init_size);
  std::unique_ptr<arrow::ArrayBuilder> arrow_pls;
  arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint64(), &arrow_pls);
  seq_builder =
      std::move(std::unique_ptr<arrow::UInt64Builder>(dynamic_cast<arrow::UInt64Builder *>(arrow_pls.release())));
}

auto BatchBuilder::Buffer(const illex::JSONQueueItem &item) -> Status {
  // Obtain current sizes.
  size_t current_size = this->str_buffer->size();
  size_t str_len = item.string.length();

  // Make some space.
  ARROW_ROE(this->str_buffer->Resize(current_size + str_len + 1)); // +1 for '\n'

  // Copy string into buffer and place a newline.
  uint8_t *offset = this->str_buffer->mutable_data() + current_size;
  memcpy(offset, item.string.data(), str_len); // we know what we're doing clang tidy
  offset[str_len] = '\n';

  // Copy sequence number into vector.
  this->seq_builder->Append(item.seq);

  return Status::OK();
}

void BatchBuilder::Reset() {
  this->batches.clear();
  this->size_ = 0;
}

auto BatchBuilder::Finish(IpcQueueItem *out) -> Status {
  // Set up a pointer for the combined batch.6
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> combined_batch_r;
  std::shared_ptr<arrow::RecordBatch> combined_batch;

  // Only wrap a table and combine chunks if there is more than one RecordBatch.
  if (batches.size() == 1) {
    combined_batch = batches[0];
  } else {
    auto packed_table = arrow::Table::FromRecordBatches(batches);
    if (!packed_table.ok()) {
      return Status(Error::ArrowError,
                    "Could not create table: " + packed_table.status().message());
    }
    auto combined_table = packed_table.ValueOrDie()->CombineChunks();
    if (!combined_table.ok()) {
      return Status(Error::ArrowError,
                    "Could not pack table chunks: " + combined_table.status().message());
    }
    auto table_reader = arrow::TableBatchReader(*combined_table.ValueOrDie());
    combined_batch_r = table_reader.Next();
    if (!combined_batch_r.ok()) {
      return Status(Error::ArrowError,
                    "Could not pack table chunks: "
                        + combined_batch_r.status().message());
    }
    combined_batch = combined_batch_r.ValueOrDie();

  }

  SPDLOG_DEBUG("Packed IPC batch: {}", combined_batch->ToString());

  //
  auto serialized = arrow::ipc::SerializeRecordBatch(*combined_batch,
                                                     arrow::ipc::IpcWriteOptions::Defaults());
  if (!serialized.ok()) {
    return Status(Error::ArrowError,
                  "Could not serialize batch: " + serialized.status().message());
  }

  // Convert to Arrow IPC message.
  auto ipc_item = IpcQueueItem{static_cast<size_t>(combined_batch->num_rows()),
                               serialized.ValueOrDie()};

  this->Reset();

  *out = ipc_item;

  return Status::OK();
}

auto BatchBuilder::ToString() -> std::string {
  std::stringstream o;
  o << "Batch builder:" << std::endl;
  o << "  Buffered batches: " << this->batches.size() << std::endl;
  o << "  Buffered batches size: " << this->size() << std::endl;
  o << "  Buffered JSONS: " << this->num_buffered() << std::endl;
  o << "  JSON buffer: " << this->str_buffer->size() << std::endl;
  return o.str();
}

auto ArrowBatchBuilder::AppendAsBatch(const illex::JSONQueueItem &item) -> Status {
  // Wrap Arrow buffer around the JSON string so we can parse it using Arrow.
  auto buffer_wrapper =
      std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(item.string.data()),
                                      item.string.length());
  // The following code could be better because it seems like:
  // - it always delivers a chunked table
  // - we cannot reuse the internal builders for new JSONs that are being dequeued, we
  //   can only read once.
  // - we could try to work-around previous by buffering JSONs, but that would increase
  //   latency.
  // - we wouldn't know how long to buffer because the i/o size ratio is not known,
  //   - preferably we would append jsons until some threshold is reached, then construct
  //     the ipc message.

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
  // - we cannot reuse the internal builders for new JSONs that are being dequeued, we
  //   can only read once.
  // - we could try to work-around previous by buffering JSONs, but that would increase
  //   latency.
  // - we wouldn't know how long to buffer because the i/o size ratio is not known,
  //   - preferably we would append jsons until some threshold is reached, then construct
  //     the ipc message.

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

auto ArrowBatchBuilder::FlushBuffered() -> Status {
  // Check if there is anything to flush.
  if (str_buffer->size() > 0) {
    SPDLOG_DEBUG("Flushing: {}",
                 std::string(reinterpret_cast<const char *>(str_buffer->data()),
                             str_buffer->size()));
    auto br = std::make_shared<arrow::io::BufferReader>(str_buffer);
    auto tr = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                             br,
                                             arrow::json::ReadOptions::Defaults(),
                                             parse_options).ValueOrDie();
    auto table = tr->Read().ValueOrDie();

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

    ARROW_ROE(this->str_buffer->Resize(0));
  }
  return Status::OK();
}

#define SHUTDOWN_ON_FAILURE() \
if (!stats.status.ok()) { \
  thread_timer.Stop(); \
  stats.thread_time = thread_timer.seconds(); \
  stats_promise.set_value(stats); \
  SPDLOG_DEBUG("Drone {} Terminating with errors.", id); \
  shutdown->store(true); \
  return; \
} void()

static void ConversionDroneThread(size_t id,
                                  std::atomic<bool> *shutdown,
                                  std::promise<Stats> &&stats_promise,
                                  illex::JSONQueue *in,
                                  IpcQueue *out,
                                  const arrow::json::ParseOptions &parse_options,
                                  const arrow::json::ReadOptions &read_options,
                                  size_t json_threshold,
                                  size_t batch_threshold) {
  // Prepare some timers
  putong::Timer thread_timer(true);
  putong::Timer convert_timer;
  putong::Timer ipc_construct_timer;

  SPDLOG_DEBUG("[conversion] [drone {}] Spawned.", id);

  // Prepare BatchBuilder
  ArrowBatchBuilder builder(parse_options);

  // Prepare statistics
  Stats stats;

  // Reserve a queue item
  illex::JSONQueueItem json_item;

  // Triggers for IPC construction
  size_t batches_size = 0;
  bool attempt_dequeue = true;

  // Loop until thread is stopped.
  while (!shutdown->load()) {
    // Attempt to dequeue an item if the size of the current RecordBatches is not larger
    // than the limit.
    if (attempt_dequeue
        && in->wait_dequeue_timed(json_item, std::chrono::microseconds(1))) {
      SPDLOG_DEBUG("Builder status: {}", builder.ToString());
      // There is a JSON.
      SPDLOG_DEBUG("Drone {} popping JSON: {}.", id, json_item.string);
      // Buffer the JSON.
      stats.status = builder.Buffer(json_item);
      SHUTDOWN_ON_FAILURE();

      // Check if we need to flush the JSON buffer.
      if (builder.num_buffered() >= json_threshold) {
        SPDLOG_DEBUG("Builder JSON buffer reached threshold.");
        convert_timer.Start();
        stats.num_jsons += builder.num_buffered();
        stats.status = builder.FlushBuffered();
        SHUTDOWN_ON_FAILURE();
        convert_timer.Stop();
        stats.convert_time += convert_timer.seconds();
      }

      // Check if either of the threshold was reached.
      if ((builder.size() >= batch_threshold)) {
        SPDLOG_DEBUG("Size of batches has reached threshold. "
                     "Current size: {} bytes", builder.size());
        // We reached the threshold, so in the next iteration, just send off the
        // RecordBatch.
        attempt_dequeue = false;
      }
    } else {
      // If there is nothing in the queue or if the RecordBatch size threshold has been
      // reached, construct the IPC message to have this batch sent off. When the queue
      // is empty, this causes the latency to be low. When it is full, it still provides
      // good throughput.

      SPDLOG_DEBUG("Nothing left in queue.");
      // If there is still something in the buffer, flush it.
      if (builder.num_buffered() > 0) {
        SPDLOG_DEBUG("Flushing JSON buffer.");
        convert_timer.Start();
        stats.num_jsons += builder.num_buffered();
        stats.status = builder.FlushBuffered();
        SHUTDOWN_ON_FAILURE();
        convert_timer.Stop();
        stats.convert_time += convert_timer.seconds();
      }

      // There has to be at least one batch.
      if (!builder.empty()) {
        SPDLOG_DEBUG("Flushing Batch buffer.");
        // Finish the builder, delivering an IPC item.
        ipc_construct_timer.Start();
        IpcQueueItem ipc_item;
        stats.status = builder.Finish(&ipc_item);
        SHUTDOWN_ON_FAILURE();
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

#undef SHUTDOWN_ON_FAILURE

void ConvertWithCPU(illex::JSONQueue *in,
                    IpcQueue *out,
                    std::atomic<bool> *shutdown,
                    size_t num_drones,
                    const arrow::json::ParseOptions &parse_options,
                    const arrow::json::ReadOptions &read_options,
                    size_t json_threshold,
                    size_t batch_threshold,
                    std::promise<std::vector<Stats>> &&stats) {
  // Reserve some space for the thread handles and futures.
  std::vector<std::thread> threads;
  std::vector<std::future<Stats>> futures;
  std::vector<Stats> threads_stats(num_drones);
  threads.reserve(num_drones);
  futures.reserve(num_drones);

  SPDLOG_DEBUG("Starting {} JSON conversion drones.", num_drones);
  SPDLOG_DEBUG("  with JSON threshold: {}", json_threshold);

  // Start the conversion threads.
  for (int thread = 0; thread < num_drones; thread++) {
    // Prepare the future/promise.
    std::promise<Stats> promise_stats;
    futures.push_back(promise_stats.get_future());
    threads.emplace_back(ConversionDroneThread,
                         thread,
                         shutdown,
                         std::move(promise_stats),
                         in,
                         out,
                         parse_options,
                         read_options,
                         json_threshold,
                         batch_threshold);
  }

  // Wait for the drones to get shut down by the caller of this function.
  // Also gather the statistics and see if there was any error.
  for (size_t t = 0; t < num_drones; t++) {
    if (threads[t].joinable()) {
      threads[t].join();
      threads_stats[t] = futures[t].get();
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
