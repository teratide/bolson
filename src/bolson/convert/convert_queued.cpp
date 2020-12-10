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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <illex/latency.h>

#include "bolson/log.h"
#include "bolson/latency.h"
#include "bolson/convert/convert_queued.h"

namespace bolson::convert {

QueuedIPCBuilder::QueuedIPCBuilder(size_t json_threshold,
                                   size_t batch_threshold,
                                   size_t seq_buf_init_size,
                                   size_t str_buf_init_size)
    : json_buffer_threshold_(json_threshold),
      batch_buffer_threshold_(batch_threshold) {
  str_buffer = std::shared_ptr(std::move(arrow::AllocateResizableBuffer(0).ValueOrDie()));

  auto status = str_buffer->Reserve(str_buf_init_size);
  // TODO: make a static function that returns status to construct ipc builder
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }

  std::unique_ptr<arrow::ArrayBuilder> sb;
  status = arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint64(), &sb);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }

  seq_builder =
      std::move(std::unique_ptr<arrow::UInt64Builder>(dynamic_cast<arrow::UInt64Builder *>(sb.release())));
}

auto QueuedIPCBuilder::Buffer(const illex::JSONQueueItem &item,
                              illex::LatencyTracker *lat_tracker) -> Status {
  // Keep track of which items are buffered that we also want to track latency for.
  // Anything that is buffered is also batched, so we push it to both buffers.
  if (lat_tracker->Put(item.seq, BOLSON_LAT_BUFFER_ENTRY, illex::Timer::now())) {
    this->lat_tracked_seq_in_buffer.push_back(item.seq);
    this->lat_tracked_seq_in_batches.push_back(item.seq);
  }

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
  ARROW_ROE(this->seq_builder->Append(item.seq));

  return Status::OK();
}

void QueuedIPCBuilder::Reset() {
  this->lat_tracked_seq_in_batches.clear();
  this->lat_tracked_seq_in_buffer.clear();
  this->batches.clear();
  this->size_ = 0;
}

auto QueuedIPCBuilder::Finish(IpcQueueItem *out,
                              putong::Timer<> *comb,
                              putong::Timer<> *ipc,
                              illex::LatencyTracker *lat_tracker) -> Status {
  // Set up a pointer for the combined batch.
  std::shared_ptr<arrow::RecordBatch> combined_batch;

  // Combine tables into one.
  // TODO: It would be nice if we could serialize a Table into an IPC message directly.

  comb->Start(); // Measure combination time.
  // Only wrap a table and combine chunks if there is more than one RecordBatch.
  if (this->batches.size() == 1) {
    combined_batch = batches[0];
  } else {
    auto packed_table = arrow::Table::FromRecordBatches(this->batches);
    if (!packed_table.ok()) {
      return Status(Error::ArrowError,
                    "Could not create table: " + packed_table.status().message());
    }
    auto combined_table = packed_table.ValueOrDie()->CombineChunks();
    if (!combined_table.ok()) {
      return Status(Error::ArrowError,
                    "Could not combine chunks: " + combined_table.status().message());
    }
    auto table_reader = arrow::TableBatchReader(*combined_table.ValueOrDie());
    auto combined_batch_r = table_reader.Next();
    if (!combined_batch_r.ok()) {
      return Status(Error::ArrowError,
                    "Could not read first chunk with combined batch: "
                        + combined_batch_r.status().message());
    }
    combined_batch = combined_batch_r.ValueOrDie();
  }
  comb->Stop();

  // Mark time point batches are combined.
  for (const auto &s : this->lat_tracked_seq_in_batches) {
    lat_tracker->Put(s, BOLSON_LAT_BATCH_COMBINED, illex::Timer::now());
  }

  // Serialize combined batch.
  ipc->Start(); // Measure serialize time.
  auto serialized = arrow::ipc::SerializeRecordBatch(*combined_batch,
                                                     arrow::ipc::IpcWriteOptions::Defaults());
  if (!serialized.ok()) {
    return Status(Error::ArrowError,
                  "Could not serialize batch: " + serialized.status().message());
  }

  auto ipc_item = IpcQueueItem{static_cast<size_t>(combined_batch->num_rows()),
                               serialized.ValueOrDie(),
                               std::make_shared<std::vector<illex::Seq>>(
                                   lat_tracked_seq_in_batches)};
  ipc->Stop();

  // Mark time point batch is serialized
  for (const auto &s : this->lat_tracked_seq_in_batches) {
    lat_tracker->Put(s, BOLSON_LAT_BATCH_SERIALIZED, illex::Timer::now());
  }

  this->Reset();
  *out = ipc_item;

  return Status::OK();
}

auto QueuedIPCBuilder::ToString() -> std::string {
  std::stringstream o;
  o << "Batches: " << this->batches.size() << " | " << this->size() << " B, ";
  o << "JSONs: " << this->jsons_buffered() << " | " << this->str_buffer->size() << " B";
  return o.str();
}

#define SHUTDOWN_ON_FAILURE() \
if (!stats.status.ok()) { \
  t.thread.Stop(); \
  stats.t.thread = t.thread.seconds(); \
  stats_promise.set_value(stats); \
  SPDLOG_DEBUG("Drone {} Terminating with errors.", id); \
  shutdown->store(true); \
  return; \
} void()

void ConvertFromQueue(size_t id,
                      std::unique_ptr<QueuedIPCBuilder> builder,
                      illex::JSONQueue *in,
                      IpcQueue *out,
                      illex::LatencyTracker *lat_tracker,
                      std::atomic<bool> *shutdown,
                      std::promise<Stats> &&stats_promise) {

  ConversionTimers t;
  SPDLOG_DEBUG("[conversion] [drone {}] Spawned.", id);

  // Prepare statistics
  Stats stats;

  // Reserve a queue item
  illex::JSONQueueItem json_item;

  // Triggers for IPC construction
  bool attempt_dequeue = true;

  // Loop until thread is stopped.
  while (!shutdown->load()) {
    // Attempt to dequeue an item if the size of the current RecordBatches is not larger
    // than the limit.
    if (attempt_dequeue &&
        in->wait_dequeue_timed(json_item,
                               std::chrono::microseconds(BOLSON_QUEUE_WAIT_US))) {
      // Buffer the JSON.
      stats.status = builder->Buffer(json_item, lat_tracker);
      SHUTDOWN_ON_FAILURE();

      // Check if we need to flush the JSON buffer.
      if (builder->jsons_buffered() >= builder->json_buffer_threshold()) {
        // Add counts to stats.
        stats.num_jsons += builder->jsons_buffered();
        stats.num_json_bytes += builder->bytes_buffered();
        // Flush the buffer
        stats.status = builder->FlushBuffered(&t.parse, &t.seq, lat_tracker);
        SHUTDOWN_ON_FAILURE();
        // Add time to stats.
        stats.t.parse += t.parse.seconds();
        stats.t.seq += t.seq.seconds();
      }

      // Check if either of the threshold was reached.
      if ((builder->size() >= builder->batch_size_threshold())) {
        // We reached the threshold, so in the next iteration, just send off the
        // RecordBatch.
        attempt_dequeue = false;
      }
    } else {
      // If there is nothing in the queue or if the RecordBatch size threshold has been
      // reached, construct the IPC message to have this batch sent off. When the queue
      // is empty, this causes the latency to be low. When it is full, it still provides
      // good throughput.

      // If there is still something in the JSON buffer, flush it.
      if (builder->jsons_buffered() > 0) {
        // Add counts to stats.
        stats.num_jsons += builder->jsons_buffered();
        stats.num_json_bytes += builder->bytes_buffered();
        // Flush the buffer
        stats.status = builder->FlushBuffered(&t.parse, &t.seq, lat_tracker);
        SHUTDOWN_ON_FAILURE();
        // Add time to stats.
        stats.t.parse += t.parse.seconds();
        stats.t.seq += t.seq.seconds();
      }

      // There has to be at least one batch.
      if (!builder->empty()) {
        // Finish the builder, delivering an IPC item.
        IpcQueueItem ipc_item;
        stats.status = builder->Finish(&ipc_item, &t.combine, &t.serialize, lat_tracker);
        SHUTDOWN_ON_FAILURE();

        // Enqueue the IPC item.
        out->enqueue(ipc_item);

        // Attempt to dequeue again.
        attempt_dequeue = true;

        // Update stats.
        stats.t.combine += t.combine.seconds();
        stats.t.serialize += t.serialize.seconds();
        stats.total_ipc_bytes += ipc_item.ipc->size();
        stats.num_ipc++;
      }
    }
  }
  t.thread.Stop();
  stats.t.thread = t.thread.seconds();
  stats_promise.set_value(stats);
  SPDLOG_DEBUG("Drone {} Terminating.", id);
}
#undef SHUTDOWN_ON_FAILURE

auto ToString(const Impl &impl) -> std::string {
  switch (impl) {
    case Impl::CPU: return "CPU";
    case Impl::OPAE_BATTERY: return "OPAE Battery";
  }
  throw std::runtime_error("Corrupt impl.");
}

}
