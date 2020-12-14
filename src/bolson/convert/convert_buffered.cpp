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

#include <memory>
#include <vector>
#include <mutex>
#include <string_view>
#include <arrow/ipc/api.h>

#include "bolson/convert/convert_buffered.h"
#include "bolson/convert/stats.h"
#include "bolson/latency.h"
#include "bolson/status.h"

namespace bolson::convert {

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

auto TryGetFilledBuffer(const std::vector<illex::RawJSONBuffer *> &buffers,
                        const std::vector<std::mutex *> &mutexes,
                        illex::RawJSONBuffer **out,
                        size_t *lock_idx) -> bool {
  const size_t num_buffers = buffers.size();
  for (size_t i = 0; i < num_buffers; i++) {
    if (!buffers[i]->empty()) {
      if (mutexes[i]->try_lock()) {
        *lock_idx = i;
        *out = buffers[i];
        return true;
      }
    }
  }
  *out = nullptr;
  return false;
}

#define SHUTDOWN_ON_FAILURE() \
if (!stats.status.ok()) { \
  t.thread.Stop(); \
  stats.t.thread = t.thread.seconds(); \
  stats_promise.set_value(stats); \
  SPDLOG_DEBUG("Drone {} Terminating with error: {}", id, stats.status.msg()); \
  shutdown->store(true); \
  return; \
} void()

void ConvertFromBuffers(size_t id,
                        std::unique_ptr<BufferedIPCBuilder> builder,
                        const std::vector<illex::RawJSONBuffer *> &buffers,
                        const std::vector<std::mutex *> &mutexes,
                        IpcQueue *out,
                        illex::LatencyTracker *lat_tracker,
                        std::atomic<bool> *shutdown,
                        std::promise<Stats> &&stats_promise) {
  ConversionTimers t;
  SPDLOG_DEBUG("Convert thread {} spawned.", id);
  bool try_buffers = true;
  Stats stats;

  while (!shutdown->load()) {
    if (try_buffers) {
      illex::RawJSONBuffer *buf = nullptr;
      size_t lock_idx = 0;
      if (TryGetFilledBuffer(buffers, mutexes, &buf, &lock_idx)) {
        //SPDLOG_DEBUG("Attempting to convert buffer: {}",
        //             std::string_view((char *) buf->data(), buf->size()));
        // Add sizes stats before buffer is converted and reset.
        stats.num_jsons += buf->num_jsons();
        stats.num_json_bytes += buf->size();
        // Convert the buffer.
        t.parse.Start();
        stats.status = builder->ConvertBuffer(buf);
        t.parse.Stop();
        // Add parse time to stats.
        stats.t.parse += t.parse.seconds();

        SHUTDOWN_ON_FAILURE();
        mutexes[lock_idx]->unlock();
        if ((builder->size() >= builder->batch_size_threshold())) {
          try_buffers = false;
        }
      } else {
        try_buffers = false;
      }
    } else {
      // There has to be at least one batch.
      if (!builder->empty()) {
        // Finish the builder, delivering an IPC item.
        IpcQueueItem ipc_item;
        stats.status = builder->Flush(&ipc_item, &t.combine, &t.serialize, lat_tracker);
        SHUTDOWN_ON_FAILURE();

        // Enqueue the IPC item.
        out->enqueue(ipc_item);

        // Attempt to get buffer lock again.
        try_buffers = true;

        // Update stats.
        stats.t.combine += t.combine.seconds();
        stats.t.serialize += t.serialize.seconds();
        stats.total_ipc_bytes += ipc_item.ipc->size();
        stats.num_ipc++;
      } else {
        try_buffers = true;
        std::this_thread::sleep_for(std::chrono::microseconds(BOLSON_QUEUE_WAIT_US));
      }
    }
  }
  t.thread.Stop();
  stats.t.thread = t.thread.seconds();
  stats_promise.set_value(stats);
  SPDLOG_DEBUG("Drone {} Terminating.", id);
}
#undef SHUTDOWN_ON_FAILURE

auto BufferedIPCBuilder::Flush(IpcQueueItem *out,
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

  // Append sequence numbers.
  std::shared_ptr<arrow::Array> seq_no;
  ARROW_ROE(seq_builder->Finish(&seq_no));

  auto batch_seq = combined_batch->AddColumn(0, SeqField(), seq_no);
  if (!batch_seq.ok()) {
    return Status(Error::ArrowError,
                  "Couldn't add seq. column (" + std::to_string(seq_no->length())
                      + ") to batch (" + std::to_string(combined_batch->num_rows())
                      + "): " + batch_seq.status().message());
  }
  comb->Stop();

  // Mark time point batches are combined.
  for (const auto &s : this->lat_tracked_seq_in_batches) {
    lat_tracker->Put(s, BOLSON_LAT_BATCH_COMBINED, illex::Timer::now());
  }

  // Serialize combined batch.
  ipc->Start(); // Measure serialize time.
  auto serialized = arrow::ipc::SerializeRecordBatch(*batch_seq.ValueOrDie(),
                                                     arrow::ipc::IpcWriteOptions::Defaults());
  if (!serialized.ok()) {
    return Status(Error::ArrowError,
                  "Could not serialize batch: " + serialized.status().message());
  }

  *out = IpcQueueItem{static_cast<size_t>(combined_batch->num_rows()),
                      serialized.ValueOrDie(),
                      std::make_shared<std::vector<illex::Seq>>(
                          lat_tracked_seq_in_batches)};
  ipc->Stop();

  // Mark time point batch is serialized
  for (const auto &s : this->lat_tracked_seq_in_batches) {
    lat_tracker->Put(s, BOLSON_LAT_BATCH_SERIALIZED, illex::Timer::now());
  }

  // Reset this builder.
  this->Reset();

  return Status::OK();
}

void BufferedIPCBuilder::Reset() {
  this->seq_builder->Reset();
  this->lat_tracked_seq_in_batches.clear();
  this->batches.clear();
  this->size_ = 0;
}

BufferedIPCBuilder::BufferedIPCBuilder(size_t batch_threshold, size_t seq_buf_init_size)
    : batch_buffer_threshold_(batch_threshold) {

  std::unique_ptr<arrow::ArrayBuilder> sb;
  auto status =
      arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint64(), &sb);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }

  seq_builder =
      std::move(std::unique_ptr<arrow::UInt64Builder>(dynamic_cast<arrow::UInt64Builder *>(sb.release())));
}

}
