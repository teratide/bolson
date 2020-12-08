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

#pragma once

#include <arrow/api.h>
#include <illex/queue.h>
#include <illex/latency.h>

#include "bolson/log.h"
#include "bolson/status.h"
#include "bolson/pulsar.h"
#include "bolson/convert/stats.h"

/// Convert Arrow status and return on error.
#define ARROW_ROE(s) {                                                  \
  const auto& status = s;                                               \
  if (!status.ok()) return Status(Error::ArrowError, status.ToString());\
}                                                                       \
void()

namespace bolson::convert {

/// Class to support incremental building up of a RecordBatch from JSONQueueItems.
class IPCBuilder {
 public:
  /// \brief Take multiple JSONQueueItems and convert them into an Arrow RecordBatch.
  auto Buffer(const illex::JSONQueueItem &item,
              illex::LatencyTracker *lat_tracker) -> Status;

  /// \brief Flush the buffered JSONs and convert them to a single RecordBatch.
  virtual auto FlushBuffered(putong::Timer<> *t,
                             illex::LatencyTracker *lat_tracker) -> Status = 0;

  /**
   * \brief Reset this builder, clearing buffered JSONs and batches.
   *
   * The builder can be re-used afterwards.
   */
  void Reset();

  /**
   * \brief Finish the builder, resulting in an IPC queue item.
   *
   * This resets the builder, which can be reused afterwards.
   *
   * \param out         The IpcQueueItem output.
   * \param lat_tracker   The latency tracker to use to track latencies of specific JSONs.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Finish(IpcQueueItem *out, illex::LatencyTracker *lat_tracker) -> Status;

  /// \brief Return the size of the buffers kept by all RecordBatches in this builder.
  [[nodiscard]] auto size() const -> size_t { return this->size_; }

  /// \brief Return true if there are no batches in this builder.
  [[nodiscard]] auto empty() const -> bool { return this->batches.empty(); }

  /// \brief Return the number of buffered JSONs.
  [[nodiscard]] inline auto jsons_buffered() const -> size_t {
    return this->seq_builder->length();
  }

  /// \brief Return the number of buffered batches.
  [[nodiscard]] inline auto batches_buffered() const -> size_t {
    return this->batches.size();
  }

  /// \brief Access a specific batch.
  [[nodiscard]] inline auto GetBatch(size_t i) const -> std::shared_ptr<arrow::RecordBatch> {
    return this->batches[i];
  }

  /// \brief Return the number of buffered bytes.
  [[nodiscard]] inline auto bytes_buffered() const -> size_t {
    return this->str_buffer->size();
  }

  /// \brief Return the configured JSON buffering threshold.
  [[nodiscard]] inline auto json_buffer_threshold() const -> size_t {
    return this->json_buffer_threshold_;
  }

  /// \brief Return the configured batch size threshold.
  [[nodiscard]] inline auto batch_size_threshold() const -> size_t {
    return this->batch_buffer_threshold_;
  }

  /// \brief Return a string with the state of this builder.
  auto ToString() -> std::string;

 protected:
  /// Constructor
  explicit IPCBuilder(size_t json_threshold,
                      size_t batch_threshold,
                      size_t seq_buf_init_size,
                      size_t str_buf_init_size);

  /// Sequence numbers of JSONs that are in the JSON buffer.
  std::vector<illex::Seq> lat_tracked_seq_in_buffer;
  /// Sequence numbers of JSONs that are in the Batches buffer.
  std::vector<illex::Seq> lat_tracked_seq_in_batches;
  /// A vector to hold batches that collapse into a single batch when finish is called.
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  /// Size of the batches so far.
  size_t size_ = 0;
  /// Buffered queue items sequence numbers
  std::unique_ptr<arrow::UInt64Builder> seq_builder;
  /// Buffered queue items strings
  std::shared_ptr<arrow::ResizableBuffer> str_buffer;
  /// Number of bytes in the JSON buffer before it should be flushed.
  size_t json_buffer_threshold_;
  /// Number of bytes in all RecordBatches before builder should be finished.
  size_t batch_buffer_threshold_;
};

/**
 * \brief Pull JSONs from a queue and convert them to Arrow IPC messages.
 * \param id            The thread ID of this conversion thread.
 * \param builder       The builder to use to construct IPC messages.
 * \param in            The input queue containing the JSON items.
 * \param out           The output queue containing the IPC messages.
 * \param lat_tracker   The latency tracker to use to track latencies of specific JSONs.
 * \param shutdown      Whether the shut this thread down.
 * \param stats_promise Conversion statistics output.
 */
void Convert(size_t id,
             std::unique_ptr<IPCBuilder> builder,
             illex::JSONQueue *in,
             IpcQueue *out,
             illex::LatencyTracker *lat_tracker,
             std::atomic<bool> *shutdown,
             std::promise<Stats> &&stats_promise);

/// Implementations available to convert JSONs to IPC messages.
enum class Impl {
  CPU,          ///< A CPU version based on Arrow's internal JSON parser using RapidJSON.
  OPAE_BATTERY  ///< An FPGA version for only one specific schema.
};

}