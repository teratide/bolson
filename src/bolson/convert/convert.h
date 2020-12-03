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
  explicit IPCBuilder(size_t json_threshold,
                      size_t batch_threshold,
                      size_t seq_buf_init_size,
                      size_t str_buf_init_size);

  /// \brief Return the size of the buffers kept by all RecordBatches in this builder.
  [[nodiscard]] auto size() const -> size_t { return size_; }

  /// \brief Return true if there are no batches in this builder.
  [[nodiscard]] auto empty() const -> bool { return batches.empty(); }

  /// \brief Take one JSON and convert it to a RecordBatch, and append it to this builder.
  virtual auto AppendAsBatch(const illex::JSONQueueItem &item) -> Status = 0;

  /// \brief Take multiple JSONQueueItems and convert them into an Arrow RecordBatch.
  auto Buffer(const illex::JSONQueueItem &item) -> Status;

  /// \brief Flush the buffered JSONs and convert them to a single RecordBatch.
  virtual auto FlushBuffered(putong::Timer<> *t) -> Status = 0;

  /// \brief Resets this builder, clearing contained batches. Can be reused afterwards.
  void Reset();

  /**
   * \brief Finish the builder, resulting in an IPC queue item.
   * \param out The IpcQueueItem output.
   * \return This resets the builder, and can be reused afterwards.
   */
  auto Finish(IpcQueueItem *out) -> Status;

  /// \brief Return a string with the state of this builder.
  auto ToString() -> std::string;

  /// \brief Return the number of buffered JSONs.
  [[nodiscard]] inline auto jsons_buffered() const -> size_t {
    return seq_builder->length();
  }

  /// \brief Return the number of buffered batches.
  [[nodiscard]] inline auto batches_buffered() const -> size_t {
    return this->batches.size();
  }

  /// \brief Access a specific batch.
  [[nodiscard]] inline auto GetBatch(size_t i) const -> std::shared_ptr<arrow::RecordBatch> {
    return batches[i];
  }

  /// \brief Return the number of buffered bytes.
  [[nodiscard]] inline auto bytes_buffered() const -> size_t {
    return str_buffer->size();
  }

  [[nodiscard]] inline auto json_buffer_threshold() const -> size_t {
    return json_buffer_threshold_;
  }

  [[nodiscard]] inline auto batch_size_threshold() const -> size_t {
    return batch_buffer_threshold_;
  }

 protected:
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
 *
 * The Arrow IPC messages contain the JSONs parsed and deserialized as Arrow
 * RecordBatches.
 *
 * This function is meant to run multi-threaded.
 *
 * \param id            Thread id for this function.
 * \param builder       The Builder to use for conversion.
 * \param in            The input queue.
 * \param out           The output queue.
 * \param shutdown      Shutdown signal for this thread.
 * \param stats_promise Statistics output.
 */
void Convert(size_t id,
             std::unique_ptr<IPCBuilder> builder,
             illex::JSONQueue *in,
             IpcQueue *out,
             std::atomic<bool> *shutdown,
             std::promise<Stats> &&stats_promise);

/// Implementations available to convert JSONs to IPC messages.
enum class Impl {
  CPU,          ///< A CPU version based on Arrow's internal JSON parser using RapidJSON.
  OPAE_BATTERY  ///< An FPGA version for only one specific schema.
};

}