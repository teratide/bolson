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

#include <memory>
#include <utility>

#include <arrow/api.h>
#include <arrow/json/api.h>
#include <illex/queue.h>
#include <blockingconcurrentqueue.h>

#include "bolson/convert/convert.h"
#include "bolson/status.h"
#include "bolson/stream.h"

namespace bolson::convert {

/// Class to support incremental building up of a RecordBatch from JSONQueueItems.
class BatchBuilder {
 public:
  explicit BatchBuilder(size_t seq_buf_init_size = 1024 * 1024,
                        size_t str_buf_init_size = 1024 * 1024 * 16);

  /// \brief Return the size of the buffers kept by all RecordBatches in this builder.
  [[nodiscard]] auto size() const -> size_t { return size_; }

  /// \brief Return true if there are no batches in this builder.
  [[nodiscard]] auto empty() const -> bool { return batches.empty(); }

  /// \brief Take one JSONQueueItem and convert it to an Arrow RecordBatch, and append it to this builder.
  virtual auto AppendAsBatch(const illex::JSONQueueItem &item) -> Status = 0;

  /// \brief Take multiple JSONQueueItems and convert them into an Arrow RecordBatch.
  auto Buffer(const illex::JSONQueueItem &item) -> Status;

  /// \brief Flush the buffered JSONs and convert them to a single RecordBatch.
  virtual auto FlushBuffered(putong::Timer<> *t) -> Status = 0;

  inline auto jsons_buffered() -> size_t { return seq_builder->length(); }

  inline auto bytes_buffered() -> size_t { return str_buffer->size(); }

  /// \brief Resets this builder, clearing contained batches. Can be reused afterwards.
  void Reset();

  /// \brief Finish the builder, resulting in an IPC queue item. This resets the builder, and can be reused afterwards.
  auto Finish(IpcQueueItem *out) -> Status;

  /// \brief Return a string with the state of this builder.
  auto ToString() -> std::string;

 protected:
  /// A vector to hold RecordBatches that we collapse into a single RecordBatch when we finish this builder.
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  /// Size of the batches so far.
  size_t size_ = 0;
  /// Buffered queue items sequence numbers
  std::unique_ptr<arrow::UInt64Builder> seq_builder;
  /// Buffered queue items strings
  std::shared_ptr<arrow::ResizableBuffer> str_buffer;
};

class ArrowBatchBuilder : public BatchBuilder {
 public:
  explicit ArrowBatchBuilder(arrow::json::ParseOptions parse_options,
                             size_t seq_buf_init_size = 1024 * 1024,
                             size_t str_buf_init_size = 1024 * 1024 * 16)
      : BatchBuilder(seq_buf_init_size, str_buf_init_size),
        parse_options(std::move(parse_options)) {}

  auto AppendAsBatch(const illex::JSONQueueItem &item) -> Status override;
  auto FlushBuffered(putong::Timer<> *t) -> Status override;
 private:
  /// Parsing options.
  arrow::json::ParseOptions parse_options;
};

/**
 * \brief Converts one or multiple JSONs to Arrow RecordBatches, and batches to IPC messages. Multi-threaded.
 * \param in                The input queue of JSONs
 * \param out               The output queue for Arrow IPC messages.
 * \param shutdown          Signal to shut down this thread (typically used when there will be no more new inputs).
 * \param num_drones        Number of conversion threads to spawn.
 * \param batch_threshold   Threshold batch size. If batch goes over this size, it will be converted and queued.
 * \param parse_options     The JSON parsing options for Arrow.
 * \param stats             Statistics for each conversion thread.
 */
void ConvertWithCPU(illex::JSONQueue *in,
                    IpcQueue *out,
                    std::atomic<bool> *shutdown,
                    size_t num_drones,
                    const arrow::json::ParseOptions &parse_options,
                    const arrow::json::ReadOptions &read_options,
                    size_t json_threshold,
                    size_t batch_threshold,
                    std::promise<std::vector<convert::Stats>> &&stats);

}
