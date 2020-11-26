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
  explicit BatchBuilder(arrow::json::ParseOptions parse_options)
      : parse_options(std::move(parse_options)) {}

  /// \brief Return the size of the buffers kept by all RecordBatches in this builder.
  [[nodiscard]] auto size() const -> size_t { return size_; }

  /// \brief Return true if there are no batches in this builder.
  [[nodiscard]] auto empty() const -> bool { return batches.empty(); }

  /// \brief Take a JSONQueueItem and convert it to an Arrow RecordBatch, and append it to this builder.
  auto Append(const illex::JSONQueueItem &item) -> Status;

  /// \brief Resets this builder, clearing contained batches. Can be reused afterwards.
  void Reset();

  /// \brief Finish the builder, resulting in an IPC queue item. This resets the builder, and can be reused afterwards.
  auto Finish(IpcQueueItem *out) -> Status;

 private:
  /// Parsing options.
  arrow::json::ParseOptions parse_options;
  /// A vector to hold RecordBatches that we collapse into a single RecordBatch when we finish this builder.
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  /// Size of the batches so far.
  size_t size_ = 0;
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
                    size_t batch_threshold,
                    std::promise<std::vector<convert::Stats>> &&stats);

}
