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
#include <illex/latency.h>
#include <illex/client_queued.h>
#include <illex/client_buffered.h>

#include "bolson/log.h"
#include "bolson/status.h"
#include "bolson/pulsar.h"
#include "bolson/convert/stats.h"

namespace bolson::convert {

class BufferedIPCBuilder {
 public:
  virtual auto ConvertBuffer(illex::RawJSONBuffer *in) -> Status = 0;

  auto Flush(IpcQueueItem *out,
             putong::Timer<> *comb,
             putong::Timer<> *ipc,
             illex::LatencyTracker *lat_tracker) -> Status;

  void Reset();

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

  /// \brief Return the configured batch size threshold.
  [[nodiscard]] inline auto batch_size_threshold() const -> size_t {
    return this->batch_buffer_threshold_;
  }

 protected:
  BufferedIPCBuilder(size_t batch_threshold, size_t seq_buf_init_size);

  /// Sequence numbers of latency-tracked JSONs that are in the Batches buffer.
  std::vector<illex::Seq> lat_tracked_seq_in_batches;
  /// A vector to hold batches that collapse into a single batch when finish is called.
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  /// Size of the batches so far.
  size_t size_ = 0;
  /// Buffered queue items sequence numbers
  std::unique_ptr<arrow::UInt64Builder> seq_builder;
  /// Number of bytes in all RecordBatches before builder should be finished.
  size_t batch_buffer_threshold_;
};

void ConvertFromBuffers(size_t id,
                        std::unique_ptr<BufferedIPCBuilder> builder,
                        const std::vector<illex::RawJSONBuffer *> &buffers,
                        const std::vector<std::mutex *> &mutexes,
                        IpcQueue *out,
                        illex::LatencyTracker *lat_tracker,
                        std::atomic<bool> *shutdown,
                        std::promise<Stats> &&stats_promise);

}