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

#include <arrow/ipc/api.h>

#include "bolson/convert/resizer.h"
#include "bolson/status.h"

namespace bolson::convert {

/// A serialized RecordBatch.
struct SerializedBatch {
  /// The serialized batch.
  std::shared_ptr<arrow::Buffer> message = nullptr;
  /// The range of sequence numbers it contains.
  illex::SeqRange seq_range = {0, 0};
  /// When the batch was where in the pipeline.
  TimePoints time_points;
};

/// \brief Returns true if lhs batch has lower first index than rhs batch.
auto operator<(const SerializedBatch& a, const SerializedBatch& b) -> bool;

/// Batches that were serialized to an Arrow IPC message.
using SerializedBatches = std::vector<SerializedBatch>;

/// Return the number of records in a serialized batch.
auto RecordSizeOf(const SerializedBatch& batch) -> size_t;

/// Return the number of bytes in multiple serialized batches.
auto ByteSizeOf(const SerializedBatches& batches) -> size_t;

/**
 * \brief Class used to serialize a batch of Arrow RecordBatches into Arrow IPC messages.
 */
class Serializer {
 public:
  /**
   * \brief Serializer constructor.
   * \param max_ipc_size Maximum size of Arrow IPC messages.
   */
  explicit Serializer(size_t max_ipc_size) : max_ipc_size(max_ipc_size) {}
  /**
   * \brief Serialize RecordBatches.
   *
   * If the serialized RecordBatch exceeds max_ipc_size in bytes, this function returns
   * an error.
   *
   * \param in  The RecordBatches to be resized.
   * \param out The serialized RecordBatches.
   * \return Status::OK() if successful, some error otherwise.
   */
  virtual auto Serialize(const ResizedBatches& in, SerializedBatches* out) const
      -> Status;

 private:
  /// Options for Arrow's IPC writer.
  arrow::ipc::IpcWriteOptions opts = arrow::ipc::IpcWriteOptions::Defaults();

  /// Maximum IPC size. Serialize() will return an Error if this is exceeded.
  size_t max_ipc_size;
};

/// \brief A serializer that doesn't do anything, for benchmarking purposes.
class SerializerMock : public Serializer {
 public:
  SerializerMock() : Serializer(0) {}
  auto Serialize(const ResizedBatches& in, SerializedBatches* out) const
      -> Status override;
};

}  // namespace bolson::convert