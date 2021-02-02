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

struct SerializedBatch {
  std::shared_ptr<arrow::Buffer> message;
  illex::SeqRange seq_range;
  TimePoints time_points;
};

using SerializedBatches = std::vector<SerializedBatch>;

auto RecordSizeOf(const SerializedBatch& batch) -> size_t;

auto RecordSizeOf(const SerializedBatches& batches) -> size_t;

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
  auto Serialize(const ResizedBatches& in, SerializedBatches* out) -> Status;

 private:
  arrow::ipc::IpcWriteOptions opts = arrow::ipc::IpcWriteOptions::Defaults();
  size_t max_ipc_size;
};

}  // namespace bolson::convert