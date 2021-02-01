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

#include "bolson/convert/serializer.h"

namespace bolson::convert {

auto Serializer::Serialize(const ResizedBatches& in, SerializedBatches* out) -> Status {
  SerializedBatches result;

  // Set up a pointer for the combined batch.
  std::shared_ptr<arrow::RecordBatch> combined_batch;

  // Serialize each batch.
  for (const auto& batch : in) {
    auto serialize_result = arrow::ipc::SerializeRecordBatch(*batch.batch, opts);
    if (!serialize_result.ok()) {
      return Status(Error::ArrowError,
                    "Could not serialize batch: " + serialize_result.status().message());
    }
    auto serialized = serialize_result.ValueOrDie();
    if (serialized->size() > max_ipc_size) {
      return Status(Error::GenericError,
                    "Maximum IPC message size exceeded."
                    "Reduce max number of rows per batch.");
    }
    result.push_back({serialize_result.ValueOrDie(), batch.seq_range});
  }

  *out = result;

  return Status::OK();
}

auto ByteSizeOf(const SerializedBatches& batches) -> size_t {
  size_t result = 0;
  for (const auto& b : batches) {
    result += b.message->size();
  }
  return result;
}

auto RecordSizeOf(const SerializedBatch& batch) -> size_t {
  return batch.seq_range.last - batch.seq_range.first + 1;
}

auto RecordSizeOf(const SerializedBatches& batches) -> size_t {
  size_t result = 0;
  for (const auto& b : batches) {
    result += RecordSizeOf(b);
  }
  return result;
}

}  // namespace bolson::convert