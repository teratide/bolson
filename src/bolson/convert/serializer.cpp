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

auto Serializer::Serialize(const ResizedBatches &in, SerializedBatches *out) -> Status {
  SerializedBatches result;

  // Set up a pointer for the combined batch.
  std::shared_ptr<arrow::RecordBatch> combined_batch;

  // Serialize each batch.
  for (const auto &batch : in.batches) {
    auto serialized = arrow::ipc::SerializeRecordBatch(*batch, opts);
    if (!serialized.ok()) {
      return Status(Error::ArrowError,
                    "Could not serialize batch: " + serialized.status().message());
    }

    result.messages.push_back(serialized.ValueOrDie());
  }

  *out = result;

  return Status::OK();
}

}