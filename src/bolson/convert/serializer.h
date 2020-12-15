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

#include "bolson/status.h"
#include "bolson/convert/resizer.h"

namespace bolson::convert {

struct SerializedBatches {
  size_t total_bytes = 0;
  std::vector<std::shared_ptr<arrow::Buffer>> messages;
};

class Serializer {
 public:
  explicit Serializer(size_t max_ipc_size) : max_ipc_size(max_ipc_size) {}
  auto Serialize(const ResizedBatches &in, SerializedBatches *out) -> Status;
 private:
  arrow::ipc::IpcWriteOptions opts = arrow::ipc::IpcWriteOptions::Defaults();
  size_t max_ipc_size;
};

}