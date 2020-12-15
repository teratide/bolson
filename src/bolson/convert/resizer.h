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

#include <vector>
#include <arrow/api.h>

#include "bolson/parse/parser.h"
#include "bolson/status.h"

namespace bolson::convert {

struct ResizedBatches {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

// TODO: not use number of rows but something better to resize.
class Resizer {
 public:
  explicit Resizer(size_t max_rows) : max_rows(max_rows) {}
  auto Resize(const parse::ParsedBuffer &in, ResizedBatches *out) -> Status;
 private:
  size_t max_rows;
};

}
