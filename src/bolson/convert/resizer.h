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
#include <illex/client_buffered.h>

#include "bolson/parse/parser.h"
#include "bolson/status.h"

namespace bolson::convert {

using ResizedBatches = std::vector<parse::ParsedBatch>;

/**
 * \brief Resizes RecordBatches to not exceed a specific number of rows.
 */
class Resizer {
 public:
  /**
   * \brief Resizer constructor.
   * \param max_rows The maximum number of rows a RecordBatch may contain.
   */
  explicit Resizer(size_t max_rows) : max_rows(max_rows) {}
  /**
   * \brief Resize all RecordBatches in a parsed buffer to not exceed a maximum no. rows.
   * \param in  The parsed buffer containing resulting Arrow RecordBatches.
   * \param out The resized RecordBatches.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Resize(const parse::ParsedBatch &in, ResizedBatches *out) -> Status;
 private:
  size_t max_rows;
};

}
