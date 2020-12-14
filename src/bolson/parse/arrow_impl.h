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
#include <blockingconcurrentqueue.h>

#include "bolson/parse/parser.h"
#include "bolson/status.h"
#include "bolson/stream.h"

namespace bolson::parse {

class ArrowBufferParser : public Parser {
 public:
  explicit ArrowBufferParser(arrow::json::ParseOptions parse_options,
                             const arrow::json::ReadOptions &read_options)
      : parse_options(std::move(parse_options)), read_options(read_options) {}

  auto Parse(illex::RawJSONBuffer *in, ParsedBuffer *out) -> Status override;
 private:
  /// Arrow JSON parser parse options.
  arrow::json::ParseOptions parse_options;
  /// Arrow JSON parser read options.
  arrow::json::ReadOptions read_options;
};

}
