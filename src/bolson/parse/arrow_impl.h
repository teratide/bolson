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
#include <arrow/json/api.h>
#include <blockingconcurrentqueue.h>

#include <memory>
#include <utility>

#include "bolson/parse/parser.h"
#include "bolson/status.h"

namespace bolson::parse {

/**
 * \brief Options for Arrow's built-in JSON parser.
 */
struct ArrowOptions {
  arrow::json::ParseOptions parse;
  arrow::json::ReadOptions read;
};

/**
 * \brief Parser implementation using Arrow's built-in JSON parser.
 */
class ArrowParser : public Parser {
 public:
  explicit ArrowParser(ArrowOptions opts) : opts(std::move(opts)) {}
  auto Parse(illex::JSONBuffer* in, ParsedBatch* out) -> Status override;

 private:
  ArrowOptions opts;
};

}  // namespace bolson::parse
