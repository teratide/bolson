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
#include <fletcher/api.h>

#include <CLI/CLI.hpp>
#include <memory>
#include <utility>

#include "bolson/parse/parser.h"
#include "bolson/utils.h"

#define BOLSON_CUSTOM_BATTERY_DEFAULT_BUFFER_CAP (16 * 1024 * 1024)

namespace bolson::parse::custom {

struct BatteryOptions {
  /// Number of input buffers to use, when set to 0, it will be equal to the number of
  /// threads.
  size_t num_buffers = 0;
  /// Whether to store sequence numbers as a column.
  bool seq_column = true;
  /// Capacity of input buffers.
  size_t buf_capacity = BOLSON_CUSTOM_BATTERY_DEFAULT_BUFFER_CAP;
};

void AddBatteryOptionsToCLI(CLI::App* sub, BatteryOptions* out);

class BatteryParser : public Parser {
 public:
  explicit BatteryParser(bool seq_column) : seq_column(seq_column) {}

  auto Parse(const std::vector<illex::JSONBuffer*>& in, std::vector<ParsedBatch>* out)
      -> Status override;

  auto ParseOne(const illex::JSONBuffer* buffer, ParsedBatch* out) -> Status;

  static auto input_schema() -> std::shared_ptr<arrow::Schema>;
  static auto output_schema() -> std::shared_ptr<arrow::Schema>;

 private:
  bool seq_column = false;
};

class BatteryParserContext : public ParserContext {
 public:
  static auto Make(const BatteryOptions& opts, size_t num_parsers,
                   std::shared_ptr<ParserContext>* out) -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;

  [[nodiscard]] auto input_schema() const -> std::shared_ptr<arrow::Schema> override;
  [[nodiscard]] auto output_schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  std::vector<std::shared_ptr<BatteryParser>> parsers_;
};

}  // namespace bolson::parse::custom
