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

#include <CLI/CLI.hpp>
#include <memory>
#include <utility>
#include <variant>

#include "bolson/parse/parser.h"
#include "bolson/status.h"
#include "bolson/utils.h"

#define BOLSON_ARROW_DEFAULT_BUFFER_CAP (16 * 1024 * 1024)

namespace bolson::parse {

/**
 * \brief Read an Arrow schema from a file.
 * \param file Path to the file.
 * \param out  Schema output.
 * \return Status::OK() if successful, some error otherwise.
 */
auto ReadSchemaFromFile(const std::string& file, std::shared_ptr<arrow::Schema>* out)
    -> Status;

/// \brief Options for Arrow's built-in JSON parser.
struct ArrowOptions {
  /// Arrow schema.
  std::shared_ptr<arrow::Schema> schema = nullptr;
  /// Path to Arrow schema.
  std::string schema_path;
  /// Number of input buffers to use, when set to 0, it will be equal to the number of
  /// threads.
  size_t num_buffers = 0;
  /// Whether to store sequence numbers as a column.
  bool seq_column = true;

  auto ReadSchema() -> Status;
};

/// \brief Options exposed to CLI.
void AddArrowOptionsToCLI(CLI::App* sub, ArrowOptions* out);

/// \brief Parser implementation using Arrow's built-in JSON parser.
class ArrowParser : public Parser {
 public:
  explicit ArrowParser(arrow::json::ParseOptions parse_options,
                       arrow::json::ReadOptions read_options, bool seq_column)
      : parse_opts(std::move(parse_options)),
        read_opts(read_options),
        seq_column(seq_column) {}

  auto Parse(const std::vector<illex::JSONBuffer*>& buffers_in,
             std::vector<ParsedBatch>* batches_out) -> Status override;

 private:
  arrow::json::ParseOptions parse_opts;
  arrow::json::ReadOptions read_opts;
  bool seq_column;
};

/// \brief Context for Arrow parsers.
class ArrowParserContext : public ParserContext {
 public:
  static auto Make(const ArrowOptions& opts, size_t num_parsers, size_t input_size,
                   std::shared_ptr<ParserContext>* out) -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;

  [[nodiscard]] auto input_schema() const -> std::shared_ptr<arrow::Schema> override;
  [[nodiscard]] auto output_schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  std::shared_ptr<arrow::Schema> input_schema_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<std::shared_ptr<ArrowParser>> parsers_;
};

}  // namespace bolson::parse
