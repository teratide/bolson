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

namespace bolson::parse::custom {

struct TripOptions {
  /// Number of input buffers to use, when set to 0, it will be equal to the number of
  /// threads.
  size_t num_buffers = 0;
  /// Capacity of input buffers.
  size_t buf_capacity = 0;

  size_t pre_alloc_records;
  size_t pre_alloc_timestamp_values;
};

void AddTripOptionsToCLI(CLI::App* sub, TripOptions* out);

struct TripBuilder {
  explicit TripBuilder(int64_t pre_alloc_rows = 0, int64_t pre_alloc_ts_values = 0);

  auto Finish() -> std::shared_ptr<arrow::RecordBatch>;

  std::shared_ptr<arrow::StringBuilder> timestamp;
  std::shared_ptr<arrow::UInt64Builder> timezone;
  std::shared_ptr<arrow::UInt64Builder> vin;
  std::shared_ptr<arrow::UInt64Builder> odometer;
  std::shared_ptr<arrow::BooleanBuilder> hypermiling;
  std::shared_ptr<arrow::UInt64Builder> avgspeed;
  std::shared_ptr<arrow::FixedSizeListBuilder> sec_in_band;
  std::shared_ptr<arrow::FixedSizeListBuilder> miles_in_time_range;
  std::shared_ptr<arrow::FixedSizeListBuilder> const_speed_miles_in_band;
  std::shared_ptr<arrow::FixedSizeListBuilder> vary_speed_miles_in_band;
  std::shared_ptr<arrow::FixedSizeListBuilder> sec_decel;
  std::shared_ptr<arrow::FixedSizeListBuilder> sec_accel;
  std::shared_ptr<arrow::FixedSizeListBuilder> braking;
  std::shared_ptr<arrow::FixedSizeListBuilder> accel;
  std::shared_ptr<arrow::BooleanBuilder> orientation;
  std::shared_ptr<arrow::FixedSizeListBuilder> small_speed_var;
  std::shared_ptr<arrow::FixedSizeListBuilder> large_speed_var;
  std::shared_ptr<arrow::UInt64Builder> accel_decel;
  std::shared_ptr<arrow::UInt64Builder> speed_changes;
};

class TripParser : public Parser {
 public:
  TripParser(size_t pre_alloc_records, size_t pre_alloc_timestamp_values);

  auto Parse(const std::vector<illex::JSONBuffer*>& in, std::vector<ParsedBatch>* out)
      -> Status override;

  auto ParseOne(const illex::JSONBuffer* buffer, ParsedBatch* out) -> Status;

  static auto input_schema() -> std::shared_ptr<arrow::Schema>;
  [[nodiscard]] auto output_schema() const -> std::shared_ptr<arrow::Schema>;

 private:
  TripBuilder builder;
};

class TripParserContext : public ParserContext {
 public:
  static auto Make(const TripOptions& opts, size_t num_parsers, size_t input_size,
                   std::shared_ptr<ParserContext>* out) -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;

  [[nodiscard]] auto input_schema() const -> std::shared_ptr<arrow::Schema> override;
  [[nodiscard]] auto output_schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  std::vector<std::shared_ptr<TripParser>> parsers_;
};

}  // namespace bolson::parse::custom
