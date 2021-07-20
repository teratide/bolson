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

#include "bolson/parse/custom/battery.h"

#include <arrow/api.h>

#include <CLI/CLI.hpp>
#include <chrono>
#include <thread>

#include "bolson/latency.h"
#include "bolson/log.h"
#include "bolson/parse/custom/common.h"
#include "bolson/parse/parser.h"

namespace bolson::parse::custom {

// assume ndjson
static inline auto ParseBatteryNDJSONs(const char* data, size_t size,
                                       arrow::ListBuilder* list_bld,
                                       arrow::UInt64Builder* values_bld) -> Status {
  auto error = Status(Error::GenericError, "Unable to parse JSON data.");

  const auto* pos = data;
  const auto* end = data + size;
  // Eat any initial whitespace.
  pos = EatWhitespace(pos, end);

  // Start parsing objects
  while ((pos < end) && (pos != nullptr)) {
    pos = EatObjectStart(pos, end);  // {
    pos = EatWhitespace(pos, end);
    pos = EatMemberKey(pos, end, "voltage");  // "voltage"
    pos = EatWhitespace(pos, end);
    pos = EatMemberKeyValueSeperator(pos, end);  // :
    pos = EatWhitespace(pos, end);
    pos = EatUInt64Array(pos, end, list_bld, values_bld);  // e.g. [1,2,3]
    pos = EatWhitespace(pos, end);
    pos = EatObjectEnd(pos, end);  // }
    pos = EatWhitespace(pos, end);
    pos = EatChar(pos, end, '\n');

    // The newline may be the last byte, check if we didn't reach end of input before
    // continuing.
    if ((pos < end) && (pos != nullptr)) {
      pos = EatWhitespace(pos, end);
    }
  }

  return Status::OK();
}

// assume ndjson
static inline auto UnsafeParseBatteryNDJSONs(const char* data, size_t size,
                                             arrow::ListBuilder* list_bld,
                                             arrow::UInt64Builder* values_bld) -> Status {
  auto error = Status(Error::GenericError, "Unable to parse JSON data.");

  const auto* pos = data;
  const auto* end = data + size;
  // Eat any initial whitespace.
  pos = EatWhitespace(pos, end);

  // Start parsing objects
  while ((pos < end) && (pos != nullptr)) {
    pos = EatObjectStart(pos, end);  // {
    pos = EatWhitespace(pos, end);
    pos = EatMemberKey(pos, end, "voltage");  // "voltage"
    pos = EatWhitespace(pos, end);
    pos = EatMemberKeyValueSeperator(pos, end);  // :
    pos = EatWhitespace(pos, end);
    pos = EatUInt64ArrayUnsafe(pos, end, list_bld, values_bld);  // e.g. [1,2,3]
    pos = EatWhitespace(pos, end);
    pos = EatObjectEnd(pos, end);  // }
    pos = EatWhitespace(pos, end);
    pos = EatChar(pos, end, '\n');

    // The newline may be the last byte, check if we didn't reach end of input before
    // continuing.
    if ((pos < end) && (pos != nullptr)) {
      pos = EatWhitespace(pos, end);
    }
  }

  return Status::OK();
}

auto UnsafeBatteryParser::ParseOne(const illex::JSONBuffer* buffer, ParsedBatch* out)
    -> Status {
  auto values_builder = std::make_shared<arrow::UInt64Builder>();
  auto voltage_builder =
      std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), values_builder);
  values_builder->Reserve(pre_alloc_values);
  voltage_builder->Reserve(pre_alloc_offsets);

  BOLSON_ROE(UnsafeParseBatteryNDJSONs(reinterpret_cast<const char*>(buffer->data()),
                                       buffer->size(), voltage_builder.get(),
                                       values_builder.get()));

  std::shared_ptr<arrow::ListArray> voltage;
  ARROW_ROE(voltage_builder->Finish(&voltage));

  out->seq_range = buffer->range();
  out->batch = arrow::RecordBatch::Make(output_schema(), voltage->length(), {voltage});

  return Status::OK();
}

auto BatteryParser::ParseOne(const illex::JSONBuffer* buffer, ParsedBatch* out)
    -> Status {
  auto values_builder = std::make_shared<arrow::UInt64Builder>();
  auto voltage_builder =
      std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), values_builder);

  // Reserve expected number of JSONs in case a seq. no. column is desired.
  arrow::UInt64Builder seq_bld;
  uint64_t seq = buffer->range().first;
  if (seq_column) {
    ARROW_ROE(seq_bld.Reserve(buffer->range().last - buffer->range().first + 1));
  }

  BOLSON_ROE(ParseBatteryNDJSONs(reinterpret_cast<const char*>(buffer->data()),
                                 buffer->size(), voltage_builder.get(),
                                 values_builder.get()));

  std::shared_ptr<arrow::ListArray> voltage;
  ARROW_ROE(voltage_builder->Finish(&voltage));

  out->seq_range = buffer->range();
  out->batch = arrow::RecordBatch::Make(output_schema(), voltage->length(), {voltage});

  return Status::OK();
}

auto BatteryParser::Parse(const std::vector<illex::JSONBuffer*>& in,
                          std::vector<ParsedBatch>* out) -> Status {
  for (auto* buf : in) {
    ParsedBatch batch;
    BOLSON_ROE(this->ParseOne(buf, &batch));
    out->push_back(batch);
  }

  return Status::OK();
}

static auto voltage_type() -> std::shared_ptr<arrow::DataType> {
  static auto result = arrow::list(arrow::field("item", arrow::uint64(), false));
  return result;
}

auto BatteryParser::input_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = arrow::schema({arrow::field("voltage", voltage_type(), false)});
  return result;
}
auto BatteryParser::output_schema() const -> std::shared_ptr<arrow::Schema> {
  return output_schema_;
}

BatteryParser::BatteryParser(bool seq_column) : seq_column(seq_column) {
  if (seq_column) {
    WithSeqField(*input_schema(), &output_schema_);
  } else {
    output_schema_ = input_schema();
  }
}

UnsafeBatteryParser::UnsafeBatteryParser(bool seq_column, size_t pre_alloc_offsets = 0,
                                         size_t pre_alloc_values = 0)
    : BatteryParser(seq_column),
      pre_alloc_offsets(pre_alloc_offsets),
      pre_alloc_values(pre_alloc_values) {
  if (seq_column) {
    WithSeqField(*input_schema(), &output_schema_);
  } else {
    output_schema_ = input_schema();
  }
}

auto BatteryParserContext::Make(const BatteryOptions& opts, size_t num_parsers,
                                size_t input_size, std::shared_ptr<ParserContext>* out)
    -> Status {
  auto result = std::make_shared<BatteryParserContext>();

  // Use default allocator.
  result->allocator_ = std::make_shared<buffer::Allocator>();

  // Initialize all parsers.
  if (opts.pre_alloc_offsets + opts.pre_alloc_values > 0) {
    result->parsers_ = std::vector<std::shared_ptr<BatteryParser>>(
        num_parsers, std::make_shared<UnsafeBatteryParser>(
                         opts.seq_column, opts.pre_alloc_offsets, opts.pre_alloc_values));
  } else {
    for (size_t i = 0; i < num_parsers; i++) {
      result->parsers_.push_back(std::make_shared<BatteryParser>(opts.seq_column));
    }
  }

  // Allocate buffers. Use number of parsers if number of buffers is 0 in options.
  auto num_buffers = opts.num_buffers == 0 ? num_parsers : opts.num_buffers;
  BOLSON_ROE(result->AllocateBuffers(num_buffers, DivideCeil(input_size, num_buffers)));

  *out = std::static_pointer_cast<ParserContext>(result);

  return Status::OK();
}

auto BatteryParserContext::parsers() -> std::vector<std::shared_ptr<Parser>> {
  return CastPtrs<Parser>(parsers_);
}
auto BatteryParserContext::input_schema() const -> std::shared_ptr<arrow::Schema> {
  return BatteryParser::input_schema();
}

auto BatteryParserContext::output_schema() const -> std::shared_ptr<arrow::Schema> {
  return parsers_.front()->output_schema();
}

void AddBatteryOptionsToCLI(CLI::App* sub, BatteryOptions* out) {
  sub->add_flag("--custom-battery-seq-col", out->seq_column,
                "Custom battery parser, retain ordering information by adding a sequence "
                "number column.")
      ->default_val(false);
  sub->add_option("--custom-battery-pre-alloc-offsets", out->pre_alloc_offsets,
                  "Pre-allocate this many offsets when this value is > 0. Enables unsafe "
                  "behavior.")
      ->default_val(0);
  sub->add_option(
         "--custom-battery-pre-alloc-values", out->pre_alloc_values,
         "Pre-allocate this many values when this value is > 0. Enables unsafe behavior.")
      ->default_val(0);
}

}  // namespace bolson::parse::custom
