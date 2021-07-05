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
#include <charconv>
#include <chrono>
#include <thread>
#include <utility>

#include "bolson/latency.h"
#include "bolson/log.h"
#include "bolson/parse/parser.h"

namespace bolson::parse::custom {

static inline auto SkipWhitespace(const char* pos, const char* end) -> const char* {
  // Whitespace includes: space, line feed, carriage return, character tabulation
  // const char ws[] = {' ', '\t', '\n', '\r'};
  const char* result = pos;
  while (((*result == ' ') || (*result == '\t')) && (result < end)) {
    result++;
  }
  if (result == end) {
    return nullptr;
  }
  return result;
}

// assume ndjson
static inline auto ParseBatteryNDJSONs(const char* data, size_t size,
                                       arrow::ListBuilder* list_bld,
                                       arrow::UInt64Builder* values_bld) -> Status {
  auto error = Status(Error::GenericError, "Unable to parse JSON data.");
  const auto max_uint64_len =
      std::to_string(std::numeric_limits<uint64_t>::max()).length();
  const auto* pos = data;
  const auto* end = data + size;

  while (pos < end) {
    // Scan for object start
    pos = SkipWhitespace(pos, end);
    if (*pos != '{') {
      spdlog::error("Expected '{', encountered '{}'", *pos);
      return error;
    }
    pos++;

    // Scan for voltage key
    const char* voltage_key = "\"voltage\"";
    const size_t voltage_key_len = std::strlen(voltage_key);
    pos = SkipWhitespace(pos, end);
    if (std::memcmp(pos, voltage_key, voltage_key_len) != 0) {
      spdlog::error("Expected \"voltage\", encountered {}",
                    std::string_view(pos, voltage_key_len));
      return error;
    }
    pos += voltage_key_len;

    // Scan for key-value separator
    pos = SkipWhitespace(pos, end);
    if (*pos != ':') {
      spdlog::error("Expected ':', encountered '{}'", *pos);
      return error;
    }
    pos++;

    // Scan for array start.
    pos = SkipWhitespace(pos, end);
    if (*pos != '[') {
      spdlog::error("Expected '[', encountered '{}'", *pos);
      return error;
    }
    pos++;

    // Start new list.
    ARROW_ROE(list_bld->Append());

    // Scan values
    while (true) {
      uint64_t val = 0;
      pos = SkipWhitespace(pos, end);
      if (pos > end) {
        throw std::runtime_error(
            "Unexpected end of JSON data while parsing array values..");
      } else if (*pos == ']') {  // Check array end
        pos++;
        break;
      } else {  // Parse values
        auto val_result =
            std::from_chars<uint64_t>(pos, std::min(pos + max_uint64_len, end), val);
        switch (val_result.ec) {
          default:
            break;
          case std::errc::invalid_argument:
            spdlog::error("Battery voltage values contained invalid value: {}",
                          std::string(pos, max_uint64_len));
            return error;
          case std::errc::result_out_of_range:
            spdlog::error("Battery voltage value out of uint64_t range.");
            return error;
        }

        // Append the value
        ARROW_ROE(values_bld->Append(val));

        pos = SkipWhitespace(val_result.ptr, end);
        if (*pos == ',') {
          pos++;
        }
      }
    }

    // Scan for object end
    pos = SkipWhitespace(pos, end);
    if (*pos != '}') {
      spdlog::error("Expected '}', encountered '{}'", *pos);
      return error;
    }
    pos++;

    // Scan for newline delimiter
    pos = SkipWhitespace(pos, end);
    if (*pos != '\n') {
      spdlog::error("Expected '\\n' (0x20), encountered '{}' (0x{})", *pos,
                    static_cast<uint8_t>(*pos));
      return error;
    }
    pos++;
  }

  return Status::OK();
}

auto BatteryParser::ParseOne(const illex::JSONBuffer* buffer, ParsedBatch* out)
    -> Status {
  // Reserve expected number of JSONs in case a seq. no. column is desired.
  arrow::UInt64Builder seq_bld;
  uint64_t seq = buffer->range().first;
  if (seq_column) {
    ARROW_ROE(seq_bld.Reserve(buffer->range().last - buffer->range().first + 1));
  }

  auto values_bld = std::make_shared<arrow::UInt64Builder>();
  arrow::ListBuilder list_bld(arrow::default_memory_pool(), values_bld);

  BOLSON_ROE(ParseBatteryNDJSONs(reinterpret_cast<const char*>(buffer->data()),
                                 buffer->size(), &list_bld, values_bld.get()));

  std::shared_ptr<arrow::ListArray> voltage;
  ARROW_ROE(list_bld.Finish(&voltage));

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

auto BatteryParserContext::Make(const BatteryOptions& opts, size_t num_parsers,
                                std::shared_ptr<ParserContext>* out) -> Status {
  auto result = std::make_shared<BatteryParserContext>();

  // Use default allocator.
  result->allocator_ = std::make_shared<buffer::Allocator>();

  // Initialize all parsers.
  result->parsers_ = std::vector<std::shared_ptr<BatteryParser>>(
      num_parsers, std::make_shared<BatteryParser>(opts.seq_column));

  // Allocate buffers. Use number of parsers if number of buffers is 0 in options.
  auto num_buffers = opts.num_buffers == 0 ? num_parsers : opts.num_buffers;
  BOLSON_ROE(result->AllocateBuffers(num_buffers, opts.buf_capacity));

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
  sub->add_option("--custom-battery-buf-cap", out->buf_capacity,
                  "Custom battery parser input buffer capacity.")
      ->default_val(BOLSON_CUSTOM_BATTERY_DEFAULT_BUFFER_CAP);
  sub->add_flag("--custom-battery-seq-col", out->seq_column,
                "Custom battery parser, retain ordering information by adding a sequence "
                "number column.")
      ->default_val(false);
}

}  // namespace bolson::parse::custom
