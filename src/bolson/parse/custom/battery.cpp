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

auto BatteryParser::ParseOne(const illex::JSONBuffer* buffer, ParsedBatch* out)
    -> Status {
  auto values_bld = std::make_shared<arrow::UInt64Builder>();
  arrow::ListBuilder list_bld(arrow::default_memory_pool(), values_bld);
  // assume no unnecessary whitespaces anywhere, i.e. minified jsons
  // assume ndjson

  const char* battery_header = "{\"voltage\":[";
  const auto max_uint64_len =
      std::to_string(std::numeric_limits<uint64_t>::max()).length();

  const std::byte* pos = buffer->data();
  const std::byte* end = pos + buffer->size();

  SPDLOG_DEBUG("{}", ToString(*buffer, true));

  while (pos < end) {
    // Check header.
    if (std::memcmp(pos, battery_header, strlen(battery_header)) != 0) {
      throw std::runtime_error("Battery header did not correspond to expected header.");
    }

    // Start new list.
    ARROW_ROE(list_bld.Append());

    // Advance to values.
    pos += strlen(battery_header);
    // Attempt to get all array values.
    while (true) {  // fixme
      uint64_t val = 0;
      auto val_result = std::from_chars<uint64_t>(
          reinterpret_cast<const char*>(pos),
          reinterpret_cast<const char*>(pos + max_uint64_len), val);
      switch (val_result.ec) {
        default:
          break;
        case std::errc::invalid_argument:
          throw std::runtime_error(
              std::string("Battery voltage values contained invalid value: ") +
              std::string(reinterpret_cast<const char*>(pos), max_uint64_len));
        case std::errc::result_out_of_range:
          throw std::runtime_error("Battery voltage value out of uint64_t range.");
      }

      // Append the value
      ARROW_ROE(values_bld->Append(val));

      pos = reinterpret_cast<const std::byte*>(val_result.ptr);

      // Check for end of array.
      if ((char)*pos == ']') {
        pos += 3;  // for "}]\n"
        break;
      }
      if ((char)*pos != ',') {
        throw std::runtime_error("Battery voltage array expected ',' value separator.");
      }
      pos++;
    }
  }

  /*
  offsets.push_back(static_cast<int32_t>(values.size()));

  auto values_array = std::make_shared<arrow::UInt64Array>(arrow::uint64(), values.size(),
                                                           arrow::Buffer::Wrap(values));
  auto voltage_column = std::make_shared<arrow::ListArray>(
      arrow::list(arrow::uint64()), static_cast<int64_t>(offsets.size() - 1),
      arrow::Buffer::Wrap(offsets), values_array);
  */

  std::shared_ptr<arrow::ListArray> voltage;
  ARROW_ROE(list_bld.Finish(&voltage));

  out->seq_range = buffer->range();
  out->batch = arrow::RecordBatch::Make(output_schema(), voltage->length(), {voltage});

  SPDLOG_DEBUG("{}", out->batch->ToString());

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
auto BatteryParser::output_schema() -> std::shared_ptr<arrow::Schema> {
  return input_schema();
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
  return BatteryParser::output_schema();
}

}  // namespace bolson::parse::custom
