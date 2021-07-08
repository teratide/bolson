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

#include "bolson/parse/custom/trip.h"

#include <arrow/api.h>

#include <CLI/CLI.hpp>
#include <charconv>
#include <chrono>
#include <thread>
#include <utility>

#include "bolson/latency.h"
#include "bolson/log.h"
#include "bolson/parse/custom/common.h"
#include "bolson/parse/parser.h"

namespace bolson::parse::custom {

static auto schema_trip() -> std::shared_ptr<arrow::Schema> {
  static auto result = arrow::schema(
      {arrow::field("timestamp", arrow::utf8(), false),       //
       arrow::field("timezone", arrow::uint64(), false),      //
       arrow::field("vin", arrow::uint64(), false),           //
       arrow::field("odometer", arrow::uint64(), false),      //
       arrow::field("hypermiling", arrow::boolean(), false),  //
       arrow::field("avgspeed", arrow::uint64(), false),      //
       arrow::field(
           "sec_in_band",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12),
           false),
       arrow::field(
           "miles_in_time_range",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 24),
           false),
       arrow::field(
           "const_speed_miles_in_band",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12),
           false),
       arrow::field(
           "vary_speed_miles_in_band",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12),
           false),
       arrow::field(
           "sec_decel",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 10),
           false),
       arrow::field(
           "sec_accel",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 10),
           false),
       arrow::field(
           "braking",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 6),
           false),
       arrow::field(
           "accel",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 6),
           false),
       arrow::field("orientation", arrow::boolean(), false),
       arrow::field(
           "small_speed_var",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13),
           false),
       arrow::field(
           "large_speed_var",
           arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13),
           false),
       arrow::field("accel_decel", arrow::uint64(), false),  //
       arrow::field("speed_changes", arrow::uint64(), false)});
  return result;
}

auto TripParser::ParseOne(const illex::JSONBuffer* buffer, ParsedBatch* out) -> Status {
  SPDLOG_DEBUG(
      "Unsafe Trip Parsing: {}",
      std::string_view(reinterpret_cast<const char*>(buffer->data()), buffer->size()));

  const auto* pos = reinterpret_cast<const char*>(buffer->data());
  const auto* end = pos + buffer->size();

  // Eat any initial whitespace.
  pos = EatWhitespace(pos, end);

  // Start parsing objects
  while ((pos < end) && (pos != nullptr)) {
    pos = EatObjectStart(pos, end);  // {
    pos = EatWhitespace(pos, end);

    pos = EatMemberKey(pos, end, "timestamp");
    pos = EatWhitespace(pos, end);
    pos = EatMemberKeyValueSeperator(pos, end);
    pos = EatWhitespace(pos, end);
    pos = EatStringWithoutEscapes(pos, end, builder.timestamp.get());
    pos = EatWhitespace(pos, end);
    pos = EatChar(pos, end, ',');

    pos = EatUInt64MemberUnsafe(pos, end, "timezone", builder.timezone.get(), true);
    pos = EatUInt64MemberUnsafe(pos, end, "vin", builder.vin.get(), true);
    pos = EatUInt64MemberUnsafe(pos, end, "odometer", builder.odometer.get(), true);
    pos = EatBoolMemberUnsafe(pos, end, "hypermiling", builder.hypermiling.get(), true);
    pos = EatUInt64MemberUnsafe(pos, end, "avgspeed", builder.avgspeed.get(), true);

    // todo: make macros
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "sec_in_band", builder.sec_in_band.get(),
        reinterpret_cast<arrow::UInt64Builder*>(builder.sec_in_band->value_builder()),
        true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "miles_in_time_range", builder.miles_in_time_range.get(),
        reinterpret_cast<arrow::UInt64Builder*>(
            builder.miles_in_time_range->value_builder()),
        true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "const_speed_miles_in_band", builder.const_speed_miles_in_band.get(),
        reinterpret_cast<arrow::UInt64Builder*>(
            builder.const_speed_miles_in_band->value_builder()),
        true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "vary_speed_miles_in_band", builder.vary_speed_miles_in_band.get(),
        reinterpret_cast<arrow::UInt64Builder*>(
            builder.vary_speed_miles_in_band->value_builder()),
        true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "sec_decel", builder.sec_decel.get(),
        reinterpret_cast<arrow::UInt64Builder*>(builder.sec_decel->value_builder()),
        true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "sec_accel", builder.sec_accel.get(),
        reinterpret_cast<arrow::UInt64Builder*>(builder.sec_accel->value_builder()),
        true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "braking", builder.braking.get(),
        reinterpret_cast<arrow::UInt64Builder*>(builder.braking->value_builder()), true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "accel", builder.accel.get(),
        reinterpret_cast<arrow::UInt64Builder*>(builder.accel->value_builder()), true);
    pos = EatBoolMemberUnsafe(pos, end, "orientation", builder.orientation.get(), true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "small_speed_var", builder.small_speed_var.get(),
        reinterpret_cast<arrow::UInt64Builder*>(builder.small_speed_var->value_builder()),
        true);
    pos = EatUInt64FixedSizeArrayMemberUnsafe(
        pos, end, "large_speed_var", builder.large_speed_var.get(),
        reinterpret_cast<arrow::UInt64Builder*>(builder.large_speed_var->value_builder()),
        true);

    pos = EatUInt64MemberUnsafe(pos, end, "accel_decel", builder.accel_decel.get(), true);
    pos = EatUInt64MemberUnsafe(pos, end, "speed_changes", builder.speed_changes.get(),
                                false);

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

  out->seq_range = buffer->range();
  out->batch = builder.Finish();

  SPDLOG_DEBUG("Result: {}", out->batch->ToString());

  return Status::OK();
}

auto TripParser::Parse(const std::vector<illex::JSONBuffer*>& in,
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

auto TripParser::input_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = arrow::schema({arrow::field("voltage", voltage_type(), false)});
  return result;
}
auto TripParser::output_schema() const -> std::shared_ptr<arrow::Schema> {
  return schema_trip();
}

TripParser::TripParser(size_t pre_alloc_records, size_t pre_alloc_timestamp_values)
    : builder(TripBuilder(pre_alloc_records, pre_alloc_timestamp_values)) {}

auto TripParserContext::Make(const TripOptions& opts, size_t num_parsers,
                             std::shared_ptr<ParserContext>* out) -> Status {
  auto result = std::make_shared<TripParserContext>();

  // Use default allocator.
  result->allocator_ = std::make_shared<buffer::Allocator>();

  // Initialize all parsers.
  result->parsers_ = std::vector<std::shared_ptr<TripParser>>(
      num_parsers, std::make_shared<TripParser>(opts.pre_alloc_records,
                                                opts.pre_alloc_timestamp_values));

  // Allocate buffers. Use number of parsers if number of buffers is 0 in options.
  auto num_buffers = opts.num_buffers == 0 ? num_parsers : opts.num_buffers;
  BOLSON_ROE(result->AllocateBuffers(num_buffers, opts.buf_capacity));

  *out = std::static_pointer_cast<ParserContext>(result);

  return Status::OK();
}

auto TripParserContext::parsers() -> std::vector<std::shared_ptr<Parser>> {
  return CastPtrs<Parser>(parsers_);
}
auto TripParserContext::input_schema() const -> std::shared_ptr<arrow::Schema> {
  return TripParser::input_schema();
}

auto TripParserContext::output_schema() const -> std::shared_ptr<arrow::Schema> {
  return parsers_.front()->output_schema();
}

TripBuilder::TripBuilder(int64_t pre_alloc_rows, int64_t pre_alloc_ts_values)
    : timestamp(std::make_shared<arrow::StringBuilder>()),
      timezone(std::make_shared<arrow::UInt64Builder>()),
      vin(std::make_shared<arrow::UInt64Builder>()),
      odometer(std::make_shared<arrow::UInt64Builder>()),
      hypermiling(std::make_shared<arrow::BooleanBuilder>()),
      avgspeed(std::make_shared<arrow::UInt64Builder>()),
      sec_in_band(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 12)),
      miles_in_time_range(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 24)),
      const_speed_miles_in_band(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 12)),
      vary_speed_miles_in_band(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 12)),
      sec_decel(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 10)),
      sec_accel(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 10)),
      braking(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 6)),
      accel(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 6)),
      orientation(std::make_shared<arrow::BooleanBuilder>()),
      small_speed_var(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 13)),
      large_speed_var(std::make_shared<arrow::FixedSizeListBuilder>(
          arrow::default_memory_pool(), std::make_shared<arrow::UInt64Builder>(), 13)),
      accel_decel(std::make_shared<arrow::UInt64Builder>()),
      speed_changes(std::make_shared<arrow::UInt64Builder>()) {
  timestamp->Reserve(pre_alloc_rows);
  timestamp->ReserveData(pre_alloc_ts_values);
  timezone->Reserve(pre_alloc_rows);
  vin->Reserve(pre_alloc_rows);
  odometer->Reserve(pre_alloc_rows);
  hypermiling->Reserve(pre_alloc_rows);
  avgspeed->Reserve(pre_alloc_rows);
  sec_in_band->value_builder()->Reserve(pre_alloc_rows * 12);
  miles_in_time_range->value_builder()->Reserve(pre_alloc_rows * 24);
  const_speed_miles_in_band->value_builder()->Reserve(pre_alloc_rows * 12);
  vary_speed_miles_in_band->value_builder()->Reserve(pre_alloc_rows * 12);
  sec_decel->value_builder()->Reserve(pre_alloc_rows * 10);
  sec_accel->value_builder()->Reserve(pre_alloc_rows * 10);
  braking->value_builder()->Reserve(pre_alloc_rows * 6);
  accel->value_builder()->Reserve(pre_alloc_rows * 6);
  orientation->Reserve(pre_alloc_rows);
  small_speed_var->value_builder()->Reserve(pre_alloc_rows * 13);
  large_speed_var->value_builder()->Reserve(pre_alloc_rows * 13);
  accel_decel->Reserve(pre_alloc_rows);
  speed_changes->Reserve(pre_alloc_rows);
}

auto TripBuilder::Finish() -> std::shared_ptr<arrow::RecordBatch> {
  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      timestamp->Finish().ValueOrDie(),
      timezone->Finish().ValueOrDie(),
      vin->Finish().ValueOrDie(),
      odometer->Finish().ValueOrDie(),
      hypermiling->Finish().ValueOrDie(),
      avgspeed->Finish().ValueOrDie(),
      sec_in_band->Finish().ValueOrDie(),
      miles_in_time_range->Finish().ValueOrDie(),
      const_speed_miles_in_band->Finish().ValueOrDie(),
      vary_speed_miles_in_band->Finish().ValueOrDie(),
      sec_decel->Finish().ValueOrDie(),
      sec_accel->Finish().ValueOrDie(),
      braking->Finish().ValueOrDie(),
      accel->Finish().ValueOrDie(),
      orientation->Finish().ValueOrDie(),
      small_speed_var->Finish().ValueOrDie(),
      large_speed_var->Finish().ValueOrDie(),
      accel_decel->Finish().ValueOrDie(),
      speed_changes->Finish().ValueOrDie()};

  auto result = arrow::RecordBatch::Make(schema_trip(), arrays[0]->length(), arrays);
  assert(result != nullptr);

  return result;
}

void AddTripOptionsToCLI(CLI::App* sub, TripOptions* out) {
  sub->add_option("--custom-trip-buf-cap", out->buf_capacity,
                  "Custom trip report parser input buffer capacity.")
      ->default_val(BOLSON_CUSTOM_TRIP_DEFAULT_BUFFER_CAP);
  sub->add_option("--custom-trip-pre-alloc-records", out->pre_alloc_records,
                  "Pre-allocate this many records.")
      ->default_val(1024);
  sub->add_option("--custom-battery-pre-alloc-timestamp-values",
                  out->pre_alloc_timestamp_values,
                  "Pre-allocate this many values in the string values buffer for the "
                  "timestamp field.")
      ->default_val(1024);
}

}  // namespace bolson::parse::custom
