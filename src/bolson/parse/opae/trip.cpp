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

#include "bolson/parse/opae/trip.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <fletcher/context.h>
#include <fletcher/fletcher.h>
#include <fletcher/kernel.h>
#include <fletcher/platform.h>
#include <putong/timer.h>

#include <CLI/CLI.hpp>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>

#include "bolson/buffer/opae_allocator.h"
#include "bolson/latency.h"
#include "bolson/log.h"
#include "bolson/parse/opae/opae.h"
#include "bolson/parse/parser.h"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s)                                                 \
  {                                                                     \
    auto __status = (s);                                                \
    if (!__status.ok())                                                 \
      return Status(Error::OpaeError, "Fletcher: " + __status.message); \
  }                                                                     \
  void()

namespace bolson::parse::opae {

auto TripParser::input_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = arrow::schema(
      {arrow::field("timestamp", arrow::utf8(), false),
       arrow::field("timezone", arrow::uint64(), false),
       arrow::field("vin", arrow::uint64(), false),
       arrow::field("odometer", arrow::uint64(), false),
       arrow::field("hypermiling", arrow::boolean(), false),
       arrow::field("avgspeed", arrow::uint64(), false),
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
       arrow::field("accel_decel", arrow::uint64(), false),
       arrow::field("speed_changes", arrow::uint64(), false)});
  return result;
}

static auto output_schema_hw() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema(
          {fletcher::WithMetaEPC(*arrow::field("timestamp", arrow::utf8(), false), 1),
           arrow::field("tag", arrow::uint64(), false),
           arrow::field("timezone", arrow::uint64(), false),
           arrow::field("vin", arrow::uint64(), false),
           arrow::field("odometer", arrow::uint64(), false),
           arrow::field("hypermiling", arrow::uint8(), false),
           arrow::field("avgspeed", arrow::uint64(), false),
           arrow::field("sec_in_band", arrow::uint64(), false),
           arrow::field("miles_in_time_range", arrow::uint64(), false),
           arrow::field("const_speed_miles_in_band", arrow::uint64(), false),
           arrow::field("vary_speed_miles_in_band", arrow::uint64(), false),
           arrow::field("sec_decel", arrow::uint64(), false),
           arrow::field("sec_accel", arrow::uint64(), false),
           arrow::field("braking", arrow::uint64(), false),
           arrow::field("accel", arrow::uint64(), false),
           arrow::field("orientation", arrow::uint8(), false),
           arrow::field("small_speed_var", arrow::uint64(), false),
           arrow::field("large_speed_var", arrow::uint64(), false),
           arrow::field("accel_decel", arrow::uint64(), false),
           arrow::field("speed_changes", arrow::uint64(), false)}),
      "output", fletcher::Mode::WRITE);
  return result;
}

static auto output_schema_sw() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema(
          {arrow::field("timestamp", arrow::utf8(), false),
           arrow::field("tag", arrow::uint64(), false),
           arrow::field("timezone", arrow::uint64(), false),
           arrow::field("vin", arrow::uint64(), false),
           arrow::field("odometer", arrow::uint64(), false),
           arrow::field("hypermiling", arrow::uint8(), false),
           arrow::field("avgspeed", arrow::uint64(), false),
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
           arrow::field("orientation", arrow::uint8(), false),
           arrow::field(
               "small_speed_var",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13),
               false),
           arrow::field(
               "large_speed_var",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13),
               false),
           arrow::field("accel_decel", arrow::uint64(), false),
           arrow::field("speed_changes", arrow::uint64(), false)}),
      "output", fletcher::Mode::WRITE);
  return result;
}

auto TripParser::output_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = arrow::schema(
      {arrow::field("bolson_seq", arrow::uint64(), false),
       arrow::field("timestamp", arrow::utf8(), false),
       arrow::field("timezone", arrow::uint64(), false),
       arrow::field("vin", arrow::uint64(), false),
       arrow::field("odometer", arrow::uint64(), false),
       arrow::field("hypermiling", arrow::boolean(), false),
       arrow::field("avgspeed", arrow::uint64(), false),
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
       arrow::field("accel_decel", arrow::uint64(), false),
       arrow::field("speed_changes", arrow::uint64(), false)});
  return result;
}

// Fletcher default regs (unused):
// 0 control
// 1 status
// 2 return lo
// 3 return hi
static const size_t default_regs = 4;

// Input batches ranges per parser hardware instance:
// 0 input firstidx
// 1 input lastidx
static const size_t input_range_regs_per_inst = 2;

// Output batch ranges:
// 0 output firstidx
// 1 output lastidx
static const size_t output_range_regs = 2;

// Input buffers address registers per hardware instance:
// 0 input val addr lo
// 1 input val addr hi
static const size_t in_addr_regs_per_inst = 2;

// Output buffer address registers:
static const size_t out_addr_regs = 42;

// Custom registers per instance:
// 0 tag
// 1 consumed_bytes
static const size_t custom_regs_per_inst = 2;

auto TripParser::custom_regs_offset() const -> size_t {
  return default_regs +
         num_hardware_parsers_ * (input_range_regs_per_inst + in_addr_regs_per_inst) +
         output_range_regs + out_addr_regs;
}

auto TripParser::input_firstidx_offset(size_t idx) -> size_t {
  return default_regs + input_range_regs_per_inst * idx;
}

auto TripParser::input_lastidx_offset(size_t idx) -> size_t {
  return input_firstidx_offset(idx) + 1;
}

auto TripParser::input_values_lo_offset(size_t idx) const -> size_t {
  return default_regs + input_range_regs_per_inst * num_hardware_parsers_ + 2 +
         in_addr_regs_per_inst * idx;
}

auto TripParser::input_values_hi_offset(size_t idx) const -> size_t {
  return input_values_lo_offset(idx) + 1;
}

auto TripParser::tag_offset(size_t idx) const -> size_t {
  return custom_regs_offset() + custom_regs_per_inst * idx;
}

auto TripParser::bytes_consumed_offset(size_t idx) const -> size_t {
  return custom_regs_offset() + custom_regs_per_inst * idx + 1;
}

auto TripParserContext::PrepareInputBatches() -> Status {
  for (const auto& buf : buffers_) {
    auto wrapped = arrow::Buffer::Wrap(buf.data(), buf.capacity());
    auto array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), buf.capacity(), wrapped);
    auto batch =
        arrow::RecordBatch::Make(raw_json_input_schema(), buf.capacity(), {array});
    batches_in.push_back(batch);
  }
  return Status::OK();
}

[[nodiscard]] static auto AllocateFixedSizeListArray(
    const std::shared_ptr<arrow::DataType>& type,
    const std::shared_ptr<arrow::Array>& value_array, size_t list_size,
    std::shared_ptr<arrow::FixedSizeListArray>* out) -> Status {
  auto list_array = std::make_shared<arrow::FixedSizeListArray>(
      arrow::fixed_size_list(type, list_size), 0, value_array);
  *out = list_array;
  return Status::OK();
}

[[nodiscard]] static auto AllocatePrimitiveArray(
    buffer::OpaeAllocator* allocator, const std::shared_ptr<arrow::DataType>& type,
    size_t buffer_size, std::shared_ptr<arrow::PrimitiveArray>* out) -> Status {
  std::byte* value_data = nullptr;
  BOLSON_ROE(allocator->Allocate(buffer::OpaeAllocator().fixed_capacity(), &value_data));
  auto value_buffer = arrow::Buffer::Wrap(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(type, 0, value_buffer);
  *out = value_array;
  return Status::OK();
}

[[nodiscard]] static auto AllocateStringArray(buffer::OpaeAllocator* allocator,
                                              size_t offset_buffer_size,
                                              size_t value_buffer_size,
                                              std::shared_ptr<arrow::StringArray>* out)
    -> Status {
  std::byte* offset_data = nullptr;
  BOLSON_ROE(allocator->Allocate(offset_buffer_size, &offset_data));
  memset(offset_data, 0, offset_buffer_size);
  auto offset_buffer = arrow::Buffer::Wrap(offset_data, offset_buffer_size);
  std::byte* value_data = nullptr;
  BOLSON_ROE(allocator->Allocate(value_buffer_size, &value_data));
  auto value_buffer = arrow::Buffer::Wrap(value_data, value_buffer_size);
  auto string_array =
      std::make_shared<arrow::StringArray>(0, offset_buffer, value_buffer);
  *out = string_array;
  return Status::OK();
}

template <typename T>
static auto ResizeArray(const std::shared_ptr<T>& array,
                        const std::shared_ptr<arrow::DataType>& type, size_t num_rows)
    -> std::shared_ptr<T> {
  auto value_array = std::make_shared<T>(num_rows, array->values(), nullptr, 0);
  return value_array;
}

static auto ResizeArray(const std::shared_ptr<arrow::StringArray>& array, size_t num_rows)
    -> std::shared_ptr<arrow::StringArray> {
  auto string_array = std::make_shared<arrow::StringArray>(
      num_rows, array->value_offsets(), array->value_data());
  return string_array;
}

static auto ResizeArray(const std::shared_ptr<arrow::FixedSizeListArray>& array,
                        const std::shared_ptr<arrow::DataType>& type, size_t num_rows)
    -> std::shared_ptr<arrow::FixedSizeListArray> {
  assert(array->type()->field(0)->type()->id() == arrow::Type::UINT64);
  size_t list_size = array->list_type()->list_size();
  auto values_buf = array->values()->data()->buffers[1];
  auto num_values = num_rows * list_size;

  auto values = std::make_shared<arrow::UInt64Array>(num_values, values_buf, nullptr, 0);

  auto list_array = arrow::FixedSizeListArray::FromArrays(values, list_size).ValueOrDie();
  return std::static_pointer_cast<arrow::FixedSizeListArray>(list_array);
}

static auto WrapTripReport(int32_t num_rows,
                           const std::vector<std::shared_ptr<arrow::Array>>& arrays,
                           const std::shared_ptr<arrow::Schema>& schema,
                           std::shared_ptr<arrow::RecordBatch>* out) -> Status {
  std::vector<std::shared_ptr<arrow::Array>> resized_arrays;

  for (const auto& array : arrays) {
    if (array->type()->Equals(arrow::uint64())) {
      auto field = std::static_pointer_cast<arrow::UInt64Array>(array);
      resized_arrays.push_back(ResizeArray(field, arrow::uint64(), num_rows));
    } else if (array->type()->Equals(arrow::uint8())) {
      auto field = std::static_pointer_cast<arrow::UInt8Array>(array);
      resized_arrays.push_back(ResizeArray(field, arrow::uint8(), num_rows));
    } else if (array->type()->id() == arrow::Type::FIXED_SIZE_LIST) {
      if (array->type()->field(0)->type()->id() == arrow::Type::UINT64) {
        auto field = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
        resized_arrays.push_back(ResizeArray(field, arrow::uint64(), num_rows));
      } else {
        return Status(Error::GenericError,
                      "Unexpected fixed size list item type when wrapping trip report "
                      "parser output.");
      }
    } else if (array->type()->Equals(arrow::utf8())) {
      auto field = std::static_pointer_cast<arrow::StringArray>(array);
      resized_arrays.push_back(ResizeArray(field, num_rows));
    } else {
      return Status(Error::GenericError,
                    "Unexpected array type when wrapping trip report parser output.");
    }
  }

  *out = arrow::RecordBatch::Make(schema, num_rows, resized_arrays);
  return Status::OK();
}

auto TripParserContext::PrepareOutputBatch() -> Status {
  for (const auto& f : output_schema_sw()->fields()) {
    if (f->type()->Equals(arrow::uint64())) {
      std::shared_ptr<arrow::PrimitiveArray> array;
      BOLSON_ROE(AllocatePrimitiveArray(&allocator, arrow::uint64(),
                                        allocator.fixed_capacity(), &array));
      output_arrays_sw.push_back(array);
      output_arrays_hw.push_back(array);
    } else if (f->type()->Equals(arrow::uint8())) {
      std::shared_ptr<arrow::PrimitiveArray> array;
      BOLSON_ROE(AllocatePrimitiveArray(&allocator, arrow::uint8(),
                                        allocator.fixed_capacity(), &array));
      output_arrays_sw.push_back(array);
      output_arrays_hw.push_back(array);
    } else if (f->type()->Equals(arrow::utf8())) {
      std::shared_ptr<arrow::StringArray> array;
      BOLSON_ROE(AllocateStringArray(&allocator, allocator.fixed_capacity(),
                                     allocator.fixed_capacity(), &array));
      output_arrays_sw.push_back(array);
      output_arrays_hw.push_back(array);
    } else if (f->type()->id() == arrow::Type::FIXED_SIZE_LIST) {
      std::shared_ptr<arrow::PrimitiveArray> values_array;
      std::shared_ptr<arrow::FixedSizeListArray> fixed_size_list_array;
      auto field = std::static_pointer_cast<arrow::FixedSizeListType>(f->type());
      BOLSON_ROE(AllocatePrimitiveArray(&allocator, arrow::uint64(),
                                        allocator.fixed_capacity(), &values_array));
      BOLSON_ROE(AllocateFixedSizeListArray(field->value_type(), values_array,
                                            field->list_size(), &fixed_size_list_array));
      output_arrays_sw.push_back(fixed_size_list_array);
      output_arrays_hw.push_back(values_array);
    }
  }

  batch_out_sw = arrow::RecordBatch::Make(output_schema_sw(), 0, output_arrays_sw);
  batch_out_hw = arrow::RecordBatch::Make(output_schema_hw(), 0, output_arrays_hw);

  return Status::OK();
}

static auto ConvertTagsToSeq(const std::shared_ptr<arrow::UInt64Array>& tags,
                             std::vector<uint64_t> seq_nos,
                             std::shared_ptr<arrow::UInt64Array>* out) -> Status {
  arrow::UInt64Builder bld;
  ARROW_ROE(bld.Reserve(tags->length()));

  for (size_t i = 0; i < tags->length(); i++) {
    assert(tags->Value(i) < seq_nos.size());
    bld.UnsafeAppend(seq_nos[tags->Value(i)]++);
  }
  ARROW_ROE(bld.Finish(out));
  return Status::OK();
}

static auto ConvertUInt8ToBool(const std::shared_ptr<arrow::UInt8Array>& col,
                               std::shared_ptr<arrow::BooleanArray>* out) -> Status {
  arrow::BooleanBuilder bld;
  ARROW_ROE(bld.Reserve(col->length()));
  const uint8_t* values = col->raw_values();
  for (size_t i = 0; i < col->length(); i++) {
    bld.UnsafeAppend(static_cast<bool>(values[i]));
  }
  ARROW_ROE(bld.Finish(out));
  return Status::OK();
}

/**
 * \brief Work-around for a couple of limitations to Fletcher / FPGA impl.
 *
 * Limitations:
 *  - no boolean ArrayWriter (this is probably supported in HW, but the type may just not
 *    have been included in Fletchgen.
 *  - rows from each parser are interleaved in the output in an order depending on the
 *    data.
 *  - rows currently get a tag saying which parser processed them, this must be turned
 *    into sequence numbers to comply to the other out of order parser implementations
 */
static auto FixResult(const std::shared_ptr<arrow::RecordBatch>& batch,
                      std::vector<uint64_t> seq_nos)
    -> std::shared_ptr<arrow::RecordBatch> {
  // Work-around to turn tag into sequence numbers (overwrites existing buffer)
  assert(batch->column_name(1) == "tag");  // sanity check
  assert(batch->column(1)->type_id() == arrow::Type::UINT64);
  std::shared_ptr<arrow::UInt64Array> seq;
  ConvertTagsToSeq(std::static_pointer_cast<arrow::UInt64Array>(batch->column(1)),
                   std::move(seq_nos), &seq);

  // Work-around to turn uint8 fields back into boolean again.
  std::shared_ptr<arrow::BooleanArray> hypermiling, orientation;
  ConvertUInt8ToBool(
      std::static_pointer_cast<arrow::UInt8Array>(batch->GetColumnByName("hypermiling")),
      &hypermiling);
  ConvertUInt8ToBool(
      std::static_pointer_cast<arrow::UInt8Array>(batch->GetColumnByName("orientation")),
      &orientation);

  std::vector<std::shared_ptr<arrow::Array>> columns = {
      seq,
      batch->GetColumnByName("timestamp"),
      batch->GetColumnByName("timezone"),
      batch->GetColumnByName("vin"),
      batch->GetColumnByName("odometer"),
      hypermiling,
      batch->GetColumnByName("avgspeed"),
      batch->GetColumnByName("sec_in_band"),
      batch->GetColumnByName("miles_in_time_range"),
      batch->GetColumnByName("const_speed_miles_in_band"),
      batch->GetColumnByName("vary_speed_miles_in_band"),
      batch->GetColumnByName("sec_decel"),
      batch->GetColumnByName("sec_accel"),
      batch->GetColumnByName("braking"),
      batch->GetColumnByName("accel"),
      orientation,
      batch->GetColumnByName("small_speed_var"),
      batch->GetColumnByName("large_speed_var"),
      batch->GetColumnByName("accel_decel"),
      batch->GetColumnByName("speed_changes")};

  // Sanity check in debug.
  for (const auto& col : columns) {
    assert(col != nullptr);
  }

  // Work-around to fix schema field order (should be zero-copy)
  auto result =
      arrow::RecordBatch::Make(TripParser::output_schema(), batch->num_rows(), columns);

  return result;
}

[[nodiscard]] auto TripParser::Parse(const std::vector<illex::JSONBuffer*>& in,
                                     std::vector<ParsedBatch>* out) -> Status {
  std::vector<uint64_t> seq_nos;

  size_t bytes_total = 0;
  size_t expected_rows = 0;
  for (size_t i = 0; i < in.size(); i++) {
    SPDLOG_DEBUG("TripParser | Parsing buffer {:2}:\n{}", i, ToString(*in[i], false));
    BOLSON_ROE(WriteInputMetaData(platform_, in[i], i));
    seq_nos.push_back(in[i]->range().first);
    bytes_total += in[i]->size();
    expected_rows += in[i]->num_jsons();
  }

  // Reset kernel.
  kernel_->Reset();
  // Start kernel.
  kernel_->Start();
  // Wait for finish.
  // kernel_->PollUntilDone();

  // Grab the return value (number of parsed JSON objects) and wrap the output Batch.
  dau_t ret_val;
  ret_val.full = 0;
  uint64_t num_rows = 0;
  uint32_t status = 0;

  do {
    // status reg @ offset 1
    ReadMMIO(platform_, 1, &status, 0, "Status");
#ifndef NDEBUG
    uint64_t bytes_consumed = 0;
    for (int i = 0; i < num_hardware_parsers_; i++) {
      uint32_t bc = 0;
      BOLSON_ROE(ReadMMIO(platform_, bytes_consumed_offset(i), &bc, 0,
                          "Bytes consumed " + std::to_string(i)));
      SPDLOG_DEBUG("TripParser | Parser {:2} bytes consumed: {}/{}", i, bc,
                   in[i]->size());
      bytes_consumed += bc;
    }
    SPDLOG_DEBUG("TripParser | Total bytes consumed: {}/{}", bytes_consumed, bytes_total);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
#else
    std::this_thread::sleep_for(std::chrono::microseconds(BOLSON_QUEUE_WAIT_US));
#endif
  } while ((status & stat_done) != stat_done);

  // Grab the return value (number of parsed JSON objects) and wrap the output Batch.
  FLETCHER_ROE(kernel_->GetReturn(&ret_val.lo, &ret_val.hi));
  num_rows = static_cast<uint64_t>(ret_val.full);

  if (num_rows != expected_rows) {
    return Status(Error::OpaeError,
                  "Expected " + std::to_string(expected_rows) +
                      " rows, but OPAE TripParser returned batch with " +
                      std::to_string(num_rows) + " rows.");
  }

  std::shared_ptr<arrow::RecordBatch> unfixed_result;
  BOLSON_ROE(
      WrapTripReport(num_rows, *output_arrays_sw_, output_schema_sw(), &unfixed_result));
  auto result = FixResult(unfixed_result, seq_nos);

  out->push_back(ParsedBatch(result, {0, static_cast<uint64_t>(result->num_rows() - 1)}));

  return Status::OK();
}

auto TripParser::WriteInputMetaData(fletcher::Platform* platform, illex::JSONBuffer* in,
                                    size_t idx) -> Status {
  // rewrite the input last index because of opae limitations.
  BOLSON_ROE(WriteMMIO(platform, input_lastidx_offset(idx),
                       static_cast<uint32_t>(in->size()), idx, "input last idx"));

  dau_t input_addr;
  input_addr.full = h2d_addr_map->at(in->data());

  BOLSON_ROE(WriteMMIO(platform, input_values_lo_offset(idx), input_addr.lo, idx,
                       "input values addr lo"));
  BOLSON_ROE(WriteMMIO(platform, input_values_hi_offset(idx), input_addr.hi, idx,
                       "input values addr hi"));

  BOLSON_ROE(WriteMMIO(platform, tag_offset(idx), idx, idx, "tag"));

  return Status::OK();
}

TripParser::TripParser(fletcher::Platform* platform, fletcher::Context* context,
                       fletcher::Kernel* kernel, AddrMap* addr_map,
                       std::vector<std::shared_ptr<arrow::Array>>* output_arrays,
                       size_t num_parsers)
    : platform_(platform),
      context_(context),
      kernel_(kernel),
      h2d_addr_map(addr_map),
      output_arrays_sw_(output_arrays),
      num_hardware_parsers_(num_parsers) {}

auto ToString(const illex::JSONBuffer& buffer, bool show_contents) -> std::string {
  std::stringstream ss;
  ss << "Buffer   : " << buffer.data() << "\n"
     << "Capacity : " << buffer.capacity() << "\n"
     << "Size     : " << buffer.size() << "\n"
     << "JSONS    : " << buffer.num_jsons();
  if (show_contents) {
    ss << "\n";
    ss << std::string_view(reinterpret_cast<const char*>(buffer.data()), buffer.size());
  }
  return ss.str();
}

void AddTripOptionsToCLI(CLI::App* sub, TripOptions* out) {
  sub->add_option("--trip-afu-id", out->afu_id,
                  "OPAE \"trip report\" AFU ID. "
                  "If not supplied, it is derived from number of parser instances.");
  sub->add_option("--trip-num-parsers", out->num_parsers,
                  "OPAE \"trip report\" number of parser instances.")
      ->default_val(BOLSON_DEFAULT_OPAE_TRIP_PARSERS);
}

auto TripReportBatchToString(const arrow::RecordBatch& batch) -> std::string {
  std::stringstream ss;
  for (size_t r = 0; r < batch.num_rows(); r++) {
    for (size_t c = 0; c < batch.num_columns(); c++) {
      ss << std::setw(25) << batch.column_name(c) << " : ";
      switch (batch.column(c)->type_id()) {
        case arrow::Type::UINT64:
          ss << std::static_pointer_cast<arrow::UInt64Array>(batch.column(c))->Value(r);
          break;
        case arrow::Type::BOOL:
          ss << std::static_pointer_cast<arrow::BooleanArray>(batch.column(c))->Value(r);
          break;
        case arrow::Type::FIXED_SIZE_LIST: {
          assert(batch.column(c)->type()->field(0)->type()->id() == arrow::Type::UINT64);
          auto slice = std::static_pointer_cast<arrow::UInt64Array>(
              std::static_pointer_cast<arrow::FixedSizeListArray>(batch.column(c))
                  ->value_slice(r));
          ss << "[";
          for (int i = 0; i < slice->length(); i++) {
            ss << slice->Value(i);
            if (i != slice->length() - 1) {
              ss << ",";
            }
          }
          ss << "]";
          break;
        }
        case arrow::Type::STRING:
          ss << std::static_pointer_cast<arrow::StringArray>(batch.column(c))
                    ->GetString(r);
          break;
        default:
          ss << "INVALID TYPE!";
      }
      ss << std::endl;
    }
    ss << std::string(96, '-') << std::endl;
  }
  return ss.str();
}

auto TripParserContext::input_schema() const -> std::shared_ptr<arrow::Schema> {
  return TripParser::input_schema();
}

auto TripParserContext::output_schema() const -> std::shared_ptr<arrow::Schema> {
  return TripParser::output_schema();
}

auto TripParserContext::CheckThreadCount(size_t num_threads) const -> size_t { return 1; }

auto TripParserContext::CheckBufferCount(size_t num_buffers) const -> size_t {
  return num_parsers_;
}

auto TripParserContext::Make(const TripOptions& opts, std::shared_ptr<ParserContext>* out)
    -> Status {
  std::string afu_id;
  DeriveAFUID(opts.afu_id, BOLSON_DEFAULT_OPAE_TRIP_AFUID, opts.num_parsers, &afu_id);
  SPDLOG_DEBUG("TripParserContext | Using AFU ID: {}", afu_id);

  // Create and set up result.
  auto result = std::shared_ptr<TripParserContext>(new TripParserContext(opts));
  SPDLOG_DEBUG("TripParserContext | Setting up for {} parsers.", result->num_parsers_);

  FLETCHER_ROE(fletcher::Platform::Make("opae", &result->platform, false));

  result->afu_id_ = afu_id;
  char* afu_id_ptr = result->afu_id_.data();
  result->platform->init_data = &afu_id_ptr;

  // Initialize the platform.
  FLETCHER_ROE(result->platform->Init());

  // Allocate input buffers.
  BOLSON_ROE(result->AllocateBuffers(result->num_parsers_,
                                     result->allocator_->fixed_capacity()));

  // Pull everything through the fletcher stack once.
  FLETCHER_ROE(fletcher::Context::Make(&result->context, result->platform));

  BOLSON_ROE(result->PrepareInputBatches());
  BOLSON_ROE(result->PrepareOutputBatch());

  for (const auto& batch : result->batches_in) {
    FLETCHER_ROE(result->context->QueueRecordBatch(batch));
  }

  FLETCHER_ROE(result->context->QueueRecordBatch(result->batch_out_hw));

  // Enable context.
  FLETCHER_ROE(result->context->Enable());
  // Construct kernel handler.
  result->kernel = std::make_shared<fletcher::Kernel>(result->context);
  // Write metadata.
  FLETCHER_ROE(result->kernel->WriteMetaData());

  // Workaround to obtain buffer device address.
  result->h2d_addr_map = ExtractAddrMap(result->context.get());
  SPDLOG_DEBUG("TripParserContext | OPAE host address / device address map:");
  for (auto& kv : result->h2d_addr_map) {
    SPDLOG_DEBUG("  H: 0x{:016X} <--> D: 0x{:016X}", reinterpret_cast<uint64_t>(kv.first),
                 kv.second);
  }

  SPDLOG_DEBUG("TripParserContext | Preparing parser.");
  BOLSON_ROE(result->PrepareParser());

  *out = result;

  return Status::OK();
}

auto TripParserContext::parsers() -> std::vector<std::shared_ptr<Parser>> {
  return {parser};
}

TripParserContext::TripParserContext(const TripOptions& opts)
    : num_parsers_(opts.num_parsers), afu_id_(opts.afu_id) {
  allocator_ = std::make_shared<buffer::OpaeAllocator>();
}

auto TripParserContext::PrepareParser() -> Status {
  parser = std::make_shared<TripParser>(platform.get(), context.get(), kernel.get(),
                                        &h2d_addr_map, &output_arrays_sw, num_parsers_);
  return Status::OK();
}

}  // namespace bolson::parse::opae