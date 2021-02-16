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
         num_hardware_parsers * (input_range_regs_per_inst + in_addr_regs_per_inst) +
         output_range_regs + out_addr_regs;
}

auto TripParser::ctrl_offset(size_t idx) -> size_t {
  return custom_regs_offset() + custom_regs_per_inst * idx;
}

auto TripParser::result_rows_offset_hi(size_t idx) -> size_t {
  return result_rows_offset_lo(idx) + 1;
}

auto TripParser::input_firstidx_offset(size_t idx) -> size_t {
  return default_regs + input_range_regs_per_inst * idx;
}

auto TripParser::input_lastidx_offset(size_t idx) -> size_t {
  return input_firstidx_offset(idx) + 1;
}

auto TripParser::input_values_lo_offset(size_t idx) const -> size_t {
  return default_regs + input_range_regs_per_inst * num_hardware_parsers + 2 +
         in_addr_regs_per_inst * idx;
}

auto TripParser::input_values_hi_offset(size_t idx) -> size_t {
  return input_values_lo_offset(idx) + 1;
}

auto TripParserContext::PrepareInputBatches(
    const std::vector<illex::JSONBuffer*>& buffers) -> Status {
  for (const auto& buf : buffers) {
    auto wrapped = arrow::Buffer::Wrap(buf->data(), buf->capacity());
    auto array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), buf->capacity(), wrapped);
    batches_in.push_back(
        arrow::RecordBatch::Make(input_schema(), buf->capacity(), {array}));
  }
  return Status::OK();
}

static auto AllocateFixedSizeListArray(const std::shared_ptr<arrow::DataType>& type,
                                       const std::shared_ptr<arrow::Array>& value_array,
                                       size_t list_size,
                                       std::shared_ptr<arrow::FixedSizeListArray>* out)
    -> Status {
  auto list_array = std::make_shared<arrow::FixedSizeListArray>(
      arrow::fixed_size_list(type, list_size), 0, value_array);
  *out = list_array;
  return Status::OK();
}

static auto AllocatePrimitiveArray(buffer::OpaeAllocator* allocator,
                                   const std::shared_ptr<arrow::DataType>& type,
                                   size_t buffer_size,
                                   std::shared_ptr<arrow::PrimitiveArray>* out)
    -> Status {
  std::byte* value_data = nullptr;
  BOLSON_ROE(allocator->Allocate(buffer::OpaeAllocator().fixed_capacity(), &value_data));
  auto value_buffer = arrow::Buffer::Wrap(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(type, 0, value_buffer);
  *out = value_array;
  return Status::OK();
}

static auto AllocateStringArray(buffer::OpaeAllocator* allocator,
                                size_t offset_buffer_size, size_t value_buffer_size,
                                std::shared_ptr<arrow::StringArray>* out) -> Status {
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

auto ResizeArray(const std::shared_ptr<arrow::PrimitiveArray>& array, size_t num_rows)
    -> std::shared_ptr<arrow::PrimitiveArray> {
  const auto* data_buff = array->data()->buffers[1]->data();
  auto value_buffer = std::make_shared<arrow::Buffer>(data_buff, num_rows);
  auto value_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_rows, value_buffer);
  return value_array;
}

auto ResizeArray(const std::shared_ptr<arrow::StringArray>& array, size_t num_rows)
    -> std::shared_ptr<arrow::StringArray> {
  auto string_array = std::make_shared<arrow::StringArray>(
      num_rows, array->value_offsets(), array->value_data());
  return string_array;
}

auto ResizeArray(const std::shared_ptr<arrow::FixedSizeListArray>& array, size_t num_rows)
    -> std::shared_ptr<arrow::FixedSizeListArray> {
  const auto* data_buff = array->values()->data()->buffers[1]->data();
  auto value_buff_size = array->length() * num_rows * sizeof(uint64_t);
  ;
  auto value_buffer = std::make_shared<arrow::Buffer>(data_buff, value_buff_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(
      arrow::uint64(), num_rows * array->value_length(), value_buffer);
  auto list_array = std::make_shared<arrow::FixedSizeListArray>(
      arrow::fixed_size_list(arrow::uint64(), array->value_length()), num_rows,
      value_array);
  return list_array;
}

auto WrapTripReport(int32_t num_rows,
                    const std::vector<std::shared_ptr<arrow::Array>>& arrays,
                    const std::shared_ptr<arrow::Schema>& schema)
    -> std::shared_ptr<arrow::RecordBatch> {
  std::vector<std::shared_ptr<arrow::Array>> resized_arrays;

  for (const auto& f : arrays) {
    if (f->type()->Equals(arrow::uint64())) {
      auto field = std::static_pointer_cast<arrow::PrimitiveArray>(f);
      resized_arrays.push_back(ResizeArray(field, num_rows));
    } else if (f->type()->Equals(arrow::uint8())) {
      auto field = std::static_pointer_cast<arrow::PrimitiveArray>(f);
      resized_arrays.push_back(ResizeArray(field, num_rows));
    } else if (f->type()->id() == arrow::Type::FIXED_SIZE_LIST) {
      auto field = std::static_pointer_cast<arrow::FixedSizeListArray>(f);
      resized_arrays.push_back(ResizeArray(field, num_rows));
    } else if (f->type()->Equals(arrow::utf8())) {
      auto field = std::static_pointer_cast<arrow::StringArray>(f);
      resized_arrays.push_back(ResizeArray(field, num_rows));
    }
  }

  return arrow::RecordBatch::Make(schema, num_rows, resized_arrays);
}

auto TripParserContext::PrepareOutputBatch() -> Status {
  for (const auto& f : output_schema_sw()->fields()) {
    if (f->type()->Equals(arrow::uint64())) {
      std::shared_ptr<arrow::PrimitiveArray> array;
      AllocatePrimitiveArray(&allocator, arrow::uint64(), allocator.fixed_capacity(),
                             &array);

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

static auto WrapOutput(int32_t num_rows, uint8_t* offsets, uint8_t* values,
                       std::shared_ptr<arrow::Schema> schema,
                       std::shared_ptr<arrow::RecordBatch>* out) -> Status {
  auto ret = Status::OK();

  // +1 because the last value in offsets buffer is the next free index in the values
  // buffer.
  int32_t num_offsets = num_rows + 1;

  // Obtain the last value in the offsets buffer to know how many values there are.
  int32_t num_values = (reinterpret_cast<int32_t*>(offsets))[num_rows];

  size_t num_offset_bytes = num_offsets * sizeof(int32_t);
  size_t num_values_bytes = num_values * sizeof(uint64_t);

  try {
    auto values_buf = arrow::Buffer::Wrap(values, num_values_bytes);
    auto offsets_buf = arrow::Buffer::Wrap(offsets, num_offset_bytes);
    auto value_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, values_buf);
    auto offsets_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::int32(), num_offsets, offsets_buf);
    auto list_array = arrow::ListArray::FromArrays(*offsets_array, *value_array);

    std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array.ValueOrDie()};
    *out = arrow::RecordBatch::Make(std::move(schema), num_rows, arrays);
  } catch (std::exception& e) {
    return Status(Error::ArrowError, e.what());
  }

  return Status::OK();
}

auto TripParser::Parse(const std::vector<illex::JSONBuffer*>& in,
                       std::vector<ParsedBatch>* out) -> Status {
  //  // Start kernel
  //  kernel->Start();
  //  // Wait for finish
  //  kernel->PollUntilDone();
  //
  //  // Grab the return value (number of parsed JSON objects) and wrap the output Batch.
  //  dau_t ret_val;
  //  FLETCHER_ROE(kernel->GetReturn(&ret_val.lo, &ret_val.hi));
  //  auto num_rows = static_cast<uint64_t>(ret_val.full);
  //
  //  auto result = WrapTripReport(num_rows, output_arrays_sw, output_schema_sw());
  //
  //  out->push_back(ParsedBatch(result, {0, 0}));
  //
  //  return Status::OK();
  return Status(Error::GenericError, "Not implemented.");
}

auto TripParser::WriteInputMetaData(fletcher::Platform* platform, illex::JSONBuffer* in,
                                    const AddrMap& h2d_addr_map, size_t idx) -> Status {
  // rewrite the input last index because of opae limitations.
  BOLSON_ROE(WriteMMIO(platform, input_lastidx_offset(idx),
                       static_cast<uint32_t>(in->size()), idx, "input last idx"));

  dau_t input_addr;
  input_addr.full = h2d_addr_map.at(in->data());

  BOLSON_ROE(WriteMMIO(platform, input_values_lo_offset(idx), input_addr.lo, idx,
                       "input values addr lo"));
  BOLSON_ROE(WriteMMIO(platform, input_values_hi_offset(idx), input_addr.hi, idx,
                       "input values addr hi"));

  BOLSON_ROE(WriteMMIO(platform, ctrl_offset(idx), idx, idx, "tag address"));

  return Status::OK();
}

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
  sub->add_option("--trip-afu-id",
                  "OPAE \"trip report\" AFU ID. "
                  "If not supplied, it is derived from number of parser instances.");
  sub->add_option("--trip-num-parsers",
                  "OPAE \"trip report\" number of parser instances.")
      ->default_val(BOLSON_DEFAULT_OPAE_TRIP_PARSERS);
}

auto TripParserContext::schema() const -> std::shared_ptr<arrow::Schema> {
  return output_schema_sw();
}

auto TripParserContext::CheckThreadCount(size_t num_threads) const -> size_t { return 1; }

auto TripParserContext::CheckBufferCount(size_t num_buffers) const -> size_t {
  return num_hardware_parsers;
}

auto TripParserContext::Make(const TripOptions& opts,
                             std::shared_ptr<ParserContext>* out) -> Status {
  // Attempt to auto derive AFU ID if not supplied.
  std::string afu_id_str;
  if (opts.afu_id.empty()) {
    if (opts.num_parsers > 255) {
      return Status(Error::OpaeError,
                    "Number of parsers larger than 255 is not supported.");
    }
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << std::hex << opts.num_parsers;
    // AFU ID from the hardware design script.
    afu_id_str = "5d2f9dba-e8d0-44f8-943d-36b25c2d40" + ss.str();
  } else {
    afu_id_str = opts.afu_id;
  }
  SPDLOG_DEBUG("TripParser | Using AFU ID: {}", afu_id_str);

  // Create and set up manager
  auto result = std::make_shared<TripParserContext>();
  result->num_hardware_parsers = opts.num_parsers;

  FLETCHER_ROE(fletcher::Platform::Make("opae", &result->platform, false));

  result->afu_id = afu_id_str;
  char* afu_id = result->afu_id.data();
  result->platform->init_data = &afu_id;

  // Initialize the platform.
  FLETCHER_ROE(result->platform->Init());

  return Status::OK();
}

auto TripParserContext::parsers() -> std::vector<std::shared_ptr<Parser>> {
  return {parser};
}

auto TripParserContext::Init(const std::vector<illex::JSONBuffer*>& buffers) -> Status {
  if (num_hardware_parsers != buffers.size()) {
    return Status(Error::OpaeError,
                  "TripParser implementation requires number of input buffers and "
                  "hardware internal number of parsers to be equal.");
  }

  SPDLOG_DEBUG("TripParser | Setting up for {} buffers.", buffers.size());

  BOLSON_ROE(PrepareInputBatches(buffers));
  BOLSON_ROE(PrepareOutputBatch());

  // Pull everything through the fletcher stack once.
  FLETCHER_ROE(fletcher::Context::Make(&context, platform));

  for (const auto& batch : batches_in) {
    FLETCHER_ROE(context->QueueRecordBatch(batch));
  }

  FLETCHER_ROE(context->QueueRecordBatch(batch_out_hw));

  // Enable context.
  FLETCHER_ROE(context->Enable());

  // Construct kernel handler.
  kernel = std::make_shared<fletcher::Kernel>(context);
  // Write metadata.
  FLETCHER_ROE(kernel->WriteMetaData());

  SPDLOG_DEBUG("TripParser | Preparing parsers.");

  // BOLSON_ROE(result->PrepareParsers());

  return Status::OK();
}

}  // namespace bolson::parse::opae
