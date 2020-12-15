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

#include <utility>
#include <sys/mman.h>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <putong/timer.h>

#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include "bolson/parse/parser.h"
#include "bolson/parse/opae_battery_impl.h"
#include "bolson/utils.h"

#define OPAE_BATTERY_REG_INPUT_FIRSTIDX      4
#define OPAE_BATTERY_REG_INPUT_LASTIDX       5
#define OPAE_BATTERY_REG_OUTPUT_FIRSTIDX     6
#define OPAE_BATTERY_REG_OUTPUT_LASTIDX      7
#define OPAE_BATTERY_REG_INPUT_VALUES_LO     8
#define OPAE_BATTERY_REG_INPUT_VALUES_HI     9
#define OPAE_BATTERY_REG_OUTPUT_OFFSETS_LO  10
#define OPAE_BATTERY_REG_OUTPUT_OFFSETS_HI  11
#define OPAE_BATTERY_REG_OUTPUT_VALUES_LO   12
#define OPAE_BATTERY_REG_OUTPUT_VALUES_HI   13

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                                               \
  auto __status = (s);                                                                  \
  if (!__status.ok()) return Status(Error::OpaeError, "Fletcher: " + __status.message); \
}                                                                                       \
void()

namespace bolson::parse {

static auto input_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("input", arrow::uint8(), false)}),
      "input",
      fletcher::Mode::READ);
  return result;
}

static auto output_type() -> std::shared_ptr<arrow::DataType> {
  static auto result = arrow::list(arrow::field("item", arrow::uint64(), false));
  return result;
}

static auto output_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("voltage", output_type(), false)}),
      "output",
      fletcher::Mode::WRITE);
  return result;
}

auto OpaeBatteryParser::PrepareInputBatch(const uint8_t *buffer_raw,
                                          size_t size) -> Status {
  auto buf = arrow::Buffer::Wrap(buffer_raw, size);
  auto arr = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), size, buf);
  batch_in = arrow::RecordBatch::Make(input_schema(), size, {arr});
  return Status::OK();
}

auto OpaeBatteryParser::PrepareOutputBatch(size_t offsets_capacity,
                                           size_t values_capacity) -> Status {
  BOLSON_ROE(allocator.Allocate(offsets_capacity, &out_offsets));
  BOLSON_ROE(allocator.Allocate(values_capacity, &out_values));

  auto offset_buffer = arrow::Buffer::Wrap(out_offsets, offsets_capacity);
  auto values_buffer = arrow::Buffer::Wrap(out_values, values_capacity);
  auto values_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, values_buffer);
  auto list_array = std::make_shared<arrow::ListArray>(output_type(),
                                                       0,
                                                       offset_buffer,
                                                       values_array);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  batch_out = arrow::RecordBatch::Make(output_schema(), 0, arrays);

  return Status::OK();
}

auto OpaeBatteryParser::Make(const OpaeBatteryOptions &opts,
                             std::shared_ptr<OpaeBatteryParser> *out) -> Status {
  auto result = std::shared_ptr<OpaeBatteryParser>(new OpaeBatteryParser(opts));

  result->PrepareOutputBatch(opts.output_capacity_off, opts.output_capacity_val);

  FLETCHER_ROE(fletcher::Platform::Make("opae", &result->platform, false));
  char *afu_id = result->opts_.afu_id.data();
  result->platform->init_data = &afu_id;
  FLETCHER_ROE(result->platform->Init());

  *out = std::move(result);

  return Status::OK();
}

static auto WrapOutput(int32_t num_rows,
                       uint8_t *offsets,
                       uint8_t *values,
                       std::shared_ptr<arrow::Schema> schema,
                       std::shared_ptr<arrow::RecordBatch> *out) -> Status {
  auto ret = Status::OK();

  // +1 because the last value in offsets buffer is the next free index in the values
  // buffer.
  int32_t num_offsets = num_rows + 1;

  // Obtain the last value in the offsets buffer to know how many values there are.
  int32_t num_values = (reinterpret_cast<int32_t *>(offsets))[num_rows];

  size_t num_offset_bytes = num_offsets * sizeof(int32_t);
  size_t num_values_bytes = num_values * sizeof(uint64_t);

  try {
//    auto new_offs =
//        std::shared_ptr(std::move(arrow::AllocateBuffer(num_offset_bytes).ValueOrDie()));
//    auto new_vals =
//        std::shared_ptr(std::move(arrow::AllocateBuffer(num_values_bytes).ValueOrDie()));

//    std::memcpy(new_offs->mutable_data(), offsets, num_offset_bytes);
//    std::memcpy(new_vals->mutable_data(), values, num_values_bytes);

    auto values_buf = arrow::Buffer::Wrap(values, num_values);
    auto offsets_buf = arrow::Buffer::Wrap(offsets, num_values);
    auto value_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, values_buf);
    auto offsets_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::int32(), num_offsets, offsets_buf);
    auto list_array = arrow::ListArray::FromArrays(*offsets_array, *value_array);

    std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array.ValueOrDie()};
    *out = arrow::RecordBatch::Make(std::move(schema), num_rows, arrays);
  } catch (std::exception &e) {
    return Status(Error::ArrowError, e.what());
  }

  return Status::OK();
}

auto OpaeBatteryParser::Parse(illex::RawJSONBuffer *in, ParsedBuffer *out) -> Status {
  ParsedBuffer result;
  // rewrite the input last index because of opae limitations.
  FLETCHER_ROE(platform->WriteMMIO(OPAE_BATTERY_REG_INPUT_LASTIDX, in->size()));

  dau_t input_addr;
  input_addr.full = buffer_addr_map[in->data()];

  FLETCHER_ROE(platform->WriteMMIO(OPAE_BATTERY_REG_INPUT_VALUES_LO, input_addr.lo));
  FLETCHER_ROE(platform->WriteMMIO(OPAE_BATTERY_REG_INPUT_VALUES_HI, input_addr.hi));

  // Reset the kernel, start it, and poll until completion.
  FLETCHER_ROE(kernel->Reset());
  FLETCHER_ROE(kernel->Start());
  FLETCHER_ROE(kernel->PollUntilDone());

  // Obtain the result.
  dau_t num_rows;
  FLETCHER_ROE(kernel->GetReturn(&num_rows.lo, &num_rows.hi));

  std::shared_ptr<arrow::RecordBatch> out_batch;
  BOLSON_ROE(WrapOutput(num_rows.full,
                        reinterpret_cast<uint8_t *>(out_offsets),
                        reinterpret_cast<uint8_t *>(out_values),
                        output_schema(),
                        &result.batch));

  result.parsed_bytes = in->size();

  *out = result;
  return Status::OK();
}
auto OpaeBatteryParser::Initialize(std::vector<illex::RawJSONBuffer *> buffers) -> Status {
  // Work-around, pull each buffer through the Fletcher stack.
  for (auto *buf : buffers) {
    // Prepare the input batch.
    // Because of limitations to the opae stuff, we need to pretend this input batch
    // is buffer::g_opae_buffercap in size for now.
    BOLSON_ROE(PrepareInputBatch(reinterpret_cast<const uint8_t *>(buf->data()),
                                 buffer::g_opae_buffercap));
    // Create a context.
    FLETCHER_ROE(fletcher::Context::Make(&context, platform));
    // Queue batches.
    FLETCHER_ROE(context->QueueRecordBatch(batch_in));
    FLETCHER_ROE(context->QueueRecordBatch(batch_out));
    // Enable context.
    FLETCHER_ROE(context->Enable());
    // Construct kernel handler.
    kernel = std::make_shared<fletcher::Kernel>(context);
    // Write metadata.
    FLETCHER_ROE(kernel->WriteMetaData());

    // Workaround to store buffer device address.
    buffer_addr_map[buf->data()] = context->device_buffer(0).device_address;
  }
  return Status::OK();
}

}