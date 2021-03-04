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
#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include "./log.h"
#include "./opae_battery_impl.h"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                                        \
  auto __status = (s);                                                           \
  if (!__status.ok()) throw std::runtime_error("Fletcher: " + __status.message); \
}                                                                                \
void()

/// Return on error status.
#define ROE(s) {                  \
  auto __status = (s);            \
  if (!__status) return __status; \
}                                 \
void()

static std::shared_ptr<arrow::Schema> input_schema() {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("input", arrow::uint8(), false)}),
      "input",
      fletcher::Mode::READ);
  return result;
}

static std::shared_ptr<arrow::DataType> output_type() {
  static auto result = arrow::list(arrow::field("item", arrow::uint64(), false));
  return result;
}

static std::shared_ptr<arrow::Schema> output_schema() {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("voltage", output_type(), false)}),
      "output",
      fletcher::Mode::WRITE);
  return result;
}

bool OpaeBatteryParserManager::PrepareInputBatches(const std::vector<RawJSONBuffer *> &buffers) {
  for (const auto &buf : buffers) {
    spdlog::info("Wrapping buffer (@:{:016X} s:{}) into Arrow RecordBatch.",
                 reinterpret_cast<uint64_t>(buf->data_),
                 buf->capacity_);
    auto wrapped = arrow::Buffer::Wrap(buf->data_, buf->capacity_);
    auto array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), buf->size_, wrapped);
    batches_in.push_back(arrow::RecordBatch::Make(input_schema(),
                                                  buf->capacity_,
                                                  {array}));
  }
  return true;
}

bool OpaeBatteryParserManager::PrepareOutputBatches() {
  for (size_t i = 0; i < num_parsers_; i++) {
    byte *offsets = nullptr;
    byte *values = nullptr;
    ROE(allocator.Allocate(opae_fixed_capacity, &offsets));
    ROE(allocator.Allocate(opae_fixed_capacity, &values));

    auto offset_buffer = arrow::Buffer::Wrap(offsets, opae_fixed_capacity);
    auto values_buffer = arrow::Buffer::Wrap(values, opae_fixed_capacity);
    auto values_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, values_buffer);
    auto list_array = std::make_shared<arrow::ListArray>(output_type(),
                                                         0,
                                                         offset_buffer,
                                                         values_array);
    std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
    raw_out_offsets.push_back(offsets);
    raw_out_values.push_back(values);
    batches_out.push_back(arrow::RecordBatch::Make(output_schema(), 0, arrays));
  }

  return true;
}

bool OpaeBatteryParserManager::Make(const OpaeBatteryOptions &opts,
                                    const std::vector<RawJSONBuffer *> &buffers,
                                    size_t num_parsers,
                                    std::shared_ptr<OpaeBatteryParserManager> *out) {
  auto result = std::make_shared<OpaeBatteryParserManager>();
  result->opts_ = opts;
  result->num_parsers_ = num_parsers;

  spdlog::info("Setting up OpaeBatteryParserManager for {} buffers.", buffers.size());

  ROE(result->PrepareInputBatches(buffers));
  ROE(result->PrepareOutputBatches());

  spdlog::info("Using {}", FLETCHER_PLATFORM);
  FLETCHER_ROE(fletcher::Platform::Make(FLETCHER_PLATFORM, &result->platform, false));

  // Fix AFU id
  std::stringstream ss;
  ss << std::hex << num_parsers;
  result->opts_.afu_id[strlen(OPAE_BATTERY_AFU_ID) - 1] = ss.str()[0];
  spdlog::info("AFU ID: {}", result->opts_.afu_id);
  const char *afu_id = result->opts_.afu_id.data();
  result->platform->init_data = &afu_id;
  FLETCHER_ROE(result->platform->Init());

  // Pull everything through the fletcher stack once.
  FLETCHER_ROE(fletcher::Context::Make(&result->context, result->platform));

  for (const auto &batch : result->batches_in) {
    FLETCHER_ROE(result->context->QueueRecordBatch(batch));
  }

  for (const auto &batch : result->batches_out) {
    FLETCHER_ROE(result->context->QueueRecordBatch(batch));
  }

  spdlog::info("Enabling context...");

  // Enable context.
  FLETCHER_ROE(result->context->Enable());
  // Construct kernel handler.
  result->kernel = std::make_shared<fletcher::Kernel>(result->context);
  // Write metadata.
  FLETCHER_ROE(result->kernel->WriteMetaData());

  // Workaround to store buffer device address.
  for (size_t i = 0; i < buffers.size(); i++) {
    result->h2d_addr_map[buffers[i]->data_] =
        result->context->device_buffer(i).device_address;
  }

  spdlog::info("Preparing parsers.");

  ROE(result->PrepareParsers());

  *out = result;

  return true;
}

bool OpaeBatteryParserManager::PrepareParsers() {
  for (size_t i = 0; i < num_parsers_; i++) {
    parsers_.push_back(std::make_shared<OpaeBatteryParser>(platform.get(),
                                                           context.get(),
                                                           kernel.get(),
                                                           &h2d_addr_map,
                                                           i,
                                                           num_parsers_,
                                                           raw_out_offsets[i],
                                                           raw_out_values[i],
                                                           &platform_mutex));
  }
  return true;
}

static bool WrapOutput(int32_t num_rows,
                       uint8_t *offsets,
                       uint8_t *values,
                       std::shared_ptr<arrow::Schema> schema,
                       std::shared_ptr<arrow::RecordBatch> *out) {
  auto ret = true;

  // +1 because the last value in offsets buffer is the next free index in the values
  // buffer.
  int32_t num_offsets = num_rows + 1;

  // Obtain the last value in the offsets buffer to know how many values there are.
  int32_t num_values = (reinterpret_cast<int32_t *>(offsets))[num_rows];

  size_t num_offset_bytes = num_offsets * sizeof(int32_t);
  size_t num_values_bytes = num_values * sizeof(uint64_t);

  auto values_buf = arrow::Buffer::Wrap(values, num_values_bytes);
  auto offsets_buf = arrow::Buffer::Wrap(offsets, num_offset_bytes);
  auto value_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, values_buf);
  auto offsets_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::int32(), num_offsets, offsets_buf);
  auto list_array = arrow::ListArray::FromArrays(*offsets_array, *value_array);

  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array.ValueOrDie()};
  *out = arrow::RecordBatch::Make(std::move(schema), num_rows, arrays);

  return true;
}

static fletcher::Status WriteMMIO(fletcher::Platform *platform,
                                  uint64_t offset,
                                  uint32_t value,
                                  size_t idx,
                                  std::string desc = "") {
  spdlog::info("{} writing {} to offset {} ({}) {}",
               idx,
               value,
               offset,
               64 + 4 * offset,
               desc);
  return platform->WriteMMIO(offset, value);
}

static fletcher::Status ReadMMIO(fletcher::Platform *platform,
                                 uint64_t offset,
                                 uint32_t *value,
                                 size_t idx,
                                 std::string desc = "") {
  spdlog::info("{} reading from offset {} ({}) {}",
               idx,
               offset,
               64 + 4 * offset,
               desc);
  return platform->ReadMMIO(offset, value);
}

bool OpaeBatteryParser::Parse(RawJSONBuffer *in, ParsedBuffer *out) {
  platform_mutex->lock();
  ParsedBuffer result;
  // rewrite the input last index because of opae limitations.
  FLETCHER_ROE(WriteMMIO(platform_,
                         input_lastidx_offset(idx_),
                         in->size_,
                         idx_,
                         "input last idx"));

  dau_t input_addr;
  input_addr.full = h2d_addr_map->at(in->data_);

  FLETCHER_ROE(WriteMMIO(platform_,
                         input_values_lo_offset(idx_),
                         input_addr.lo,
                         idx_,
                         "input values addr lo"));
  FLETCHER_ROE(WriteMMIO(platform_,
                         input_values_hi_offset(idx_),
                         input_addr.hi,
                         idx_,
                         "input values addr hi"));

  // Reset the kernel, start it, and poll until completion.
  // FLETCHER_ROE(kernel_->Reset());
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_),
                         4,
                         idx_,
                         "ctrl"));
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_), 0, idx_, "ctrl"));

  //FLETCHER_ROE(kernel_->Start());
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_),
                         1, idx_, "ctrl"));
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_), 0, idx_, "ctrl"));

  // FLETCHER_ROE(kernel_->PollUntilDone());
  bool done = false;
  uint32_t done_mask = 4;
  uint32_t done_status = 4;
  uint32_t status = 0;
  dau_t num_rows;

  while (!done) {
#ifndef NDEBUG
    ReadMMIO(platform_, status_offset(idx_), &status, idx_, "status");
    spdlog::info("Status value: {}", status);
    ReadMMIO(platform_, result_rows_offset_lo(idx_), &num_rows.lo, idx_, "rows lo");
    ReadMMIO(platform_, result_rows_offset_hi(idx_), &num_rows.hi, idx_, "rows hi");
    spdlog::info("{} number of rows: {}", idx_, num_rows.full);
    platform_mutex->unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    platform_mutex->lock();
#else
    platform_->ReadMMIO(status_offset(idx_), &status);
    platform_mutex->unlock();
    std::this_thread::sleep_for(std::chrono::microseconds(1));
    platform_mutex->lock();
#endif
    done = (status & done_mask) == done_status;
  }

  // FLETCHER_ROE(kernel->GetReturn(&num_rows.lo, &num_rows.hi));
  ReadMMIO(platform_, result_rows_offset_lo(idx_), &num_rows.lo, idx_, "rows lo");
  ReadMMIO(platform_, result_rows_offset_hi(idx_), &num_rows.hi, idx_, "rows hi");
  spdlog::info("{} number of rows: {}", idx_, num_rows.full);

  std::shared_ptr<arrow::RecordBatch> out_batch;
  ROE(WrapOutput(num_rows.full,
                 reinterpret_cast<uint8_t *>(raw_out_offsets),
                 reinterpret_cast<uint8_t *>(raw_out_values),
                 output_schema(),
                 &result.batch));

  result.parsed_bytes = in->size_;
  platform_mutex->unlock();
  *out = result;
  return true;
}
