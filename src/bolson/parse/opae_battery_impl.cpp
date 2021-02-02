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

#include "bolson/parse/opae_battery_impl.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <fletcher/context.h>
#include <fletcher/fletcher.h>
#include <fletcher/kernel.h>
#include <fletcher/platform.h>
#include <putong/timer.h>

#include <chrono>
#include <thread>
#include <utility>

#include "bolson/latency.h"
#include "bolson/log.h"
#include "bolson/parse/parser.h"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s)                                                 \
  {                                                                     \
    auto __status = (s);                                                \
    if (!__status.ok())                                                 \
      return Status(Error::OpaeError, "Fletcher: " + __status.message); \
  }                                                                     \
  void()

namespace bolson::parse {

auto OpaeBatteryParserManager::PrepareInputBatches(
    const std::vector<illex::JSONBuffer*>& buffers) -> Status {
  for (const auto& buf : buffers) {
    auto wrapped = arrow::Buffer::Wrap(buf->data(), buf->capacity());
    auto array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), buf->capacity(), wrapped);
    batches_in.push_back(arrow::RecordBatch::Make(OpaeBatteryParser::input_schema(),
                                                  buf->capacity(), {array}));
  }
  return Status::OK();
}

auto OpaeBatteryParserManager::PrepareOutputBatches() -> Status {
  for (size_t i = 0; i < num_parsers_; i++) {
    std::byte* offsets = nullptr;
    std::byte* values = nullptr;
    BOLSON_ROE(allocator.Allocate(buffer::OpaeAllocator::opae_fixed_capacity, &offsets));
    BOLSON_ROE(allocator.Allocate(buffer::OpaeAllocator::opae_fixed_capacity, &values));

    auto offset_buffer =
        arrow::Buffer::Wrap(offsets, buffer::OpaeAllocator::opae_fixed_capacity);
    auto values_buffer =
        arrow::Buffer::Wrap(values, buffer::OpaeAllocator::opae_fixed_capacity);
    auto values_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, values_buffer);
    auto list_array = std::make_shared<arrow::ListArray>(OpaeBatteryParser::output_type(),
                                                         0, offset_buffer, values_array);
    std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
    raw_out_offsets.push_back(offsets);
    raw_out_values.push_back(values);
    batches_out.push_back(
        arrow::RecordBatch::Make(OpaeBatteryParser::output_schema(), 0, arrays));
  }

  return Status::OK();
}

auto OpaeBatteryParserManager::Make(const OpaeBatteryOptions& opts,
                                    const std::vector<illex::JSONBuffer*>& buffers,
                                    size_t num_parsers,
                                    std::shared_ptr<OpaeBatteryParserManager>* out)
    -> Status {
  if (num_parsers != buffers.size()) {
    return Status(Error::OpaeError,
                  "OpaeBatteryParser implementation requires number of buffers and "
                  "parsers to be equal.");
  }
  // Attempt to auto derive AFU ID if not supplied.
  std::string afu_id_str;
  if (opts.afu_id.empty()) {
    if (num_parsers > 255) {
      return Status(Error::OpaeError,
                    "Number of parsers larger than 255 is not supported.");
    }
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << std::hex << num_parsers;
    // AFU ID from the hardware design script.
    afu_id_str = "9ca43fb0-c340-4908-b79b-5c89b4ef5e" + ss.str();
  } else {
    afu_id_str = opts.afu_id;
  }
  SPDLOG_DEBUG("OpaeBatteryParserManager | Using AFU ID: {}", afu_id_str);

  // Create and set up manager
  auto result = std::make_shared<OpaeBatteryParserManager>();
  result->opts_ = opts;
  result->num_parsers_ = num_parsers;

  SPDLOG_DEBUG("OpaeBatteryParserManager | Setting up for {} buffers.", buffers.size());

  BOLSON_ROE(result->PrepareInputBatches(buffers));
  BOLSON_ROE(result->PrepareOutputBatches());

  FLETCHER_ROE(fletcher::Platform::Make("opae", &result->platform, false));

  result->opts_.afu_id = afu_id_str;
  char* afu_id = result->opts_.afu_id.data();
  result->platform->init_data = &afu_id;

  // Initialize the platform.
  FLETCHER_ROE(result->platform->Init());

  // Pull everything through the fletcher stack once.
  FLETCHER_ROE(fletcher::Context::Make(&result->context, result->platform));

  for (const auto& batch : result->batches_in) {
    FLETCHER_ROE(result->context->QueueRecordBatch(batch));
  }

  for (const auto& batch : result->batches_out) {
    FLETCHER_ROE(result->context->QueueRecordBatch(batch));
  }

  // Enable context.
  FLETCHER_ROE(result->context->Enable());
  // Construct kernel handler.
  result->kernel = std::make_shared<fletcher::Kernel>(result->context);
  // Write metadata.
  FLETCHER_ROE(result->kernel->WriteMetaData());

  SPDLOG_DEBUG("OpaeBatteryParserManager | OPAE host address / device address map:");

  // Workaround to obtain buffer device address.
  for (size_t i = 0; i < result->context->num_buffers(); i++) {
    const auto* ha = reinterpret_cast<const std::byte*>(
        result->context->device_buffer(i).host_address);
    auto da = result->context->device_buffer(i).device_address;
    result->h2d_addr_map[ha] = da;
    SPDLOG_DEBUG("  H: 0x{:016X} <--> D: 0x{:016X}", reinterpret_cast<uint64_t>(ha), da);
  }

  SPDLOG_DEBUG("OpaeBatteryParserManager | Preparing parsers.");

  BOLSON_ROE(result->PrepareParsers());

  *out = result;

  return Status::OK();
}

auto OpaeBatteryParserManager::PrepareParsers() -> Status {
  for (size_t i = 0; i < num_parsers_; i++) {
    parsers_.push_back(std::make_shared<OpaeBatteryParser>(
        platform.get(), context.get(), kernel.get(), &h2d_addr_map, i, num_parsers_,
        raw_out_offsets[i], raw_out_values[i], &platform_mutex));
  }
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

static inline auto WriteMMIO(fletcher::Platform* platform, uint64_t offset,
                             uint32_t value, size_t idx, const std::string& desc = "")
    -> fletcher::Status {
  SPDLOG_DEBUG("Thread {:2} | MMIO WRITE 0x{:08X} --> [off:{:4}] [@ 0x{:04X}] {}", idx,
               value, offset, 64 + 4 * offset, desc);
  return platform->WriteMMIO(offset, value);
}

static inline auto ReadMMIO(fletcher::Platform* platform, uint64_t offset,
                            uint32_t* value, size_t idx, const std::string& desc = "")
    -> fletcher::Status {
  auto status = platform->ReadMMIO(offset, value);
  SPDLOG_DEBUG("Thread {:2} | MMIO READ  0x{:08X} <-- [off:{:4}] [@ 0x{:04X}] {}", idx,
               *value, offset, 64 + 4 * offset, desc);
  return status;
}

auto OpaeBatteryParser::Parse(illex::JSONBuffer* in, ParsedBatch* out) -> Status {
  platform_mutex->lock();
  SPDLOG_DEBUG("Thread {:2} | Obtained platform lock", idx_);
  SPDLOG_DEBUG("Thread {:2} | Attempting to parse buffer:\n {}", idx_,
               ToString(*in, false));

  // Reset the kernel, start it, and poll until completion.
  // FLETCHER_ROE(kernel_->Reset());
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_), ctrl_reset, idx_, "ctrl"));
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_), 0, idx_, "ctrl"));

  // rewrite the input last index because of opae limitations.
  FLETCHER_ROE(WriteMMIO(platform_, input_lastidx_offset(idx_), in->size(), idx_,
                         "input last idx"));

  dau_t input_addr;
  input_addr.full = h2d_addr_map->at(in->data());

  FLETCHER_ROE(WriteMMIO(platform_, input_values_lo_offset(idx_), input_addr.lo, idx_,
                         "in values addr lo"));
  FLETCHER_ROE(WriteMMIO(platform_, input_values_hi_offset(idx_), input_addr.hi, idx_,
                         "in values addr hi"));

  // FLETCHER_ROE(kernel_->Start());
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_), ctrl_start, idx_, "ctrl"));
  FLETCHER_ROE(WriteMMIO(platform_, ctrl_offset(idx_), 0, idx_, "ctrl"));

  // FLETCHER_ROE(kernel_->PollUntilDone());
  bool done = false;
  uint32_t status = 0;
  dau_t num_rows;

  do {
#ifndef NDEBUG
    ReadMMIO(platform_, status_offset(idx_), &status, idx_, "status");
    done = (status & stat_done) == stat_done;

    // Obtain the result for debugging.
    ReadMMIO(platform_, result_rows_offset_lo(idx_), &num_rows.lo, idx_, "rows lo");
    ReadMMIO(platform_, result_rows_offset_hi(idx_), &num_rows.hi, idx_, "rows hi");
    SPDLOG_DEBUG("Thread {:2} | Number of rows: {}", idx_, num_rows.full);
    platform_mutex->unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    platform_mutex->lock();
#else
    platform_->ReadMMIO(status_offset(idx_), &status);
    done = (status & stat_done) == stat_done;

    platform_mutex->unlock();
    std::this_thread::sleep_for(std::chrono::microseconds(BOLSON_QUEUE_WAIT_US));
    platform_mutex->lock();
#endif
  } while (!done);

  ReadMMIO(platform_, result_rows_offset_lo(idx_), &num_rows.lo, idx_, "rows lo");
  ReadMMIO(platform_, result_rows_offset_hi(idx_), &num_rows.hi, idx_, "rows hi");
  platform_mutex->unlock();

  std::shared_ptr<arrow::RecordBatch> out_batch;
  BOLSON_ROE(WrapOutput(num_rows.full, reinterpret_cast<uint8_t*>(raw_out_offsets),
                        reinterpret_cast<uint8_t*>(raw_out_values), output_schema(),
                        &out_batch));

  SPDLOG_DEBUG("Thread {:2} | Parsing {} JSONs completed.", idx_, out_batch->num_rows());

  ParsedBatch result;
  result.batch = out_batch;
  result.seq_range = in->range();

  *out = result;

  return Status::OK();
}

auto OpaeBatteryParser::input_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("input", arrow::uint8(), false)}), "input",
      fletcher::Mode::READ);
  return result;
}

auto OpaeBatteryParser::output_type() -> std::shared_ptr<arrow::DataType> {
  static auto result = arrow::list(arrow::field("item", arrow::uint64(), false));
  return result;
}

auto OpaeBatteryParser::output_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("voltage", output_type(), false)}), "output",
      fletcher::Mode::WRITE);
  return result;
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

}  // namespace bolson::parse
