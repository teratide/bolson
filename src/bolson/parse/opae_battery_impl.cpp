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

#include <arrow/ipc/api.h>
#include <putong/timer.h>

#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include "bolson/parse/parser.h"
#include "bolson/parse/opae_battery_impl.h"
#include "bolson/utils.h"

#define OPAE_BATTERY_INPUT_LASTIDX 5

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                                             \
  auto _status = s;                                                                   \
  if (!_status.ok()) return Status(Error::FPGAError, "Fletcher: " + _status.message); \
}                                                                                     \
void()

namespace bolson::parse {

// Because the input is a plain buffer but managed by Fletcher, we create some helper
// functions that make an Arrow RecordBatch out of it with a column of uint8 primitives.
// This is required to be able to pass it to Fletcher.

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

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

static auto GetHugePageBuffer(uint8_t **buffer, size_t size) -> Status {
  // TODO: describe this magic
  void *addr = mmap(nullptr,
                    size,
                    (PROT_READ | PROT_WRITE),
                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30u << 26)),
                    -1,
                    0);
  if (addr == MAP_FAILED) {
    return Status(Error::FPGAError, "Unable to allocate huge page buffer.");
  }
  memset(addr, 0, size);
  *buffer = (uint8_t *) addr;
  return Status::OK();
}

static auto PrepareInputBatch(std::shared_ptr<arrow::RecordBatch> *out,
                              uint8_t **buffer_raw,
                              size_t size) -> Status {
  BOLSON_ROE(GetHugePageBuffer(buffer_raw, size));
  auto buf = arrow::Buffer::Wrap(*buffer_raw, size);
  auto arr = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), size, buf);
  *out = arrow::RecordBatch::Make(input_schema(), size, {arr});
  return Status::OK();
}

static auto PrepareOutputBatch(std::shared_ptr<arrow::RecordBatch> *out,
                               uint8_t **output_off_raw,
                               uint8_t **output_val_raw,
                               size_t offsets_size,
                               size_t values_size) -> Status {

  BOLSON_ROE(GetHugePageBuffer(output_off_raw, offsets_size));
  BOLSON_ROE(GetHugePageBuffer(output_val_raw, values_size));

  auto offset_buffer = arrow::Buffer::Wrap(*output_off_raw, offsets_size);
  auto values_buffer = arrow::Buffer::Wrap(*output_val_raw, values_size);
  auto values_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, values_buffer);
  auto list_array = std::make_shared<arrow::ListArray>(output_type(),
                                                       0,
                                                       offset_buffer,
                                                       values_array);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  *out = arrow::RecordBatch::Make(output_schema(), 0, arrays);

  return Status::OK();
}

static auto CopyAndWrapOutput(int32_t num_rows,
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
    auto new_offs =
        std::shared_ptr(std::move(arrow::AllocateBuffer(num_offset_bytes).ValueOrDie()));
    auto new_vals =
        std::shared_ptr(std::move(arrow::AllocateBuffer(num_values_bytes).ValueOrDie()));

    std::memcpy(new_offs->mutable_data(), offsets, num_offset_bytes);
    std::memcpy(new_vals->mutable_data(), values, num_values_bytes);

    auto value_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, new_vals);
    auto offsets_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::int32(), num_offsets, new_offs);
    auto list_array = arrow::ListArray::FromArrays(*offsets_array, *value_array);

    std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array.ValueOrDie()};
    *out = arrow::RecordBatch::Make(std::move(schema), num_rows, arrays);
  } catch (std::exception &e) {
    return Status(Error::ArrowError, e.what());
  }

  return Status::OK();
}

}