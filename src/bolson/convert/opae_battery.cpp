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

#include <illex/queue.h>

#include "bolson/convert/convert.h"
#include "bolson/convert/opae_battery.h"
#include "bolson/utils.h"

#define OPAE_BATTERY_INPUT_LASTIDX 5

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                                             \
  auto _status = s;                                                                   \
  if (!_status.ok()) return Status(Error::FPGAError, "Fletcher: " + _status.message); \
}                                                                                     \
void()

namespace bolson::convert {

void ConvertBatteryWithOPAE(size_t json_threshold,
                            size_t batch_threshold,
                            illex::JSONQueue *in,
                            IpcQueue *out,
                            illex::LatencyTracker *lat_tracker,
                            std::atomic<bool> *shutdown,
                            std::promise<std::vector<Stats>> &&stats_promise) {
  std::promise<Stats> stats;
  auto future_stats = stats.get_future();
  std::unique_ptr<OPAEBatteryIPCBuilder> builder;
  auto status = OPAEBatteryIPCBuilder::Make(&builder);
  if (!status.ok()) {
    auto err_stats = Stats();
    err_stats.status = status;
    stats_promise.set_value({err_stats});
    shutdown->store(true);
    return;
  } else {
    Convert(0, std::move(builder), in, out, lat_tracker, shutdown, std::move(stats));
  }
  std::vector<Stats> result = {future_stats.get()};
  stats_promise.set_value(result);
}

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

auto OPAEBatteryIPCBuilder::Make(std::unique_ptr<OPAEBatteryIPCBuilder> *out,
                                 size_t json_buffer_threshold,
                                 size_t batch_size_threshold,
                                 const OPAEBatteryOptions &opts) -> Status {

  auto result = std::unique_ptr<OPAEBatteryIPCBuilder>(
      new OPAEBatteryIPCBuilder(json_buffer_threshold,
                                batch_size_threshold,
                                opts.afu_id,
                                opts.seq_buffer_init_size,
                                opts.str_buffer_init_size)
  );

  // Prepare input and output batch.
  BOLSON_ROE(PrepareInputBatch(&result->input, &result->input_raw, opts.input_capacity));
  BOLSON_ROE(PrepareOutputBatch(&result->output,
                                &result->output_off_raw,
                                &result->output_val_raw,
                                opts.output_capacity_off,
                                opts.output_capacity_val));

  FLETCHER_ROE(fletcher::Platform::Make(FPGA_PLATFORM, &result->platform, false));

  char *fu_id = result->afu_id_.data();
  result->platform->init_data = &fu_id;
  FLETCHER_ROE(result->platform->Init());

  FLETCHER_ROE(fletcher::Context::Make(&result->context, result->platform));

  // Queue batches.
  FLETCHER_ROE(result->context->QueueRecordBatch(result->input));
  FLETCHER_ROE(result->context->QueueRecordBatch(result->output));

  // Enable context.
  FLETCHER_ROE(result->context->Enable());

  // Construct kernel handler.
  result->kernel = std::make_shared<fletcher::Kernel>(result->context);
  FLETCHER_ROE(result->kernel->WriteMetaData());

  *out = std::move(result);

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
  int32_t num_values = reinterpret_cast<int32_t *>(offsets)[num_offsets];

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
    auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()),
                                                         num_rows,
                                                         new_offs,
                                                         value_array);

    std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
    *out = arrow::RecordBatch::Make(std::move(schema), num_rows, arrays);
  } catch (std::exception &e) {
    return Status(Error::ArrowError, e.what());
  }

  return Status::OK();
}

auto OPAEBatteryIPCBuilder::FlushBuffered(putong::Timer<> *t,
                                          illex::LatencyTracker *lat_tracker) -> Status {
  if (str_buffer->size() > 0) {

    // Mark time point buffer is flushed into the parser
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BUFFER_FLUSH, illex::Timer::now());
    }

    SPDLOG_DEBUG("Flushing: {}",
                 std::string(reinterpret_cast<const char *>(str_buffer->data()),
                             str_buffer->size()));

    // Copy the JSON data onto the buffer.
    std::memcpy(this->input_raw, this->str_buffer->data(), this->str_buffer->size());

    FLETCHER_ROE(this->platform->WriteMMIO(OPAE_BATTERY_INPUT_LASTIDX,
                                           static_cast<int32_t>(this->str_buffer->size())));
    FLETCHER_ROE(this->kernel->Reset());
    t->Start();
    FLETCHER_ROE(this->kernel->Start());
    FLETCHER_ROE(this->kernel->PollUntilDone());
    t->Stop();

    // Mark time point buffer is parsed
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BUFFER_PARSED, illex::Timer::now());
    }

    dau_t result;
    FLETCHER_ROE(this->kernel->GetReturn(&result.lo, &result.hi));
    uint64_t num_rows = result.full;

    std::shared_ptr<arrow::RecordBatch> out_batch;
    BOLSON_ROE(CopyAndWrapOutput(num_rows,
                                 output_off_raw,
                                 output_val_raw,
                                 output_schema(),
                                 &out_batch));

    // Construct the column for the sequence number.
    std::shared_ptr<arrow::Array> seq;
    ARROW_ROE(seq_builder->Finish(&seq));

    // Add the column to the batch.
    auto batch_with_seq_result = out_batch->AddColumn(0, SeqField(), seq);
    ARROW_ROE(batch_with_seq_result.status());
    auto batch_with_seq = batch_with_seq_result.ValueOrDie();

    this->size_ += GetBatchSize(batch_with_seq);
    this->batches.push_back(std::move(batch_with_seq));

    // Mark time point sequence numbers are added.
    for (const auto &s : this->lat_tracked_seq_in_buffer) {
      lat_tracker->Put(s, BOLSON_LAT_BATCH_CONSTRUCTED, illex::Timer::now());
    }

    this->lat_tracked_seq_in_buffer.clear();
    ARROW_ROE(this->str_buffer->Resize(0));
  }

  return Status::OK();
}

}