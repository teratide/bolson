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

#include <cstdlib>

#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>

#include <illex/queue.h>

#include "bolson/convert/convert.h"
#include "bolson/convert/fpga.h"

#define FPGA_PLATFORM "opae"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                              \
  auto _status = s;                                                    \
  if (!_status.ok()) return Status(Error::FPGAError, _status.message); \
}                                                                      \
void()

namespace bolson::convert {

void ConvertWithFPGA(illex::JSONQueue *in,
                     IpcQueue *out,
                     std::atomic<bool> *shutdown,
                     size_t batch_threshold,
                     std::promise<std::vector<Stats>> &&stats) {
  // Prepare some timers
  putong::Timer thread_timer(true);
  putong::Timer convert_timer;
  putong::Timer ipc_construct_timer;

  SPDLOG_DEBUG("[convert] FPGA active.");

  // Prepare statistics
  std::vector<Stats> stats_result;

  // Reserve a queue item
  illex::JSONQueueItem json_item;

  bool attempt_dequeue = true;

  while (!shutdown->load()) {
    if (attempt_dequeue
        && in->wait_dequeue_timed(json_item, std::chrono::microseconds(1))) {
      // There is a JSON.
      SPDLOG_DEBUG("[convert] FPGA popping JSON: {}.", json_item.string);
      // Convert the JSON.
      convert_timer.Start();

    }
  }
}

// Because the input is a plain buffer but managed by Fletcher, we create some helper
// functions that make an Arrow RecordBatch out of it with a column of uint8 primitives.
// This is required to be able to pass it to Fletcher.

static auto input_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = arrow::schema({arrow::field("input", arrow::uint8(), false)});
  return result;
}

static auto output_type() -> std::shared_ptr<arrow::DataType> {
  static auto result = arrow::list(arrow::field("item", arrow::uint64(), false));
  return result;
}

static auto
output_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = arrow::schema({arrow::field("voltage", output_type(), false)});
  return result;
}

static auto AsInputBuffer(const std::string &str) -> std::shared_ptr<arrow::RecordBatch> {
  auto buf = arrow::Buffer::Wrap(str.data(), str.length());
  auto arr = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), str.length(), buf);
  return arrow::RecordBatch::Make(input_schema(), str.length(), {arr});
}

static auto GetPageAlignedBuffer(uint8_t **buffer, size_t size) -> Status {
  int pmar = posix_memalign(reinterpret_cast<void **>(buffer),
                            sysconf(_SC_PAGESIZE),
                            size);
  if (pmar == 0) {
    return Status(Error::FPGAError, "Unable to allocate aligned buffer.");
  }
  std::memset(*buffer, '0', size);
  return Status::OK();
}

static auto PrepareOutputBatch(std::shared_ptr<arrow::RecordBatch> *out,
                               size_t offsets_size,
                               size_t values_size) -> Status {

  uint8_t *offsets;
  uint8_t *values;
  BOLSON_ROE(GetPageAlignedBuffer(&offsets, offsets_size));
  BOLSON_ROE(GetPageAlignedBuffer(&values, values_size));

  auto offset_buffer = std::make_shared<arrow::Buffer>(offsets, offsets_size);

  auto values_buffer = std::make_shared<arrow::Buffer>(values, values_size);
  auto values_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, values_buffer);

  auto list_array = std::make_shared<arrow::ListArray>(output_type(),
                                                       0,
                                                       offset_buffer,
                                                       values_array);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  auto output_batch = arrow::RecordBatch::Make(output_schema(), 0, arrays);

  return Status::OK();
}

auto FPGABatchBuilder::Make(std::shared_ptr<FPGABatchBuilder> *out,
                            std::string afu_id,
                            size_t input_capacity,
                            size_t output_capacity_off,
                            size_t output_capacity_val) -> Status {
  auto result = std::shared_ptr<FPGABatchBuilder>(new FPGABatchBuilder(afu_id));

  FLETCHER_ROE(fletcher::Platform::Make(FPGA_PLATFORM, &result->platform, false));
  FLETCHER_ROE(fletcher::Context::Make(&result->context, result->platform));

  // Prepare input batch
  std::shared_ptr<arrow::RecordBatch>
      input_batch = AsInputBuffer(std::string(input_capacity, '\0'));

  // Prepare output batch
  std::shared_ptr<arrow::RecordBatch> output_batch;
  BOLSON_ROE(PrepareOutputBatch(&output_batch, output_capacity_off, output_capacity_val));

  // Queue batches.
  FLETCHER_ROE(result->context->QueueRecordBatch(input_batch));
  FLETCHER_ROE(result->context->QueueRecordBatch(output_batch));

  return Status::OK();
}

}