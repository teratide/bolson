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
#include <utility>

#include <malloc.h>
#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <arrow/ipc/api.h>

#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include <illex/queue.h>

#include "bolson/convert/convert.h"
#include "bolson/convert/opae_battery.h"
#include "bolson/utils.h"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                                             \
  auto _status = s;                                                                   \
  if (!_status.ok()) return Status(Error::FPGAError, "Fletcher: " + _status.message); \
}                                                                                     \
void()

namespace bolson::convert {

#define SHUTDOWN_ON_FAILURE() if (!stats.status.ok()) { \
  thread_timer.Stop(); \
  stats.thread_time = thread_timer.seconds(); \
  stats_promise.set_value({stats}); \
  SPDLOG_DEBUG("FPGA terminating with errors."); \
  shutdown->store(true); \
  return; \
} void()

void ConvertBatteryWithOPAE(illex::JSONQueue *in,
                            IpcQueue *out,
                            std::atomic<bool> *shutdown,
                            size_t json_threshold,
                            size_t batch_threshold,
                            std::promise<std::vector<Stats>> &&stats_promise) {
  // Prepare some timers
  putong::Timer thread_timer(true);
  putong::Timer parse_timer;
  putong::Timer convert_timer;
  putong::Timer ipc_construct_timer;

  // Prepare statistics
  Stats stats;

  SPDLOG_DEBUG("[FPGA] Converter thread spawned.");

  // Prepare BatchBuilder
  std::shared_ptr<OPAEBatteryBatchBuilder> builder;
  stats.status = OPAEBatteryBatchBuilder::Make(&builder);
  SHUTDOWN_ON_FAILURE();

  // Reserve a queue item
  illex::JSONQueueItem json_item;

  // Triggers for IPC construction
  size_t batches_size = 0;
  bool attempt_dequeue = true;

  // Loop until thread is stopped.
  while (!shutdown->load()) {
    // Attempt to dequeue an item if the size of the current RecordBatches is not larger
    // than the limit.
    if (attempt_dequeue
        && in->wait_dequeue_timed(json_item, std::chrono::microseconds(1))) {
      SPDLOG_DEBUG("[FPGA] Builder status: {}", builder->ToString());
      // There is a JSON.
      SPDLOG_DEBUG("[FPGA] popping JSON: {}.", json_item.string);
      // Buffer the JSON.
      stats.status = builder->Buffer(json_item);
      SHUTDOWN_ON_FAILURE();

      // Check if we need to flush the JSON buffer.
      if (builder->jsons_buffered() >= json_threshold) {
        SPDLOG_DEBUG("[FPGA] Builder JSON buffer reached threshold.");
        // Add to stats
        stats.num_jsons += builder->jsons_buffered();
        stats.num_json_bytes += builder->bytes_buffered();
        convert_timer.Start();
        // Flush the buffer
        stats.status = builder->FlushBuffered(&parse_timer);
        SHUTDOWN_ON_FAILURE();
        convert_timer.Stop();
        stats.parse_time += parse_timer.seconds();
        stats.convert_time += convert_timer.seconds();
      }

      // Check if either of the threshold was reached.
      if ((builder->size() >= batch_threshold)) {
        SPDLOG_DEBUG("[FPGA] Size of batches has reached threshold. "
                     "Current size: {} bytes", builder->size());
        // We reached the threshold, so in the next iteration, just send off the
        // RecordBatch.
        attempt_dequeue = false;
      }
    } else {
      // If there is nothing in the queue or if the RecordBatch size threshold has been
      // reached, construct the IPC message to have this batch sent off. When the queue
      // is empty, this causes the latency to be low. When it is full, it still provides
      // good throughput.

      SPDLOG_DEBUG("[FPGA] Nothing left in queue.");
      // If there is still something in the buffer, flush it.
      if (builder->jsons_buffered() > 0) {
        SPDLOG_DEBUG("[FPGA] Flushing JSON buffer.");
        // Add to stats
        stats.num_jsons += builder->jsons_buffered();
        stats.num_json_bytes += builder->bytes_buffered();
        convert_timer.Start();
        // Flush the buffer
        stats.status = builder->FlushBuffered(&parse_timer);
        SHUTDOWN_ON_FAILURE();
        convert_timer.Stop();
        stats.parse_time += parse_timer.seconds();
        stats.convert_time += convert_timer.seconds();
      }

      // There has to be at least one batch.
      if (!builder->empty()) {
        SPDLOG_DEBUG("[FPGA] Flushing Batch buffer.");
        // Finish the builder, delivering an IPC item.
        ipc_construct_timer.Start();
        IpcQueueItem ipc_item;
        stats.status = builder->Finish(&ipc_item);
        SHUTDOWN_ON_FAILURE();
        ipc_construct_timer.Stop();

        // Enqueue the IPC item.
        out->enqueue(ipc_item);

        SPDLOG_DEBUG("[FPGA] Enqueued IPC message.");

        // Attempt to dequeue again.
        attempt_dequeue = true;

        // Update stats.
        stats.ipc_construct_time += ipc_construct_timer.seconds();
        stats.total_ipc_bytes += ipc_item.ipc->size();
        stats.num_ipc++;
      }
    }
  }
  thread_timer.Stop();
  stats.thread_time = thread_timer.seconds();
  stats_promise.set_value({stats});
  SPDLOG_DEBUG("[FPGA] Terminating.");
}

#undef SHUTDOWN_ON_FAILURE

// Because the input is a plain buffer but managed by Fletcher, we create some helper
// functions that make an Arrow RecordBatch out of it with a column of uint8 primitives.
// This is required to be able to pass it to Fletcher.

static auto input_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("input",
                                   arrow::uint8(),
                                   false)}),
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
  void *addr;
  addr = mmap((void *) (0x0UL),
              size,
              (PROT_READ | PROT_WRITE),
              (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30 << 26)),
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

auto OPAEBatteryBatchBuilder::Make(std::shared_ptr<OPAEBatteryBatchBuilder> *out,
                                   std::string afu_id,
                                   size_t input_capacity,
                                   size_t output_capacity_off,
                                   size_t output_capacity_val,
                                   size_t seq_buffer_init_size,
                                   size_t str_buffer_init_size) -> Status {
  auto
      result = std::shared_ptr<OPAEBatteryBatchBuilder>(new OPAEBatteryBatchBuilder(std::move(afu_id),
                                                                                    seq_buffer_init_size,
                                                                                    str_buffer_init_size));

  // Prepare input and output batch.
  BOLSON_ROE(PrepareInputBatch(&result->input, &result->input_raw, input_capacity));
  BOLSON_ROE(PrepareOutputBatch(&result->output,
                                &result->output_off_raw,
                                &result->output_val_raw,
                                output_capacity_off,
                                output_capacity_val));

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

  *out = result;

  return Status::OK();
}

#define INPUT_LASTIDX 5

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

auto OPAEBatteryBatchBuilder::AppendAsBatch(const illex::JSONQueueItem &item) -> Status {
  SPDLOG_DEBUG("Appending JSON as RecordBatch");

  // Copy the JSON data onto the buffer.
  std::memcpy(this->input_raw, item.string.data(), item.string.length());

  FLETCHER_ROE(this->platform->WriteMMIO(INPUT_LASTIDX,
                                         static_cast<int32_t>(item.string.length())));
  FLETCHER_ROE(this->kernel->Reset());
  FLETCHER_ROE(this->kernel->Start());
  FLETCHER_ROE(this->kernel->PollUntilDone());

  dau_t result;
  FLETCHER_ROE(this->kernel->GetReturn(&result.lo, &result.hi));
  uint64_t num_rows = result.full;

  std::shared_ptr<arrow::RecordBatch> batch_result;
  BOLSON_ROE(CopyAndWrapOutput(num_rows,
                               output_off_raw,
                               output_val_raw,
                               output_schema(),
                               &batch_result));

  // Construct the column for the sequence number.
  std::shared_ptr<arrow::Array> seq;
  arrow::UInt64Builder seq_builder;
  ARROW_ROE(seq_builder.Append(item.seq));
  ARROW_ROE(seq_builder.Finish(&seq));

  // Add the column to the batch.
  auto batch_with_seq_result = batch_result->AddColumn(0, SeqField(), seq);
  ARROW_ROE(batch_with_seq_result.status());
  auto batch_with_seq = batch_with_seq_result.ValueOrDie();

  this->size_ += GetBatchSize(batch_with_seq);
  this->batches.push_back(std::move(batch_with_seq));

  return Status::OK();
}

auto OPAEBatteryBatchBuilder::FlushBuffered(putong::Timer<> *t) -> Status {
  if (str_buffer->size() > 0) {
    SPDLOG_DEBUG("Flushing: {}",
                 std::string(reinterpret_cast<const char *>(str_buffer->data()),
                             str_buffer->size()));

    // Copy the JSON data onto the buffer.
    std::memcpy(this->input_raw, this->str_buffer->data(), this->str_buffer->size());

    FLETCHER_ROE(this->platform->WriteMMIO(INPUT_LASTIDX,
                                           static_cast<int32_t>(this->str_buffer->size())));
    FLETCHER_ROE(this->kernel->Reset());
    t->Start();
    FLETCHER_ROE(this->kernel->Start());
    FLETCHER_ROE(this->kernel->PollUntilDone());
    t->Stop();

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

    ARROW_ROE(this->str_buffer->Resize(0));
  }

  return Status::OK();
}

}