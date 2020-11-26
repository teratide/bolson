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

#include <arrow/ipc/api.h>

#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include <illex/queue.h>

#include "bolson/convert/convert.h"
#include "bolson/convert/fpga.h"
#include "bolson/utils.h"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                                             \
  auto _status = s;                                                                   \
  if (!_status.ok()) return Status(Error::FPGAError, "Fletcher: " + _status.message); \
}                                                                                     \
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

  // Prepare statistics
  std::vector<Stats> stats_result(1);

  std::shared_ptr<FPGABatchBuilder> fpga;
  Status status = FPGABatchBuilder::Make(&fpga);

  if (!status.ok()) {
    thread_timer.Stop();
    stats_result[0].thread_time = thread_timer.seconds();
    stats_result[0].status = status;
    stats.set_value(stats_result);
    shutdown->store(true);
    return;
  }

  SPDLOG_DEBUG("[convert] FPGA active.");

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
      stats_result[0].status = fpga->Append(json_item);
      if (!stats_result[0].status.ok()) {
        break;
      }
      convert_timer.Stop();
      // Check if threshold was reached.
      if (fpga->size() >= batch_threshold) {
        SPDLOG_DEBUG("Size of batches has reached threshold. Current size: {} bytes",
                     fpga->size());
        // We reached the threshold, so in the next iteration, just send off the
        // RecordBatch.
        attempt_dequeue = false;
      }
      // Update stats.
      stats_result[0].convert_time += convert_timer.seconds();
      stats_result[0].num_jsons++;
    } else {
      // If there is nothing in the queue or if the RecordBatch size threshold has been
      // reached, construct the IPC message to have this batch sent off. When the queue
      // is empty, this causes the latency to be low. When it is full, it still provides
      // good throughput.

      // There has to be at least one batch.
      if (!fpga->empty()) {
        // Finish the builder, delivering an IPC item.
        ipc_construct_timer.Start();
        IpcQueueItem ipc_item;
        auto status = fpga->Finish(&ipc_item);
        if (!status.ok()) {
          thread_timer.Stop();
          stats_result[0].thread_time = thread_timer.seconds();
          stats_result[0].status = status;
          stats.set_value(stats_result);
          SPDLOG_DEBUG("FPGA terminating with errors.");
          shutdown->store(true);
          return;
        }
        ipc_construct_timer.Stop();

        // Enqueue the IPC item.
        out->enqueue(ipc_item);

        SPDLOG_DEBUG("FPGA enqueued IPC message.");

        // Attempt to dequeue again.
        attempt_dequeue = true;

        // Update stats.
        stats_result[0].ipc_construct_time += ipc_construct_timer.seconds();
        stats_result[0].total_ipc_bytes += ipc_item.ipc->size();
        stats_result[0].num_ipc++;
      }
    }
  }
  thread_timer.Stop();
  stats_result[0].thread_time = thread_timer.seconds();
  stats.set_value(stats_result);
  SPDLOG_DEBUG("FPGA Terminating.");
}

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

static auto GetPageAlignedBuffer(uint8_t **buffer, size_t size) -> Status {
  size_t page_size = 2 * 1024 * 1024;
  // size_t page_size = sysconf(_SC_PAGESIZE);
  if (size % page_size != 0) {
    return Status(Error::FPGAError,
                  "Size " + std::to_string(size)
                      + " is not integer multiple of page size "
                      + std::to_string(page_size));
  }
  int pmar = posix_memalign(reinterpret_cast<void **>(buffer),
                            page_size,
                            size);
  if (pmar != 0) {
    return Status(Error::FPGAError, "Unable to allocate aligned buffer.");
  }
  std::memset(*buffer, '0', size);
  return Status::OK();
}

static auto PrepareInputBatch(std::shared_ptr<arrow::RecordBatch> *out,
                              uint8_t **buffer_raw,
                              size_t size) -> Status {
  BOLSON_ROE(GetPageAlignedBuffer(buffer_raw, size));
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

  BOLSON_ROE(GetPageAlignedBuffer(output_off_raw, offsets_size));
  BOLSON_ROE(GetPageAlignedBuffer(output_val_raw, values_size));

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

auto FPGABatchBuilder::Make(std::shared_ptr<FPGABatchBuilder> *out,
                            std::string afu_id,
                            size_t input_capacity,
                            size_t output_capacity_off,
                            size_t output_capacity_val) -> Status {
  auto
      result = std::shared_ptr<FPGABatchBuilder>(new FPGABatchBuilder(std::move(afu_id)));

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

static Status WrapOutput(int32_t num_rows,
                         uint8_t *offsets,
                         uint8_t *values,
                         std::shared_ptr<arrow::Schema> schema,
                         std::shared_ptr<arrow::RecordBatch> *out) {
  auto ret = Status::OK();

  // +1 because the last value in offsets buffer is the next free index in the values
  // buffer.
  int32_t num_offsets = num_rows + 1;

  // Obtain the last value in the offsets buffer to know how many values there are.
  int32_t num_values = reinterpret_cast<int32_t *>(offsets)[num_offsets];

  size_t num_offset_bytes = num_offsets * sizeof(int32_t);
  size_t num_values_bytes = num_values * sizeof(uint64_t);

  try {
    // Wrap data into arrow buffers
    auto offsets_buf = arrow::Buffer::Wrap(offsets, num_offset_bytes);
    auto values_buf = arrow::Buffer::Wrap(values, num_values_bytes);

    auto value_array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, values_buf);
    auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()),
                                                         num_rows,
                                                         offsets_buf,
                                                         value_array);

    std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
    *out = arrow::RecordBatch::Make(std::move(schema), num_rows, arrays);
  } catch (std::exception &e) {
    return Status(Error::ArrowError, e.what());
  }

  return Status::OK();
}

auto FPGABatchBuilder::Append(const illex::JSONQueueItem &item) -> Status {
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
  BOLSON_ROE(WrapOutput(num_rows,
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

void FPGABatchBuilder::Reset() {
  this->batches.clear();
  this->size_ = 0;
}

auto FPGABatchBuilder::Finish(IpcQueueItem *out) -> Status {
  // Set up a pointer for the combined batch.6
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> combined_batch_r;
  std::shared_ptr<arrow::RecordBatch> combined_batch;

  // Only wrap a table and combine chunks if there is more than one RecordBatch.
  if (batches.size() == 1) {
    combined_batch = batches[0];
  } else {
    auto packed_table = arrow::Table::FromRecordBatches(batches);
    if (!packed_table.ok()) {
      return Status(Error::ArrowError,
                    "Could not create table: " + packed_table.status().message());
    }
    auto combined_table = packed_table.ValueOrDie()->CombineChunks();
    if (!combined_table.ok()) {
      return Status(Error::ArrowError,
                    "Could not pack table chunks: " + combined_table.status().message());
    }
    auto table_reader = arrow::TableBatchReader(*combined_table.ValueOrDie());
    combined_batch_r = table_reader.Next();
    if (!combined_batch_r.ok()) {
      return Status(Error::ArrowError,
                    "Could not combine table chunks: "
                        + combined_batch_r.status().message());
    }
    combined_batch = combined_batch_r.ValueOrDie();

  }

  SPDLOG_DEBUG("Packed IPC batch: {}", combined_batch->ToString());

  //
  auto serialized = arrow::ipc::SerializeRecordBatch(*combined_batch,
                                                     arrow::ipc::IpcWriteOptions::Defaults());
  if (!serialized.ok()) {
    return Status(Error::ArrowError,
                  "Could not serialize batch: " + serialized.status().message());
  }

  // Convert to Arrow IPC message.
  auto ipc_item = IpcQueueItem{static_cast<size_t>(combined_batch->num_rows()),
                               serialized.ValueOrDie()};

  this->Reset();

  *out = ipc_item;

  return Status::OK();
}

}