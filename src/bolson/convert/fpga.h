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

#pragma once

#include <memory>
#include <utility>

#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include "bolson/stream.h"
#include "bolson/convert/cpu.h"

#define FPGA_PLATFORM "opae"
#define AFU_ID "9ca43fb0-c340-4908-b79b-5c89b4ef5eed"

namespace bolson::convert {

/// Class to support incremental building up of a RecordBatch from JSONQueueItems.
class FPGABatchBuilder : public BatchBuilder {
 public:
  static auto Make(std::shared_ptr<FPGABatchBuilder> *out,
                   std::string afu_id = AFU_ID,
                   size_t input_capacity = 2 * 1024 * 1024,
                   size_t output_capacity_off = 2 * 1024 * 1024,
                   size_t output_capacity_val = 2 * 1024 * 1024,
                   size_t seq_buffer_init_size = 1024,
                   size_t str_buffer_init_size = 16 * 1024 * 1024) -> Status;

  auto AppendAsBatch(const illex::JSONQueueItem &item) -> Status override;
  auto FlushBuffered() -> Status override;
 protected:
  explicit FPGABatchBuilder(std::string afu_id,
                            size_t seq_buf_init_size,
                            size_t str_buf_init_size)
      : BatchBuilder(seq_buf_init_size, str_buf_init_size),
        afu_id_(std::move(afu_id)) {}
 private:
  uint64_t result_counter = 0;
  std::string afu_id_;

  std::shared_ptr<fletcher::Platform> platform;
  std::shared_ptr<fletcher::Context> context;
  std::shared_ptr<fletcher::Kernel> kernel;

  std::shared_ptr<arrow::RecordBatch> input;
  uint8_t *input_raw = nullptr;
  std::shared_ptr<arrow::RecordBatch> output;
  uint8_t *output_off_raw = nullptr;
  uint8_t *output_val_raw = nullptr;
};

void ConvertWithFPGA(illex::JSONQueue *in,
                     IpcQueue *out,
                     std::atomic<bool> *shutdown,
                     size_t json_threshold,
                     size_t batch_threshold,
                     std::promise<std::vector<Stats>> &&stats_promise);

}