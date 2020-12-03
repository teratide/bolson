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
#define OPAE_BATTERY_AFU_ID "9ca43fb0-c340-4908-b79b-5c89b4ef5eed"

namespace bolson::convert {

struct OPAEBatteryOptions {
  std::string afu_id = OPAE_BATTERY_AFU_ID;
  size_t seq_buffer_init_size = 1024;
  size_t str_buffer_init_size = 16 * 1024 * 1024;
  size_t input_capacity = 1000 * 1024 * 1024;
  size_t output_capacity_off = 1000 * 1024 * 1024;
  size_t output_capacity_val = 1000 * 1024 * 1024;
};

/// Class to support incremental building up of a RecordBatch from JSONQueueItems.
class OPAEBatteryIPCBuilder : public IPCBuilder {
 public:
  static auto Make(std::unique_ptr<OPAEBatteryIPCBuilder> *out,
                   size_t json_buffer_threshold = 5 * 1024 * 1024,
                   size_t batch_size_threshold = 5 * 1024 * 1024 - 100 * 1024,
                   const OPAEBatteryOptions &opts = OPAEBatteryOptions()) -> Status;

  auto AppendAsBatch(const illex::JSONQueueItem &item) -> Status override;
  auto FlushBuffered(putong::Timer<> *t) -> Status override;
 protected:
  explicit OPAEBatteryIPCBuilder(size_t json_buffer_threshold,
                                 size_t batch_size_threshold,
                                 std::string afu_id,
                                 size_t seq_buf_init_size,
                                 size_t str_buf_init_size)
      : IPCBuilder(json_buffer_threshold,
                   batch_size_threshold,
                   seq_buf_init_size,
                   str_buf_init_size),
        afu_id_(std::move(afu_id)) {}
 private:
  /// AFU ID
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

void ConvertBatteryWithOPAE(size_t json_threshold,
                            size_t batch_threshold,
                            illex::JSONQueue *in,
                            IpcQueue *out,
                            std::atomic<bool> *shutdown,
                            std::promise<std::vector<Stats>> &&stats_promise);

}