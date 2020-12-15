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

#include <arrow/api.h>
#include <fletcher/api.h>

#include "bolson/buffer/opae_allocator.h"
#include "bolson/stream.h"

#define OPAE_BATTERY_AFU_ID "9ca43fb0-c340-4908-b79b-5c89b4ef5eed"

namespace bolson::parse {

struct OpaeBatteryOptions {
  std::string afu_id = OPAE_BATTERY_AFU_ID;
  size_t output_capacity_off = 1000 * 1024 * 1024;
  size_t output_capacity_val = 1000 * 1024 * 1024;
};

class OpaeBatteryParser : public Parser {
 public:
  static auto Make(const OpaeBatteryOptions &opts,
                   std::shared_ptr<OpaeBatteryParser> *out) -> Status;
  auto Initialize(std::vector<illex::RawJSONBuffer *> buffers) -> Status override;
  auto Parse(illex::RawJSONBuffer *in, ParsedBuffer *out) -> Status override;
 private:
  explicit OpaeBatteryParser(const OpaeBatteryOptions &opts) : opts_(opts) {}

  OpaeBatteryOptions opts_;

  buffer::OpaeAllocator allocator;

  std::unordered_map<const std::byte *, da_t> buffer_addr_map;

  std::shared_ptr<arrow::RecordBatch> batch_in = nullptr;
  std::shared_ptr<arrow::RecordBatch> batch_out = nullptr;
  std::byte *out_offsets = nullptr;
  std::byte *out_values = nullptr;

  std::shared_ptr<fletcher::Platform> platform = nullptr;
  std::shared_ptr<fletcher::Context> context = nullptr;
  std::shared_ptr<fletcher::Kernel> kernel = nullptr;

  auto PrepareOutputBatch(size_t offsets_capacity,
                          size_t values_capacity) -> Status;
  auto PrepareInputBatch(const uint8_t *buffer_raw, size_t size) -> Status;

};

}