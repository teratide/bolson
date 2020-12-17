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

#define OPAE_BATTERY_AFU_ID "9ca43fb0-c340-4908-b79b-5c89b4ef5ee0"

namespace bolson::parse {

using AddrMap = std::unordered_map<const std::byte *, da_t>;

class OpaeBatteryParser : public Parser {
 public:
  OpaeBatteryParser(fletcher::Platform *platform,
                    fletcher::Context *context,
                    fletcher::Kernel *kernel,
                    AddrMap *addr_map,
                    size_t parser_idx,
                    size_t num_parsers,
                    std::byte *raw_out_offsets,
                    std::byte *raw_out_values,
                    std::mutex *platform_mutex)
      : platform_(platform),
        context_(context),
        kernel_(kernel),
        h2d_addr_map(addr_map),
        idx_(parser_idx),
        num_parsers(num_parsers),
        raw_out_offsets(raw_out_offsets),
        raw_out_values(raw_out_values),
        platform_mutex(platform_mutex) {}

  auto Parse(illex::RawJSONBuffer *in, ParsedBuffer *out) -> Status override;

 private:
  // Fletcher default regs (unused):
  // 0 control
  // 1 status
  // 2 return lo
  // 3 return hi
  static constexpr size_t default_regs = 4;

  // Arrow input ranges
  // 0 input firstidx
  // 1 input lastidx

  // Arrow output ranges
  // 0 output firstidx
  // 1 output lastidx
  static constexpr size_t range_regs_per_inst = 2;

  // 0 input val addr lo
  // 1 input val addr hi
  static constexpr size_t in_addr_regs_per_inst = 2;
  // 2 output off addr lo
  // 3 output off addr hi
  // 4 output val addr lo
  // 5 output val addr hi
  static constexpr size_t out_addr_regs_per_inst = 4;

  // Custom regs per instance:
  // 0 control
  // 1 status
  // 2 result num rows lo
  // 3 result num rows hi
  static constexpr size_t custom_regs_per_inst = 4;

  [[nodiscard]] auto custom_regs_offset() const -> size_t {
    return default_regs + num_parsers * (2 * range_regs_per_inst + in_addr_regs_per_inst + out_addr_regs_per_inst);
  }

  auto ctrl_offset(size_t idx) -> size_t {
    return custom_regs_offset() + custom_regs_per_inst * idx;
  }
  auto status_offset(size_t idx) -> size_t { return ctrl_offset(idx) + 1; }

  auto result_rows_offset_lo(size_t idx) -> size_t {
    return status_offset(idx) + 1;
  }
  auto result_rows_offset_hi(size_t idx) -> size_t {
    return result_rows_offset_lo(idx) + 1;
  }

  auto input_firstidx_offset(size_t idx) -> size_t {
    return default_regs + range_regs_per_inst * idx;
  }

  auto input_lastidx_offset(size_t idx) -> size_t {
    return input_firstidx_offset(idx) + 1;
  }

  auto input_values_lo_offset(size_t idx) -> size_t {
    return default_regs + (2 * range_regs_per_inst) * num_parsers + in_addr_regs_per_inst * idx;
  }

  auto input_values_hi_offset(size_t idx) -> size_t {
    return input_values_lo_offset(idx) + 1;
  }

  size_t idx_;
  size_t num_parsers;
  fletcher::Platform *platform_;
  fletcher::Context *context_;
  fletcher::Kernel *kernel_;
  AddrMap *h2d_addr_map;
  std::byte *raw_out_offsets;
  std::byte *raw_out_values;
  std::mutex *platform_mutex;
};

struct OpaeBatteryOptions {
  std::string afu_id = OPAE_BATTERY_AFU_ID;
};

class OpaeBatteryParserManager {
 public:
  static auto Make(const OpaeBatteryOptions &opts,
                   const std::vector<illex::RawJSONBuffer *> &buffers,
                   size_t num_parsers,
                   std::shared_ptr<OpaeBatteryParserManager> *out) -> Status;

  auto num_parsers() const -> size_t { return num_parsers_; }
  auto parsers() -> std::vector<std::shared_ptr<OpaeBatteryParser>> { return parsers_; }
 private:
  auto PrepareInputBatches(const std::vector<illex::RawJSONBuffer *> &buffers) -> Status;
  auto PrepareOutputBatches() -> Status;
  auto PrepareParsers() -> Status;

  OpaeBatteryOptions opts_;

  std::unordered_map<const std::byte *, da_t> h2d_addr_map;

  size_t num_parsers_;
  buffer::OpaeAllocator allocator;
  std::vector<std::byte *> raw_out_offsets;
  std::vector<std::byte *> raw_out_values;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_in;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_out;

  std::shared_ptr<fletcher::Platform> platform;
  std::shared_ptr<fletcher::Context> context;
  std::shared_ptr<fletcher::Kernel> kernel;

  std::vector<std::shared_ptr<OpaeBatteryParser>> parsers_;

  std::mutex platform_mutex;
};

}
