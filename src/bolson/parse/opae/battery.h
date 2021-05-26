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

#include <arrow/api.h>
#include <fletcher/api.h>

#include <CLI/CLI.hpp>
#include <memory>
#include <utility>

#include "bolson/buffer/opae_allocator.h"
#include "bolson/parse/opae/opae.h"
#include "bolson/parse/parser.h"
#include "bolson/utils.h"

#define BOLSON_DEFAULT_OPAE_BATTERY_PARSERS 8
#define BOLSON_DEFAULT_OPAE_BATTERY_AFUID "9ca43fb0-c340-4908-b79b-5c89b4ef5e"

namespace bolson::parse::opae {

struct BatteryOptions {
  std::string afu_id;  // left empty to auto-derive by default.
  size_t num_parsers = BOLSON_DEFAULT_OPAE_BATTERY_PARSERS;
  bool seq_column = true;
};

void AddBatteryOptionsToCLI(CLI::App* sub, BatteryOptions* out);

class BatteryParser : public Parser {
 public:
  /// \brief Return the Arrow schema with an output_type field.
  static auto input_schema() -> std::shared_ptr<arrow::Schema>;

  /// \brief OpaeBatteryParser constructor.
  BatteryParser(fletcher::Platform* platform, fletcher::Context* context,
                fletcher::Kernel* kernel, AddrMap* addr_map, size_t parser_idx,
                size_t num_parsers, std::byte* raw_out_offsets, std::byte* raw_out_values,
                std::mutex* platform_mutex, bool seq_column)
      : platform_(platform),
        context_(context),
        kernel_(kernel),
        h2d_addr_map(addr_map),
        idx_(parser_idx),
        num_parsers(num_parsers),
        raw_out_offsets(raw_out_offsets),
        raw_out_values(raw_out_values),
        platform_mutex(platform_mutex),
        seq_column(seq_column) {}

  auto Parse(const std::vector<illex::JSONBuffer*>& in, std::vector<ParsedBatch>* out)
      -> Status override;

  auto ParseOne(illex::JSONBuffer* in, ParsedBatch* out) -> Status;

 private:
  static const uint32_t stat_idle = (1u << 0u);
  static const uint32_t stat_busy = (1u << 1u);
  static const uint32_t stat_done = (1u << 2u);
  static const uint32_t ctrl_start = (1u << 0u);
  static const uint32_t ctrl_stop = (1u << 1u);
  static const uint32_t ctrl_reset = (1u << 2u);

  // Fletcher default regs (unused):
  // 0 control
  // 1 status
  // 2 return lo
  // 3 return hi
  static const size_t default_regs = 4;

  // Arrow input ranges
  // 0 input firstidx
  // 1 input lastidx

  // Arrow output ranges
  // 0 output firstidx
  // 1 output lastidx
  static const size_t range_regs_per_inst = 2;

  // 0 input val addr lo
  // 1 input val addr hi
  static const size_t in_addr_regs_per_inst = 2;
  // 2 output off addr lo
  // 3 output off addr hi
  // 4 output val addr lo
  // 5 output val addr hi
  static const size_t out_addr_regs_per_inst = 4;

  // Custom regs per instance:
  // 0 control
  // 1 status
  // 2 result num rows lo
  // 3 result num rows hi
  static const size_t custom_regs_per_inst = 4;

  [[nodiscard]] auto custom_regs_offset() const -> size_t;
  [[nodiscard]] auto ctrl_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto status_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto result_rows_offset_lo(size_t idx) const -> size_t;
  [[nodiscard]] auto result_rows_offset_hi(size_t idx) const -> size_t;
  [[nodiscard]] auto input_firstidx_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto input_lastidx_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto input_values_lo_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto input_values_hi_offset(size_t idx) const -> size_t;

  size_t idx_;
  size_t num_parsers;
  fletcher::Platform* platform_;
  fletcher::Context* context_;
  fletcher::Kernel* kernel_;
  AddrMap* h2d_addr_map;
  std::byte* raw_out_offsets;
  std::byte* raw_out_values;
  std::mutex* platform_mutex;
  bool seq_column;
};

class BatteryParserContext : public ParserContext {
 public:
  static auto Make(const BatteryOptions& opts, std::shared_ptr<ParserContext>* out)
      -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;
  [[nodiscard]] auto CheckThreadCount(size_t num_threads) const -> size_t override;
  [[nodiscard]] auto CheckBufferCount(size_t num_buffers) const -> size_t override;
  [[nodiscard]] auto input_schema() const -> std::shared_ptr<arrow::Schema> override;
  [[nodiscard]] auto output_schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  explicit BatteryParserContext(const BatteryOptions& opts);

  auto PrepareInputBatches() -> Status;
  auto PrepareOutputBatches() -> Status;
  auto PrepareParsers(bool seq_column) -> Status;

  size_t num_parsers_;
  std::string afu_id_;

  std::unordered_map<const std::byte*, da_t> h2d_addr_map;

  std::vector<std::byte*> raw_out_offsets;
  std::vector<std::byte*> raw_out_values;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_in;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_out;

  std::shared_ptr<fletcher::Platform> platform;
  std::shared_ptr<fletcher::Context> context;
  std::shared_ptr<fletcher::Kernel> kernel;

  std::vector<std::shared_ptr<BatteryParser>> parsers_;

  std::mutex platform_mutex;

  std::shared_ptr<arrow::Schema> input_schema_;
  std::shared_ptr<arrow::Schema> output_schema_;

  bool seq_column;
};

}  // namespace bolson::parse::opae
