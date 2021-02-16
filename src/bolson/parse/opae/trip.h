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
#include "bolson/parse/parser.h"

#define BOLSON_DEFAULT_OPAE_TRIP_PARSERS 4

namespace bolson::parse::opae {

struct TripOptions {
  std::string afu_id;
  size_t num_parsers = BOLSON_DEFAULT_OPAE_TRIP_PARSERS;
};

void AddTripOptionsToCLI(CLI::App* sub, TripOptions* out);

using AddrMap = std::unordered_map<const std::byte*, da_t>;

class TripParser : public Parser {
 public:
  auto Parse(const std::vector<illex::JSONBuffer*>& in, std::vector<ParsedBatch>* out)
      -> Status override;

 private:
  auto WriteInputMetaData(fletcher::Platform* platform, illex::JSONBuffer* in,
                          const AddrMap& h2d_addr_map, size_t idx) -> Status;

  auto custom_regs_offset() const -> size_t;
  static auto input_firstidx_offset(size_t idx) -> size_t;
  static auto input_lastidx_offset(size_t idx) -> size_t;
  auto ctrl_offset(size_t idx) -> size_t;
  auto status_offset(size_t idx) -> size_t { return ctrl_offset(idx) + 1; }
  auto result_rows_offset_lo(size_t idx) -> size_t { return status_offset(idx) + 1; }
  auto result_rows_offset_hi(size_t idx) -> size_t;
  auto input_values_lo_offset(size_t idx) const -> size_t;
  auto input_values_hi_offset(size_t idx) -> size_t;

  size_t num_hardware_parsers;
};

class TripParserContext : public ParserContext {
 public:
  static auto Make(const TripOptions& opts, std::shared_ptr<ParserContext>* out)
      -> Status;

  auto Init(const std::vector<illex::JSONBuffer*>& buffers) -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;
  [[nodiscard]] auto CheckThreadCount(size_t num_threads) const -> size_t override;
  [[nodiscard]] auto CheckBufferCount(size_t num_buffers) const -> size_t override;
  [[nodiscard]] auto schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  auto PrepareInputBatches(const std::vector<illex::JSONBuffer*>& buffers) -> Status;
  auto PrepareOutputBatch() -> Status;
  auto PrepareParsers() -> Status;
  auto WriteInputMetaData(illex::JSONBuffer* in, size_t idx) -> Status;

  size_t num_hardware_parsers;

  std::string afu_id;

  buffer::OpaeAllocator allocator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_in;
  std::shared_ptr<arrow::RecordBatch> batch_out_hw;

  // We create different views of the data for Fletcher integration and the downstream
  // code, since Fletcher currently doesn't support fixed size lists without workarounds.
  std::vector<std::shared_ptr<arrow::Array>> output_arrays_sw;
  // In case of 'output_arrays_hw', we wrap the buffers behind fixed size list fields
  // as primitive arrays.
  std::vector<std::shared_ptr<arrow::Array>> output_arrays_hw;

  std::shared_ptr<arrow::RecordBatch> batch_out_sw;

  std::shared_ptr<fletcher::Platform> platform;
  std::shared_ptr<fletcher::Context> context;
  std::shared_ptr<fletcher::Kernel> kernel;

  AddrMap h2d_addr_map;

  std::shared_ptr<TripParser> parser;
};

}  // namespace bolson::parse::opae
