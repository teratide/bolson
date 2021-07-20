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

#include "bolson/buffer/fpga_allocator.h"
#include "bolson/parse/parser.h"

#define BOLSON_DEFAULT_FPGA_TRIP_PARSERS 8

namespace bolson::parse::fpga {

struct TripOptions {
  size_t num_parsers = BOLSON_DEFAULT_FPGA_TRIP_PARSERS;
  size_t num_output_rows = 1024 * 1024;
  size_t num_string_value_bytes = 1024 * 1024 * 1024;
};

void AddTripOptionsToCLI(CLI::App* sub, TripOptions* out);

/**
 * \brief Host-side representation of the N:1 hardware parsers for trip report.
 */
class TripParser : public Parser {
 public:
  /// \brief TripParser constructor.
  TripParser(fletcher::Platform* platform, fletcher::Context* context,
             fletcher::Kernel* kernel,
             std::vector<std::shared_ptr<arrow::Array>>* output_arrays,
             size_t num_parsers);

  auto Parse(const std::vector<illex::JSONBuffer*>& in, std::vector<ParsedBatch>* out)
      -> Status override;

  static auto input_schema() -> std::shared_ptr<arrow::Schema>;
  static auto output_schema() -> std::shared_ptr<arrow::Schema>;

 private:
  static const uint32_t stat_idle = (1u << 0u);
  static const uint32_t stat_busy = (1u << 1u);
  static const uint32_t stat_done = (1u << 2u);
  static const uint32_t ctrl_start = (1u << 0u);
  static const uint32_t ctrl_stop = (1u << 1u);
  static const uint32_t ctrl_reset = (1u << 2u);

  auto WriteInputMetaData(fletcher::Platform* platform, illex::JSONBuffer* in, size_t idx)
      -> Status;

  [[nodiscard]] auto custom_regs_offset() const -> size_t;
  static auto input_firstidx_offset(size_t idx) -> size_t;
  static auto input_lastidx_offset(size_t idx) -> size_t;
  [[nodiscard]] auto input_values_lo_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto input_values_hi_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto tag_offset(size_t idx) const -> size_t;
  [[nodiscard]] auto bytes_consumed_offset(size_t idx) const -> size_t;

  size_t num_hardware_parsers_;
  fletcher::Platform* platform_;
  fletcher::Context* context_;
  fletcher::Kernel* kernel_;
  std::vector<std::shared_ptr<arrow::Array>>* output_arrays_sw_{};
};

/**
 * \brief ParserContext for the trip report schema.
 */
class TripParserContext : public ParserContext {
 public:
  static auto Make(const TripOptions& opts, size_t input_size,
                   std::shared_ptr<ParserContext>* out) -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;
  [[nodiscard]] auto CheckThreadCount(size_t num_threads) const -> size_t override;
  [[nodiscard]] auto CheckBufferCount(size_t num_buffers) const -> size_t override;
  [[nodiscard]] auto input_schema() const -> std::shared_ptr<arrow::Schema> override;
  [[nodiscard]] auto output_schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  explicit TripParserContext(const TripOptions& opts);

  [[nodiscard]] auto PrepareInputBatches() -> Status;
  [[nodiscard]] auto PrepareOutputBatch(size_t num_rows, size_t num_string_value_bytes)
      -> Status;
  [[nodiscard]] auto PrepareParser() -> Status;

  size_t num_parsers_;

  buffer::FpgaAllocator allocator;

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

  std::shared_ptr<TripParser> parser;
};

auto TripReportBatchToString(const arrow::RecordBatch& batch) -> std::string;

}  // namespace bolson::parse::fpga
