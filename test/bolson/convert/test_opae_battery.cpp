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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>

#include "bolson/bench.h"
#include "bolson/convert/converter.h"
#include "bolson/convert/test_convert.h"
#include "bolson/parse/opae/battery.h"

namespace bolson::convert {

/// \brief Opae Battery Status schema
auto test_schema() -> std::shared_ptr<arrow::Schema> {
  // FNC04: Number fields of the JSON Object only represent and are converted to unsigned
  //  integers of 64 bits.

  // This is guaranteed in the consumer by the schema below:
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema(
          {arrow::field("voltage",
                        arrow::list(arrow::field("item", arrow::uint64(), false)
                                        ->WithMetadata(arrow::key_value_metadata(
                                            {"illex_MIN", "ILLEX_MAX"}, {"0", "2047"}))),
                        false)
               ->WithMetadata(arrow::key_value_metadata(
                   {"illex_MIN_LENGTH", "ILLEX_MAX_LENGTH"}, {"1", "16"}))}),
      "output", fletcher::Mode::WRITE);
  return result;
}

/// \brief Test Arrow impl. vs. Opae FPGA impl. for battery status.
TEST(OPAE, OPAE_BATTERY_8_KERNELS) {
  // [FNC01]: The system performs the Convert JSON objects function, where JSON Objects
  //  are streamed into the system, aggregated in JSON Objects Messages, and converted to
  //  Pulsar Messages containing Arrow IPC messages containing Arrow RecordBatches.

  const size_t opae_battery_parsers_instances = 8;          // Number of parser instances.
  const size_t num_jsons = 1024 * 1024;                     // Number of JSONs to test.
  const size_t max_ipc_size = 5 * 1024 * 1024 - 10 * 1024;  // Max IPC size.

  // Generate a bunch of JSONs
  std::vector<illex::JSONItem> jsons_in;
  auto bytes_largest =
      GenerateJSONs(num_jsons, *test_schema(), illex::GenerateOptions(0), &jsons_in);

  // Set OPAE Converter options.
  ConverterOptions opae_opts;
  opae_opts.parser.impl = parse::Impl::OPAE_BATTERY;
  opae_opts.num_threads = opae_battery_parsers_instances;
  opae_opts.max_batch_rows = 1024;
  opae_opts.max_ipc_size = max_ipc_size;

  // Set Arrow Converter options, using the same options where applicable.
  ConverterOptions arrow_opts = opae_opts;
  arrow_opts.parser.impl = parse::Impl::ARROW;
  arrow_opts.parser.arrow.schema = test_schema();

  // Run both implementations.
  std::vector<publish::IpcQueueItem> arrow_out;
  std::vector<publish::IpcQueueItem> opae_out;
  FAIL_ON_ERROR(Convert(arrow_opts, jsons_in, &arrow_out));
  FAIL_ON_ERROR(Convert(opae_opts, jsons_in, &opae_out));

  // Sort outputs by seq. no.
  std::sort(arrow_out.begin(), arrow_out.end());
  std::sort(opae_out.begin(), opae_out.end());

  ConsumeMessages(arrow_out, opae_out, test_schema(), num_jsons, max_ipc_size);
}

}  // namespace bolson::convert
