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

static auto test_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema(
          {arrow::field("timestamp", arrow::utf8(), false),
           arrow::field("tag", arrow::uint64(), false),
           arrow::field("timezone", arrow::uint64(), false),
           arrow::field("vin", arrow::uint64(), false),
           arrow::field("odometer", arrow::uint64(), false),
           arrow::field("hypermiling", arrow::uint8(), false),
           arrow::field("avgspeed", arrow::uint64(), false),
           arrow::field(
               "sec_in_band",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12),
               false),
           arrow::field(
               "miles_in_time_range",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 24),
               false),
           arrow::field(
               "const_speed_miles_in_band",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12),
               false),
           arrow::field(
               "vary_speed_miles_in_band",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12),
               false),
           arrow::field(
               "sec_decel",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 10),
               false),
           arrow::field(
               "sec_accel",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 10),
               false),
           arrow::field(
               "braking",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 6),
               false),
           arrow::field(
               "accel",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 6),
               false),
           arrow::field("orientation", arrow::uint8(), false),
           arrow::field(
               "small_speed_var",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13),
               false),
           arrow::field(
               "large_speed_var",
               arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13),
               false),
           arrow::field("accel_decel", arrow::uint64(), false),
           arrow::field("speed_changes", arrow::uint64(), false)}),
      "output", fletcher::Mode::WRITE);
  return result;
}

/// \brief Test Arrow impl. vs. Opae FPGA impl. for battery status.
TEST(OPAE, OPAE_TRIP_3_KERNELS) {
  const size_t opae_trip_instances = 3;                     // Number of parser instances.
  const size_t num_jsons = 1024 * 1024;                     // Number of JSONs to test.
  const size_t max_ipc_size = 5 * 1024 * 1024 - 10 * 1024;  // Max IPC size.

  // Generate a bunch of JSONs
  std::vector<illex::JSONItem> jsons_in;
  auto bytes_largest =
      GenerateJSONs(num_jsons, *test_schema(), illex::GenerateOptions(0), &jsons_in);

  // Set OPAE Converter options.
  ConverterOptions opae_opts;
  opae_opts.parser.impl = parse::Impl::OPAE_TRIP;
  opae_opts.num_threads = opae_trip_instances;
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
