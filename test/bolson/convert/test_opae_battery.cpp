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
#include <illex/document.h>

#include "bolson/bench.h"
#include "bolson/convert/converter.h"
#include "bolson/convert/test_convert.h"
#include "bolson/parse/opae_battery_impl.h"
#include "bolson/convert/test_convert.h"

namespace bolson::convert {

/// \brief Opae Battery Status schema
auto test_schema() -> std::shared_ptr<arrow::Schema> {
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

/// Test Arrow impl. vs. Opae FPGA impl. for battery status.
TEST(FPGA, OPAE_BATTERY_8_KERNELS) {
  StartLogger();

  const size_t opae_battery_parsers_instances = 8;  // Number of parser instances.
  const size_t num_jsons = 1024 * 1024;             // Number of JSONs to test.

  // Generate a bunch of JSONs
  std::vector<illex::JSONQueueItem> jsons_in;
  auto bytes_largest =
      GenerateJSONs(num_jsons, *test_schema(), illex::GenerateOptions(0), &jsons_in);

  // Set OPAE Converter options.
  Options opae_opts;
  opae_opts.implementation = parse::Impl::OPAE_BATTERY;
  opae_opts.buf_capacity = buffer::OpaeAllocator::opae_fixed_capacity;
  opae_opts.num_threads = opae_battery_parsers_instances;
  opae_opts.num_buffers = opae_battery_parsers_instances;
  opae_opts.max_batch_rows = 1024;
  opae_opts.max_ipc_size = 5 * 1024 * 1024 - 10 * 1024;

  // Set Arrow Converter options, using the same options where applicable.
  Options arrow_opts = opae_opts;
  arrow_opts.implementation = parse::Impl::ARROW;
  arrow_opts.buf_capacity = 2 * bytes_largest.first;
  arrow_opts.arrow.read.use_threads = false;
  arrow_opts.arrow.parse.explicit_schema = test_schema();

  // Run both implementations.
  std::vector<IpcQueueItem> arrow_out;
  std::vector<IpcQueueItem> opae_out;
  FAIL_ON_ERROR(Convert(arrow_opts, jsons_in, &arrow_out));
  FAIL_ON_ERROR(Convert(opae_opts, jsons_in, &opae_out));

  // Sort outputs by seq. no.
  std::sort(arrow_out.begin(), arrow_out.end(), IpcSortBySeq);
  std::sort(opae_out.begin(), opae_out.end(), IpcSortBySeq);

  // Assert number of output IPC messages is the same for CPU & FPGA impl.
  ASSERT_EQ(arrow_out.size(), opae_out.size());

  // Assert the contents of each RecordBatch is the same for CPU & FPGA impl.
  for (size_t i = 0; i < num_jsons; i++) {
    auto arrow_batch = GetRecordBatch(test_schema(), arrow_out[i].message);
    auto opae_batch = GetRecordBatch(test_schema(), opae_out[i].message);
    ASSERT_TRUE(arrow_batch->Equals(*opae_batch));
  }
}

}  // namespace bolson::convert
