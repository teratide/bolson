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
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>

#include "bolson/bench.h"
#include "bolson/convert/converter.h"
#include "bolson/convert/test_convert.h"
#include "bolson/log.h"
#include "bolson/parse/opae/trip.h"

namespace bolson::convert {

static auto generate_schema() -> std::shared_ptr<arrow::Schema> {
  static auto kvm = arrow::key_value_metadata({"illex_MIN", "illex_MAX"}, {"1", "99"});
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema(
          {arrow::field("timestamp", arrow::utf8(), false),
           arrow::field("timezone", arrow::uint64(), false)->WithMetadata(kvm),
           arrow::field("vin", arrow::uint64(), false)->WithMetadata(kvm),
           arrow::field("odometer", arrow::uint64(), false)->WithMetadata(kvm),
           arrow::field("hypermiling", arrow::boolean(), false),
           arrow::field("avgspeed", arrow::uint64(), false)->WithMetadata(kvm),
           arrow::field(
               "sec_in_band",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 12),
               false),
           arrow::field(
               "miles_in_time_range",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 24),
               false),
           arrow::field(
               "const_speed_miles_in_band",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 12),
               false),
           arrow::field(
               "vary_speed_miles_in_band",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 12),
               false),
           arrow::field(
               "sec_decel",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 10),
               false),
           arrow::field(
               "sec_accel",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 10),
               false),
           arrow::field(
               "braking",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 6),
               false),
           arrow::field(
               "accel",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 6),
               false),
           arrow::field("orientation", arrow::boolean(), false),
           arrow::field(
               "small_speed_var",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 13),
               false),
           arrow::field(
               "large_speed_var",
               arrow::fixed_size_list(
                   arrow::field("item", arrow::uint64(), false)->WithMetadata(kvm), 13),
               false),
           arrow::field("accel_decel", arrow::uint64(), false)->WithMetadata(kvm),
           arrow::field("speed_changes", arrow::uint64(), false)->WithMetadata(kvm)}),
      "output", fletcher::Mode::WRITE);
  return result;
}

static auto message_schema() -> std::shared_ptr<arrow::Schema> {
  auto fields = generate_schema()->fields();
  fields.insert(fields.begin(), arrow::field("bolson_seq", arrow::uint64(), false));
  auto result = arrow::schema(fields);
  return result;
}

inline auto GetSortOrder(const std::shared_ptr<arrow::RecordBatch>& batch)
    -> std::shared_ptr<arrow::UInt64Array> {
  return std::dynamic_pointer_cast<arrow::UInt64Array>(
      arrow::compute::SortIndices(
          arrow::Datum(*batch),
          arrow::compute::SortOptions({arrow::compute::SortKey("bolson_seq")}))
          .ValueOrDie());
}

void CompareReindexedColumns(const std::shared_ptr<arrow::Array>& a,
                             const std::shared_ptr<arrow::UInt64Array>& a_index,
                             const std::shared_ptr<arrow::Array>& b,
                             const std::shared_ptr<arrow::UInt64Array>& b_index) {
  EXPECT_EQ(a_index->length(), b_index->length());
  EXPECT_TRUE(a->type()->Equals(b->type()));

  if (a->type_id() == arrow::Type::BOOL) {
    auto ca = std::dynamic_pointer_cast<arrow::BooleanArray>(a);
    auto cb = std::dynamic_pointer_cast<arrow::BooleanArray>(b);
    for (size_t i = 0; i < a->length(); i++) {
      EXPECT_EQ(ca->Value(a_index->Value(i)), cb->Value(b_index->Value(i)));
    }
  } else if (a->type_id() == arrow::Type::UINT8) {
    auto ca = std::dynamic_pointer_cast<arrow::UInt8Array>(a);
    auto cb = std::dynamic_pointer_cast<arrow::UInt8Array>(b);
    for (size_t i = 0; i < a->length(); i++) {
      EXPECT_EQ(ca->Value(a_index->Value(i)), cb->Value(b_index->Value(i)));
    }
  } else if (a->type_id() == arrow::Type::UINT64) {
    auto ca = std::dynamic_pointer_cast<arrow::UInt64Array>(a);
    auto cb = std::dynamic_pointer_cast<arrow::UInt64Array>(b);
    for (size_t i = 0; i < a->length(); i++) {
      EXPECT_EQ(ca->Value(a_index->Value(i)), cb->Value(b_index->Value(i)));
    }
  } else if (a->type_id() == arrow::Type::STRING) {
    auto ca = std::dynamic_pointer_cast<arrow::StringArray>(a);
    auto cb = std::dynamic_pointer_cast<arrow::StringArray>(b);
    for (size_t i = 0; i < a->length(); i++) {
      EXPECT_EQ(ca->GetString(a_index->Value(i)), cb->GetString(b_index->Value(i)));
    }
  } else if (a->type_id() == arrow::Type::FIXED_SIZE_LIST) {
    EXPECT_TRUE(a->type()->field(0)->type()->Equals(arrow::uint64()));
    // This must be a uint64
    auto ca = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(a);
    auto cb = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(b);
    for (size_t i = 0; i < a->length(); i++) {
      EXPECT_TRUE(
          ca->value_slice(a_index->Value(i))->Equals(cb->value_slice(b_index->Value(i))));
    }
  } else {
    FAIL() << "Unexpected type: " << a->type()->ToString();
  }
}

void CompareTripBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& ref_batches,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& uut_batches,
    const size_t expected_rows) {
  ASSERT_EQ(ref_batches.size(), uut_batches.size());

  size_t ref_rows = 0;
  size_t uut_rows = 0;

  for (size_t b = 0; b < ref_batches.size(); b++) {
    auto ref_batch = ref_batches[b];
    auto uut_batch = uut_batches[b];

    EXPECT_TRUE(ref_batch->schema()->Equals(uut_batch->schema()));

    std::string str_ref;
    std::string str_uut;

    // Make sure batch contains some data before comparing.
    EXPECT_TRUE(ref_batch->num_rows() > 0);
    EXPECT_TRUE(uut_batch->num_rows() > 0);

    // Both batches should have an equal number of rows and columns.
    EXPECT_TRUE(ref_batch->num_rows() == uut_batch->num_rows());
    EXPECT_TRUE(ref_batch->num_columns() == uut_batch->num_columns());

    // Increment total row counts.
    ref_rows += ref_batch->num_rows();
    uut_rows += uut_batch->num_rows();

    // Sort by sequence number.
    auto ref_order = GetSortOrder(ref_batch);
    auto uut_order = GetSortOrder(uut_batch);

    for (size_t c = 0; c < ref_batch->num_columns(); c++) {
      CompareReindexedColumns(ref_batch->column(c), ref_order, uut_batch->column(c),
                              uut_order);
    }
  }

  // [FNC02]: Each JSON Object is represented as a row in an Arrow RecordBatch.
  ASSERT_EQ(ref_rows, expected_rows);
  ASSERT_EQ(uut_rows, expected_rows);
}

/// \brief Test Arrow impl. vs. Opae FPGA impl. for battery status.
TEST(OPAE, OPAE_TRIP_3_KERNELS) {
  StartLogger();
  const size_t opae_trip_instances = 3;
  const size_t num_jsons = 64 * 1024;
  const size_t max_ipc_size = 100 * 1024 * 1024;

  // Generate a bunch of JSONs
  std::vector<illex::JSONItem> jsons_in;
  auto gen_result =
      GenerateJSONs(num_jsons, *generate_schema(), illex::GenerateOptions(0), &jsons_in);

  // Set OPAE Converter options.
  ConverterOptions opae_opts;
  opae_opts.parser.impl = parse::Impl::OPAE_TRIP;
  opae_opts.parser.opae_trip.num_parsers = opae_trip_instances;
  opae_opts.max_batch_rows = num_jsons;
  opae_opts.max_ipc_size = max_ipc_size;

  // Set Arrow Converter options, using the same options where applicable.
  ConverterOptions arrow_opts = opae_opts;
  arrow_opts.parser.impl = parse::Impl::ARROW;
  arrow_opts.parser.arrow.buf_capacity = gen_result.first + num_jsons;
  arrow_opts.parser.arrow.schema = generate_schema();

  // Run both implementations.
  std::vector<publish::IpcQueueItem> arrow_out;
  std::vector<publish::IpcQueueItem> opae_out;
  FAIL_ON_ERROR(Convert(arrow_opts, jsons_in, &arrow_out));
  FAIL_ON_ERROR(Convert(opae_opts, jsons_in, &opae_out));

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_batches;
  std::vector<std::shared_ptr<arrow::RecordBatch>> opae_batches;
  DeserializeMessages(arrow_out, opae_out, parse::opae::TripParser::output_schema(),
                      max_ipc_size, &arrow_batches, &opae_batches);

  CompareTripBatches(arrow_batches, opae_batches, num_jsons);
}

}  // namespace bolson::convert