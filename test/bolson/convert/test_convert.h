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
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <illex/client_queueing.h>

#include <memory>

#include "bolson/bench.h"
#include "bolson/convert/converter.h"
#include "bolson/convert/test_convert.h"
#include "bolson/log.h"
#include "bolson/publish/publisher.h"

namespace bolson::convert {

#define FAIL_ON_ERROR(status)   \
  {                             \
    auto __status = (status);   \
    if (!__status.ok()) {       \
      FAIL() << __status.msg(); \
    }                           \
  }

/// \brief Deserialize an Arrow RecordBatch given a schema and a buffer.
auto GetRecordBatch(const std::shared_ptr<arrow::Schema>& schema,
                    const std::shared_ptr<arrow::Buffer>& buffer)
    -> std::shared_ptr<arrow::RecordBatch> {
  auto stream = std::dynamic_pointer_cast<arrow::io::InputStream>(
      std::make_shared<arrow::io::BufferReader>(buffer));
  assert(stream != nullptr);
  auto ipc_read_opts = arrow::ipc::IpcReadOptions::Defaults();
  auto batch = arrow::ipc::ReadRecordBatch(schema, nullptr, ipc_read_opts, stream.get())
                   .ValueOrDie();
  return batch;
}

/**
 * \brief Convert a bunch of JSONs to Arrow IPC messages with given options.
 * \param opts  Converter options.
 * \param in    JSON input.
 * \param out   JSON output.
 * \return Status::OK() if successful, some error otherwise.
 */
auto Convert(const ConverterOptions& opts, const std::vector<illex::JSONItem>& in,
             std::vector<publish::IpcQueueItem>* out) -> Status {
  // Set up the output queue.
  publish::IpcQueue out_queue;
  std::shared_ptr<Converter> conv;
  BOLSON_ROE(Converter::Make(opts, &out_queue, &conv));
  FillBuffers(conv->parser_context()->mutable_buffers(), in);
  std::atomic<bool> shutdown = false;
  conv->Start(&shutdown);

  size_t rows = 0;
  while ((rows != in.size()) && !shutdown.load()) {
    publish::IpcQueueItem item;
    if (out_queue.wait_dequeue_timed(item, std::chrono::milliseconds(1))) {
      rows += RecordSizeOf(item);
      out->push_back(item);
    }
  }

  shutdown.store(true);
  BOLSON_ROE(Aggregate(conv->Finish()));
  return Status::OK();
}

/**
 * \brief Deserialize reference and unit under test messages.
 * \param ref_data      Reference messages.
 * \param uut_data      UUT messages.
 * \param schema        The schema to which the messages should comply.
 * \param max_ipc_size  The maximum IPC size they should comply to.
 * \param ref_out       The deserialized reference batches.
 * \param uut_out       The deserialized UUT batches.
 */
void DeserializeMessages(const std::vector<publish::IpcQueueItem>& ref_data,
                         const std::vector<publish::IpcQueueItem>& uut_data,
                         const std::shared_ptr<arrow::Schema>& schema,
                         const size_t max_ipc_size,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* ref_out,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* uut_out) {
  // Validate schema.

  // [FNC04]: Number fields of the JSON Object only represent and are converted to
  //  unsigned integers of 64 bits.
  // [FNC05]: Text fields of the JSON Object representing timestamps are converted to
  //  Arrow’s “utf8” type, not to Arrow’s “date32” nor to the “date64” type.
  for (const auto& field : schema->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::BOOL:
      case arrow::Type::UINT64:
      case arrow::Type::STRING:
        break;
      case arrow::Type::LIST:
        ASSERT_TRUE(field->type()->field(0)->type()->Equals(arrow::uint64()));
        break;
      case arrow::Type::FIXED_SIZE_LIST:
        ASSERT_TRUE(field->type()->field(0)->type()->Equals(arrow::uint64()));
        break;
      default:
        // [FNC07]: Nested JSON Objects are flattened in the resulting Arrow Schema.
        // Note: If nested JSONs were not flattened, then an arrow::struct needs to be
        //   supplied, and this test will fail.
        FAIL() << "Fields must be converted to arrow::boolean, arrow::uint64 or "
                  "arrow::utf8";
    }
  }

  // Assert number of output IPC messages is the same for CPU & FPGA impl.
  ASSERT_EQ(ref_data.size(), uut_data.size());

  // Assert some rules about message sizes.
  for (size_t i = 0; i < ref_data.size(); i++) {
    // FNC10: The system will aggregate JSON objects into Arrow RecordBatches such that
    // the serialized RecordBatch into a Pulsar message will not exceed the Pulsar maximum
    // message size, which is never larger than 100 MiB.
    ASSERT_LT(ref_data[i].message->size(), 100 * 1024 * 1024);
    ASSERT_LT(uut_data[i].message->size(), 100 * 1024 * 1024);
    ASSERT_LE(ref_data[i].message->size(), max_ipc_size);
    ASSERT_LE(uut_data[i].message->size(), max_ipc_size);

    // Deserialize batches
    auto ref_batch = GetRecordBatch(schema, ref_data[i].message);
    auto uut_batch = GetRecordBatch(schema, uut_data[i].message);

    ref_out->push_back(ref_batch);
    uut_out->push_back(uut_batch);
  }
}

void CompareBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& ref_batches,
                    const std::vector<std::shared_ptr<arrow::RecordBatch>>& uut_batches,
                    const size_t expected_rows) {
  ASSERT_EQ(ref_batches.size(), uut_batches.size());

  size_t ref_rows = 0;
  size_t uut_rows = 0;

  for (size_t i = 0; i < ref_batches.size(); i++) {
    auto ref_batch = ref_batches[i];
    auto uut_batch = uut_batches[i];

    // Make sure batch contains some data before comparing.
    EXPECT_TRUE(ref_batch->num_rows() > 0);
    EXPECT_TRUE(uut_batch->num_rows() > 0);

    // Both batches should have an equal number of rows and columns.
    EXPECT_TRUE(ref_batch->num_rows() == uut_batch->num_rows());
    EXPECT_TRUE(ref_batch->num_columns() == uut_batch->num_columns());

    // Increment total row counts.
    ref_rows += ref_batch->num_rows();
    uut_rows += uut_batch->num_rows();

    // [FNC03]: The information content of the JSON Objects and the Arrow row
    //  representation of the data is equal.
    ASSERT_TRUE(ref_batch->Equals(*uut_batch));
  }

  // [FNC02]: Each JSON Object is represented as a row in an Arrow RecordBatch.
  ASSERT_EQ(ref_rows, expected_rows);
  ASSERT_EQ(uut_rows, expected_rows);
}

}  // namespace bolson::convert