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

namespace bolson::convert {

#define FAIL_ON_ERROR(status)                   \
  {                                             \
    auto __status = (status);                   \
    if (!__status.ok()) {                       \
      throw std::runtime_error(__status.msg()); \
    }                                           \
  }

/// \brief Deserialize an Arrow RecordBatch given a schema and a buffer.
auto GetRecordBatch(const std::shared_ptr<arrow::Schema>& schema,
                    const std::shared_ptr<arrow::Buffer>& buffer)
    -> std::shared_ptr<arrow::RecordBatch> {
  auto stream = std::dynamic_pointer_cast<arrow::io::InputStream>(
      std::make_shared<arrow::io::BufferReader>(buffer));
  assert(stream != nullptr);
  auto batch = arrow::ipc::ReadRecordBatch(
                   schema, nullptr, arrow::ipc::IpcReadOptions::Defaults(), stream.get())
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
             std::vector<IpcQueueItem>* out) -> Status {
  // Set up the output queue.
  IpcQueue out_queue;
  std::shared_ptr<Converter> conv;
  BOLSON_ROE(Converter::Make(opts, &out_queue, &conv));
  FillBuffers(conv->mutable_buffers(), in);
  std::atomic<bool> shutdown = false;
  conv->Start(&shutdown);

  size_t rows = 0;
  while ((rows != in.size()) && (shutdown.load() == false)) {
    IpcQueueItem item;
    out_queue.wait_dequeue(item);
    rows += RecordSizeOf(item);
    out->push_back(item);
  }

  shutdown.store(true);
  BOLSON_ROE(conv->Finish());
  return Status::OK();
}

/**
 * \brief This function "consumes" messages from two sources, and compares them.
 *
 * The function also tests some other functional requirements.
 *
 * \param ref_data      Reference batches with converted JSONs.
 * \param uut_data      Unit under test batches with converted JSONs.
 * \param schema        The Arrow schema.
 * \param expected_rows The expected number of rows in all batches.
 * \param max_ipc_size  The maximum IPC message size to test.
 */
void ConsumeMessages(const std::vector<IpcQueueItem>& ref_data,
                     const std::vector<IpcQueueItem>& uut_data,
                     const std::shared_ptr<arrow::Schema>& schema,
                     const size_t expected_rows, const size_t max_ipc_size) {
  // Validate schema.

  // [FNC04]: Number fields of the JSON Object only represent and are converted to
  //  unsigned integers of 64 bits.
  // [FNC05]: Text fields of the JSON Object representing timestamps are converted to
  //  Arrow’s “utf8” type, not to Arrow’s “date32” nor to the “date64” type.
  for (const auto& field : schema->fields()) {
    switch (field->type()->id()) {
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
        FAIL() << "Fields must be converted to arrow::uint64 or arrow::utf8";
    }
  }

  // Assert number of output IPC messages is the same for CPU & FPGA impl.
  ASSERT_EQ(ref_data.size(), uut_data.size());

  // Below code implements the functionality of "Consume Messages"

  size_t ref_rows = 0;
  size_t uut_rows = 0;

  // Assert the contents of each batch is the same for CPU & FPGA impl.
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

    // Make sure batch contains some data before comparing.
    ASSERT_TRUE(ref_batch->num_rows() > 0);

    // Both batches should have an equal number of rows.
    ASSERT_TRUE(ref_batch->num_rows() == uut_batch->num_rows());

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