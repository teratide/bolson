#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <illex/document.h>

#include "bolson/log.h"
#include "bolson/bench.h"
#include "bolson/convert/converter.h"
#include "bolson/parse/opae_battery_impl.h"

#define OPAE_BATTERY_KERNELS 8

namespace bolson::convert {

#define FAIL_ON_ERROR(status) {               \
  auto __status = (status);                   \
  if (!__status.ok()) {                       \
    throw std::runtime_error(__status.msg()); \
  }                                           \
}

auto test_schema() -> std::shared_ptr<arrow::Schema> {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("voltage",
          arrow::list(arrow::field("item", arrow::uint64(), false)
              ->WithMetadata(arrow::key_value_metadata({"illex_MIN", "ILLEX_MAX"},
                  {"0", "2047"}))),
          false)->WithMetadata(arrow::key_value_metadata({"illex_MIN_LENGTH",
                                                          "ILLEX_MAX_LENGTH"},
          {"1", "16"}))}), "output", fletcher::Mode::WRITE);
  return result;
}

auto GetRecordBatch(std::shared_ptr<arrow::Schema> schema,
                    std::shared_ptr<arrow::Buffer> buffer)
-> std::shared_ptr<arrow::RecordBatch> {
  auto stream = std::dynamic_pointer_cast<arrow::io::InputStream>(
      std::make_shared<arrow::io::BufferReader>(buffer));

  assert(stream != nullptr);

  auto batch = arrow::ipc::ReadRecordBatch(test_schema(),
      nullptr,
      arrow::ipc::IpcReadOptions::Defaults(),
      stream.get()).ValueOrDie();

  return batch;
}

TEST(FPGA, OPAE_BATTERY_8_KERNELS) {
  StartLogger();

  Status status;

  size_t num_jsons = 8;

  // Generate a bunch of JSONs
  std::vector<illex::JSONQueueItem> jsons_in;
  auto bytes_largest = GenerateJSONs(num_jsons,
      *test_schema(),
      illex::GenerateOptions(0),
      &jsons_in);

  // Set OPAE Converter options.
  Options opae_opts;
  opae_opts.buf_capacity = 1024 * 1024 * 1024;
  opae_opts.num_threads = OPAE_BATTERY_KERNELS;
  opae_opts.num_buffers = OPAE_BATTERY_KERNELS;
  opae_opts.max_batch_rows = 1024;
  opae_opts.implementation = parse::Impl::OPAE_BATTERY;
  opae_opts.opae_battery.afu_id = "9ca43fb0-c340-4908-b79b-5c89b4ef5ee8";
  opae_opts.max_ipc_size = 5 * 1024 * 1024 - 10 * 1024;

  // Set Arrow Converter options, using the same options where applicable.
  Options arrow_opts = opae_opts;
  arrow_opts.buf_capacity = 2 * bytes_largest.first;
  arrow_opts.implementation = parse::Impl::ARROW;
  arrow_opts.arrow.read.use_threads = false;
  arrow_opts.arrow.parse.explicit_schema = test_schema();

  // Run Arrow impl.
  IpcQueue arrow_queue;
  std::vector<IpcQueueItem> arrow_out;
  std::shared_ptr<Converter> arrow_conv;
  FAIL_ON_ERROR(Converter::Make(arrow_opts, &arrow_queue, &arrow_conv));
  FillBuffers(arrow_conv->mutable_buffers(), jsons_in);
  std::atomic<bool> arrow_shutdown = false;
  arrow_conv->Start(&arrow_shutdown);

  size_t arrow_rows = 0;
  while ((arrow_rows != num_jsons) && (arrow_shutdown.load() == false)) {
    IpcQueueItem item;
    arrow_queue.wait_dequeue(item);
    arrow_rows += RecordSizeOf(item);
    arrow_out.push_back(item);
  }
  arrow_shutdown.store(true);
  FAIL_ON_ERROR(arrow_conv->Finish());

  // Run OPAE impl.
  IpcQueue opae_queue;
  std::vector<IpcQueueItem> opae_out;
  std::shared_ptr<Converter> opae_conv;
  FAIL_ON_ERROR(Converter::Make(opae_opts, &opae_queue, &opae_conv));
  FillBuffers(opae_conv->mutable_buffers(), jsons_in);
  std::atomic<bool> opae_shutdown = false;
  opae_conv->Start(&opae_shutdown);

  size_t opae_rows = 0;
  while ((opae_rows != num_jsons) && (opae_shutdown.load() == false)) {
    IpcQueueItem item;
    opae_queue.wait_dequeue(item);
    opae_rows += RecordSizeOf(item);
    opae_out.push_back(item);
  }
  opae_shutdown.store(true);
  FAIL_ON_ERROR(opae_conv->Finish());

  // Compare outputs.

  ASSERT_EQ(arrow_rows, opae_rows);

  for (size_t i = 0; i < num_jsons; i++) {
    auto arrow_batch = GetRecordBatch(test_schema(), arrow_out[i].message);
    auto opae_batch = GetRecordBatch(test_schema(), opae_out[i].message);

    std::cout << "Arrow:" << std::endl;
    std::cout << arrow_batch->ToString() << std::endl;
    std::cout << "OPAE:" << std::endl;
    std::cout << opae_batch->ToString() << std::endl;

    ASSERT_TRUE(arrow_batch->Equals(*opae_batch));
  }
}

}
