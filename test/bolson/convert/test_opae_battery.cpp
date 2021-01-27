#include <gtest/gtest.h>

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

TEST(FPGA, OPAE_BATTERY_8_KERNELS) {
  Status status;

  size_t num_jsons = 1024;

  // Generate a bunch of JSONs
  std::vector<illex::JSONQueueItem> jsons_in;
  auto bytes_largest = GenerateJSONs(num_jsons,
                                     *parse::OpaeBatteryParser::output_schema(),
                                     illex::GenerateOptions(0),
                                     &jsons_in);

  // Set OPAE Converter options.
  Options opae_opts;
  opae_opts.buf_capacity = 1024 * 1024 * 1024;
  opae_opts.num_threads = OPAE_BATTERY_KERNELS;
  opae_opts.num_buffers = OPAE_BATTERY_KERNELS;
  opae_opts.max_batch_rows = 1024;
  opae_opts.implementation = parse::Impl::OPAE_BATTERY;

  // Set Arrow Converter options, using the same options where applicable.
  Options arrow_opts = opae_opts;
  arrow_opts.buf_capacity = 2 * bytes_largest.first;
  arrow_opts.implementation = parse::Impl::ARROW;
  arrow_opts.arrow.read.use_threads = false;
  arrow_opts.arrow.parse.explicit_schema = parse::OpaeBatteryParser::output_schema();

  // Set up IPC queues.
  IpcQueue arrow_queue;
  IpcQueue opae_queue;
  std::vector<IpcQueueItem> arrow_out;
  std::vector<IpcQueueItem> opae_out;

  // Set up converters.
  std::shared_ptr<Converter> arrow_conv;
  std::shared_ptr<Converter> opae_conv;

  SPDLOG_DEBUG("Setting up converters.");

  FAIL_ON_ERROR(Converter::Make(arrow_opts, &arrow_queue, &arrow_conv));
  FAIL_ON_ERROR(Converter::Make(opae_opts, &opae_queue, &opae_conv));

  // Fill buffers with the same data:
  FillBuffers(arrow_conv->mutable_buffers(), jsons_in);
  FillBuffers(opae_conv->mutable_buffers(), jsons_in);

  // Start the converters
  std::atomic<bool> arrow_shutdown = false;
  std::atomic<bool> opae_shutdown = false;
  arrow_conv->Start(&arrow_shutdown);
  opae_conv->Start(&opae_shutdown);

  // Wait for OPAE to finish.
  size_t opae_rows = 0;
  while (opae_rows != num_jsons) {
    IpcQueueItem item;
    opae_queue.wait_dequeue(item);
    SPDLOG_DEBUG("Popped IPC item of {} rows {}/{}", item.num_rows, opae_rows, num_jsons);
    opae_rows += item.num_rows;
    opae_out.push_back(item);
  }
  opae_shutdown.store(true);
  FAIL_ON_ERROR(opae_conv->Finish());

  // Wait for Arrow to finish.
  size_t arrow_rows = 0;
  while (arrow_rows != num_jsons) {
    IpcQueueItem item;
    arrow_queue.wait_dequeue(item);
    SPDLOG_DEBUG("Popped IPC item of {} rows {}/{}", item.num_rows, opae_rows, num_jsons);
    arrow_rows += item.num_rows;
    arrow_out.push_back(item);
  }
  arrow_shutdown.store(true);
  FAIL_ON_ERROR(arrow_conv->Finish());

  // Compare outputs.
  for (size_t i = 0; i < num_jsons; i++) {
    if (!arrow_out[i].ipc->Equals(*opae_out[i].ipc)) {
      FAIL();
    }
  }
}
}
