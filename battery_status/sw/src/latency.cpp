#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <fletcher/api.h>
#include <fletcher/common.h>

#include <malloc.h>
#include <unistd.h>

#include <memory>
#include <iostream>
#include <bitset>

#define AFU_GUID "9ca43fb0-c340-4908-b79b-5c89b4ef5eed";
#define PLATFORM "opae"

static const std::string TINY_RECORD("{\"voltage\": [1]}");
static const std::string SMALL_RECORD("{\"voltage\": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]}");

template <typename Clock = std::chrono::high_resolution_clock>
class Timer
{
private:
  typename Clock::time_point start_point;
  double accumulator;
  int count;

public:
  Timer() : start_point(Clock::now()), accumulator(0.0), count(0)
  {
  }
  void start()
  {
    start_point = Clock::now();
  }
  void stop()
  {
    accumulator += std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start_point).count() * 1.0e-9;
    count++;
  }
  double time() const
  {
    return accumulator / count;
  }
  void print(const std::string &msg) const
  {
    std::printf("%50s: %15.9fs (average over %d iterations)\n", msg.c_str(), time(), count);
  }
};

static auto input_schema() -> std::shared_ptr<arrow::Schema>
{
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("input",
                                   arrow::uint8(),
                                   false)}),
      "input",
      fletcher::Mode::READ);
  return result;
}

static auto output_type() -> std::shared_ptr<arrow::DataType>
{
  static auto result = arrow::list(arrow::field("item", arrow::uint64(), false));
  return result;
}

static auto output_schema() -> std::shared_ptr<arrow::Schema>
{
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("voltage", output_type(), false)}),
      "output",
      fletcher::Mode::WRITE);
  return result;
}

int main(int argc, char **argv)
{
  fletcher::Status status;

  static const char *guid = AFU_GUID;
  std::shared_ptr<fletcher::Platform> platform;

  // open fpga handle
  Timer<> open_fpga_handle;
  Timer<> close_fpga_handle;
  for (int i = 0; i < 1000; i++)
  {

    open_fpga_handle.start();
    fletcher::Platform::Make(PLATFORM, &platform, false);
    platform->init_data = &guid;
    platform->Init();
    open_fpga_handle.stop();

    close_fpga_handle.start();
    platform->Terminate();
    close_fpga_handle.stop();
  }

  open_fpga_handle.print("Open and init Platform");
  close_fpga_handle.print("Terminate Platform");

  // mmio read -> write -> read latency
  fletcher::Platform::Make(PLATFORM, &platform, false);
  platform->init_data = &guid;
  platform->Init();
  uint32_t value;

  Timer<> read_mmio;
  Timer<> write_mmio;
  Timer<> read_write_read_mmio;
  for (int i = 0; i < 1000; i++)
  {
    read_write_read_mmio.start();

    read_mmio.start();
    platform->ReadMMIO(5, &value);
    read_mmio.stop();

    write_mmio.start();
    platform->WriteMMIO(5, 1234);
    write_mmio.stop();

    platform->ReadMMIO(5, &value);
    read_write_read_mmio.stop();
  }

  read_mmio.print("Read MMIO");
  write_mmio.print("Write MMIO");
  read_write_read_mmio.print("RWR MMIO");

  // allocate output buffers
  size_t buffer_size = 4096;
  uint8_t *offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(offset_data, 0, buffer_size);
  auto offset_buffer = std::make_shared<arrow::Buffer>(offset_data, buffer_size);
  uint8_t *value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer);
  auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, offset_buffer, value_array);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  auto output_batch = arrow::RecordBatch::Make(output_schema(), 0, arrays);

  // create input record batches
  arrow::UInt8Builder tiny_record_builder;
  tiny_record_builder.AppendValues({TINY_RECORD.begin(), TINY_RECORD.end()});
  std::shared_ptr<arrow::Array> tiny_record_array;
  tiny_record_builder.Finish(&tiny_record_array);
  auto tiny_record_batch = arrow::RecordBatch::Make(input_schema(), 1, {tiny_record_array});

  arrow::UInt8Builder small_record_builder;
  small_record_builder.AppendValues({SMALL_RECORD.begin(), SMALL_RECORD.end()});
  std::shared_ptr<arrow::Array> small_record_array;
  small_record_builder.Finish(&small_record_array);
  auto small_record_batch = arrow::RecordBatch::Make(input_schema(), 1, {small_record_array});

  // create context
  std::shared_ptr<fletcher::Context> context;
  fletcher::Context::Make(&context, platform);

  // tiny record test
  context->QueueRecordBatch(tiny_record_batch);
  context->QueueRecordBatch(output_batch);
  context->Enable();

  fletcher::Kernel kernel(context);
  kernel.WriteMetaData();

  Timer<> tiny_record;
  for (int i = 0; i < 1000; i++)
  {
    tiny_record.start();
    kernel.Start();
    kernel.PollUntilDone();
    tiny_record.stop();
  }
  tiny_record.print("Tiny record");
}