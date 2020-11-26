#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <fletcher/api.h>
#include <fletcher/common.h>

#include <malloc.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <memory>
#include <iostream>
#include <bitset>

#define AFU_GUID "9ca43fb0-c340-4908-b79b-5c89b4ef5eed";
#define PLATFORM "opae"
#define ITER_COUNT 1000
#define MULTIPLE 32000
#define HUGE_PAGE_SIZE 1000 * 1024 * 1024

static const std::string TINY_RECORD("{\"voltage\": [1], \"asdf\": \"asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf\"}");
static const std::string SMALL_RECORD("{\"voltage\": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]}");
static const std::string LARGE_RECORD("{\"voltage\": [1234561,1234562,1234563,1234564,1234565,1234566,1234567,1234568,1234569,12345610,12345611,12345612,12345613,12345614,12345615,12345616,12345617,12345618,12345619,12345620,12345621,12345622,12345623,12345624,12345625,12345626,12345627,12345628,12345629,12345630,12345631,12345632,12345633,12345634,12345635,12345636,12345637,12345638,12345639,12345640,12345641,12345642,12345643,12345644,12345645,12345646,12345647,12345648,12345649,12345650,12345651,12345652,12345653,12345654,12345655,12345656,12345657,12345658,12345659,12345660,12345661,12345662,12345663,12345664]}");

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

uint8_t *huge_page()
{
  // credits: https://github.com/torvalds/linux/blob/master/tools/testing/selftests/vm/map_hugetlb.c
  void *addr;
  addr = mmap((void *)(0x0UL), 1000 * 1024 * 1024, (PROT_READ | PROT_WRITE), (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30 << 26)), -1, 0);
  if (addr == MAP_FAILED)
  {
    perror("mmap");
    exit(1);
  }
  return (uint8_t *)addr;
}

int main(int argc, char **argv)
{
  fletcher::Status status;

  static const char *guid = AFU_GUID;
  std::shared_ptr<fletcher::Platform> platform;

  // open fpga handle
  Timer<> open_fpga_handle;
  Timer<> close_fpga_handle;
  for (int i = 0; i < ITER_COUNT; i++)
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
  for (int i = 0; i < ITER_COUNT; i++)
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

  platform->Terminate();

  // allocate output buffers

  // uint8_t *offset_data = (uint8_t *)memalign(2 * 1024 * 1024, buffer_size);
  uint8_t *offset_data = huge_page();
  memset(offset_data, 0, HUGE_PAGE_SIZE);
  auto offset_buffer = std::make_shared<arrow::Buffer>(offset_data, HUGE_PAGE_SIZE);
  uint8_t *value_data = huge_page();
  // uint8_t *value_data = (uint8_t *)memalign(2 * 1024 * 1024, buffer_size);
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, HUGE_PAGE_SIZE);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer);
  auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, offset_buffer, value_array);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  auto output_batch = arrow::RecordBatch::Make(output_schema(), 0, arrays);

  // tiny record
  uint8_t *tiny_record_data = huge_page();
  memcpy(tiny_record_data, TINY_RECORD.data(), TINY_RECORD.size());
  auto tiny_record_buffer = std::make_shared<arrow::Buffer>(tiny_record_data, HUGE_PAGE_SIZE);
  auto tiny_record_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), TINY_RECORD.size(), tiny_record_buffer);
  auto tiny_record_batch = arrow::RecordBatch::Make(input_schema(), TINY_RECORD.size(), {tiny_record_array});

  fletcher::Platform::Make(PLATFORM, &platform, false);
  platform->init_data = &guid;
  platform->Init();
  std::shared_ptr<fletcher::Context> context;
  fletcher::Context::Make(&context, platform);
  context->QueueRecordBatch(tiny_record_batch);
  context->QueueRecordBatch(output_batch);
  context->Enable();
  std::cerr << std::endl;
  fletcher::Kernel kernel(context);
  kernel.WriteMetaData();

  Timer<> tiny_record;
  Timer<> kernel_reset;

  for (int i = 0; i < ITER_COUNT; i++)
  {
    tiny_record.start();
    kernel.Start();
    kernel.PollUntilDone();
    tiny_record.stop();

    kernel_reset.start();
    kernel.Reset();
    kernel_reset.stop();
  }

  tiny_record.print("Tiny record");
  std::cerr << TINY_RECORD.size() / tiny_record.time() << " bytes per second" << std::endl;

  kernel_reset.print("Kernel reset");
  platform->Terminate();

  // small record
  uint8_t *small_record_data = huge_page();
  memcpy(small_record_data, SMALL_RECORD.data(), SMALL_RECORD.size());
  auto small_record_buffer = std::make_shared<arrow::Buffer>(small_record_data, HUGE_PAGE_SIZE);
  auto small_record_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), SMALL_RECORD.size(), small_record_buffer);
  auto small_record_batch = arrow::RecordBatch::Make(input_schema(), SMALL_RECORD.size(), {small_record_array});

  fletcher::Platform::Make(PLATFORM, &platform, false);
  platform->init_data = &guid;
  platform->Init();
  fletcher::Context::Make(&context, platform);
  context->QueueRecordBatch(small_record_batch);
  context->QueueRecordBatch(output_batch);
  context->Enable();
  std::cerr << std::endl;
  kernel = fletcher::Kernel(context);
  kernel.WriteMetaData();

  Timer<> small_record;

  for (int i = 0; i < ITER_COUNT; i++)
  {
    small_record.start();
    kernel.Start();
    kernel.PollUntilDone();
    small_record.stop();
    kernel.Reset();
  }

  small_record.print("Small record");
  std::cerr << SMALL_RECORD.size() / small_record.time() << " bytes per second" << std::endl;
  platform->Terminate();

  // large record
  uint8_t *large_record_data = huge_page();
  memcpy(large_record_data, LARGE_RECORD.data(), LARGE_RECORD.size());
  auto large_record_buffer = std::make_shared<arrow::Buffer>(large_record_data, HUGE_PAGE_SIZE);
  auto large_record_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), LARGE_RECORD.size(), large_record_buffer);
  auto large_record_batch = arrow::RecordBatch::Make(input_schema(), LARGE_RECORD.size(), {large_record_array});

  fletcher::Platform::Make(PLATFORM, &platform, false);
  platform->init_data = &guid;
  platform->Init();
  fletcher::Context::Make(&context, platform);
  context->QueueRecordBatch(large_record_batch);
  context->QueueRecordBatch(output_batch);
  context->Enable();
  std::cerr << std::endl;
  kernel = fletcher::Kernel(context);
  kernel.WriteMetaData();

  Timer<> large_record;

  for (int i = 0; i < ITER_COUNT; i++)
  {
    large_record.start();
    kernel.Start();
    kernel.PollUntilDone();
    large_record.stop();
    kernel.Reset();
  }

  large_record.print("Large record");
  std::cerr << LARGE_RECORD.size() / large_record.time() << " bytes per second" << std::endl;
  platform->Terminate();

  // multiple tiny record
  uint8_t *multiple_tiny_record_data = huge_page();
  memcpy(multiple_tiny_record_data, TINY_RECORD.data(), TINY_RECORD.size());
  for (int i = 0; i < MULTIPLE; i++)
  {
    memcpy(multiple_tiny_record_data + i * TINY_RECORD.size(), TINY_RECORD.data(), TINY_RECORD.size());
  }
  auto multiple_tiny_record_buffer = std::make_shared<arrow::Buffer>(multiple_tiny_record_data, HUGE_PAGE_SIZE);
  auto multiple_tiny_record_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), MULTIPLE * TINY_RECORD.size(), multiple_tiny_record_buffer);
  auto multiple_tiny_record_batch = arrow::RecordBatch::Make(input_schema(), MULTIPLE * TINY_RECORD.size(), {multiple_tiny_record_array});

  fletcher::Platform::Make(PLATFORM, &platform, false);
  platform->init_data = &guid;
  platform->Init();
  fletcher::Context::Make(&context, platform);
  context->QueueRecordBatch(multiple_tiny_record_batch);
  context->QueueRecordBatch(output_batch);
  context->Enable();
  std::cerr << std::endl;
  kernel = fletcher::Kernel(context);
  kernel.WriteMetaData();

  Timer<> multiple_tiny_record;

  for (int i = 0; i < ITER_COUNT; i++)
  {
    multiple_tiny_record.start();
    kernel.Start();
    kernel.PollUntilDone();
    multiple_tiny_record.stop();
    kernel.Reset();
  }

  multiple_tiny_record.print("Multiple tiny records");
  std::cerr << MULTIPLE * TINY_RECORD.size() / multiple_tiny_record.time() << " bytes per second" << std::endl;
  platform->Terminate();

  // multiple small record
  uint8_t *multiple_small_record_data = huge_page();
  memcpy(multiple_small_record_data, SMALL_RECORD.data(), SMALL_RECORD.size());
  for (int i = 0; i < MULTIPLE; i++)
  {
    memcpy(multiple_small_record_data + i * SMALL_RECORD.size(), SMALL_RECORD.data(), SMALL_RECORD.size());
  }
  auto multiple_small_record_buffer = std::make_shared<arrow::Buffer>(multiple_small_record_data, HUGE_PAGE_SIZE);
  auto multiple_small_record_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), MULTIPLE * SMALL_RECORD.size(), multiple_small_record_buffer);
  auto multiple_small_record_batch = arrow::RecordBatch::Make(input_schema(), MULTIPLE * SMALL_RECORD.size(), {multiple_small_record_array});

  fletcher::Platform::Make(PLATFORM, &platform, false);
  platform->init_data = &guid;
  platform->Init();
  fletcher::Context::Make(&context, platform);
  context->QueueRecordBatch(multiple_small_record_batch);
  context->QueueRecordBatch(output_batch);
  context->Enable();
  std::cerr << std::endl;
  kernel = fletcher::Kernel(context);
  kernel.WriteMetaData();

  Timer<> multiple_small_record;

  for (int i = 0; i < ITER_COUNT; i++)
  {
    multiple_small_record.start();
    kernel.Start();
    kernel.PollUntilDone();
    multiple_small_record.stop();
    kernel.Reset();
  }

  multiple_small_record.print("Multiple small records");
  std::cerr << MULTIPLE * SMALL_RECORD.size() / multiple_small_record.time() << " bytes per second" << std::endl;
  platform->Terminate();

  // multiple large record
  uint8_t *multiple_large_record_data = huge_page();
  memcpy(multiple_large_record_data, LARGE_RECORD.data(), LARGE_RECORD.size());
  for (int i = 0; i < MULTIPLE; i++)
  {
    memcpy(multiple_large_record_data + i * LARGE_RECORD.size(), LARGE_RECORD.data(), LARGE_RECORD.size());
  }
  auto multiple_large_record_buffer = std::make_shared<arrow::Buffer>(multiple_large_record_data, HUGE_PAGE_SIZE);
  auto multiple_large_record_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), MULTIPLE * LARGE_RECORD.size(), multiple_large_record_buffer);
  auto multiple_large_record_batch = arrow::RecordBatch::Make(input_schema(), MULTIPLE * LARGE_RECORD.size(), {multiple_large_record_array});

  fletcher::Platform::Make(PLATFORM, &platform, false);
  platform->init_data = &guid;
  platform->Init();
  fletcher::Context::Make(&context, platform);
  context->QueueRecordBatch(multiple_large_record_batch);
  context->QueueRecordBatch(output_batch);
  context->Enable();
  std::cerr << std::endl;
  kernel = fletcher::Kernel(context);
  kernel.WriteMetaData();

  Timer<> multiple_large_record;

  for (int i = 0; i < ITER_COUNT; i++)
  {
    multiple_large_record.start();
    kernel.Start();
    kernel.PollUntilDone();
    multiple_large_record.stop();
    kernel.Reset();
  }

  multiple_large_record.print("Multiple large records");
  std::cerr << MULTIPLE * LARGE_RECORD.size() / multiple_large_record.time() << " bytes per second" << std::endl;
  platform->Terminate();
}