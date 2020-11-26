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
    mmio_write.stop();

    platform->ReadMMIO(5, &value);
    read_write_read_mmio.stop();
  }

  read_mmio.print("Read MMIO");
  write_mmio.print("Write MMIO");
  read_write_read_mmio.print("RWR MMIO");

  // // create context
  // std::shared_ptr<fletcher::Context> context;
  // fletcher::Context::Make(&context, platform);
  // context->QueueRecordBatch(input_batch);
  // context->QueueRecordBatch(output_batch);
}
