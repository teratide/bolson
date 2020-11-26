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
  typename Clock::time_point start;
  typename Clock::time_point end;

public:
  Timer() : start(Clock::now())
  {
  }
  void stop()
  {
    end = Clock::now();
  }
  double time()
  {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() * 1.0e-9;
  }
};

int main(int argc, char **argv)
{
  fletcher::Status status;

  // open fpga handle
  Timer<> open_fpga_handle;

  static const char *guid = AFU_GUID;
  std::shared_ptr<fletcher::Platform> platform;
  status = fletcher::Platform::Make(PLATFORM, &platform, false);
  if (!status.ok())
  {
    return -1;
  }
  platform->init_data = &guid;
  status = platform->Init();
  if (!status.ok())
  {
    return -1;
  }

  open_fpga_handle.stop();
  std::cerr << "open fpga handle: " << open_fpga_handle.time() << " s" << std::endl;

  // close fpga handle
  Timer<> close_fpga_handle;
  if (!status.ok())
  {
    return -1;
  }

  close_fpga_handle.stop();
  std::cerr << "close fpga handle: " << close_fpga_handle.time() << " s" << std::endl;

  // mmio read -> write -> read latency
  status = fletcher::Platform::Make(PLATFORM, &platform, false);
  if (!status.ok())
  {
    return -1;
  }
  platform->init_data = &guid;
  status = platform->Init();
  if (!status.ok())
  {
    return -1;
  }

  uint32_t value;

  Timer<> mmio_read;
  status = platform->ReadMMIO(5, &value);
  if (!status.ok())
  {
    return -1;
  }
  mmio_read.stop();
  std::cerr << "read mmio: " << mmio_read.time() << " s" << std::endl;

  Timer<> mmio_write;
  status = platform->WriteMMIO(5, 1234);
  if (!status.ok())
  {
    return -1;
  }
  mmio_write.stop();
  std::cerr << "write mmio: " << mmio_write.time() << " s" << std::endl;

  Timer<> read_write_read_mmio;
  status = platform->ReadMMIO(5, &value);
  if (!status.ok())
  {
    return -1;
  }
  status = platform->WriteMMIO(5, 1234);
  if (!status.ok())
  {
    return -1;
  }
  status = platform->ReadMMIO(5, &value);
  if (!status.ok())
  {
    return -1;
  }
  read_write_read_mmio.stop();
  std::cerr << "read -> write -> read mmio: " << read_write_read_mmio.time() << " s" << std::endl;
}
