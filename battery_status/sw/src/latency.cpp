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

int main(int argc, char **argv)
{
  fletcher::Status status;

  // open fpga handle
  auto start = std::chrono::high_resolution_clock::now();

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

  auto end = std::chrono::high_resolution_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::microseconds>(start - end).count();
  std::cerr << "open fpga handle: " << time << " us" << std::endl;

  // close fpga handle
  start = std::chrono::high_resolution_clock::now();
  status = platform->Terminate();
  if (!status.ok())
  {
    return -1;
  }

  end = std::chrono::high_resolution_clock::now();
  time = std::chrono::duration_cast<std::chrono::microseconds>(start - end).count();
  std::cerr << "close fpga handle: " << time << " us" << std::endl;

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

  start = std::chrono::high_resolution_clock::now();
  status = platform->ReadMMIO(5, &value);
  if (!status.ok())
  {
    return -1;
  }
  end = std::chrono::high_resolution_clock::now();
  time = std::chrono::duration_cast<std::chrono::microseconds>(start - end).count();
  std::cerr << "read mmio: " << time << " us" << std::endl;

  start = std::chrono::high_resolution_clock::now();
  status = platform->WriteMMIO(5, 1234);
  if (!status.ok())
  {
    return -1;
  }
  end = std::chrono::high_resolution_clock::now();
  time = std::chrono::duration_cast<std::chrono::microseconds>(start - end).count();
  std::cerr << "write mmio: " << time << " us" << std::endl;

  start = std::chrono::high_resolution_clock::now();
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
  end = std::chrono::high_resolution_clock::now();
  time = std::chrono::duration_cast<std::chrono::microseconds>(start - end).count();
  std::cerr << "read -> write -> read mmio: " << time << " us" << std::endl;
}
