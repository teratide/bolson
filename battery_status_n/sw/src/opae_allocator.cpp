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

#include <sys/mman.h>
#include <cstring>

#include <spdlog/spdlog.h>

#include "./opae_allocator.h"

bool OpaeAllocator::Allocate(size_t size, byte **out) {
  if (size != opae_fixed_capacity) {
    spdlog::warn("OpaeAllocator requested to allocate {} bytes, "
                 "but only allows allocating exactly {} bytes for now.",
                 size,
                 opae_fixed_capacity);
  }
  size = opae_fixed_capacity;

  void *addr;
  posix_memalign(&addr, 4096, size);
  // TODO(mbrobbel): explain this
//  void *addr = mmap(nullptr,
//                    size,
//                    (PROT_READ | PROT_WRITE),
//                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30u << 26)),
//                    -1,
//                    0);
//  if (addr == MAP_FAILED) {
//    throw std::runtime_error("OpaeAllocator unable to allocate huge page buffer. "
//                             "Errno: " + std::to_string(errno) + " : "
//                                 + std::strerror(errno));
//  }
  // Clear memory.
  std::memset(addr, 0, size);
  // Add to current allocations.
  allocations[addr] = size;

  *out = static_cast<byte *>(addr);
  return true;
}

bool OpaeAllocator::Free(byte *buffer) {
  spdlog::warn("Freeing OPAE buffers on exit. :tm:");
  // TODO: find out why munmap returns an error.

//  auto *addr = static_cast<void *>(buffer);
//  size_t size = 0;
//  if (allocations.count(addr) > 0) {
//    size = allocations[addr];
//  }
//
//  // Temporary work-around.
//  size = g_opae_buffercap;
//
//  if (munmap(addr, size) != 0) {
//    return Status(Error::OpaeError,
//                  "OpaeAllocator unable to unmap huge page buffer. "
//                  "Errno: " + std::to_string(errno) + " : " + std::strerror(errno));
//  }
//  if (allocations.erase(addr) != 1) {
//    return Status(Error::OpaeError, "OpaeAllocator unable to erase allocation.");
//  }
  return true;
}
