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

#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <cstring>

#include "./log.h"
#include "./opae_allocator.h"

bool OpaeAllocator::Allocate(size_t size, byte **out) {
#ifndef NDEBUG
  size_t page_size = sysconf(_SC_PAGESIZE);
  size_t alloc_size = ((size / page_size) + (size % page_size > 0)) * page_size;
  posix_memalign(reinterpret_cast<void**>(out), page_size, alloc_size);
  return true;
#else
    if (size != opae_fixed_capacity) {
    spdlog::warn("OpaeAllocator requested to allocate {} bytes, "
                 "but only allows allocating exactly {} bytes for now.",
                 size,
                 opae_fixed_capacity);
  }
  size = opae_fixed_capacity;

  void *addr = mmap(nullptr,
                    size,
                    (PROT_READ | PROT_WRITE),
                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30u << 26)),
                    -1,
                    0);
  if (addr == MAP_FAILED) {
    spdlog::error("OpaeAllocator unable to allocate huge page buffer. "
                  "Errno: " + std::to_string(errno) + " : " + std::strerror(errno));
    return false;
  }
  allocations[addr] = size;

  *out = static_cast<byte *>(addr);
  return true;
#endif
}

bool OpaeAllocator::Free(byte *buffer) {
  free(buffer);
  return true;
}
