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

#include <malloc.h>
#include <unistd.h>
#include <sys/mman.h>
#include <cstring>

#include <spdlog/spdlog.h>

#include "./opae_allocator.h"

bool OpaeAllocator::Allocate(size_t size, byte **out) {
  size_t page = sysconf(_SC_PAGESIZE);
  if (size != page) {
    spdlog::warn("OpaeAllocator requested to allocate {} bytes, "
                 "but only allows allocating exactly {} bytes for now.",
                 size,
                 page);
  }
  void *addr = memalign(page, page);
  // Clear memory.
  std::memset(addr, 0, size);
  // Add to current allocations.
  allocations[addr] = size;

  *out = static_cast<byte *>(addr);
  return true;
}

bool OpaeAllocator::Free(byte *buffer) {
  free(buffer);
  return true;
}
