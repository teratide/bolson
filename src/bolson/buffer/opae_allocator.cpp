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

#include "bolson/buffer/opae_allocator.h"

namespace bolson::buffer {

auto OpaeAllocator::Allocate(size_t size, std::byte **out) -> Status {
  // TODO(mbrobbel): explain this
  void *addr = mmap(nullptr,
                    size,
                    (PROT_READ | PROT_WRITE),
                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30u << 26)),
                    -1,
                    0);
  if (addr == MAP_FAILED) {
    return Status(Error::OpaeError,
                  "OpaeAllocator unable to allocate huge page buffer. Errno: "
                      + std::to_string(errno));
  }
  // Clear memory.
  std::memset(addr, 0, size);
  // Add to current allocations.
  allocations[addr] = size;

  *out = static_cast<std::byte *>(addr);
  return Status::OK();
}

auto OpaeAllocator::Free(std::byte *buffer) -> Status {
  auto *addr = static_cast<void *>(buffer);
  size_t size = 0;
  if (allocations.count(addr) > 0) {
    size = allocations[addr];
  }
  if (munmap(addr, size) != 0) {
    return Status(Error::OpaeError,
                  "OpaeAllocator unable to unmap huge page buffer. Errno: "
                      + std::to_string(errno));
  }
  if (allocations.erase(addr) != 1) {
    return Status(Error::OpaeError, "OpaeAllocator unable to erase allocation.");
  }
  return Status::OK();
}

}