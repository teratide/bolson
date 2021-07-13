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

#include "bolson/buffer/fpga_allocator.h"

#include <cstring>

namespace bolson::buffer {

auto FpgaAllocator::Allocate(size_t size, std::byte** out) -> Status {
  auto result = posix_memalign(reinterpret_cast<void**>(out), 4096, size);
  if (result != 0) {
    return Status(Error::FletcherError,
                  "Unable to allocate " + std::to_string(size) +
                      " bytes. posix_memalign returned: " + std::to_string(result));
  }

  // Clear the allocated buffer.
  if (std::memset(*out, 0, size) != *out) {
    return Status(Error::FletcherError, "Unable to zero-initialize buffers.");
  }

  return Status::OK();
}

}  // namespace bolson::buffer