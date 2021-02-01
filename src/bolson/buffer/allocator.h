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

#pragma once

#include <cstddef>
#include <cstdint>

#include "bolson/status.h"

/// Contains all constructs to handle buffer allocation.
namespace bolson::buffer {

/**
 * \brief Abstract class for memory allocators.
 *
 * This can be used to plug in custom allocators to enable e.g. FPGA processing.
 */
class Allocator {
 public:
  /**
   * \brief Allocate memory.
   * \param size The number of bytes to allocate.
   * \param out A pointer to the pointer of the allocated memory.
   * \return Status::OK() if successful, some error otherwise.
   */
  virtual auto Allocate(size_t size, std::byte** out) -> Status;

  /**
   * \brief Free memory.
   * \param buffer A pointer to the allocated memory.
   * \return Status::OK() if successful, some error otherwise.
   */
  virtual auto Free(std::byte* buffer) -> Status;
};

}  // namespace bolson::buffer