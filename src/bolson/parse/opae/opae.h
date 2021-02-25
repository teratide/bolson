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

#include <arrow/api.h>
#include <fletcher/context.h>
#include <fletcher/fletcher.h>
#include <fletcher/platform.h>

#include <memory>

#include "bolson/log.h"
#include "bolson/status.h"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s)                                                 \
  {                                                                     \
    auto __status = (s);                                                \
    if (!__status.ok())                                                 \
      return Status(Error::OpaeError, "Fletcher: " + __status.message); \
  }                                                                     \
  void()

/// Fletcher OPAE FPGA implementations of specific schema parsers
namespace bolson::parse::opae {

/// \brief Return the Arrow schema "input: uint8" used as input batch.
auto input_schema() -> std::shared_ptr<arrow::Schema>;

/// Address map from host pointer to device pointer.
using AddrMap = std::unordered_map<const std::byte*, da_t>;

/// \brief Extract the host to device address map from the Fletcher context.
auto ExtractAddrMap(fletcher::Context* context) -> AddrMap;

/// Write MMIO wrapper for debugging.
inline auto WriteMMIO(fletcher::Platform* platform, uint64_t offset, uint32_t value,
                      size_t idx, const std::string& desc = "") -> Status {
  SPDLOG_DEBUG("Parser {:2} | MMIO WRITE 0x{:08X} --> [off:{:4}] [@ 0x{:04X}] {}", idx,
               value, offset, 64 + 4 * offset, desc);
  FLETCHER_ROE(platform->WriteMMIO(offset, value));
  return Status::OK();
}

/// Read MMIO wrapper for debugging.
inline auto ReadMMIO(fletcher::Platform* platform, uint64_t offset, uint32_t* value,
                     size_t idx, const std::string& desc = "") -> Status {
  FLETCHER_ROE(platform->ReadMMIO(offset, value));
  SPDLOG_DEBUG("Parser {:2} | MMIO READ  0x{:08X} <-- [off:{:4}] [@ 0x{:04X}] {}", idx,
               *value, offset, 64 + 4 * offset, desc);
  return Status::OK();
}

/// \brief Derive AFU ID from base and no. parsers if supplied is empty.
auto DeriveAFUID(const std::string& supplied, const std::string& base, size_t num_parsers,
                 std::string* result) -> Status;

}  // namespace bolson::parse::opae