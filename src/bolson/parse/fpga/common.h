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

#include <fletcher/api.h>

#include "bolson/log.h"
#include "bolson/status.h"

namespace bolson::parse::fpga {

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s)                                                           \
  {                                                                               \
    auto __status = (s);                                                          \
    if (!__status.ok())                                                           \
      return Status(Error::FletcherError, "Fletcher error: " + __status.message); \
  }                                                                               \
  void()

/// \brief Return the Arrow schema "input: uint8" used as input batch.
auto raw_json_input_schema() -> std::shared_ptr<arrow::Schema>;

/// Read MMIO wrapper for debugging.
inline auto ReadMMIO(fletcher::Platform* platform, uint64_t offset, uint32_t* value,
                     size_t idx, const std::string& desc = "") -> Status {
  FLETCHER_ROE(platform->ReadMMIO(offset, value));
  SPDLOG_DEBUG("Parser {:2} | MMIO READ  0x{:08X} <-- [off:{:4}] [@ 0x{:04X}] {}", idx,
               *value, offset, 4 * offset, desc);
  return Status::OK();
}

/// Write MMIO wrapper for debugging.
inline auto WriteMMIO(fletcher::Platform* platform, uint64_t offset, uint32_t value,
                      size_t idx, const std::string& desc = "", bool read_back = false)
    -> Status {
  SPDLOG_DEBUG("Parser {:2} | MMIO WRITE 0x{:08X} --> [off:{:4}] [@ 0x{:04X}] {}", idx,
               value, offset, 4 * offset, desc);
  FLETCHER_ROE(platform->WriteMMIO(offset, value));
  if (read_back) {
    uint32_t read_back_value = 0;
    ReadMMIO(platform, offset, &read_back_value, idx, desc + " (read back)");
  }
  return Status::OK();
}

}  // namespace bolson::parse::fpga