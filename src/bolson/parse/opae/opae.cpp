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

#include "bolson/parse/opae/opae.h"

#include <arrow/api.h>
#include <fletcher/common.h>
#include <fletcher/context.h>

#include <memory>

namespace bolson::parse::opae {

auto ExtractAddrMap(fletcher::Context* context)
    -> std::unordered_map<const std::byte*, da_t> {
  AddrMap result;
  // Workaround to obtain buffer device address.
  for (size_t i = 0; i < context->num_buffers(); i++) {
    const auto* ha =
        reinterpret_cast<const std::byte*>(context->device_buffer(i).host_address);
    auto da = context->device_buffer(i).device_address;
    result[ha] = da;
  }
  return result;
}

auto DeriveAFUID(const std::string& supplied, const std::string& base, size_t num_parsers,
                 std::string* result) -> Status {
  // AFU IDs are 36 chars long, with the last two chars reserved for the number of
  // parsers when using this function.
  assert(base.length() == 34);
  // Attempt to derive AFU ID if not supplied.
  std::string afu_id_str;
  if (supplied.empty()) {
    if (num_parsers > 255) {
      return Status(
          Error::OpaeError,
          "Auto-deriving AFU ID for number of parsers larger than 255 is not supported.");
    }
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << std::hex << num_parsers;
    // AFU ID from the hardware design script.
    afu_id_str = base + ss.str();
  } else {
    afu_id_str = supplied;
  }
  *result = afu_id_str;
  return Status::OK();
}

}  // namespace bolson::parse::opae