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

#include "bolson/status.h"
#include "bolson/parse/parser.h"

namespace bolson::parse {

auto Parser::AllocateBuffer(size_t size, std::byte **out) -> Status {
  *out = static_cast<std::byte *>(malloc(size));
  if (*out == nullptr) {
    return Status(Error::GenericError,
                  "Unable to allocate " + std::to_string(size) + " bytes.");
  }
  return Status::OK();
}

auto Parser::FreeBuffer(std::byte *buffer) -> Status {
  free(buffer);
  return Status::OK();
}

auto ToString(const Impl &impl) -> std::string {
  switch (impl) {
    case Impl::ARROW: return "Arrow (CPU)";
    case Impl::OPAE_BATTERY: return "OPAE Battery (FPGA)";
  }
  throw std::runtime_error("Corrupt impl.");
}

}
