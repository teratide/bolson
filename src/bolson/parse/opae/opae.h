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

/// Fletcher OPAE FPGA implementations of specific schema parsers
namespace bolson::parse::opae {

/// Address map from host pointer to device pointer.
using AddrMap = std::unordered_map<const std::byte*, da_t>;

/// \brief Extract the host to device address map from the Fletcher context.
auto ExtractAddrMap(fletcher::Context* context) -> AddrMap;

/// \brief Derive AFU ID from base and no. parsers if supplied is empty.
auto DeriveAFUID(const std::string& supplied, const std::string& base, size_t num_parsers,
                 std::string* result) -> Status;

}  // namespace bolson::parse::opae