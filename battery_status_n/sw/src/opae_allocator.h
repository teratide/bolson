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
#include <unordered_map>

typedef uint8_t byte;

// TODO: Temporary work-around for limitations to the OPAE platform.
constexpr size_t opae_fixed_capacity = 1024 * 1024 * 1024;

class OpaeAllocator {
 public:
  bool Allocate(size_t size, byte **out);
  bool Free(byte *buffer);
 private:
  std::unordered_map<void *, size_t> allocations;
};
