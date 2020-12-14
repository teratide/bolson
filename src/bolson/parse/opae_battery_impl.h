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

#include <memory>
#include <utility>

#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include "bolson/stream.h"

#define FPGA_PLATFORM "opae"
#define OPAE_BATTERY_AFU_ID "9ca43fb0-c340-4908-b79b-5c89b4ef5eed"

namespace bolson::parse {

struct OPAEBatteryOptions {
  std::string afu_id = OPAE_BATTERY_AFU_ID;
  size_t seq_buffer_init_size = 1024;
  size_t str_buffer_init_size = 16 * 1024 * 1024;
  size_t input_capacity = 1000 * 1024 * 1024;
  size_t output_capacity_off = 1000 * 1024 * 1024;
  size_t output_capacity_val = 1000 * 1024 * 1024;
};

}