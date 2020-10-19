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

#include <putong/timer.h>

#include "bolson/pulsar.h"
#include "bolson/status.h"

namespace bolson {

struct BenchOptions {
  PulsarOptions pulsar;

  bool csv = false;
  size_t pulsar_messages = 256;
  size_t pulsar_message_size = 5 * 1024 * 1024 - 10 * 1024;
};

auto RunBench(const BenchOptions &opts) -> Status;

auto BenchPulsar(size_t repeats, size_t message_size, const PulsarOptions &opt, bool csv = false) -> Status;

}
