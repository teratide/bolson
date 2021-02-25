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

#include <CLI/CLI.hpp>

#include "bolson/publish/publisher.h"

#pragma once

namespace bolson::publish {

/// Options for Pulsar interface benchmark.
struct BenchOptions {
  /// Pulsar options.
  Options pulsar;
  /// Number of Pulsar messages to publish.
  size_t num_messages = 1;
  /// Size of each message.
  size_t message_size = BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE;
  /// File to dump latency metrics.
  std::string latency_file;
};

void AddPublishBenchToCLI(CLI::App* sub, BenchOptions* out);

/// \brief Run the Pulsar producer benchmark.
auto BenchPulsar(const BenchOptions& opt) -> Status;

}  // namespace bolson::publish