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

#include <iostream>

#include <CLI/CLI.hpp>

#pragma once

/// @brief Micro-benchmark options
struct MicroBenchOptions {
  bool tweets_builder = false;

  [[nodiscard]] auto must_run() const -> bool { return tweets_builder; }
};

/// @brief Pulsar options.
struct PulsarOptions {
  std::string url = "pulsar://localhost:6650/";
  std::string topic = "flitter";
  // From an obscure place in the Pulsar sources
  size_t max_message_size = (5 * 1024 * 1024 - (10 * 1024));
};

/// @brief Application options.
struct AppOptions {
  AppOptions(int argc, char *argv[]);

  static auto failure() -> int { return -1; };
  static auto success() -> int { return 0; };

  PulsarOptions pulsar;
  std::string json_file;
  bool succinct = false;
  bool exit = false;
  int return_value = 0;

  // Micro-benchmarks
  MicroBenchOptions micro_bench;
};