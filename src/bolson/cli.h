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
#include <iostream>

#include "bolson/bench.h"
#include "bolson/stream.h"

#pragma once

namespace bolson {

/// Possible subcommands to run.
enum class SubCommand {
  NONE,    ///< Run no subcommand.
  STREAM,  ///< Run the stream subcommand.
  BENCH    ///< Run the bench subcommand.
};

/// \brief Application options.
struct AppOptions {
  /// The name of the application.
  static constexpr auto name = "bolson";
  /// A description of the application.
  static constexpr auto desc =
      "Converts raw JSONs to Arrow RecordBatches and publishes them to Pulsar.";

  /// \brief Populate an instance of the application options based on CLI arguments.
  static auto FromArguments(int argc, char* argv[], AppOptions* out) -> Status;

  /// Subcommand to run.
  SubCommand sub = SubCommand::NONE;

  /// Options for the stream subcommand.
  StreamOptions stream;

  /// Options for the bench subcommand.
  BenchOptions bench;
};

}  // namespace bolson
