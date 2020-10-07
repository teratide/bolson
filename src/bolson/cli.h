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
#include <illex/zmq_protocol.h>

#include "bolson/pulsar.h"
#include "bolson/file.h"
#include "bolson/stream.h"

#pragma once

namespace bolson {

enum class SubCommand { NONE, FILE, STREAM };

/// \brief Application options.
struct AppOptions {
  /// The name of the application.
  constexpr static auto name = "bolson";
  /// A description of the application.
  constexpr static auto desc = "Converting JSONs to Arrow IPC messages that get sent to Pulsar.";

  /// \brief Populate an instance of the application options based on CLI arguments.
  static auto FromArguments(int argc, char *argv[], AppOptions *out) -> Status;

  SubCommand sub = SubCommand::NONE;

  FileOptions file;
  StreamOptions stream;

  bool succinct = false;
  bool exit = false;
};

}  // namespace bolson
