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

#include "flitter/pulsar.h"
#include "flitter/file.h"
#include "flitter/stream.h"

#pragma once

namespace flitter {

/// @brief Application options.
struct AppOptions {
  AppOptions(int argc, char *argv[]);

  static auto failure() -> int { return -1; };
  static auto success() -> int { return 0; };

  enum class SubCommand { FILE, STREAM, BENCH } sub;

  FileOptions file;
  StreamOptions stream;

  bool succinct = false;
  bool exit = false;
  int return_value = 0;
};

}  // namespace flitter
