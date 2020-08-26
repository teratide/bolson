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

#include <random>
#include <cstdint>
#include <string>
#include <CLI/CLI.hpp>
#include <arrow/api.h>

#include "./file.h"
#include "./zmq_server.h"

namespace jsongen {

/// @brief The subcommands that can be run.
enum class SubCommand { FILE, STREAM };

/// @brief Application options parser.
struct AppOptions {
  /// The name of the application.
  constexpr static auto name = "jsongen";
  /// A description of the application.
  constexpr static auto desc = "A json generator based on Arrow Schemas.";

  /// @brief Construct an instance of the application options parser.
  AppOptions(int argc, char *argv[]);

  /// @brief Returns the return code for failure.
  static auto failure() -> int { return -1; };
  /// @brief Returns the return code for success.
  static auto success() -> int { return 0; };

  /// The subcommand to run.
  SubCommand sub;

  /// The Arrow schema to base generators on.
  std::shared_ptr<arrow::Schema> schema;

  /// The file subcommand parameters.
  FileOptions file;
  /// The stream subcommand parameters.
  StreamOptions stream;

  /// Whether to immediately exit the application after parsing the CLI options.
  bool exit = false;
  /// The return value in case immediate exit is required.
  int return_value = 0;
};

} // namespace jsongen
