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
#include <string>

#include "./document.h"

namespace jsongen {

/// @brief Options for the file subcommand.
struct FileOptions {
  /// The Arrow schema to base the JSON messages on.
  std::shared_ptr<arrow::Schema> schema;
  /// Options for the random generators.
  GenerateOptions gen;
  /// The output file path.
  std::string out_path;
  /// Whether to dump the file to stdout as well.
  bool verbose = false;
  /// Whether to pretty-print the JSON file.
  bool pretty = false;
};

auto GenerateFile(const FileOptions &opt) -> int;

}  // namespace jsongen
