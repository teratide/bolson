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
#include <flitter/protocol.h>

#include "./document.h"

namespace jsongen {

/// @brief Options for the stream subcommand.
struct StreamOptions {
  /// The Arrow schema to base the JSON messages on.
  std::shared_ptr<arrow::Schema> schema;
  /// The number of messages to send.
  size_t num_messages = 1;
  /// Options for the random generators.
  GenerateOptions gen;
  /// Properties of the message protocol.
  flitter::StreamProtocol protocol;
  /// Whether to pretty-print the JSON messages.
  bool pretty = false;
};

auto StreamServer(const StreamOptions &opt) -> int;

}  // namespace jsongen
