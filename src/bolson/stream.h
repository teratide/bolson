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

#include <arrow/json/api.h>
#include <blockingconcurrentqueue.h>
#include <illex/protocol.h>

#include <utility>
#include <variant>

#include "bolson/convert/converter.h"
#include "bolson/latency.h"
#include "bolson/parse/parser.h"
#include "bolson/pulsar.h"

namespace bolson {

/// Stream subcommand options.
struct StreamOptions {
  /// The client options.
  illex::ClientOptions client;
  /// The Pulsar options.
  PulsarOptions pulsar;
  /// Enable statistics.
  bool statistics = true;
  /// Latency stats output file. If empty, no latency stats will be written.
  std::string latency_file;
  /// Whether to produce succinct statistics.
  bool succinct = false;
  /// Options related to conversion.
  convert::ConverterOptions converter;
  /// Size of the TCP buffers.
  size_t tcp_buffer_capacity = ILLEX_DEFAULT_TCP_BUFSIZE;
};

/**
 * \brief Produce Pulsar messages from an incoming stream.
 * \param opt The properties of the stream.
 * \return Status::OK() if successful, error otherwise.
 */
auto ProduceFromStream(const StreamOptions& opt) -> Status;

}  // namespace bolson
