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

#include <utility>
#include <variant>

#include <blockingconcurrentqueue.h>
#include <illex/protocol.h>
#include <arrow/json/api.h>

#include "bolson/pulsar.h"
#include "bolson/latency.h"
#include "bolson/convert/convert_queued.h"

namespace bolson {

/// Stream subcommand options.
struct StreamOptions {
  /// The hostname of the stream server.
  std::string hostname = "localhost";
  /// The protocol to use.
  illex::StreamProtocol protocol;
  /// Starting sequence number
  uint64_t seq = 0;
  /// The Arrow JSON parsing options.
  arrow::json::ParseOptions parse;
  /// The Arrow JSON table reader options.
  arrow::json::ReadOptions read;
  /// RecordBatch size threshold before constructing an IPC message.
  /// The default Pulsar message size limit is 5 MiB - 10 KiB.
  /// We subtract 32 KiB to make some room for padding of RecordBatches.
  // TODO: This is just guesswork and should be improved.
  size_t batch_threshold = (5 * 1024 * 1024) - (32 * 1024);
  // TODO: sdfsdfsdf
  size_t json_threshold = 1024;
  /// Number of conversion drone threads to spawn.
  size_t num_threads = 1;
  /// The Pulsar options.
  PulsarOptions pulsar;
  /// Enable statistics.
  bool statistics = true;
  /// Whether to produce succinct statistics.
  bool succinct = false;
  /// The converter implementation to use.
  convert::Impl conversion;
  /// Options related to tracking latency.
  LatencyOptions latency;
};

/**
 * \brief Produce Pulsar messages from an incoming stream.
 * \param opt The properties of the stream.
 * \return Status::OK() if successful, error otherwise.
 */
auto ProduceFromStream(const StreamOptions &opt) -> Status;

auto ProduceFromStreamMultiBuffered(const StreamOptions &opt) -> Status;

}  // namespace bolson
