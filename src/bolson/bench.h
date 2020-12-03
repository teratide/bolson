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
#include <putong/timer.h>
#include <illex/protocol.h>
#include <illex/document.h>

#include "bolson/pulsar.h"
#include "bolson/status.h"
#include "bolson/convert/convert.h"

namespace bolson {

/// Options for the Client interface benchmark
struct ClientBenchOptions {
  /// The hostname of the stream server.
  std::string hostname = "localhost";
  /// The protocol to use.
  illex::StreamProtocol protocol;
};

/// Options for the Convert benchmark
struct ConvertBenchOptions {
  std::shared_ptr<arrow::Schema> schema;
  illex::GenerateOptions generate;
  arrow::json::ParseOptions parse_opts;
  arrow::json::ReadOptions read_opts;
  size_t num_jsons = 1024;
  bool csv = false;
  size_t max_ipc_size = PULSAR_DEFAULT_MAX_MESSAGE_SIZE;
  size_t json_threshold = 1024;
  size_t batch_threshold = (5 * 1024 * 1024) - (32 * 1024);
  size_t num_threads = 1;
  convert::Impl conversion = convert::Impl::CPU;
};

/// Options for Pulsar interface benchmark.
struct PulsarBenchOptions {
  /// Pulsar options
  PulsarOptions pulsar;
  /// Print output as CSV-like line
  bool csv = false;
  /// Number of Pulsar messages
  size_t num_messages = 256;
  /// Size of each message
  size_t message_size = PULSAR_DEFAULT_MAX_MESSAGE_SIZE;
};

/// Options for queue benchmark
struct QueueBenchOptions {
  size_t num_items = 256;
};

/// Possible benchmark subcommands
enum class Bench {
  /// Benchmark the client stream interface
  CLIENT,
  /// Benchmark conversion from JSON to Arrow RecordBatch to Arrow IPC
  CONVERT,
  /// Benchmark the Pulsar interface
  PULSAR,
  /// Benchmark for queues.
  QUEUE
};

/// Benchmark subcommand options
struct BenchOptions {
  /// Chosen subcommand
  Bench bench = Bench::CONVERT;
  /// Options for client bench
  ClientBenchOptions client;
  /// Options for convert bench
  ConvertBenchOptions convert;
  /// Options for Pulsar bench
  PulsarBenchOptions pulsar;
  /// Options for Queue bench
  QueueBenchOptions queue;
};

/**
 * \brief Run benchmark subcommand.
 *
 * This subcommand can be used to test specific components of the pipeline independently.
 *
 * \param opt The options for the benchmark.
 * \return Status::OK() if successful, some error otherwise.
 */
auto RunBench(const BenchOptions &opt) -> Status;

auto BenchClient(const ClientBenchOptions &opt) -> Status;
auto BenchConvert(const ConvertBenchOptions &opt) -> Status;
auto BenchPulsar(const PulsarBenchOptions &opt) -> Status;

}