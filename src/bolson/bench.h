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
#include <illex/client_queued.h>
#include <illex/document.h>
#include <illex/protocol.h>
#include <putong/timer.h>

#include "bolson/convert/converter.h"
#include "bolson/parse/arrow_impl.h"
#include "bolson/parse/parser.h"
#include "bolson/pulsar.h"
#include "bolson/status.h"
#include "bolson/utils.h"

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
  bool csv = false;
  size_t num_jsons = 1024;
  convert::Options converter;
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
auto RunBench(const BenchOptions& opt) -> Status;

/// \brief Run the TCP client benchmark.
auto BenchClient(const ClientBenchOptions& opt) -> Status;

/// \brief Run the Pulsar producer benchmark.
auto BenchPulsar(const PulsarBenchOptions& opt) -> Status;

/// \brief Run the JSON-to-Arrow conversion benchmark.
auto BenchConvert(ConvertBenchOptions opt) -> Status;

/// \brief Generate a bunch of JSONs, returns number of bytes and largest JSON size.
auto GenerateJSONs(size_t num_jsons, const arrow::Schema& schema,
                   const illex::GenerateOptions& gen_opts,
                   std::vector<illex::JSONQueueItem>* items) -> std::pair<size_t, size_t>;

/**
 * \brief Prepare input buffers for benchmarking or testing.
 * \param buffers   The buffers to fill.
 * \param jsons     The JSONs to copy into the buffers.
 */
void FillBuffers(std::vector<illex::RawJSONBuffer*> buffers,
                 const std::vector<illex::JSONQueueItem>& jsons);

}  // namespace bolson