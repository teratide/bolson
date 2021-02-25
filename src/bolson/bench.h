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
#include <illex/client_queueing.h>
#include <illex/document.h>
#include <illex/protocol.h>
#include <putong/timer.h>

#include "bolson/convert/converter.h"
#include "bolson/parse/arrow.h"
#include "bolson/parse/parser.h"
#include "bolson/publish/bench.h"
#include "bolson/publish/publisher.h"
#include "bolson/status.h"
#include "bolson/utils.h"

namespace bolson {

/// Options for the Convert benchmark
struct ConvertBenchOptions {
  /// JSON generator options
  illex::GenerateOptions generate;
  /// Number of JSONs to generate
  size_t num_jsons = 1024;
  /// Converter implementation options.
  convert::ConverterOptions converter;
  /// Latency stats output file. If empty, no latency stats will be written.
  std::string latency_file;
  /// Metrics output file. If empty, no metrics file is written.
  std::string metrics_file;
  /// Number of times to repeat the measurement.
  size_t repeats = 1;
};

/// Options for queue benchmark
struct QueueBenchOptions {
  /// Number of items to queue.
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
  illex::ClientOptions client;
  /// Options for convert bench
  ConvertBenchOptions convert;
  /// Options for Pulsar bench
  publish::BenchOptions pulsar;
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
auto BenchClient(const illex::ClientOptions& opt) -> Status;

/// \brief Run the JSON-to-Arrow conversion benchmark.
auto BenchConvert(const ConvertBenchOptions& opts) -> Status;

/// \brief Generate a bunch of JSONs, returns number of bytes and largest JSON size.
auto GenerateJSONs(size_t num_jsons, const arrow::Schema& schema,
                   const illex::GenerateOptions& gen_opts,
                   std::vector<illex::JSONItem>* items) -> std::pair<size_t, size_t>;

/**
 * \brief Prepare input buffers for benchmarking or testing.
 * \param buffers   The buffers to fill.
 * \param jsons     The JSONs to copy into the buffers.
 */
auto FillBuffers(std::vector<illex::JSONBuffer*> buffers,
                 const std::vector<illex::JSONItem>& jsons) -> Status;

}  // namespace bolson