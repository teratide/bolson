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

#include "bolson/cli.h"

#include <arrow/io/api.h>

#include <algorithm>

#include "bolson/convert/converter.h"
#include "bolson/parse/implementations.h"
#include "bolson/publish/publisher.h"
#include "bolson/status.h"

namespace bolson {

static void AddClientOpts(CLI::App* sub, illex::ClientOptions* client) {
  sub->add_option("--host", client->host, "JSON source TCP server hostname.")
      ->default_val("localhost");
  sub->add_option("--port", client->port, "JSON source TCP server port.")
      ->default_val(ILLEX_DEFAULT_PORT);
}

static void AddConvertOpts(CLI::App* sub, convert::ConverterOptions* opts) {
  sub->add_option("--max-rows", opts->max_batch_rows,
                  "Maximum number of rows per RecordBatch.")
      ->default_val(1024);
  sub->add_option("--max-ipc", opts->max_ipc_size,
                  "Maximum size of IPC messages in bytes.")
      ->default_val(BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE);
  sub->add_option("--threads", opts->num_threads,
                  "Number of threads to use for conversion.")
      ->default_val(1);
  AddParserOptions(sub, &opts->parser);
}

static void AddPublishOpts(CLI::App* sub, publish::Options* pulsar) {
  sub->add_option("-u,--pulsar-url", pulsar->url, "Pulsar broker service URL.")
      ->default_val("pulsar://localhost:6650/");
  sub->add_option("-t,--pulsar-topic", pulsar->topic, "Pulsar topic.")
      ->default_val("non-persistent://public/default/bolson");

  sub->add_option("--pulsar-max-msg-size", pulsar->max_msg_size)
      ->default_val(BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE);

  sub->add_option("--pulsar-producers", pulsar->num_producers,
                  "Number of concurrent Pulsar producers.")
      ->default_val(1);

  // Pulsar batching defaults taken from the Pulsar CPP client sources.
  // pulsar-client-cpp/lib/ProducerConfigurationImpl.h
  sub->add_flag("--pulsar-batch", pulsar->batching.enable,
                "Enable batching Pulsar producer(s).");
  sub->add_option("--pulsar-batch-max-messages", pulsar->batching.max_messages,
                  "Pulsar batching max. messages.")
      ->default_val(1000);
  sub->add_option("--pulsar-batch-max-bytes", pulsar->batching.max_bytes,
                  "Pulsar batching max. bytes.")
      ->default_val(128 * 1024);
  sub->add_option("--pulsar-batch-max-delay", pulsar->batching.max_delay_ms,
                  "Pulsar batching max. delay (ms).")
      ->default_val(10);
}

static void AddBenchOpts(CLI::App* bench, BenchOptions* out) {
  // 'bench client' subcommand.
  auto* bench_client =
      bench->add_subcommand("client", "Run TCP client interface microbenchmark.");
  AddClientOpts(bench_client, &out->client);

  // 'bench convert' subcommand.
  auto* bench_conv =
      bench->add_subcommand("convert", "Run JSON to Arrow IPC convert microbenchmark.");
  AddConvertOpts(bench_conv, &out->convert.converter);
  bench_conv
      ->add_option("--num-jsons", out->convert.num_jsons, "Number of JSONs to convert.")
      ->default_val(1024);
  bench_conv->add_option("--seed", out->convert.generate.seed, "Generation seed.")
      ->default_val(0);

  // 'bench pulsar' subcommand.
  auto* bench_pulsar = bench->add_subcommand("pulsar", "Run Pulsar microbenchmark.");
  bench_pulsar->add_option("-s", out->pulsar.message_size, "Pulsar message size.")
      ->default_val(BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE);
  bench_pulsar->add_option("-m", out->pulsar.num_messages, "Pulsar number of messages.")
      ->default_val(1024);
  AddPublishOpts(bench_pulsar, &out->pulsar.pulsar);

  // 'bench queue' subcommand
  auto* bench_queue = bench->add_subcommand("queue", "Run queue microbenchmark.");
  bench_queue->add_option("m,-m,--num-items,", out->queue.num_items)->default_val(256);
}

auto AppOptions::FromArguments(int argc, char** argv, AppOptions* out) -> Status {
  bool csv = false;

  CLI::App app{"bolson : A JSON to Arrow IPC converter and Pulsar publishing tool."};

  app.require_subcommand();
  app.get_formatter()->column_width(50);

  // 'stream' subcommand:
  auto* stream =
      app.add_subcommand("stream", "Produce Pulsar messages from a JSON TCP stream.");
  stream->add_option("--latency", out->stream.latency_file,
                     "Enable batch latency measurements and write to supplied file.");
  AddConvertOpts(stream, &out->stream.converter);
  AddPublishOpts(stream, &out->stream.pulsar);
  AddClientOpts(stream, &out->stream.client);

  // 'bench' subcommand:
  auto* bench =
      app.add_subcommand("bench", "Run micro-benchmarks on isolated pipeline stages.")
          ->require_subcommand();
  AddBenchOpts(bench, &out->bench);

  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp& e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    return Status::OK();
  } catch (CLI::Error& e) {
    // There is some CLI error.
    std::cerr << app.help() << std::endl;
    return Status(Error::CLIError, "CLI Error: " + e.get_name() + ":" + e.what());
  }

  if (stream->parsed()) {
    out->sub = SubCommand::STREAM;
    out->stream.succinct = csv;

  } else if (bench->parsed()) {
    out->sub = SubCommand::BENCH;
    if (bench->get_subcommand_ptr("client")->parsed()) {
      out->bench.bench = Bench::CLIENT;
    } else if (bench->get_subcommand_ptr("convert")->parsed()) {
      out->bench.bench = Bench::CONVERT;
      out->bench.convert.csv = csv;
    } else if (bench->get_subcommand_ptr("pulsar")->parsed()) {
      out->bench.bench = Bench::PULSAR;
      out->bench.pulsar.csv = csv;
    } else if (bench->get_subcommand_ptr("queue")->parsed()) {
      out->bench.bench = Bench::QUEUE;
    }
  }

  return Status::OK();
}

}  // namespace bolson
