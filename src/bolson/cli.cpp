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

#include <algorithm>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "bolson/log.h"
#include "bolson/cli.h"
#include "bolson/status.h"

namespace bolson {

/// Function to read schema from a file.
static auto ReadSchemaFromFile(const std::string &file, std::shared_ptr<arrow::Schema> *out) -> Status {
  // TODO(johanpel): use filesystem lib for path
  auto rfos = arrow::io::ReadableFile::Open(file);

  if (!rfos.ok()) { return Status(Error::IOError, rfos.status().message()); }
  auto fis = rfos.ValueOrDie();

  // Dictionaries are not supported yet, hence nullptr. If there are actual dictionaries, the function will return an
  // error status, which is propagated to the caller of this function.
  auto rsch = arrow::ipc::ReadSchema(fis.get(), nullptr);

  if (rsch.ok()) { *out = rsch.ValueOrDie(); }
  else { return Status(Error::IOError, rsch.status().message()); }

  auto status = fis->Close();

  return Status::OK();
}

/// Create a mock IPC message to figure out the IPC message header size.
static auto CreateMockIPC(const std::shared_ptr<arrow::Schema> &schema, std::shared_ptr<arrow::Buffer> *out) -> Status {
  // Construct an empty RecordBatch from the schema.
  std::unique_ptr<arrow::RecordBatchBuilder> mock_builder;
  std::shared_ptr<arrow::RecordBatch> mock_batch;
  // TODO: Error check the Arrow results and statuses below
  arrow::Status status = arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), &mock_builder);
  // todo: write status converters from arrow, and perhaps the other libs also
  if (!status.ok()) { return Status(Error::ArrowError, status.message()); }
  status = mock_builder->Flush(&mock_batch);
  if (!status.ok()) { return Status(Error::ArrowError, status.message()); }
  auto result = arrow::ipc::SerializeRecordBatch(*mock_batch, arrow::ipc::IpcWriteOptions::Defaults());
  if (!result.ok()) { return Status(Error::ArrowError, result.status().message()); }
  (*out) = result.ValueOrDie();
  return Status::OK();
}

void AddCommonOpts(CLI::App *sub, AppOptions *app, std::string *schema_file, PulsarOptions *pulsar) {
  // An arrow schema is required:
  sub->add_option("input,-i,--input",
                  *schema_file,
                  "The Arrow schema to generate the JSON from.")->required()->check(CLI::ExistingFile);
  sub->add_option("-u,--pulsar-url", pulsar->url, "Pulsar broker service URL (default: pulsar://localhost:6650/");
  sub->add_option("-t,--pulsar-topic", pulsar->topic, "Pulsar topic (default: bolson)");
  sub->add_option("-r,--pulsar-max-msg-size",
                  pulsar->max_msg_size,
                  "Pulsar max. message size (default: 5 MiB - 10 KiB)");
  sub->add_flag("-c", app->succinct, "Print measurements on single CSV-like line.");
}

auto AppOptions::FromArguments(int argc, char **argv, AppOptions *out) -> Status {
  std::string schema_file;
  uint16_t stream_port = 0;

  CLI::App app{"bolson : A JSON stream to Arrow IPC to Pulsar conversion and publish tool."};

  app.require_subcommand();

  // File subcommand:
  auto *sub_file = app.add_subcommand("file", "Produce Pulsar messages from a JSON file.");
  sub_file->add_option("f,-f,--file",
                       out->file.input,
                       "Input file with Tweets.")->check(CLI::ExistingFile)->required();
  AddCommonOpts(sub_file, out, &schema_file, &out->file.pulsar);

  // Stream subcommand:
  auto *sub_stream = app.add_subcommand("stream", "Produce Pulsar messages from a JSON TCP stream.");
  auto *port_opt =
      sub_stream->add_option("-p,--port", stream_port, "Port (default=" + std::to_string(illex::RAW_PORT) + ").");
  sub_stream->add_option("--seq", out->stream.seq, "Starting sequence number, 64-bit unsigned integer (default = 0).");
  AddCommonOpts(sub_stream, out, &schema_file, &out->stream.pulsar);

  // Bench subcommand:
  auto *sub_bench = app.add_subcommand("bench", "Run some microbenchmarks.");
  sub_bench->add_option("--message-size", out->bench.pulsar_message_size, "Pulsar message size.");
  sub_bench->add_option("--num-messages", out->bench.pulsar_messages, "Pulsar number of messages.");
  AddCommonOpts(sub_bench, out, &schema_file, &out->bench.pulsar);

  //auto *zmq_flag = sub_stream->add_flag("-z,--zeromq", "Use the ZeroMQ push-pull protocol for the stream.");

  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp &e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    return Status::OK();
  } catch (CLI::Error &e) {
    // There is some CLI error.
    std::cerr << app.help() << std::endl;
    return Status(Error::CLIError, e.get_name() + ":" + e.what());
  }

  // Read the Arrow schema
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::Buffer> mock_ipc;
  BOLSON_ROE(ReadSchemaFromFile(schema_file, &schema));
  BOLSON_ROE(CreateMockIPC(schema, &mock_ipc));

  auto parse_options = arrow::json::ParseOptions::Defaults();
  parse_options.explicit_schema = schema;
  parse_options.unexpected_field_behavior = arrow::json::UnexpectedFieldBehavior::Error;

  if (sub_file->parsed()) {
    out->sub = SubCommand::FILE;
    out->file.succinct = out->succinct;
  } else if (sub_stream->parsed()) {
    out->sub = SubCommand::STREAM;

    // Check which streaming protocol to use.
//    if (*zmq_flag) {
//      illex::ZMQProtocol zmq;
//      if (*port_opt) {
//        zmq.port = stream_port;
//      }
//      out->stream.protocol = zmq;
//    } else
    {
      illex::RawProtocol raw;
      if (*port_opt) {
        raw.port = stream_port;
      }
      out->stream.protocol = raw;
      out->stream.succinct = out->succinct;
      out->stream.parse = parse_options;

      ssize_t desired_threshold = static_cast<ssize_t>(out->stream.pulsar.max_msg_size) - mock_ipc->size();

      if (desired_threshold < 0) {
        spdlog::warn(
            "Arrow IPC header size as result of supplied Arrow schema is larger than supplied Pulsar maximum message "
            "size. Setting batch threshold to 1 byte.");
      }

      out->stream.batch_threshold = static_cast<size_t>(std::max(1L, desired_threshold));
    }
  } else if (sub_bench->parsed()) {
    out->sub = SubCommand::BENCH;
    out->bench.csv = out->succinct;
  }

  return Status::OK();
}

}  // namespace bolson
