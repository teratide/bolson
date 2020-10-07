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

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "flitter/log.h"
#include "flitter/cli.h"
#include "flitter/status.h"

namespace flitter {

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

void AddCommonOpts(CLI::App *sub, AppOptions *app, std::string *schema_file, PulsarOptions *pulsar) {
  sub->add_option("s,-s,--schema",
                  *schema_file,
                  "An Arrow schema to generate the JSON from.")->required()->check(CLI::ExistingFile);
  sub->add_option("-u,--pulsar-url", pulsar->url, "Pulsar broker service URL (default: pulsar://localhost:6650/");
  sub->add_option("-t,--pulsar-topic", pulsar->topic, "Pulsar topic (default: flitter)");
  sub->add_option("-m,--pulsar-max-msg-size",
                  pulsar->max_msg_size,
                  "Pulsar max. message size (default: 5 MiB - 10 KiB)");
  sub->add_flag("-c", app->succinct, "Print measurements on single CSV-like line.");
}

auto AppOptions::FromArguments(int argc, char **argv, AppOptions *out) -> Status {
  CLI::App app{"Flitter : Exploring Pulsar, Arrow, and FPGA."};

  std::string schema_file;
  uint16_t stream_port = 0;

  // File subcommand:
  auto *sub_file = app.add_subcommand("file", "Produce Pulsar messages from a JSON file.");
  sub_file->add_option("i,-i,--input",
                       out->file.input,
                       "Input file with Tweets.")->check(CLI::ExistingFile)->required();
  AddCommonOpts(sub_file, out, &schema_file, &out->file.pulsar);

  // Stream subcommand:
  auto *sub_stream = app.add_subcommand("stream", "Produce Pulsar messages from a JSON TCP stream.");
  auto *port_opt =
      sub_stream->add_option("-p,--port", stream_port, "Port (default=" + std::to_string(illex::RAW_PORT) + ").");
  sub_stream->add_option("--seq", out->stream.seq, "Starting sequence number, 64-bit unsigned integer (default = 0).");
  AddCommonOpts(sub_stream, out, &schema_file, &out->stream.pulsar);

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

  std::shared_ptr<arrow::Schema> schema;
  auto status = ReadSchemaFromFile(schema_file, &schema);

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
    }
  }
  return status;
}

}  // namespace flitter
