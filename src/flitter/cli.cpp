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

#include "flitter/log.h"
#include "flitter/cli.h"
#include "flitter/status.h"

namespace flitter {

void AddCommonOpts(CLI::App *sub, PulsarOptions *pulsar) {
  sub->add_option("-p,--pulsar-url", pulsar->url, "Pulsar broker service URL (default: pulsar://localhost:6650/");
  sub->add_option("-t,--pulsar-topic", pulsar->topic, "Pulsar topic (default: flitter)");
  sub->add_option("-m,--pulsar-max-msg-size",
                  pulsar->max_msg_size,
                  "Pulsar max. message size (default: 5 MiB - 10 KiB)");
}

auto AppOptions::FromArguments(int argc, char **argv, AppOptions *out) -> Status {
  CLI::App app{"Flitter : Exploring Pulsar, Arrow, and FPGA."};

  uint16_t stream_port = 0;

  // CLI options:
  auto *sub_file = app.add_subcommand("file", "Produce Pulsar messages from a JSON file.");
  sub_file->add_option("i,-i,--input",
                       out->file.input,
                       "Input file with Tweets.")->check(CLI::ExistingFile)->required();
  sub_file->add_flag("-s,--succinct-stats", out->file.succinct, "Prints measurements to stdout on a single line.");

  auto *sub_stream = app.add_subcommand("stream", "Produce Pulsar messages from a JSON TCP stream.");
  auto *zmq_flag = sub_stream->add_flag("-z,--zeromq", "Use the ZeroMQ push-pull protocol for the stream.");
  auto *port_opt = sub_stream->add_option("-p,--port",
                                          stream_port,
                                          "Port (default=" + std::to_string(illex::ZMQ_PORT) + ").");

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

  if (sub_file->parsed()) out->sub = SubCommand::FILE;
  else if (sub_stream->parsed()) {
    out->sub = SubCommand::STREAM;

    // Check which streaming protocol to use.
    if (*zmq_flag) {
      illex::ZMQProtocol zmq;
      if (*port_opt) {
        zmq.port = stream_port;
      }
      out->stream.protocol = zmq;
    } else {
      illex::RawProtocol raw;
      if (*port_opt) {
        raw.port = stream_port;
      }
      out->stream.protocol = raw;
    }
  }
  return Status::OK();
}

}  // namespace flitter
