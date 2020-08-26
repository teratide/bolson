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

#include "jsongen/log.h"
#include "jsongen/cli.h"
#include "jsongen/file.h"
#include "jsongen/zmq_server.h"
#include "jsongen/arrow.h"

namespace jsongen {

static constexpr auto input_flags = "i,-i,--input";
static constexpr auto input_help = "An Arrow schema to generate the JSON from.";

static constexpr auto seed_flags = "-s,--seed";
static constexpr auto seed_help = "Random generator seed (default: taken from random device).";

static constexpr auto pretty_flags = "--pretty";
static constexpr auto pretty_help = "Generate \"pretty-printed\" JSONs.";

AppOptions::AppOptions(int argc, char **argv) {
  CLI::App app{std::string(AppOptions::name) + ": " + AppOptions::desc};

  std::string schema_file;
  uint16_t stream_port = 0;

  // File generation:
  auto *sub_file = app.add_subcommand("file", "Generate a file with JSONs.");
  sub_file->add_option(input_flags, schema_file, input_help)->required()->check(CLI::ExistingFile);
  sub_file->add_option(seed_flags, file.gen.seed, seed_help);
  sub_file->add_flag(pretty_flags, file.pretty, pretty_help);
  sub_file->add_option("-o,--output", file.out_path, "Output file. JSONs will be written to stdout if not set.");
  sub_file->add_flag("-v", file.verbose, "Print the JSONs to stdout, even if -o or --output is used.");

  // Server mode:
  auto *sub_stream = app.add_subcommand("stream", "Stream raw JSONs over a TCP network socket.");
  sub_stream->add_option(input_flags, schema_file, input_help)->required()->check(CLI::ExistingFile);
  sub_stream->add_option(seed_flags, stream.production.gen.seed, seed_help);
  sub_stream->add_flag(pretty_flags, stream.production.pretty, pretty_help);
  auto *port_opt = sub_stream->add_option("-p,--port",
                                          stream_port,
                                          "Port (default=" + std::to_string(ZMQ_PORT) + ").");
  auto *zmq_flag = sub_stream->add_flag("-z,--zeromq", "Use the ZeroMQ push-pull protocol for the stream.");
  sub_stream->add_option("-m,--num-jsons",
                         stream.production.num_jsons,
                         "Number of JSONs to send (default = 1).");


  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp &e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    return_value = success();
    exit = true;
  } catch (CLI::Error &e) {
    // There is some error.
    spdlog::error("{} : {}", e.get_name(), e.what());
    std::cerr << app.help() << std::endl;
    return_value = failure();
    exit = true;
  }

  if (!ReadSchemaFromFile(schema_file, &schema)) {
    spdlog::error("Could not load Arrow schema.");
    return_value = failure();
    exit = true;
  }

  if (sub_file->parsed()) {
    this->sub = SubCommand::FILE;
    this->file.schema = this->schema;
  } else if (sub_stream->parsed()) {
    this->sub = SubCommand::STREAM;
    this->stream.production.schema = this->schema;

    // Check which streaming protocol to use.
    if (*zmq_flag) {
      ZMQProtocol zmq;
      if (*port_opt) {
        zmq.port = stream_port;
      }
      this->stream.protocol = zmq;
    } else {
      RawProtocol raw;
      if (*port_opt) {
        raw.port = stream_port;
      }
      this->stream.protocol = raw;
    }
  }

}

} // namespace jsongen
