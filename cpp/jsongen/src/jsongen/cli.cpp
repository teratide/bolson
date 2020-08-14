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

#include <flitter/log.h>

#include "./cli.h"
#include "./file.h"
#include "./stream.h"
#include "./arrow.h"

namespace jsongen {

static constexpr auto input_flags = "i,-i,--input";
static constexpr auto input_help = "An Arrow schema to generate the JSON from.";

static constexpr auto seed_flags = "-s,--seed";
static constexpr auto seed_help = "Random generator seed (default: taken from random device).";

static constexpr auto pretty_flags = "--pretty";
static constexpr auto pretty_help = "Generate \"pretty-printed\" JSON data.";

AppOptions::AppOptions(int argc, char **argv) {
  CLI::App app{"jsongen: a json generator based on Arrow Schemas."};
  std::string schema_file;

  // File generation:
  auto *sub_file = app.add_subcommand("file", "Generate a JSON file.");
  sub_file->add_option(input_flags, schema_file, input_help)->required()->check(CLI::ExistingFile);
  sub_file->add_option(seed_flags, file.gen.seed, seed_help);
  sub_file->add_flag(pretty_flags, file.pretty, pretty_help);
  sub_file->add_option("-o,--output", file.out_path, "Output file. JSON file will be written to stdout if not set.");
  sub_file->add_flag("-v", file.verbose, "Print the JSON output to stdout, even if -o or --output is used.");

  // Server mode:
  auto *sub_stream = app.add_subcommand("stream", "Stream JSONs with tweets over the network.");
  sub_stream->add_option(input_flags, schema_file, input_help)->required()->check(CLI::ExistingFile);
  sub_stream->add_option(seed_flags, stream.gen.seed, seed_help);
  sub_stream->add_flag(pretty_flags, stream.pretty, pretty_help);
  sub_stream->add_option("-p,--port", stream.protocol.port, "Port (default=61292).");
  sub_stream->add_option("-m,--num-messages", stream.num_messages, "Number of messages to send (default = 1).");


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
    this->stream.schema = this->schema;
  }

}

} // namespace jsongen
