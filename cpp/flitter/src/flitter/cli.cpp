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

namespace flitter {

AppOptions::AppOptions(int argc, char **argv) {
  CLI::App app{"Flitter : Exploring Pulsar, Arrow, and FPGA."};

  // CLI options:
  auto *sub_file = app.add_subcommand("file", "Produce Pulsar messages from a JSON file.");
  sub_file->add_option("i,-i,--input", file.input, "Input file with Tweets.")->check(CLI::ExistingFile)->required();
  sub_file->add_option("-p,--pulsar-url",
                       file.pulsar.url,
                       "Pulsar broker service URL (default: pulsar://localhost:6650/");
  sub_file->add_option("-t,--pulsar-topic", file.pulsar.topic, "Pulsar topic (default: flitter)");
  sub_file->add_option("-m,--pulsar-max-msg-size",
                       file.pulsar.max_msg_size,
                       "Pulsar max. message size (default: 5 MiB - 10 KiB)");
  sub_file->add_flag("-s,--succinct-stats", file.succinct, "Prints measurements to stdout on a single line.");

  auto *sub_bench = app.add_subcommand("bench", "Run micro-benchmarks on internals.");
  sub_bench->add_flag("--tweets-builder",
                      bench.tweets_builder,
                      "Run TweetsBuilder microbenchmark. Enabling any microbenchmark flag disables all other functionality.");

  auto *sub_stream = app.add_subcommand("stream", "Produce Pulsar messages from a JSON TCP stream.");

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

  if (sub_file->parsed()) this->sub = SubCommand::FILE;
  else if (sub_stream->parsed()) this->sub = SubCommand::STREAM;
  else if (sub_bench->parsed()) this->sub = SubCommand::BENCH;

}

}  // namespace flitter
