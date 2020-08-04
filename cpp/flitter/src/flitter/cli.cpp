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

#include "./cli.h"

AppOptions::AppOptions(int argc, char **argv) {
  CLI::App app{"Flitter : doing stuff with tweets in FPGAs and Pulsar"};

  // CLI options:
  app.add_option("i,-i,--input", json_file, "Input file with Tweets.")->check(CLI::ExistingFile)->required();
  app.add_option("-p,--pulsar-url", pulsar.url, "Pulsar broker service URL (default: pulsar://localhost:6650/");
  app.add_option("-t,--pulsar-topic", pulsar.topic, "Pulsar topic (default: flitter)");
  app.add_option("-m,--pulsar-max-message-size",
                 pulsar.max_message_size,
                 "Pulsar maximum message size (default: 5 MiB - 10 KiB)");
  app.add_flag("-s,--succinct-stats", succinct, "Prints measurements to stdout on a single line.");

  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp &e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    exit = true;
  } catch (CLI::Error &e) {
    // There is some error.
    std::cerr << e.get_name() << ":\n" << e.what() << std::endl;
    std::cerr << app.help() << std::endl;
    exit = true;
    return_value = -1;
  }
}
