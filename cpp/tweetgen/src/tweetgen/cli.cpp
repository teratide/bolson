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

namespace tweetgen {

AppOptions::AppOptions(int argc, char **argv) {
  CLI::App app{"Tweetgen: an incredibly simple tweet generator."};

  // File generation:
  auto *sub_file = app.add_subcommand("file", "Generate a JSON file with tweets.");
  sub_file->add_option("-o,--output", file.output, "Output file. JSON file will be written to stdout if not set.");
  sub_file->add_flag("-v", file.verbose, "Print the JSON output to stdout, even if -o or --output is used.");

  sub_file->add_option("-s,--seed", file.gen.seed, "Random generator seed (default: taken from random device).");
  sub_file->add_option("-n,--num-tweets", file.gen.num_tweets, "Number of tweets (default = 1).");
  sub_file->add_option("--length-avg", file.gen.tweet_len_mean, "Tweet length average. (default = 50).");
  sub_file->add_option("--length-dev", file.gen.tweet_len_stdev, "Tweet length std. dev. (default = 50).");
  sub_file->add_option("--max-ref", file.gen.max_ref_tweets, "Maximum no. referenced tweets. (default = 2).");

  // Server mode:
  auto *sub_stream = app.add_subcommand("stream", "Stream JSONs with tweets over the network.");
  sub_stream->add_option("-p,--port", stream.protocol.port, "Port (default=61292).");
  sub_stream->add_option("-m,--num-messages", stream.num_messages, "Number of messages to send (default = 1).");

  sub_stream->add_option("-s,--seed", stream.gen.seed, "Random generator seed (default: taken from random device).");
  sub_stream->add_option("-n,--num-tweets-avg", stream.gen.num_tweets, "Avg. no. tweets per message. (default = 1).");
  sub_stream->add_option("-d,--num-tweets-dev", stream.gen.num_tweets, "Tweets per packet std. dev. (default = 0).");
  sub_stream->add_option("--length-avg", stream.gen.tweet_len_mean, "Tweet length average. (default = 50).");
  sub_stream->add_option("--length-dev", stream.gen.tweet_len_stdev, "Tweet length std. dev. (default = 50).");
  sub_stream->add_option("--max-ref", stream.gen.max_ref_tweets, "Maximum no. referenced tweets. (default = 2).");

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

}

} // namespace tweetgen
