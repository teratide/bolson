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

#include <iostream>
#include <fstream>
#include <rapidjson/rapidjson.h>

#include "./file.h"

namespace tweetgen {

auto GenerateFile(const FileOptions &opt) -> int {
  // Generate the document:
  auto json_tweets = GenerateTweets(opt.gen);

  // Write it to a StringBuffer
  StringBuffer buffer;
  rapidjson::PrettyWriter<StringBuffer> writer(buffer);
  json_tweets.Accept(writer);
  const char *output = buffer.GetString();

  // Print it to stdout if requested.
  if (opt.verbose || opt.output.empty()) {
    std::cout << output << std::endl;
  }

  // Write it to a file.
  if (!opt.output.empty()) {
    std::ofstream of(opt.output);
    of << output << std::endl;
  }

  return 0;
}

}  // namespace tweetgen
