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

#include <random>
#include <iostream>
#include <algorithm>

#include <CLI/CLI.hpp>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#define TWEET_LENGTH_MAX 280

using std::string;

using rapidjson::Document;
using rapidjson::Value;
using rapidjson::StringBuffer;
using rapidjson::PrettyWriter;

/**
 * Generate a uniform random tweet reference type, either one of "retweeted", "quoted" or "replied_to".
 * @param engine The random engine to use.
 * @return The string expressing the type.
 */
inline auto GetRandomRefTweetType(std::mt19937_64 *engine) -> string {
  switch ((*engine)() % 3) {
    default: return "retweeted";
    case 1: return "quoted";
    case 2: return "replied_to";
  }
}

/**
 * Generate a random date.
 *
 * Does not generate any month days over 28.
 *
 * @param engine The random engine to use.
 * @return The string expressing the date, formatted according to ISO 8601.
 */
inline auto GetRandomDate(std::mt19937_64 *engine) -> string {
  std::uniform_int_distribution<> year(2006, 2020);
  std::uniform_int_distribution<> month(1, 12);
  std::uniform_int_distribution<> day(1, 28);
  std::uniform_int_distribution<> hour(0, 23);
  std::uniform_int_distribution<> minsec(0, 59);
  char buff[25];
  snprintf(buff,
           sizeof(buff),
           "%04d-%02d-%02dT%02d:%02d:%02d.000Z",
           year(*engine),
           month(*engine),
           day(*engine),
           hour(*engine),
           minsec(*engine),
           minsec(*engine));
  return buff;
}

/**
 * Generate random text consisting of digits and A-z, with a length distributed around \p mean with std.dev. \p dev.
 *
 * @param engine The random engine to use.
 * @return The text string.
 */
inline auto GetRandomText(std::mt19937_64 *engine, int mean, int dev) -> string {
  std::normal_distribution<float> len_dist(mean, dev);
  std::uniform_int_distribution<> chars_dist('a', 'z');
  // Generate a random length, but clip between 1 and maximum tweet length.
  string result(std::max(1, std::min(static_cast<int>(len_dist(*engine)), TWEET_LENGTH_MAX)), ' ');
  for (char &c : result) {
    c = chars_dist(*engine);
  }
  return result;
}

/**
 * Generate a random Twitter ID string.
 *
 * @param engine The random engine to use.
 * @return A random Twitter ID string.
 */
inline auto GetRandomTwitterID(std::mt19937_64 *engine) -> string {
  uint64_t value = (*engine)();
  return std::to_string(value);
}

/**
 * Generate a JSON document with random tweets.
 *
 * @param seed       The seed to use for the random generator engine.
 * @param num_tweets The number of Tweets to generate.
 * @param max_refs   The maximum number of Tweets to refer to in some generated Tweet.
 * @param len_mean   The mean of the tweet lengths.
 * @param len_stdev  The std. dev. of the tweet lengths.
 * @return The generated JSON document.
 */
auto GenerateTweets(int seed, size_t num_tweets, size_t max_refs, int len_mean, int len_stdev) -> Document {
  std::mt19937_64 gen(seed);
  Document doc(rapidjson::kObjectType);
  Value tweet_array(rapidjson::kArrayType);
  auto &a = doc.GetAllocator();

  for (int i = 0; i < num_tweets; i++) {

    Value tweet(rapidjson::kObjectType);
    Value data(rapidjson::kObjectType);

    Value ref_tweets(rapidjson::kArrayType);
    Value format("compact");

    auto id_str = GetRandomTwitterID(&gen);
    auto created_at_str = GetRandomDate(&gen);
    auto author_id_str = GetRandomTwitterID(&gen);
    auto text_str = GetRandomText(&gen, len_mean, len_stdev);
    auto in_reply_to_user_id_str = GetRandomTwitterID(&gen);

    Value id(id_str.c_str(), id_str.size(), a);
    Value created_at(created_at_str.c_str(), created_at_str.size(), a);
    Value author_id(author_id_str.c_str(), author_id_str.size(), a);
    Value text(text_str.c_str(), text_str.size(), a);
    Value in_reply_to_user_id(in_reply_to_user_id_str.c_str(), in_reply_to_user_id_str.size(), a);

    data.AddMember("id", id, a);
    data.AddMember("created_at", created_at, a);
    data.AddMember("text", text, a);
    data.AddMember("author_id", author_id, a);
    data.AddMember("in_reply_to_user_id", in_reply_to_user_id, a);

    // Add a bunch of referenced tweets.
    for (int j = 0; j < gen() % (max_refs + 1); j++) {
      Value rt_obj(rapidjson::kObjectType);
      Value rt_type;
      Value rt_id;

      auto rt_type_str = GetRandomRefTweetType(&gen);
      auto rt_id_str = GetRandomTwitterID(&gen);
      rt_type.SetString(rt_type_str.c_str(), rt_type_str.size(), a);
      rt_id.SetString(rt_id_str.c_str(), rt_id_str.size(), a);
      rt_obj.AddMember("type", rt_type, a);
      rt_obj.AddMember("id", rt_id, a);

      ref_tweets.PushBack(rt_obj, a);
    }

    data.AddMember("referenced_tweets", ref_tweets, a);

    tweet.AddMember("data", data, a);
    tweet.AddMember("format", format, a);

    tweet_array.PushBack(tweet, a);
  }

  doc.AddMember("tweets", tweet_array, a);

  return doc;
}

auto main(int argc, char *argv[]) -> int {
  CLI::App app{"An incredibly simple tweet generator."};

  // Options:
  std::random_device rd;
  int seed = rd();
  size_t tweets = 1;
  size_t max_ref_tweets = 2;
  size_t tweet_len_mean = 50;
  size_t tweet_len_stdev = 50;
  std::string output_file;
  bool verbose = false;

  // CLI options:
  app.add_option("-s,--seed", seed, "Random generator seed (default: taken from random device).");

  app.add_option("-n,--no-tweets", tweets, "Number of tweets (default = 1).");
  app.add_option("-u,--length-mean", tweet_len_mean, "Tweet length average.");
  app.add_option("-d,--length-deviation", tweet_len_stdev, "Tweet length deviation.");
  app.add_option("-m,--max-referenced-tweets", max_ref_tweets, "Maximum number of referenced tweets. (default = 2)");

  app.add_option("-o,--output",
                 output_file,
                 "Output file. If set, JSON file will be written there, and not to stdout.");
  app.add_flag("-v", verbose, "Print the JSON output to stdout, even if -o or --output is used.");

  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp &e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    return 0;
  } catch (CLI::Error &e) {
    // There is some error.
    std::cerr << e.get_name() << ":\n" << e.what() << std::endl;
    return -1;
  }

  // Generate the document.
  auto json_tweets = GenerateTweets(seed, tweets, max_ref_tweets, tweet_len_mean, tweet_len_stdev);

  // Write it to a StringBuffer
  StringBuffer buffer;
  PrettyWriter<StringBuffer> writer(buffer);
  json_tweets.Accept(writer);
  const char *output = buffer.GetString();

  // Print it to stdout if requested.
  if (verbose || output_file.empty()) {
    std::cout << output << std::endl;
  }

  // Write it to a file.
  std::ofstream of(output_file);
  of << output << std::endl;

  return 0;
}