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

#pragma once

#include <random>
#include <algorithm>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#define TWEET_LENGTH_MAX 280

using std::string;

using rapidjson::Document;
using rapidjson::Value;
using rapidjson::StringBuffer;
using rapidjson::PrettyWriter;

namespace tweetgen {

struct GenerateOptions {
  std::random_device rd;
  int seed = rd();
  size_t num_tweets = 1;
  size_t max_ref_tweets = 2;
  size_t tweet_len_mean = 50;
  size_t tweet_len_stdev = 50;
};

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
auto GenerateTweets(const GenerateOptions& options) -> Document;

} // namespace tweetgen
