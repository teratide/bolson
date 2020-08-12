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
#include <rapidjson/document.h>

#include "./generate.h"

namespace tweetgen {

auto GenerateTweets(const GenerateOptions &options) -> Document {
  std::mt19937_64 gen(options.seed);
  Document doc(rapidjson::kObjectType);
  Value tweet_array(rapidjson::kArrayType);
  auto &a = doc.GetAllocator();

  for (int i = 0; i < options.num_tweets; i++) {

    Value tweet(rapidjson::kObjectType);
    Value data(rapidjson::kObjectType);

    Value ref_tweets(rapidjson::kArrayType);
    Value format("compact");

    auto id_str = GetRandomTwitterID(&gen);
    auto created_at_str = GetRandomDate(&gen);
    auto author_id_str = GetRandomTwitterID(&gen);
    auto text_str = GetRandomText(&gen, options.tweet_len_mean, options.tweet_len_stdev);
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
    for (int j = 0; j < gen() % (options.max_ref_tweets + 1); j++) {
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

} // namespace tweetgen
