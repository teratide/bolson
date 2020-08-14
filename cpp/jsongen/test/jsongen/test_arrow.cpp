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

#include <unordered_map>
#include <gtest/gtest.h>
#include <rapidjson/writer.h>
#include <arrow/api.h>

#include "jsongen/arrow.h"
#include "jsongen/value.h"

namespace jsongen::test {

static auto GenerateJSON(const arrow::Schema &schema, int seed = 0) -> std::string {
  auto gen = FromSchema(schema, GenerateOptions(seed));
  rapidjson::StringBuffer b;
  rapidjson::Writer p(b);
  gen.Get().Accept(p);
  return std::string(b.GetString());
}

TEST(Arrow, Empty) {
  auto schema = arrow::Schema({});
  ASSERT_EQ(GenerateJSON(schema), R"({})");
}

TEST(Arrow, UInt64) {
  auto schema = arrow::Schema({std::make_shared<arrow::Field>("uint64", arrow::uint64(), false)});
  ASSERT_EQ(GenerateJSON(schema), R"({"uint64":1537163486874432223})");
}

TEST(Arrow, UInt64Meta) {
  std::unordered_map<std::string, std::string> meta = {
      {"JSONGEN_MAX", "3"},
      {"JSONGEN_MIN", "1"}
  };
  auto field = std::make_shared<arrow::Field>("uint64", arrow::uint64(), false)
      ->WithMetadata(std::make_shared<arrow::KeyValueMetadata>(meta));
  auto schema = arrow::Schema({field});
  // Is this test good enough?
  for (int i = 0; i < 64; i++) {
    auto json = GenerateJSON(schema, 0);
    ASSERT_TRUE(json == R"({"uint64":1})" || json == R"({"uint64":2})" || json == R"({"uint64":3})");
  }
}

TEST(Arrow, String) {
  auto schema = arrow::Schema({std::make_shared<arrow::Field>("str", arrow::utf8(), false)});
  ASSERT_EQ(GenerateJSON(schema), R"({"str":"htgwxfziuvfnabo"})");
}

TEST(Arrow, FixedSizeList) {
  auto list_item = std::make_shared<arrow::Field>("item", arrow::uint64(), false);
  auto list_field = std::make_shared<arrow::Field>("fsl", arrow::fixed_size_list(list_item, 3), false);
  auto schema = arrow::Schema({list_field});
  ASSERT_EQ(GenerateJSON(schema), R"({"fsl":[1537163486874432223,18143445020509408007,5528658168453055457]})");
}

}