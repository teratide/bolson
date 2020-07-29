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

#include <arrow/api.h>

using std::vector;
using std::shared_ptr;
using std::make_shared;
using std::string;
using std::pair;

using arrow::Schema;
using arrow::Field;
using arrow::UInt64Builder;
using arrow::UInt8Builder;
using arrow::Date64Builder;
using arrow::StringBuilder;
using arrow::StructBuilder;
using arrow::ListBuilder;
using arrow::RecordBatch;
using arrow::DataType;
using arrow::ArrayBuilder;
using arrow::Status;
using arrow::Array;

class TweetsBuilder {
 public:
  TweetsBuilder();

  static auto schema() -> shared_ptr<Schema>;

  auto Append(uint64_t id,
              const string &created_at,
              const string &text,
              uint64_t author_id,
              uint64_t in_reply_to_user_id,
              const vector<pair<uint8_t, uint64_t>> &referenced_tweets) -> Status;

  auto Finish(std::shared_ptr<RecordBatch>* batch) -> Status;

 private:
  shared_ptr<UInt8Builder> id_;
  shared_ptr<StringBuilder> created_at_;
  shared_ptr<StringBuilder> text_;
  shared_ptr<UInt64Builder> author_id_;
  shared_ptr<UInt64Builder> in_reply_to_user_id_;
  shared_ptr<UInt8Builder> ref_tweets_type_;
  shared_ptr<UInt64Builder> ref_tweets_id_;
  shared_ptr<StructBuilder> ref_tweets_struct_;
  shared_ptr<ListBuilder> ref_tweets_;
  shared_ptr<Schema> schema_;

  static auto rt_fields() -> vector<shared_ptr<Field>>;
  static auto rt_struct() -> shared_ptr<DataType>;
};
