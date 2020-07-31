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

#include "./tweets.h"

using std::vector;
using std::string;
using std::pair;
using std::shared_ptr;
using std::make_shared;

using arrow::DataType;
using arrow::Field;
using arrow::Schema;
using arrow::Array;
using arrow::RecordBatch;
using arrow::UInt8Builder;
using arrow::UInt64Builder;
using arrow::StringBuilder;
using arrow::StructBuilder;
using arrow::ListBuilder;
using arrow::ArrayBuilder;
using arrow::Status;

TweetsBuilder::TweetsBuilder() {
  // Set up the schema first.
  // For ID's, Twitter uses string types to not get in trouble with JavaScript, but we can use
  // 64-bit unsigned ints; it should be enough to store those IDs.
  schema_ = schema();

  // Set up builders for all fields.
  id_ = make_shared<UInt8Builder>();
  // TODO(johanpel): convert to date64
  //created_at_ = make_shared<Date64Builder>();
  created_at_ = make_shared<StringBuilder>();
  text_ = make_shared<StringBuilder>();
  author_id_ = make_shared<UInt64Builder>();
  in_reply_to_user_id_ = make_shared<UInt64Builder>();

  // Builders for individual fields of referenced_tweets:
  ref_tweets_type_ = make_shared<UInt8Builder>();
  ref_tweets_id_ = make_shared<UInt64Builder>();
  // Builder for each struct:
  ref_tweets_struct_ = make_shared<StructBuilder>(rt_struct(),
                                                  arrow::default_memory_pool(),
                                                  vector<shared_ptr<ArrayBuilder>>({ref_tweets_type_,
                                                                                    ref_tweets_id_}));
  // Builder for the referenced_tweets field that is a JSON array that we will store as an Arrow list.
  ref_tweets_ = make_shared<ListBuilder>(arrow::default_memory_pool(), ref_tweets_struct_, rt_struct());
}

auto TweetsBuilder::Append(uint64_t id,
                           const string &created_at,
                           const string &text,
                           uint64_t author_id,
                           uint64_t in_reply_to_user_id,
                           const vector<pair<uint8_t, uint64_t>> &referenced_tweets) -> Status {
  ARROW_RETURN_NOT_OK(this->id_->Append(id));
  ARROW_RETURN_NOT_OK(this->created_at_->Append(created_at));
  ARROW_RETURN_NOT_OK(this->text_->Append(text));
  ARROW_RETURN_NOT_OK(this->author_id_->Append(author_id));
  ARROW_RETURN_NOT_OK(this->in_reply_to_user_id_->Append(in_reply_to_user_id));

  // Reserve a new list slot.
  ARROW_RETURN_NOT_OK(ref_tweets_->Append());

  for (const auto &p : referenced_tweets) {
    ARROW_RETURN_NOT_OK(ref_tweets_type_->Append(p.first));
    ARROW_RETURN_NOT_OK(ref_tweets_id_->Append(p.second));
    ARROW_RETURN_NOT_OK(ref_tweets_struct_->Append());
  }

  return Status::OK();
}

auto TweetsBuilder::schema() -> shared_ptr<Schema> {
  return make_shared<Schema>(
      vector<shared_ptr<Field>>({make_shared<Field>("id", arrow::uint64(), false),
                                 make_shared<Field>("created_at", arrow::utf8(), false),
                                 make_shared<Field>("text", arrow::utf8(), false),
                                 make_shared<Field>("author_id", arrow::uint64(), false),
                                 make_shared<Field>("in_reply_to_user_id", arrow::uint64(), false),
                                 make_shared<Field>("referenced_tweets", arrow::list(rt_struct()), false),
                                })
  );
}

auto TweetsBuilder::rt_fields() -> vector<shared_ptr<Field>> {
  // TODO(johanpel): we don't support unions in hardware at the moment,
  //  so we just use UInt8 for the referenced_tweets.type enum.
  auto fields = vector<shared_ptr<Field>>({make_shared<Field>("type", arrow::uint8(), false),
                                           make_shared<Field>("id", arrow::uint64(), false)
                                          });
  return fields;
}

auto TweetsBuilder::rt_struct() -> shared_ptr<DataType> {
  return arrow::struct_(rt_fields());
}

auto TweetsBuilder::Finish(std::shared_ptr<RecordBatch> *batch) -> Status {
  shared_ptr<Array> id;
  shared_ptr<Array> created_at;
  shared_ptr<Array> text;
  shared_ptr<Array> author_id;
  shared_ptr<Array> in_reply_to_user_id;
  shared_ptr<Array> ref_tweets_type;
  shared_ptr<Array> ref_tweets_id;
  shared_ptr<Array> ref_tweets_struct;
  shared_ptr<Array> ref_tweets;

  ARROW_RETURN_NOT_OK(id_->Finish(&id));
  ARROW_RETURN_NOT_OK(created_at_->Finish(&created_at));
  ARROW_RETURN_NOT_OK(text_->Finish(&text));
  ARROW_RETURN_NOT_OK(author_id_->Finish(&author_id));
  ARROW_RETURN_NOT_OK(in_reply_to_user_id_->Finish(&in_reply_to_user_id));
  ARROW_RETURN_NOT_OK(ref_tweets_->Finish(&ref_tweets));

  *batch = RecordBatch::Make(schema(),
                             id->length(),
                             vector({id, created_at, text, author_id, in_reply_to_user_id, ref_tweets}));

  return Status::OK();
}

auto ParseRefType(const string &s) -> uint8_t {
  if (s == "retweeted") return 0;
  if (s == "quoted") return 1;
  if (s == "replied_to") return 2;

  // TODO(johanpel): improve this:
  throw std::runtime_error("Unkown referenced_tweet type:" + s);
}

auto CreateRecordBatch(const rapidjson::Document &doc) -> shared_ptr<RecordBatch> {
  // Set up Arrow RecordBatch builder for tweets:
  TweetsBuilder t;
  // Iterate over every tweet:
  for (const auto &tweet : doc.GetArray()) {
    const auto &d = tweet["data"];

    // Parse referenced tweets:
    vector<pair<uint8_t, uint64_t>> ref_tweets;
    for (const auto &r : d["referenced_tweets"].GetArray()) {
      ref_tweets.emplace_back(ParseRefType(r["type"].GetString()),
                              strtoul(r["id"].GetString(), nullptr, 10));
    }

    t.Append(strtoul(d["id"].GetString(), nullptr, 10),
             d["created_at"].GetString(),
             d["text"].GetString(),
             strtoul(d["author_id"].GetString(), nullptr, 10),
             strtoul(d["in_reply_to_user_id"].GetString(), nullptr, 10),
             ref_tweets);
  }
  // Finalize the builder:
  shared_ptr<RecordBatch> batch;
  t.Finish(&batch);
  return batch;
}
