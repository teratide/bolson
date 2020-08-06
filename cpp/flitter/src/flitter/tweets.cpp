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

#include "./tweets.h"
#include "./timer.h"
#include "./utils.h"

using std::vector;
using std::string;
using std::string_view;
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
using arrow::Result;

/// Length of a date string in ISO 8601
#define DATE_LENGTH 24

TweetsBuilder::TweetsBuilder(int64_t tweets_reserve, int64_t text_reserve, int64_t referenced_tweets_reserve) {
  // Set up the schema first.
  // For ID's, Twitter uses string types to not get in trouble with JavaScript, but we can use
  // 64-bit unsigned ints; it should be enough to store those IDs.
  schema_ = schema();

  // Set up builders for all fields.
  id_ = make_shared<UInt64Builder>();
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

  id_->Reserve(tweets_reserve);
  created_at_->Reserve(tweets_reserve + 1); // + 1 for final offset
  created_at_->ReserveData(tweets_reserve * DATE_LENGTH);
  text_->Reserve(tweets_reserve + 1);
  text_->ReserveData(tweets_reserve * text_reserve);
  author_id_->Reserve(tweets_reserve);
  in_reply_to_user_id_->Reserve(tweets_reserve);
  ref_tweets_type_->Reserve(tweets_reserve * referenced_tweets_reserve);
  ref_tweets_id_->Reserve(tweets_reserve * referenced_tweets_reserve);
  ref_tweets_struct_->Reserve(tweets_reserve * referenced_tweets_reserve);
  ref_tweets_->Reserve(tweets_reserve + 1);
}

auto TweetsBuilder::Append(uint64_t id,
                           const string_view &created_at,
                           const string_view &text,
                           uint64_t author_id,
                           uint64_t in_reply_to_user_id,
                           const vector<ReferencedTweet> &referenced_tweets) -> Status {
  ARROW_RETURN_NOT_OK(this->id_->Append(id));
  ARROW_RETURN_NOT_OK(this->created_at_->Append(created_at.data(), created_at.size()));
  ARROW_RETURN_NOT_OK(this->text_->Append(text.data(), text.size()));
  ARROW_RETURN_NOT_OK(this->author_id_->Append(author_id));
  ARROW_RETURN_NOT_OK(this->in_reply_to_user_id_->Append(in_reply_to_user_id));

  // Reserve a new list slot.
  ARROW_RETURN_NOT_OK(ref_tweets_->Append());

  for (const auto &p : referenced_tweets) {
    ARROW_RETURN_NOT_OK(ref_tweets_type_->Append(p.type));
    ARROW_RETURN_NOT_OK(ref_tweets_id_->Append(p.id));
    ARROW_RETURN_NOT_OK(ref_tweets_struct_->Append());
  }

  num_rows_++;

  return Status::OK();
}

void TweetsBuilder::Reset() {
  id_->Reset();
  created_at_->Reset();
  text_->Reset();
  author_id_->Reset();
  in_reply_to_user_id_->Reset();
  ref_tweets_type_->Reset();
  ref_tweets_id_->Reset();
  ref_tweets_struct_->Reset();
  ref_tweets_->Reset();
  num_rows_ = 0;
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

auto TweetsBuilder::size() const -> int64_t {
  // TODO(johanpel): this most certainly requires some testing, and sizes should come from Arrow ideally.
  if (num_rows_ == 0) return 0;
  else {
    // +1's are for offsets buffers
    auto result = id_->length() * sizeof(uint64_t) +
        (1 + created_at_->length()) * sizeof(int32_t) + created_at_->value_data_length() +
        (1 + text_->length()) * sizeof(int32_t) + text_->value_data_length() +
        author_id_->length() * sizeof(uint64_t) +
        in_reply_to_user_id_->length() * sizeof(uint64_t) +
        (1 + ref_tweets_->length()) * sizeof(int32_t) +
        ref_tweets_type_->length() * sizeof(uint8_t) +
        ref_tweets_id_->length() * sizeof(uint64_t);
    return result;
  }
}

// Parse reference type by looking at the string length
static auto ParseRefType(size_t l) -> uint8_t {
  if (l == sizeof("retweeted") - 1) return 0;
  if (l == sizeof("quoted") - 1) return 1;
  if (l == sizeof("replied_to") - 1) return 2;
  // TODO(johanpel): improve this:
  throw std::runtime_error("Unknown referenced_tweet type string length.");
}

auto CreateRecordBatches(const rapidjson::Document &doc, size_t max_size) -> Result<vector<shared_ptr<RecordBatch>>> {
  // Set up Arrow RecordBatch builder for tweets:
  vector<shared_ptr<RecordBatch>> batches;
  const auto &tweets = doc["tweets"].GetArray();

  size_t num_tweets = tweets.Size();
  size_t tweet_idx = 0;

  // Pre-allocate a buffer to hold referenced tweets.
  // If it is not large enough, it will, grow through emplace_back.
  // After clearing it for a new tweet, it should retain its reserved allocation.
  vector<ReferencedTweet> rtb(ReferencedTweet::max_referenced);

  // Set up a batch builder.
  TweetsBuilder t;

  // Keep going until we have put all Tweets in batches.
  while (tweet_idx < num_tweets) {

    // Keep adding tweets to this batch, until it runs over the maximum size or there are no tweets left.
    while ((t.size() < max_size) && (tweet_idx < num_tweets)) {
      const auto &tweet = tweets[tweet_idx]["data"];  // Get a ref to the tweet.
      auto iter = tweet.MemberBegin();  // Create an iterator over all tweet data members.

      // Rather than using strings to index the tweet data DOM object fields, we use an iterator
      // to prevent the extensive use of string comparison operations in getting the field values
      // from some field index.

      uint64_t id = strtoul(iter->value.GetString(), nullptr, 10);
      iter++;
      std::string_view created_at(iter->value.GetString(), iter->value.GetStringLength());
      iter++;
      std::string_view text(iter->value.GetString(), iter->value.GetStringLength());
      iter++;
      uint64_t author_id = strtoul(iter->value.GetString(), nullptr, 10);
      iter++;
      uint64_t in_reply_to_user_id = strtoul(iter->value.GetString(), nullptr, 10);
      iter++;

      // Parse referenced tweets:
      rtb.clear();  // Clear the buffer for referenced tweets.
      for (const auto &r : iter->value.GetArray()) {
        auto rt_iter = r.MemberBegin();
        auto rt_type = ParseRefType(rt_iter->value.GetStringLength());
        rt_iter++;
        uint64_t rt_id = strtoul(rt_iter->value.GetString(), nullptr, 10);
        rtb.emplace_back(rt_type, rt_id);
      }

      // Append the tweet.
      auto status = t.Append(id, created_at, text, author_id, in_reply_to_user_id, rtb);
      if (!status.ok()) return arrow::Result<vector<shared_ptr<RecordBatch>>>(status);
      tweet_idx++;
    }
    // We have gone over the maximum size or there are no more Tweets in the document.
    // Continue to finalize the builder and add the resulting batch to the collection of batches.
    shared_ptr<RecordBatch> batch;
    auto status = t.Finish(&batch);
    batches.push_back(batch);
    if (!status.ok()) return arrow::Result<vector<shared_ptr<RecordBatch>>>(status);

    // Reset the builder, so we can reuse it.
    t.Reset();
  }

  return batches;
}

void ReportParserError(const rapidjson::Document &doc, const std::vector<char> &file_buffer) {
  auto code = doc.GetParseError();
  auto offset = doc.GetErrorOffset();
  std::cerr << "  Parser error: " << rapidjson::GetParseError_En(code) << std::endl;
  std::cerr << "  Offset: " << offset << std::endl;
  std::cerr << "  Character: " << file_buffer[offset] << " / 0x"
            << std::hex << static_cast<uint8_t>(file_buffer[offset]) << std::endl;
  std::cerr << "  Around: "
            << std::string_view(&file_buffer[offset < 40UL ? 0 : offset - 40], std::min(40UL, file_buffer.size()))
            << std::endl;
}

void TweetsBuilder::RunBenchmark(size_t num_records) {
  Timer t;

  t.start();
  TweetsBuilder b;
  t.stop();
  std::cout << std::setw(42) << "Builder construction" << " : " << t.seconds() << std::endl;

  t.start();
  for (size_t i = 0; i < num_records; i++) {
    b.Append(0, "2014-02-26T03:45:38.000Z", "anrqvytlbbtdl", 0, 0, {{0, 0}, {0, 0}});
  }
  t.stop();
  shared_ptr<RecordBatch> batch;
  auto status = b.Finish(&batch);

  auto size = GetBatchSize(batch);
  ReportGBps("TweetsBuilder throughput", size, t.seconds());
  std::cout << std::setw(42) << "TweetsBuilder RecordBatch size" << ": " << size << std::endl;
}