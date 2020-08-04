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
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

/// @brief Report errors when parsing a JSON document from a file buffer to stderr.
void ReportParserError(const rapidjson::Document &doc, const std::vector<char> &file_buffer);

/// @brief Parse a referenced tweet type.
auto ParseRefType(const std::string &s) -> uint8_t;

/**
 * @brief Convert a parsed JSON document with tweets into an Arrow RecordBatch
 * @param doc       The rapidjson document.
 * @param max_size  The maximum size of the resulting RecordBatch
 * @return The Arrow RecordBatch.
 */
auto CreateRecordBatches(const rapidjson::Document &doc,
                         size_t max_size) -> arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>;

/**
 * @brief A RecordBatchBuilder for Tweets
 */
class TweetsBuilder {
 public:
  explicit TweetsBuilder(int64_t size_limit = -1);

  static auto schema() -> std::shared_ptr<arrow::Schema>;

  auto Append(uint64_t id,
              const std::string &created_at,
              const std::string &text,
              uint64_t author_id,
              uint64_t in_reply_to_user_id,
              const std::vector<std::pair<uint8_t, uint64_t>> &referenced_tweets) -> arrow::Status;

  auto Finish(std::shared_ptr<arrow::RecordBatch> *batch) -> arrow::Status;

  auto size() -> int64_t;

  [[nodiscard]] auto rows() const -> int64_t { return num_rows_; }

 private:
  std::shared_ptr<arrow::UInt64Builder> id_;
  std::shared_ptr<arrow::StringBuilder> created_at_;
  std::shared_ptr<arrow::StringBuilder> text_;
  std::shared_ptr<arrow::UInt64Builder> author_id_;
  std::shared_ptr<arrow::UInt64Builder> in_reply_to_user_id_;
  std::shared_ptr<arrow::UInt8Builder> ref_tweets_type_;
  std::shared_ptr<arrow::UInt64Builder> ref_tweets_id_;
  std::shared_ptr<arrow::StructBuilder> ref_tweets_struct_;
  std::shared_ptr<arrow::ListBuilder> ref_tweets_;
  std::shared_ptr<arrow::Schema> schema_;

  int64_t num_rows_ = 0;
  int64_t size_limit_ = -1;

  static auto rt_fields() -> std::vector<std::shared_ptr<arrow::Field>>;
  static auto rt_struct() -> std::shared_ptr<arrow::DataType>;
};
