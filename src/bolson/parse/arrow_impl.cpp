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

#include "bolson/parse/arrow_impl.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/json/api.h>

#include <memory>

#include "bolson/parse/parser.h"

namespace bolson::parse {

auto ArrowParser::Parse(illex::RawJSONBuffer* in, ParsedBatch* out) -> Status {
  auto buffer = arrow::Buffer::Wrap(in->data(), in->size());
  auto br = std::make_shared<arrow::io::BufferReader>(buffer);
  auto tr_make_result = arrow::json::TableReader::Make(arrow::default_memory_pool(), br,
                                                       opts.read, opts.parse);
  if (!tr_make_result.ok()) {
    return Status(Error::ArrowError, "Unable to make JSON Table Reader: " +
                                         tr_make_result.status().message());
  }
  auto t_reader = tr_make_result.ValueOrDie();

  auto tr_read_result = t_reader->Read();
  if (!tr_read_result.ok()) {
    return Status(Error::ArrowError,
                  "Unable to read JSON as table: " + tr_read_result.status().message());
  }
  auto table = tr_read_result.ValueOrDie();

  // Combine potential chunks in this table and read the first batch.
  auto table_combine_result = table->CombineChunks();
  if (!table_combine_result.ok()) {
    return Status(Error::ArrowError, table_combine_result.status().message());
  }
  auto tb_reader = arrow::TableBatchReader(*table_combine_result.ValueOrDie());
  auto table_reader_next_result = tb_reader.Next();
  if (!table_reader_next_result.ok()) {
    return Status(Error::ArrowError, table_reader_next_result.status().message());
  }

  auto final_batch = table_reader_next_result.ValueOrDie();

  out->batch = final_batch;
  out->seq_range = in->range();

  return Status::OK();
}

}  // namespace bolson::parse
