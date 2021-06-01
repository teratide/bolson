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

#include "bolson/parse/arrow.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/json/api.h>

#include <CLI/CLI.hpp>
#include <memory>
#include <string_view>

#include "bolson/log.h"
#include "bolson/parse/parser.h"

namespace bolson::parse {

auto ReadSchemaFromFile(const std::string& file, std::shared_ptr<arrow::Schema>* out)
    -> Status {
  if (file.empty()) {
    return Status(Error::IOError, "Arrow Schema file path empty.");
  }
  // TODO(johanpel): use filesystem lib for path
  auto result_file_open = arrow::io::ReadableFile::Open(file);

  if (!result_file_open.ok()) {
    return Status(Error::IOError, result_file_open.status().message());
  }
  auto file_input_stream = result_file_open.ValueOrDie();

  // Dictionaries are not supported yet, hence nullptr. If there are actual dictionaries,
  // the function will return an error status, which is propagated to the caller of this
  // function.
  auto read_schema_result = arrow::ipc::ReadSchema(file_input_stream.get(), nullptr);

  if (read_schema_result.ok()) {
    *out = read_schema_result.ValueOrDie();
  } else {
    return Status(Error::IOError, read_schema_result.status().message());
  }

  auto status = file_input_stream->Close();

  return Status::OK();
}

auto ArrowOptions::ReadSchema() -> Status {
  BOLSON_ROE(ReadSchemaFromFile(schema_path, &schema));
  return Status::OK();
}

auto ArrowParserContext::Make(const ArrowOptions& opts, size_t num_parsers,
                              std::shared_ptr<ParserContext>* out) -> Status {
  auto result = std::make_shared<ArrowParserContext>();

  // Use default allocator.
  result->allocator_ = std::make_shared<buffer::Allocator>();

  // Determine Arrow JSON parser options.
  arrow::json::ParseOptions parse_opts;
  if (opts.schema == nullptr) {
    BOLSON_ROE(ReadSchemaFromFile(opts.schema_path, &result->input_schema_));
  } else {
    result->input_schema_ = opts.schema;
  }

  // Add the sequence number field to the output schema if specified.
  if (opts.seq_column) {
    BOLSON_ROE(WithSeqField(*result->input_schema_, &result->output_schema_));
  } else {
    result->output_schema_ = result->input_schema_;
  }

  parse_opts.explicit_schema = result->input_schema_;
  parse_opts.unexpected_field_behavior = arrow::json::UnexpectedFieldBehavior::Error;

  // Determine Arrow JSON table reader options.
  arrow::json::ReadOptions read_opts;
  read_opts.use_threads = false;  // threading is handled by Bolson
  read_opts.block_size = 2 * read_opts.block_size;

  // Initialize all parsers.
  result->parsers_ = std::vector<std::shared_ptr<ArrowParser>>(
      num_parsers, std::make_shared<ArrowParser>(parse_opts, read_opts, opts.seq_column));
  *out = std::static_pointer_cast<ParserContext>(result);

  // Allocate buffers. Use number of parsers if number of buffers is 0 in options.
  auto num_buffers = opts.num_buffers == 0 ? num_parsers : opts.num_buffers;
  BOLSON_ROE(result->AllocateBuffers(num_buffers, opts.buf_capacity));

  return Status::OK();
}

auto ArrowParser::Parse(const std::vector<illex::JSONBuffer*>& buffers_in,
                        std::vector<ParsedBatch>* batches_out) -> Status {
  assert(batches_out != nullptr);

  for (auto* in : buffers_in) {
    assert(in != nullptr);
    auto buffer = arrow::Buffer::Wrap(in->data(), in->size());
    auto br = std::make_shared<arrow::io::BufferReader>(buffer);
    auto tr_make_result = arrow::json::TableReader::Make(arrow::default_memory_pool(), br,
                                                         read_opts, parse_opts);
    if (!tr_make_result.ok()) {
      return Status(Error::ArrowError, "Unable to make JSON Table Reader: " +
                                           tr_make_result.status().message());
    }
    auto t_reader = tr_make_result.ValueOrDie();

    auto tr_read_result = t_reader->Read();
    if (!tr_read_result.ok()) {
      SPDLOG_DEBUG("Encountered error while parsing: {}",
                   std::string(reinterpret_cast<const char*>(in->data()), in->size()));
      return Status(Error::ArrowError, "Unable to read JSONs to RecordBatch(es): " +
                                           tr_read_result.status().message());
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

    auto combined_batch = table_reader_next_result.ValueOrDie();

    std::shared_ptr<arrow::RecordBatch> final_batch;

    if (seq_column) {
      std::shared_ptr<arrow::UInt64Array> seq;
      arrow::UInt64Builder builder;
      ARROW_ROE(builder.Reserve(in->range().last - in->range().first + 1));
      for (uint64_t s = in->range().first; s <= in->range().last; s++) {
        builder.UnsafeAppend(s);
      }
      ARROW_ROE(builder.Finish(&seq));
      auto final_batch_result = combined_batch->AddColumn(0, "bolson_seq", seq);
      if (!final_batch_result.ok()) {
        return Status(Error::ArrowError, final_batch_result.status().message());
      }
      final_batch = final_batch_result.ValueOrDie();
    } else {
      final_batch = AddSeqAsSchemaMeta(combined_batch, in->range());
    }

    batches_out->emplace_back(final_batch, in->range());
  }

  return Status::OK();
}

auto ArrowParserContext::parsers() -> std::vector<std::shared_ptr<Parser>> {
  return CastPtrs<Parser>(parsers_);
}

auto ArrowParserContext::output_schema() const -> std::shared_ptr<arrow::Schema> {
  return output_schema_;
}

auto ArrowParserContext::input_schema() const -> std::shared_ptr<arrow::Schema> {
  return std::shared_ptr<arrow::Schema>();
}

void AddArrowOptionsToCLI(CLI::App* sub, ArrowOptions* out) {
  sub->add_option("input,-i,--input", out->schema_path,
                  "Serialized Arrow schema file for records to convert to.")
      ->check(CLI::ExistingFile);
  sub->add_option("--arrow-buf-cap", out->buf_capacity, "Arrow input buffer capacity.")
      ->default_val(BOLSON_ARROW_DEFAULT_BUFFER_CAP);
  sub->add_flag(
         "--arrow-seq-col", out->seq_column,
         "Arrow parser, retain ordering information by adding a sequence number column.")
      ->default_val(false);
}

}  // namespace bolson::parse
