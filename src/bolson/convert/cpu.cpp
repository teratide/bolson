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

#include <thread>
#include <memory>
#include <arrow/json/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <illex/queue.h>
#include <putong/putong.h>

#include "bolson/convert/cpu.h"
#include "bolson/log.h"
#include "bolson/utils.h"

namespace bolson::convert {

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

auto ArrowIPCBuilder::AppendAsBatch(const illex::JSONQueueItem &item) -> Status {
  // Wrap Arrow buffer around the JSON string so we can parse it using Arrow.
  auto buffer_wrapper =
      std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(item.string.data()),
                                      item.string.length());
  // The following code could be better because it seems like:
  // - it always delivers a chunked table
  // - we cannot reuse the internal builders for new JSONs that are being dequeued, we
  //   can only read once.
  // - we could try to work-around previous by buffering JSONs, but that would increase
  //   latency.
  // - we wouldn't know how long to buffer because the i/o size ratio is not known,
  //   - preferably we would append jsons until some threshold is reached, then construct
  //     the ipc message.

  /*
  // Wrap a random access file abstraction around the buffer.
  auto ra_buffer = arrow::Buffer::GetReader(buffer_wrapper).ValueOrDie();
  // Construct an arrow json TableReader.
  auto reader = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                               ra_buffer,
                                               arrow::json::ReadOptions::Defaults(),
                                               arrow::json::ParseOptions::Defaults()).ValueOrDie();
  auto table = reader->Read().ValueOrDie();
  auto batch = table->CombineChunks().ValueOrDie();
  */

  // The following code could be better because it seems like:
  // - we cannot reuse the internal builders for new JSONs that are being dequeued, we
  //   can only read once.
  // - we could try to work-around previous by buffering JSONs, but that would increase
  //   latency.
  // - we wouldn't know how long to buffer because the i/o size ratio is not known,
  //   - preferably we would append jsons until some threshold is reached, then construct
  //     the ipc message.

  // TODO(johanpel): come up with a solution for all of this. Also don't use ValueOrDie();
  // Construct a RecordBatch from the JSON string.
  auto batch_result = arrow::json::ParseOne(parse_options, buffer_wrapper);
  ARROW_ROE(batch_result.status());

  // Construct the column for the sequence number.
  std::shared_ptr<arrow::Array> seq;
  arrow::UInt64Builder seq_builder;
  ARROW_ROE(seq_builder.Append(item.seq));
  ARROW_ROE(seq_builder.Finish(&seq));

  // Add the column to the batch.
  auto batch_with_seq_result = batch_result.ValueOrDie()->AddColumn(0, SeqField(), seq);
  ARROW_ROE(batch_with_seq_result.status());
  auto batch_with_seq = batch_with_seq_result.ValueOrDie();

  this->size_ += GetBatchSize(batch_with_seq);
  this->batches.push_back(std::move(batch_with_seq));

  return Status::OK();
}

auto ArrowIPCBuilder::FlushBuffered(putong::Timer<> *t) -> Status {
  // Check if there is anything to flush.
  if (str_buffer->size() > 0) {
    SPDLOG_DEBUG("Flushing: {}",
                 std::string(reinterpret_cast<const char *>(str_buffer->data()),
                             str_buffer->size()));
    auto br = std::make_shared<arrow::io::BufferReader>(str_buffer);
    auto tr = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                             br,
                                             read_options,
                                             parse_options).ValueOrDie();
    t->Start();
    auto table = tr->Read().ValueOrDie();
    t->Stop();

    // Construct the column for the sequence number.
    std::shared_ptr<arrow::Array> seq;
    ARROW_ROE(seq_builder->Finish(&seq));
    auto chunked_seq = std::make_shared<arrow::ChunkedArray>(seq);

    // Add the column to the batch.
    auto tab_with_seq_result = table->AddColumn(0, SeqField(), chunked_seq);
    ARROW_ROE(tab_with_seq_result.status());
    auto table_with_seq = tab_with_seq_result.ValueOrDie();

    auto combined_table = table_with_seq->CombineChunks();
    auto table_reader = arrow::TableBatchReader(*combined_table.ValueOrDie());
    auto combined_batch = table_reader.Next().ValueOrDie();

    this->size_ += GetBatchSize(combined_batch);
    this->batches.push_back(std::move(combined_batch));

    ARROW_ROE(this->str_buffer->Resize(0));
  }
  return Status::OK();
}

void ConvertWithCPU(illex::JSONQueue *in,
                    IpcQueue *out,
                    std::atomic<bool> *shutdown,
                    size_t num_drones,
                    const arrow::json::ParseOptions &parse_options,
                    const arrow::json::ReadOptions &read_options,
                    size_t json_buffer_threshold,
                    size_t batch_size_threshold,
                    std::promise<std::vector<Stats>> &&stats) {
  // Reserve some space for the thread handles and futures.
  std::vector<std::thread> threads;
  std::vector<std::future<Stats>> thread_stats_futures;
  std::vector<Stats> threads_stats(num_drones);
  threads.reserve(num_drones);
  thread_stats_futures.reserve(num_drones);

  SPDLOG_DEBUG("Starting {} JSON conversion drones.", num_drones);
  SPDLOG_DEBUG("  with JSON threshold: {}", json_buffer_threshold);

  // Start the conversion threads.
  for (int thread_id = 0; thread_id < num_drones; thread_id++) {
    // Prepare the future/promise.
    std::promise<Stats> thread_stats_promise;
    thread_stats_futures.push_back(thread_stats_promise.get_future());
    // Create builders based on Arrow's CPU implementation.
    auto builder = std::make_unique<ArrowIPCBuilder>(parse_options,
                                                     read_options,
                                                     json_buffer_threshold,
                                                     batch_size_threshold);
    threads.emplace_back(Convert,
                         thread_id,
                         std::move(builder),
                         in,
                         out,
                         shutdown,
                         std::move(thread_stats_promise));
  }

  // Wait for the drones to get shut down by the caller of this function.
  // Also gather the statistics and see if there was any error.
  for (size_t t = 0; t < num_drones; t++) {
    if (threads[t].joinable()) {
      threads[t].join();
      threads_stats[t] = thread_stats_futures[t].get();
      if (!threads_stats[t].status.ok()) {
        // If a thread returned some error status, shut everything down without waiting
        // for the caller to do so.
        shutdown->store(true);
      }
    }
  }

  stats.set_value(threads_stats);
}

}
