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
#include <arrow/api.h>

#include "illex/client_buffered.h"

#include "bolson/parse/parser.h"
#include "bolson/convert/resizer.h"
#include "bolson/convert/serializer.h"
#include "bolson/convert/converter.h"
#include "bolson/utils.h"
#include "bolson/latency.h"

namespace bolson::convert {

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

auto TryGetFilledBuffer(const std::vector<illex::RawJSONBuffer *> &buffers,
                        const std::vector<std::mutex *> &mutexes,
                        illex::RawJSONBuffer **out,
                        size_t *lock_idx) -> bool {
  const size_t num_buffers = buffers.size();
  for (size_t i = 0; i < num_buffers; i++) {
    if (!buffers[i]->empty()) {
      if (mutexes[i]->try_lock()) {
        *lock_idx = i;
        *out = buffers[i];
        return true;
      }
    }
  }
  *out = nullptr;
  return false;
}

#define SHUTDOWN_ON_FAILURE() \
if (!stats->status.ok()) { \
  t.thread.Stop(); \
  stats->t.thread = t.thread.seconds(); \
  SPDLOG_DEBUG("Drone {} Terminating with error: {}", id, stats->status.msg()); \
  shutdown->store(true); \
  return; \
} void()

void ConvertThread(size_t id,
                   parse::Parser *parser,
                   Resizer *resizer,
                   Serializer *serializer,
                   const std::vector<illex::RawJSONBuffer *> &buffers,
                   const std::vector<std::mutex *> &mutexes,
                   IpcQueue *out,
                   std::atomic<bool> *shutdown,
                   Stats *stats) {
  ConversionTimers t;
  SPDLOG_DEBUG("Convert thread {} spawned.", id);
  bool try_buffers = true;

  while (!shutdown->load()) {
    if (try_buffers) {
      illex::RawJSONBuffer *buf = nullptr;
      size_t lock_idx = 0;
      if (TryGetFilledBuffer(buffers, mutexes, &buf, &lock_idx)) {
        // Prepare intermediate wrappers.
        parse::ParsedBuffer parsed;
        ResizedBatches resized;
        SerializedBatches serialized;

        SPDLOG_DEBUG("Thread {} converting {}",
                     id,
                     std::string_view((char *) buf->data(), buf->size()));

        // Convert the buffer.
        t.parse.Start();
        stats->status = parser->Parse(buf, &parsed);
        SHUTDOWN_ON_FAILURE();
        t.parse.Stop();

        SPDLOG_DEBUG("Thread {} parsed {} bytes from buffer resulting in {} rows.",
                     id,
                     parsed.parsed_bytes,
                     parsed.batch->num_rows());

        // Reset and unlock the buffer.
        // Add sizes stats before buffer is converted and reset.
        stats->num_jsons += parsed.batch->num_rows();
        stats->num_json_bytes += parsed.parsed_bytes;
        buf->Reset();
        mutexes[lock_idx]->unlock();

        // Resize the batch.
        t.resize.Start();
        stats->status = resizer->Resize(parsed, &resized);
        SHUTDOWN_ON_FAILURE();
        t.resize.Stop();

        SPDLOG_DEBUG("Thread {} resized: {} RecordBatches.", id, resized.batches.size());

        // Serialize the batch.
        t.serialize.Start();
        stats->status = serializer->Serialize(resized, &serialized);
        SHUTDOWN_ON_FAILURE();
        t.serialize.Stop();

        // Enqueue IPC items
        for (size_t i = 0; i < serialized.messages.size(); i++) {
          IpcQueueItem ipc{static_cast<size_t>(resized.batches[i]->num_rows()),
                           serialized.messages[i], {}};
          SPDLOG_DEBUG("Thread {} enqueueing message with {} rows.",
                       id,
                       resized.batches[i]->num_rows());
          out->enqueue(ipc);
        }

        // Add parse time to stats.
        stats->t.parse += t.parse.seconds();
        stats->t.resize += t.resize.seconds();
        stats->t.serialize += t.serialize.seconds();
        SHUTDOWN_ON_FAILURE();
      } else {
        SPDLOG_DEBUG("Thread {} unable to get lock / or buffers not filled.", id);
        try_buffers = false;
      }
    } else {
      SPDLOG_DEBUG("Thread {} sleeping...", id);
      std::this_thread::sleep_for(std::chrono::microseconds(BOLSON_QUEUE_WAIT_US));
      try_buffers = true;
    }
  }

  t.thread.Stop();
  stats->t.thread = t.thread.seconds();
  SPDLOG_DEBUG("Drone {} Terminating.", id);
}
#undef SHUTDOWN_ON_FAILURE

void Converter::Start(std::atomic<bool> *stop_signal) {
  shutdown = stop_signal;
  for (int t = 0; t < num_threads_; t++) {
    threads.emplace_back(ConvertThread,
                         t,
                         parsers[t].get(),
                         &resizers[t],
                         &serializers[t],
                         ToPointers(buffers),
                         ToPointers(mutexes),
                         output_queue_,
                         shutdown,
                         &stats[t]);
  }
}

auto Converter::Stop() -> Status {
  // Wait for the drones to get shut down by the caller of this function.
  // Also gather the statistics and see if there was any error.
  for (size_t t = 0; t < num_threads_; t++) {
    if (threads[t].joinable()) {
      threads[t].join();
      if (!stats[t].status.ok()) {
        // If a thread returned some error status, shut everything down without waiting
        // for the caller to do so.
        shutdown->store(true);
      }
    }
  }

  return Status::OK();
}

auto Converter::AllocateBuffers(size_t capacity) -> Status {
  // Allocate buffers.
  for (size_t b = 0; b < num_buffers_; b++) {
    std::byte *raw = nullptr;
    BOLSON_ROE(allocator_->Allocate(capacity, &raw));
    illex::RawJSONBuffer buf;
    illex::RawJSONBuffer::Create(raw, capacity, &buf);
    buffers.push_back(buf);
  }

  return Status::OK();
}

auto Converter::FreeBuffers() -> Status {
  for (size_t b = 0; b < num_buffers_; b++) {
    BOLSON_ROE(allocator_->Free(buffers[b].mutable_data()));
  }
  return Status::OK();
}

}