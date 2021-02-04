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

#include "bolson/convert/converter.h"

#include <arrow/api.h>
#include <illex/client_buffering.h>

#include <memory>
#include <thread>

#include "bolson/convert/resizer.h"
#include "bolson/convert/serializer.h"
#include "bolson/latency.h"
#include "bolson/parse/parser.h"
#include "bolson/utils.h"

namespace bolson::convert {

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

/**
 * \brief Attempt to get a lock on a buffer.
 * \param buffers   The buffers.
 * \param mutexes   The mutexes.
 * \param out       A pointer to the locked buffer.
 * \param lock_idx  The lock index. Is updated in case of lock. Lock attempts start here.
 * \return True if a lock was obtained, false otherwise.
 */
static auto TryGetFilledBuffer(const std::vector<illex::JSONBuffer*>& buffers,
                               const std::vector<std::mutex*>& mutexes,
                               illex::JSONBuffer** out, size_t* lock_idx) -> bool {
  auto b = *lock_idx;
  const size_t num_buffers = buffers.size();
  for (size_t i = 0; i <= num_buffers; i++) {
    b = (b + i) % num_buffers;  // increase buffer to try by i, but wrap around
    if (mutexes[b]->try_lock()) {
      if (!buffers[b]->empty()) {
        *lock_idx = b;
        *out = buffers[b];
        return true;
      } else {
        mutexes[b]->unlock();
      }
    }
  }
  *out = nullptr;
  return false;
}

void ConvertThread(size_t id, parse::Parser* parser, Resizer* resizer,
                   Serializer* serializer, const std::vector<illex::JSONBuffer*>& buffers,
                   const std::vector<std::mutex*>& mutexes, publish::IpcQueue* out,
                   std::atomic<bool>* shutdown, Metrics* stats) {
  /// Macro to shut this thread and others down when something failed.
#define SHUTDOWN_ON_FAILURE()                                                          \
  if (!stats->status.ok()) {                                                           \
    t_thread.Stop();                                                                   \
    stats->t.thread = t_thread.seconds();                                              \
    SPDLOG_DEBUG("Thread {:2} | terminating with error: {}", id, stats->status.msg()); \
    shutdown->store(true);                                                             \
    return;                                                                            \
  }                                                                                    \
  void()

  // Thread timer.
  putong::Timer<> t_thread(true);
  // Workload stage timer.
  putong::SplitTimer<4> t_stages;
  // Latency time points.
  TimePoints lat;
  // Whether to try and unlock a buffer or to wait a bit.
  bool try_buffers = true;
  // Buffer to unlock.
  size_t lock_idx = 0;

  SPDLOG_DEBUG("Thread {:2} | Spawned.", id);

  while (!shutdown->load()) {
    if (try_buffers) {
      illex::JSONBuffer* buf = nullptr;
      if (TryGetFilledBuffer(buffers, mutexes, &buf, &lock_idx)) {
        t_stages.Start();
        lat[TimePoints::received] = buf->recv_time();

        // Prepare intermediate wrappers.
        parse::ParsedBatch parsed;
        ResizedBatches resized;
        SerializedBatches serialized;

        // Parse the buffer.
        {
          stats->status = parser->Parse(buf, &parsed);
          SHUTDOWN_ON_FAILURE();

          // Reset and unlock the buffer.
          // Add sizes stats before buffer is converted and reset.
          stats->num_jsons += parsed.batch->num_rows();
          stats->json_bytes += buf->size();
          stats->num_parsed++;
          buf->Reset();
          mutexes[lock_idx]->unlock();
          lock_idx++;  // start at next buffer next time we try to unlock.
          lat[TimePoints::parsed] = illex::Timer::now();
          t_stages.Split();
        }

        // Resize the batch.
        {
          stats->status = resizer->Resize(parsed, &resized);
          SHUTDOWN_ON_FAILURE();
          // Mark time points resized for all batches.
          lat[TimePoints::resized] = illex::Timer::now();
          t_stages.Split();
        }

        // Serialize the batch.
        {
          stats->status = serializer->Serialize(resized, &serialized);
          SHUTDOWN_ON_FAILURE();
          stats->num_ipc += serialized.size();
          stats->ipc_bytes += ByteSizeOf(serialized);
          // Mark time points serialized for all batches.
          lat[TimePoints::serialized] = illex::Timer::now();
          // Copy the latency statistics to all serialized batches.
          for (auto& s : serialized) {
            s.time_points = lat;
          }
          t_stages.Split();
        }

        // Enqueue IPC items
        for (const auto sb : serialized) {
          out->enqueue(sb);
        }
        t_stages.Split();

        // Add parse time to stats.
        stats->t.parse += t_stages.seconds()[0];
        stats->t.resize += t_stages.seconds()[1];
        stats->t.serialize += t_stages.seconds()[2];
        stats->t.enqueue += t_stages.seconds()[3];
      } else {
        try_buffers = false;
      }
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(BOLSON_QUEUE_WAIT_US));
      try_buffers = true;
    }
  }

  t_thread.Stop();
  stats->t.thread = t_thread.seconds();
  SPDLOG_DEBUG("Thread {:2} | Terminating.", id);
}
#undef SHUTDOWN_ON_FAILURE

void ConcurrentConverter::Start(std::atomic<bool>* shutdown) {
  shutdown_ = shutdown;
  for (int t = 0; t < num_threads_; t++) {
    threads.emplace_back(ConvertThread, t, parsers[t].get(), &resizers[t],
                         &serializers[t], mutable_buffers(), mutexes(), output_queue_,
                         shutdown_, &statistics_[t]);
  }
}

auto ConcurrentConverter::Finish() -> Status {
  // Wait for the drones to get shut down by the caller of this function.
  // Also gather the statistics and see if there was any error.
  for (size_t t = 0; t < num_threads_; t++) {
    if (threads[t].joinable()) {
      threads[t].join();
      if (!statistics_[t].status.ok()) {
        // If a thread returned some error status, shut everything down without waiting
        // for the caller to do so.
        shutdown_->store(true);
      }
    }
  }

  BOLSON_ROE(FreeBuffers());

  // TODO: return MultiStatus like pulsar.h
  return Status::OK();
}

auto ConcurrentConverter::AllocateBuffers(size_t capacity) -> Status {
  // Allocate buffers.
  for (size_t b = 0; b < num_buffers_; b++) {
    std::byte* raw = nullptr;
    BOLSON_ROE(allocator_->Allocate(capacity, &raw));
    illex::JSONBuffer buf;
    BILLEX_ROE(illex::JSONBuffer::Create(raw, capacity, &buf));
    buffers.push_back(buf);
  }

  return Status::OK();
}

auto ConcurrentConverter::FreeBuffers() -> Status {
  for (size_t b = 0; b < num_buffers_; b++) {
    BOLSON_ROE(allocator_->Free(buffers[b].mutable_data()));
  }
  return Status::OK();
}

auto ConcurrentConverter::Make(const ConverterOptions& opts, publish::IpcQueue* ipc_queue,
                               std::shared_ptr<ConcurrentConverter>* out) -> Status {
  // Select allocator.
  std::shared_ptr<buffer::Allocator> allocator;
  switch (opts.implementation) {
    case parse::Impl::ARROW:
      allocator = std::make_shared<buffer::Allocator>();
      break;
    case parse::Impl::OPAE_BATTERY:
      allocator = std::make_shared<buffer::OpaeAllocator>();
      break;
  }

  // Set up the Converter.
  size_t num_buffers = opts.num_buffers.value_or(opts.num_threads);

  auto converter =
      std::shared_ptr<convert::ConcurrentConverter>(new convert::ConcurrentConverter(
          ipc_queue, allocator, num_buffers, opts.num_threads));

  // Allocate buffers.
  switch (opts.implementation) {
    case parse::Impl::ARROW: {
      BOLSON_ROE(converter->AllocateBuffers(opts.buf_capacity));
      break;
    }
    case parse::Impl::OPAE_BATTERY: {
      if (opts.buf_capacity > buffer::OpaeAllocator::opae_fixed_capacity) {
        return Status(Error::OpaeError,
                      "Cannot allocate buffers larger than " +
                          std::to_string(buffer::OpaeAllocator::opae_fixed_capacity) +
                          " bytes.");
      }
      BOLSON_ROE(converter->AllocateBuffers(buffer::OpaeAllocator::opae_fixed_capacity));
      break;
    }
  }

  // Set up the parsers.
  switch (opts.implementation) {
    case parse::Impl::ARROW: {
      for (size_t t = 0; t < opts.num_threads; t++) {
        auto parser = std::make_shared<parse::ArrowParser>(opts.arrow);
        converter->parsers.push_back(parser);
      }
      break;
    }
    case parse::Impl::OPAE_BATTERY: {
      BOLSON_ROE(parse::OpaeBatteryParserManager::Make(
          opts.opae_battery, ToPointers(converter->buffers), opts.num_threads,
          &converter->opae_battery_manager));
      converter->parsers =
          CastPtrs<parse::Parser>(converter->opae_battery_manager->parsers());
    }
  }

  // Set up Resizers and Serializers.
  for (size_t t = 0; t < opts.num_threads; t++) {
    converter->resizers.emplace_back(opts.max_batch_rows);
    converter->serializers.emplace_back(opts.max_ipc_size);
  }

  *out = std::move(converter);

  return Status::OK();
}

auto ConcurrentConverter::mutable_buffers() -> std::vector<illex::JSONBuffer*> {
  return ToPointers(buffers);
}

auto ConcurrentConverter::mutexes() -> std::vector<std::mutex*> {
  return ToPointers(mutexes_);
}

void ConcurrentConverter::LockBuffers() {
  for (auto& mutex : mutexes_) {
    mutex.lock();
  }
}

void ConcurrentConverter::UnlockBuffers() {
  for (auto& mutex : mutexes_) {
    mutex.unlock();
  }
}

auto ConcurrentConverter::metrics() const -> std::vector<Metrics> { return statistics_; }

}  // namespace bolson::convert
