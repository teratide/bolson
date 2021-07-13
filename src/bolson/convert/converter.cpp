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

#include <cassert>
#include <future>
#include <memory>
#include <thread>

#include "bolson/convert/resizer.h"
#include "bolson/convert/serializer.h"
#include "bolson/latency.h"
#include "bolson/parse/parser.h"

namespace bolson::convert {

auto Converter::metrics() const -> std::vector<Metrics> { return metrics_; }

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
  // Try next buffer next time.
  *lock_idx = (*lock_idx + 1) % num_buffers;
  return false;
}

static void OneToOneConvertThread(size_t id, parse::Parser* parser,
                                  const std::shared_ptr<Resizer>& resizer,
                                  const std::shared_ptr<Serializer>& serializer,
                                  const std::vector<illex::JSONBuffer*>& buffers,
                                  const std::vector<std::mutex*>& mutexes,
                                  publish::IpcQueue* out, std::atomic<bool>* shutdown,
                                  std::promise<Metrics>&& metrics_promise) {
  assert(mutexes.size() == buffers.size());
  /// Macro to shut this thread and others down when something failed.
#define SHUTDOWN_ON_FAILURE()                                                           \
  if (!metrics.status.ok()) {                                                           \
    t_thread.Stop();                                                                    \
    metrics.t.thread = t_thread.seconds();                                              \
    SPDLOG_DEBUG("Thread {:2} | terminating with error: {}", id, metrics.status.msg()); \
    metrics_promise.set_value(metrics);                                                 \
    shutdown->store(true);                                                              \
    return;                                                                             \
  }
  Metrics metrics;
  metrics.num_threads = 1;

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

        // Parse the buffer.
        std::vector<parse::ParsedBatch> parsed_batches;
        {
          metrics.status = parser->Parse({buf}, &parsed_batches);
          SHUTDOWN_ON_FAILURE();

          // Add metrics before buffer is converted and reset.
          metrics.num_jsons_converted += parsed_batches[0].batch->num_rows();
          metrics.num_json_bytes_converted += buf->size();
          metrics.num_buffers_converted++;
          // Reset and unlock the buffer.
          buf->Reset();
          mutexes[lock_idx]->unlock();
          lock_idx++;  // start at next buffer next time we try to unlock.
          lat[TimePoints::parsed] = illex::Timer::now();
        }

        t_stages.Split();

        // Resize the batch.
        ResizedBatches resized;
        {
          metrics.status = resizer->Resize(parsed_batches[0], &resized);
          SHUTDOWN_ON_FAILURE();
          // Mark time points resized for all batches.
          lat[TimePoints::resized] = illex::Timer::now();
        }

        t_stages.Split();

        // Serialize the batch.
        SerializedBatches serialized;
        {
          metrics.status = serializer->Serialize(resized, &serialized);
          SHUTDOWN_ON_FAILURE();
          metrics.num_ipc += serialized.size();
          metrics.ipc_bytes += ByteSizeOf(serialized);
          // Mark time points serialized for all batches.
          lat[TimePoints::serialized] = illex::Timer::now();
          // Copy the latency statistics to all serialized batches.
          for (auto& s : serialized) {
            s.time_points = lat;
          }
        }

        t_stages.Split();

        // Enqueue IPC items
        {
          for (const auto& sb : serialized) {
            out->enqueue(sb);
          }
        }

        t_stages.Split();

        // Add parse time to stats.
        metrics.t.parse += t_stages.seconds()[0];
        metrics.t.resize += t_stages.seconds()[1];
        metrics.t.serialize += t_stages.seconds()[2];
        metrics.t.enqueue += t_stages.seconds()[3];
      } else {
        try_buffers = false;
      }
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(BOLSON_QUEUE_WAIT_US));
      try_buffers = true;
    }
  }

  t_thread.Stop();
  metrics.t.thread = t_thread.seconds();
  metrics_promise.set_value(metrics);
  SPDLOG_DEBUG("Thread {:2} | Terminating.", id);
#undef SHUTDOWN_ON_FAILURE
}

static void AllToOneConverterThread(size_t id, parse::Parser* parser,
                                    const std::shared_ptr<Resizer>& resizer,
                                    const std::shared_ptr<Serializer>& serializer,
                                    const std::vector<illex::JSONBuffer*>& buffers,
                                    const std::vector<std::mutex*>& mutexes,
                                    publish::IpcQueue* out, std::atomic<bool>* shutdown,
                                    std::promise<Metrics>&& metrics_promise) {
  assert(mutexes.size() == buffers.size());
  /// Macro to shut this thread and others down when something failed.
#define SHUTDOWN_ON_FAILURE()                                                           \
  if (!metrics.status.ok()) {                                                           \
    t_thread.Stop();                                                                    \
    metrics.t.thread = t_thread.seconds();                                              \
    SPDLOG_DEBUG("Thread {:2} | terminating with error: {}", id, metrics.status.msg()); \
    metrics_promise.set_value(metrics);                                                 \
    shutdown->store(true);                                                              \
    return;                                                                             \
  }                                                                                     \
  void()

  Metrics metrics;
  metrics.num_threads = 1;

  // Thread timer.
  putong::Timer<> t_thread(true);
  // Workload stage timer.
  putong::SplitTimer<4> t_stages;
  // Latency time points.
  TimePoints lat;

  SPDLOG_DEBUG("Thread {:2} | Spawned.", id);

  while (!shutdown->load()) {
    // Obtain a lock on all buffers.
    for (auto* m : mutexes) {
      m->lock();
    }

    // Check if there is anything to do.
    bool skip = true;
    for (const auto& buf : buffers) {
      skip = skip && buf->empty();
    }

    if (skip) {
      for (auto* m : mutexes) {
        m->unlock();
      }
    } else {
      t_stages.Start();

      // Prepare intermediate wrappers.
      std::vector<parse::ParsedBatch> parsed_batches;

      // Parse the buffers
      {
        metrics.status = parser->Parse(buffers, &parsed_batches);
        SHUTDOWN_ON_FAILURE();

        // Update metrics
        metrics.num_jsons_converted += parsed_batches[0].batch->num_rows();
        metrics.num_buffers_converted += buffers.size();

        lat[TimePoints::received] = buffers[0]->recv_time();  // init with first buf time
        for (int i = 0; i < buffers.size(); i++) {
          metrics.num_json_bytes_converted += buffers[i]->size();
          // Mark worst-case latency time point for the output batch.
          if (buffers[i]->recv_time() < lat[TimePoints::received]) {
            lat[TimePoints::received] = buffers[i]->recv_time();
          }
          // Reset and unlock the buffer.
          buffers[i]->Reset();
          mutexes[i]->unlock();
        }

        lat[TimePoints::parsed] = illex::Timer::now();
        t_stages.Split();
      }

      // Resize the batch.
      ResizedBatches resized;
      {
        for (auto pb : parsed_batches) {
          ResizedBatches rb;
          metrics.status = resizer->Resize(parsed_batches[0], &rb);
          resized.insert(resized.end(), rb.begin(), rb.end());
        }
        SHUTDOWN_ON_FAILURE();
        // Mark time points resized for all batches.
        lat[TimePoints::resized] = illex::Timer::now();
        t_stages.Split();
      }

      // Serialize the batch.
      SerializedBatches serialized;
      {
        metrics.status = serializer->Serialize(resized, &serialized);
        SHUTDOWN_ON_FAILURE();
        metrics.num_ipc += serialized.size();
        metrics.ipc_bytes += ByteSizeOf(serialized);
        // Mark time points serialized for all batches.
        lat[TimePoints::serialized] = illex::Timer::now();
        // Copy the latency statistics to all serialized batches.
        for (auto& s : serialized) {
          s.time_points = lat;
        }
        t_stages.Split();
      }

      // Enqueue IPC items
      {
        for (const auto& sb : serialized) {
          SPDLOG_DEBUG("Enqueued IPC message with records {}...{}", sb.seq_range.first,
                       sb.seq_range.last);
          out->enqueue(sb);
        }
      }
      t_stages.Split();

      // Add parse time to stats.
      metrics.t.parse += t_stages.seconds()[0];
      metrics.t.resize += t_stages.seconds()[1];
      metrics.t.serialize += t_stages.seconds()[2];
      metrics.t.enqueue += t_stages.seconds()[3];
    }

    std::this_thread::sleep_for(std::chrono::microseconds(BOLSON_QUEUE_WAIT_US));
  }

  t_thread.Stop();
  metrics.t.thread = t_thread.seconds();
  metrics_promise.set_value(metrics);
  SPDLOG_DEBUG("Thread {:2} | Terminating.", id);
#undef SHUTDOWN_ON_FAILURE
}

auto Converter::Start(std::atomic<bool>* shutdown) -> Status {
  shutdown_ = shutdown;
  auto buffers = parser_context()->mutable_buffers().size();

  if ((num_threads_ > 1) || ((num_threads_ == 1) && (buffers == 1))) {
    SPDLOG_DEBUG("Spawning {} one-to-one parser threads.", num_threads_);
    // One to one parsers, spawn as many threads as parser context allows, and give each
    // thread a parser to work with.
    for (int t = 0; t < num_threads_; t++) {
      std::promise<Metrics> m;
      metrics_futures_.push_back(m.get_future());
      threads_.emplace_back(
          OneToOneConvertThread, t, parser_context_->parsers()[t].get(), resizers_[t],
          serializers_[t], parser_context_->mutable_buffers(), parser_context_->mutexes(),
          output_queue_, shutdown_, std::move(m));
    }
  } else if (num_threads_ == 1) {
    SPDLOG_DEBUG("Spawning one many-to-one parser thread.");
    // Many to one parsers, spawn one thread, give the thread the only parser.
    // This parser can operate on all input buffers.
    assert(parser_context()->parsers().size() == 1);
    std::promise<Metrics> m;
    metrics_futures_.push_back(m.get_future());
    threads_.emplace_back(AllToOneConverterThread, 0, parser_context_->parsers()[0].get(),
                          resizers_[0], serializers_[0],
                          parser_context_->mutable_buffers(), parser_context_->mutexes(),
                          output_queue_, shutdown_, std::move(m));
  }
  return Status::OK();
}

auto Converter::Finish() -> MultiThreadStatus {
  MultiThreadStatus result;
  // Attempt to join all threads.
  for (size_t t = 0; t < threads_.size(); t++) {
    // Check if the thread can be joined.
    if (threads_[t].joinable()) {
      threads_[t].join();
      // Get the metrics.
      auto metric = metrics_futures_[t].get();
      metrics_.push_back(metric);
      result.push_back(metric.status);
      // If a thread returned an error status, shut everything down.
      if (!metric.status.ok()) {
        shutdown_->store(true);
      }
    }
  }
  return result;
}

auto Converter::Make(const ConverterOptions& opts, publish::IpcQueue* ipc_queue,
                     std::shared_ptr<Converter>* out) -> Status {
  std::shared_ptr<parse::ParserContext> parser_context;
  std::vector<std::shared_ptr<Resizer>> resizers;
  std::vector<std::shared_ptr<Serializer>> serializers;

  // Determine which parser and allocator implementation to use.
  switch (opts.parser.impl) {
    case parse::Impl::ARROW:
      BOLSON_ROE(parse::ArrowParserContext::Make(opts.parser.arrow, opts.num_threads,
                                                 opts.input_size, &parser_context));
      break;
    case parse::Impl::OPAE_BATTERY:
      BOLSON_ROE(parse::opae::BatteryParserContext::Make(opts.parser.opae_battery,
                                                         &parser_context));
      break;
    case parse::Impl::OPAE_TRIP:
      BOLSON_ROE(
          parse::opae::TripParserContext::Make(opts.parser.opae_trip, &parser_context));
      break;
    case parse::Impl::CUSTOM_BATTERY:
      BOLSON_ROE(parse::custom::BatteryParserContext::Make(
          opts.parser.custom_battery, opts.num_threads, opts.input_size,
          &parser_context));
      break;
    case parse::Impl::CUSTOM_TRIP:
      BOLSON_ROE(parse::custom::TripParserContext::Make(
          opts.parser.custom_trip, opts.num_threads, opts.input_size, &parser_context));
      break;
    case parse::Impl::FPGA_BATTERY:
      BOLSON_ROE(parse::fpga::BatteryParserContext::Make(
          opts.parser.fpga_battery, opts.input_size, &parser_context));
      break;
  }

  assert(parser_context != nullptr);

  // Determine how many threads this context allows to use.
  auto num_threads = parser_context->CheckThreadCount(opts.num_threads);
  if (num_threads != opts.num_threads) {
    spdlog::warn("Parser implementation cannot use {} threads, using {} threads instead.",
                 opts.num_threads, num_threads);
  }

  // Set up Resizers and Serializers.
  for (size_t t = 0; t < num_threads; t++) {
    if (!opts.mock_resize) {
      resizers.push_back(std::make_shared<Resizer>(opts.max_batch_rows));
    } else {
      resizers.push_back(std::make_shared<ResizerMock>());
    }
    if (!opts.mock_serialize) {
      serializers.push_back(std::make_shared<Serializer>(opts.max_ipc_size));
    } else {
      serializers.push_back(std::make_shared<SerializerMock>());
    }
  }

  // Create the converter.
  auto result = std::shared_ptr<convert::Converter>(new convert::Converter(
      parser_context, resizers, serializers, ipc_queue, num_threads));

  *out = std::move(result);

  return Status::OK();
}

auto Converter::parser_context() const -> std::shared_ptr<parse::ParserContext> {
  return parser_context_;
}

Converter::Converter(std::shared_ptr<parse::ParserContext> parser_context,
                     std::vector<std::shared_ptr<convert::Resizer>> resizers,
                     std::vector<std::shared_ptr<convert::Serializer>> serializers,
                     publish::IpcQueue* output_queue, size_t num_threads)
    : parser_context_(std::move(parser_context)),
      resizers_(std::move(resizers)),
      serializers_(std::move(serializers)),
      output_queue_(output_queue),
      num_threads_(num_threads) {
  assert(output_queue_ != nullptr);
  assert(num_threads_ != 0);
}

// Sequence number field.
static inline auto SeqField() -> std::shared_ptr<arrow::Field> {
  static auto seq_field = arrow::field("seq", arrow::uint64(), false);
  return seq_field;
}

auto ConverterOptions::ParseInput() -> Status {
  BOLSON_ROE(ParseWithScale(this->input_size_str, &this->input_size));
  return Status::OK();
}

void AddConverterOptionsToCLI(CLI::App* sub, convert::ConverterOptions* opts) {
  sub->add_option("--max-rows", opts->max_batch_rows,
                  "Maximum number of rows per RecordBatch.")
      ->default_val(1024);
  sub->add_option("--max-ipc", opts->max_ipc_size,
                  "Maximum size of IPC messages in bytes.")
      ->default_val(BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE);
  sub->add_option("--threads", opts->num_threads,
                  "Number of threads to use for conversion.")
      ->default_val(1);
  sub->add_option("--input-buffers-capacity", opts->input_size_str,
                  "Total capacity of all input buffers in bytes. Also accepts <n>KiB, "
                  "<n>MiB, etc.")
      ->default_val("16MiB");
  AddParserOptions(sub, &opts->parser);
}

}  // namespace bolson::convert
