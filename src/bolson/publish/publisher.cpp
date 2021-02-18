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

#include "bolson/publish/publisher.h"

#include <pulsar/Client.h>
#include <pulsar/ClientConfiguration.h>
#include <pulsar/Logger.h>
#include <pulsar/Producer.h>
#include <pulsar/Result.h>
#include <putong/timer.h>

#include <CLI/CLI.hpp>
#include <cassert>
#include <memory>
#include <utility>

#include "bolson/log.h"
#include "bolson/status.h"

#define CHECK_PULSAR(result)                                                 \
  {                                                                          \
    auto res = result;                                                       \
    if (res != pulsar::ResultOk) {                                           \
      return Status(Error::PulsarError,                                      \
                    std::string("Pulsar error: ") + pulsar::strResult(res)); \
    }                                                                        \
  }

namespace bolson::publish {

auto ConcurrentPublisher::Make(const Options& opts, IpcQueue* ipc_queue,
                               std::atomic<size_t>* publish_count,
                               std::shared_ptr<ConcurrentPublisher>* out) -> Status {
  auto* result = new ConcurrentPublisher();

  assert(ipc_queue != nullptr);
  assert(publish_count != nullptr);
  result->queue_ = ipc_queue;
  result->published_ = publish_count;

  // Configure client
  auto client_config = pulsar::ClientConfiguration().setLogger(new bolsonLoggerFactory());

  // Configure producer
  auto producer_config = pulsar::ProducerConfiguration();

  // Explicitly not setting Schema:
  // producer_config.setSchema()

  // Handle batching
  if (opts.batching.enable) {
    producer_config.setBatchingEnabled(opts.batching.enable)
        .setBatchingMaxAllowedSizeInBytes(opts.batching.max_bytes)
        .setBatchingMaxMessages(opts.batching.max_messages)
        .setBatchingMaxPublishDelayMs(opts.batching.max_delay_ms);
  }

  // Set up client.
  result->client = std::make_unique<pulsar::Client>(opts.url, client_config);

  // Create producer instances.
  for (int i = 0; i < opts.num_producers; i++) {
    std::unique_ptr<pulsar::Producer>& prod =
        result->producers.emplace_back(new pulsar::Producer);
    CHECK_PULSAR(result->client->createProducer(opts.topic, producer_config, *prod));
  }

  *out = std::shared_ptr<ConcurrentPublisher>(result);

  return Status::OK();
}

void ConcurrentPublisher::Start(std::atomic<bool>* shutdown) {
  shutdown_ = shutdown;
  for (auto& producer : producers) {
    std::promise<Metrics> s;
    metrics_futures.push_back(s.get_future());
    threads.emplace_back(PublishThread, producer.get(), queue_, shutdown_, published_,
                         std::move(s));
  }
}

auto ConcurrentPublisher::Finish() -> MultiThreadStatus {
  MultiThreadStatus result;
  // Attempt to join all threads.
  for (size_t t = 0; t < threads.size(); t++) {
    // Check if the thread can be joined.
    if (threads[t].joinable()) {
      threads[t].join();
      // Get the metrics.
      auto metric = metrics_futures[t].get();
      metrics_.push_back(metric);
      result.push_back(metric.status);
      // If a thread returned an error status, shut everything down.
      if (!metric.status.ok()) {
        shutdown_->store(true);
      }
      producers[t]->close();
    }
  }
  client->close();

  return result;
}

auto ConcurrentPublisher::metrics() const -> std::vector<Metrics> { return metrics_; }

auto Publish(pulsar::Producer* producer, const uint8_t* buffer, size_t size) -> Status {
  pulsar::Message msg = pulsar::MessageBuilder()
                            .setAllocatedContent(const_cast<uint8_t*>(buffer), size)
                            .build();
  // [IFR06]: The Pulsar messages leave the system through the Pulsar C++ client API
  // call pulsar::Producer::send().
  CHECK_PULSAR(producer->send(msg));
  return Status::OK();
}

void PublishThread(pulsar::Producer* producer, IpcQueue* queue,
                   std::atomic<bool>* shutdown, std::atomic<size_t>* count,
                   std::promise<Metrics>&& metrics) {
  // Set up timers.
  auto thread_timer = putong::Timer(true);
  auto publish_timer = putong::Timer();

  Metrics s;

  // Try pulling stuff from the queue until the stop signal is given.
  IpcQueueItem ipc_item;
  while (!shutdown->load()) {
    if (queue->wait_dequeue_timed(ipc_item,
                                  std::chrono::microseconds(BOLSON_QUEUE_WAIT_US))) {
      // Start measuring time to handle an IPC message on the Pulsar side.
      publish_timer.Start();

      // Publish the message
      ipc_item.time_points[TimePoints::popped] = illex::Timer::now();
      auto status = Publish(producer, ipc_item.message->data(), ipc_item.message->size());
      ipc_item.time_points[TimePoints::published] = illex::Timer::now();

      // Deal with Pulsar errors.
      // In case the Producer causes some error, shut everything down.
      if (!status.ok()) {
        spdlog::error("Pulsar error: {} for message of size {} B with {} rows.",
                      status.msg(), ipc_item.message->size(),
                      ipc_item.seq_range.last - ipc_item.seq_range.first);
        // Fulfill the promise.
        s.status = status;
        metrics.set_value(s);
        shutdown->store(true);
        return;
      }

      // Add number of rows in IPC message to the count, signaling the main thread how
      // many JSONs are published.
      assert(RecordSizeOf(ipc_item) != 0);
      count->fetch_add(RecordSizeOf(ipc_item));

      // Update some statistics.
      s.ipc++;
      s.rows += RecordSizeOf(ipc_item);
      publish_timer.Stop();
      s.publish_time += publish_timer.seconds();
      // Dump the latency stats.
      s.latencies.push_back({ipc_item.seq_range, ipc_item.time_points});
    }
  }
  // Stop thread timer.
  thread_timer.Stop();
  s.thread_time = thread_timer.seconds();

  // Fulfill the promise.
  metrics.set_value(s);
}

void AddPublishOptsToCLI(CLI::App* sub, publish::Options* pulsar) {
  sub->add_option("-u,--pulsar-url", pulsar->url, "Pulsar broker service URL.")
      ->default_val("pulsar://localhost:6650/");
  sub->add_option("-t,--pulsar-topic", pulsar->topic, "Pulsar topic.")
      ->default_val("non-persistent://public/default/bolson");

  sub->add_option("--pulsar-max-msg-size", pulsar->max_msg_size)
      ->default_val(BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE);

  sub->add_option("--pulsar-producers", pulsar->num_producers,
                  "Number of concurrent Pulsar producers.")
      ->default_val(1);

  // Pulsar batching defaults taken from the Pulsar CPP client sources.
  // pulsar-client-cpp/lib/ProducerConfigurationImpl.h
  sub->add_flag("--pulsar-batch", pulsar->batching.enable,
                "Enable batching Pulsar producer(s).");
  sub->add_option("--pulsar-batch-max-messages", pulsar->batching.max_messages,
                  "Pulsar batching max. messages.")
      ->default_val(1000);
  sub->add_option("--pulsar-batch-max-bytes", pulsar->batching.max_bytes,
                  "Pulsar batching max. bytes.")
      ->default_val(128 * 1024);
  sub->add_option("--pulsar-batch-max-delay", pulsar->batching.max_delay_ms,
                  "Pulsar batching max. delay (ms).")
      ->default_val(10);
}

/// A custom logger to redirect Pulsar client log messages to the Bolson logger.
class bolsonLogger : public pulsar::Logger {
  std::string _logger;

 public:
  explicit bolsonLogger(std::string logger) : _logger(std::move(logger)) {}
  auto isEnabled(Level level) -> bool override { return level >= Level::LEVEL_WARN; }
  void log(Level level, int line, const std::string& message) override {
    if (level == Level::LEVEL_WARN) {
      spdlog::warn("Pulsar warning: {}:{} {}", _logger, line, message);
    } else {
      spdlog::error("Pulsar error: {}:{} {}", _logger, line, message);
    }
  }
};

auto bolsonLoggerFactory::getLogger(const std::string& file) -> pulsar::Logger* {
  return new bolsonLogger(file);
}

auto bolsonLoggerFactory::create() -> std::unique_ptr<bolsonLoggerFactory> {
  return std::make_unique<bolsonLoggerFactory>();
}

void Options::Log() const {
  spdlog::info("Pulsar:");
  spdlog::info("  URL                     : {}", url);
  spdlog::info("  Topic                   : {}", topic);
  spdlog::info("  Max msg. size           : {} B", max_msg_size);
  spdlog::info("  Producer threads        : {}", num_producers);
  spdlog::info("  Batching                : {}", batching.enable);
  if (batching.enable) {
    spdlog::info("    Max. messages       : {}", batching.max_messages);
    spdlog::info("    Max. bytes          : {} B", batching.max_bytes);
    spdlog::info("    Max. delay          : {} ms", batching.max_delay_ms);
  }
}

}  // namespace bolson::publish
