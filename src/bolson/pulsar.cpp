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

#include "bolson/pulsar.h"

#include <pulsar/Client.h>
#include <pulsar/ClientConfiguration.h>
#include <pulsar/Logger.h>
#include <pulsar/Producer.h>
#include <pulsar/Result.h>
#include <putong/timer.h>

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

namespace bolson {

auto SetupClientProducer(const PulsarOptions& opts, PulsarConsumerContext* out)
    -> Status {
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
  out->client = std::make_unique<pulsar::Client>(opts.url, client_config);

  // Set up producer
  out->producer = std::make_unique<pulsar::Producer>();
  CHECK_PULSAR(out->client->createProducer(opts.topic, producer_config, *out->producer));

  return Status::OK();
}

auto Publish(pulsar::Producer* producer, const uint8_t* buffer, size_t size) -> Status {
  pulsar::Message msg = pulsar::MessageBuilder()
                            .setAllocatedContent(const_cast<uint8_t*>(buffer), size)
                            .build();
  // [IFR06]: The Pulsar messages leave the system through the Pulsar C++ client API
  // call pulsar::Producer::send().
  CHECK_PULSAR(producer->send(msg));
  return Status::OK();
}

void PublishThread(PulsarConsumerContext pulsar, IpcQueue* in,
                   std::atomic<bool>* shutdown, std::atomic<size_t>* count,
                   std::promise<PublishStats>&& stats,
                   std::promise<LatencyMeasurements>&& latencies) {
  // Set up timers.
  auto thread_timer = putong::Timer(true);
  auto publish_timer = putong::Timer();
  LatencyMeasurements lat;

  PublishStats s;

  // Try pulling stuff from the queue until the stop signal is given.
  IpcQueueItem ipc_item;
  while (!shutdown->load()) {
    if (in->wait_dequeue_timed(ipc_item,
                               std::chrono::microseconds(BOLSON_QUEUE_WAIT_US))) {
      // Start measuring time to handle an IPC message on the Pulsar side.
      publish_timer.Start();

      // Publish the message
      auto status = Publish(pulsar.producer.get(), ipc_item.message->data(),
                            ipc_item.message->size());

      // Deal with Pulsar errors.
      // In case the Producer causes some error, shut everything down.
      if (!status.ok()) {
        spdlog::error("Pulsar error: {} for message of size {} B with {} rows.",
                      status.msg(), ipc_item.message->size(),
                      ipc_item.seq_range.last - ipc_item.seq_range.first);
        pulsar.producer->close();
        pulsar.client->close();
        // Fulfill the promise.
        s.status = status;
        stats.set_value(s);
        shutdown->store(true);
        return;
      }

      // Measure point in time after publishing the batch.
      ipc_item.time_points[TimePoints::published] = illex::Timer::now();

      // Add number of rows in IPC message to the count, signaling the main thread how
      // many JSONs are published.
      count->fetch_add(RecordSizeOf(ipc_item));

      // Update some statistics.
      s.num_ipc_published++;
      s.num_jsons_published += RecordSizeOf(ipc_item);
      publish_timer.Stop();
      s.publish_time += publish_timer.seconds();
      // Dump the latency stats.
      lat.push_back({ipc_item.seq_range, ipc_item.time_points});
    }
  }
  // Stop thread timer.
  thread_timer.Stop();
  s.thread_time = thread_timer.seconds();

  pulsar.producer->close();
  pulsar.client->close();
  // Fulfill the promise.
  stats.set_value(s);
  latencies.set_value(lat);
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

}  // namespace bolson
