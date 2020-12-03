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

#include <utility>
#include <pulsar/Client.h>
#include <pulsar/ClientConfiguration.h>
#include <pulsar/Producer.h>
#include <pulsar/Result.h>
#include <pulsar/Logger.h>
#include <putong/timer.h>

#include "bolson/log.h"
#include "bolson/pulsar.h"
#include "bolson/status.h"
#include "bolson/stream.h"

#define CHECK_PULSAR(result) { \
  auto res = result; \
  if (res != pulsar::ResultOk) \
    return Status(Error::PulsarError, std::string("Pulsar error: ") + pulsar::strResult(result)); \
}

namespace bolson {

auto SetupClientProducer(const std::string &url,
                         const std::string &topic,
                         PulsarContext *out) -> Status {
  auto config = pulsar::ClientConfiguration().setLogger(new bolsonLoggerFactory());
  out->client = std::make_unique<pulsar::Client>(url, config);
  out->producer = std::make_unique<pulsar::Producer>();
  CHECK_PULSAR(out->client->createProducer(topic, *out->producer));
  return Status::OK();
}

auto Publish(pulsar::Producer *producer,
             const uint8_t *buffer,
             size_t size,
             putong::Timer<> *latency_timer) -> Status {
  pulsar::Message msg = pulsar::MessageBuilder().setAllocatedContent(const_cast<uint8_t *>(buffer), size).build();
  // TODO: no
  if (latency_timer != nullptr) {
    latency_timer->Stop();
  }
  CHECK_PULSAR(producer->send(msg));
  return Status::OK();
}

void PublishThread(PulsarContext pulsar,
                   IpcQueue *in,
                   std::atomic<bool> *shutdown,
                   std::atomic<size_t> *count,
                   putong::Timer<> *latency, // TODO: this could be wrapped in an atomic
                   std::promise<PublishStats> &&stats) {

  // Remember whether this is the first message.
  bool first = true;
  // Set up timers.
  auto thread_timer = putong::Timer(true);
  auto publish_timer = putong::Timer();

  PublishStats s;

  // Try pulling stuff from the queue until the stop signal is given.
  IpcQueueItem ipc_item;
  while (!shutdown->load()) {
    if (in->wait_dequeue_timed(ipc_item, std::chrono::microseconds(100))) {
      SPDLOG_DEBUG("Publishing Arrow IPC message.");
      publish_timer.Start();
      //auto status = Publish(pulsar.producer.get(), ipc_item.ipc->data(), ipc_item.ipc->size(), latency);
      auto status = Status::OK();
      publish_timer.Stop();
      if (first) {
        latency = nullptr; // todo
        first = false;
      }

      // Deal with Pulsar errors.
      // In case the Producer causes some error, shut everything down.
      if (!status.ok()) {
        spdlog::error("Pulsar error: {}", status.msg());
        pulsar.producer->close();
        pulsar.client->close();
        // Fulfill the promise.
        s.status = status;
        stats.set_value(s);
        shutdown->store(true);
        return;
      }

      // Update some statistics.
      s.publish_time += publish_timer.seconds();
      s.num_published++;
      count->fetch_add(ipc_item.num_rows);
    }
    //std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  // Stop thread timer.
  thread_timer.Stop();
  s.thread_time = thread_timer.seconds();

  pulsar.producer->close();
  pulsar.client->close();
  // Fulfill the promise.
  stats.set_value(s);
}

/// A custom logger to redirect Pulsar client log messages to the Bolson logger.
class bolsonLogger : public pulsar::Logger {
  std::string _logger;
 public:
  explicit bolsonLogger(std::string logger) : _logger(std::move(logger)) {}
  auto isEnabled(Level level) -> bool override { return level >= Level::LEVEL_WARN; }
  void log(Level level, int line, const std::string &message) override {
    if (level == Level::LEVEL_WARN) {
      spdlog::warn("Pulsar warning: {}:{} {}", _logger, line, message);
    } else {
      spdlog::error("Pulsar error: {}:{} {}", _logger, line, message);
    }
  }
};

auto bolsonLoggerFactory::getLogger(const std::string &file) -> pulsar::Logger * { return new bolsonLogger(file); }

auto bolsonLoggerFactory::create() -> std::unique_ptr<bolsonLoggerFactory> {
  return std::make_unique<bolsonLoggerFactory>();
}

}  // namespace bolson
