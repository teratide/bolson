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
#include <blockingconcurrentqueue.h>
#include <illex/latency.h>
#include <illex/protocol.h>
#include <pulsar/Client.h>
#include <pulsar/Producer.h>
#include <putong/timer.h>

#include <future>
#include <memory>

#include "bolson/convert/serializer.h"
#include "bolson/log.h"
#include "bolson/publish/metrics.h"
#include "bolson/status.h"

namespace bolson::publish {

/// Initial IPC queue reservation.
#define BOLSON_PUBLISH_IPC_QUEUE_SIZE 1024

/// Default max. message size.
// From Pulsar sources.
#define BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE (5 * 1024 * 1024 - 10 * 1024)

/// An item in the IPC queue.
using IpcQueueItem = convert::SerializedBatch;

/// A queue with Arrow IPC messages.
using IpcQueue = moodycamel::BlockingConcurrentQueue<IpcQueueItem>;

/// Pulsar batching producer options.
struct BatchingOptions {
  /// Whether to enable batching.
  bool enable = false;
  /// Maximum batch messages.
  uint32_t max_messages;
  /// Maximum batch bytes.
  size_t max_bytes;
  /// Maximum batch delay.
  size_t max_delay_ms;
};

/// Pulsar options.
struct Options {
  /// Pulsar URL.
  std::string url;
  /// Pulsar topic to publish on.
  std::string topic;
  /// Maximum message size.
  size_t max_msg_size;
  /// Options related to batching producer.
  BatchingOptions batching;
  /// Number of Pulsar producers.
  size_t num_producers;
  /// Log these options.
  void Log() const;
};

/**
 * Publish an Arrow buffer as a Pulsar message through a Pulsar producer.
 * \param producer    The Pulsar producer to publish the message through.
 * \param buffer      The raw bytes buffer to publish.
 * \param size        The size of the buffer.
 * \return            Status::OK() if successful, some error otherwise.
 */
auto Publish(pulsar::Producer* producer, const uint8_t* buffer, size_t size) -> Status;

/**
 * \brief A thread to pull IPC messages from the queue and publish them to Pulsar.
 * \param producer  The producer to use for publishing.
 * \param queue     The queue with IPC messages.
 * \param shutdown  Shutdown signal.
 * \param count     Number of published rows.
 * \param metrics   Throughput metrics.
 */
void PublishThread(pulsar::Producer* producer, IpcQueue* queue,
                   std::atomic<bool>* shutdown, std::atomic<size_t>* count,
                   std::promise<Metrics>&& metrics);

/// A Pulsar context for functions to operate on.
struct ConcurrentPublisher {
 public:
  /**
   * Set up a Pulsar client with concurrent producers.
   * \param[in]     opts          Pulsar client and producer options.
   * \param[in,out] ipc_queue     A concurrent queue of IPC messages to pull from.
   * \param[out]    publish_count The number of published JSONs.
   * \param[out]    out           The constructed
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Make(const Options& opts, IpcQueue* ipc_queue,
                   std::atomic<size_t>* publish_count,
                   std::shared_ptr<ConcurrentPublisher>* out) -> Status;

  /**
   * \brief Start Pulsar producer threads.
   * \param[in] shutdown Shutdown signal.
   */
  void Start(std::atomic<bool>* shutdown);

  /**
   * \brief Finish producing, shutting down all threads and closing client and producers.
   * \return Status for each thread.
   */
  auto Finish() -> MultiThreadStatus;

  /// \brief Return publish metrics.
  [[nodiscard]] auto metrics() const -> std::vector<Metrics>;

 private:
  ConcurrentPublisher() = default;
  /// Concurrent queue to pull IPC messages from.
  IpcQueue* queue_ = nullptr;
  /// The Pulsar client.
  std::unique_ptr<pulsar::Client> client = nullptr;
  /// The Pulsar producers.
  std::vector<std::unique_ptr<pulsar::Producer>> producers;
  /// Shutdown signal for all threads.
  std::atomic<bool>* shutdown_ = nullptr;
  /// Published row count.
  std::atomic<size_t>* published_ = nullptr;
  /// The threads.
  std::vector<std::thread> threads;
  /// Publish metrics futures.
  std::vector<std::future<Metrics>> metrics_futures;
  /// Publish metrics for each thread..
  std::vector<Metrics> metrics_;
};

/**
 * \brief Factory function for the custom Pulsar logger.
 */
class bolsonLoggerFactory : public pulsar::LoggerFactory {
 public:
  auto getLogger(const std::string& file) -> pulsar::Logger* override;
  static auto create() -> std::unique_ptr<bolsonLoggerFactory>;
};

}  // namespace bolson::publish
