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
#include "bolson/status.h"

namespace bolson {

/// An item in the IPC queue.
using IpcQueueItem = convert::SerializedBatch;

/// A queue with Arrow IPC messages.
using IpcQueue = moodycamel::BlockingConcurrentQueue<IpcQueueItem>;

/// Pulsar default max message size (from an obscure place in the Pulsar sources)
constexpr size_t PULSAR_DEFAULT_MAX_MESSAGE_SIZE = 5 * 1024 * 1024 - 32 * 1024;

/// Ipc queue item sorting function, sorts by sequence number.
struct {
  bool operator()(const IpcQueueItem& a, const IpcQueueItem& b) const {
    return a.seq_range.first < b.seq_range.first;
  }
} IpcSortBySeq;

/// A Pulsar context for functions to operate on.
struct PulsarConsumerContext {
  std::unique_ptr<pulsar::Client> client;
  std::unique_ptr<pulsar::Producer> producer;
};

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
struct PulsarOptions {
  /// Pulsar URL.
  std::string url;
  /// Pulsar topic to publish on.
  std::string topic;
  /// Maximum message size.
  size_t max_msg_size = PULSAR_DEFAULT_MAX_MESSAGE_SIZE;
  /// Options related to batching producer.
  BatchingOptions batching;

  inline void Log() const {
    spdlog::info("Pulsar:");
    spdlog::info("  URL             : {}", url);
    spdlog::info("  Topic           : {}", topic);
    spdlog::info("  Max msg. size   : {} B", max_msg_size);
    spdlog::info("  Batching        : {}", batching.enable);
    if (batching.enable) {
      spdlog::info("    Max. messages : {}", batching.max_messages);
      spdlog::info("    Max. bytes    : {} B", batching.max_bytes);
      spdlog::info("    Max. delay    : {} ms", batching.max_delay_ms);
    }
  }
};

/// Statistics about publishing
struct PublishStats {
  /// Number of JSONs published.
  size_t num_jsons_published = 0;
  /// Number of IPC messages published.
  size_t num_ipc_published = 0;
  /// Time spent on publishing message.
  double publish_time = 0.;
  /// Time spent in publish thread.
  double thread_time = 0.;
  /// Status of the publishing thread.
  Status status = Status::OK();
};

/**
 * Set up a Pulsar client and producer.
 * \param[in]  opts Pulsar client and producer options.
 * \param[out] out  A context including the client and producer.
 * \return Status::OK() if successful, some error otherwise.
 */
auto SetupClientProducer(const PulsarOptions& opts, PulsarConsumerContext* out) -> Status;

/**
 * Publish an Arrow buffer as a Pulsar message through a Pulsar producer.
 * \param producer    The Pulsar producer to publish the message through.
 * \param buffer      The raw bytes buffer to publish.
 * \param size        The size of the buffer.
 * \return            Status::OK() if successful, some error otherwise.
 */
auto Publish(pulsar::Producer* producer, const uint8_t* buffer, size_t size) -> Status;

/**
 * \brief A thread to pull IPC messages from the queue and publish them to a Pulsar queue.
 * \param pulsar        Pulsar client and producer.
 * \param in            Incoming queue with IPC messages.
 * \param shutdown      If this is true, this thread will try to terminate.
 * \param count         The number of published messages.
 * \param stats         Statistics about this thread.
 * \param latencies     Latency statistics for batches.
 */
void PublishThread(PulsarConsumerContext pulsar, IpcQueue* in,
                   std::atomic<bool>* shutdown, std::atomic<size_t>* count,
                   std::promise<PublishStats>&& stats,
                   std::promise<LatencyMeasurements>&& latencies);

/**
 * \brief Factory function for the custom Pulsar logger.
 */
class bolsonLoggerFactory : public pulsar::LoggerFactory {
 public:
  auto getLogger(const std::string& file) -> pulsar::Logger* override;
  static auto create() -> std::unique_ptr<bolsonLoggerFactory>;
};

}  // namespace bolson
