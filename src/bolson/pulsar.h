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

#include <memory>
#include <arrow/api.h>
#include <pulsar/Client.h>
#include <pulsar/Producer.h>
#include <putong/timer.h>

#include "bolson/log.h"
#include "bolson/status.h"
#include "bolson/converter.h"

namespace bolson {

/// Pulsar default max message size (from an obscure place in the Pulsar sources)
constexpr size_t PULSAR_DEFAULT_MAX_MESSAGE_SIZE = 5 * 1024 * 1024 - 10 * 1024;

/// A Pulsar context for functions to operate on.
struct PulsarContext {
  std::unique_ptr<pulsar::Client> client;
  std::unique_ptr<pulsar::Producer> producer;
};

/// Pulsar options.
struct PulsarOptions {
  /// Pulsar URL.
  std::string url = "pulsar://localhost:6650/";
  /// Pulsar topic to publish on.
  std::string topic = "bolson";
  /// Maximum message size.
  size_t max_msg_size = PULSAR_DEFAULT_MAX_MESSAGE_SIZE;
};

/// Statistics about publishing
struct PublishStats {
  /// Number of messages published.
  size_t num_published = 0;
  /// Time spent on publishing message.
  double publish_time = 0.;
  /// Time spent in thread.
  double thread_time = 0.;
  /// Status of the publishing thread.
  Status status = Status::OK();
};

/**
 * Set a Pulsar client and producer up.
 * \param url    The Pulsar broker service URL.
 * \param topic  The Pulsar topic to produce message in.
 * \param out    A context including the client and producer.
 * \return       Status::OK() if successful, some error otherwise.
 */
auto SetupClientProducer(const std::string &url,
                         const std::string &topic,
                         PulsarContext *out) -> Status;

/**
 * Publish an Arrow buffer as a Pulsar message through a Pulsar producer.
 * \param producer      The Pulsar producer to publish the message through.
 * \param buffer        The Arrow buffer to publish.
 * \param latency_timer A timer that, if supplied, is stopped by this function just before sending the message.
 * \return              Status::OK() if successful, some error otherwise.
 */
auto Publish(pulsar::Producer *producer,
             const uint8_t *buffer,
             size_t size,
             putong::Timer<> *latency_timer = nullptr) -> Status;

/**
 * \brief A thread to pull IPC messages from the queue and publish them to some Pulsar queue.
 * \param pulsar            Pulsar client and producer.
 * \param in                Incoming queue with IPC messages.
 * \param shutdown              If this is true, this thread will try to terminate.
 * \param count             The number of published messages.
 * \param latency_timer     An optional latency timer that is stopped just before the first Pulsar message is sent.
 * \param stats             Statistics about this thread.
 */
void PublishThread(PulsarContext pulsar,
                   IpcQueue *in,
                   std::atomic<bool> *shutdown,
                   std::atomic<size_t> *count,
                   putong::Timer<> *latency_timer,
                   std::promise<PublishStats> &&stats);

/**
 * \brief Factory function for the custom Pulsar logger.
 */
class bolsonLoggerFactory : public pulsar::LoggerFactory {
 public:
  auto getLogger(const std::string &file) -> pulsar::Logger * override;
  static auto create() -> std::unique_ptr<bolsonLoggerFactory>;
};

}  // namespace bolson
