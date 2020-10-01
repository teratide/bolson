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

#include "flitter/log.h"
#include "flitter/status.h"
#include "flitter/converter.h"

namespace flitter {

using ClientProducerPair = std::pair<std::shared_ptr<pulsar::Client>, std::shared_ptr<pulsar::Producer>>;

/// \brief Pulsar options.
struct PulsarOptions {
  std::string url = "pulsar://localhost:6650/";
  std::string topic = "flitter";
  // From an obscure place in the Pulsar sources
  size_t max_msg_size = (5 * 1024 * 1024 - (10 * 1024));
};

/// \brief Statistics about publishing
struct PublishStats {
  /// Number of messages published.
  size_t num_published = 0;
  /// Time spent on publishing message.
  double publish_time = 0.;
  /// Time spent in thread.
  double thread_time = 0.;
};

/**
 * Set a Pulsar client and producer up.
 * \param url    The Pulsar broker service URL.
 * \param topic  The Pulsar topic to produce message in.
 * \param out    A pair with shared pointers to the client and producer objects.
 * \return       Status::OK() if successful, some error otherwise.
 */
auto SetupClientProducer(const std::string &url,
                         const std::string &topic,
                         ClientProducerPair *out) -> Status;

/**
 * Publish an Arrow buffer as a Pulsar message through a Pulsar producer.
 * \param producer      The Pulsar producer to publish the message through.
 * \param buffer        The Arrow buffer to publish.
 * \param latency_timer A timer that is stopped by this function just before calling the Pulsar send function.
 * \return         Status::OK() if successful, some error otherwise.
 */
auto PublishArrowBuffer(const std::shared_ptr<pulsar::Producer> &producer,
                        const std::shared_ptr<arrow::Buffer> &buffer,
                        putong::Timer<> *latency_timer) -> Status;

void PublishThread(const std::shared_ptr<pulsar::Producer> &producer,
                   IpcQueue *in,
                   std::atomic<bool> *stop,
                   std::atomic<size_t> *count,
                   putong::Timer<> *latency_timer,
                   std::promise<PublishStats> &&stats);

class FlitterLoggerFactory : public pulsar::LoggerFactory {
 public:
  auto getLogger(const std::string &file) -> pulsar::Logger * override;
  static auto create() -> std::unique_ptr<FlitterLoggerFactory>;
};

}  // namespace flitter
