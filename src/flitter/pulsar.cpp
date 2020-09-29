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
#include <arrow/api.h>
#include <pulsar/Client.h>
#include <pulsar/ClientConfiguration.h>
#include <pulsar/Producer.h>
#include <pulsar/Result.h>
#include <pulsar/Logger.h>
#include <pulsar/defines.h>

#include "flitter/log.h"
#include "flitter/pulsar.h"
#include "flitter/status.h"

#define CHECK_PULSAR(result) \
  if (result != pulsar::ResultOk) { \
    return Status(Error::PulsarError, std::string("Pulsar error: ") + pulsar::strResult(result)); \
  }

namespace flitter {

auto SetupClientProducer(const std::string &url,
                         const std::string &topic,
                         pulsar::LoggerFactory *logger,
                         ClientProducerPair *out) -> Status {
  auto config = pulsar::ClientConfiguration();
  config.setLogger(logger);
  auto client = std::make_shared<pulsar::Client>(url, config);
  auto producer = std::make_shared<pulsar::Producer>();
  auto result = client->createProducer(topic, *producer);
  CHECK_PULSAR(result);
  // TODO(johanpel): if createProducer fails and we return this function, the destructors of the pulsar objects cause
  //  a segmentation fault. This needs more investigation.
  *out = {client, producer};
  return Status::OK();
}

auto PublishArrowBuffer(const std::shared_ptr<pulsar::Producer> &producer,
                        const std::shared_ptr<arrow::Buffer> &buffer) -> Status {
  pulsar::Message msg = pulsar::MessageBuilder().setAllocatedContent(reinterpret_cast<void *>(buffer->mutable_data()),
                                                                     buffer->size()).build();
  CHECK_PULSAR(producer->send(msg));
  return Status::OK();
}

void PublishThread(const std::shared_ptr<pulsar::Producer> &producer,
                   IpcQueue *in,
                   std::atomic<bool> *stop,
                   std::atomic<size_t> *count) {
  while (!stop->load()) {
    std::shared_ptr<arrow::Buffer> ipc_msg;
    if (in->try_dequeue(ipc_msg)) {
      SPDLOG_DEBUG("[publish] Publishing Arrow IPC message.");
      PublishArrowBuffer(producer, ipc_msg);
      count->fetch_add(1);
    }
  }
}

void FlitterLogger::log(pulsar::Logger::Level level, int line, const std::string &message) {
  if (level == Level::LEVEL_WARN) {
    spdlog::warn("[pulsar] {}", message);
  } else {
    spdlog::error("[pulsar] {}", message);
  }
}

auto FlitterLogger::isEnabled(pulsar::Logger::Level level) -> bool { return level >= Level::LEVEL_WARN; }

FlitterLogger::FlitterLogger(std::string logger) : _logger(std::move(logger)) {}

auto FlitterLoggerFactory::getLogger(const std::string &fileName) -> pulsar::Logger * {
  return new FlitterLogger(fileName);
}

auto FlitterLoggerFactory::create() -> std::unique_ptr<FlitterLoggerFactory> {
  return std::make_unique<FlitterLoggerFactory>();
}

}  // namespace flitter
