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

#include "./pulsar.h"

auto SetupClientProducer(const std::string &pulsar_url,
                         const std::string &pulsar_topic,
                         pulsar::LoggerFactory *logger,
                         std::pair<std::shared_ptr<pulsar::Client>,
                                   std::shared_ptr<pulsar::Producer>> *out) -> pulsar::Result {
  auto config = pulsar::ClientConfiguration();
  config.setLogger(logger);
  auto client = std::make_shared<pulsar::Client>(pulsar_url, config);
  auto producer = std::make_shared<pulsar::Producer>();
  pulsar::Result result = client->createProducer(pulsar_topic, *producer);
  *out = {client, producer};
  return result;
}

auto PublishArrowBuffer(const std::shared_ptr<pulsar::Producer> &producer,
                        const std::shared_ptr<arrow::Buffer> &buffer) -> pulsar::Result {
  pulsar::Message msg = pulsar::MessageBuilder().setAllocatedContent(reinterpret_cast<void *>(buffer->mutable_data()),
                                                                     buffer->size()).build();
  return producer->send(msg);
}
