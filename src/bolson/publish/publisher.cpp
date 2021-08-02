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

#include <arrow/io/api.h>
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

  return Status::OK();
}

void ConcurrentPublisher::Start(std::atomic<bool>* shutdown) {
  shutdown_ = shutdown;
}

auto ConcurrentPublisher::Finish() -> MultiThreadStatus {
  MultiThreadStatus result;

  return result;
}

auto ConcurrentPublisher::metrics() const -> std::vector<Metrics> { return metrics_; }

void PublishThread(void* producer, IpcQueue* queue,
                   std::atomic<bool>* shutdown, std::atomic<size_t>* count,
                   std::promise<Metrics>&& metrics) {
  Metrics s;
  metrics.set_value(s);
}

void AddPublishOptsToCLI(CLI::App* sub, publish::Options* pulsar) {
  sub->add_option("-u,--pulsar-url", pulsar->url, "Pulsar broker service URL.")
      ->default_val("pulsar://localhost:6650/");
  sub->add_option("-t,--pulsar-topic", pulsar->topic, "Pulsar topic.")
      ->default_val("persistent://public/default/bolson");

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
