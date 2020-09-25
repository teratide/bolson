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

#include <thread>
#include <illex/raw_client.h>
#include <illex/zmq_client.h>
#include <putong/timer.h>

#include "flitter/stream.h"
#include "flitter/converter.h"
#include "flitter/pulsar.h"

namespace flitter {

#define illex(status) { \
  if (!status.ok()) { \
    spdlog::error("illex error: {}", status.msg()); \
    return -1; \
  } \
}

auto ProduceFromStream(const StreamOptions &opt) -> int {
  if (std::holds_alternative<illex::ZMQProtocol>(opt.protocol)) {
    illex::Queue output;
    illex::ZMQClient client;
    illex::ZMQClient::Create(std::get<illex::ZMQProtocol>(opt.protocol), "localhost", &client);
    client.ReceiveJSONs(&output);
    client.Close();
  } else {
    putong::Timer t;
    std::atomic<bool> shutdown = false;
    illex::Queue raw_json_queue;
    IpcQueue arrow_ipc_queue;
    auto pulsar_logger = FlitterLoggerFactory::create();
    ClientProducerPair client_prod;
    auto pulsar_result = SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, pulsar_logger.get(), &client_prod);

    std::atomic<size_t> publish_count = 0;
    auto publish_thread = std::thread(PublishThread, client_prod.second, &arrow_ipc_queue, &shutdown, &publish_count);
    auto converter_thread = std::thread(ConversionHiveThread,
                                        &raw_json_queue,
                                        &arrow_ipc_queue,
                                        &shutdown,
                                        opt.num_conversion_drones);

    illex::RawClient client;

    illex(illex::RawClient::Create(std::get<illex::RawProtocol>(opt.protocol), "localhost", &client));

    if (opt.profile) { t.Start(); }

    illex(client.ReceiveJSONs(&raw_json_queue));
    illex(client.Close());

    spdlog::info("Received {} JSONs over TCP.", client.received());

    while (client.received() != publish_count) {
#ifndef NDEBUG
      std::this_thread::sleep_for(std::chrono::seconds(1));
      SPDLOG_DEBUG("Received: {}, Published: {}", client.received(), publish_count);
#endif
    }

    if (opt.profile) {
      t.Stop();
      spdlog::info("Published {} messages in {} seconds.", publish_count, t.seconds());
    }

    shutdown = true;
    converter_thread.join();
    publish_thread.join();
  }

  return 0;
}

}  // namespace flitter
