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
#include <memory>
#include <illex/raw_client.h>
#include <illex/zmq_client.h>
#include <putong/timer.h>

#include "flitter/stream.h"
#include "flitter/converter.h"
#include "flitter/pulsar.h"
#include "flitter/status.h"

namespace flitter {

// Structure to hold threads and atomics
struct StreamThreads {
  std::unique_ptr<std::thread> publish_thread;
  std::unique_ptr<std::thread> converter_thread;
  std::atomic<bool> shutdown = false;
  std::atomic<size_t> publish_count = 0;

  void Shutdown() {
    shutdown.store(true);
    converter_thread->join();
    publish_thread->join();
  }
};

// Macro to shut down threads in the function below whenever Illex client returns some error.
#define SHUTDOWN_ON_ERROR(status) \
  if (!status.ok()) { \
    threads.Shutdown(); \
    return Status(Error::IllexError, status.msg()); \
  }

auto ProduceFromStream(const StreamOptions &opt) -> Status {
  putong::Timer t;

  // Check which protocol to use.
  if (std::holds_alternative<illex::ZMQProtocol>(opt.protocol)) {
    return Status(Error::GenericError, "Not implemented.");
//    illex::Queue output;
//    illex::ZMQClient client;
//    illex::ZMQClient::Create(std::get<illex::ZMQProtocol>(opt.protocol), "localhost", &client);
//    CHECK_ILLEX(client.ReceiveJSONs(&output));
//    CHECK_ILLEX(client.Close());
  } else {
    StreamThreads threads;

    // Set up Pulsar client and producer.
    ClientProducerPair client_prod;
    FLITTER_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &client_prod));
    // (Here we can still normally return on error since no thread have spawned yet)

    // Set up queues.
    illex::Queue raw_json_queue;
    IpcQueue arrow_ipc_queue;

    // Spawn two threads:
    // The conversion thread spawns one or multiple drone threads that convert JSONs to Arrow IPC messages and queues
    // them. The publish thread pull from the Arrow IPC queue, fed by the conversion thread, and publish them in Pulsar.
    threads.converter_thread = std::make_unique<std::thread>(ConversionHiveThread,
                                                             &raw_json_queue,
                                                             &arrow_ipc_queue,
                                                             &threads.shutdown,
                                                             opt.num_conversion_drones);
    threads.publish_thread = std::make_unique<std::thread>(PublishThread,
                                                           client_prod.second,
                                                           &arrow_ipc_queue,
                                                           &threads.shutdown,
                                                           &threads.publish_count);

    // Set up the client that receives incoming JSONs.
    // We must shut down the threads in case the client returns some errors.
    illex::RawClient client;
    SHUTDOWN_ON_ERROR(illex::RawClient::Create(std::get<illex::RawProtocol>(opt.protocol), opt.hostname, &client));

    if (opt.profile) { t.Start(); }

    // Receive JSONs until the server closes the connection.
    // Concurrently, the conversion and publish thread will do their job.
    SHUTDOWN_ON_ERROR(client.ReceiveJSONs(&raw_json_queue));
    SHUTDOWN_ON_ERROR(client.Close());

    // Once the server disconnects, we can work towards shutting down this function.
    // Wait until all JSONs have been published.
    while (client.received() != threads.publish_count.load()) {
#ifndef NDEBUG
      std::this_thread::sleep_for(std::chrono::seconds(1));
      SPDLOG_DEBUG("Received: {}, Published: {}", client.received(), threads.publish_count.load());
#endif
    }

    if (opt.profile) {
      t.Stop();
      spdlog::info("Published {} messages in {} seconds.", threads.publish_count.load(), t.seconds());
    }

    // We can now shut down all threads.
    threads.Shutdown();

    // Report some statistics.
    spdlog::info("Received {} JSONs over TCP.", client.received());
  }

  return Status::OK();
}

}  // namespace flitter
