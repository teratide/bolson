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
  putong::Timer<> latency_timer;

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

    // Set up queues.
    illex::Queue raw_json_queue;
    IpcQueue arrow_ipc_queue;

    // Set up futures for statistics delivered by threads.
    std::promise<std::vector<ConversionStats>> conv_stats_promise;
    auto conv_stats_future = conv_stats_promise.get_future();
    std::promise<PublishStats> pub_stats_promise;
    auto pub_stats_future = pub_stats_promise.get_future();

    // Spawn two threads:
    // The conversion thread spawns one or multiple drone threads that convert JSONs to Arrow IPC messages and queues
    // them. The publish thread pull from the Arrow IPC queue, fed by the conversion thread, and publish them in Pulsar.
    threads.converter_thread = std::make_unique<std::thread>(ConversionHiveThread,
                                                             &raw_json_queue,
                                                             &arrow_ipc_queue,
                                                             &threads.shutdown,
                                                             opt.num_conversion_drones,
                                                             std::move(conv_stats_promise));

    threads.publish_thread = std::make_unique<std::thread>(PublishThread,
                                                           opt.pulsar,
                                                           &arrow_ipc_queue,
                                                           &threads.shutdown,
                                                           &threads.publish_count,
                                                           &latency_timer,
                                                           std::move(pub_stats_promise));

    // Set up the client that receives incoming JSONs.
    // We must shut down the threads in case the client returns some errors.
    illex::RawClient client;
    SHUTDOWN_ON_ERROR(illex::RawClient::Create(std::get<illex::RawProtocol>(opt.protocol), opt.hostname, &client));

    // Receive JSONs until the server closes the connection.
    // Concurrently, the conversion and publish thread will do their job.
    SHUTDOWN_ON_ERROR(client.ReceiveJSONs(&raw_json_queue, &latency_timer));
    SHUTDOWN_ON_ERROR(client.Close());

    SPDLOG_DEBUG("Waiting to empty JSON queue.");
    // Once the server disconnects, we can work towards shutting down this function.
    // Wait until all JSONs have been published.
    while (client.received() != threads.publish_count.load()) {
#ifndef NDEBUG
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      SPDLOG_DEBUG("Received: {}, Published: {}", client.received(), threads.publish_count.load());
#endif
    }

    // We can now shut down all threads and collect futures.
    threads.Shutdown();
    auto conv_stats = conv_stats_future.get();
    auto pub_stats = pub_stats_future.get();

    // Report some statistics.
    if (opt.statistics) {
      spdlog::info("Received {} JSONs over TCP.", client.received());

      ConversionStats all_conv_stats;

      for (const auto &t : conv_stats) {
        all_conv_stats.num_jsons += t.num_jsons;
        all_conv_stats.convert_time += t.convert_time;
        all_conv_stats.thread_time += t.thread_time;
        all_conv_stats.ipc_bytes += t.ipc_bytes;
      }

      spdlog::info("Conversion stats:");
      spdlog::info("  JSONs converted     : {}", all_conv_stats.num_jsons);
      spdlog::info("  Total IPC bytes     : {}", all_conv_stats.ipc_bytes);
      spdlog::info("  Avg. bytes/msg      : {}", static_cast<double>(all_conv_stats.ipc_bytes) / all_conv_stats.num_jsons);
      spdlog::info("  Avg. conv. time     : {} us.", 1E6 * all_conv_stats.convert_time / all_conv_stats.num_jsons);
      spdlog::info("  Avg. thread time    : {} s.", all_conv_stats.thread_time / all_conv_stats.num_jsons);

      spdlog::info("Publish stats:");
      spdlog::info("  IPC messages        : {}", pub_stats.num_published);
      spdlog::info("  Avg. publish time   : {} us.", 1E6 * pub_stats.publish_time / pub_stats.num_published);
      spdlog::info("  Publish thread time : {} s", pub_stats.thread_time);

      spdlog::info("Latency stats:");
      spdlog::info("  First latency       : {} us", 1E6 * latency_timer.seconds());
    }
  }

  return Status::OK();
}

}  // namespace flitter
