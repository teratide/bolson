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

#include <iostream>
#include <thread>
#include <memory>
#include <illex/raw_client.h>
#include <illex/zmq_client.h>
#include <putong/timer.h>

#include "bolson/stream.h"
#include "bolson/pulsar.h"
#include "bolson/status.h"
#include "bolson/convert/convert.h"
#include "bolson/convert/cpu.h"
#include "bolson/convert/opae_battery.h"

namespace bolson {

using convert::Stats;

/// Structure to hold timers.
struct StreamTimers {
  putong::Timer<> latency;
  putong::Timer<> tcp;
  putong::Timer<> init;
};

/// Structure to hold threads and atomics
struct StreamThreads {
  std::unique_ptr<std::thread> publish_thread;
  std::unique_ptr<std::thread> converter_thread;
  std::atomic<bool> shutdown = false;
  std::atomic<size_t> publish_count = 0;

  /// Shut down the threads.
  void Shutdown() {
    shutdown.store(true);
    converter_thread->join();
    publish_thread->join();
  }
};

/// \brief Stream succinct CSV-like stats to some output stream.
static void OutputCSVStats(const StreamTimers &timers,
                           const illex::RawClient &client,
                           const std::vector<Stats> &conv_stats,
                           const PublishStats &pub_stats,
                           std::ostream *output) {
  auto all_conv_stats = AggrStats(conv_stats);
  (*output) << client.received() << ",";
  (*output) << client.bytes_received() << ",";
  (*output) << timers.tcp.seconds() << ",";
  (*output) << all_conv_stats.num_jsons << ",";
  //(*output) << all_conv_stats.num_ipc << ","; // this one is redundant
  (*output) << all_conv_stats.total_ipc_bytes << ",";
  (*output)
      << static_cast<double>(all_conv_stats.total_ipc_bytes) / all_conv_stats.num_jsons
      << ",";
  (*output) << all_conv_stats.convert_time / all_conv_stats.num_jsons << ",";
  (*output) << all_conv_stats.thread_time / all_conv_stats.num_jsons << ",";
  (*output) << pub_stats.num_published << ",";
  (*output) << pub_stats.publish_time / pub_stats.num_published << ",";
  (*output) << pub_stats.thread_time << ",";
  (*output) << timers.latency.seconds() << std::endl;
}

/// \brief Log the statistics.
static void LogStats(const StreamTimers &timers,
                     const illex::RawClient &client,
                     const std::vector<Stats> &conv_stats,
                     const PublishStats &pub_stats) {
  auto all = AggrStats(conv_stats);
  spdlog::info("Initialization stats:");
  spdlog::info("  Initialization time : {}", timers.init.seconds());
  spdlog::info("TCP stats:");
  spdlog::info("  Received {} JSONs over TCP.", client.received());
  spdlog::info("  Bytes received      : {} MiB",
               static_cast<double>(client.bytes_received()) / (1024.0 * 1024.0));
  spdlog::info("  Client receive time : {} s", timers.tcp.seconds());
  spdlog::info("  Throughput          : {} MB/s",
               client.bytes_received() / timers.tcp.seconds() * 1E-6);

  LogConvertStats(all, conv_stats.size());

  spdlog::info("Publish stats:");
  spdlog::info("  IPC messages        : {}", pub_stats.num_published);
  spdlog::info("  Avg. publish time   : {} us.",
               1E6 * pub_stats.publish_time / pub_stats.num_published);
  spdlog::info("  Publish thread time : {} s", pub_stats.thread_time);

  spdlog::info("Latency stats:");
  spdlog::info("  First latency       : {} us", 1E6 * timers.latency.seconds());

  spdlog::info("Timer steady?         : {}", putong::Timer<>::steady());
  spdlog::info("Timer resoluion       : {} us", putong::Timer<>::resolution_us());

}

// Macro to shut down threads in ProduceFromStream whenever Illex client returns some error.
#define SHUTDOWN_ON_FAILURE(status) \
  if (!status.ok()) { \
    threads.Shutdown(); \
    return Status(Error::IllexError, status.msg()); \
  } void()

auto ProduceFromStream(const StreamOptions &opt) -> Status {
  StreamTimers timers;
  // Check which protocol to use.
  if (std::holds_alternative<illex::ZMQProtocol>(opt.protocol)) {
    return Status(Error::GenericError, "Not implemented.");
//    illex::Queue output;
//    illex::ZMQClient client;
//    illex::ZMQClient::Create(std::get<illex::ZMQProtocol>(opt.protocol), "localhost", &client);
//    CHECK_ILLEX(client.ReceiveJSONs(&output));
//    CHECK_ILLEX(client.Close());
  } else {
    timers.init.Start();
    // Set up Pulsar client and producer.
    PulsarContext pulsar;
    BOLSON_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &pulsar));

    StreamThreads threads;

    // Set up queues.
    illex::JSONQueue raw_json_queue;
    IpcQueue arrow_ipc_queue;

    // Set up futures for statistics delivered by threads.
    std::promise<std::vector<Stats>> conv_stats_promise;
    auto conv_stats_future = conv_stats_promise.get_future();
    std::promise<PublishStats> pub_stats_promise;
    auto pub_stats_future = pub_stats_promise.get_future();

    // Spawn two threads:
    // The conversion thread spawns one or multiple drone threads that convert JSONs to Arrow IPC messages and queues
    // them.
    switch (opt.conversion) {
      case convert::Impl::CPU:
        threads.converter_thread = std::make_unique<std::thread>(convert::ConvertWithCPU,
                                                                 &raw_json_queue,
                                                                 &arrow_ipc_queue,
                                                                 &threads.shutdown,
                                                                 opt.num_threads,
                                                                 opt.parse,
                                                                 opt.read,
                                                                 opt.json_threshold,
                                                                 opt.batch_threshold,
                                                                 std::move(
                                                                     conv_stats_promise));
        break;
      case convert::Impl::OPAE_BATTERY:
        threads.converter_thread =
            std::make_unique<std::thread>(convert::ConvertBatteryWithOPAE,
                                          &raw_json_queue,
                                          &arrow_ipc_queue,
                                          &threads.shutdown,
                                          opt.json_threshold,
                                          opt.batch_threshold,
                                          std::move(
                                              conv_stats_promise));
    }

    // The publish thread pull from the Arrow IPC queue, fed by the conversion thread, and publish them in Pulsar.
    threads.publish_thread = std::make_unique<std::thread>(PublishThread,
                                                           std::move(pulsar),
                                                           &arrow_ipc_queue,
                                                           &threads.shutdown,
                                                           &threads.publish_count,
                                                           &timers.latency,
                                                           std::move(pub_stats_promise));

    // Set up the client that receives incoming JSONs.
    // We must shut down the threads in case the client returns some errors.
    illex::RawClient client;
    SHUTDOWN_ON_FAILURE(illex::RawClient::Create(std::get<illex::RawProtocol>(opt.protocol),
                                                 opt.hostname,
                                                 opt.seq,
                                                 &client));

    timers.init.Stop();

    // Receive JSONs (blocking) until the server closes the connection.
    // Concurrently, the conversion and publish thread will do their job.
    timers.tcp.Start();
    SHUTDOWN_ON_FAILURE(client.ReceiveJSONs(&raw_json_queue, &timers.latency));
    timers.tcp.Stop();
    SHUTDOWN_ON_FAILURE(client.Close());

    SPDLOG_DEBUG("Waiting to empty JSON queue.");
    // Once the server disconnects, we can work towards finishing down this function. Wait until all JSONs have been
    // published, or if either the publish or converter thread have asserted the shutdown signal. The latter indicates
    // some error.
    while ((client.received() != threads.publish_count.load())
        && !threads.shutdown.load()) {
      // TODO: use some conditional variable for this
      // Sleep this thread for a bit.
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
#ifndef NDEBUG
      // Sleep a bit longer in debug.
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      SPDLOG_DEBUG("Received: {}, Published: {}", client.received(), threads.publish_count.load());
#endif
    }

    // We can now shut down all threads and collect futures.
    threads.Shutdown();

    auto conv_stats = conv_stats_future.get();
    auto pub_stats = pub_stats_future.get();

    // Check if the publish thread had an error.
    BOLSON_ROE(pub_stats.status);

    // Check if any of the converter threads had an error.
    for (size_t t = 0; t < conv_stats.size(); t++) {
      bool produce_error = false;
      std::stringstream msg;
      msg << "Convert threads reported the following errors:" << std::endl;
      if (!conv_stats[t].status.ok()) {
        msg << "  Thread:" << t << ", error: " << conv_stats[t].status.msg() << std::endl;
        produce_error = true;
      }
      if (produce_error) {
        return Status(Error::GenericError, msg.str());
      }
    }

    // Report some statistics.
    if (opt.statistics) {
      if (opt.succinct) {
        OutputCSVStats(timers, client, conv_stats, pub_stats, &std::cout);
      } else {
        LogStats(timers, client, conv_stats, pub_stats);
      }
    }
  }

  return Status::OK();
}

}  // namespace bolson
