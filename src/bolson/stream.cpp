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
#include <memory>
#include <thread>
#include <vector>
#include <putong/timer.h>

#include "bolson/latency.h"
#include "bolson/status.h"
#include "bolson/stream.h"
#include "bolson/pulsar.h"
#include "bolson/utils.h"
#include "bolson/parse/arrow_impl.h"
#include "bolson/parse/opae_battery_impl.h"

namespace bolson {

using convert::Stats;

/// Structure to hold timers.
struct StreamTimers {
  putong::Timer<> tcp;
  putong::Timer<> init;
};

/// Structure to hold threads and atomics
struct StreamThreads {
  std::unique_ptr<std::thread> publish_thread;
  std::atomic<bool> shutdown = false;
  std::atomic<size_t> publish_count = 0;

  /// Shut down the threads.
  void Shutdown(const std::shared_ptr<convert::Converter> &converter) {
    shutdown.store(true);
    converter->Finish();
    publish_thread->join();
  }
};

/// \brief Log the statistics.
static void LogStreamStats(const StreamTimers &timers,
                           const illex::RawClient &client,
                           const std::vector<Stats> &conv_stats,
                           const PublishStats &pub_stats) {
  auto all = AggrStats(conv_stats);
  spdlog::info("Initialization");
  spdlog::info("  Time : {}", timers.init.seconds());

  // TCP client statistics.
  auto tcp_MiB = static_cast<double>(client.bytes_received()) / (1024.0 * 1024.0);
  auto tcp_MB = static_cast<double>(client.bytes_received()) / 1E6;
  auto tcp_MJs = client.received() / 1E6;

  spdlog::info("TCP client:");
  spdlog::info("  JSONs received : {}", client.received());
  spdlog::info("  Bytes received : {} MiB", tcp_MiB);
  spdlog::info("  Time           : {} s", timers.tcp.seconds());
  spdlog::info("  Throughput     : {} MJ/s", tcp_MJs / timers.tcp.seconds());
  spdlog::info("  Throughput     : {} MB/s", tcp_MB / timers.tcp.seconds());

  spdlog::info("JSONs to IPC conversion:");
  LogConvertStats(all, conv_stats.size(), "  ");

  // Pulsar producer / publishing statistics
  auto pub_MJs = pub_stats.num_jsons_published / 1E6;

  spdlog::info("Publish stats:");
  spdlog::info("  JSONs published : {}", pub_stats.num_jsons_published);
  spdlog::info("  IPC messages    : {}", pub_stats.num_ipc_published);
  spdlog::info("  Time            : {} s", pub_stats.publish_time);
  spdlog::info("    in thread     : {} s", pub_stats.thread_time);
  spdlog::info("  Throughput      : {} MJ/s.", pub_MJs / pub_stats.publish_time);

  // Timer properties
  spdlog::info("Timers:");
  spdlog::info("  Steady?         : {}", putong::Timer<>::steady());
  spdlog::info("  Resolution      : {} us", putong::Timer<>::resolution_us());

}

// Macro to shut down threads in ProduceFromStream whenever Illex client returns some
// error.
#define SHUTDOWN_ON_FAILURE(status)                     \
  {                                                     \
    auto __status = status;                             \
    if (!__status.ok()) {                               \
      threads.Shutdown(converter);                      \
      return Status(Error::IllexError, __status.msg()); \
    }                                                   \
  } void()

auto ProduceFromStream(const StreamOptions &opt) -> Status {
  // Timers for throughput
  StreamTimers timers;
  // Check which protocol to use.
  if (std::holds_alternative<illex::RawProtocol>(opt.protocol)) {
    timers.init.Start();
    // Set up Pulsar client and producer.
    PulsarContext pulsar;
    BOLSON_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &pulsar));

    // Set up output queue.
    IpcQueue ipc_queue(16 * 1024 * 1024);

    StreamThreads threads;

    // TODO: wrap into a function from down here:

    // Set up futures for statistics delivered by threads.
    std::promise<PublishStats> pub_stats_promise;
    auto pub_stats_future = pub_stats_promise.get_future();

    // Select allocator.
    std::shared_ptr<buffer::Allocator> allocator;
    switch (opt.converter.implementation) {
      case parse::Impl::ARROW:allocator = std::make_shared<buffer::Allocator>();
        break;
      case parse::Impl::OPAE_BATTERY:
        allocator = std::make_shared<buffer::OpaeAllocator>();
        break;
    }

    // Set up the Converter.
    std::shared_ptr<convert::Converter> converter;
    BOLSON_ROE(convert::Converter::Make(opt.converter, &ipc_queue, &converter));

    // Start the converter threads.
    converter->Start(&threads.shutdown);

    // Spawn the publish thread, which pulls from the IPC queue and publishes the IPC
    // messages in Pulsar.
    threads.publish_thread = std::make_unique<std::thread>(PublishThread,
                                                           std::move(pulsar),
                                                           &ipc_queue,
                                                           &threads.shutdown,
                                                           &threads.publish_count,
                                                           std::move(pub_stats_promise));

    // Set up the client that receives incoming JSONs.
    // We must shut down the already spawned threads in case the client returns some
    // errors.
    illex::DirectBufferClient client;
    SHUTDOWN_ON_FAILURE(illex::DirectBufferClient::Create(std::get<illex::RawProtocol>(opt.protocol),
                                                          opt.hostname,
                                                          0,
                                                          converter->mutable_buffers(),
                                                          converter->mutexes(),
                                                          &client));

    timers.init.Stop();

    // Receive JSONs (blocking) until the server closes the connection.
    // Concurrently, the conversion and publish thread will do their job.
    timers.tcp.Start();
    SHUTDOWN_ON_FAILURE(client.ReceiveJSONs(nullptr));
    timers.tcp.Stop();
    SHUTDOWN_ON_FAILURE(client.Close());

    SPDLOG_DEBUG("Waiting to empty JSON queue.");
    // Once the server disconnects, we can work towards finishing down this function.
    // Wait until all JSONs have been published, or if either the publish or converter
    // thread have asserted the shutdown signal. The latter indicates some error.
    while ((client.received() != threads.publish_count.load())
        && !threads.shutdown.load()) {
      // TODO: use some conditional variable for this
      // Sleep this thread for a bit.
      std::this_thread::sleep_for(std::chrono::milliseconds(BOLSON_QUEUE_WAIT_US));
#ifndef NDEBUG
      // Sleep a bit longer in debug.
      std::this_thread::sleep_for(std::chrono::milliseconds(100 * BOLSON_QUEUE_WAIT_US));
      SPDLOG_DEBUG("Received: {}, Published: {}",
                   client.received(),
                   threads.publish_count.load());
#endif
    }

    // We can now shut down all threads and collect futures.
    threads.Shutdown(converter);

    auto conv_stats = converter->Statistics();
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
        return Status(Error::GenericError, "Not implemented.");
      } else {
        LogStreamStats(timers, client, conv_stats, pub_stats);
        opt.pulsar.Log();
        spdlog::info("Implementation    : {}", ToString(opt.converter.implementation));
        spdlog::info("Conversion threads: {}", opt.converter.num_threads);
        spdlog::info("TCP clients       : {}", 1);
        spdlog::info("Publish threads   : {}", 1);
      }
    }
  } else {
    return Status(Error::GenericError, "Not implemented.");
  }

  return Status(Error::GenericError, "Not implemented.");
}

}  // namespace bolson
