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
#include "bolson/convert/convert_queued.h"
#include "bolson/convert/cpu.h"
#include "bolson/convert/opae_battery.h"

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

  LogConvertStats(all, conv_stats.size());

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
#define SHUTDOWN_ON_FAILURE(status) \
  { \
    auto __status = status; \
    if (!__status.ok()) { \
      threads.Shutdown(); \
      return Status(Error::IllexError, __status.msg()); \
    } \
  } void()

auto ProduceFromStream(const StreamOptions &opt) -> Status {
  illex::LatencyTracker lat_tracker
      (opt.latency.num_samples, BOLSON_LAT_NUM_POINTS, opt.latency.interval);
  // Timers for throughput
  StreamTimers timers;
  // Check which protocol to use.
  if (std::holds_alternative<illex::RawProtocol>(opt.protocol)) {
    timers.init.Start();
    // Set up Pulsar client and producer.
    PulsarContext pulsar;
    BOLSON_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &pulsar));

    StreamThreads threads;

    // Set up queues.
    illex::JSONQueue raw_json_queue(16 * 1024 * 1024);
    IpcQueue arrow_ipc_queue(16 * 1024 * 1024);

    // Set up futures for statistics delivered by threads.
    std::promise<std::vector<Stats>> conv_stats_promise;
    auto conv_stats_future = conv_stats_promise.get_future();
    std::promise<PublishStats> pub_stats_promise;
    auto pub_stats_future = pub_stats_promise.get_future();

    // Spawn a conversion thread. This has various possible implementations.
    // It takes JSON items and converts them into Arrow RecordBatches and serializes those
    // into Arrow IPC messages, and pushes them to the IPC queue.
    switch (opt.conversion) {
      case convert::Impl::CPU: {
        threads.converter_thread =
            std::make_unique<std::thread>(convert::ConvertFromQueueWithCPU,
                                          &raw_json_queue,
                                          &arrow_ipc_queue,
                                          &threads.shutdown,
                                          opt.num_threads,
                                          opt.parse,
                                          opt.read,
                                          opt.json_threshold,
                                          opt.batch_threshold,
                                          &lat_tracker,
                                          std::move(conv_stats_promise));
        break;
      }
      case convert::Impl::OPAE_BATTERY: {
        threads.converter_thread = std::make_unique<std::thread>(
            convert::ConvertBatteryWithOPAE,
            opt.json_threshold,
            opt.batch_threshold,
            &raw_json_queue,
            &arrow_ipc_queue,
            &lat_tracker,
            &threads.shutdown,
            std::move(conv_stats_promise));
      }
    }

    // Spawn the publish thread, which pulls from the IPC queue and publishes the IPC
    // messages in Pulsar.
    threads.publish_thread = std::make_unique<std::thread>(PublishThread,
                                                           std::move(pulsar),
                                                           &arrow_ipc_queue,
                                                           &threads.shutdown,
                                                           &threads.publish_count,
                                                           &lat_tracker,
                                                           std::move(pub_stats_promise));

    // Set up the client that receives incoming JSONs.
    // We must shut down the already spawned threads in case the client returns some
    // errors.
    illex::RawQueueingClient client;
    SHUTDOWN_ON_FAILURE(illex::RawQueueingClient::Create(
        std::get<illex::RawProtocol>(opt.protocol),
        opt.hostname,
        opt.seq,
        &raw_json_queue,
        &client)
    );

    timers.init.Stop();

    // Receive JSONs (blocking) until the server closes the connection.
    // Concurrently, the conversion and publish thread will do their job.
    timers.tcp.Start();
    SHUTDOWN_ON_FAILURE(client.ReceiveJSONs(&lat_tracker));
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
        return Status(Error::GenericError, "Not implemented.");
      } else {
        LogStreamStats(timers, client, conv_stats, pub_stats);
        opt.pulsar.Log();
        spdlog::info("Implementation    : {}", ToString(opt.conversion));
        spdlog::info("Conversion threads: {}", opt.num_threads);
        // for clarity, but this may be more in the future:
        spdlog::info("Publish threads   : {}", 1);
        BOLSON_ROE(LogLatencyCSV(opt.latency.file, lat_tracker));
      }
    }
  } else {
    return Status(Error::GenericError, "Not implemented.");
  }

  return Status::OK();
}

auto ProduceFromStreamMultiBuffered(const StreamOptions &opt) -> Status {
  const size_t num_buffers = opt.num_threads;
  illex::LatencyTracker lat_tracker
      (opt.latency.num_samples, BOLSON_LAT_NUM_POINTS, opt.latency.interval);
  // Timers for throughput
  StreamTimers timers;
  // Check which protocol to use.
  if (std::holds_alternative<illex::RawProtocol>(opt.protocol)) {
    timers.init.Start();
    // Set up Pulsar client and producer.
    PulsarContext pulsar;
    BOLSON_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &pulsar));

    StreamThreads threads;

    // Set up buffers
    std::vector<illex::RawJSONBuffer> buffers;
    std::vector<std::mutex> mutexes(num_buffers);
    for (int i = 0; i < num_buffers; i++) {
      illex::RawJSONBuffer b;
      auto *bp = static_cast<std::byte *>(malloc(ILLEX_TCP_BUFFER_SIZE));
      auto is = illex::RawJSONBuffer::Create(bp, ILLEX_TCP_BUFFER_SIZE, &b);
      if (!is.ok()) {
        return Status(Error::IllexError, "Could not create Illex buffer: " + is.msg());
      }
      buffers.push_back(b);
    }

    // Set up output queue.
    IpcQueue arrow_ipc_queue(16 * 1024 * 1024);

    // Set up futures for statistics delivered by threads.
    std::promise<std::vector<Stats>> conv_stats_promise;
    auto conv_stats_future = conv_stats_promise.get_future();
    std::promise<PublishStats> pub_stats_promise;
    auto pub_stats_future = pub_stats_promise.get_future();

    // Spawn a conversion thread. This has various possible implementations.
    // It takes JSON items and converts them into Arrow RecordBatches and serializes those
    // into Arrow IPC messages, and pushes them to the IPC queue.
    switch (opt.conversion) {
      case convert::Impl::CPU: {
        threads.converter_thread =
            std::make_unique<std::thread>(convert::ConvertFromBuffersWithCPU,
                                          ToPointers(&buffers),
                                          ToPointers(&mutexes),
                                          &arrow_ipc_queue,
                                          &threads.shutdown,
                                          opt.num_threads,
                                          opt.parse,
                                          opt.read,
                                          opt.batch_threshold,
                                          &lat_tracker,
                                          std::move(conv_stats_promise));
        break;
        case convert::Impl::OPAE_BATTERY:
          return Status(Error::GenericError,
                        "Not yet implemented.");
      }
    }

    // Spawn the publish thread, which pulls from the IPC queue and publishes the IPC
    // messages in Pulsar.
    threads.publish_thread = std::make_unique<std::thread>(PublishThread,
                                                           std::move(pulsar),
                                                           &arrow_ipc_queue,
                                                           &threads.shutdown,
                                                           &threads.publish_count,
                                                           &lat_tracker,
                                                           std::move(pub_stats_promise));

    // Set up the client that receives incoming JSONs.
    // We must shut down the already spawned threads in case the client returns some
    // errors.
    illex::DirectBufferClient client;
    SHUTDOWN_ON_FAILURE(illex::DirectBufferClient::Create(std::get<illex::RawProtocol>(opt.protocol),
                                                          opt.hostname,
                                                          opt.seq,
                                                          ToPointers(&buffers),
                                                          ToPointers(&mutexes),
                                                          &client));

    timers.init.Stop();

    // Receive JSONs (blocking) until the server closes the connection.
    // Concurrently, the conversion and publish thread will do their job.
    timers.tcp.Start();
    SHUTDOWN_ON_FAILURE(client.ReceiveJSONs(&lat_tracker));
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
        return Status(Error::GenericError, "Not implemented.");
      } else {
        LogStreamStats(timers, client, conv_stats, pub_stats);
        opt.pulsar.Log();
        spdlog::info("Implementation    : {}", ToString(opt.conversion));
        spdlog::info("Conversion threads: {}", opt.num_threads);
        // for clarity, but this may be more in the future:
        spdlog::info("Publish threads   : {}", 1);
        BOLSON_ROE(LogLatencyCSV(opt.latency.file, lat_tracker));
      }
    }
  } else {
    return Status(Error::GenericError, "Not implemented.");
  }

  return Status::OK();
}

}  // namespace bolson
