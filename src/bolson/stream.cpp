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

#include "bolson/stream.h"

#include <putong/timer.h>

#include <memory>
#include <thread>
#include <vector>

#include "bolson/latency.h"
#include "bolson/publish/publisher.h"
#include "bolson/status.h"
#include "bolson/utils.h"

namespace bolson {

/// Structure to hold timers.
struct StreamTimers {
  putong::Timer<> tcp;
  putong::Timer<> init;
};

/// Structure to hold threads and atomics
struct StreamThreads {
  std::atomic<bool> shutdown = false;
  std::atomic<size_t> publish_count = 0;

  /// Shut down the threads.
  void Shutdown(const std::shared_ptr<convert::Converter>& converter,
                const std::shared_ptr<publish::ConcurrentPublisher>& publisher) {
    shutdown.store(true);
    converter->Finish();
    publisher->Finish();
  }
};

/// \brief Log the statistics.
static auto LogStreamMetrics(const StreamOptions& opt, const StreamTimers& timers,
                             const illex::BufferingClient& client,
                             const convert::Converter& converter,
                             const publish::ConcurrentPublisher& publisher) -> Status {
  // Report some statistics.
  if (opt.statistics) {
    if (opt.succinct) {
      return Status(Error::GenericError, "Not implemented.");
    } else {
      auto c = Aggregate(converter.metrics());
      auto p = Aggregate(publisher.metrics());

      spdlog::info("Initialization");
      spdlog::info("  Time                    : {}", timers.init.seconds());
      spdlog::info("  Conversion impl.        : {}", ToString(opt.converter.parser.impl));
      spdlog::info("  Conversion threads      : {}", opt.converter.num_threads);
      spdlog::info("  TCP clients             : {}", 1);
      opt.pulsar.Log();

      // TCP client statistics.
      auto tcp_MiB = static_cast<double>(client.bytes_received()) / (1024.0 * 1024.0);
      auto tcp_MB = static_cast<double>(client.bytes_received()) / 1E6;
      auto tcp_MJs = client.jsons_received() / 1E6;

      spdlog::info("TCP client:");
      spdlog::info("  JSONs received          : {}", client.jsons_received());
      spdlog::info("  Bytes received          : {} MiB", tcp_MiB);
      spdlog::info("  Time                    : {} s", timers.tcp.seconds());
      spdlog::info("  Throughput              : {} MJ/s", tcp_MJs / timers.tcp.seconds());
      spdlog::info("  Throughput              : {} MB/s", tcp_MB / timers.tcp.seconds());

      spdlog::info("JSONs to IPC conversion:");
      LogConvertMetrics(c, converter.metrics().size(), "  ");

      // Pulsar producer / publishing statistics
      auto pub_MJs = p.rows / 1E6;

      spdlog::info("Publish stats:");
      spdlog::info("  JSONs published         : {}", p.rows);
      spdlog::info("  IPC messages            : {}", p.ipc);
      spdlog::info("  Time                    : {} s", p.publish_time);
      spdlog::info("    in thread             : {} s", p.thread_time);
      spdlog::info("  Throughput              : {} MJ/s.", pub_MJs / p.publish_time);

      if (!opt.latency_file.empty()) {
        BOLSON_ROE(SaveLatencyMetrics(p.latencies, opt.latency_file));
      }
    }
  }
  return Status::OK();
}

// Macro to shut down threads in ProduceFromStream whenever Illex client returns some
// error.
#define SHUTDOWN_ON_FAILURE(status)                       \
  {                                                       \
    auto __status = status;                               \
    if (!__status.ok()) {                                 \
      threads.Shutdown(converter, publisher);             \
      return Status(Error::GenericError, __status.msg()); \
    }                                                     \
  }                                                       \
  void()

auto ProduceFromStream(const StreamOptions& opt) -> Status {
  StreamThreads threads;  // Management of all threads.
  StreamTimers timers;    // Performance metric timers.
  publish::IpcQueue ipc_queue(
      BOLSON_PUBLISH_IPC_QUEUE_SIZE);  // IPC queue to Pulsar producer.

  illex::BufferingClient client;                            // TCP client.
  std::shared_ptr<convert::Converter> converter;            // Converters.
  std::shared_ptr<publish::ConcurrentPublisher> publisher;  // Pulsar producers.

  timers.init.Start();
  spdlog::info("Initializing Pulsar client and producer...");
  BOLSON_ROE(publish::ConcurrentPublisher::Make(opt.pulsar, &ipc_queue,
                                                &threads.publish_count, &publisher));

  spdlog::info("Initializing converter(s)...");
  BOLSON_ROE(convert::Converter::Make(opt.converter, &ipc_queue, &converter));

  spdlog::info("Initializing stream source client...");
  BILLEX_ROE(illex::BufferingClient::Create(
      opt.client, converter->parser_context()->mutable_buffers(),
      converter->parser_context()->mutexes(), &client));
  timers.init.Stop();

  spdlog::info("Starting JSON-to-Arrow converter thread(s)...");
  converter->Start(&threads.shutdown);

  spdlog::info("Starting Pulsar publish thread(s)...");
  publisher->Start(&threads.shutdown);

  spdlog::info("Receiving, converting, and publishing JSONs...");
  // Receive JSONs (blocking) until the server closes the connection.
  // Concurrently, the conversion and publish thread will do their job.
  timers.tcp.Start();
  SHUTDOWN_ON_FAILURE(client.ReceiveJSONs());
  timers.tcp.Stop();
  SHUTDOWN_ON_FAILURE(client.Close());

  spdlog::info("Source server disconnected, emptying buffers...");

  // Once the server disconnects, we can work towards finishing this function.
  // Wait until all JSONs have been published, or if either the publish or converter
  // thread have asserted the shutdown signal, the latter indicating some error.
  while ((client.jsons_received() != threads.publish_count.load()) &&
         !threads.shutdown.load()) {
    // Sleep this thread for a bit.
    std::this_thread::sleep_for(std::chrono::milliseconds(BOLSON_QUEUE_WAIT_US));
#ifndef NDEBUG
    // Sleep a bit longer in debug.
    std::this_thread::sleep_for(std::chrono::milliseconds(100 * BOLSON_QUEUE_WAIT_US));
    SPDLOG_DEBUG("Received: {}, Published: {}", client.jsons_received(),
                 threads.publish_count.load());
#endif
  }

  // We can now shut down all threads and collect futures.
  spdlog::info("Done, shutting down...");
  threads.Shutdown(converter, publisher);
  spdlog::info("----------------------------------------------------------------");

  BOLSON_ROE(LogStreamMetrics(opt, timers, client, *converter, *publisher));

  return Status::OK();
}

}  // namespace bolson
