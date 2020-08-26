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
#include <future>
#include <string>
#include <rapidjson/prettywriter.h>
#include <concurrentqueue.h>
#include <putong/timer.h>
#include <kissnet.hpp>

#include "jsongen/log.h"
#include "jsongen/document.h"
#include "jsongen/arrow.h"
#include "jsongen/raw_server.h"

namespace jsongen {

namespace kn = kissnet;

auto RawServer::Create(RawProtocol protocol_options, RawServer *out) -> Status {
  out->protocol = protocol_options;
  out->server = std::make_shared<RawSocket>(kn::endpoint("0.0.0.0:" + std::to_string(out->protocol.port)));
  try {
    out->server->bind();
  } catch (const std::runtime_error &e) {
    return Status(Error::RawError, e.what());
  }
  out->server->listen();
  spdlog::info("Listening on port {}", out->protocol.port);
  return Status::OK();
}

auto RawServer::SendJSONs(const ProductionOptions &options, StreamStatistics *stats) -> Status {
  // Check for some potential misuse.
  assert(stats != nullptr);
  if (this->server == nullptr) {
    return Status(Error::GenericError, "RawServer uninitialized. Use RawServer::Create().");
  }

  StreamStatistics result;
  putong::Timer t;

  // Create a concurrent queue for the JSON production threads.
  ProductionQueue production_queue;
  // Spawn production hive thread.
  std::promise<ProductionStats> production_stats;
  auto producer_stats_future = production_stats.get_future();
  auto producer = std::thread(ProductionHive, options, &production_queue, std::move(production_stats));

  // Accept a client.
  SPDLOG_DEBUG("Waiting for client to connect.");
  auto client = server->accept();

  // Start a timer.
  t.Start();

  // Attempt to pull all produced messages from the production queue and send them over the ZMQ socket.
  for (size_t m = 0; m < options.num_jsons; m++) {
    // Get the message
    std::string message_str;
    while (!production_queue.try_dequeue(message_str)) {
#ifndef NDEBUG
      SPDLOG_DEBUG("Nothing in queue... {}");
      std::this_thread::sleep_for(std::chrono::seconds(1));
#endif
    }
    SPDLOG_DEBUG("Popped string {} from production queue.", message_str);

    // Attempt to send the message.
    client.send(reinterpret_cast<std::byte *>(message_str.data()), message_str.length());

    result.num_messages++;
  }

  // Stop the timer.
  t.Stop();
  result.time = t.seconds();

  // Wait for the producer thread to stop, and obtain the statistics.
  producer.join();
  result.producer = producer_stats_future.get();

  return Status::OK();
}

auto RawServer::Close() -> Status {
  spdlog::info("Raw server shutting down...");
  try {
    server->close();
  } catch (const std::exception &e) {
    return Status(Error::RawError, e.what());
  }

  return Status::OK();
}

static void LogSendStats(const StreamStatistics &result) {
  spdlog::info("Streamed {} messages in {:.4f} seconds.", result.num_messages, result.time);
  spdlog::info("  {:.1f} messages/second (avg).", result.num_messages / result.time);
  spdlog::info("  {:.2f} gigabits/second (avg).",
               static_cast<double>(result.num_bytes * 8) / result.time * 1E-9);
}

auto RunRawServer(const RawProtocol &protocol_options, const ProductionOptions &production_options) -> Status {
  spdlog::info("Starting Raw server.");
  RawServer server;
  RETURN_ON_ERROR(RawServer::Create(protocol_options, &server));

  spdlog::info("Streaming {} messages.", production_options.num_jsons);
  StreamStatistics stats;
  RETURN_ON_ERROR(server.SendJSONs(production_options, &stats));
  LogSendStats(stats);

  spdlog::info("Raw server shutting down.");
  RETURN_ON_ERROR(server.Close());

  return Status::OK();
}

}

