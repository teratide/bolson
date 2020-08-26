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
#include <utility>
#include <zmqpp/zmqpp.hpp>
#include <rapidjson/prettywriter.h>
#include <concurrentqueue.h>
#include <putong/timer.h>

#include "jsongen/log.h"
#include "jsongen/zmq_server.h"
#include "jsongen/document.h"
#include "jsongen/arrow.h"
#include "jsongen/stream.h"
#include "jsongen/producer.h"

namespace jsongen {

auto ZMQServer::Create(ZMQProtocol protocol_options, ZMQServer *out) -> Status {
  out->protocol = std::move(protocol_options);

  const std::string endpoint = "tcp://*:" + std::to_string(out->protocol.port);

  try {
    // Initialize the ZMQ context
    out->context = std::make_shared<zmqpp::context>();
    // Generate a push socket
    zmqpp::socket_type type = zmqpp::socket_type::push;
    out->socket = std::make_shared<zmqpp::socket>(*out->context, type);
    // Bind to the socket
    spdlog::info("Binding to {}", endpoint);
    out->socket->bind(endpoint);
  } catch (const std::exception &e) {
    return Status(Error::ZMQError, e.what());
  }

  return Status::OK();
}

auto ZMQServer::SendJSONs(const ProductionOptions &options, StreamStatistics *stats) -> Status {
  // Check for some potential misuse.
  assert(stats != nullptr);
  if ((this->context == nullptr) || (this->socket == nullptr)) {
    return Status(Error::GenericError, "ZMQServer uninitialized. Use ZMQServer::Create().");
  }

  StreamStatistics result;
  putong::Timer t;

  // Create a concurrent queue for the JSON production threads.
  ProductionQueue production_queue;

  // Spawn production hive thread.
  std::promise<ProductionStats> production_stats;
  auto producer_stats_future = production_stats.get_future();
  auto producer = std::thread(ProductionHive, options, &production_queue, std::move(production_stats));

  // Attempt to pull all produced messages from the production queue and send them over the ZMQ socket.
  for (size_t m = 0; m < options.num_jsons; m++) {
    // Get the message
    std::string message_str;
    while (!production_queue.try_dequeue(message_str)) {
      SPDLOG_DEBUG("Nothing in queue... {}");
#ifndef NDEBUG
      std::this_thread::sleep_for(std::chrono::seconds(1));
#endif
    }
    SPDLOG_DEBUG("Popped string {} from production queue.", message_str);

    result.num_bytes += message_str.size();
    if (this->socket->send(message_str)) {
      result.num_messages++;
      // Start the time when this is the first message.
      if (result.num_messages == 1) {
        t.Start();
      }
    } else {
      return Status(Error::ZMQError, "Could not push ZMQ message.");
    }
  }

  t.Stop();
  producer.join();
  result.time = t.seconds();
  result.producer = producer_stats_future.get();

  return Status::OK();
}

auto ZMQServer::Close() -> Status {
  spdlog::info("Stream server shutting down...");
  // Send the end-of-stream marker.
  try {
    zmqpp::message stop;
    stop << this->protocol.eos_marker;
    this->socket->send(stop);
    this->socket->close();
  } catch (const std::exception &e) {
    return Status(Error::ZMQError, e.what());
  }

  return Status::OK();
}

static void LogSendStats(const StreamStatistics &result) {
  spdlog::info("Streamed {} messages in {:.4f} seconds.", result.num_messages, result.time);
  spdlog::info("  {:.1f} messages/second (avg).", result.num_messages / result.time);
  spdlog::info("  {:.2f} gigabits/second (avg).",
               static_cast<double>(result.num_bytes * 8) / result.time * 1E-9);
}

auto RunZMQServer(const ZMQProtocol &protocol_options, const ProductionOptions &production_options) -> Status {
  spdlog::info("Starting ZMQ push server.");
  ZMQServer server;
  RETURN_ON_ERROR(ZMQServer::Create(protocol_options, &server));

  spdlog::info("Streaming {} JSONs.", production_options.num_jsons);
  StreamStatistics stats;
  RETURN_ON_ERROR(server.SendJSONs(production_options, &stats));
  LogSendStats(stats);

  spdlog::info("ZMQ server shutting down.");
  RETURN_ON_ERROR(server.Close());

  return Status::OK();
}

}

