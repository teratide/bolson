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

#include <memory>
#include <iostream>
#include <zmqpp/zmqpp.hpp>
#include <flitter/log.h>

#include "./stream.h"
#include "./hive.h"

namespace flitter {

struct ClientConnection {
  std::shared_ptr<zmqpp::context> context;
  std::shared_ptr<zmqpp::socket> socket;
};

auto StartClient(const StreamOptions &opts) -> ClientConnection {
  // TODO(johanpel): error handling
  ClientConnection result;
  const std::string endpoint = "tcp://" + opts.host + ":" + std::to_string(opts.protocol.port);
  // Initialize the 0MQ context.
  result.context = std::make_shared<zmqpp::context>();
  // Generate a pull socket.
  zmqpp::socket_type type = zmqpp::socket_type::pull;
  result.socket = std::make_shared<zmqpp::socket>(*result.context, type);
  // Open the connection
  spdlog::info("Connecting to {}", endpoint);
  result.socket->connect(endpoint);
  return result;
}

auto StreamClient(const StreamOptions &opts) -> int {
  spdlog::info("Starting stream client.");
  auto c = StartClient(opts);

  Hive h;
  h.Start();

  spdlog::info("Receiving messages.");
  // Try to pull messages as long as we don't receive the end-of-stream marker.
  while (true) {
    zmqpp::message message;
    c.socket->receive(message);
    if (message.get(0) == opts.protocol.eos_marker) {
      spdlog::info("End of stream.");
      break;
    } else {
      SPDLOG_DEBUG("Received: {}", message.get(0));
      h.Push(message.get(0));
    }
  }

  spdlog::info("Stream client shutting down.");
  h.Stop();

  return 0;
}

}  // namespace flitter
