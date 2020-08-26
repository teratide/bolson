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

#include "jsongen/zmq_client.h"

namespace jsongen {

auto ZMQClient::Create(ZMQProtocol protocol, std::string host, ZMQClient *out) -> Status {
  out->host = std::move(host);
  out->protocol = std::move(protocol);

  try {
    const std::string endpoint = "tcp://" + out->host + ":" + std::to_string(out->protocol.port);
    // Initialize the ZeroMQ context.
    out->context = std::make_shared<zmqpp::context>();
    // Create a pull socket.
    zmqpp::socket_type type = zmqpp::socket_type::pull;
    out->socket = std::make_shared<zmqpp::socket>(*out->context, type);
    // Open the connection
    spdlog::info("Connecting to {}", endpoint);
    out->socket->connect(endpoint);
  } catch (const std::exception &e) {
    return Status(Error::ZMQError, e.what());
  }

  return Status::OK();
}

auto ZMQClient::ReceiveJSONs(ConsumptionQueue *queue) -> Status {
  // Loop while the socket is still valid.
  while (*socket) {
    zmqpp::message message;
    socket->receive(message);
    if (message.get(0) == protocol.eos_marker) {
      break;
    } else {
      spdlog::info("Received: {}", message.get(0));
      //queue.enqueue(message.get(0));
    }
  }

  return Status::OK();
}

auto ZMQClient::Close() -> Status {
  spdlog::info("Stream client shutting down...");
  try {
    this->socket->close();
  } catch (const std::exception &e) {
    return Status(Error::ZMQError, e.what());
  }
  return Status::OK();
}

}
