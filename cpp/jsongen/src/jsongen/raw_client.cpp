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

#include <chrono>
#include <thread>
#include <memory>
#include <iostream>

#include "jsongen/raw_client.h"

namespace jsongen {

using TCPBuffer = std::array<std::byte, jsongen::RAW_BUFFER_SIZE>;

auto RawClient::Create(RawProtocol protocol, std::string host, RawClient *out) -> Status {
  auto endpoint = out->host + ":" + std::to_string(out->protocol.port);
  out->host = std::move(host);
  out->protocol = protocol;
  out->client = std::make_shared<RawSocket>(kissnet::endpoint(endpoint));

  spdlog::info("Connecting to {}...", endpoint);
  out->client->connect();

  return Status::OK();
}

static void EnqueueAllJSONsInBuffer(std::string *json_buffer,
                                    TCPBuffer *tcp_buffer,
                                    size_t valid_bytes,
                                    ConsumptionQueue *queue) {
  // TODO(johanpel): implement mechanism to allow newlines within JSON objects,
  //   this only works for non-pretty printed JSONs now.
  auto *recv_chars = reinterpret_cast<char *>(tcp_buffer->data());

  // Scan the buffer for a newline.
  char *json_start = recv_chars;
  char *json_end = recv_chars + valid_bytes;
  size_t remaining = valid_bytes;
  do {
    json_end = std::strchr(json_start, '\n');
    if (json_end == nullptr) {
      // Append the remaining characters to the JSON buffer.
      json_buffer->append(json_start, remaining);
      // We appended everything up to the end of the buffer, so we can set remaining bytes to 0.
      remaining = 0;
    } else {
      // There is a newline. Only append the remaining characters to the json_msg.
      json_buffer->append(json_start, json_end - json_start);

      // Copy the JSON string into the consumption queue.
      SPDLOG_DEBUG("Queueing JSON: {}", *json_buffer);
      queue->enqueue(*json_buffer);

      // Clear the JSON string buffer.
      // The implementation of std::string is allowed to change its allocated buffer here, but there
      // are no implementations that actually do it, they retain the allocated buffer.
      // An implementation using std::vector<char> might be desired here just to make sure.
      json_buffer->clear();

      // Move the start to the character after the newline.
      json_start = json_end + 1;

      // Calculate the remaining number of bytes in the buffer.
      remaining = (recv_chars + valid_bytes) - json_start;
    }
    // Repeat until there are no remaining bytes.
  } while (remaining > 0);

  // Clear the buffer when finished.
  tcp_buffer->fill(std::byte(0x00));
}

auto RawClient::ReceiveJSONs(ConsumptionQueue *queue) -> Status {
  // Buffer for the JSON string.
  std::string json_string;
  // TCP receive buffer.
  TCPBuffer recv_buffer{};

  // Loop while the socket is still valid.
  while (client->is_valid()) {
    try {
      // Attempt to receive some bytes.
      auto recv_status = client->recv(recv_buffer);
      auto bytes_received = std::get<0>(recv_status);
      auto sock_status = std::get<1>(recv_status).get_value();
      // Perhaps the server disconnected because it's done sending JSONs.
      if (sock_status == kissnet::socket_status::cleanly_disconnected) {
        SPDLOG_DEBUG("Raw server has cleanly disconnected.");
        return Status::OK();
      } else if (sock_status != kissnet::socket_status::valid) {
        // Otherwise, if it's not valid, there is something wrong.
        return Status(Error::RawError, "Raw server error. Status: " + std::to_string(sock_status));
      }
      // We must now handle the received bytes in the TCP buffer.
      EnqueueAllJSONsInBuffer(&json_string, &recv_buffer, bytes_received, queue);
    } catch (const std::exception &e) {
      // But first we catch any exceptions.
      return Status(Error::RawError, e.what());
    }
  }

  return Status::OK();
}

auto RawClient::Close() -> Status {
  spdlog::info("Raw client shutting down...");
  client->close();
  return Status::OK();
}

}
