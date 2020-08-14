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

#include <string>
#include <iostream>
#include <zmqpp/zmqpp.hpp>
#include <flitter/log.h>
#include <rapidjson/prettywriter.h>

#include "./stream.h"
#include "./document.h"
#include "./arrow.h"

namespace jsongen {

auto StreamServer(const StreamOptions &opt) -> int {
  spdlog::info("Starting stream server.");

  const std::string endpoint = "tcp://*:" + std::to_string(opt.protocol.port);

  // Initialize the 0MQ context
  zmqpp::context context;
  // Generate a push socket
  zmqpp::socket_type type = zmqpp::socket_type::push;
  zmqpp::socket socket(context, type);
  // Bind to the socket
  spdlog::info("Binding to {}", endpoint);
  socket.bind(endpoint);

  // Receive the message
  spdlog::info("Producing {} messages.", opt.num_messages);

  for (size_t m = 0; m < opt.num_messages; m++) {
    // Generate a message with tweets in JSON format.
    auto gen = FromSchema(*opt.schema, opt.gen);
    auto json = gen.Get();

    rapidjson::StringBuffer buffer;
    if (opt.pretty) {
      rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
      writer.SetFormatOptions(rj::PrettyFormatOptions::kFormatSingleLineArray);
      json.Accept(writer);
    } else {
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      json.Accept(writer);
    }

    // Send the message.
    zmqpp::message message;
    message << buffer.GetString();
    socket.send(message);
  }

  // Send the end-of-stream marker.
  zmqpp::message stop;
  stop << opt.protocol.eos_marker;
  socket.send(stop);

  spdlog::info("Stream server shutting down.");

  return 0;
}

}  // namespace jsongen
