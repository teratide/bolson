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

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <variant>

namespace jsongen {

/// Default port number, derived from the alphabet index (arrays start at one) of JSG
constexpr uint16_t ZMQ_PORT = 10197;
/// Default end-of-stream marker for ZMQ.
constexpr const char *ZMQ_EOS = "stahp";

/// Protocol options for the ZMQ streaming client/server
struct ZMQProtocol {
  /// \brief Constructor
  explicit ZMQProtocol(uint16_t port = ZMQ_PORT, std::string eos_marker = ZMQ_EOS)
      : port(port), eos_marker(std::move(eos_marker)) {}
  /// Port to use for the TCP connection.
  uint16_t port;
  /// End of stream marker.
  std::string eos_marker;
};

}
