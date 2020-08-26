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
#include <cstdlib>

#include <kissnet.hpp>

namespace jsongen {

using RawSocket = kissnet::socket<kissnet::protocol::tcp>;

/// TCP receive buffer size.
constexpr const size_t RAW_BUFFER_SIZE = 5 * 1024 * 1024;

/// TCP default port.
constexpr uint16_t RAW_PORT = 10197;

/// Protocol options for the raw streaming client/server
struct RawProtocol {
  /// \brief Constructor
  explicit RawProtocol(uint16_t port = RAW_PORT) : port(port) {}
  /// Port to use for the TCP connection.
  uint16_t port;
  /// Buffer size.
  size_t buffer_size = RAW_BUFFER_SIZE;
};

}