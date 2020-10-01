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

#include <utility>
#include <variant>

#include <illex/protocol.h>

#include "flitter/pulsar.h"

namespace flitter {

struct StreamOptions {
  /// Enable statistics.
  bool statistics = true;
  /// The hostname of the stream server.
  std::string hostname = "localhost";
  /// The protocol to use.
  illex::StreamProtocol protocol;
  /// The Pulsar options.
  PulsarOptions pulsar;
  /// Number of conversion drone threads to spawn.
  size_t num_conversion_drones = 1;
};

/**
 * \brief Produce Pulsar messages from an incoming stream.
 * \param opt The properties of the stream.
 * \return Status::OK() if successful, error otherwise.
 */
auto ProduceFromStream(const StreamOptions &opt) -> Status;

}  // namespace flitter
