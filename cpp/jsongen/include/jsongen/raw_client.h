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

#include <string>
#include <utility>

#include <kissnet.hpp>

#include "jsongen/log.h"
#include "jsongen/status.h"
#include "jsongen/consumer.h"
#include "jsongen/raw_protocol.h"

namespace jsongen {

/// A streaming client using the Raw protocol.
struct RawClient {
 public:
  /**
   * \brief Construct a new raw client.
   * \param protocol The protocol options.
   * \param host The hostname to connect to.
   * \param out The raw client that will be populated by this function.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Create(RawProtocol protocol, std::string host, RawClient *out) -> Status;

  /**
   * \brief Receive JSONs on this raw stream client and put them in queue.
   * \param queue The queue to put the JSONs in.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto ReceiveJSONs(ConsumptionQueue *queue) -> Status;

  /**
   * \brief Close this raw client.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Close() -> Status;

 private:
  std::string host = "localhost";
  jsongen::RawProtocol protocol;
  std::shared_ptr<RawSocket> client;

};

}  // namespace flitter
