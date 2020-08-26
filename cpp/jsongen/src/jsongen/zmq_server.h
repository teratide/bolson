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

#include <zmqpp/zmqpp.hpp>
#include <arrow/api.h>
#include <jsongen/zmq_protocol.h>

#include "jsongen/status.h"
#include "jsongen/document.h"
#include "jsongen/producer.h"
#include "jsongen/stream.h"

namespace jsongen {

/**
 * \brief A streaming server based on ZeroMQ's push-pull configuration.
 */
class ZMQServer {
 public:
  /**
   * \brief Create a new ZeroMQ push server to stream JSONs to some pull socket.
   *
   * The ZMQServer constructor cannot be used because setting up the server can fail.
   *
   * \param[in] protocol_options The protocol options.
   * \param[out] out The ZMQServer to populate.
   * \return Status::OK() if successful, some error status otherwise.
   */
  static auto Create(ZMQProtocol protocol_options, ZMQServer *out) -> Status;

  /**
   * \brief Send JSONs using this ZMQServer.
   * \param[in] options Options for the JSON production facilities.
   * \param[out] stats Server statistics.
   * \return Status::OK() if successful, some error status otherwise.
   */
  auto SendJSONs(const ProductionOptions &options, StreamStatistics *stats) -> Status;

  /**
   * \brief Close the ZMQServer.
   * \return Status::OK() if successful, some error status otherwise.
   */
  auto Close() -> Status;

 private:
  ZMQProtocol protocol;
  std::shared_ptr<zmqpp::context> context;
  std::shared_ptr<zmqpp::socket> socket;
};

/**
 * \brief Use a ZMQServer to stream the specified JSONs out.
 * \param protocol_options Protocol options for the server.
 * \param production_options Options for JSON production.
 * \return Status::OK if successful, some error otherwise.
 */
auto RunZMQServer(const ZMQProtocol &protocol_options, const ProductionOptions &production_options) -> Status;

}
