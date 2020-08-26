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

#include <jsongen/raw_client.h>
#include <jsongen/zmq_client.h>

#include "flitter/stream.h"

namespace flitter {

auto ProduceFromStream(const StreamOptions &opt) -> int {
  if (std::holds_alternative<jsongen::ZMQProtocol>(opt.protocol)) {
    jsongen::ConsumptionQueue output;
    jsongen::ZMQClient client;
    jsongen::ZMQClient::Create(std::get<jsongen::ZMQProtocol>(opt.protocol), "localhost", &client);
    client.ReceiveJSONs(&output);
    client.Close();
  } else {
    jsongen::ConsumptionQueue output;
    jsongen::RawClient client;
    auto status = jsongen::RawClient::Create(std::get<jsongen::RawProtocol>(opt.protocol), "localhost", &client);
    if (!status.ok()) {
      spdlog::error("Could not create raw client {}.", status.msg());
    }
    status = client.ReceiveJSONs(&output);
    if (!status.ok()) {
      spdlog::error("Could not create raw client {}.", status.msg());
    }
    status = client.Close();
    if (!status.ok()) {
      spdlog::error("Could not create raw client {}.", status.msg());
    }
  }

  return 0;
}

}  // namespace flitter
