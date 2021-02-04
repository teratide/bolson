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

#include "bolson/status.h"

namespace bolson {

auto Aggregate(const MultiThreadStatus& status, const std::string& prefix) -> Status {
  bool produce_error = false;
  std::stringstream msg;
  for (size_t t = 0; t < status.size(); t++) {
    if (!status[t].ok()) {
      msg << prefix;
      msg << "thread:" << t << ", error: " << ToString(status[t].err())
          << " msg:" << status[t].msg() << '\n';
      produce_error = true;
    }
  }
  if (produce_error) {
    return Status(Error::GenericError, msg.str());
  }
  return Status::OK();
}

auto ToString(Error e) -> std::string {
  switch (e) {
    case Error::GenericError:
      return "GenericError";
    case Error::CLIError:
      return "CLIError";
    case Error::PulsarError:
      return "PulsarError";
    case Error::IllexError:
      return "IllexError";
    case Error::ArrowError:
      return "ArrowError";
    case Error::IOError:
      return "IOError";
    case Error::OpaeError:
      return "OpaeError";
  }
  return "Error enum class corrupted.";
}

}  // namespace bolson