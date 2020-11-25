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

#include "bolson/status.h"

namespace bolson::convert {

/// Statistics from the conversion drone.
struct Stats {
  /// Number of converted JSONs.
  size_t num_jsons = 0;
  /// Number of IPC messages.
  size_t num_ipc = 0;
  /// Number of bytes in the IPC messages.
  size_t total_ipc_bytes = 0;
  /// Total time spent on conversion only.
  double convert_time = 0.0;
  /// Total time spent on IPC construction only.
  double ipc_construct_time = 0.0;
  /// Total time spent in this thread.
  double thread_time = 0.0;
  /// Status about the conversion.
  Status status = Status::OK();
};

enum class Impl {
  CPU,
  FPGA
};

}