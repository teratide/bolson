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

#include "bolson/latency.h"
#include "bolson/status.h"

#pragma once

namespace bolson::publish {

/// Statistics about publishing
struct Metrics {
  /// Number of RecordBatch rows published.
  size_t rows = 0;
  /// Number of IPC messages published.
  size_t ipc = 0;
  /// Time spent on publishing message.
  double publish_time = 0.;
  /// Time spent in publish thread.
  double thread_time = 0.;
  /// Status of the publishing thread.
  Status status = Status::OK();
  /// Latency measurements of all batches published.
  LatencyMeasurements latencies;

  auto operator+=(const Metrics& r) -> Metrics&;
};

}  // namespace bolson::publish