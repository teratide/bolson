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

#include <illex/client_buffering.h>
#include <illex/latency.h>
#include <putong/timer.h>

#include <vector>

#include "bolson/status.h"

// Wait time for queues.
#define BOLSON_QUEUE_WAIT_US 1

namespace bolson {

struct TimePoints {
  // Indices for points in time.
  static constexpr size_t received = 0;              ///< TCP buffer was filled.
  static constexpr size_t parsed = received + 1;     ///< JSON buffer was parse.
  static constexpr size_t resized = parsed + 1;      ///< Batch was resized.
  static constexpr size_t serialized = resized + 1;  ///< Batch was serialized.
  static constexpr size_t popped = serialized + 1;   ///< Batch popped from IPC queue.
  static constexpr size_t published = popped + 1;    ///< Pulsar send returned

  // Total number of points.
  static constexpr size_t num_points = published + 1;

  inline static auto point_name(size_t i) -> std::string {
    static std::vector<std::string> result(
        {"Receive", "Parse", "Resize", "Serialize", "Pop", "Publish"});
    assert(i < result.size());
    return result[i];
  }

  template <typename T>
  [[nodiscard]] inline auto GetDiff(size_t index) const -> size_t {
    assert(index > 0);
    assert(index < num_points);
    return std::chrono::duration_cast<T>(time[index] - time[index - 1]).count();
  }

  auto operator[](size_t i) -> illex::TimePoint& { return time[i]; }
  auto operator[](size_t i) const -> const illex::TimePoint& { return time[i]; }

  illex::TimePoint time[published + 1];
};

struct LatencyMeasurement {
  illex::SeqRange seq{};
  TimePoints time;
};

using LatencyMeasurements = std::vector<LatencyMeasurement>;

auto SaveLatencyMetrics(const LatencyMeasurements& measurements, const std::string& file,
                        size_t from = TimePoints::received,
                        size_t to = TimePoints::published, bool with_seq = true)
    -> Status;

}  // namespace bolson