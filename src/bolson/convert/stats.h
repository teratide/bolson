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

#include <putong/timer.h>

#include "bolson/status.h"
#include "bolson/log.h"

#pragma once

namespace bolson::convert {

/**
 * \brief Various statistics related to durations of pipeline stages.
 */
struct TimeStats {
  /// Total time spent on parsing JSONs to Arrow RecordBatch.
  double parse = 0.0;
  /// Total time spent on resizing parsed batches to fit in a message.
  double resize = 0.0;
  /// Total time spent on serializing the RecordBatch.
  double serialize = 0.0;
  /// Total time spent on enqueueing serialized RecordBatches
  double enqueue = 0.0;
  /// Total time spent in the conversion thread.
  double thread = 0.0;
};

/**
 * \brief Converter statistics.
 */
struct Stats {
  /// Number of converted JSONs.
  size_t num_jsons = 0;
  /// Number of converted JSON bytes.
  size_t json_bytes = 0;
  /// Number of buffers parsed.
  size_t num_parsed = 0;
  /// Number of IPC messages.
  size_t num_ipc = 0;
  /// Number of bytes in the IPC messages.
  size_t ipc_bytes = 0;
  /// Total time of specific operations in the pipeline.
  TimeStats t;
  /// Status about the conversion.
  Status status = Status::OK();

  auto operator+=(const Stats &r) -> Stats &;
};

/**
 * \brief Print some stats about conversion.
 * \param stats The stats to print.
 * \param num_threads The number of threads used.
 * \param t Prefix for indenting.
 */
void LogConvertStats(const Stats &stats, size_t num_threads, const std::string &t = "");

/// \brief Aggregate statistics from multiple threads.
auto AggrStats(const std::vector<Stats> &conv_stats) -> Stats;

}
