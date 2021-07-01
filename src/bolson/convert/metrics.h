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

#pragma once

namespace bolson::convert {

/**
 * \brief Converter metrics.
 */
struct Metrics {
  /// Number of threads used.
  size_t num_threads = 0;
  /// Number of converted JSONs.
  size_t num_jsons_converted = 0;
  /// Number of converted JSON bytes.
  size_t num_json_bytes_converted = 0;
  /// Number of buffers converted.
  size_t num_buffers_converted = 0;
  /// Number of IPC messages.
  size_t num_ipc = 0;
  /// Number of bytes in the IPC messages.
  size_t ipc_bytes = 0;
  /// Total time of specific operations in the pipeline.
  struct {
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
  } t;
  /// Status about the conversion.
  Status status = Status::OK();

  auto operator+=(const Metrics& r) -> Metrics&;

  /**\
   * \brief Return a CSV string with the metrics.
   */
  [[nodiscard]] std::string ToCSV() const;
};

/**
 * \brief Print some metrics about conversion.
 * \param metrics The metrics to print.
 * \param t Prefix for indenting.
 */
void LogConvertMetrics(const Metrics& metrics, const std::string& t = "");

/**
 * \brief Save convert metrics to a file.
 */
Status SaveConvertMetrics(const std::vector<Metrics>& metrics, const std::string& file);

}  // namespace bolson::convert
