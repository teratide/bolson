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

#include <chrono>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <arrow/api.h>
#include <rapidjson/document.h>

namespace flitter {

/**
 * @brief Returns the total size in memory of all (nested) buffers backing Arrow ArrayData.
 *
 * Returns int64_t because Arrow.
 * @param array_data The ArrayData to analyze.
 * @returns The total size of all (nested) buffer contents in bytes.
 */
auto GetArrayDataSize(const std::shared_ptr<arrow::ArrayData> &array_data) -> int64_t;

/**
 * @brief Return the total size in memory of the data in an Arrow RecordBatch. Does not include buffer padding.
 * @param batch The RecordBatch to analyze.
 * @return The total size in bytes.
 */
auto GetBatchSize(const std::shared_ptr<arrow::RecordBatch> &batch) -> int64_t;

/// @brief Write an Arrow RecordBatch into a file as an Arrow IPC message.
auto WriteIPCMessageBuffer(const std::shared_ptr<arrow::RecordBatch> &batch) -> arrow::Result<std::shared_ptr<arrow::Buffer>>;

/// @brief Report some gigabytes per second.
void ReportGBps(const std::string &text, size_t bytes, double s, bool succinct = false);

/**
 * @brief Read num_bytes from a file and buffer it in memory. Appends a C-style string terminator to please rapidjson.
 * @param file_name The file to load.
 * @param num_bytes The number of bytes to read into the buffer.
 * @return The buffer, will be size num_bytes + 1 to accommodate the terminator character.
 */
auto LoadFile(const std::string &file_name, size_t num_bytes) -> std::vector<char>;

/**
 * @brief Report a rapidjson parsing error on stderr.
 * @param doc The document that has a presumed error.
 * @param file_buffer The buffer from which the document was attempted to be parsed.
 */
void ReportParserError(const rapidjson::Document &doc, const std::vector<char> &file_buffer);

/// @brief A timer using the C++11 steady monotonic clock.
struct Timer {
  using steady_clock = std::chrono::steady_clock;
  using nanoseconds = std::chrono::nanoseconds;
  using time_point = std::chrono::time_point<steady_clock, nanoseconds>;
  using duration = std::chrono::duration<double>;

  Timer() = default;

  /// @brief Timer start point.
  time_point start_{};
  /// @brief Timer stop point.
  time_point stop_{};

  /// @brief Start the timer.
  inline void Start() { start_ = std::chrono::steady_clock::now(); }

  /// @brief Stop the timer.
  inline void Stop() { stop_ = std::chrono::steady_clock::now(); }

  /// @brief Retrieve the interval in seconds.
  inline double seconds() {
    duration diff = stop_ - start_;
    return diff.count();
  }

  /// @brief Return the interval in seconds as a formatted string.
  inline std::string str(int width = 14) {
    std::stringstream ss;
    ss << std::setprecision(width - 5) << std::setw(width) << std::fixed << seconds();
    return ss.str();
  }

  /// @brief Print the interval on some output stream
  inline void report(std::ostream &os = std::cout, bool last = false, int width = 15) {
    os << std::setw(width) << ((last ? " " : "") + str() + (last ? "\n" : ",")) << std::flush;
  }
};

}  // namespace flitter
