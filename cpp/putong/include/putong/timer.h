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

#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>

namespace putong {

/// @brief An std::chrono-based timer wrapper.
template<typename clock=std::chrono::high_resolution_clock>
struct Timer {
  using ns = std::chrono::nanoseconds;
  using point = std::chrono::time_point<clock, ns>;
  using duration = std::chrono::duration<double>;

  /// @brief Construct a new timer. This also starts the timer if start=true.
  explicit Timer(bool start = false) {
    if (start)
      Start();
  }

  /// @brief Timer start point.
  point start_{};
  /// @brief Timer stop point.
  point stop_{};

  /// @brief Start the timer.
  inline void Start() { start_ = clock::now(); }

  /// @brief Stop the timer.
  inline void Stop() { stop_ = clock::now(); }

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

} // namespace putong
