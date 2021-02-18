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

#include <fstream>

#include "bolson/log.h"
#include "bolson/status.h"

namespace bolson {

auto SaveLatencyMetrics(const LatencyMeasurements& measurements, const std::string& file,
                        size_t from, size_t to, bool with_seq) -> Status {
  using ns = std::chrono::nanoseconds;

  std::ofstream ofs(file);

  if (!ofs.good()) {
    return Status(Error::IOError, "Could not open " + file + " to save latency metrics.");
  }

  // Print header.
  if (with_seq) ofs << "First,Last,";
  for (size_t i = from; i <= to; i++) {
    ofs << TimePoints::point_name(i);
    if (i != to) ofs << ',';
  }
  ofs << std::endl;

  // Print data.
  for (const auto& m : measurements) {
    if (with_seq) {
      ofs << m.seq.first << ",";
      ofs << m.seq.last << ",";
    }
    for (size_t i = from; i <= to; i++) {
      ofs << m.time.GetDiff<ns>(i);
      if (i != to) ofs << ',';
    }
    ofs << std::endl;
  }

  return Status::OK();
}

}  // namespace bolson