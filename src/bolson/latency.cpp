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

template <typename T>
inline static auto GetDiff(const LatencyMeasurement& m, size_t index) -> size_t {
  return std::chrono::duration_cast<T>(m.time[index] - m.time[index - 1]).count();
}

auto SaveLatencyMetrics(const LatencyMeasurements& measurements, const std::string& file)
    -> Status {
  using ns = std::chrono::nanoseconds;

  std::ofstream ofs(file);

  if (!ofs.good()) {
    return Status(Error::IOError, "Could not open " + file + " to save latency metrics.");
  }

  ofs << "First,Last,Parse,Resize,Serialize,Publish" << std::endl;
  for (const auto& m : measurements) {
    ofs << m.seq.first << ",";
    ofs << m.seq.last << ",";
    ofs << GetDiff<ns>(m, TimePoints::parsed) << ",";
    ofs << GetDiff<ns>(m, TimePoints::resized) << ",";
    ofs << GetDiff<ns>(m, TimePoints::serialized) << ",";
    ofs << GetDiff<ns>(m, TimePoints::published) << std::endl;
  }

  return Status::OK();
}

}  // namespace bolson