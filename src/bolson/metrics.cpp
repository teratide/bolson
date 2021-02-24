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

#include "bolson/metrics.h"

#include <fstream>

#include "bolson/latency.h"
#include "bolson/log.h"
#include "bolson/parse/implementations.h"
#include "bolson/status.h"

namespace bolson {

auto SaveMetrics(const bolson::convert::Metrics& converter_metrics,
                 const bolson::publish::Metrics& publisher_metrics,
                 const StreamOptions& opt) -> Status {
  using ns = std::chrono::nanoseconds;

  // Open output stream to write file.
  std::ofstream ofs(opt.metrics_file);
  if (!ofs.good()) {
    return Status(Error::IOError,
                  "Could not open " + opt.metrics_file + " to save metrics.");
  }

  // Write header.
  ofs << "Producer threads,Converter threads,Parser,Persistent topic,Batched mode,";
  for (size_t i = TimePoints::received; i <= TimePoints::published; i++) {
    ofs << TimePoints::point_name(i);
    if (i != TimePoints::published) ofs << ',';
  }
  ofs << std::endl;

  // Print data.
  for (const auto& m : publisher_metrics.latencies) {
    ofs << opt.pulsar.num_producers << ",";
    ofs << opt.converter.num_threads << ",";
    ofs << bolson::parse::ToString(opt.converter.parser.impl) << ",";
    ofs << (opt.pulsar.topic.find("non-persistent") == std::string::npos) << ",";
    ofs << opt.pulsar.batching.enable << ",";

    for (size_t i = TimePoints::received; i <= TimePoints::published; i++) {
      ofs << m.time.GetDiff<ns>(i);
      if (i != TimePoints::published) ofs << ',';
    }
    ofs << std::endl;
  }

  return Status::OK();
}

}  // namespace bolson