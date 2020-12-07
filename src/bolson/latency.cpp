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

namespace bolson {

void LogLatency(const illex::LatencyTracker &lat_tracker) {
  std::cout << "Seq,"
            << "Time in TCP recv. buffer,"
            << "Time in JSON item queue,"
            << "Time in JSON buffer,"
            << "Time in JSON parser,"
            << "Time adding seq. \\#,"
            << "Time combining batches,"
            << "Time serializing batch,"
            << "Time in publish queue,"
            << "Time to Pulsar send(),"
            << "Total"
            << std::endl;
  for (size_t t = 0; t < lat_tracker.num_samples(); t++) {
    std::cout << t << ",";
    for (size_t s = 1; s < BOLSON_LAT_NUM_POINTS; s++) {
      std::cout << std::fixed << std::setprecision(9)
                << lat_tracker.GetInterval(t, s);
      std::cout << ",";
    }
    std::chrono::duration<double>
        total = lat_tracker.Get(t, BOLSON_LAT_NUM_POINTS - 1) - lat_tracker.Get(t, 0);
    std::cout << std::fixed << std::setprecision(9) << total.count() << std::endl;
  }
}

}