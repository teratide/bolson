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

#include <fstream>

#include "bolson/latency.h"
#include "bolson/status.h"

namespace bolson {

auto LogLatencyCSV(const std::string &file,
                   const illex::LatencyTracker &lat_tracker) -> Status {
  auto o = std::ofstream(file);
  if (!o.good()) {
    return Status(Error::IOError, "Couldn't open latency output file.");
  }

  // Print header.
  o << "Seq,"
    << " 1. TCP recv. buffer,"
    << " 2. JSON queue,"
    << " 3. JSON buffer,"
    << " 4. JSON parser,"
    << " 5. Adding seq. nos.,"
    << " 6. Combining batches,"
    << " 7. Serializing,"
    << " 8. Publish queue,"
    << " 9. Pulsar message build,"
    << "10. Pulsar send(),"
    << "11. TCP recv -> Pulsar send(),"
    << "12. TCP recv -> Publish queue"
    << std::endl;

  // Dump every sample
  for (size_t t = 0; t < lat_tracker.num_samples(); t++) {
    o << t << ",";
    // Dump every time point
    for (size_t s = BOLSON_LAT_TCP_UNWRAP; s < BOLSON_LAT_NUM_POINTS; s++) {
      o << std::fixed << std::setprecision(9) << lat_tracker.GetInterval(t, s) << ",";
    }
    // Get the time after Pulsar send:
    std::chrono::duration<double>
        total = lat_tracker.Get(t, BOLSON_LAT_MESSAGE_SENT) - lat_tracker.Get(t, 0);
    o << std::fixed << std::setprecision(9) << total.count() << ',';

    // Get the time when the IPC message was serialized.
    // This should be very near the time it was put in the publish queue.
    std::chrono::duration<double>
        fom = lat_tracker.Get(t, BOLSON_LAT_BATCH_SERIALIZED) - lat_tracker.Get(t, 0);
    o << std::fixed << std::setprecision(9) << fom.count() << std::endl;
  }

  return Status::OK();
}

}