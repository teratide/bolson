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

#include <vector>
#include <putong/timer.h>
#include <illex/latency.h>

namespace bolson {

// Wait time for queues.
#define BOLSON_QUEUE_WAIT_US          1

// Time points measured.
#define BOLSON_LAT_TCP_RECV           0  // measured in illex.
#define BOLSON_LAT_TCP_UNWRAP         1  // measured in illex.
#define BOLSON_LAT_BUFFER_ENTRY       2
#define BOLSON_LAT_BUFFER_FLUSH       3
#define BOLSON_LAT_BUFFER_PARSED      4
#define BOLSON_LAT_BATCH_CONSTRUCTED  5
#define BOLSON_LAT_BATCH_COMBINED     6
#define BOLSON_LAT_BATCH_SERIALIZED   7
#define BOLSON_LAT_PUBLISH_DEQUEUE    8
#define BOLSON_LAT_MESSAGE_BUILT      9
#define BOLSON_LAT_MESSAGE_SENT      10
#define BOLSON_LAT_NUM_POINTS        11

struct LatencyOptions {
  /// Number of latency samples
  size_t num_samples = 1;
  /// Sequence number sample interval for latency samples.
  size_t interval = 1024;
};

void LogLatency(const illex::LatencyTracker &lat_tracker);

}