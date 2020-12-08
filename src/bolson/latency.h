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

#include "bolson/status.h"

// Wait time for queues.
#define BOLSON_QUEUE_WAIT_US          1

// todo: move this into something better and relate the strings outputted in the csv
// Time points measured.

#define BOLSON_LAT_TCP_RECV           0 ///< Time point at which a JSON has arrived in the TCP buffer.
#define BOLSON_LAT_TCP_UNWRAP         1 ///< Time point at which a JSON is unwrapped from the TCP buffer.
#define BOLSON_LAT_BUFFER_ENTRY       2 ///< Time point at which a JSON enters the JSON buffer in a builder.
#define BOLSON_LAT_BUFFER_FLUSH       3 ///< Time point at which a JSON is flushed from the buffer.
#define BOLSON_LAT_BUFFER_PARSED      4 ///< Time point at which a JSON is parsed and converted to Arrow RecordBatch.
#define BOLSON_LAT_BATCH_CONSTRUCTED  5 ///< Time point at which the batch the JSON is in is constructed with seq nrs.
#define BOLSON_LAT_BATCH_COMBINED     6 ///< Time point at which the buffered batches are combined into one.
#define BOLSON_LAT_BATCH_SERIALIZED   7 ///< Time point at which the combined batch is serlized into an IPC message.
#define BOLSON_LAT_PUBLISH_DEQUEUE    8 ///< Time point at which the combined batch is dequeued in the Pulsar publish thread.
#define BOLSON_LAT_MESSAGE_BUILT      9 ///< Time point at which the Pulsar message with the batch is constructed.
#define BOLSON_LAT_MESSAGE_SENT      10 ///< Time point at which the Pulsar message was successfully sent.
#define BOLSON_LAT_NUM_POINTS        11 ///< Total number of time points for latency measurement.

namespace bolson {

/// Options related to measuring JSON latency through the pipeline.
struct LatencyOptions {
  /// Number of latency samples
  size_t num_samples = 1;
  /// Sequence number sample interval for latency samples.
  size_t interval = 1024;
  /// File to dump output
  std::string file;
};

/**
 * \brief Output latency statistics as CSV to some file.
 * \param file          The file to output the CSV to.
 * \param lat_tracker   The latency tracker from which to get the measurements.
 * \return Status::OK() if successful, some error otherwise.
 */
auto LogLatencyCSV(const std::string &file,
                   const illex::LatencyTracker &lat_tracker) -> Status;

}