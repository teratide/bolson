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

#include <memory>

#include <arrow/api.h>
#include <illex/queue.h>

namespace flitter {

using IpcQueue = moodycamel::ConcurrentQueue<std::shared_ptr<arrow::Buffer>>;

/// Statistics about the conversion functions.
struct ConversionStats {
  /// Number of converted JSONs.
  size_t num_jsons = 0;
  /// Number of IPC messages.
  size_t num_ipc = 0;
  /// Number of bytes in the IPC messages.
  size_t ipc_bytes = 0;
  /// Total time spent on conversion only.
  double convert_time = 0.0;
  /// Total time spent in this thread.
  double thread_time = 0.0;
};

/**
 * \brief Converts JSONs to Arrow IPC messages. Multi-threaded.
 * \param in            The input queue of JSONs
 * \param out           The output queue for Arrow IPC messages.
 * \param shutdown      Signal to shut down this thread (typically used when there will be no more new inputs).
 * \param num_drones    Number of conversion threads to spawn.
 * \param stats         Statistics for each conversion thread.
 */
void ConversionHiveThread(illex::Queue *in,
                          IpcQueue *out,
                          std::atomic<bool> *shutdown,
                          size_t num_drones,
                          std::promise<std::vector<ConversionStats>> &&stats);

}
