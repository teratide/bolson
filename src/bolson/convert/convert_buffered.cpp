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

#include <vector>
#include <mutex>
#include <string_view>

#include "bolson/convert/convert_buffered.h"
#include "bolson/convert/stats.h"

namespace bolson::convert {

void ConvertFromBuffers(size_t id,
                        const std::vector<illex::RawJSONBuffer *> &buffers,
                        const std::vector<std::mutex *> &mutexes,
                        IpcQueue *out,
                        illex::LatencyTracker *lat_tracker,
                        std::atomic<bool> *shutdown,
                        std::promise<Stats> &&stats_promise) {
  SPDLOG_DEBUG("Convert thread {} spawned.", id);
  const size_t num_buffers = buffers.size();
  size_t next_buf_idx = 0;
  size_t current_buf_idx = 0;
  while (!shutdown->load()) {
    // Attempt to get a lock on a non-empty buffer.
    illex::RawJSONBuffer *buf = nullptr;
    do {
      current_buf_idx = next_buf_idx;
      auto lock = mutexes[current_buf_idx]->try_lock();
      if (lock) {
        if (buffers[current_buf_idx]->empty()) {
          mutexes[current_buf_idx]->unlock();
        } else {
          buf = buffers[current_buf_idx];
        }
      }
      next_buf_idx = (current_buf_idx + 1) % num_buffers;
    } while (buf == nullptr);
    // We now have a non-empty buffer.

    SPDLOG_DEBUG("Thread {} has lock on buffer {} which contains {}",
                 id,
                 current_buf_idx,
                 std::string_view((char *) buf->data(), buf->size()));

    // Conversion goes here.


    buf->Reset();

    mutexes[current_buf_idx]->unlock();
  }
}

}