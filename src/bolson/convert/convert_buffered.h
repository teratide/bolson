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

#include <arrow/api.h>
#include <illex/latency.h>
#include <illex/client_queued.h>
#include <illex/client_buffered.h>

#include "bolson/log.h"
#include "bolson/status.h"
#include "bolson/pulsar.h"
#include "bolson/convert/stats.h"

namespace bolson::convert {

void ConvertFromBuffers(size_t id,
                        const std::vector<illex::RawJSONBuffer *>& buffers,
                        const std::vector<std::mutex *>& mutexes,
                        IpcQueue *out,
                        illex::LatencyTracker *lat_tracker,
                        std::atomic<bool> *shutdown,
                        std::promise<Stats> &&stats_promise);

}