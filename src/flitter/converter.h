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

void ConversionDroneThread(size_t id, illex::Queue *in, IpcQueue *out, std::atomic<bool> *shutdown);

void ConversionHiveThread(illex::Queue *in, IpcQueue *out, std::atomic<bool> *shutdown, size_t num_drones);

}  // namespace flitter
