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

#include <illex/client_buffering.h>
#include <illex/latency.h>
#include <putong/timer.h>

#include <vector>

#include "bolson/convert/metrics.h"
#include "bolson/publish/metrics.h"
#include "bolson/status.h"
#include "bolson/stream.h"

namespace bolson {

auto SaveStreamMetrics(const bolson::convert::Metrics& converter_metrics,
                 const bolson::publish::Metrics& publisher_metrics,
                 const StreamOptions& opt) -> Status;

}  // namespace bolson