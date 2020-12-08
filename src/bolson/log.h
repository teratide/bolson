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

#ifndef NDEBUG
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#endif

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_sinks.h>

namespace bolson {

inline void StartLogger() {
  auto logger = spdlog::stdout_logger_mt("bolson");
  logger->set_pattern("[%n] [%l] %v");
  spdlog::set_default_logger(logger);
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::debug);
#endif
}

}  // namespace bolson
