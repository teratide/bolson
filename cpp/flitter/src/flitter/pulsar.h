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
#include <pulsar/Client.h>
#include <pulsar/Producer.h>

// Some stuff to shut the default logger in Pulsar up.
class SilentLogger : public pulsar::Logger {
  std::string _logger;
 public:
  explicit SilentLogger(std::string logger) : _logger(std::move(logger)) {}
  auto isEnabled(Level level) -> bool override { return false; }
  void log(Level level, int line, const std::string &message) override {}
};

class SilentLoggerFactory : public pulsar::LoggerFactory {
 public:
  auto getLogger(const std::string &fileName) -> pulsar::Logger * override {
    return new SilentLogger(fileName);
  }
  static auto create() -> std::unique_ptr<LoggerFactory> {
    return std::unique_ptr<LoggerFactory>(new SilentLoggerFactory());
  }
};

auto SetupClientProducer(pulsar::LoggerFactory *logger) -> std::pair<std::shared_ptr<pulsar::Client>,
                                                              std::shared_ptr<pulsar::Producer>>;

auto PublishArrowBuffer(const std::shared_ptr<pulsar::Producer> &producer,
                        const std::shared_ptr<arrow::Buffer> &buffer) -> int;
