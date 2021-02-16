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

#include "bolson/cli.h"
#include "bolson/log.h"
#include "bolson/status.h"
#include "bolson/stream.h"

auto main(int argc, char* argv[]) -> int {
  // Set up logger.
  bolson::StartLogger();

  // Handle CLI.
  bolson::AppOptions opts;
  auto status = bolson::AppOptions::FromArguments(argc, argv, &opts);
  if (status.ok()) {
    // Run sub-programs.
    bolson::Status result;
    switch (opts.sub) {
      case bolson::SubCommand::STREAM:
        status = bolson::ProduceFromStream(opts.stream);
        break;
      case bolson::SubCommand::BENCH:
        status = bolson::RunBench(opts.bench);
        break;
      case bolson::SubCommand::NONE:
        break;
    }
  }

  if (!status.ok()) {
    spdlog::error("{} exiting with {}:", bolson::AppOptions::name,
                  ToString(status.err()));
    spdlog::error("{}", status.msg());
    return -1;
  }

  return 0;
}
