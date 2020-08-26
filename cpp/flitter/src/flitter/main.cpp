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

#include "./log.h"
#include "./cli.h"
#include "./file.h"
#include "./stream.h"

auto main(int argc, char *argv[]) -> int {
  // Set up logger.
  flitter::StartLogger();

  // Handle CLI.
  using flitter::AppOptions;
  auto opts = AppOptions(argc, argv);
  if (opts.exit) { return opts.return_value; }

  // Run sub-programs.
  switch (opts.sub) {
    case AppOptions::SubCommand::FILE: return flitter::ProduceFromFile(opts.file);
    case AppOptions::SubCommand::STREAM: return flitter::ProduceFromStream(opts.stream);
  }

  return 0;
}
