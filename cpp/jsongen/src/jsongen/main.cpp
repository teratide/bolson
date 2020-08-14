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

#include <iostream>
#include <flitter/log.h>

#include "./cli.h"
#include "./file.h"
#include "./stream.h"

using jsongen::AppOptions;
using jsongen::GenerateFile;
using jsongen::StreamServer;

auto main(int argc, char *argv[]) -> int {
  // Set up logger.
  flitter::StartLogger();

  // Parse command-line arguments:
  auto opt = jsongen::AppOptions(argc, argv);
  if (opt.exit) { return opt.return_value; }

  // Run the requested sub-program:
  switch (opt.sub) {
    case jsongen::SubCommand::FILE: return GenerateFile(opt.file);
    case jsongen::SubCommand::STREAM: return StreamServer(opt.stream);
  }

  return 0;
}
