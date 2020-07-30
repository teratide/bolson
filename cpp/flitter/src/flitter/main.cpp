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

#include <cstdio>
#include <iostream>
#include <filesystem>
#include <iomanip>

#include <CLI/CLI.hpp>

#include "./tweets.h"
#include "./utils.h"
#include "./timer.h"

namespace fs = std::filesystem;

auto main(int argc, char *argv[]) -> int {
  Timer timer;
  CLI::App app{"Flitter : doing stuff with tweets in FPGAs and Pulsar"};

  // Options:
  std::string json_file;

  // CLI options:
  app.add_option("i,-i,--input", json_file, "Input file with Tweets.")->check(CLI::ExistingFile)->required();

  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp &e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    return 0;
  } catch (CLI::Error &e) {
    // There is some error.
    std::cerr << e.get_name() << ":\n" << e.what() << std::endl;
    std::cerr << app.help() << std::endl;
    return -1;
  }

  // Obtain the file size
  fs::path p = fs::current_path() / json_file;
  size_t json_file_size = fs::file_size(p);

  // Load file into memory
  timer.start();
  auto buffer = LoadFile(json_file, json_file_size);
  timer.stop();
  ReportGBps("Load JSON file", json_file_size, timer.seconds());

  // Parse JSON in-place
  timer.start();
  rapidjson::Document doc;
  doc.ParseInsitu(buffer.data());
  timer.stop();
  ReportGBps("Parse JSON file (rapidjson)", json_file_size, timer.seconds());

  // Convert to Arrow:
  timer.start();
  auto batch = CreateRecordBatch(doc);
  timer.stop();
  int64_t batch_size = GetBatchSize(batch);
  ReportGBps("Convert to Arrow RecordBatch (input)", json_file_size, timer.seconds());
  ReportGBps("Convert to Arrow RecordBatch (output)", batch_size, timer.seconds());

  // Write as IPC message into a file:
  timer.start();
  auto ipc_buffer = WriteIPCMessageBuffer(batch);
  timer.stop();
  auto ipc_size = ipc_buffer->size();
  ReportGBps("Write into Arrow IPC message buffer: ", ipc_size, timer.seconds());

  // Report some additional properties:
  double ipc_file_size_GiB = static_cast<double>(ipc_size) / std::pow(2.0, 30);
  double batch_size_GiB = static_cast<double>(batch_size) / std::pow(2.0, 30);
  double json_file_size_GiB = static_cast<double>(json_file_size) / std::pow(2.0, 30);

  std::cout << std::setw(42) << "Number of tweets" << ": " << batch->num_rows() << std::endl;
  std::cout << std::setw(42) << "JSON File size (GiB)" << ": " << json_file_size_GiB << std::endl;
  std::cout << std::setw(42) << "Arrow RecordBatch size (GiB)" << ": " << batch_size_GiB << std::endl;
  std::cout << std::setw(42) << "Arrow IPC message size (GiB)" << ": " << ipc_file_size_GiB << std::endl;

  return 0;
}
