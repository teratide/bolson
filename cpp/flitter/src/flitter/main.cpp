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
#include "./pulsar.h"

namespace fs = std::filesystem;

auto main(int argc, char *argv[]) -> int {
  Timer timer;
  CLI::App app{"Flitter : doing stuff with tweets in FPGAs and Pulsar"};

  // Options:
  std::string json_file;
  bool succinct = false;

  // CLI options:
  app.add_option("i,-i,--input", json_file, "Input file with Tweets.")->check(CLI::ExistingFile)->required();
  app.add_flag("-s,--succinct-stats", succinct, "Prints measurements to stdout on a single line.");

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
  auto file_buffer = LoadFile(json_file, json_file_size);
  timer.stop();
  ReportGBps("Load JSON file", json_file_size, timer.seconds(), succinct);

  // Parse JSON in-place
  timer.start();
  rapidjson::Document doc;
  doc.ParseInsitu(file_buffer.data());
  timer.stop();
  ReportGBps("Parse JSON file (rapidjson)", json_file_size, timer.seconds(), succinct);

  // Convert to Arrow:
  timer.start();
  auto batch = CreateRecordBatch(doc);
  timer.stop();
  int64_t batch_size = GetBatchSize(batch);
  ReportGBps("Convert to Arrow RecordBatch (input)", json_file_size, timer.seconds(), succinct);
  ReportGBps("Convert to Arrow RecordBatch (output)", batch_size, timer.seconds(), succinct);

  // Write as IPC message into a buffer:
  timer.start();
  auto ipc_buffer = WriteIPCMessageBuffer(batch);
  timer.stop();
  auto ipc_size = ipc_buffer->size();
  ReportGBps("Write into Arrow IPC message buffer: ", ipc_size, timer.seconds(), succinct);


  // Setup a Pulsar client and shut the logger up.
  auto pulsar_logger = SilentLoggerFactory::create();
  auto client_producer = SetupClientProducer(pulsar_logger.get());

  // Publish the buffer in Pulsar:
  timer.start();
  PublishArrowBuffer(client_producer.second, ipc_buffer);
  timer.stop();
  ReportGBps("Publish IPC message in Pulsar: ", ipc_size, timer.seconds(), succinct);

  // Report some additional properties:
  double ipc_size_GiB = static_cast<double>(ipc_size) / std::pow(2.0, 30);
  double batch_size_GiB = static_cast<double>(batch_size) / std::pow(2.0, 30);
  double json_file_size_GiB = static_cast<double>(json_file_size) / std::pow(2.0, 30);

  if (succinct) {
    std::cout << batch->num_rows() << ", ";
    std::cout << json_file_size << ", ";
    std::cout << batch_size << ", ";
    std::cout << ipc_size << std::endl;
  } else {
    std::cout << std::setw(42) << "Number of tweets" << ": " << batch->num_rows() << std::endl;
    std::cout << std::setw(42) << "JSON File size (GiB)" << ": " << json_file_size_GiB << std::endl;
    std::cout << std::setw(42) << "Arrow RecordBatch size (GiB)" << ": " << batch_size_GiB << std::endl;
    std::cout << std::setw(42) << "Arrow IPC message size (GiB)" << ": " << ipc_size_GiB << std::endl;
  }

  //return 0;
}
