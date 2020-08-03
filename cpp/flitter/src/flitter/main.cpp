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
#include <filesystem>
#include <iomanip>

#include <rapidjson/error/en.h>

#include "./cli.h"
#include "./tweets.h"
#include "./utils.h"
#include "./timer.h"
#include "./pulsar.h"

namespace fs = std::filesystem;

auto main(int argc, char *argv[]) -> int {
  // Handle CLI.
  auto opt = AppOptions(argc, argv);
  if (opt.exit) {
    return opt.return_value;
  }

  Timer timer;

  // Obtain the file size
  fs::path p = fs::current_path() / opt.json_file;
  size_t json_file_size = fs::file_size(p);

  // Load file into memory
  timer.start();
  auto file_buffer = LoadFile(opt.json_file, json_file_size);
  timer.stop();
  ReportGBps("Load JSON file", json_file_size, timer.seconds(), opt.succinct);

  // Parse JSON in-place
  timer.start();
  rapidjson::Document doc;
  doc.ParseInsitu(file_buffer.data());
  if (doc.HasParseError()) {
    auto code = doc.GetParseError();
    auto offset = doc.GetErrorOffset();
    std::cerr << "Could not parse JSON file: " << rapidjson::GetParseError_En(code) << " Offset: " << offset
              << std::endl;
  }
  timer.stop();
  ReportGBps("Parse JSON file (rapidjson)", json_file_size, timer.seconds(), opt.succinct);

  // Convert to Arrow RecordBatch.
  timer.start();
  auto batch = CreateRecordBatch(doc);
  timer.stop();

  // Check for errors.
  if (!batch.ok()) {
    std::cerr << "Could not create RecordBatch: " << batch.status().CodeAsString() << std::endl;
    return AppOptions::failure();
  }

  // Extract properties.
  auto num_rows = batch.ValueOrDie()->num_rows();
  auto batch_size = GetBatchSize(batch.ValueOrDie());

  ReportGBps("Convert to Arrow RecordBatch (input)", json_file_size, timer.seconds(), opt.succinct);
  ReportGBps("Convert to Arrow RecordBatch (output)", batch_size, timer.seconds(), opt.succinct);

  // Write as IPC message into a buffer.
  timer.start();
  auto ipc_buffer = WriteIPCMessageBuffer(batch.ValueOrDie());
  timer.stop();

  // Check for errors.
  if (!ipc_buffer.ok()) {
    std::cerr << "Could not write Arrow IPC message: " << ipc_buffer.status().CodeAsString() << std::endl;
    return AppOptions::failure();
  }

  // Extract properties.
  auto ipc_size = ipc_buffer.ValueOrDie()->size();
  ReportGBps("Write into Arrow IPC message buffer: ", ipc_size, timer.seconds(), opt.succinct);

  // Setup a Pulsar client and shut the logger up.
  auto pulsar_logger = SilentLoggerFactory::create();

  // Create Pulsar client and producer objects and attempt to connect to broker.
  std::pair<std::shared_ptr<pulsar::Client>, std::shared_ptr<pulsar::Producer>> client_producer;
  pulsar::Result pulsar_result = SetupClientProducer(opt.pulsar.url, opt.pulsar.topic,
                                                     pulsar_logger.get(), &client_producer);
  // Check for errors.
  if (pulsar_result != pulsar::ResultOk) {
    std::cerr << "Could not set up Pulsar client/producer: " << pulsar::strResult(pulsar_result) << std::endl;
    return AppOptions::failure();
  }

  // Publish the buffer in Pulsar:
  timer.start();
  pulsar_result = PublishArrowBuffer(client_producer.second, ipc_buffer.ValueOrDie());
  timer.stop();
  // Check for errors.
  if (pulsar_result != pulsar::ResultOk) {
    std::cerr << "Could publish Arrow buffer with Pulsar producer: " << pulsar::strResult(pulsar_result) << std::endl;
    return AppOptions::failure();
  }
  ReportGBps("Publish IPC message in Pulsar: ", ipc_size, timer.seconds(), opt.succinct);

  // Report some additional properties:
  double ipc_size_GiB = static_cast<double>(ipc_size) / std::pow(2.0, 30);
  double batch_size_GiB = static_cast<double>(batch_size) / std::pow(2.0, 30);
  double json_file_size_GiB = static_cast<double>(json_file_size) / std::pow(2.0, 30);

  if (opt.succinct) {
    std::cout << num_rows << ", ";
    std::cout << json_file_size << ", ";
    std::cout << batch_size << ", ";
    std::cout << ipc_size << std::endl;
  } else {
    std::cout << std::setw(42) << "Number of tweets" << ": " << num_rows << std::endl;
    std::cout << std::setw(42) << "JSON File size (GiB)" << ": " << json_file_size_GiB << std::endl;
    std::cout << std::setw(42) << "Arrow RecordBatch size (GiB)" << ": " << batch_size_GiB << std::endl;
    std::cout << std::setw(42) << "Arrow IPC message size (GiB)" << ": " << ipc_size_GiB << std::endl;
  }

  return AppOptions::success();
}
