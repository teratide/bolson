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
#include <algorithm>

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

  // Run microbenchmarks.
  if (opt.micro_bench.must_run()) {
    if (opt.micro_bench.tweets_builder) {
      TweetsBuilder::RunBenchmark(1024 * 1024 * 16);
    }
    return AppOptions::success();
  }

  if (opt.json_file.empty()) {
    return AppOptions::failure();
  }

  // Setup a Pulsar client and shut the logger up.
  auto pulsar_logger = FlitterLoggerFactory::create();
  // Create Pulsar client and producer objects and attempt to connect to broker.
  std::pair<std::shared_ptr<pulsar::Client>, std::shared_ptr<pulsar::Producer>> client_producer;
  pulsar::Result pulsar_result = SetupClientProducer(opt.pulsar.url, opt.pulsar.topic,
                                                     pulsar_logger.get(), &client_producer);
  // Check for errors.
  if (pulsar_result != pulsar::ResultOk) {
    std::cerr << "Could not set up Pulsar client/producer: " << pulsar::strResult(pulsar_result) << std::endl;
    return AppOptions::failure();
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
  // Check for errors.
  if (doc.HasParseError()) {
    std::cerr << "Could not parse JSON file: " << opt.json_file << std::endl;
    ReportParserError(doc, file_buffer);
    return AppOptions::failure();
  }
  timer.stop();
  ReportGBps("Parse JSON file (rapidjson)", json_file_size, timer.seconds(), opt.succinct);

  // Convert to Arrow RecordBatches.
  timer.start();
  auto batches = CreateRecordBatches(doc, opt.pulsar.max_message_size);
  timer.stop();
  // Check for errors.
  if (!batches.ok()) {
    std::cerr << "Could not create RecordBatch: " << batches.status().CodeAsString() << std::endl;
    return AppOptions::failure();
  }

  // Extract properties and statistics.
  auto num_batches = batches.ValueOrDie().size();
  size_t batches_total_size = 0;
  size_t num_tweets = 0;
  for (const auto &batch : batches.ValueOrDie()) {
    num_tweets += batch->num_rows();
    batches_total_size += GetBatchSize(batch);
  }
  auto batches_avg_rows = static_cast<double>(num_tweets) / num_batches;
  auto batches_avg_size = static_cast<double>(batches_total_size) / num_batches;

  ReportGBps("Convert to Arrow RecordBatch (input)", json_file_size, timer.seconds(), opt.succinct);
  ReportGBps("Convert to Arrow RecordBatch (output)", batches_total_size, timer.seconds(), opt.succinct);

  // Write every batch as IPC message.
  timer.start();
  size_t ipc_total_size = 0;
  std::vector<std::shared_ptr<arrow::Buffer>> ipc_buffers;
  for (const auto &batch : batches.ValueOrDie()) {
    // Write into buffer
    auto ipc_buffer = WriteIPCMessageBuffer(batch);
    // Check for errors.
    if (!ipc_buffer.ok()) {
      std::cerr << "Could not write Arrow IPC message: " << ipc_buffer.status().CodeAsString() << std::endl;
      return AppOptions::failure();
    }
    ipc_total_size += ipc_buffer.ValueOrDie()->size();
    ipc_buffers.push_back(ipc_buffer.ValueOrDie());
  }
  timer.stop();

  // Extract properties.
  auto ipc_avg_size = static_cast<double>(ipc_total_size) / num_batches;
  ReportGBps("Write into Arrow IPC message buffer: ", ipc_total_size, timer.seconds(), opt.succinct);

  // Publish the buffer in Pulsar:
  timer.start();
  for (const auto &ipc_buffer : ipc_buffers) {
    pulsar_result = PublishArrowBuffer(client_producer.second, ipc_buffer);
  }
  timer.stop();
  // Check for errors.
  if (pulsar_result != pulsar::ResultOk) {
    std::cerr << "Could not publish Arrow buffer with Pulsar producer: "
              << pulsar::strResult(pulsar_result)
              << std::endl;
    return AppOptions::failure();
  }
  ReportGBps("Publish IPC message in Pulsar: ", ipc_total_size, timer.seconds(), opt.succinct);

  // Report some additional properties:
  double ipc_total_size_GiB = static_cast<double>(ipc_total_size) / std::pow(2.0, 30);
  double batches_total_size_GiB = static_cast<double>(batches_total_size) / std::pow(2.0, 30);
  double json_file_size_GiB = static_cast<double>(json_file_size) / std::pow(2.0, 30);

  if (opt.succinct) {
    std::cout << json_file_size << ", ";
    std::cout << num_tweets << ", ";
    std::cout << num_batches << ", ";
    std::cout << batches_total_size << ", ";
    std::cout << batches_avg_size << ", ";
    std::cout << ipc_total_size << ", ";
    std::cout << ipc_avg_size << std::endl;
  } else {
    std::cout << std::setw(42) << "JSON File size (GiB)" << ": " << json_file_size_GiB << std::endl;
    std::cout << std::setw(42) << "Number of tweets" << ": " << num_tweets << std::endl;
    std::cout << std::setw(42) << "Number of RecordBatches" << ": " << num_batches << std::endl;
    std::cout << std::setw(42) << "Arrow RecordBatches total size (GiB)" << ": " << batches_total_size_GiB << std::endl;
    std::cout << std::setw(42) << "Arrow RecordBatch avg. size (B)" << ": " << batches_avg_size << std::endl;
    std::cout << std::setw(42) << "Arrow IPC messages total size (GiB)" << ": " << ipc_total_size_GiB << std::endl;
    std::cout << std::setw(42) << "Arrow IPC messages avg. size (B)" << ": " << ipc_avg_size << std::endl;
  }

  return AppOptions::success();
}
