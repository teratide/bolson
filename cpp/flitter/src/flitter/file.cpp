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

#include <filesystem>
#include <rapidjson/document.h>
#include <putong/timer.h>

#include "./utils.h"
#include "./file.h"
#include "./pulsar.h"
#include "./hive.h"

namespace fs = std::filesystem;
namespace pt = putong;

namespace flitter {

auto ProduceFromFile(const FileOptions &opt) -> int {
  // Setup a Pulsar client and redirect the logger.
  auto pulsar_logger = FlitterLoggerFactory::create();
  // Create Pulsar client and producer objects and attempt to connect to broker.
  std::pair<std::shared_ptr<pulsar::Client>, std::shared_ptr<pulsar::Producer>> client_producer;
  pulsar::Result pulsar_result = SetupClientProducer(opt.pulsar.url, opt.pulsar.topic,
                                                     pulsar_logger.get(), &client_producer);
  // Check for errors.
  if (pulsar_result != pulsar::ResultOk) {
    spdlog::error("Could not set up Pulsar client/producer: {}", pulsar::strResult(pulsar_result));
    return -1;
  }

  pt::Timer timer;

  // Obtain the file size
  fs::path p = fs::current_path() / opt.input;
  size_t json_file_size = fs::file_size(p);

  // Load file into memory
  timer.Start();
  auto file_buffer = LoadFile(opt.input, json_file_size);
  timer.Stop();
  ReportGBps("Load JSON file", json_file_size, timer.seconds(), opt.succinct);

  // Parse JSON in-place
  timer.Start();
  rapidjson::Document doc;
  doc.ParseInsitu(file_buffer.data());
  // Check for errors.
  if (doc.HasParseError()) {
    spdlog::error("Could not parse JSON file: {}", opt.input);
    ReportParserError(doc, file_buffer);
    return -1;
  }
  timer.Stop();
  ReportGBps("Parse JSON file (rapidjson)", json_file_size, timer.seconds(), opt.succinct);

  // Convert to Arrow RecordBatches.
//  timer.Start();
//  auto batches = CreateRecordBatches(doc, opt.pulsar.max_msg_size);
//  timer.Stop();
//  // Check for errors.
//  if (!batches.ok()) {
//    spdlog::error("Could not create RecordBatch: ", batches.status().CodeAsString());
//    return -1;
//  }

  // Extract properties and statistics.
//  auto num_batches = batches.ValueOrDie().size();
//  size_t batches_total_size = 0;
//  size_t num_tweets = 0;
//  for (const auto &batch : batches.ValueOrDie()) {
//    num_tweets += batch->num_rows();
//    batches_total_size += GetBatchSize(batch);
//  }
//  auto batches_avg_rows = static_cast<double>(num_tweets) / num_batches;
//  auto batches_avg_size = static_cast<double>(batches_total_size) / num_batches;
//
//  ReportGBps("Convert to Arrow RecordBatch (input)", json_file_size, timer.seconds(), opt.succinct);
//  ReportGBps("Convert to Arrow RecordBatch (output)", batches_total_size, timer.seconds(), opt.succinct);

  // Write every batch as IPC message.
  timer.Start();
  size_t ipc_total_size = 0;
  std::vector<std::shared_ptr<arrow::Buffer>> ipc_buffers;
//  for (const auto &batch : batches.ValueOrDie()) {
//    // Write into buffer
//    auto ipc_buffer = WriteIPCMessageBuffer(batch);
//    // Check for errors.
//    if (!ipc_buffer.ok()) {
//      spdlog::error("Could not write Arrow IPC message: {}", ipc_buffer.status().CodeAsString());
//      return -1;
//    }
//    ipc_total_size += ipc_buffer.ValueOrDie()->size();
//    ipc_buffers.push_back(ipc_buffer.ValueOrDie());
//  }
  timer.Stop();

  // Extract properties.
//  auto ipc_avg_size = static_cast<double>(ipc_total_size) / num_batches;
//  ReportGBps("Write into Arrow IPC message buffer: ", ipc_total_size, timer.seconds(), opt.succinct);

  // Publish the buffer in Pulsar:
  timer.Start();
  for (const auto &ipc_buffer : ipc_buffers) {
    pulsar_result = PublishArrowBuffer(client_producer.second, ipc_buffer);
  }
  timer.Stop();
  // Check for errors.
  if (pulsar_result != pulsar::ResultOk) {
    spdlog::error("Could not publish Arrow buffer with Pulsar producer: {}", pulsar::strResult(pulsar_result));
    return -1;
  }
  ReportGBps("Publish IPC message in Pulsar: ", ipc_total_size, timer.seconds(), opt.succinct);

  // Report some additional properties:

//  if (opt.succinct) {
//    std::cout << json_file_size << ", ";
//    std::cout << num_tweets << ", ";
//    std::cout << num_batches << ", ";
//    std::cout << batches_total_size << ", ";
//    std::cout << batches_avg_size << ", ";
//    std::cout << ipc_total_size << ", ";
//    std::cout << ipc_avg_size << std::endl;
//  } else {
//    std::cout << std::setw(42) << "JSON File size (B)" << ": " << json_file_size << std::endl;
//    std::cout << std::setw(42) << "Number of tweets" << ": " << num_tweets << std::endl;
//    std::cout << std::setw(42) << "Number of RecordBatches" << ": " << num_batches << std::endl;
//    std::cout << std::setw(42) << "Arrow RecordBatches total size (B)" << ": " << batches_total_size << std::endl;
//    std::cout << std::setw(42) << "Arrow RecordBatch avg. size (B)" << ": " << batches_avg_size << std::endl;
//    std::cout << std::setw(42) << "Arrow IPC messages total size (B)" << ": " << ipc_total_size << std::endl;
//    std::cout << std::setw(42) << "Arrow IPC messages avg. size (B)" << ": " << ipc_avg_size << std::endl;
//  }

  return 0;
}

}  // namespace flitter
