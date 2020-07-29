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

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <CLI/CLI.hpp>

#include <rapidjson/document.h>

#include "./tweets.h"
#include "./timer.h"

namespace fs = std::filesystem;

/// @brief Parse a referenced tweet type.
auto parse_ref_type(const std::string &s) -> uint8_t {
  if (s == "retweeted") return 0;
  if (s == "quoted") return 1;
  if (s == "replied_to") return 2;

  // TODO(johanpel): improve this:
  throw std::runtime_error("Unkown referenced_tweet type:" + s);
}

/// @brief Report some gigabytes per second.
void report_GBps(const std::string &text, size_t bytes, double s) {
  double GB = static_cast<double>(bytes) * std::pow(10.0, -9);
  std::cout << std::setw(42) << std::left << text << ": "
            << std::setw(8) << std::setprecision(3) << s << " s | "
            << std::setw(8) << std::setprecision(3) << (GB / s) << " GB/s"
            << std::endl;
}

/**
 * @brief Returns the total size in memory of all (nested) buffers backing Arrow ArrayData.
 *
 * Returns int64_t because Arrow.
 * @param array_data The ArrayData to analyze.
 * @returns The total size of all (nested) buffer contents in bytes.
 */
auto GetArrayDataSize(const std::shared_ptr<arrow::ArrayData> &array_data) -> int64_t {
  int64_t result = 0;

  // First obtain the size of all children:
  for (const auto &child : array_data->child_data) {
    result += GetArrayDataSize(child);
  }
  // Obtain the size of all buffers at this level of ArrayData
  for (const auto &buffer : array_data->buffers) {
    // Buffers can be nullptrs in Arrow, hurray.
    if (buffer != nullptr) {
      result += buffer->size();
    }
  }

  return result;
}

/// @brief Write an Arrow RecordBatch into a file as an Arrow IPC message.
auto WriteIPCMessageFile(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &file) -> int64_t {
  std::shared_ptr<arrow::io::FileOutputStream> f = arrow::io::FileOutputStream::Open(file).ValueOrDie();
  std::shared_ptr<arrow::ipc::RecordBatchWriter>
      writer = arrow::ipc::NewFileWriter(f.get(), batch->schema()).ValueOrDie();
  auto status = writer->WriteRecordBatch(*batch);
  int64_t ipc_file_size = f->Tell().ValueOrDie();
  if (!status.ok()) {
    throw std::runtime_error("Error writing RecordBatch to file.");
  }
  status = writer->Close();
  f->Close();
  return ipc_file_size;
}

/**
 * @brief Return the total size in memory of an Arrow RecordBatch.
 * @param batch The RecordBatch to analyze.
 * @return The total size in bytes.
 */
auto GetBatchSize(const std::shared_ptr<arrow::RecordBatch> &batch) -> int64_t {
  int64_t batch_size = 0;
  for (const auto &column : batch->columns()) {
    batch_size += GetArrayDataSize(column->data());
  }
  return batch_size;
}

/**
 * @brief Read num_bytes from a file and buffer it in memory.
 * @param file_name The file to load.
 * @param num_bytes The number of bytes to read into the buffer.
 * @return The buffer.
 */
auto LoadFile(const std::string &file_name, size_t num_bytes) -> std::vector<char> {
  std::ifstream ifs(file_name, std::ios::binary);
  std::vector<char> buffer(num_bytes);
  if (!ifs.read(buffer.data(), num_bytes)) {
    // TODO(johanpel): don't throw
    throw std::runtime_error("Could not read file " + file_name + " into memory.");
  }
  return buffer;
}

/**
 * @brief Convert a parsed JSON document with tweets into an Arrow RecordBatch
 * @param doc The rapidjson document.
 * @return The Arrow RecordBatch.
 */
auto CreateRecordBatch(const rapidjson::Document &doc) -> shared_ptr<RecordBatch> {
  // Set up Arrow RecordBatch builder for tweets:
  TweetsBuilder t;
  // Iterate over every tweet:
  for (const auto &tweet : doc.GetArray()) {
    const auto &d = tweet["data"];

    // Parse referenced tweets:
    vector<pair<uint8_t, uint64_t>> ref_tweets;
    for (const auto &r : d["referenced_tweets"].GetArray()) {
      ref_tweets.emplace_back(parse_ref_type(r["type"].GetString()),
                              strtoul(r["id"].GetString(), nullptr, 10));
    }

    t.Append(strtoul(d["id"].GetString(), nullptr, 10),
             d["created_at"].GetString(),
             d["text"].GetString(),
             strtoul(d["author_id"].GetString(), nullptr, 10),
             strtoul(d["in_reply_to_user_id"].GetString(), nullptr, 10),
             ref_tweets);
  }
  // Finalize the builder:
  shared_ptr<RecordBatch> batch;
  t.Finish(&batch);
  return batch;
}

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
  report_GBps("Load JSON file", json_file_size, timer.seconds());

  // Parse JSON in-place
  timer.start();
  rapidjson::Document doc;
  doc.ParseInsitu(buffer.data());
  timer.stop();
  report_GBps("Parse JSON file (rapidjson)", json_file_size, timer.seconds());

  // Convert to Arrow:
  timer.start();
  shared_ptr<RecordBatch> batch = CreateRecordBatch(doc);
  timer.stop();
  int64_t batch_size = GetBatchSize(batch);
  report_GBps("Convert to Arrow RecordBatch (input)", json_file_size, timer.seconds());
  report_GBps("Convert to Arrow RecordBatch (output)", batch_size, timer.seconds());

  // Write as IPC message into a file:
  timer.start();
  auto ipc_file_size = WriteIPCMessageFile(batch, "test.rb");
  timer.stop();
  report_GBps("Write into Arrow IPC message file: ", ipc_file_size, timer.seconds());

  // Report some additional properties:
  double ipc_file_size_GiB = static_cast<double>(ipc_file_size) / std::pow(2.0, 30);
  double batch_size_GiB = static_cast<double>(batch_size) / std::pow(2.0, 30);
  double json_file_size_GiB = static_cast<double>(json_file_size) / std::pow(2.0, 30);

  std::cout << std::setw(42) << "Number of tweets" << ": " << batch->num_rows() << std::endl;
  std::cout << std::setw(42) << "JSON File size (GiB)" << ": " << json_file_size_GiB << std::endl;
  std::cout << std::setw(42) << "Arrow RecordBatch size in memory (GiB)" << ": " << batch_size_GiB << std::endl;
  std::cout << std::setw(42) << "Arrow IPC message file size (GiB)" << ": " << ipc_file_size_GiB << std::endl;

  return 0;
}
