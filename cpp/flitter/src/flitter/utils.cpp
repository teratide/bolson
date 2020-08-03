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

#include <cmath>
#include <iostream>
#include <fstream>
#include <iomanip>

#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "./utils.h"

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

auto GetBatchSize(const std::shared_ptr<arrow::RecordBatch> &batch) -> int64_t {
  int64_t batch_size = 0;
  for (const auto &column : batch->columns()) {
    batch_size += GetArrayDataSize(column->data());
  }
  return batch_size;
}

void ReportGBps(const std::string &text, size_t bytes, double s, bool succinct) {
  double GB = static_cast<double>(bytes) * std::pow(10.0, -9);
  if (succinct) {
    std::cout << s << ", " << (GB / s) << ", ";
  } else {
    std::cout << std::setw(42) << std::left << text << ": "
              << std::setw(8) << std::setprecision(3) << s << " s | "
              << std::setw(8) << std::setprecision(3) << (GB / s) << " GB/s"
              << std::endl;
  }
}

auto LoadFile(const std::string &file_name, size_t num_bytes) -> std::vector<char> {
  std::ifstream ifs(file_name, std::ios::binary);
  std::vector<char> buffer(num_bytes);
  if (!ifs.read(buffer.data(), num_bytes)) {
    // TODO(johanpel): don't throw
    throw std::runtime_error("Could not read file " + file_name + " into memory.");
  }
  return buffer;
}

auto WriteIPCMessageBuffer(const std::shared_ptr<arrow::RecordBatch> &batch) -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
  auto buffer = arrow::io::BufferOutputStream::Create(GetBatchSize(batch));
  if (!buffer.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer.status());

  auto writer = arrow::ipc::NewStreamWriter(buffer.ValueOrDie().get(), batch->schema());
  if (!writer.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(writer.status());

  auto status = writer.ValueOrDie()->WriteRecordBatch(*batch);
  if (!status.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(status);

  status = writer.ValueOrDie()->Close();
  if (!status.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(status);

  return buffer.ValueOrDie()->Finish();
}
