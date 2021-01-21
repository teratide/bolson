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
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "bolson/utils.h"
#include "bolson/status.h"

namespace bolson {

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

auto LoadFile(const std::string &file_name,
              size_t num_bytes,
              std::vector<char> *dest) -> Status {
  std::ifstream ifs(file_name, std::ios::binary);
  dest->reserve(num_bytes + 1);
  if (!ifs.read(dest->data(), num_bytes)) {
    return Status(Error::IOError, "Could not load file: " + file_name);
  }
  (*dest)[num_bytes] = '\0';
  return Status::OK();
}

auto WriteIPCMessageBuffer(const std::shared_ptr<arrow::RecordBatch> &batch) -> arrow::Result<
    std::shared_ptr<arrow::Buffer>> {
  auto buffer = arrow::io::BufferOutputStream::Create(GetBatchSize(batch));
  if (!buffer.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer.status());

  auto writer = arrow::ipc::MakeStreamWriter(buffer.ValueOrDie().get(), batch->schema());
  if (!writer.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(writer.status());

  auto status = writer.ValueOrDie()->WriteRecordBatch(*batch);
  if (!status.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(status);

  status = writer.ValueOrDie()->Close();
  if (!status.ok()) return arrow::Result<std::shared_ptr<arrow::Buffer>>(status);

  return buffer.ValueOrDie()->Finish();
}

auto ConvertParserError(const rapidjson::Document &doc,
                        const std::vector<char> &file_buffer) -> std::string {
  std::stringstream ss;
  auto code = doc.GetParseError();
  auto offset = doc.GetErrorOffset();
  ss << "  Parser error: " << rapidjson::GetParseError_En(code) << std::endl;
  ss << "  Offset: " << offset << std::endl;
  ss << "  Character: " << file_buffer[offset] << " / 0x"
     << std::hex << static_cast<uint8_t>(file_buffer[offset]) << std::endl;
  ss << "  Around: "
     << std::string_view(&file_buffer[offset < 40UL ? 0 : offset - 40],
                         std::min(40UL, file_buffer.size()))
     << std::endl;
  return ss.str();
}

}  // namespace bolson
