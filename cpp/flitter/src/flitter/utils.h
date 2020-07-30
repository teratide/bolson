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

#pragma once

#include <arrow/api.h>

/**
 * @brief Returns the total size in memory of all (nested) buffers backing Arrow ArrayData.
 *
 * Returns int64_t because Arrow.
 * @param array_data The ArrayData to analyze.
 * @returns The total size of all (nested) buffer contents in bytes.
 */
auto GetArrayDataSize(const std::shared_ptr<arrow::ArrayData> &array_data) -> int64_t;

/**
 * @brief Return the total size in memory of an Arrow RecordBatch.
 * @param batch The RecordBatch to analyze.
 * @return The total size in bytes.
 */
auto GetBatchSize(const std::shared_ptr<arrow::RecordBatch> &batch) -> int64_t;

/// @brief Write an Arrow RecordBatch into a file as an Arrow IPC message.
auto WriteIPCMessageBuffer(const std::shared_ptr<arrow::RecordBatch> &batch) -> std::shared_ptr<arrow::Buffer>;

/// @brief Report some gigabytes per second.
void ReportGBps(const std::string &text, size_t bytes, double s, bool succinct = false);

/**
 * @brief Read num_bytes from a file and buffer it in memory.
 * @param file_name The file to load.
 * @param num_bytes The number of bytes to read into the buffer.
 * @return The buffer.
 */
auto LoadFile(const std::string &file_name, size_t num_bytes) -> std::vector<char>;

