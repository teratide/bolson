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
#include <illex/client_buffering.h>

#include <utility>
#include <variant>

#include "bolson/buffer/allocator.h"
#include "bolson/latency.h"
#include "bolson/status.h"
#include "bolson/utils.h"

/// Contains all constructs to parse JSONs to Arrow RecordBatches
namespace bolson::parse {

/**
 * \brief The result of parsing a raw JSON buffer.
 */
struct ParsedBatch {
  ParsedBatch() = default;
  ParsedBatch(std::shared_ptr<arrow::RecordBatch> batch, illex::SeqRange seq_range)
      : batch(std::move(batch)), seq_range(seq_range){};
  /// The resulting Arrow RecordBatch.
  std::shared_ptr<arrow::RecordBatch> batch = nullptr;
  /// Range of sequence numbers in batch.
  illex::SeqRange seq_range = {0, 0};
};

/**
 * \brief Abstract class for implementations of parsing supplied buffers to RecordBatches.
 */
class Parser {
 public:
  /**
   * \brief Parse buffers containing raw JSON data.
   *
   * Appends parsed buffers as RecordBatches to batches_out.
   * No guarantees are made about the relation between the input buffers and output
   * batches, other than that for each valid JSON object, there will be one corresponding
   * Arrow record in one of the resulting batches.
   *
   * \param buffers_in  The buffers with the raw JSON data.
   * \param batches_out The parsed data represented as Arrow RecordBatches.
   * \return Status::OK() if successful, some error otherwise.
   */
  virtual auto Parse(const std::vector<illex::JSONBuffer*>& buffers_in,
                     std::vector<ParsedBatch>* batches_out) -> Status = 0;
};

/**
 * \brief Abstract class for implementations to define contexts around parsers.
 */
class ParserContext {
 public:
  /// \brief Return the parsers managed by this context.
  virtual auto parsers() -> std::vector<std::shared_ptr<Parser>> = 0;

  /// \brief Return no. threads allowed by impl. based on desired no. threads.
  [[nodiscard]] virtual auto CheckThreadCount(size_t num_threads) const -> size_t {
    return num_threads;
  }

  /// \brief Return no. input buffers allowed by impl. based on desired no. buffers.
  [[nodiscard]] virtual auto CheckBufferCount(size_t num_buffers) const -> size_t {
    return num_buffers;
  }

  /// \brief Return the Arrow input schema used by the parsers to convert JSONS.
  [[nodiscard]] virtual auto input_schema() const -> std::shared_ptr<arrow::Schema> = 0;

  /// \brief Return the Arrow output schema used by the parsers to convert JSONs.
  [[nodiscard]] virtual auto output_schema() const -> std::shared_ptr<arrow::Schema> = 0;

  /**
   * \brief Return pointers to all input buffers.
   *
   * Buffers should only be accessed after obtaining a lock using mutexes().
   */
  auto mutable_buffers() -> std::vector<illex::JSONBuffer*>;

  /// \brief Return pointers to the mutexes of all input buffers.
  auto mutexes() -> std::vector<std::mutex*>;

  /// \brief Lock all mutexes of all buffers.
  void LockBuffers();

  /// \brief Unlock all mutexes of all buffers.
  void UnlockBuffers();

 protected:
  virtual auto AllocateBuffers(size_t num_buffers, size_t capacity) -> Status;
  virtual auto FreeBuffers() -> Status;

  // The allocator used for the buffers.
  std::shared_ptr<buffer::Allocator> allocator_ = nullptr;
  /// The input buffers.
  std::vector<illex::JSONBuffer> buffers_;
  /// The mutexes for the input buffers.
  std::vector<std::mutex> mutexes_;
};

/// \brief Print properties of the buffer in human-readable format.
auto ToString(const illex::JSONBuffer& buffer, bool show_contents = true) -> std::string;

/// \brief Add sequence numbers as schema metadata to a batch.
auto AddSeqAsSchemaMeta(const std::shared_ptr<arrow::RecordBatch>& batch,
                        illex::SeqRange seq_range) -> std::shared_ptr<arrow::RecordBatch>;

/// \brief Return a new schema with the sequence number field added.
auto WithSeqField(const arrow::Schema& schema, std::shared_ptr<arrow::Schema>* output)
    -> Status;

}  // namespace bolson::parse