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
#include <illex/latency.h>
#include <illex/protocol.h>
#include <illex/client_queued.h>
#include <illex/client_buffered.h>

#include "bolson/log.h"
#include "bolson/status.h"
#include "bolson/pulsar.h"
#include "bolson/convert/stats.h"

/// Contains all constructs to parse JSONs.
namespace bolson::parse {

/**
 * \brief The result of parsing a raw JSON buffer.
 */
struct ParsedBuffer {
  /// The resulting Arrow RecordBatch
  std::shared_ptr<arrow::RecordBatch> batch;
  /// The number of bytes parsed.
  size_t parsed_bytes = 0;
  /// The sequence number.
  illex::Seq seq = 0;
};

/**
 * \brief Abstract class for implementations of parsing JSONs to Arrow RecordBatch.
 */
class Parser {
 public:
  /**
   * \brief Parse a buffer containing raw JSON data.
   * \param in  The buffer with the raw JSON data.
   * \param out The buffer with the parsed data represented as Arrow RecordBatch.
   * \return Status::OK() if successful, some error otherwise.
   */
  virtual auto Parse(illex::RawJSONBuffer *in, ParsedBuffer *out) -> Status = 0;
};

/// Available parser implementations.
enum class Impl {
  ARROW,        ///< A CPU version based on Arrow's internal JSON parser using RapidJSON.
  OPAE_BATTERY  ///< An FPGA version for only one specific schema.
};

/// \brief Convert an implementation enum to a human-readable string.
auto ToString(const Impl &impl) -> std::string;

}