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

#include <atomic>
#include <optional>
#include <utility>

#include "bolson/buffer/allocator.h"
#include "bolson/convert/metrics.h"
#include "bolson/convert/resizer.h"
#include "bolson/convert/serializer.h"
#include "bolson/parse/arrow.h"
#include "bolson/parse/implementations.h"
#include "bolson/parse/opae/battery.h"
#include "bolson/parse/opae/trip.h"
#include "bolson/parse/parser.h"
#include "bolson/publish/publisher.h"
#include "bolson/status.h"

/// Contains all constructs to support JSON to Arrow conversion and serialization.
namespace bolson::convert {

/**
 * \brief Converter options.
 */
struct ConverterOptions {
  /// Number of desired converter threads.
  /// May not be possible, depending on implementation.
  size_t num_threads = 1;
  /// Maximum size of an IPC message in bytes.
  size_t max_ipc_size = 0;
  /// Maximum number of rows in a RecordBatch.
  size_t max_batch_rows = 0;

  /// Use a no-op resizer.
  bool mock_resize = false;
  /// Use a no-op serializer;
  bool mock_serialize = false;

  /// Parser options.
  parse::ParserOptions parser;
};

/**
 * \brief Converter for JSON to Arrow IPC messages.
 *
 * The converter spawns at least one (but typically multiple) thread(s) after
 * calling Start(). These threads produce IPC messages and push them onto the output
 * queue whenever data appears in input buffers.
 *
 * The converter can be stopped by storing "false" in the shutdown signal, and calling
 * Finish(). After the converter is finished, conversion metrics may be extracted by
 * calling metrics(), where each metric in the output vector are the metrics for each
 * converter thread.
 */
class Converter {
 public:
  /**
   * \brief Construct an AllToOneConverter instance.
   *
   * \param opts        Conversion options.
   * \param ipc_queue   The IPC queue to push converted batches to.
   * \param out         A pointer to a shared pointer to store the converter.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Make(const ConverterOptions& opts, publish::IpcQueue* ipc_queue,
                   std::shared_ptr<Converter>* out) -> Status;

  /**
   * \brief Start the converter (non-blocking).
   * \param shutdown Shutdown signal.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto Start(std::atomic<bool>* shutdown) -> Status;

  /**
   * \brief Stop the converter, joining all converter threads.
   *
   * If the shutdown signal supplied to Start() was not set to true, this function will
   * block until the shutdown signal is set to true.
   */
  auto Finish() -> MultiThreadStatus;

  /// \brief Return the parser context.
  [[nodiscard]] auto parser_context() const -> std::shared_ptr<parse::ParserContext>;

  /// \brief Return converter metrics.
  [[nodiscard]] auto metrics() const -> std::vector<Metrics>;

 protected:
  /// Converter constructor.
  Converter(std::shared_ptr<parse::ParserContext> parser_context,
            std::vector<std::shared_ptr<convert::Resizer>> resizers,
            std::vector<std::shared_ptr<convert::Serializer>> serializers,
            publish::IpcQueue* output_queue, size_t num_threads = 1);

  /// The output queue.
  publish::IpcQueue* output_queue_ = nullptr;
  /// Shutdown signal.
  std::atomic<bool>* shutdown_ = nullptr;
  /// Number of threads.
  size_t num_threads_ = 1;
  /// Converter threads.
  std::vector<std::thread> threads_;
  /// Parser manager implementations.
  std::shared_ptr<parse::ParserContext> parser_context_;
  /// Resizer instances.
  std::vector<std::shared_ptr<convert::Resizer>> resizers_;
  /// Serializer instances.
  std::vector<std::shared_ptr<convert::Serializer>> serializers_;
  /// Metrics of converter thread(s).
  std::vector<Metrics> metrics_;
  /// Metrics futures of running threads.
  std::vector<std::future<Metrics>> metrics_futures_;
};

}  // namespace bolson::convert