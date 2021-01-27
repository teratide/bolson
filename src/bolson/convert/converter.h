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

#include "bolson/buffer/allocator.h"
#include "bolson/parse/parser.h"
#include "bolson/parse/arrow_impl.h"
#include "bolson/parse/opae_battery_impl.h"
#include "bolson/convert/resizer.h"
#include "bolson/convert/serializer.h"
#include "bolson/status.h"

/// Contains all constructs to support JSON to Arrow conversion and serialization.
namespace bolson::convert {

/**
 * \brief Converter options.
 */
struct Options {
  /// Maximum size of an IPC message.
  size_t max_ipc_size = 0;
  /// Maximum number of rows in a RecordBatch.
  size_t max_batch_rows = 0;
  /// Number of threads.
  size_t num_threads = 1;
  /// Number of buffers.
  std::optional<size_t> num_buffers = std::nullopt;
  /// Capacity for each buffer.
  size_t buf_capacity = 0;
  /// Parser implementation to use.
  parse::Impl implementation = parse::Impl::ARROW;
  /// Options for Arrow built-in parser implementation.
  parse::ArrowOptions arrow;
  /// Options for Opae Battery parser implementation.
  parse::OpaeBatteryOptions opae_battery;
};

/**
 * \brief Converter for JSON to Arrow IPC messages.
 *
 * When started, this unit spawns multiple threads performing the conversion
 * simultaneously.
 */
class Converter {
 public:
  /**
   * \brief Construct a converter instance.
   *
   * Allocates buffers that should be accessed only by obtaining a lock using mutexes().
   *
   * \param opts        Conversion options.
   * \param ipc_queue   The IPC queue to push converted batches to.
   * \param out         A pointer to a shared pointer to store the converter.
   * \return Status::OK() if successful, some error otherwise.
   */
  static auto Make(const Options &opts,
                   IpcQueue *ipc_queue,
                   std::shared_ptr<Converter> *out) -> Status;

  /**
   * \brief Start the converter, spawning the supplied number of converter threads.
   * \param shutdown Shutdown signal.
   */
  void Start(std::atomic<bool> *shutdown);

  /// \brief Stop the converter, joining all converter threads.
  auto Finish() -> Status;

  /// \brief Return a pointer to the buffers.
  auto mutable_buffers() -> std::vector<illex::RawJSONBuffer *>;

  /// \brief Return a pointer to the buffers.
  auto mutexes() -> std::vector<std::mutex *>;

  /// \brief Lock all mutexes of all buffers.
  void LockBuffers();

  /// brief Lock all mutexes of all buffers.
  void UnlockBuffers();

  /// \brief Return converter statistics.
  auto Statistics() -> std::vector<Stats>;

 private:
  Converter(IpcQueue *output_queue,
            buffer::Allocator *allocator,
            size_t num_buffers = 1,
            size_t num_threads = 1)
      : output_queue_(output_queue),
        allocator_(allocator),
        num_buffers_(num_buffers),
        num_threads_(num_threads),
        mutexes_(std::vector<std::mutex>(num_buffers)),
        stats(std::vector<Stats>(num_threads)) {}

  /**
   * \brief Allocate all buffers using the supplied allocator.
   * \param capacity The capacity of the buffers to allocate.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto AllocateBuffers(size_t capacity) -> Status;

  /**
   * \brief Free the buffers for this converter.
   * \return Status::OK() if successful, some error otherwise.
   */
  auto FreeBuffers() -> Status;

  IpcQueue *output_queue_ = nullptr;
  buffer::Allocator *allocator_ = nullptr;
  size_t num_threads_ = 1;
  size_t num_buffers_ = 1;
  std::atomic<bool> *shutdown = nullptr;
  std::vector<std::shared_ptr<parse::Parser>> parsers;
  std::vector<convert::Resizer> resizers;
  std::vector<convert::Serializer> serializers;
  std::vector<illex::RawJSONBuffer> buffers;
  std::vector<std::mutex> mutexes_;
  std::vector<std::thread> threads;
  std::vector<Stats> stats;

  // TODO: work-around for OPAE because it needs a manager.
  //  This should be abstracted.
  std::shared_ptr<parse::OpaeBatteryParserManager> opae_battery_manager;
};

}