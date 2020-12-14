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

#include <type_traits>

#include "bolson/parse/parser.h"
#include "bolson/convert/resizer.h"
#include "bolson/convert/serializer.h"
#include "bolson/status.h"

namespace bolson::convert {

struct Converter {

  explicit Converter(IpcQueue *output_queue,
                     size_t num_buffers = 1,
                     size_t num_threads = 1)
      : output_queue_(output_queue),
        num_buffers_(num_buffers),
        num_threads_(num_threads),
        mutexes(std::vector<std::mutex>(num_buffers)),
        stats(std::vector<Stats>(num_threads)) {}

  /**
   * \brief Allocate the buffers for this convert context.
   * \tparam T              The parser implementation to use.
   * \param num_buffers     The number of buffers to allocate.
   * \param buffer_capacity The capacity of the buffers to allocate.
   * \return A new ConvertContext.
   */
  template<typename T>
  auto AllocateBuffers(size_t capacity) -> Status {
    static_assert(std::is_base_of<parse::Parser, T>::value);

    // Allocate buffers.
    for (size_t b = 0; b < num_buffers_; b++) {
      std::byte *raw = nullptr;
      BOLSON_ROE(T::AllocateBuffer(capacity, &raw));
      illex::RawJSONBuffer buf;
      illex::RawJSONBuffer::Create(raw, capacity, &buf);
      buffers.push_back(buf);
    }

    return Status::OK();
  }

  template<typename T>
  auto FreeBuffers() -> Status {
    static_assert(std::is_base_of<parse::Parser, T>::value);
    for (size_t b = 0; b < num_buffers_; b++) {
      BOLSON_ROE(T::FreeBuffer(buffers[b].mutable_data()));
    }
    return Status::OK();
  }

  void Start(std::atomic<bool> *shutdown);
  auto Stop() -> Status;

  IpcQueue *output_queue_ = nullptr;
  size_t num_threads_ = 1;
  size_t num_buffers_ = 1;
  std::atomic<bool> *shutdown = nullptr;
  std::vector<std::shared_ptr<parse::Parser>> parsers;
  std::vector<convert::Resizer> resizers;
  std::vector<convert::Serializer> serializers;
  std::vector<illex::RawJSONBuffer> buffers;
  std::vector<std::mutex> mutexes;
  std::vector<std::thread> threads;
  std::vector<Stats> stats;
};

}