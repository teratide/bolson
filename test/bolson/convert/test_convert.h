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
#include <arrow/io/api.h>
#include <illex/client_queued.h>

#include <memory>

#include "bolson/bench.h"
#include "bolson/convert/converter.h"
#include "bolson/convert/test_convert.h"
#include "bolson/log.h"

namespace bolson::convert {

#define FAIL_ON_ERROR(status)                   \
  {                                             \
    auto __status = (status);                   \
    if (!__status.ok()) {                       \
      throw std::runtime_error(__status.msg()); \
    }                                           \
  }

/// \brief Deserialize an Arrow RecordBatch given a schema and a buffer.
auto GetRecordBatch(const std::shared_ptr<arrow::Schema>& schema,
                    const std::shared_ptr<arrow::Buffer>& buffer)
    -> std::shared_ptr<arrow::RecordBatch> {
  auto stream = std::dynamic_pointer_cast<arrow::io::InputStream>(
      std::make_shared<arrow::io::BufferReader>(buffer));
  assert(stream != nullptr);
  auto batch = arrow::ipc::ReadRecordBatch(
                   schema, nullptr, arrow::ipc::IpcReadOptions::Defaults(), stream.get())
                   .ValueOrDie();
  return batch;
}

/// \brief Convert a bunch of JSONs to Arrow IPC messages with given options.
auto Convert(const Options& opts, const std::vector<illex::JSONQueueItem>& in,
             std::vector<IpcQueueItem>* out) -> Status {
  IpcQueue out_queue;
  std::shared_ptr<Converter> arrow_conv;
  BOLSON_ROE(Converter::Make(opts, &out_queue, &arrow_conv));
  FillBuffers(arrow_conv->mutable_buffers(), in);
  std::atomic<bool> arrow_shutdown = false;
  arrow_conv->Start(&arrow_shutdown);

  size_t arrow_rows = 0;
  while ((arrow_rows != in.size()) && (arrow_shutdown.load() == false)) {
    IpcQueueItem item;
    out_queue.wait_dequeue(item);
    arrow_rows += RecordSizeOf(item);
    out->push_back(item);
  }
  arrow_shutdown.store(true);
  BOLSON_ROE(arrow_conv->Finish());
  return Status::OK();
}

}  // namespace bolson::convert