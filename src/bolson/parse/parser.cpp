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

#include "bolson/parse/parser.h"

#include "bolson/status.h"

namespace bolson::parse {

auto ToString(const illex::JSONBuffer& buffer, bool show_contents) -> std::string {
  std::stringstream ss;
  ss << "Buffer    : " << buffer.data() << "\n"
     << "Capacity  : " << buffer.capacity() << "\n"
     << "Size      : " << buffer.size() << "\n"
     << "JSON data : " << buffer.num_jsons();
  if (show_contents) {
    ss << "\n";
    ss << std::string_view(reinterpret_cast<const char*>(buffer.data()), buffer.size());
  }
  return ss.str();
}

auto AddSeqAsSchemaMeta(const std::shared_ptr<arrow::RecordBatch>& batch,
                        illex::SeqRange seq_range)
    -> std::shared_ptr<arrow::RecordBatch> {
  auto additional_meta = arrow::key_value_metadata(
      {"bolson_seq_first", "bolson_seq_last"},
      {std::to_string(seq_range.first), std::to_string(seq_range.last)});
  auto current_meta = batch->schema()->metadata();
  if (current_meta != nullptr) {
    auto new_meta = current_meta->Merge(*additional_meta);
    return batch->ReplaceSchemaMetadata(new_meta);
  } else {
    return batch->ReplaceSchemaMetadata(additional_meta);
  }
}

auto ParserContext::AllocateBuffers(size_t num_buffers, size_t capacity) -> Status {
  // Sanity check.
  if (allocator_ == nullptr) {
    return Status(Error::GenericError,
                  "Parser context has no allocator to allocate buffers.");
  }
  // Allocate all buffers and create mutexes.
  for (size_t b = 0; b < num_buffers; b++) {
    std::byte* raw = nullptr;
    BOLSON_ROE(allocator_->Allocate(capacity, &raw));
    illex::JSONBuffer buf;
    BILLEX_ROE(illex::JSONBuffer::Create(raw, capacity, &buf));
    buffers_.push_back(buf);
  }

  mutexes_ = std::vector<std::mutex>(num_buffers);

  return Status::OK();
}

auto ParserContext::FreeBuffers() -> Status {
  // Sanity check.
  if (allocator_ == nullptr) {
    return Status(Error::GenericError,
                  "Parser context has no allocator to free buffers.");
  }
  // Free all buffers.
  for (auto& buffer : buffers_) {
    BOLSON_ROE(allocator_->Free(buffer.mutable_data()));
  }
  return Status::OK();
}

void ParserContext::LockBuffers() {
  for (auto& mutex : mutexes_) {
    mutex.lock();
  }
}

void ParserContext::UnlockBuffers() {
  for (auto& mutex : mutexes_) {
    mutex.unlock();
  }
}

auto ParserContext::mutable_buffers() -> std::vector<illex::JSONBuffer*> {
  return ToPointers(buffers_);
}

auto ParserContext::mutexes() -> std::vector<std::mutex*> { return ToPointers(mutexes_); }

}  // namespace bolson::parse
