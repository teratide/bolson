// Copyright 2021 Teratide B.V.
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

#include "./opae_allocator.h"
#include "./opae_battery_impl.h"

#ifdef NDEBUG
#define PLATFORM "opae"
#else
#define PLATFORM "opae-ase"
#endif

const char *test_string = "{\"voltage\" : [0, 1, 2, 3]}";

const int num_parsers = 8;

int main(int argc, char **argv) {
  OpaeAllocator alloc;
  size_t num_bytes = std::strlen(test_string);

  std::vector<RawJSONBuffer> raw_buffers;
  std::vector<RawJSONBuffer *> raw_buffers_ptr;
  std::vector<ParsedBuffer> outputs;

  // Create buffer wrappers, allocate buffers, copy test string into buffers.
  for (int i = 0; i < num_parsers; i++) {
    RawJSONBuffer buf;
    alloc.Allocate(num_bytes, &buf.data_);
    std::memcpy(buf.data_, test_string, num_bytes);
    buf.size_ = num_bytes;
    buf.capacity_ = num_bytes;
    raw_buffers.push_back(buf);
    raw_buffers_ptr.push_back(&raw_buffers[i]);
  }

  // Set up manager.
  std::shared_ptr<OpaeBatteryParserManager> m;
  OpaeBatteryParserManager::Make(OpaeBatteryOptions{},
                                 raw_buffers_ptr,
                                 num_parsers,
                                 &m);

  // Parse buffer with every parser.
  auto parsers = m->parsers();
  for (int i = 0; i < num_parsers; i++) {
    parsers[i]->Parse(&raw_buffers[i], &outputs[i]);
  }

  return 0;
}
