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

#include <iostream>

#include "./log.h"
#include "./opae_allocator.h"
#include "./opae_battery_impl.h"

const int num_parsers = 8;
const int num_jsons = 2;

int main(int argc, char **argv) {
  OpaeAllocator alloc;

  std::vector<RawJSONBuffer> buffers(num_parsers);
  std::vector<RawJSONBuffer *> buffers_ptr(num_parsers);
  std::vector<ParsedBuffer> outputs(num_parsers);

  // Create buffer wrappers, allocate buffers, copy test string into buffers.

  for (int i = 0; i < num_parsers; i++) {
    std::string test_string;
    for (int j = 0; j < (i + 1) * num_jsons; j++) {
      test_string += "{\"voltage\":[";
      for (int k = 0; k < i * j / 4 + j + 1; k++) {
        test_string += std::to_string(k);
        if (k != i * j / 4 + j) { test_string += ","; }
      }
      test_string += "]}\n";
    }
    size_t num_bytes = test_string.length();
    std::cout << "JSON " << i << ", len: " << num_bytes << " = " << test_string;
    alloc.Allocate(opae_fixed_capacity, &buffers[i].data_);
    std::memcpy(buffers[i].data_, test_string.data(), num_bytes);
    buffers[i].size_ = num_bytes;
    buffers[i].capacity_ = opae_fixed_capacity;
    buffers_ptr[i] = &buffers[i];
  }

  // Set up manager.
  std::shared_ptr<OpaeBatteryParserManager> m;
  OpaeBatteryParserManager::Make(OpaeBatteryOptions{},
                                 buffers_ptr,
                                 num_parsers,
                                 &m);

  // Parse buffer with every parser.
  auto parsers = m->parsers();
  for (int i = 0; i < num_parsers; i++) {
    parsers[i]->Parse(&buffers[i], &outputs[i]);
    std::cout << outputs[i].batch->ToString() << std::endl;
  }

  return 0;
}

