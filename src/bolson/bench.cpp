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

#include <iostream>
#include <putong/timer.h>

#include "bolson/bench.h"
#include "bolson/status.h"
#include "bolson/pulsar.h"

namespace bolson {

auto BenchPulsar(size_t repeats, size_t message_size, const PulsarOptions &opt, bool csv) -> Status {
  // Setup Pulsar context
  PulsarContext pulsar;
  BOLSON_ROE(SetupClientProducer(opt.url, opt.topic, &pulsar));

  putong::Timer<> t;

  // Allocate some buffer.
  auto *junk = static_cast<uint8_t *>(std::malloc(message_size));
  // Clear the buffer.
  std::memset(junk, 0, message_size);

  // Start a timer.
  t.Start();
  for (int i = 0; i < repeats; i++) {
    BOLSON_ROE(Publish(pulsar.producer.get(), junk, message_size));
  }
  t.Stop();

  // Free buffer.
  free(junk);

  if (csv) {
    std::cout << repeats << "," << message_size << "," << t.seconds() << std::endl;
  } else {
    // Print stats.
    spdlog::info("Pulsar {} messages of size {} B", repeats, message_size);
    spdlog::info("  Time   : {} s", t.seconds());
    spdlog::info("  Goodput: {} MB/s", 1E-6 * static_cast<double>(repeats * message_size) / t.seconds());
  }

  return Status::OK();
}

auto RunBench(const BenchOptions &opts) -> Status {
  return BenchPulsar(opts.pulsar_messages, opts.pulsar_message_size, opts.pulsar, opts.csv);
}

}
