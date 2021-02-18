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

#include "bolson/publish/bench.h"

#include <chrono>

#include "bolson/publish/publisher.h"

namespace bolson::publish {

auto BenchPulsar(const BenchOptions& opt) -> Status {
  spdlog::info("Initializing publisher...");
  IpcQueue queue;
  std::atomic<size_t> row_count = 0;
  std::atomic<bool> shutdown = false;
  std::shared_ptr<ConcurrentPublisher> publisher;
  BOLSON_ROE(ConcurrentPublisher::Make(opt.pulsar, &queue, &row_count, &publisher));

  spdlog::info("Preparing {} messages of size {} ...", opt.num_messages,
               opt.message_size);
  // Construct a message buffer with A's in it.
  auto* junk = static_cast<uint8_t*>(std::malloc(opt.message_size));
  std::memset(junk, 'A', opt.message_size);
  auto buffer = arrow::Buffer::Wrap(junk, opt.message_size);

  // Fill the queue with messages.
  for (int i = 0; i < opt.num_messages; i++) {
    IpcQueueItem item;
    item.message = buffer;
    queue.enqueue(std::move(item));
  }

  spdlog::info("Starting publisher...");
  putong::Timer<> t(true);
  publisher->Start(&shutdown);

  // Poll publish count until finished.
  size_t published = 0;
  do {
    published = row_count.load();
#ifndef NDEBUG
    spdlog::info("{}/{}", published, opt.num_messages);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
#else
    std::this_thread::sleep_for(std::chrono::microseconds(100));
#endif
  } while (published != opt.num_messages);
  t.Stop();
  shutdown.store(true);
  // Stop publisher.
  BOLSON_ROE(Aggregate(publisher->Finish()));
  spdlog::info("Done.");

  // Log options and metrics:
  opt.pulsar.Log();

  auto MB = 1E-6 * static_cast<double>(opt.num_messages * opt.message_size);
  spdlog::info("Time                      : {} s", t.seconds());
  spdlog::info("Goodput                   : {} MB/s", MB / t.seconds());

  auto metrics = Aggregate(publisher->metrics());

  // Calculate an average.
  size_t lat_total = 0;
  for (const auto& l : metrics.latencies) {
    lat_total += l.time.GetDiff<std::chrono::nanoseconds>(TimePoints::published);
  }
  auto lat_avg = static_cast<double>(lat_total) / published * 1e-6;
  spdlog::info("Avg. latency              : {:.3f} ms", lat_avg);

  // Save latency metrics
  if (!opt.latency_file.empty()) {
    SaveLatencyMetrics(metrics.latencies, opt.latency_file, TimePoints::published,
                       TimePoints::published, false);
  }

  free(junk);

  return Status::OK();
}

void AddPublishBenchToCLI(CLI::App* sub, BenchOptions* out) {
  sub->add_option("-s", out->message_size, "Pulsar message size.")
      ->default_val(BOLSON_DEFAULT_PULSAR_MAX_MSG_SIZE);
  sub->add_option("-n", out->num_messages, "Pulsar number of messages.")->default_val(1);
  sub->add_option("-l", out->latency_file, "File to write latency measurements to.");
  AddPublishOptsToCLI(sub, &out->pulsar);
}

}  // namespace bolson::publish