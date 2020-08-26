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

#include <cstdint>
#include <rapidjson/prettywriter.h>
#include <putong/timer.h>

#include "jsongen/log.h"
#include "jsongen/arrow.h"
#include "jsongen/producer.h"

namespace jsongen {

namespace rj = rapidjson;

void ProductionDrone(size_t thread_id,
                     const ProductionOptions &opt,
                     size_t num_items,
                     ProductionQueue *q,
                     std::promise<size_t> &&size) {
  // Accumulator for total number of characters generated.
  size_t drone_size = 0;

  // Generation options. We increment the seed by the thread id, so we get different values from each thread.
  auto gen_opt = opt.gen;
  gen_opt.seed += thread_id;

  // Generate a message with tweets in JSON format.
  auto gen = FromArrowSchema(*opt.schema, gen_opt);
  rapidjson::StringBuffer buffer;

  for (size_t m = 0; m < num_items; m++) {
    auto json = gen.Get();
    buffer.Clear();

    // Check whether we must pretty-prent the JSON
    if (opt.pretty) {
      rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
      writer.SetFormatOptions(rj::PrettyFormatOptions::kFormatSingleLineArray);
      json.Accept(writer);
    } else {
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      json.Accept(writer);
    }

    // Put the JSON string in the queue.
    auto json_str = std::string(buffer.GetString());

    // Check if we need to append whitespace.
    if (opt.whitespace) {
      json_str.push_back(opt.whitespace_char);
    }

    // Accumulate the number of characters this drone has produced.
    drone_size += json_str.size();

    // Place the JSON in the queue.
    if (!q->enqueue(std::move(json_str))) {
      spdlog::error("[Drone {}] Could not place JSON string in queue.", thread_id);
    }
  }

  size.set_value(drone_size);
}

void ProductionHive(const ProductionOptions &opt,
                    ProductionQueue *q,
                    std::promise<ProductionStats> &&stats) {
  putong::Timer t;
  ProductionStats result;

  const auto num_threads = std::thread::hardware_concurrency();
  const auto jsons_per_thread = opt.num_jsons / num_threads;

  spdlog::info("[Hive] Starting {} JSON producer drones.", num_threads);
  t.Start();
  std::vector<std::thread> threads;
  std::vector<std::future<size_t>> futures;
  threads.reserve(num_threads);
  for (int thread = 0; thread < num_threads; thread++) {
    std::promise<size_t> p;
    futures.push_back(p.get_future());
    // Spawn the threads and let the first thread do the remainder of the work.
    threads.emplace_back(ProductionDrone,
                         thread,
                         opt,
                         jsons_per_thread + (thread != 0 ? 0 : opt.num_jsons % jsons_per_thread),
                         q,
                         std::move(p));
  }
  for (auto &thread : threads) {
    thread.join();
  }
  t.Stop();
  size_t total_size = 0;
  for (auto &f : futures) {
    total_size += f.get();
  }
  result.time = t.seconds();
  // Print some stats.
  spdlog::info("[Hive] Drones finished.");
  spdlog::info("Produced {} JSONs in {:.4f} seconds.", opt.num_jsons, result.time);
  spdlog::info("  {:.1f} JSONs/second (avg).", opt.num_jsons / result.time);
  spdlog::info("  {:.2f} gigabits/second (avg).",
               static_cast<double>(total_size * 8) / result.time * 1E-9);
  stats.set_value(result);
}

}
