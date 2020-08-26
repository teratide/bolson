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

#include <future>
#include <cstdint>
#include <arrow/api.h>
#include <concurrentqueue.h>

#include "jsongen/document.h"

namespace jsongen {

using ProductionQueue = moodycamel::ConcurrentQueue<std::string>;

/// Options for the random JSON production facility.
struct ProductionOptions {
  /// Random generation options.
  GenerateOptions gen;
  /// The Arrow schema to base the JSONs on.
  std::shared_ptr<arrow::Schema> schema;
  /// The number of JSONs to produce.
  size_t num_jsons;
  /// Whether to pretty-print the JSONs.
  bool pretty = false;
  /// Number of production threads to spawn.
  size_t num_threads = std::thread::hardware_concurrency();
  /// Whether to insert a whitespace after a JSON.
  bool whitespace = true;
  /// The whitespace character to insert.
  char whitespace_char = '\n';
};

/// Statistics on JSON production.
struct ProductionStats {
  /// The time spent producing all JSONs
  double time = 0.0;
};

/**
 * \brief A production drone thread. Produces JSONs into a queue.
 * \param thread_id The id of this thread.
 * \param opt The options this thread must use to produce.
 * \param num_items The number of JSONs it has to produce.
 * \param[out] size
 */
void ProductionDrone(size_t thread_id,
                     const ProductionOptions &opt,
                     size_t num_items,
                     ProductionQueue *q,
                     std::promise<size_t> &&size);

/**
 * \brief Production hive thread. Spawns JSON production drones.
 * \param opt Options for the production drones.
 * \param q The concurrent queue to operate on.
 * \param stats Statistics about the hive and drone thread(s).
 */
void ProductionHive(const ProductionOptions &opt,
                    ProductionQueue *q,
                    std::promise<ProductionStats> &&stats);

}