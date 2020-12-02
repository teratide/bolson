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

#include "bolson/log.h"
#include "bolson/convert/convert.h"

namespace bolson::convert {

void LogConvertStats(const Stats &stats, size_t num_threads) {
  spdlog::info("Conversion stats:");
  spdlog::info("  JSONs converted            : {}", stats.num_jsons);
  spdlog::info("  JSON bytes converted       : {}", stats.num_json_bytes);
  spdlog::info("  JSON parse to Arrow Batch (without sequence numbers)");
  spdlog::info("    {} threads cumulative T   : {} s", num_threads, stats.parse_time);
  spdlog::info("    Per thread average T     : {} s", stats.parse_time / num_threads);
  spdlog::info("    Average TP               : {} MB/s",
               stats.num_json_bytes / (stats.parse_time / num_threads) * 1E-6);

  spdlog::info("  JSON parse to Arrow Batch (with sequence numbers)");
  spdlog::info("    {} threads cumulative T   : {} s", num_threads, stats.convert_time);
  spdlog::info("    Per thread average T     : {} s", stats.convert_time / num_threads);
  spdlog::info("    Average TP               : {} MB/s",
               stats.num_json_bytes / (stats.convert_time / num_threads) * 1E-6);

  spdlog::info("  Avg. bytes/json            : {} B / json",
               static_cast<double>(stats.total_ipc_bytes) / stats.num_jsons);
  spdlog::info("  IPC msgs generated         : {} ipc", stats.num_ipc);
  spdlog::info("  Total IPC bytes            : {} B", stats.total_ipc_bytes);
  spdlog::info("  Avg. IPC bytes/msg         : {} B / ipc",
               stats.total_ipc_bytes / stats.num_ipc);
  spdlog::info("  Avg. IPC cnstr. time       : {} s. / ipc",
               stats.ipc_construct_time / stats.num_ipc);
  spdlog::info("  Avg. conv. time            : {} us. / json",
               1E6 * stats.convert_time / stats.num_jsons);
  spdlog::info("  Avg. thread time           : {} s.", stats.thread_time / num_threads);
}

/// \brief Aggregate statistics for every thread.
auto AggrStats(const std::vector<Stats> &conv_stats) -> Stats {
  Stats all_conv_stats;
  for (const auto &t : conv_stats) {
    // TODO: overload +
    all_conv_stats.num_jsons += t.num_jsons;
    all_conv_stats.num_json_bytes += t.num_json_bytes;
    all_conv_stats.num_ipc += t.num_ipc;
    all_conv_stats.parse_time += t.parse_time;
    all_conv_stats.convert_time += t.convert_time;
    all_conv_stats.thread_time += t.thread_time;
    all_conv_stats.ipc_construct_time += t.ipc_construct_time;
    all_conv_stats.total_ipc_bytes += t.total_ipc_bytes;
  }
  return all_conv_stats;
}

}