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

#include "bolson/convert/stats.h"

namespace bolson::convert {

auto Stats::operator+=(const bolson::convert::Stats &r) -> Stats & {
  num_jsons += r.num_jsons;
  num_json_bytes += r.num_json_bytes;
  num_ipc += r.num_ipc;
  total_ipc_bytes += r.total_ipc_bytes;
  parse_time += r.parse_time;
  convert_time += r.convert_time;
  ipc_construct_time += r.ipc_construct_time;
  thread_time += r.thread_time;
  return *this;
}

auto AggrStats(const std::vector<Stats> &conv_stats) -> Stats {
  Stats all_conv_stats;
  for (const auto &t : conv_stats) {
    all_conv_stats += t;
  }
  return all_conv_stats;
}

void LogConvertStats(const Stats &stats, size_t num_threads) {

  // Input stats.
  auto json_MiB = stats.num_json_bytes;

  spdlog::info("JSON to Arrow conversion:");
  spdlog::info("  Converted              : {}", stats.num_jsons);
  spdlog::info("  Raw JSON bytes         : {} MiB", json_MiB);

  // Raw parsing stats.
  auto parse_tt = stats.parse_time / num_threads;
  auto json_MB = stats.num_json_bytes / 1e6;
  auto json_M = stats.num_jsons / 1e6;

  spdlog::info("  JSON parse to Arrow RecordBatch (without sequence numbers)");
  spdlog::info("    Time in {:2} threads   : {} s", num_threads, stats.parse_time);
  spdlog::info("    Avg. time            : {} s", parse_tt);
  spdlog::info("    Avg. throughput      : {} MB/s", json_MB / parse_tt);
  spdlog::info("    Avg. throughput      : {} MJ/s", json_M / parse_tt);

  // Parsing plus seq no's stats.
  auto seq_tt = stats.convert_time / num_threads;

  spdlog::info("  JSON parse to Arrow RecordBatch (with sequence numbers)");
  spdlog::info("    Time in {:2} threads   : {} s", num_threads, stats.convert_time);
  spdlog::info("    Avg. time            : {} s", seq_tt);
  spdlog::info("    Avg. throughput      : {} MB/s", json_MB / seq_tt);
  spdlog::info("    Avg. throughput      : {} MJ/s", json_M / seq_tt);

  // IPC construction
  auto ipc_tt = stats.ipc_construct_time / num_threads;
  auto ipc_bpj = static_cast<double>(stats.total_ipc_bytes) / stats.num_jsons;
  auto ipc_bpi = stats.total_ipc_bytes / stats.num_ipc;

  spdlog::info("  Arrow RecordBatch serialization");
  spdlog::info("    IPC messages         : {}", stats.num_ipc);
  spdlog::info("    IPC bytes            : {}", stats.total_ipc_bytes);
  spdlog::info("    Avg. IPC bytes/json  : {} B / J", ipc_bpj);
  spdlog::info("    Avg. IPC bytes/msg   : {} B / I", ipc_bpi);
  spdlog::info("    Time in {:2} threads   : {} s", num_threads, stats.ipc_construct_time);
  spdlog::info("    Avg. time            : {} s", ipc_tt);
  spdlog::info("    Avg. throughput      : {} MB/s", json_MB / ipc_tt);
  spdlog::info("    Avg. throughput      : {} MJ/s", json_M / ipc_tt);
}

}