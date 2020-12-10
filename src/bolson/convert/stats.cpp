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
  t.parse += r.t.parse;
  t.serialize += r.t.serialize;
  t.combine += r.t.combine;
  t.seq += r.t.seq;
  t.thread += r.t.thread;
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

  // Parsing stats.
  auto parse_tt = stats.t.parse / num_threads;
  auto json_MB = stats.num_json_bytes / 1e6;
  auto json_M = stats.num_jsons / 1e6;

  spdlog::info("  JSON parse to Arrow RecordBatch");
  spdlog::info("    Time in {:2} threads   : {} s", num_threads, stats.t.parse);
  spdlog::info("    Avg. time            : {} s", parse_tt);
  spdlog::info("    Avg. throughput      : {} MB/s", json_MB / parse_tt);
  spdlog::info("    Avg. throughput      : {} MJ/s", json_M / parse_tt);

  // Adding sequence number stats.
  auto seq_tt = stats.t.seq / num_threads;

  spdlog::info("  Adding sequence numbers");
  spdlog::info("    Time in {:2} threads   : {} s", num_threads, stats.t.seq);
  spdlog::info("    Avg. time            : {} s", seq_tt);
  spdlog::info("    Avg. throughput      : {} MB/s", json_MB / seq_tt);
  spdlog::info("    Avg. throughput      : {} MJ/s", json_M / seq_tt);

  // IPC stats
  auto ipc_bpj = static_cast<double>(stats.total_ipc_bytes) / stats.num_jsons;
  auto ipc_bpi = stats.num_ipc > 0 ? stats.total_ipc_bytes / stats.num_ipc : 0;

  spdlog::info("  Constructing IPC messages");
  spdlog::info("    IPC messages         : {}", stats.num_ipc);
  spdlog::info("    IPC bytes            : {}", stats.total_ipc_bytes);
  spdlog::info("    Avg. IPC bytes/json  : {} B / J", ipc_bpj);
  spdlog::info("    Avg. IPC bytes/msg   : {} B / I", ipc_bpi);

  // Combining buffered batches
  auto ipc_comb = stats.t.combine / num_threads;

  spdlog::info("    Combining batches:");
  spdlog::info("      Time in {:2} threads   : {} s", num_threads, stats.t.combine);
  spdlog::info("      Avg. time            : {} s", ipc_comb);
  spdlog::info("      Avg. throughput      : {} MB/s", json_MB / ipc_comb);
  spdlog::info("      Avg. throughput      : {} MJ/s", json_M / ipc_comb);

  // Serializing batches
  auto ipc_ser = stats.t.serialize / num_threads;

  spdlog::info("    Serializing batches:");
  spdlog::info("      Time in {:2} threads   : {} s", num_threads, stats.t.serialize);
  spdlog::info("      Avg. time            : {} s", ipc_ser);
  spdlog::info("      Avg. throughput      : {} MB/s", json_MB / ipc_ser);
  spdlog::info("      Avg. throughput      : {} MJ/s", json_M / ipc_ser);
}

}