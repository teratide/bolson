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

#include "bolson/convert/metrics.h"

#include "bolson/log.h"

namespace bolson::convert {

auto Metrics::operator+=(const bolson::convert::Metrics& r) -> Metrics& {
  num_jsons += r.num_jsons;
  json_bytes += r.json_bytes;
  num_ipc += r.num_ipc;
  ipc_bytes += r.ipc_bytes;
  num_parsed += r.num_parsed;
  t.parse += r.t.parse;
  t.resize += r.t.resize;
  t.serialize += r.t.serialize;
  t.thread += r.t.thread;
  t.enqueue += r.t.enqueue;
  if (!r.status.ok()) {
    status = r.status;
  }
  return *this;
}

void LogConvertMetrics(const Metrics& stats, size_t num_threads, const std::string& t) {
  // Input stats.
  auto json_MiB = static_cast<double>(stats.json_bytes) / (1024 * 1024);

  spdlog::info("{}JSON to Arrow conversion:", t);
  spdlog::info("{}  Converted             : {}", t, stats.num_jsons);
  spdlog::info("{}  Raw JSON bytes        : {} MiB", t, json_MiB);

  // Parsing stats.
  auto parse_tt = stats.t.parse / num_threads;
  auto json_MB = stats.json_bytes / 1e6;
  auto json_M = stats.num_jsons / 1e6;

  spdlog::info("{}Parsing:", t);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, num_threads, stats.t.parse);
  spdlog::info("{}  Avg. time             : {} s", t, parse_tt);
  spdlog::info("{}  Avg. throughput       : {} MB/s", t, json_MB / parse_tt);
  spdlog::info("{}  Avg. throughput       : {} MJ/s", t, json_M / parse_tt);

  // Resizing stats
  auto resize_tt = stats.t.resize / num_threads;
  spdlog::info("{}Resizing:", t);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, num_threads, stats.t.resize);
  spdlog::info("{}  Avg. time             : {} s", t, resize_tt);
  spdlog::info("{}  Avg. throughput       : {} MJ/s", t, json_M / resize_tt);
  spdlog::info("{}  Batches (in)          : {}", t, stats.num_parsed);
  spdlog::info("{}  Batches (out)         : {}", t, stats.num_ipc);

  // Serializing batches
  auto ipc_bpj = static_cast<double>(stats.ipc_bytes) / stats.num_jsons;
  auto ipc_bpi = (stats.num_ipc > 0) ? (stats.ipc_bytes / stats.num_ipc) : 0;
  auto ipc_MB = static_cast<double>(stats.ipc_bytes) / 1e6;
  auto ser_tt = stats.t.serialize / num_threads;

  spdlog::info("{}Serializing:", t);
  spdlog::info("{}  IPC messages          : {}", t, stats.num_ipc);
  spdlog::info("{}  IPC bytes             : {}", t, stats.ipc_bytes);
  spdlog::info("{}  Avg. IPC bytes/json   : {} B/J", t, ipc_bpj);
  spdlog::info("{}  Avg. IPC bytes/msg    : {} B/I", t, ipc_bpi);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, num_threads, stats.t.serialize);
  spdlog::info("{}  Avg. time             : {} s", t, ser_tt);
  spdlog::info("{}  Avg. throughput (out) : {} MB/s", t, ipc_MB / ser_tt);
  spdlog::info("{}  Avg. throughput       : {} MJ/s", t, json_M / ser_tt);

  // Enqueueing
  auto enq_tt = stats.t.enqueue / num_threads;
  spdlog::info("{}Enqueueing:", t);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, num_threads, stats.t.enqueue);
  spdlog::info("{}  Avg. time             : {} s", t, enq_tt);
  spdlog::info("{}  Avg. throughput       : {} MJ/s", t, json_M / enq_tt);
}

}  // namespace bolson::convert