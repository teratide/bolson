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

#include <fstream>

#include "bolson/log.h"

namespace bolson::convert {

auto Metrics::operator+=(const bolson::convert::Metrics& r) -> Metrics& {
  num_threads += r.num_threads;
  num_jsons_converted += r.num_jsons_converted;
  num_json_bytes_converted += r.num_json_bytes_converted;
  num_ipc += r.num_ipc;
  ipc_bytes += r.ipc_bytes;
  num_buffers_converted += r.num_buffers_converted;
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

std::string Metrics::ToCSV() const {
  std::stringstream ss;

  ss << num_threads << ',' << num_jsons_converted << "," << num_json_bytes_converted
     << "," << num_ipc << "," << ipc_bytes << "," << num_buffers_converted << ","
     << t.parse << "," << t.resize << "," << t.serialize << "," << t.thread << ","
     << t.enqueue << "," << status.ok();
  return ss.str();
}

void LogConvertMetrics(const Metrics& metrics, const std::string& t) {
  // Input metrics.
  auto json_MiB =
      static_cast<double>(metrics.num_json_bytes_converted) / (1024.0 * 1024.0);

  spdlog::info("{}JSON to Arrow conversion:", t);
  spdlog::info("{}  Converted             : {}", t, metrics.num_jsons_converted);
  spdlog::info("{}  Raw JSON bytes        : {} B, {:.3} MiB", t,
               metrics.num_json_bytes_converted, json_MiB);

  // Parsing metrics.
  auto parse_tt = metrics.t.parse / static_cast<double>(metrics.num_threads);
  auto json_MB = static_cast<double>(metrics.num_json_bytes_converted) / 1e6;
  auto json_M = static_cast<double>(metrics.num_jsons_converted) / 1e6;

  spdlog::info("{}Parsing:", t);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, metrics.num_threads,
               metrics.t.parse);
  spdlog::info("{}  Avg. time             : {} s", t, parse_tt);
  spdlog::info("{}  Avg. throughput       : {:.3f} MB/s", t, json_MB / parse_tt);
  spdlog::info("{}  Avg. throughput       : {:.3f} MJ/s", t, json_M / parse_tt);

  // Resizing metrics
  auto resize_tt = metrics.t.resize / static_cast<double>(metrics.num_threads);
  spdlog::info("{}Resizing:", t);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, metrics.num_threads,
               metrics.t.resize);
  spdlog::info("{}  Avg. time             : {} s", t, resize_tt);
  spdlog::info("{}  Avg. throughput       : {:.3f} MJSON/s", t, json_M / resize_tt);
  spdlog::info("{}  Batches (in)          : {}", t, metrics.num_buffers_converted);
  spdlog::info("{}  Batches (out)         : {}", t, metrics.num_ipc);

  // Serializing batches
  auto ipc_bpj = static_cast<double>(metrics.ipc_bytes) /
                 static_cast<double>(metrics.num_jsons_converted);
  auto ipc_bpi = (metrics.num_ipc == 0) ? 0
                                        : (static_cast<double>(metrics.ipc_bytes) /
                                           static_cast<double>(metrics.num_ipc));
  auto ipc_MB = static_cast<double>(metrics.ipc_bytes) / 1e6;
  auto ser_tt = metrics.t.serialize / static_cast<double>(metrics.num_threads);

  spdlog::info("{}Serializing:", t);
  spdlog::info("{}  IPC messages          : {}", t, metrics.num_ipc);
  spdlog::info("{}  IPC bytes             : {}", t, metrics.ipc_bytes);
  spdlog::info("{}  Avg. IPC bytes/json   : {:.1f} B/JSON", t, ipc_bpj);
  spdlog::info("{}  Avg. IPC bytes/msg    : {:.1f} B/IPC", t, ipc_bpi);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, metrics.num_threads,
               metrics.t.serialize);
  spdlog::info("{}  Avg. time             : {} s", t, ser_tt);
  spdlog::info("{}  Avg. throughput (out) : {:.3f} MB/s", t, ipc_MB / ser_tt);
  spdlog::info("{}  Avg. throughput       : {:.3f} MJSON/s", t, json_M / ser_tt);

  // Enqueueing
  auto enq_tt = metrics.t.enqueue / static_cast<double>(metrics.num_threads);
  spdlog::info("{}Enqueueing:", t);
  spdlog::info("{}  Time in {:2} threads    : {} s", t, metrics.num_threads,
               metrics.t.enqueue);
  spdlog::info("{}  Avg. time             : {} s", t, enq_tt);
  spdlog::info("{}  Avg. throughput       : {.3f} MJSON/s", t, json_M / enq_tt);
}

Status SaveConvertMetrics(const std::vector<Metrics>& metrics, const std::string& file) {
  // Open output stream to write file.
  std::ofstream ofs(file);
  if (!ofs.good()) {
    return Status(Error::IOError, "Could not open " + file + " to save metrics.");
  }

  // Header:
  ofs << "num_threads,num_jsons_converted,num_json_bytes_converted,"
         "num_ipc,ipc_bytes,num_buffers_converted,"
         "t_parse,t_resize,t_serialize,t_thread,t_enqueue,status\n";

  for (const auto& m : metrics) {
    ofs << m.ToCSV() << '\n';
  }

  return Status::OK();
}

}  // namespace bolson::convert