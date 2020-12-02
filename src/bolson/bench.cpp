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
#include <thread>

#include <putong/timer.h>
#include <illex/arrow.h>

#include "bolson/bench.h"
#include "bolson/status.h"
#include "bolson/pulsar.h"
#include "bolson/convert/cpu.h"
#include "bolson/convert/opae_battery.h"
#include "bolson/convert/convert.h"

namespace bolson {

auto BenchClient(const ClientBenchOptions &opt) -> Status {
  return Status(Error::GenericError, "Not yet implemented.");
}

auto BenchConvertSingleThread(const ConvertBenchOptions &opt,
                              putong::Timer<> *t,
                              size_t *ipc_size,
                              illex::JSONQueue *json_queue,
                              IpcQueue *ipc_queue) -> Status {
  putong::Timer<> m;
  std::vector<IpcQueueItem> ipc_messages;
  ipc_messages.reserve(opt.num_jsons);

  convert::ArrowBatchBuilder builder(opt.parse_opts);
  t->Start();
  for (size_t i = 0; i < opt.num_jsons; i++) {
    illex::JSONQueueItem json_item;
    json_queue->wait_dequeue(json_item);
    BOLSON_ROE(builder.AppendAsBatch(json_item));
    // Create IPC msg if the threshold is reached or this is the last JSON.
    if ((builder.size() >= opt.batch_threshold) || (i == opt.num_jsons - 1)) {
      IpcQueueItem ipc_msg;
      BOLSON_ROE(builder.Finish(&ipc_msg));
      *ipc_size += ipc_msg.ipc->size();
      ipc_queue->enqueue(ipc_msg);
    }
  }
  t->Stop();

  return Status::OK();
}

auto BenchConvertMultiThread(const ConvertBenchOptions &opt,
                             putong::Timer<> *t,
                             size_t *ipc_size,
                             illex::JSONQueue *json_queue,
                             IpcQueue *ipc_queue) -> Status {
  std::promise<std::vector<convert::Stats>> promise_stats;
  std::atomic<bool> shutdown = false;
  auto future_stats = promise_stats.get_future();
  size_t num_rows = 0;
  IpcQueueItem ipc_item;
  std::thread conversion_thread;

  t->Start();
  switch (opt.conversion) {
    case convert::Impl::CPU:
      conversion_thread = std::thread(convert::ConvertWithCPU,
                                      json_queue,
                                      ipc_queue,
                                      &shutdown,
                                      opt.num_threads,
                                      opt.parse_opts,
                                      opt.read_opts,
                                      opt.json_threshold,
                                      opt.batch_threshold,
                                      std::move(promise_stats));
      break;
    case convert::Impl::OPAE_BATTERY:
      conversion_thread = std::thread(convert::ConvertBatteryWithOPAE,
                                      json_queue,
                                      ipc_queue,
                                      &shutdown,
                                      opt.json_threshold,
                                      opt.batch_threshold,
                                      std::move(promise_stats));
      break;
  }

  // Pull from the output queue as fast as possible to know when we're done.
  while (num_rows != opt.num_jsons) {
    ipc_queue->wait_dequeue(ipc_item);
    num_rows += ipc_item.num_rows;
    *ipc_size += ipc_item.ipc->size();
  }
  t->Stop();
  shutdown.store(true);
  conversion_thread.join();

  auto stats = future_stats.get();

  convert::LogConvertStats(AggrStats(stats), opt.num_threads);

  return Status::OK();
}

static auto GenerateJSONs(putong::Timer<> *g,
                          const ConvertBenchOptions &opt,
                          std::vector<illex::JSONQueueItem> *items) -> size_t {
  // Generate a message with tweets in JSON format.
  auto gen = illex::FromArrowSchema(*opt.schema, opt.generate);

  // Generate the JSONs.
  g->Start();
  items->reserve(opt.num_jsons);
  size_t raw_chars = 0;
  for (size_t i = 0; i < opt.num_jsons; i++) {
    auto json = gen.GetString();
    raw_chars += json.size();
    items->push_back(illex::JSONQueueItem{i, json});
  }
  g->Stop();

  return raw_chars;
}

auto BenchConvert(const ConvertBenchOptions &opt) -> Status {
  putong::Timer<> g, q, c, m;

  if (!opt.csv) {
    spdlog::info("Converting {} JSONs to Arrow IPC", opt.num_jsons);
    spdlog::info("  Arrow Schema: {}", opt.schema->ToString());
  } else {
    std::cout << opt.num_jsons << ",";
  }
  // Generate JSONs
  std::vector<illex::JSONQueueItem> items;
  auto raw_chars = GenerateJSONs(&g, opt, &items);

  if (!opt.csv) {
    spdlog::info("Generated JSONs bytes          : {} MiB",
                 static_cast<double>(raw_chars) / (1024 * 1024));
    spdlog::info("Generate time                  : {} s", g.seconds());
    spdlog::info("Generate throughput            : {} MB/s",
                 static_cast<double>(raw_chars) / g.seconds() * 1E-6);
  } else {
    std::cout << raw_chars << "," << g.seconds() << ",";
  }

  // Measure time to push all queue items into the queue
  illex::JSONQueue json_queue;
  q.Start();
  for (size_t i = 0; i < opt.num_jsons; i++) {
    json_queue.enqueue(items[i]);
  }
  q.Stop();

  auto json_queue_size = raw_chars + sizeof(illex::JSONQueueItem) * opt.num_jsons;

  if (!opt.csv) {
    spdlog::info("JSON Queue bytes               : {} MiB",
                 static_cast<double>(json_queue_size) / (1024 * 1024));
    spdlog::info("JSON Queue fill time           : {} s", q.seconds());
    spdlog::info("JSON Queue fill throughput     : {} MB/s",
                 static_cast<double>(json_queue_size) / q.seconds() * 1E-6);
  } else {
    std::cout << json_queue_size << "," << q.seconds();
  }

  // Measure time to convert JSONs and fill the IPC queue.
  IpcQueue ipc_queue;
  size_t ipc_size = 0;

  //if (opt.num_threads <= 1) {
  //BOLSON_ROE(BenchConvertSingleThread(opt, &c, &ipc_size, &json_queue, &ipc_queue));
  //} else {
  BOLSON_ROE(BenchConvertMultiThread(opt, &c, &ipc_size, &json_queue, &ipc_queue));
  //}

  if (!opt.csv) {
    spdlog::info("IPC bytes                      : {} MiB",
                 static_cast<double>(ipc_size) / (1024 * 1024));
    spdlog::info("IPC Convert time               : {} s", c.seconds());
    spdlog::info("IPC Convert throughput (in)    : {} MB/s",
                 static_cast<double>(json_queue_size) / c.seconds() * 1E-6);
    spdlog::info("IPC Convert throughput (out)   : {} MB/s",
                 static_cast<double>(ipc_size) / c.seconds() * 1E-6);
  } else {
    std::cout << ipc_size << "," << c.seconds() << "," << opt.num_threads << std::endl;
  }

  return Status::OK();
}

auto BenchPulsar(const PulsarBenchOptions &opt) -> Status {
  if (!opt.csv) {
    spdlog::info("Sending {} Pulsar messages of size {} B to topic {}",
                 opt.num_messages,
                 opt.message_size,
                 opt.pulsar.topic);
  }
  // Setup Pulsar context
  PulsarContext pulsar;
  BOLSON_ROE(SetupClientProducer(opt.pulsar.url, opt.pulsar.topic, &pulsar));

  putong::Timer<> t;

  // Allocate some buffer.
  auto *junk = static_cast<uint8_t *>(std::malloc(opt.message_size));
  // Clear the buffer.
  std::memset(junk, 0, opt.message_size);

  // Start a timer.
  t.Start();
  for (int i = 0; i < opt.num_messages; i++) {
    BOLSON_ROE(Publish(pulsar.producer.get(), junk, opt.message_size));
  }
  t.Stop();

  // Free buffer.
  free(junk);

  if (opt.csv) {
    std::cout << opt.num_messages << "," << opt.message_size << "," << t.seconds()
              << std::endl;
  } else {
    // Print stats.
    spdlog::info("  Time   : {} s", t.seconds());
    spdlog::info("  Goodput: {} MB/s",
                 1E-6 * static_cast<double>(opt.num_messages * opt.message_size)
                     / t.seconds());
  }

  return Status::OK();
}

auto RunBench(const BenchOptions &opt) -> Status {
  switch (opt.bench) {
    case Bench::CLIENT:return BenchClient(opt.client);
    case Bench::CONVERT:return BenchConvert(opt.convert);
    case Bench::PULSAR:return BenchPulsar(opt.pulsar);
  }
  return Status::OK();
}

}
