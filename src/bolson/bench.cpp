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

#include <blockingconcurrentqueue.h>
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
    case convert::Impl::CPU: {
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
    }
    case convert::Impl::OPAE_BATTERY: {
      conversion_thread = std::thread(convert::ConvertBatteryWithOPAE,
                                      opt.json_threshold,
                                      opt.batch_threshold,
                                      json_queue,
                                      ipc_queue,
                                      &shutdown,
                                      std::move(promise_stats));
      break;
    }
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

using Queue = moodycamel::BlockingConcurrentQueue<uint8_t>;
using QueueTimers = std::vector<putong::SplitTimer<2>>;

// Thread to dequeue
static void Dequeue(const QueueBenchOptions &opt, Queue *queue, QueueTimers *timers) {
  uint64_t o;
  for (size_t i = 0; i < opt.num_items; i++) {
    queue->wait_dequeue(o);
    (*timers)[i].Split();
  }
}

auto BenchQueue(const QueueBenchOptions &opt) -> Status {
  // Make a queue
  Queue queue;
  // Make timers.
  std::vector<putong::SplitTimer<2>> timers(opt.num_items);

  auto deq_thread = std::thread(Dequeue, opt, &queue, &timers);

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  for (size_t i = 0; i < opt.num_items; i++) {
    timers[i].Start();
    queue.enqueue(static_cast<uint64_t>(i));
    timers[i].Split();
  }

  deq_thread.join();

  size_t i = 0;
  std::cout << "Item,Enqueue,Dequeue" << std::endl;
  for (const auto &t: timers) {
    std::cout << i << ",";
    i++;
    std::cout << std::setprecision(9) << std::fixed << t.seconds()[0] << ",";
    std::cout << std::setprecision(9) << std::fixed << t.seconds()[1];
    std::cout << std::endl;
  }

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

static auto QueueJSONs(putong::Timer<> *q,
                       const std::vector<illex::JSONQueueItem> &json_items,
                       illex::JSONQueue *json_queue) {
  // Measure time to push all queue items into the queue
  q->Start();
  for (const auto &json_item : json_items) {
    json_queue->enqueue(json_item);
  }
  q->Stop();
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
  auto raw_json_bytes = GenerateJSONs(&g, opt, &items);

  // Queue JSONs
  illex::JSONQueue json_queue;
  QueueJSONs(&q, items, &json_queue);
  auto json_queue_size = raw_json_bytes + sizeof(illex::JSONQueueItem) * opt.num_jsons;

  // Measure time to convert JSONs and fill the IPC queue.
  IpcQueue ipc_queue;
  size_t ipc_size = 0;

  BOLSON_ROE(BenchConvertMultiThread(opt, &c, &ipc_size, &json_queue, &ipc_queue));

  // Print all statistics:
  if (!opt.csv) {
    spdlog::info("Generated JSONs bytes          : {} MiB",
                 static_cast<double>(raw_json_bytes) / (1024 * 1024));
    spdlog::info("Generate time                  : {} s", g.seconds());
    spdlog::info("Generate throughput            : {} MB/s",
                 static_cast<double>(raw_json_bytes) / g.seconds() * 1E-6);
  } else {
    std::cout << raw_json_bytes << "," << g.seconds() << ",";
  }

  if (!opt.csv) {
    spdlog::info("JSON Queue bytes               : {} MiB",
                 static_cast<double>(json_queue_size) / (1024 * 1024));
    spdlog::info("JSON Queue fill time           : {} s", q.seconds());
    spdlog::info("JSON Queue fill throughput     : {} MB/s",
                 static_cast<double>(json_queue_size) / q.seconds() * 1E-6);
  } else {
    std::cout << json_queue_size << "," << q.seconds();
  }

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
    case Bench::CLIENT: return BenchClient(opt.client);
    case Bench::CONVERT: return BenchConvert(opt.convert);
    case Bench::PULSAR: return BenchPulsar(opt.pulsar);
    case Bench::QUEUE: return BenchQueue(opt.queue);
  }
  return Status::OK();
}

}
