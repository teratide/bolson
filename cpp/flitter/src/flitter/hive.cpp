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

#include <chrono>
#include <atomic>
#include <concurrentqueue.h>
#include <flitter/log.h>

#include "./utils.h"
#include "./hive.h"

namespace flitter {

//TODO(johanpel): make this not global
static struct ObjectQueue {
  moodycamel::ConcurrentQueue<std::string> json_queue;
  moodycamel::ConcurrentQueue<std::shared_ptr<arrow::RecordBatch>> batch_queue;
  std::atomic<size_t> produced = 0;
  std::atomic<size_t> consumed = 0;
  bool stop = false;
  // remove these:
  Timer timer;
  double t_msg = 0.;
  double t_q2q = 0.;
} docs;

Drone::Drone(int drone_id, size_t max_size, ReservationSpec reservation_spec)
    : id(drone_id), max_size(max_size), builder(TweetsBuilder(reservation_spec)) {
//  id_val.reserve(reservation_spec.tweets);
//  created_at_off.reserve(reservation_spec.tweets + 1); // + 1 for final offset
//  created_at_val.reserve(reservation_spec.tweets * DATE_LENGTH);
//  text_off.reserve(reservation_spec.tweets + 1);
//  text_val.reserve(reservation_spec.tweets * reservation_spec.text);
//  author_id_val.reserve(reservation_spec.tweets);
//  in_reply_to_user_id_val.reserve(reservation_spec.tweets);
//  ref_tweets_type_val.reserve(reservation_spec.tweets * reservation_spec.refs);
//  ref_tweets_id_val.reserve(reservation_spec.tweets * reservation_spec.refs);
//  ref_tweets_off.reserve(reservation_spec.tweets + 1);
//  created_at_off.push_back(0);
//  created_at_off_current = 0;
//  text_off.push_back(0);
//  text_off_current = 0;
//  ref_tweets_off.push_back(0);
//  ref_tweets_off_current = 0;
}

void drone_thread(Drone *drone) {
  SPDLOG_DEBUG("[Drone {}] Started.", drone->id);
  Timer t;
  // Continue to try and pull until the global stop signal is given.
  while (!docs.stop) {
    size_t popped = 0;
    // Make several attempts to pull before synchronizing with the total consumed counter.
    for (int i = 0; (i < drone->attempts_before_sync) && (drone->builder.size() < drone->max_size); i++) {
      std::string data;
      if (docs.json_queue.try_dequeue(data)) {
        t.Start();
        Tweet parsed_tweet;
        rapidjson::Document json_doc;
        json_doc.ParseInsitu(data.data());
        ParseTweet(json_doc["tweets"].GetArray()[0].GetObject(), &parsed_tweet);
        drone->builder.Append(parsed_tweet);
        SPDLOG_DEBUG("[Worker {}] popped: {}", drone->id, parsed_tweet.ToString());
        popped++;
      }
    }
    // Check if we can finish the builder.
    if (docs.stop || (drone->builder.size() >= drone->max_size)) {
      if (drone->builder.rows() > 0) {
        SPDLOG_DEBUG("[Drone {}] Finalizing builder.", drone->id);
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = drone->builder.Finish(&batch);
        drone->batches.push_back(batch);
        docs.timer.Stop();
        t.Stop();
        docs.t_q2q += docs.timer.seconds();
        docs.t_msg += t.seconds();
        //std::cout << t.seconds() << " / " << docs.timer.seconds() << std::endl;
        // Reset the builder, so we can reuse it.
        drone->builder.Reset();
      }
    }
    docs.consumed.fetch_add(popped, std::memory_order_relaxed);
    SPDLOG_DEBUG("[Drone {}] Popped {} items. Builder rows: {}. Buffer size: {}",
                 drone->id,
                 popped,
                 drone->builder.rows(),
                 drone->builder.size());

  }
  SPDLOG_DEBUG("[Drone {}] Stopped.", drone->id);
}

void Hive::Start() {
  spdlog::info("Starting drones.");
  // Spawn worker threads.
  for (Drone &worker : workers) {
    threads.emplace_back(drone_thread, &worker);
  }
}

Hive::Hive(int num_workers, size_t max_size, ReservationSpec reservations) {
  spdlog::info("Initializing Flitter hive with {} drones.", num_workers);
  // Construct worker instances.
  for (int i = 0; i < num_workers; i++) {
    workers.emplace_back(i, max_size, reservations);
  }
  docs.stop = false;
}

void Hive::Stop() {
  // Wait until all items have been consumed.
  while (docs.consumed != docs.produced) {}
  docs.stop = true;

  // Join all threads.
  for (auto &thread : threads) {
    thread.join();
  }

  spdlog::info("q2q avg: {} us, msg avg: {} us", (docs.t_q2q / docs.produced) * 1E6, (docs.t_msg / docs.produced) * 1E6);
  spdlog::info("Stopping server. {}/{} items processed.", docs.produced, docs.consumed);
}

void Hive::Kill() {
  // Stop all threads.
  docs.stop = true;
  // Join all threads.
  for (auto &thread : threads) {
    thread.join();
  }
}

void Hive::Push(const std::string &item) {
  docs.json_queue.enqueue(item);
  docs.produced.fetch_add(1);
  docs.timer.Start();
}

}  // namespace flitter
