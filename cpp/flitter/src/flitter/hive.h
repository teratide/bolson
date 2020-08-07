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

#include <memory>
#include <vector>
#include <cstdint>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <iostream>
#include <arrow/api.h>

#include "./tweets.h"

namespace flitter {

struct Drone {
  Drone(int drone_id, size_t max_size, ReservationSpec reservation_spec);

//  // Builders for every column and their children
//  std::vector<uint64_t> id_val;
//  int32_t created_at_off_current;
//  std::vector<int32_t> created_at_off;
//  std::vector<uint8_t> created_at_val;
//  int32_t text_off_current;
//  std::vector<int32_t> text_off;
//  std::vector<uint8_t> text_val;
//  std::vector<uint64_t> author_id_val;
//  std::vector<uint64_t> in_reply_to_user_id_val;
//  int32_t ref_tweets_off_current;
//  std::vector<uint8_t> ref_tweets_type_val;
//  std::vector<uint64_t> ref_tweets_id_val;
//  // ref_tweets_struct_ has no buffers
//  std::vector<int32_t> ref_tweets_off;

  TweetsBuilder builder;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

  const size_t max_size = 0;
  int64_t num_rows_ = 0;
  const int64_t attempts_before_sync = 16 * 1024;
  int id = 0;
};

void drone_thread(Drone *drone);

struct Hive {
  explicit Hive(int num_workers = 1,
                size_t max_size = 5 * 1024 * 1024 - 11 * 1024, // arrow ipc header seems to be less than a KiB
                ReservationSpec reservations = ReservationSpec());

  std::vector<Drone> workers;
  std::vector<std::thread> threads;

  void Push(const std::string &item);
  void Start();
  void Stop();
  void Kill();
};

}  // namespace flitter
