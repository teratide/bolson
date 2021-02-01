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

#include <arrow/api.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "bolson/status.h"

namespace bolson {

/**
 * \brief Returns the total size in memory of all (nested) buffers backing Arrow
 * ArrayData.
 *
 * Returns int64_t because Arrow.
 * \param array_data The ArrayData to analyze.
 * \returns The total size of all (nested) buffer contents in bytes.
 */
auto GetArrayDataSize(const std::shared_ptr<arrow::ArrayData>& array_data) -> int64_t;

/**
 * \brief Convert a vector of T to a vector with pointers to each T.
 * \tparam T    The type of the items in the vector.
 * \param vec   The vector.
 * \return A vector with pointers to the items of vec.
 */
template <typename T>
auto ToPointers(std::vector<T>& vec) -> std::vector<T*> {
  std::vector<T*> result;
  result.reserve(vec.size());
  for (size_t i = 0; i < vec.size(); i++) {
    result.push_back(&vec[i]);
  }
  return result;
}

/**
 * \brief Convert a vector of T to a vector with pointers to each T.
 * \tparam T    The type of the items in the vector.
 * \param vec   The vector.
 * \return A vector with pointers to the items of vec.
 */
template <typename To, typename From>
auto CastPtrs(std::vector<std::shared_ptr<From>> vec)
    -> std::vector<std::shared_ptr<To>> {
  std::vector<std::shared_ptr<To>> result;
  result.reserve(vec.size());
  for (size_t i = 0; i < vec.size(); i++) {
    result.push_back(std::static_pointer_cast<To>(vec[i]));
  }
  return result;
}

}  // namespace bolson
