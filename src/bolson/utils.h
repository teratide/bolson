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

#include <charconv>
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

template <typename T>
auto Aggregate(const std::vector<T>& items) -> T {
  T result;
  for (const auto& i : items) {
    result += i;
  }
  return result;
}

/**
 * @brief Parse a string to a number with a potential scaling factor.
 * @param input     The input string.
 * @param output    The output value.
 * @return Status::OK() if successful, some error otherwise.
 */
template <typename T>
auto ParseWithScale(const std::string& input, T* output) -> Status {
  T num = 0;
  auto fcr = std::from_chars(input.data(), input.data() + input.size(), num);
  // skip potential whitespaces.
  while (*fcr.ptr == ' ') {
    fcr.ptr++;
  }
  auto scale = input.substr(fcr.ptr - input.data());

  if (scale.empty()) {
    // do nothing
  } else if (scale == "Ki") {
    num = num << 10;
  } else if (scale == "Mi") {
    num = num << 20;
  } else if (scale == "Gi") {
    num = num << 30;
  } else if (scale == "K") {
    num = num * 1000;
  } else if (scale == "M") {
    num = num * 1000 * 1000;
  } else if (scale == "G") {
    num = num * 1000 * 1000 * 1000;
  } else {
    return Status(Error::CLIError, "Unexpected scaling factor: " + scale +
                                       ". Accepts only Ki, Mi, Gi, K, M, or G");
  }
  *output = num;
  return Status::OK();
}

}  // namespace bolson
