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

#include <random>
#include <utility>

#include "jsongen/value.h"

namespace jsongen {

namespace rj = rapidjson;

/// @brief Options for pseudo-random generators.
struct GenerateOptions {
  /// Construct new GenerateOptions, using a random device to obtain the seed.
  GenerateOptions() : seed(std::random_device()()) {}
  /// Construct new GenerateOptions with a specific seed.
  explicit GenerateOptions(int seed) : seed(seed) {}
  /// The seed used in pseudo-random generators.
  int seed;
};

/// @brief Allows the generation of a JSON DOM root.
class DocumentGenerator {
 public:
  /// @brief Construct a new DocumentGenerator, and feed the random engine with a seed.
  explicit DocumentGenerator(int seed);
  /// @brief Set a new root value generator.
  void SetRoot(std::shared_ptr<Value> root);

  /// @brief Get the root value generator.
  auto root() -> std::shared_ptr<Value>;

  /// @brief Generate a root value.
  auto Get() -> rj::Value;

  /// @brief Return the context of this DocumentGenerator.
  auto context() -> Context { return context_; }
 protected:
  /// A placeholder for a rapidjson Document. Used to obtain an allocator.
  rj::Document doc_;
  /// The random engine used by child generators.
  RandomEngine engine_;
  /// The context pointing to the rapidjson document allocator and random engine.
  Context context_;
  /// The root value generator.
  std::shared_ptr<Value> root_;
};

}
