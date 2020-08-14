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
#include <algorithm>
#include <utility>
#include <memory>
#include <rapidjson/document.h>

namespace jsongen {

namespace rj = rapidjson;

// The random number engine we will use is a default subtract_with_carry_engine, since this is documented to be the
// fastest engine. See: https://en.cppreference.com/w/cpp/numeric/random
using RandomEngine = std::ranlux48_base;
using Allocator = rj::Document::AllocatorType;

/**
 * @brief Context for generators to operate in.
 */
struct Context {
  /// The random engine to use in distributions.
  RandomEngine *engine_ = nullptr;
  /// The rapidjson allocator to use.
  rj::Document::AllocatorType *allocator_ = nullptr;
};

/**
 * @brief Abstract class representing a value generator.
 */
class Value {
 public:
  /// @brief Returns a value from this generator.
  virtual auto Get() -> rj::Value = 0;
  /// @brief Set the context for this generator.
  void SetContext(Context context);
 protected:
  /// The context for the generator.
  Context context_;
};

/// @brief Null value generator.
class Null : public Value {
 public:
  /// @brief Returns a null value (always).
  auto Get() -> rj::Value override;
};

/// @brief Boolean value generator.
class Bool : public Value {
 public:
  /// @brief Returns an either "true" or "false" value.
  auto Get() -> rj::Value override;
};

/// @brief Number value generator for integers.
template<typename T>
class Int : public Value {
 public:
  /// @brief Construct a new number value generator with given maximum and minimum value.
  explicit Int(T max = std::numeric_limits<T>::max(),
               T min = std::numeric_limits<T>::min()) {
    dist_ = std::uniform_int_distribution<T>(min, max);
  }

  /// @brief Returns a numeric value representing an integer.
  auto Get() -> rj::Value override {
    rj::Value result;
    result.Set(dist_(*context_.engine_));
    return result;
  }
 private:
  /// The distribution to pull from for integer generation.
  std::uniform_int_distribution<T> dist_;
};

/// @brief String value generator. Lengths follow a normal distribution.
struct String : public Value {
 public:
  /// @brief Construct a new string value generator, with associated string length distribution and limits.
  explicit String(double length_mean = 16.,
                  double length_stddev = 8.,
                  size_t length_clip_max = std::numeric_limits<size_t>::max(),
                  size_t length_clip_min = 0);

  /// @brief Returns a string value with some random characters between a-z.
  auto Get() -> rj::Value override;
 private:
  /// Maximum length for generated stings.
  size_t length_clip_max_;
  /// Minimum length for generated strings.
  size_t length_clip_min_;
  /// The normal distribution to pull the length from.
  std::normal_distribution<> len_dist_;
  /// The uniform distribution to pull characters from.
  std::uniform_int_distribution<> chars_dist_;
};

/// @brief String value generator for ISO 8601-like date and time.
struct DateString : public Value {
 public:
  DateString();
  /// @brief Returns a string value formatted according to an ISO 8601 date and time.
  auto Get() -> rj::Value override;
 private:
  /// Year distribution.
  std::uniform_int_distribution<int32_t> year;
  /// Month distribution.
  std::uniform_int_distribution<uint8_t> month;
  /// Day distribution.
  std::uniform_int_distribution<uint8_t> day;
  /// Hour distribution.
  std::uniform_int_distribution<uint8_t> hour;
  /// Minute distribution.
  std::uniform_int_distribution<uint8_t> min;
  /// Second distribution.
  std::uniform_int_distribution<uint8_t> sec;
  /// Timezone distribution.
  std::uniform_int_distribution<int8_t> timezone;
};

/// @brief Array value generator for fixed-length arrays.
struct FixedSizeArray : public Value {
  /// @brief Construct a new FixedSizeArray generator, with a given length and value generator.
  FixedSizeArray(size_t length, std::shared_ptr<Value> item_generator);
  /// @brief Returns an array of fixed length, with items generated through its value generator.
  auto Get() -> rj::Value override;
 private:
  /// The generator for the array values.
  std::shared_ptr<Value> item_;
  /// The length of every array generated.
  size_t length_;
};

/// @brief Member generator.
class Member {
 public:
  /// @brief Construct a new, unpopulated member generator. This will always generate "": null.
  Member();
  /// @brief Construct a new member generator with a populated name and value generator.
  Member(std::string name, std::shared_ptr<Value> value);

  /// @brief Set the context for this generator to operate in.
  void SetContext(Context context);

  /// @brief Return the context of this member generator.
  auto context() -> Context;

  /// @brief Set the value generator of this member generator.
  void SetValue(std::shared_ptr<Value> value);

  /// @brief Return the value generator of this member generator.
  [[nodiscard]] auto value() const -> std::shared_ptr<Value>;

  /// @brief Set the name of the members that are generated.
  void SetName(std::string name) { name_ = std::move(name); }

  /// @brief Return the name of the members that are generated.
  [[nodiscard]] auto name() const -> std::string { return name_; }

  /**
   * @brief Generate and add a member to the supplied object.
   * @param object The value to add the member to. Must be a rapidjson Object.
   */
  void AddTo(rj::Value *object);
 protected:
  /// The context child generators must work in.
  Context context_;
  /// The name of the member this generator generates.
  std::string name_;
  /// The value generator of this member generator.
  std::shared_ptr<Value> value_;
};

/// @brief Object value generator.
class Object : public Value {
 public:
  /// @brief Construct a new, empty object generator.
  Object() = default;
  /// @brief Construct a new object generator, and populate its members.
  explicit Object(const std::vector<Member> &members);
  /// @brief Returns an object, with members generated by its member generators.
  auto Get() -> rj::Value override;
  /// @brief Add a member generator to this object generator.
  void AddMember(Member member);
 protected:
  /// The member generators of this object generator.
  std::vector<Member> members_;
};

} // namespace jsongen
