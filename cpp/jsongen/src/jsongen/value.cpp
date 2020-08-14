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

#include <random>
#include <utility>
#include <rapidjson/document.h>
#include <rapidjson/allocators.h>
#include <flitter/log.h>

#include "./value.h"

namespace jsongen {

void Value::SetContext(Context context) {
  assert(context.engine_ != nullptr);
  assert(context.allocator_ != nullptr);
  context_ = context;
}

String::String(double length_mean, double length_stddev, size_t length_clip_max, size_t length_clip_min)
    : length_clip_max_(length_clip_max), length_clip_min_(length_clip_min) {
  len_dist_ = std::normal_distribution<>(length_mean, length_stddev);
  chars_dist_ = std::uniform_int_distribution<>('a', 'z');
}

auto String::Get() -> rapidjson::Value {
  rapidjson::Value result;
  // Generate the length, and clip if necessary.
  size_t length = std::max(length_clip_min_,
                           std::min(length_clip_max_,
                                    static_cast<size_t>(len_dist_(*context_.engine_))));

  std::string str(length, 0);
  // Pull characters from the character distribution.
  for (char &c : str) {
    c = chars_dist_(*context_.engine_);
  }
  // Call the overload SetString with allocator to make a copy of the string.
  result.SetString(str.c_str(), str.length(), *context_.allocator_);
  return result;
}

auto DateString::Get() -> rapidjson::Value {
  rapidjson::Value result;

  // Format like ISO8601 but without the timezone
  // Apparently this is from spdlog, but we might want to import this separately.
  auto str = ::fmt::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}{:+03d}:00",
                           year(*context_.engine_),
                           month(*context_.engine_),
                           day(*context_.engine_),
                           hour(*context_.engine_),
                           min(*context_.engine_),
                           sec(*context_.engine_),
                           timezone(*context_.engine_));
  // Call the overload SetString with allocator to make a copy of the string.
  result.SetString(str.c_str(), str.length(), *context_.allocator_);
  return result;
}

DateString::DateString() {
  year = std::uniform_int_distribution<>(2000, 2020);
  month = std::uniform_int_distribution<uint8_t>(1, 12);
  day = std::uniform_int_distribution<uint8_t>(1, 28);
  hour = std::uniform_int_distribution<uint8_t>(0, 23);
  min = std::uniform_int_distribution<uint8_t>(0, 59);
  sec = std::uniform_int_distribution<uint8_t>(0, 59);
  timezone = std::uniform_int_distribution<int8_t>(-12, 12);
}

FixedSizeArray::FixedSizeArray(size_t length, std::shared_ptr<Value> item_generator)
    : length_(length), item_(std::move(item_generator)) {}

auto FixedSizeArray::Get() -> rapidjson::Value {
  rapidjson::Value result(rapidjson::kArrayType);
  result.SetArray();
  result.Reserve(length_, *context_.allocator_);
  for (size_t i = 0; i < length_; i++) {
    result.PushBack(item_->Get(), *context_.allocator_);
  }
  return result;
}

void Member::AddTo(rapidjson::Value *object) {
  rapidjson::Value name(rapidjson::StringRef(name_.c_str()));
  rapidjson::Value val = value_->Get();
  object->AddMember(name, val, *context_.allocator_);
}

Member::Member(std::string name, std::shared_ptr<Value> value) : name_(std::move(name)), value_(std::move(value)) {}

Member::Member() { value_ = std::make_shared<Null>(); }

void Member::SetValue(std::shared_ptr<Value> value) {
  value_ = std::move(value);
  value_->SetContext(context_);
}

void Member::SetContext(Context context) { context_ = context; }

auto Member::context() -> Context { return context_; }

auto Member::value() const -> std::shared_ptr<Value> { return value_; }

auto Object::Get() -> rj::Value {
  rj::Value result(rj::kObjectType);
  for (auto &mg : members_) {
    mg.AddTo(&result);
  }
  return result;
}

void Object::AddMember(Member member) {
  member.SetContext(context_);
  members_.push_back(member);
}

Object::Object(const std::vector<Member> &members) {
  for (auto m : members) {
    m.SetContext(context_);
    members_.push_back(m);
  }
}

auto Null::Get() -> rj::Value { return rj::Value(rj::kNullType); }

auto Bool::Get() -> rj::Value {
  return rj::Value(((*this->context_.engine_)() % 2 == 0));
}

} // namespace jsongen
