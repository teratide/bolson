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

namespace bolson::parse::custom {

inline auto EatWhitespace(const char* pos, const char* end) -> const char* {
  // Whitespace includes: space, line feed, carriage return, character tabulation
  // const char ws[] = {' ', '\t', '\n', '\r'};
  const char* result = pos;
  while (((*result == ' ') || (*result == '\t')) && (result < end)) {
    result++;
  }
  if (result == end) {
    return nullptr;
  }
  return result;
}

inline auto EatChar(const char* pos, const char* end, const char c) -> const char* {
  if (*pos != c) {
    throw std::runtime_error(fmt::format("Expected '{}', encountered '{}'", c, *pos));
  }
  pos++;
  if (pos >= end) {
    return nullptr;
  }
  return pos;
}

inline auto EatObjectStart(const char* pos, const char* end) {
  return EatChar(pos, end, '{');
}
inline auto EatObjectEnd(const char* pos, const char* end) {
  return EatChar(pos, end, '}');
}
inline auto EatMemberKeyValueSeperator(const char* pos, const char* end) {
  return EatChar(pos, end, ':');
}
inline auto EatArrayStart(const char* pos, const char* end) {
  return EatChar(pos, end, '[');
}
inline auto EatArrayEnd(const char* pos, const char* end) {
  return EatChar(pos, end, ']');
}

inline auto EatMemberKey(const char* pos, const char* end, const char* key) -> const
    char* {
  const size_t key_len = std::strlen(key);
  pos = EatChar(pos, end, '"');
  if (std::memcmp(pos, key, key_len) != 0) {
    throw std::runtime_error(fmt::format("Expected \"{}\", encountered {}", key,
                                         std::string_view(pos, key_len)));
  }
  pos += key_len;
  pos = EatChar(pos, end, '"');
  if (pos >= end) {
    return nullptr;
  }
  return pos;
}

// todo: figure out how to use NumericBuilder<T> and from_chars<T> together with the same
// T so this can be a template function
inline auto EatUInt64(const char* pos, const char* end, arrow::UInt64Builder* builder)
    -> const char* {
  uint64_t val;
  auto fc_result = std::from_chars<uint64_t>(pos, end, val);
  switch (fc_result.ec) {
    default:
      break;
    case std::errc::invalid_argument:
      throw std::runtime_error(std::string("Cannot parse value as primitive: ") +
                               std::string(pos, end));
    case std::errc::result_out_of_range:
      throw std::runtime_error("Value out of range:" + std::string(pos, end));
  }
  ARROW_TOE(builder->Append(val));
  return fc_result.ptr;
}

// todo: figure out how to use NumericBuilder<T> and from_chars<T> together with the same
// T so this can be a template function
inline auto EatUInt64Unsafe(const char* pos, const char* end,
                            arrow::UInt64Builder* builder) -> const char* {
  uint64_t val;
  auto fc_result = std::from_chars<uint64_t>(pos, end, val);
  switch (fc_result.ec) {
    default:
      break;
    case std::errc::invalid_argument:
      throw std::runtime_error(std::string("Cannot parse value as primitive: ") +
                               std::string(pos, end));
    case std::errc::result_out_of_range:
      throw std::runtime_error("Value out of range:" + std::string(pos, end));
  }
  builder->UnsafeAppend(val);
  return fc_result.ptr;
}

inline auto EatBool(const char* pos, const char* end, arrow::BooleanBuilder* builder)
    -> const char* {
  bool val;
  if (memcmp(pos, "true", 4) == 0) {
    val = true;
    pos += 4;
  } else if (memcmp(pos, "false", 5) == 0) {
    val = false;
    pos += 5;
  } else {
    return nullptr;
  }
  builder->Append(val);
  return pos;
}

inline auto EatBoolUnsafe(const char* pos, const char* end,
                          arrow::BooleanBuilder* builder) -> const char* {
  bool val;
  if (memcmp(pos, "true", 4) == 0) {
    val = true;
    pos += 4;
  } else if (memcmp(pos, "false", 5) == 0) {
    val = false;
    pos += 5;
  } else {
    return nullptr;
  }
  builder->UnsafeAppend(val);
  return pos;
}

inline auto EatUInt64Array(const char* pos, const char* end,
                           arrow::ListBuilder* list_builder,
                           arrow::UInt64Builder* values_builder) -> const char* {
  pos = EatArrayStart(pos, end);  // [
  ARROW_TOE(list_builder->Append());
  // Scan values
  while (true) {
    pos = EatWhitespace(pos, end);
    if (pos > end) {
      throw std::runtime_error(
          "Unexpected end of JSON data while parsing array values..");
    } else if (*pos == ']') {  // Check array end
      pos++;
      break;
    } else {  // Parse values
      uint64_t val = 0;
      pos = EatUInt64(pos, end, values_builder);
      if (*pos == ',') {
        pos++;
      }
    }
  }
  if (pos >= end) {
    return nullptr;
  }
  return pos;
}

inline auto EatUInt64ArrayUnsafe(const char* pos, const char* end,
                                 arrow::ListBuilder* list_builder,
                                 arrow::UInt64Builder* values_builder) -> const char* {
  pos = EatArrayStart(pos, end);  // [
  ARROW_TOE(list_builder->Append());
  // Scan values
  while (true) {
    pos = EatWhitespace(pos, end);
    if (pos > end) {
      throw std::runtime_error(
          "Unexpected end of JSON data while parsing array values..");
    } else if (*pos == ']') {  // Check array end
      pos++;
      break;
    } else {  // Parse values
      uint64_t val = 0;
      pos = EatUInt64Unsafe(pos, end, values_builder);
      if (*pos == ',') {
        pos++;
      }
    }
  }
  if (pos >= end) {
    return nullptr;
  }
  return pos;
}

inline auto EatUInt64FixedSizeArray(const char* pos, const char* end,
                                    arrow::FixedSizeListBuilder* list_builder,
                                    arrow::UInt64Builder* values_builder) -> const char* {
  pos = EatArrayStart(pos, end);  // [
  ARROW_TOE(list_builder->Append());
  // Scan values
  while (true) {
    pos = EatWhitespace(pos, end);
    if (pos > end) {
      throw std::runtime_error(
          "Unexpected end of JSON data while parsing array values..");
    } else if (*pos == ']') {  // Check array end
      pos++;
      break;
    } else {  // Parse values
      uint64_t val = 0;
      pos = EatUInt64(pos, end, values_builder);
      if (*pos == ',') {
        pos++;
      }
    }
  }
  if (pos >= end) {
    return nullptr;
  }
  return pos;
}

inline auto EatUInt64FixedSizeArrayUnsafe(const char* pos, const char* end,
                                          arrow::FixedSizeListBuilder* list_builder,
                                          arrow::UInt64Builder* values_builder) -> const
    char* {
  pos = EatArrayStart(pos, end);  // [
  ARROW_TOE(list_builder->Append());
  // Scan values
  while (true) {
    pos = EatWhitespace(pos, end);
    if (pos > end) {
      throw std::runtime_error(
          "Unexpected end of JSON data while parsing array values..");
    } else if (*pos == ']') {  // Check array end
      pos++;
      break;
    } else {  // Parse values
      uint64_t val = 0;
      pos = EatUInt64Unsafe(pos, end, values_builder);
      if (*pos == ',') {
        pos++;
      }
    }
  }
  if (pos >= end) {
    return nullptr;
  }
  return pos;
}

// assumes no escaped "
inline auto EatStringWithoutEscapes(const char* pos, const char* end,
                                    arrow::StringBuilder* string_builder) -> const char* {
  pos = EatChar(pos, end, '"');  // "

  auto* str_end = std::strchr(pos, '"');  // find last "

  if (str_end != nullptr) {
    ARROW_TOE(string_builder->Append(arrow::util::string_view(pos, str_end - pos)));
    pos = str_end + 1;
  } else {
    return nullptr;
  }

  if (pos >= end) {
    return nullptr;
  }
  return pos;
}

inline auto EatUInt64Member(const char* pos, const char* end, const char* key,
                            arrow::UInt64Builder* builder, bool eat_member_sep = false)
    -> const char* {
  pos = EatMemberKey(pos, end, key);
  pos = EatWhitespace(pos, end);
  pos = EatMemberKeyValueSeperator(pos, end);
  pos = EatWhitespace(pos, end);
  pos = EatUInt64(pos, end, builder);
  pos = EatWhitespace(pos, end);
  if (eat_member_sep) {
    pos = EatChar(pos, end, ',');
    pos = EatWhitespace(pos, end);
  }
  return pos;
}

inline auto EatUInt64MemberUnsafe(const char* pos, const char* end, const char* key,
                                  arrow::UInt64Builder* builder,
                                  bool eat_member_sep = false) -> const char* {
  pos = EatMemberKey(pos, end, key);
  pos = EatWhitespace(pos, end);
  pos = EatMemberKeyValueSeperator(pos, end);
  pos = EatWhitespace(pos, end);
  pos = EatUInt64Unsafe(pos, end, builder);
  pos = EatWhitespace(pos, end);
  if (eat_member_sep) {
    pos = EatChar(pos, end, ',');
    pos = EatWhitespace(pos, end);
  }
  return pos;
}

inline auto EatBoolMember(const char* pos, const char* end, const char* key,
                          arrow::BooleanBuilder* builder, bool eat_member_sep = false)
    -> const char* {
  pos = EatMemberKey(pos, end, key);
  pos = EatWhitespace(pos, end);
  pos = EatMemberKeyValueSeperator(pos, end);
  pos = EatWhitespace(pos, end);
  pos = EatBool(pos, end, builder);
  pos = EatWhitespace(pos, end);
  if (eat_member_sep) {
    pos = EatChar(pos, end, ',');
    pos = EatWhitespace(pos, end);
  }
  return pos;
}

inline auto EatBoolMemberUnsafe(const char* pos, const char* end, const char* key,
                                arrow::BooleanBuilder* builder,
                                bool eat_member_sep = false) -> const char* {
  pos = EatMemberKey(pos, end, key);
  pos = EatWhitespace(pos, end);
  pos = EatMemberKeyValueSeperator(pos, end);
  pos = EatWhitespace(pos, end);
  pos = EatBoolUnsafe(pos, end, builder);
  pos = EatWhitespace(pos, end);
  if (eat_member_sep) {
    pos = EatChar(pos, end, ',');
    pos = EatWhitespace(pos, end);
  }
  return pos;
}

inline auto EatUInt64FixedSizeArrayMember(const char* pos, const char* end,
                                          const char* key,
                                          arrow::FixedSizeListBuilder* list_builder,
                                          arrow::UInt64Builder* values_builder,
                                          bool eat_member_sep = false) -> const char* {
  pos = EatMemberKey(pos, end, key);
  pos = EatWhitespace(pos, end);
  pos = EatMemberKeyValueSeperator(pos, end);
  pos = EatWhitespace(pos, end);
  pos = EatUInt64FixedSizeArray(pos, end, list_builder, values_builder);
  pos = EatWhitespace(pos, end);
  if (eat_member_sep) {
    pos = EatChar(pos, end, ',');
    pos = EatWhitespace(pos, end);
  }
  return pos;
}

inline auto EatUInt64FixedSizeArrayMemberUnsafe(const char* pos, const char* end,
                                                const char* key,
                                                arrow::FixedSizeListBuilder* list_builder,
                                                arrow::UInt64Builder* values_builder,
                                                bool eat_member_sep = false) -> const
    char* {
  pos = EatMemberKey(pos, end, key);
  pos = EatWhitespace(pos, end);
  pos = EatMemberKeyValueSeperator(pos, end);
  pos = EatWhitespace(pos, end);
  pos = EatUInt64FixedSizeArrayUnsafe(pos, end, list_builder, values_builder);
  pos = EatWhitespace(pos, end);
  if (eat_member_sep) {
    pos = EatChar(pos, end, ',');
    pos = EatWhitespace(pos, end);
  }
  return pos;
}

}  // namespace bolson::parse::custom