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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <flitter/log.h>

#include "./value.h"
#include "./arrow.h"

#define META(X) "JSONGEN_"#X

namespace jsongen {

auto SchemaAnalyzer::Analyze(const arrow::Schema &schema) -> bool {
  assert(out_ != nullptr);
  // Cast the output root object to a JSON object generator.
  auto obj = std::static_pointer_cast<Object>(out_->root());
  // Analyze every field using a FieldAnalyzer.
  for (const auto &field : schema.fields()) {
    Member mg;
    mg.SetContext(out_->context());
    auto fa = FieldAnalyzer(&mg);
    if (fa.Analyze(*field)) {
      obj->AddMember(mg);
    } else {
      spdlog::error("Could not analyze schema.");
      // TODO(johanpel): result type or status type
      return false;
    }
  }
  return true;
}

SchemaAnalyzer::SchemaAnalyzer(DocumentGenerator *out) : out_(out) {
  // Set the root to be an Object.
  // Arrow Schema's turn into a JSON object where Arrow fields are its JSON members.
  auto root = std::make_shared<Object>();
  out_->SetRoot(root);

}

auto FieldAnalyzer::Analyze(const arrow::Field &field) -> bool {
  field_ = &field;
  // TODO(johanpel): field could have nullable probability in metadata and could be a feature of the value generators.
  //  as well as other randomization parameters.
  if (field.nullable()) {
    spdlog::error("For field: {}", field.name());
    spdlog::error("  Nullable fields are not supported.");
    // TODO(johanpel): result type or status type
    return false;
  }
  assert(member_out_ != nullptr);
  member_out_->SetName(field.name());
  auto status = field.type()->Accept(this);
  return true;
}

auto FieldAnalyzer::Visit(const arrow::UInt64Type &type) -> arrow::Status {
  // Check for meta, use numeric limits otherwise.
  auto max = GetUInt64Meta(*field_, META(MAX)).value_or(std::numeric_limits<uint64_t>::max());
  auto min = GetUInt64Meta(*field_, META(MIN)).value_or(std::numeric_limits<uint64_t>::min());

  // Check if min doesn't exceed max.
  if (min > max) {
    spdlog::warn("While parsing field metadata of field {}", field_->name());
    spdlog::warn("  Minimum {} is larger than maximum {}.", min, max);
    spdlog::warn("  Reverting to numeric limits.");
    max = std::numeric_limits<uint64_t>::max();
    min = std::numeric_limits<uint64_t>::min();
  }

  member_out_->SetValue(std::make_shared<Int<uint64_t>>(max, min));

  return arrow::Status::OK();
}

auto FieldAnalyzer::Visit(const arrow::BooleanType &type) -> arrow::Status {
  member_out_->SetValue(std::make_shared<Bool>());
  return arrow::Status::OK();
}

auto FieldAnalyzer::Visit(const arrow::StringType &type) -> arrow::Status {
  member_out_->SetValue(std::make_shared<String>());
  return arrow::Status::OK();
}

auto FieldAnalyzer::Visit(const arrow::Date64Type &type) -> arrow::Status {
  member_out_->SetValue(std::make_shared<DateString>());
  return arrow::Status::OK();
}

auto FieldAnalyzer::Visit(const arrow::FixedSizeListType &type) -> arrow::Status {
  // Create a member generator to store the child generator in delivered by a field analyzer.
  auto mg = std::make_shared<Member>();
  mg->SetContext(member_out_->context());
  auto fa = FieldAnalyzer(mg.get());
  assert(type.num_fields() == 1);
  fa.Analyze(*type.field(0));
  member_out_->SetValue(std::make_shared<FixedSizeArray>(type.list_size(), mg->value()));
  return arrow::Status::OK();
}

auto FieldAnalyzer::Visit(const arrow::StructType &type) -> arrow::Status {
  auto result = std::make_shared<Object>();
  result->SetContext(member_out_->context());

  for (const auto &field : type.fields()) {
    Member mg;
    mg.SetContext(member_out_->context());
    mg.SetName(field->name());
    auto fa = FieldAnalyzer(&mg);
    fa.Analyze(*field);
    result->AddMember(mg);
  }

  member_out_->SetValue(result);
  return arrow::Status::OK();
}

auto ReadSchemaFromFile(const std::string &file, std::shared_ptr<arrow::Schema> *out) -> bool {
  auto f = arrow::io::ReadableFile::Open(file);
  if (!f.ok()) {
    spdlog::error("Could not open file for reading: {}", file);
    spdlog::error("Arrow: {}", f.status().ToString());
    return false;
  }
  auto fis = f.ValueOrDie();

  // Dictionaries are not supported yet, hence nullptr.
  auto res = arrow::ipc::ReadSchema(fis.get(), nullptr);
  if (res.ok()) {
    *out = res.ValueOrDie();
  } else {
    spdlog::error("Could not read schema from file: {}", file);
    spdlog::error("Arrow: {}", res.status().ToString());
    return false;
  }
  auto status = fis->Close();

  return true;
}

auto FromSchema(const arrow::Schema &schema, GenerateOptions options) -> DocumentGenerator {
  DocumentGenerator doc(options.seed);
  auto sa = SchemaAnalyzer(&doc);
  sa.Analyze(schema);
  return doc;
}

auto GetMeta(const arrow::Schema &schema, const std::string &key) -> std::optional<std::string> {
  if (schema.metadata() != nullptr) {
    std::unordered_map<std::string, std::string> meta;
    schema.metadata()->ToUnorderedMap(&meta);
    auto k = meta.find(key);
    if (k != meta.end()) {
      return k->second;
    }
  }
  return std::nullopt;
}

auto GetMeta(const arrow::Field &field, const std::string &key) -> std::optional<std::string> {
  if (field.metadata() != nullptr) {
    std::unordered_map<std::string, std::string> meta;
    field.metadata()->ToUnorderedMap(&meta);
    auto k = meta.find(key);
    if (k != meta.end()) {
      return k->second;
    }
  }
  return std::nullopt;
}

auto GetUInt64Meta(const arrow::Field &field, const std::string &key) -> std::optional<uint64_t> {
  auto str = GetMeta(field, key);
  uint64_t int_value = 0;
  size_t parsed = 0;
  if (str) {
    try {
      int_value = std::stoul(str.value(), &parsed, 10);
    } catch (const std::invalid_argument &e) {
      spdlog::warn("Metadata key {} set for field {}, but value {} is not a valid UInt64.",
                   key,
                   field.name(),
                   str.value());
      return std::nullopt;
    } catch (const std::out_of_range &e) {
      spdlog::warn("Metadata key {} set for field {}, but value {} is out of range for UInt64.",
                   key,
                   field.name(),
                   str.value());
      return std::nullopt;
    }
    // Warn if there is garbage at the end.
    if (parsed != str.value().length()) {
      spdlog::warn(
          "Metadata key {} with value {} for field {} is parsed as {}, but seems to contain additional garbage.",
          key,
          field.name(),
          str.value(),
          int_value);
    }
    return int_value;
  } else {
    return std::nullopt;
  }
}

}