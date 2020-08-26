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

#include <memory>
#include <arrow/api.h>

#include "jsongen/value.h"
#include "jsongen/document.h"

#pragma once

namespace jsongen {

/**
 * \brief From an Arrow schema, look up a metadata value by key.
 * \param schema The schema to inspect.
 * \param key The key to look up.
 * \return The value, if the KV-pair exists.
 */
auto GetMeta(const arrow::Schema &schema, const std::string &key) -> std::optional<std::string>;

/**
 * \brief From an Arrow Field, look up a metadata value by key.
 * \param field The field to inspect.
 * \param key The key to look up.
 * \return The value, if the KV-pair exists.
 */
auto GetMeta(const arrow::Field &field, const std::string &key) -> std::optional<std::string>;

/**
 * \brief From an Arrow Field, look up a metadata value by key, and convert it to an uint64_t.
 * \param field The field to inspect.
 * \param key The key to look up.
 * \return The uint64_t value, if the KV-pair exists and was successfully converted.
 */
auto GetUInt64Meta(const arrow::Field &field, const std::string &key) -> std::optional<uint64_t>;

/// @brief Class to analyze an Arrow schema and populate a DocumentGenerator.
class SchemaAnalyzer : public arrow::TypeVisitor {
 public:
  /// @brief Construct a new SchemaAnalyzer, setting the DocumentGenerator generator to populate.
  explicit SchemaAnalyzer(DocumentGenerator *out);
  /// @brief Analyze an Arrow schema. This populates the DocumentGenerator supplied during construction.
  auto Analyze(const arrow::Schema &schema) -> bool;
 protected:
  /// The DocumentGenerator to populate.
  DocumentGenerator *out_ = nullptr;
};

/// @brief Class to analyze an Arrow field, and populate a Member generator.
class FieldAnalyzer : public arrow::TypeVisitor {
 public:
  /// @brief Construct a new FieldAnalyzer, setting the Member generator to populate.
  explicit FieldAnalyzer(Member *member) : member_out_(member) {}
  /// @brief Analyze an Field schema. This populates the Member generator supplied during construction.
  auto Analyze(const arrow::Field &field) -> bool;

 protected:
  /// @brief Visit a StringType.
  auto Visit(const arrow::StringType &type) -> arrow::Status override;
  /// @brief Visit a FixedSizeListType.
  auto Visit(const arrow::FixedSizeListType &type) -> arrow::Status override;
  /// @brief Visit a StructType.
  auto Visit(const arrow::StructType &type) -> arrow::Status override;
  /// @brief Visit a UInt64Type.
  auto Visit(const arrow::UInt64Type &type) -> arrow::Status override;
  /// @brief Visit a BooleanType.
  auto Visit(const arrow::BooleanType &type) -> arrow::Status override;
  /// @brief Visit a Date64Type.
  auto Visit(const arrow::Date64Type &type) -> arrow::Status override;

  /// The field being analyzed.
  const arrow::Field *field_ = nullptr;
  /// The Member generator to populate.
  Member *member_out_ = nullptr;
};

/**
 * \brief Read an Arrow schema from file. Schema must be stored as raw output of Arrow's serialization functions.
 * \param file The file containing the schema.
 * \param out A shared pointer to store the resulting schema in.
 * \return true if successful, false otherwise.
 */
auto ReadSchemaFromFile(const std::string &file, std::shared_ptr<arrow::Schema> *out) -> bool;

/**
 * \brief Construct a DocumentGenerator from an Arrow schema.
 * \param schema The Arrow schema to use.
 * \param options Options for the generator tree.
 * \return A DocumentGenerator able to generate JSON objects derived from the Arrow schema.
 */
auto FromArrowSchema(const arrow::Schema &schema, GenerateOptions options = GenerateOptions()) -> DocumentGenerator;

}