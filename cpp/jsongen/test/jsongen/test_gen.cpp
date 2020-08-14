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

#include <gtest/gtest.h>
#include <rapidjson/writer.h>

#include "jsongen/document.h"
#include "jsongen/value.h"

namespace jsongen::test {

TEST(Generators, EmptyDocument) {
  DocumentGenerator doc(0);
  rapidjson::StringBuffer b;
  rapidjson::Writer p(b);
  doc.Get().Accept(p);
  ASSERT_STREQ(b.GetString(), "null");
}

}