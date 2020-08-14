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

#include <iostream>
#include <utility>

#include "./value.h"
#include "./document.h"

namespace jsongen {

namespace rj = rapidjson;

DocumentGenerator::DocumentGenerator(int seed) : engine_(RandomEngine(seed)) {
  context_.engine_ = &engine_;
  context_.allocator_ = &doc_.GetAllocator();
  root_ = std::make_shared<Null>();
}

void DocumentGenerator::SetRoot(std::shared_ptr<Value> root) {
  root_ = std::move(root);
  root_->SetContext(context_);
}

auto DocumentGenerator::root() -> std::shared_ptr<Value> { return root_; }

auto DocumentGenerator::Get() -> rj::Value { return root_->Get(); }

}
