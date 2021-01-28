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

#include "bolson/convert/resizer.h"

namespace bolson::convert {

// TODO: this could also be done based on arrow::ipc::GetRecordBatchSize

auto Resizer::Resize(const parse::ParsedBatch &in, ResizedBatches *out) -> Status {
  ResizedBatches result;
  if (in.batch->num_rows() > max_rows) {
    size_t offset = 0;
    size_t remaining = in.batch->num_rows();
    while (remaining > 0) {
      auto first = in.seq_range.first + offset;
      if (remaining > max_rows) {
        result.push_back(parse::ParsedBatch{in.batch->Slice(offset, max_rows),
                                            {first, first + max_rows}});
        offset += max_rows;
        remaining -= max_rows;
      } else {
        result.push_back(parse::ParsedBatch{in.batch->Slice(offset, remaining),
                                            {first, first + remaining}});
        offset += remaining;
        remaining = 0;
      }
    }
  } else {
    result.push_back({in.batch, in.seq_range});
  }

  *out = result;
  return Status::OK();
}

}
