#include <utility>
#include <sys/mman.h>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <fletcher/fletcher.h>
#include <fletcher/context.h>
#include <fletcher/platform.h>
#include <fletcher/kernel.h>

#include "./log.h"
#include "./opae_trip_report_impl.h"

/// Return Bolson error status when Fletcher error status is supplied.
#define FLETCHER_ROE(s) {                                                        \
  auto __status = (s);                                                           \
  if (!__status.ok()) throw std::runtime_error("Fletcher: " + __status.message); \
}                                                                                \
void()

/// Return on error status.
#define ROE(s) {                  \
  auto __status = (s);            \
  if (!__status) return __status; \
}                                 \
void()

static std::shared_ptr<arrow::Schema> input_schema() {
  static auto result = fletcher::WithMetaRequired(
      *arrow::schema({arrow::field("input", arrow::uint8(), false)}),
      "input",
      fletcher::Mode::READ);
  return result;
}

static std::shared_ptr<arrow::Schema> output_schema_hw() {
  static auto result = fletcher::WithMetaRequired(
          *arrow::schema({
             fletcher::WithMetaEPC(*arrow::field("timestamp", arrow::utf8(), false), 1),
             arrow::field("tag", arrow::uint64(), false),
             arrow::field("timezone", arrow::uint64(), false),
             arrow::field("vin", arrow::uint64(), false),
             arrow::field("odometer", arrow::uint64(), false),
             arrow::field("hypermiling", arrow::uint8(), false),
             arrow::field("avgspeed", arrow::uint64(), false),
             arrow::field("sec_in_band", arrow::uint64(), false),
             arrow::field("miles_in_time_range", arrow::uint64(), false),
             arrow::field("const_speed_miles_in_band", arrow::uint64(), false),
             arrow::field("vary_speed_miles_in_band", arrow::uint64(), false),
             arrow::field("sec_decel", arrow::uint64(), false),
             arrow::field("sec_accel", arrow::uint64(), false),
             arrow::field("braking", arrow::uint64(), false),
             arrow::field("accel", arrow::uint64(), false),
             arrow::field("orientation", arrow::uint8(), false),
             arrow::field("small_speed_var", arrow::uint64(), false),
             arrow::field("large_speed_var", arrow::uint64(), false),
             arrow::field("accel_decel", arrow::uint64(), false),
             arrow::field("speed_changes", arrow::uint64(), false) }),
          "output",
          fletcher::Mode::WRITE);
  return result;
}

static std::shared_ptr<arrow::Schema> output_schema_sw() {
  static auto result = fletcher::WithMetaRequired(
          *arrow::schema({
             arrow::field("timestamp", arrow::utf8(), false),
             arrow::field("tag", arrow::uint64(), false),
             arrow::field("timezone", arrow::uint64(), false),
             arrow::field("vin", arrow::uint64(), false),
             arrow::field("odometer", arrow::uint64(), false),
             arrow::field("hypermiling", arrow::uint8(), false),
             arrow::field("avgspeed", arrow::uint64(), false),
             arrow::field("sec_in_band", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12), false),
             arrow::field("miles_in_time_range", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 24), false),
             arrow::field("const_speed_miles_in_band", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 12), false),
             arrow::field("vary_speed_miles_in_band", arrow::fixed_size_list(arrow::field("item",arrow::uint64(), false), 12), false),
             arrow::field("sec_decel", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 10), false),
             arrow::field("sec_accel", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 10), false),
             arrow::field("braking", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 6), false),
             arrow::field("accel", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 6), false),
             arrow::field("orientation", arrow::uint8(), false),
             arrow::field("small_speed_var", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13), false),
             arrow::field("large_speed_var", arrow::fixed_size_list(arrow::field("item", arrow::uint64(), false), 13), false),
             arrow::field("accel_decel", arrow::uint64(), false),
             arrow::field("speed_changes", arrow::uint64(), false)}),
          "output",
          fletcher::Mode::WRITE);
  return result;
}

bool OpaeTripReportParserManager::PrepareInputBatches(const std::vector<RawJSONBuffer *> &buffers) {
  for (const auto &buf : buffers) {
    spdlog::info("Wrapping buffer (@:{:016X} s:{}) into Arrow RecordBatch.",
                 reinterpret_cast<uint64_t>(buf->data_),
                 buf->capacity_);
    auto wrapped = arrow::Buffer::Wrap(buf->data_, buf->capacity_);
    auto array =
        std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), buf->size_, wrapped);
    batches_in.push_back(arrow::RecordBatch::Make(input_schema(),
                                                  buf->capacity_,
                                                  {array}));
  }
  return true;
}



arrow::Result<std::shared_ptr<arrow::FixedSizeListArray>> AlignedFixedSizeListArray(const std::shared_ptr<arrow::DataType>& type,
                                                                                    std::shared_ptr<arrow::Array> value_array,
                                                                                    size_t length) {
  auto list_array = std::make_shared<arrow::FixedSizeListArray>(arrow::fixed_size_list(type, length), 0, value_array);
  return list_array;
}


arrow::Result<std::shared_ptr<arrow::PrimitiveArray>> AlignedPrimitiveArray(const std::shared_ptr<arrow::DataType>& type,
                                                                            OpaeAllocator& allocator,
                                                                            size_t buffer_size) {
  byte *value_data = nullptr;
  bool ret =allocator.Allocate(opae_fixed_capacity, &value_data);
  if (!ret)
  {
    return arrow::Status::OutOfMemory("Failed to allocate buffer");
  }
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(type, 0, value_buffer);
  return value_array;
}

arrow::Result<std::shared_ptr<arrow::StringArray>> AlignedStringArray(OpaeAllocator& allocator,
                                                                      size_t offset_buffer_size,
                                                                      size_t value_buffer_size) {
  byte *offset_data = nullptr;
  bool ret =allocator.Allocate(offset_buffer_size, &offset_data);
  if (!ret)
  {
    return arrow::Status::OutOfMemory("Failed to allocate buffer");
  }
  memset(offset_data, 0, offset_buffer_size);

  auto offset_buffer = std::make_shared<arrow::Buffer>(offset_data, offset_buffer_size);

  byte *value_data = nullptr;
  ret =allocator.Allocate(value_buffer_size, &value_data);
  if (!ret)
  {
    return arrow::Status::OutOfMemory("Failed to allocate buffer");
  }
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, value_buffer_size);
  auto string_array = std::make_shared<arrow::StringArray>(0, offset_buffer, value_buffer);
  return arrow::ToResult(string_array);
}

std::shared_ptr<arrow::PrimitiveArray>ReWrapArray(std::shared_ptr<arrow::PrimitiveArray> array,
                                                  size_t num_rows) {

  auto data_buff = array->data()->buffers[1]->data();
  auto value_buffer = std::make_shared<arrow::Buffer>(data_buff, num_rows);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_rows, value_buffer);
  return value_array;
}


std::shared_ptr<arrow::StringArray>ReWrapArray(std::shared_ptr<arrow::StringArray> array,
                                               size_t num_rows) {
  auto string_array = std::make_shared<arrow::StringArray>(num_rows, array->value_offsets(), array->value_data());
  return string_array;
}

std::shared_ptr<arrow::FixedSizeListArray>ReWrapArray(std::shared_ptr<arrow::FixedSizeListArray> array,
                                                      size_t num_rows) {

  auto data_buff = array->values()->data()->buffers[1]->data();
  auto value_buff_size = array->length() * num_rows * sizeof(uint64_t);;
  auto value_buffer = std::make_shared<arrow::Buffer>(data_buff, value_buff_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_rows*array->value_length(), value_buffer);
  auto list_array = std::make_shared<arrow::FixedSizeListArray>(arrow::fixed_size_list(arrow::uint64(), array->value_length()), num_rows, value_array);
  return list_array;
}

arrow::Status WrapTripReport(
        int32_t num_rows,
        std::vector<std::shared_ptr<arrow::Array>> arrays,
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<arrow::RecordBatch> *out)
{

  std::vector<std::shared_ptr<arrow::Array>> rewrapped_arrays;

  for(std::shared_ptr<arrow::Array> f: arrays) {
    if(f->type()->Equals(arrow::uint64())) {
      auto field = std::static_pointer_cast<arrow::PrimitiveArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    } else if(f->type()->Equals(arrow::uint8())) {
      auto field = std::static_pointer_cast<arrow::PrimitiveArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    } else if (f->type()->id() == arrow::Type::FIXED_SIZE_LIST) {
      auto field = std::static_pointer_cast<arrow::FixedSizeListArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    } else if (f->type()->Equals(arrow::utf8())) {
      auto field = std::static_pointer_cast<arrow::StringArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    }
  }

  *out = arrow::RecordBatch::Make(schema, num_rows, rewrapped_arrays);

  return arrow::Status::OK();
}



bool OpaeTripReportParserManager::PrepareOutputBatch() {
  for(auto f : output_schema_sw()->fields()) {
    if(f->type()->Equals(arrow::uint64())) {
      auto array = AlignedPrimitiveArray(arrow::uint64(), allocator, opae_fixed_capacity).ValueOrDie();
      output_arrays_sw.push_back(array);
      output_arrays_hw.push_back(array);
    } else if(f->type()->Equals(arrow::uint8())) {
      auto array = AlignedPrimitiveArray(arrow::uint8(), allocator, opae_fixed_capacity).ValueOrDie();
      output_arrays_sw.push_back(array);
      output_arrays_hw.push_back(array);
    } else if (f->type()->Equals(arrow::utf8())) {
      auto array = AlignedStringArray(allocator, opae_fixed_capacity, opae_fixed_capacity).ValueOrDie();
      output_arrays_sw.push_back(array);
      output_arrays_hw.push_back(array);
    } else if (f->type()->id() == arrow::Type::FIXED_SIZE_LIST) {
      auto field = std::static_pointer_cast<arrow::FixedSizeListType>(f->type());
      auto value_array = AlignedPrimitiveArray(arrow::uint64(), allocator, opae_fixed_capacity).ValueOrDie();
      auto list_array = AlignedFixedSizeListArray(field->value_type(), value_array, field->list_size()).ValueOrDie();
      output_arrays_sw.push_back(list_array);
      output_arrays_hw.push_back(value_array);
    }
  }

  batch_out_sw = arrow::RecordBatch::Make(output_schema_sw(), 0, output_arrays_sw);
  batch_out_hw = arrow::RecordBatch::Make(output_schema_hw(), 0, output_arrays_hw);

  return true;
}

bool OpaeTripReportParserManager::Make(const OpaeBatteryOptions &opts,
                                       const std::vector<RawJSONBuffer *> &buffers,
                                       size_t num_parsers,
                                       std::shared_ptr<OpaeTripReportParserManager> *out) {
  auto result = std::make_shared<OpaeTripReportParserManager>();
  result->opts_ = opts;
  result->num_parsers_ = num_parsers;

  spdlog::info("Setting up OpaeTripReportParserManager for {} buffers.", buffers.size());

  ROE(result->PrepareInputBatches(buffers));
  ROE(result->PrepareOutputBatch());

  spdlog::info("Using {}", FLETCHER_PLATFORM);
  FLETCHER_ROE(fletcher::Platform::Make(FLETCHER_PLATFORM, &result->platform, false));

  // Fix AFU id
  std::stringstream ss;
  ss << std::hex << num_parsers;
  result->opts_.afu_id[strlen(OPAE_BATTERY_AFU_ID) - 1] = ss.str()[0];
  spdlog::info("AFU ID: {}", result->opts_.afu_id);
  const char *afu_id = result->opts_.afu_id.data();
  result->platform->init_data = &afu_id;
  FLETCHER_ROE(result->platform->Init());

  // Pull everything through the fletcher stack once.
  // Pull everything through the fletcher stack once.
  FLETCHER_ROE(fletcher::Context::Make(&result->context, result->platform));

  for (const auto &batch : result->batches_in) {
    FLETCHER_ROE(result->context->QueueRecordBatch(batch));
  }


  FLETCHER_ROE(result->context->QueueRecordBatch(result->batch_out_hw));


  spdlog::info("Enabling context...");

  // Enable context.
  FLETCHER_ROE(result->context->Enable());
  // Construct kernel handler.
  result->kernel = std::make_shared<fletcher::Kernel>(result->context);
  // Write metadata.
  FLETCHER_ROE(result->kernel->WriteMetaData());

  // Workaround to store buffer device address.
  for (size_t i = 0; i < buffers.size(); i++) {
    result->h2d_addr_map[buffers[i]->data_] =
        result->context->device_buffer(i).device_address;
  }

  spdlog::info("Preparing parsers.");

  ROE(result->PrepareParsers());

  *out = result;

  return true;
}

bool OpaeTripReportParserManager::PrepareParsers() {
  for (size_t i = 0; i < num_parsers_; i++) {
    auto parser = std::make_shared<OpaeTripReportParser>(platform.get(),
                                           context.get(),
                                           kernel.get(),
                                           &h2d_addr_map,
                                           i,
                                           num_parsers_,
                                           &platform_mutex);

    parsers_.push_back(parser);
  }
  return true;
}

static bool WrapOutput(int32_t num_rows,
                       uint8_t *offsets,
                       uint8_t *values,
                       std::shared_ptr<arrow::Schema> schema,
                       std::shared_ptr<arrow::RecordBatch> *out) {
  auto ret = true;

  // +1 because the last value in offsets buffer is the next free index in the values
  // buffer.
  int32_t num_offsets = num_rows + 1;

  // Obtain the last value in the offsets buffer to know how many values there are.
  int32_t num_values = (reinterpret_cast<int32_t *>(offsets))[num_rows];

  size_t num_offset_bytes = num_offsets * sizeof(int32_t);
  size_t num_values_bytes = num_values * sizeof(uint64_t);

  auto values_buf = arrow::Buffer::Wrap(values, num_values_bytes);
  auto offsets_buf = arrow::Buffer::Wrap(offsets, num_offset_bytes);
  auto value_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, values_buf);
  auto offsets_array =
      std::make_shared<arrow::PrimitiveArray>(arrow::int32(), num_offsets, offsets_buf);
  auto list_array = arrow::ListArray::FromArrays(*offsets_array, *value_array);

  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array.ValueOrDie()};
  *out = arrow::RecordBatch::Make(std::move(schema), num_rows, arrays);

  return true;
}

static fletcher::Status WriteMMIO(fletcher::Platform *platform,
                                  uint64_t offset,
                                  uint32_t value,
                                  size_t idx,
                                  std::string desc = "") {
  spdlog::info("{} writing {} to offset {} ({}) {}",
               idx,
               value,
               offset,
               64 + 4 * offset,
               desc);
  return platform->WriteMMIO(offset, value);
}

static fletcher::Status ReadMMIO(fletcher::Platform *platform,
                                 uint64_t offset,
                                 uint32_t *value,
                                 size_t idx,
                                 std::string desc = "") {
  spdlog::info("{} reading from offset {} ({}) {}",
               idx,
               offset,
               64 + 4 * offset,
               desc);
  return platform->ReadMMIO(offset, value);
}

bool OpaeTripReportParser::SetInput(RawJSONBuffer *in, size_t tag) {
  platform_mutex->lock();
  ParsedBuffer result;
  // rewrite the input last index because of opae limitations.
  FLETCHER_ROE(WriteMMIO(platform_,
                         input_lastidx_offset(idx_),
                         in->size_,
                         idx_,
                         "input last idx"));

  dau_t input_addr;
  input_addr.full = h2d_addr_map->at(in->data_);

  FLETCHER_ROE(WriteMMIO(platform_,
                         input_values_lo_offset(idx_),
                         input_addr.lo,
                         idx_,
                         "input values addr lo"));
  FLETCHER_ROE(WriteMMIO(platform_,
                         input_values_hi_offset(idx_),
                         input_addr.hi,
                         idx_,
                         "input values addr hi"));

  FLETCHER_ROE(WriteMMIO(platform_,
                         ctrl_offset(idx_),
                         tag,
                         idx_,
                         "tag address"));
  platform_mutex->unlock();
  return true;
}

bool OpaeTripReportParserManager::ParseAll(ParsedBuffer *out) {
  ParsedBuffer result;

  // Start kernel
  kernel->Start();

  //Wait for finish
  kernel->PollUntilDone();

  // Grab the return value (number of parsed JSON objects)
  // and wrap the output recordbatch.
  uint32_t return_value_0;
  uint32_t return_value_1;
  FLETCHER_ROE(kernel->GetReturn(&return_value_0, &return_value_1));
  uint64_t num_rows = ((uint64_t)return_value_1 << 32) | return_value_0;

  std::cout << "Number of records parsed: " << num_rows << std::endl;

  auto arrow_status = WrapTripReport(num_rows, output_arrays_sw, output_schema_sw(), &result.batch);
  if (!arrow_status.ok())
  {
    std::cerr << "Could not create output recordbatch." << std::endl;
    std::cerr << arrow_status.ToString() << std::endl;
    return -1;
  }
  *out = result;
  return true;
}











