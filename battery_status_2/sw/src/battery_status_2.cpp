#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <fletcher/api.h>
#include <fletcher/common.h>

#include <malloc.h>
#include <unistd.h>

#include <memory>
#include <iostream>
#include <bitset>

#define AFU_GUID "9ca43fb0-c340-4908-b79b-5c89b4ef5eee";

#ifdef NDEBUG
#define PLATFORM "opae"
#else
#define PLATFORM "opae-ase"
#endif

arrow::Status WrapBattery(
    int32_t num_rows,
    uint8_t *offsets,
    uint8_t *values,
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<arrow::RecordBatch> *out)
{
  auto ret = arrow::Status::OK();

  // +1 because the last value in offsets buffer is the next free index in the values
  // buffer.
  int32_t num_offsets = num_rows + 1;

  // Obtain the last value in the offsets buffer to know how many values there are.
  int32_t num_values = reinterpret_cast<int32_t *>(offsets)[num_offsets];

  size_t num_offset_bytes = num_offsets * sizeof(int32_t);
  size_t num_values_bytes = num_values * sizeof(uint64_t);

  // Wrap data into arrow buffers
  auto offsets_buf = arrow::Buffer::Wrap(offsets, num_offset_bytes);
  auto values_buf = arrow::Buffer::Wrap(values, num_values_bytes);

  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, values_buf);
  auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), num_rows, offsets_buf, value_array);

  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  *out = arrow::RecordBatch::Make(schema, num_rows, arrays);

  return arrow::Status::OK();
}

int main(int argc, char **argv)
{
  if (argc != 5)
  {
    std::cerr << "Incorrect number of arguments. Expected arguments: in_1.rb in_2.rb" << std::endl;
    return -1;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  fletcher::ReadRecordBatchesFromFile(argv[1], &batches);
  fletcher::ReadRecordBatchesFromFile(argv[2], &batches);
  std::shared_ptr<arrow::RecordBatch> input_batch_1;
  input_batch_1 = batches[0];
  std::shared_ptr<arrow::RecordBatch> input_batch_2;
  input_batch_2 = batches[1];

  auto file = arrow::io::ReadableFile::Open(argv[3]).ValueOrDie();
  std::shared_ptr<arrow::Schema> output_schema_1 = arrow::ipc::ReadSchema(file.get(), nullptr)
                                                       .ValueOrDie();
  file->Close();
  file = arrow::io::ReadableFile::Open(argv[4]).ValueOrDie();
  std::shared_ptr<arrow::Schema> output_schema_2 = arrow::ipc::ReadSchema(file.get(), nullptr)
                                                       .ValueOrDie();
  file->Close();

  size_t buffer_size = 4096;

  uint8_t *offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(offset_data, 0, buffer_size);
  auto offset_buffer = std::make_shared<arrow::Buffer>(offset_data, buffer_size);
  uint8_t *value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer);
  auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, offset_buffer, value_array);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  auto output_batch_1 = arrow::RecordBatch::Make(output_schema_1, 0, arrays);

  uint8_t *offset_data_2 = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(offset_data_2, 0, buffer_size);
  auto offset_buffer_2 = std::make_shared<arrow::Buffer>(offset_data_2, buffer_size);
  uint8_t *value_data_2 = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto value_buffer_2 = std::make_shared<arrow::Buffer>(value_data_2, buffer_size);
  auto value_array_2 = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer_2);
  auto list_array_2 = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, offset_buffer_2, value_array_2);
  std::vector<std::shared_ptr<arrow::Array>> arrays_2 = {list_array_2};
  auto output_batch_2 = arrow::RecordBatch::Make(output_schema_2, 0, arrays_2);

  fletcher::Status status;
  std::shared_ptr<fletcher::Platform> platform;
  status = fletcher::Platform::Make(PLATFORM, &platform, false);

  static const char *guid = AFU_GUID;
  platform->init_data = &guid;
  status = platform->Init();
  std::shared_ptr<fletcher::Context> context;
  status = fletcher::Context::Make(&context, platform);
  status = context->QueueRecordBatch(input_batch_1);
  status = context->QueueRecordBatch(input_batch_2);
  status = context->QueueRecordBatch(output_batch_1);
  status = context->QueueRecordBatch(output_batch_1);
  status = context->Enable();
  fletcher::Kernel kernel(context);
  status = kernel.Start();
  status = kernel.PollUntilDone();

  uint32_t return_value_0;
  uint32_t return_value_1;
  status = kernel.GetReturn(&return_value_0, &return_value_1);

  // uint64_t num_rows = ((uint64_t)return_value_1 << 32) | return_value_0;
  std::cout << "1 - Number of records parsed: " << return_value_0 << std::endl;
  std::cout << "2 - Number of records parsed: " << return_value_1 << std::endl;

  // arrow::Status arrow_status;
  // arrow_status = WrapBattery(num_rows, offset_data, value_data, schema, &output_batch);
  // if (!arrow_status.ok())
  // {
  //   std::cerr << "Could not create output recordbatch." << std::endl;
  //   std::cerr << arrow_status.ToString() << std::endl;
  //   return -1;
  // }

  // std::cout << output_batch->ToString() << std::endl;

  return 0;
}