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

#define AFU_GUID "9ca43fb0-c340-4908-b79b-5c89b4ef5eed";

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
  if (argc != 3)
  {
    std::cerr << "Incorrect number of arguments. Usage: battery_status path/to/input_recordbatch.rb path/to/output_recordbatch.rb" << std::endl;
    return -1;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  fletcher::ReadRecordBatchesFromFile(argv[1], &batches);
  if (batches.size() != 1)
  {
    std::cerr << "File did not contain any input Arrow RecordBatches." << std::endl;
    return -1;
  }
  std::shared_ptr<arrow::RecordBatch> input_batch;
  input_batch = batches[0];

  // read output schema
  auto file = arrow::io::ReadableFile::Open(argv[2]).ValueOrDie();
  std::shared_ptr<arrow::Schema> schema = arrow::ipc::ReadSchema(file.get(), nullptr)
                                              .ValueOrDie();
  file->Close();

  // Allocate output buffers
  size_t buffer_size = 4096;
  uint8_t *offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  if (offset_data == nullptr)
  {
    std::cerr << "Failed to allocate offset buffer" << std::endl;
    return -1;
  }
  memset(offset_data, 0, buffer_size);
  auto offset_buffer = std::make_shared<arrow::Buffer>(offset_data, buffer_size);

  uint8_t *value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  if (value_data == nullptr)
  {
    std::cerr << "Failed to allocate value buffer" << std::endl;
    return -1;
  }
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer);

  auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, offset_buffer, value_array);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {list_array};
  auto output_batch = arrow::RecordBatch::Make(schema, 0, arrays);

  fletcher::Status status;
  std::shared_ptr<fletcher::Platform> platform;

  status = fletcher::Platform::Make(PLATFORM, &platform, false);
  if (!status.ok())
  {
    std::cerr << "Could not create Fletcher platform." << std::endl;
    std::cerr << status.message << std::endl;
    return -1;
  }

  static const char *guid = AFU_GUID;
  platform->init_data = &guid;
  status = platform->Init();
  if (!status.ok())
  {
    std::cerr << "Could not initialize platform." << std::endl;
    std::cerr << status.message << std::endl;
    return -1;
  }

  std::shared_ptr<fletcher::Context> context;
  status = fletcher::Context::Make(&context, platform);
  if (!status.ok())
  {
    std::cerr << "Could not create Fletcher context." << std::endl;
    return -1;
  }

  status = context->QueueRecordBatch(input_batch);
  if (!status.ok())
  {
    std::cerr << "Could not add input recordbatch." << std::endl;
    return -1;
  }

  status = context->QueueRecordBatch(output_batch);
  if (!status.ok())
  {
    std::cerr << "Could not add output recordbatch." << std::endl;
    return -1;
  }

  status = context->Enable();
  if (!status.ok())
  {
    std::cerr << "Could not enable the context." << std::endl;
    return -1;
  }

  fletcher::Kernel kernel(context);
  status = kernel.Start();
  if (!status.ok())
  {
    std::cerr << "Could not start the kernel." << std::endl;
    return -1;
  }

  status = kernel.PollUntilDone();
  if (!status.ok())
  {
    std::cerr << "Something went wrong waiting for the kernel to finish." << std::endl;
    return -1;
  }

  uint32_t return_value_0;
  uint32_t return_value_1;
  status = kernel.GetReturn(&return_value_0, &return_value_1);
  if (!status.ok())
  {
    std::cerr << "Failed to get return value." << std::endl;
    return -1;
  }
  uint64_t num_rows = ((uint64_t)return_value_1 << 32) | return_value_0;

  std::cout << "Number of records parsed: " << num_rows << std::endl;

  arrow::Status arrow_status;
  arrow_status = WrapBattery(num_rows, offset_data, value_data, schema, &output_batch);
  if (!arrow_status.ok())
  {
    std::cerr << "Could not create output recordbatch." << std::endl;
    std::cerr << arrow_status.ToString() << std::endl;
    return -1;
  }

  std::cout << output_batch->ToString() << std::endl;

  return 0;
}