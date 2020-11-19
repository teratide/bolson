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

#define AFU_GUID "3d5ee416-6f30-42d1-8109-a453b5b8e29d";

#ifdef NDEBUG
#define PLATFORM "opae"
#else
#define PLATFORM "opae-ase"
#endif

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

  size_t buffer_size = 4096;
  uint8_t *value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(value_data, 2, buffer_size);
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {value_array};
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

  for (int i = 0; i < context->num_buffers(); i++)
  {
    auto view = fletcher::HexView();
    view.AddData(context->device_buffer(i).host_address, 64);
    std::cout << view.ToString() << std::endl;
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

  for (int i = 0; i < context->num_buffers(); i++)
  {
    auto view = fletcher::HexView();
    view.AddData(context->device_buffer(i).host_address, 64);
    std::cout << view.ToString() << std::endl;
  }

  return 0;
}