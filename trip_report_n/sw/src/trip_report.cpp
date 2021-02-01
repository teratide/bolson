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

#define AFU_GUID "5fcd29a3-1c65-4feb-907f-34c5ba4c8534";

#ifdef NDEBUG
#define PLATFORM "opae"
#else
#define PLATFORM "opae-ase"
//#define PLATFORM "echo"
#endif

arrow::Result<std::shared_ptr<arrow::PrimitiveArray>> AlignedPrimitiveArray(const std::shared_ptr<arrow::DataType>& type,
                                                                            size_t buffer_size) {
  uint8_t *value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  if (value_data == nullptr)
  {
    return arrow::Status::OutOfMemory("Failed to allocate buffer");
  }
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(type, 0, value_buffer);
  return value_array;
}

arrow::Result<std::shared_ptr<arrow::ListArray>> AlignedListArray(const std::shared_ptr<arrow::DataType>& type,
                                                                            size_t offset_buffer_size,
                                                                            size_t value_buffer_size) {
  uint8_t *offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), offset_buffer_size);
  if (offset_data == nullptr)
  {
    return arrow::Status::OutOfMemory("Failed to allocate offset buffer");
  }
  memset(offset_data, 0, offset_buffer_size);
  auto offset_buffer = std::make_shared<arrow::Buffer>(offset_data, offset_buffer_size);

  uint8_t *value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), value_buffer_size);
  if (value_data == nullptr)
  {
    return arrow::Status::OutOfMemory("Failed to allocate value buffer");
  }
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, value_buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer);

  auto list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, offset_buffer, value_array);
  return arrow::ToResult(list_array);
}

arrow::Result<std::shared_ptr<arrow::StringArray>> AlignedStringArray(size_t offset_buffer_size,
                                                                      size_t value_buffer_size) {
  uint8_t *offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), offset_buffer_size);
  if (offset_data == nullptr)
  {
    return arrow::Status::OutOfMemory("Failed to allocate buffer");
  }
  memset(offset_data, 0, offset_buffer_size);
  auto offset_buffer = std::make_shared<arrow::Buffer>(offset_data, offset_buffer_size);
  uint8_t *value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), value_buffer_size);
  if (value_data == nullptr)
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

std::shared_ptr<arrow::ListArray>ReWrapArray(std::shared_ptr<arrow::ListArray> array,
                                                                  size_t num_rows) {
  auto type = array->type();

  int32_t num_offsets = num_rows + 1;

  uint8_t *offsets = (uint8_t *)array->offsets()->data()->buffers[1]->data();
  uint8_t *values = const_cast<uint8_t *>(array->values()->data()->buffers[1]->data());

  uint32_t num_values = reinterpret_cast<uint32_t *>(offsets)[num_offsets-1];

  size_t num_values_bytes = num_values * sizeof(array->type().get());
  size_t num_offset_bytes = num_offsets * sizeof(uint32_t);

  auto value_buffer = arrow::Buffer::Wrap(values, num_values_bytes);
  auto offsets_buffer = arrow::Buffer::Wrap(offsets, num_offset_bytes);

  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), num_values, value_buffer);
  auto list_array = std::make_shared<arrow::ListArray>(type, num_rows, offsets_buffer, value_array);
  return list_array;
}

std::shared_ptr<arrow::StringArray>ReWrapArray(std::shared_ptr<arrow::StringArray> array,
                                             size_t num_rows) {
  auto string_array = std::make_shared<arrow::StringArray>(num_rows, array->value_offsets(), array->value_data());
  return string_array;
}

arrow::Status WrapTripReport(
        int32_t num_rows,
        std::vector<std::shared_ptr<arrow::Array>> arrays,
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<arrow::RecordBatch> *out)
{

  std::vector<std::shared_ptr<arrow::Array>> rewrapped_arrays;

  for(std::shared_ptr<arrow::Array> f: arrays) {
    std::cout << f->type()->ToString() << std::endl;
    if(f->type()->Equals(arrow::uint64())) {
      auto field = std::static_pointer_cast<arrow::PrimitiveArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    } else if(f->type()->Equals(arrow::uint8())) {
      auto field = std::static_pointer_cast<arrow::PrimitiveArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    } else if (f->type()->Equals(arrow::list(arrow::uint64()))) {
      auto field = std::static_pointer_cast<arrow::ListArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    } else if (f->type()->Equals(arrow::utf8())) {
      auto field = std::static_pointer_cast<arrow::StringArray>(f);
      rewrapped_arrays.push_back(ReWrapArray(field, num_rows));
    }
  }

  *out = arrow::RecordBatch::Make(schema, num_rows, rewrapped_arrays);

  return arrow::Status::OK();
}

int main(int argc, char **argv)
{
  if (argc != 3)
  {
    std::cerr << "Incorrect number of arguments. Usage: trip_report path/to/input_recordbatch.rb path/to/output_recordbatch.rb" << std::endl;
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

  std::vector<std::shared_ptr<arrow::Array>> arrays;

  for(auto f : schema->fields()) {
    if(f->type()->Equals(arrow::uint64())) {
      arrays.push_back(AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie());
    } else if(f->type()->Equals(arrow::uint8())) {
      arrays.push_back(AlignedPrimitiveArray(arrow::uint8(), buffer_size).ValueOrDie());
    } else if (f->type()->Equals(arrow::list(arrow::field("item", arrow::uint64(), false)))) {
      arrays.push_back(AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie());
    } else if (f->type()->Equals(arrow::utf8())) {
      arrays.push_back(AlignedStringArray(buffer_size, buffer_size).ValueOrDie());
    }
  }

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

  // uint32_t return_value_0;
  // uint32_t return_value_1;
  // uint64_t state = 0;
  // uint64_t num_rows = 0;
  // while(state != 96) {
  //   status = kernel.GetReturn(&return_value_0, &return_value_1);
  //   state = ((uint64_t)112) & ((uint64_t)return_value_0);
  //   num_rows = ((uint64_t)3) & ((uint64_t)return_value_0);
  //   uint64_t reg = ((uint64_t)return_value_1 << 32) | return_value_0;
  //   std::cout << "Result reg:" << std::hex << reg << std::endl;
  //   if (!status.ok())
  //   {
  //     std::cerr << "Failed to get return value." << std::endl;
  //     return -1;
  //   }
  //   usleep(1000000);
  // }


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

  auto arrow_status = WrapTripReport(num_rows, arrays, schema, &output_batch);
  if (!arrow_status.ok())
  {
    std::cerr << "Could not create output recordbatch." << std::endl;
    std::cerr << arrow_status.ToString() << std::endl;
    return -1;
  }

  std::cout << output_batch->ToString() << std::endl;

  return 0;
}