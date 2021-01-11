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
  memset(value_data, 0, buffer_size);
  auto value_buffer = std::make_shared<arrow::Buffer>(value_data, buffer_size);
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, value_buffer);
  return arrow::ToResult(value_array);
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
  auto value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint8(), 0, value_buffer);
  auto string_array = std::make_shared<arrow::StringArray>(0, offset_buffer, value_buffer);
  return arrow::ToResult(string_array);
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


  // Int + boolean fields
  auto timezone_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();
  auto vin_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();
  auto odometer_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();
  auto avgspeed_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();
  auto accel_decel_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();
  auto speed_changes_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();
  auto hypermiling_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();
  auto orientation_array = AlignedPrimitiveArray(arrow::uint64(), buffer_size).ValueOrDie();

  // Integer array fields
  auto sec_in_band_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto miles_in_time_range_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto const_speed_miles_in_band_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto vary_speed_miles_in_band_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto sec_decel_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto sec_accel_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto braking_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto accel_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto small_speed_var_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();
  auto large_speed_var_list_array = AlignedListArray(arrow::uint64(), buffer_size, buffer_size).ValueOrDie();

  // That lonely string field

  auto timestamp_string_array = AlignedStringArray(buffer_size, buffer_size).ValueOrDie();


  std::vector<std::shared_ptr<arrow::Array>> arrays = {
          timestamp_string_array,
          timezone_array,
          vin_array,
          odometer_array,
          hypermiling_array,
          avgspeed_array,
          sec_in_band_list_array,
          miles_in_time_range_list_array,
          const_speed_miles_in_band_list_array,
          vary_speed_miles_in_band_list_array,
          sec_decel_list_array,
          sec_accel_list_array,
          braking_list_array,
          accel_list_array,
          orientation_array,
          small_speed_var_list_array,
          large_speed_var_list_array,
          accel_decel_array,
          speed_changes_array
  };
  auto output_batch = arrow::RecordBatch::Make(schema, 0, arrays);

  std::cout << schema->ToString();

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

//  for (int i = 0; i < context->num_buffers(); i++)
//  {
//    auto view = fletcher::HexView();
//    view.AddData(context->device_buffer(i).host_address, 64);
//    std::cout << view.ToString() << std::endl;
//  }

  return 0;
}