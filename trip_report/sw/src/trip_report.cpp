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

  // Int + boolean fields
  uint8_t *timezone_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(timezone_data, 2, buffer_size);
  auto timezone_buffer = std::make_shared<arrow::Buffer>(timezone_data, buffer_size);
  auto timezone_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  timezone_buffer);

  uint8_t *vin_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(vin_data, 2, buffer_size);
  auto vin_buffer = std::make_shared<arrow::Buffer>(vin_data, buffer_size);
  auto vin_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  vin_buffer);

  uint8_t *odometer_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(odometer_data, 2, buffer_size);
  auto odometer_buffer = std::make_shared<arrow::Buffer>(odometer_data, buffer_size);
  auto odometer_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  odometer_buffer);

  uint8_t *avgspeed_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(avgspeed_data, 2, buffer_size);
  auto avgspeed_buffer = std::make_shared<arrow::Buffer>(avgspeed_data, buffer_size);
  auto avgspeed_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  avgspeed_buffer);

  uint8_t *accel_decel_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(accel_decel_data, 2, buffer_size);
  auto accel_decel_buffer = std::make_shared<arrow::Buffer>(accel_decel_data, buffer_size);
  auto accel_decel_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  accel_decel_buffer);

  uint8_t *speed_changes_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(speed_changes_data, 2, buffer_size);
  auto speed_changes_buffer = std::make_shared<arrow::Buffer>(speed_changes_data, buffer_size);
  auto speed_changes_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  speed_changes_buffer);

  uint8_t *hypermiling_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(hypermiling_data, 2, buffer_size);
  auto hypermiling_buffer = std::make_shared<arrow::Buffer>(hypermiling_data, buffer_size);
  auto hypermiling_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  hypermiling_buffer);

  uint8_t *orientation_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(orientation_data, 2, buffer_size);
  auto orientation_buffer = std::make_shared<arrow::Buffer>(orientation_data, buffer_size);
  auto orientation_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0,  orientation_buffer);


  // Integer array fields
  uint8_t *sec_in_band_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(sec_in_band_offset_data, 0, buffer_size);
  auto sec_in_band_offset_buffer = std::make_shared<arrow::Buffer>(sec_in_band_offset_data, buffer_size);
  uint8_t *sec_in_band_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto sec_in_band_value_buffer = std::make_shared<arrow::Buffer>(sec_in_band_value_data, buffer_size);
  auto sec_in_band_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, sec_in_band_value_buffer);
  auto sec_in_band_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, sec_in_band_offset_buffer, sec_in_band_value_array);


  uint8_t *miles_in_time_range_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(miles_in_time_range_offset_data, 0, buffer_size);
  auto miles_in_time_range_offset_buffer = std::make_shared<arrow::Buffer>(miles_in_time_range_offset_data, buffer_size);
  uint8_t *miles_in_time_range_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto miles_in_time_range_value_buffer = std::make_shared<arrow::Buffer>(miles_in_time_range_value_data, buffer_size);
  auto miles_in_time_range_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, miles_in_time_range_value_buffer);
  auto miles_in_time_range_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, miles_in_time_range_offset_buffer, miles_in_time_range_value_array);


  uint8_t *const_speed_miles_in_band_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(const_speed_miles_in_band_offset_data, 0, buffer_size);
  auto const_speed_miles_in_band_offset_buffer = std::make_shared<arrow::Buffer>(const_speed_miles_in_band_offset_data, buffer_size);
  uint8_t *const_speed_miles_in_band_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto const_speed_miles_in_band_value_buffer = std::make_shared<arrow::Buffer>(const_speed_miles_in_band_value_data, buffer_size);
  auto const_speed_miles_in_band_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, const_speed_miles_in_band_value_buffer);
  auto const_speed_miles_in_band_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, const_speed_miles_in_band_offset_buffer, const_speed_miles_in_band_value_array);


  uint8_t *vary_speed_miles_in_band_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(vary_speed_miles_in_band_offset_data, 0, buffer_size);
  auto vary_speed_miles_in_band_offset_buffer = std::make_shared<arrow::Buffer>(vary_speed_miles_in_band_offset_data, buffer_size);
  uint8_t *vary_speed_miles_in_band_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto vary_speed_miles_in_band_value_buffer = std::make_shared<arrow::Buffer>(vary_speed_miles_in_band_value_data, buffer_size);
  auto vary_speed_miles_in_band_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, vary_speed_miles_in_band_value_buffer);
  auto vary_speed_miles_in_band_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, vary_speed_miles_in_band_offset_buffer, vary_speed_miles_in_band_value_array);


  uint8_t *sec_decel_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(sec_decel_offset_data, 0, buffer_size);
  auto sec_decel_offset_buffer = std::make_shared<arrow::Buffer>(sec_decel_offset_data, buffer_size);
  uint8_t *sec_decel_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto sec_decel_value_buffer = std::make_shared<arrow::Buffer>(sec_decel_value_data, buffer_size);
  auto sec_decel_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, sec_decel_value_buffer);
  auto sec_decel_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, sec_decel_offset_buffer, sec_decel_value_array);


  uint8_t *sec_accel_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(sec_accel_offset_data, 0, buffer_size);
  auto sec_accel_offset_buffer = std::make_shared<arrow::Buffer>(sec_accel_offset_data, buffer_size);
  uint8_t *sec_accel_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto sec_accel_value_buffer = std::make_shared<arrow::Buffer>(sec_accel_value_data, buffer_size);
  auto sec_accel_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, sec_accel_value_buffer);
  auto sec_accel_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, sec_accel_offset_buffer, sec_accel_value_array);


  uint8_t *braking_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(braking_offset_data, 0, buffer_size);
  auto braking_offset_buffer = std::make_shared<arrow::Buffer>(braking_offset_data, buffer_size);
  uint8_t *braking_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto braking_value_buffer = std::make_shared<arrow::Buffer>(braking_value_data, buffer_size);
  auto braking_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, braking_value_buffer);
  auto braking_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, braking_offset_buffer, braking_value_array);


  uint8_t *accel_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(accel_offset_data, 0, buffer_size);
  auto accel_offset_buffer = std::make_shared<arrow::Buffer>(accel_offset_data, buffer_size);
  uint8_t *accel_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto accel_value_buffer = std::make_shared<arrow::Buffer>(accel_value_data, buffer_size);
  auto accel_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, accel_value_buffer);
  auto accel_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, accel_offset_buffer, accel_value_array);


  uint8_t *small_speed_var_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(small_speed_var_offset_data, 0, buffer_size);
  auto small_speed_var_offset_buffer = std::make_shared<arrow::Buffer>(small_speed_var_offset_data, buffer_size);
  uint8_t *small_speed_var_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto small_speed_var_value_buffer = std::make_shared<arrow::Buffer>(small_speed_var_value_data, buffer_size);
  auto small_speed_var_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, small_speed_var_value_buffer);
  auto small_speed_var_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, small_speed_var_offset_buffer, small_speed_var_value_array);


  uint8_t *large_speed_var_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(large_speed_var_offset_data, 0, buffer_size);
  auto large_speed_var_offset_buffer = std::make_shared<arrow::Buffer>(large_speed_var_offset_data, buffer_size);
  uint8_t *large_speed_var_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto large_speed_var_value_buffer = std::make_shared<arrow::Buffer>(large_speed_var_value_data, buffer_size);
  auto large_speed_var_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, large_speed_var_value_buffer);
  auto large_speed_var_list_array = std::make_shared<arrow::ListArray>(arrow::list(arrow::uint64()), 0, large_speed_var_offset_buffer, large_speed_var_value_array);

  // That lonely string field
  uint8_t *timestamp_offset_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  memset(timestamp_offset_data, 0, buffer_size);
  auto timestamp_offset_buffer = std::make_shared<arrow::Buffer>(timestamp_offset_data, buffer_size);
  uint8_t *timestamp_value_data = (uint8_t *)memalign(sysconf(_SC_PAGESIZE), buffer_size);
  auto timestamp_value_buffer = std::make_shared<arrow::Buffer>(timestamp_value_data, buffer_size);
  auto timestamp_value_array = std::make_shared<arrow::PrimitiveArray>(arrow::uint64(), 0, timestamp_value_buffer);
  auto timestamp_string_array = std::make_shared<arrow::StringArray>(0, timestamp_offset_buffer, timestamp_value_buffer);


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