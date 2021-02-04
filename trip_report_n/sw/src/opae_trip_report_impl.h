#pragma once

#include <thread>
#include <mutex>
#include <memory>
#include <utility>
#include <arrow/api.h>
#include <fletcher/api.h>

#include "./opae_allocator.h"

#ifdef NDEBUG
#define FLETCHER_PLATFORM "opae"
#else
#define FLETCHER_PLATFORM "opae-ase"
//#define FLETCHER_PLATFORM "echo"
#endif

#define OPAE_BATTERY_AFU_ID "9ca43fb0-c340-4908-b79b-5c89b4ef5e01"

/// A structure to manage multi-buffered client implementation.
struct RawJSONBuffer {
  /// A pointer to the buffer.
  byte *data_ = nullptr;
  /// The number of valid bytes in the buffer.
  size_t size_ = 0;
  /// The capacity of the buffer.
  size_t capacity_ = 0;
};

using AddrMap = std::unordered_map<const byte *, da_t>;

class OpaeTripReportParser {
 public:
  OpaeTripReportParser(fletcher::Platform *platform,
                       fletcher::Context *context,
                       fletcher::Kernel *kernel,
                       AddrMap *addr_map,
                       size_t parser_idx,
                       size_t num_parsers,
                       std::mutex *platform_mutex)
      : platform_(platform),
        context_(context),
        kernel_(kernel),
        h2d_addr_map(addr_map),
        idx_(parser_idx),
        num_parsers(num_parsers),
        platform_mutex(platform_mutex) {}

  bool SetInput(RawJSONBuffer *in, size_t tag);

 private:
  // Fletcher default regs (unused):
  // 0 control
  // 1 status
  // 2 return lo
  // 3 return hi
  static constexpr size_t default_regs = 4;

  // Arrow input ranges
  // 0 input firstidx
  // 1 input lastidx

  static constexpr size_t input_range_regs_per_inst = 2;

    // 0 output firstidx
    // 1 output lastidx
  static constexpr size_t output_range_regs = 2;

  // 0 input val addr lo
  // 1 input val addr hi
  static constexpr size_t in_addr_regs_per_inst = 2;

  // Arrow address registers for the output buffers
  static constexpr size_t out_addr_regs= 42;

  // Custom regs per instance:
  // 0 tag
  static constexpr size_t custom_regs_per_inst = 1;

  size_t custom_regs_offset() const {
    return default_regs + num_parsers
        * (input_range_regs_per_inst +in_addr_regs_per_inst) + output_range_regs + out_addr_regs;
  }

  size_t ctrl_offset(size_t idx) {
    return custom_regs_offset() + custom_regs_per_inst * idx;
  }
  size_t status_offset(size_t idx) { return ctrl_offset(idx) + 1; }

  size_t result_rows_offset_lo(size_t idx) {
    return status_offset(idx) + 1;
  }
  size_t result_rows_offset_hi(size_t idx) {
    return result_rows_offset_lo(idx) + 1;
  }

  size_t input_firstidx_offset(size_t idx) {
    return default_regs + input_range_regs_per_inst * idx;
  }

  size_t input_lastidx_offset(size_t idx) {
    return input_firstidx_offset(idx) + 1;
  }

  size_t input_values_lo_offset(size_t idx) {
    return default_regs +  input_range_regs_per_inst * num_parsers + 2
        + in_addr_regs_per_inst * idx;
  }

  size_t input_values_hi_offset(size_t idx) {
    return input_values_lo_offset(idx) + 1;
  }

  size_t idx_;
  size_t tag;
  size_t num_parsers;
  fletcher::Platform *platform_;
  fletcher::Context *context_;
  fletcher::Kernel *kernel_;
  AddrMap *h2d_addr_map;
  std::mutex *platform_mutex;
};

struct OpaeBatteryOptions {
  std::string afu_id = OPAE_BATTERY_AFU_ID;
};

class OpaeTripReportParserManager {
 public:
  static bool Make(const OpaeBatteryOptions &opts,
                   const std::vector<RawJSONBuffer *> &buffers,
                   size_t num_parsers,
                   std::shared_ptr<OpaeTripReportParserManager> *out);

  bool ParseAll(std::shared_ptr<arrow::RecordBatch> *out);
  bool num_parsers() const { return num_parsers_; }
  std::vector<std::shared_ptr<OpaeTripReportParser>> parsers() { return parsers_; }
 private:
  bool PrepareInputBatches(const std::vector<RawJSONBuffer *> &buffers);
  bool PrepareOutputBatch();
  bool PrepareParsers();

  OpaeBatteryOptions opts_;

  std::unordered_map<const byte *, da_t> h2d_addr_map;

  size_t num_parsers_;
  OpaeAllocator allocator;
  // We create different views for teh host Sw and Fletcher, since Fletcher
  // currently doesn't support fixed size lists.
  // In case of 'output_arrays_hw', we wrap the buffers behind fixed size list fields
  // as primitive arrays.
  std::vector<std::shared_ptr<arrow::Array>> output_arrays_sw;
  std::vector<std::shared_ptr<arrow::Array>> output_arrays_hw;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_in;
  std::shared_ptr<arrow::RecordBatch> batch_out_sw;
    std::shared_ptr<arrow::RecordBatch> batch_out_hw;

  std::shared_ptr<fletcher::Platform> platform;
  std::shared_ptr<fletcher::Context> context;
  std::shared_ptr<fletcher::Kernel> kernel;

  std::vector<std::shared_ptr<OpaeTripReportParser>> parsers_;

  std::mutex platform_mutex;
};


