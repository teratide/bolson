#include <arrow/api.h>
#include <fletcher/api.h>

#include <memory>
#include <iostream>

#define AFU_GUID "bcdf53bc-4ffb-4933-bf7c-4a7e1602c6e2";

#ifdef NDEBUG
#define PLATFORM "opae"
#else
#define PLATFORM "opae-ase"
#endif

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        std::cerr << "Incorrect number of arguments. Usage: sum path/to/recordbatch.rb" << std::endl;
        return -1;
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    fletcher::ReadRecordBatchesFromFile(argv[1], &batches);
    if (batches.size() != 1)
    {
        std::cerr << "File did not contain any Arrow RecordBatches." << std::endl;
        return -1;
    }
    std::shared_ptr<arrow::RecordBatch> number_batch;
    number_batch = batches[0];

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

    status = context->QueueRecordBatch(number_batch);
    if (!status.ok())
    {
        std::cerr << "Could not add recordbatch." << std::endl;
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
    kernel.PollUntilDone();
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
        std::cerr << "Could not obtain the return value." << std::endl;
        return -1;
    }

    std::cout << *reinterpret_cast<int32_t *>(&return_value_0) << std::endl;

    return 0;
}
