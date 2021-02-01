cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(bolson VERSION 0.0.1 LANGUAGES CXX)

find_package(Threads REQUIRED)
find_package(Arrow 3.0.0 CONFIG REQUIRED)
find_library(pulsar 2.7.0)

include(FetchContent)

# CMake Modules
FetchContent_Declare(cmake-modules
  GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
  GIT_TAG         master
)
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)

# RapidJSON
FetchContent_Declare(rapidjson
  GIT_REPOSITORY  https://github.com/Tencent/rapidjson.git
  GIT_TAG         v1.1.0
)
FetchContent_GetProperties(rapidjson)
if (NOT rapidjson_POPULATED)
  FetchContent_Populate(rapidjson)
endif ()
include_directories("${rapidjson_SOURCE_DIR}/include")

# CLI11
FetchContent_Declare(CLI11
  GIT_REPOSITORY  https://github.com/CLIUtils/CLI11.git
  GIT_TAG         v1.9.1
)
FetchContent_MakeAvailable(CLI11)

# spdlog
FetchContent_Declare(spdlog
  GIT_REPOSITORY  https://github.com/gabime/spdlog
  GIT_TAG         v1.7.0
)
FetchContent_MakeAvailable(spdlog)

# concurrentqueue
FetchContent_Declare(concurrentqueue
  GIT_REPOSITORY  https://github.com/cameron314/concurrentqueue.git
  GIT_TAG         v1.0.2
)
FetchContent_GetProperties(concurrentqueue)
if (NOT concurrentqueue_POPULATED)
  FetchContent_Populate(concurrentqueue)
endif ()
include_directories("${concurrentqueue_SOURCE_DIR}")

# putong
FetchContent_Declare(putong
  GIT_REPOSITORY  https://github.com/abs-tudelft/putong
  GIT_TAG         master
)
FetchContent_MakeAvailable(putong)

# illex
FetchContent_Declare(illex
  GIT_REPOSITORY  https://github.com/teratide/illex
  GIT_TAG         master
)
FetchContent_MakeAvailable(illex)

FetchContent_Declare(fletcher
  GIT_REPOSITORY  https://github.com/abs-tudelft/fletcher
  GIT_TAG         arrow-3.0
)
FetchContent_MakeAvailable(fletcher)

add_compile_unit(
  NAME bolson::obj
  TYPE OBJECT
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/bolson/utils.cpp
    src/bolson/pulsar.cpp
    src/bolson/cli.cpp
    src/bolson/stream.cpp
    src/bolson/bench.cpp
    src/bolson/latency.cpp
    src/bolson/buffer/allocator.cpp
    src/bolson/buffer/opae_allocator.cpp
    src/bolson/parse/parser.cpp
    src/bolson/parse/arrow_impl.cpp
    src/bolson/parse/opae_battery_impl.cpp
    src/bolson/convert/stats.cpp
    src/bolson/convert/resizer.cpp
    src/bolson/convert/serializer.cpp
    src/bolson/convert/converter.cpp
  TSTS
    test/bolson/convert/test_opae_battery.cpp
  DEPS
    arrow_shared
    CLI11::CLI11
    pulsar
    spdlog::spdlog
    Threads::Threads
    illex::static
    putong
    fletcher
)

add_compile_unit(
  NAME bolson
  TYPE EXECUTABLE
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/bolson/main.cpp
  DEPS
    bolson::obj
)

compile_units()
