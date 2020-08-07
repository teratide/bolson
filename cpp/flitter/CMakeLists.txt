cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(flitter VERSION 0.0.1 LANGUAGES CXX)

find_package(Threads REQUIRED)
find_package(Arrow 1.0 CONFIG REQUIRED)
find_library(pulsar 2.6.0 REQUIRED NAMES libpulsar.dylib)
find_library(zmqpp REQUIRED)
find_library(zmq REQUIRED)

include(FetchContent)

# CMake Modules
FetchContent_Declare(cmake-modules
  GIT_REPOSITORY https://github.com/abs-tudelft/cmake-modules.git
  GIT_TAG master
)
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)

# RapidJSON
FetchContent_Declare(rapidjson
  GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
  GIT_TAG master
)
FetchContent_GetProperties(rapidjson)
if (NOT rapidjson_POPULATED)
  FetchContent_Populate(rapidjson)
endif ()
include_directories("${rapidjson_SOURCE_DIR}/include")

# CLI11
FetchContent_Declare(CLI11
  GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
  GIT_TAG v1.9.1
)
FetchContent_MakeAvailable(CLI11)

# spdlog
FetchContent_Declare(spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog
  GIT_TAG v1.7.0
)
FetchContent_MakeAvailable(spdlog)

# concurrentqueue
FetchContent_Declare(concurrentqueue
  GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
  GIT_TAG v1.0.1
)
FetchContent_GetProperties(concurrentqueue)
if (NOT concurrentqueue_POPULATED)
  FetchContent_Populate(concurrentqueue)
endif ()
include_directories("${concurrentqueue_SOURCE_DIR}")

add_compile_unit(
  NAME flitter
  TYPE EXECUTABLE
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/flitter/main.cpp
    src/flitter/tweets.cpp
    src/flitter/utils.cpp
    src/flitter/file.cpp
    src/flitter/pulsar.cpp
    src/flitter/cli.cpp
    src/flitter/hive.cpp
    src/flitter/stream.cpp
    src/flitter/bench.cpp
  DEPS
    arrow_shared
    CLI11::CLI11
    pulsar
    zmqpp
    zmq
    spdlog::spdlog
    Threads::Threads
)

include_directories(include)

compile_units()
