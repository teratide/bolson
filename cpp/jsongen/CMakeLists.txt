cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(jsongen VERSION 0.0.1 LANGUAGES CXX)

find_package(Arrow 1.0 CONFIG REQUIRED)
find_library(zmqpp REQUIRED)
find_library(zmq REQUIRED)

include(FetchContent)

FetchContent_Declare(cmake-modules
  GIT_REPOSITORY https://github.com/abs-tudelft/cmake-modules.git
  GIT_TAG master
  )
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)

# rapidjson
FetchContent_Declare(rapidjson
  GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
  GIT_TAG master
  )
FetchContent_GetProperties(rapidjson)
if (NOT rapidjson_POPULATED)
  FetchContent_Populate(rapidjson)
  include_directories("${rapidjson_SOURCE_DIR}/include")
endif ()

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

# Common headers
include_directories(../flitter/include)

add_compile_unit(
  NAME jsongen::obj
  TYPE OBJECT
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/jsongen/cli.cpp
    src/jsongen/file.cpp
    src/jsongen/stream.cpp
    src/jsongen/arrow.cpp
    src/jsongen/document.cpp
    src/jsongen/value.cpp
  DEPS
    arrow_shared
    CLI11::CLI11
    spdlog::spdlog
    zmqpp
    zmq
  TSTS
    test/jsongen/test_arrow.cpp
    test/jsongen/test_gen.cpp
)

add_compile_unit(
  NAME jsongen
  TYPE EXECUTABLE
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/jsongen/main.cpp
  DEPS
    jsongen::obj
)

compile_units()
