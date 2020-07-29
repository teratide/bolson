cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(flitter VERSION 0.0.1 LANGUAGES CXX)

find_package(Arrow 1.0 CONFIG REQUIRED)

include(FetchContent)

FetchContent_Declare(cmake-modules
  GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
  GIT_TAG         master
)
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)

FetchContent_Declare(rapidjson
  GIT_REPOSITORY  https://github.com/Tencent/rapidjson.git
  GIT_TAG         master
)
FetchContent_GetProperties(rapidjson)
if(NOT rapidjson_POPULATED)
  FetchContent_Populate(rapidjson)
endif()
include_directories("${rapidjson_SOURCE_DIR}/include")

FetchContent_Declare(CLI11
        GIT_REPOSITORY  https://github.com/CLIUtils/CLI11.git
        GIT_TAG         v1.9.1
        )
FetchContent_MakeAvailable(CLI11)

add_compile_unit(
  NAME flitter
  TYPE EXECUTABLE
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/flitter/main.cpp
    src/flitter/tweets.cpp
  DEPS
    arrow_shared
    CLI11::CLI11
)

compile_units()
