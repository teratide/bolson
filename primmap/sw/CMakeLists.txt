cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

include(FetchContent)
FetchContent_Declare(cmake-modules
    GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
    GIT_TAG         master
    )
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)

project(primmap VERSION 0.0.0 LANGUAGES CXX)

find_package(Arrow 1.0 CONFIG REQUIRED)
find_package(fletcher CONFIG)

add_compile_unit(
    NAME primmap
    TYPE EXECUTABLE
    PRPS
      CXX_STANDARD 11
      CXX_STANDARD_REQUIRED ON
      CXX_DEBUG_FLAGS "-g"
    SRCS
      src/primmap.cpp
    DEPS
      arrow_shared
      fletcher
)

compile_units()
