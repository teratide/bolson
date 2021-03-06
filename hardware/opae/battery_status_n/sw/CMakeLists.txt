cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(battery_status VERSION 0.0.0 LANGUAGES CXX)

include(FetchContent)
FetchContent_Declare(cmake-modules
    GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
    GIT_TAG         master
    )
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)

# spdlog
FetchContent_Declare(spdlog
  GIT_REPOSITORY  https://github.com/gabime/spdlog
  GIT_TAG         v1.7.0
  )
FetchContent_MakeAvailable(spdlog)

# spdlog
FetchContent_Declare(fletcher
  GIT_REPOSITORY  https://github.com/abs-tudelft/fletcher
  GIT_TAG         0.0.19
  )
FetchContent_MakeAvailable(fletcher)

find_package(Arrow 3.0 CONFIG REQUIRED)

add_compile_unit(
    NAME battery_status
    TYPE EXECUTABLE
    PRPS
      CXX_STANDARD 11
      CXX_STANDARD_REQUIRED ON
    SRCS
      src/test.cpp
      src/opae_allocator.cpp
      src/opae_battery_impl.cpp
    DEPS
      arrow_shared
      fletcher
      spdlog::spdlog
)

compile_units()
