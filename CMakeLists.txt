cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(bolson VERSION 0.1.0 LANGUAGES CXX)

find_package(Threads REQUIRED)
find_package(Arrow 3.0.0 CONFIG REQUIRED)

include(FetchContent)

# CMake Modules
FetchContent_Declare(cmake-modules
  GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
  GIT_TAG         c48cf51ab44642c61259c0107a4f97d5abb1a604
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

# Putong
FetchContent_Declare(putong
  GIT_REPOSITORY  https://github.com/abs-tudelft/putong
  GIT_TAG         4a2bf644f899bb46dd8512edf95235280dc484bc
)
FetchContent_MakeAvailable(putong)

# Illex
FetchContent_Declare(illex
  GIT_REPOSITORY  https://github.com/teratide/illex
  GIT_TAG         v0.1.0
)
FetchContent_MakeAvailable(illex)

# Fletcher
FetchContent_Declare(fletcher
  GIT_REPOSITORY  https://github.com/abs-tudelft/fletcher
  GIT_TAG         0.0.19
)
FetchContent_MakeAvailable(fletcher)

add_compile_unit(
  NAME bolson::obj
  TYPE OBJECT
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/bolson/bench.cpp
    src/bolson/cli.cpp
    src/bolson/latency.cpp
    src/bolson/metrics.cpp
    src/bolson/status.cpp
    src/bolson/stream.cpp
    src/bolson/utils.cpp
    src/bolson/buffer/allocator.cpp
    src/bolson/buffer/opae_allocator.cpp
    src/bolson/buffer/fpga_allocator.cpp
    src/bolson/convert/converter.cpp
    src/bolson/convert/resizer.cpp
    src/bolson/convert/serializer.cpp
    src/bolson/convert/metrics.cpp
    src/bolson/parse/arrow.cpp
    src/bolson/parse/parser.cpp
    src/bolson/parse/custom/battery.cpp
    src/bolson/parse/custom/trip.cpp
    src/bolson/parse/fpga/battery.cpp
    src/bolson/parse/fpga/common.cpp
    src/bolson/parse/fpga/trip.cpp
    src/bolson/parse/opae/battery.cpp
    src/bolson/parse/opae/opae.cpp
    src/bolson/parse/opae/trip.cpp
    src/bolson/publish/bench.cpp
    src/bolson/publish/metrics.cpp
    src/bolson/publish/publisher.cpp
  TSTS
    test/bolson/convert/test_opae_battery.cpp
    test/bolson/convert/test_opae_trip.cpp
  DEPS
    arrow_shared
    CLI11::CLI11
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

execute_process (
    COMMAND bash -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
    OUTPUT_VARIABLE OS_NAME
)

execute_process (
    COMMAND bash -c "awk -F= '/^VERSION_ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
    OUTPUT_VARIABLE OS_VERSION
)

if(OS_NAME MATCHES "ubuntu")
  set(CPACK_DEBIAN_PACKAGE_RELEASE "ubuntu${OS_VERSION}")
  set(CPACK_GENERATOR "DEB")
elseif(OS_NAME MATCHES "centos")
  set(CPACK_RPM_PACKAGE_RELEASE_DIST "el${OS_VERSION}")
  if(OS_VERSION MATCHES "7")
    set(CPACK_GENERATOR "RPM;TGZ")
  else()
    set(CPACK_GENERATOR "RPM")
  endif()
endif()

set(CPACK_PACKAGE_VENDOR "Teratide")
set(CPACK_PACKAGE_VERSION_MAJOR "${bolson_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${bolson_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${bolson_VERSION_PATCH}")
set(CPACK_PACKAGE_RELOCATABLE ON)

set(CPACK_DEBIAN_PACKAGE_NAME "${CMAKE_PROJECT_NAME}")
set(CPACK_DEBIAN_FILE_NAME "DEB-DEFAULT")
# set(CPACK_DEBIAN_PACKAGE_DEPENDS "libarrow-dev (>= 3.0.0), libarrow-dev (<< 4.0.0)")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_VENDOR}")

set(CPACK_RPM_FILE_NAME "RPM-DEFAULT")
set(CPACK_RPM_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION}")
set(CPACK_RPM_PACKAGE_LICENSE "ASL 2.0")
# set(CPACK_RPM_PACKAGE_REQUIRES "arrow-libs >= 3.0.0, arrow-libs < 4.0.0")

set(CPACK_ARCHIVE_FILE_NAME "${CMAKE_PROJECT_NAME}-${bolson_VERSION}-${CMAKE_SYSTEM_NAME}")

include(CPack)
