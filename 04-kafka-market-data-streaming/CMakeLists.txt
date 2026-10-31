cmake_minimum_required(VERSION 3.16)
project(p04_kafka_market_data_streaming CXX)

# C++20 as the design specifies. Note that g++ 11 (Ubuntu 22.04) implements
# enough of C++20 for this code but not std::format, so nothing here uses it.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

# Warnings are errors in this project. A narrowing conversion on a price field
# or an unused delivery-report variable is the kind of defect this codebase is
# specifically trying not to ship.
add_compile_options(-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion)

option(P04_ENABLE_ASAN "Build with AddressSanitizer and UBSan" OFF)
if(P04_ENABLE_ASAN)
  # Milestone M2 requires an ASan run over the asynchronous callback ownership
  # path, which is where a delivery-report opaque outliving its context would
  # show up.
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

# ---------------------------------------------------------------------------
# Pinned dependencies.
#
# Paths rather than find_package for protobuf and librdkafka: both are built
# from source into a known prefix by deploy/setup-wsl-deps.sh precisely so the
# version is pinned, and letting CMake search the system would defeat that by
# silently preferring a distribution package of a different version.
# ---------------------------------------------------------------------------
set(P04_DEPS "/opt/p04-deps" CACHE PATH "Prefix holding the pinned dependencies")
set(PROTOBUF_ROOT "${P04_DEPS}/protobuf" CACHE PATH "protobuf install prefix")
set(RDKAFKA_ROOT "${P04_DEPS}/librdkafka" CACHE PATH "librdkafka install prefix")

find_program(PROTOC_EXECUTABLE protoc
             HINTS "${PROTOBUF_ROOT}/bin" NO_DEFAULT_PATH)
if(NOT PROTOC_EXECUTABLE)
  find_program(PROTOC_EXECUTABLE protoc)
endif()
if(NOT PROTOC_EXECUTABLE)
  message(FATAL_ERROR "protoc not found; run deploy/setup-wsl-deps.sh first")
endif()

find_library(PROTOBUF_LIBRARY NAMES protobuf
             HINTS "${PROTOBUF_ROOT}/lib" NO_DEFAULT_PATH)
if(NOT PROTOBUF_LIBRARY)
  find_library(PROTOBUF_LIBRARY NAMES protobuf)
endif()
find_path(PROTOBUF_INCLUDE_DIR google/protobuf/message.h
          HINTS "${PROTOBUF_ROOT}/include" NO_DEFAULT_PATH)
if(NOT PROTOBUF_INCLUDE_DIR)
  find_path(PROTOBUF_INCLUDE_DIR google/protobuf/message.h)
endif()
if(NOT PROTOBUF_LIBRARY OR NOT PROTOBUF_INCLUDE_DIR)
  message(FATAL_ERROR "libprotobuf not found under ${PROTOBUF_ROOT}")
endif()

find_library(RDKAFKA_LIBRARY NAMES rdkafka
             HINTS "${RDKAFKA_ROOT}/lib" NO_DEFAULT_PATH)
if(NOT RDKAFKA_LIBRARY)
  find_library(RDKAFKA_LIBRARY NAMES rdkafka)
endif()
find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h
          HINTS "${RDKAFKA_ROOT}/include" NO_DEFAULT_PATH)
if(NOT RDKAFKA_INCLUDE_DIR)
  find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h)
endif()
if(NOT RDKAFKA_LIBRARY OR NOT RDKAFKA_INCLUDE_DIR)
  message(FATAL_ERROR "librdkafka not found under ${RDKAFKA_ROOT}")
endif()

message(STATUS "protoc:      ${PROTOC_EXECUTABLE}")
message(STATUS "libprotobuf: ${PROTOBUF_LIBRARY}")
message(STATUS "librdkafka:  ${RDKAFKA_LIBRARY}")

# ---------------------------------------------------------------------------
# Generated schema code.
#
# Generated at build time from the pinned .proto rather than checked in, so the
# generated code can never drift from schemas/market_data.proto. The descriptor
# set is also emitted, because the Python verifier loads it instead of carrying
# its own generated module: one descriptor, two runtimes (section 10.1 rule 4).
# ---------------------------------------------------------------------------
set(SCHEMA_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/schemas/market_data.proto")
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")
set(GENERATED_SOURCES "${GENERATED_DIR}/market_data.pb.cc" "${GENERATED_DIR}/market_data.pb.h")
set(DESCRIPTOR_SET "${GENERATED_DIR}/market_data.desc")

add_custom_command(
  OUTPUT ${GENERATED_SOURCES} ${DESCRIPTOR_SET}
  COMMAND ${PROTOC_EXECUTABLE}
          --proto_path "${CMAKE_CURRENT_SOURCE_DIR}/schemas"
          --cpp_out "${GENERATED_DIR}"
          --descriptor_set_out "${DESCRIPTOR_SET}"
          --include_imports --include_source_info
          "${SCHEMA_PROTO}"
  DEPENDS "${SCHEMA_PROTO}"
  COMMENT "protoc: generating market_data.pb.{h,cc} and the descriptor set"
  VERBATIM)

add_custom_target(p04_schema DEPENDS ${GENERATED_SOURCES} ${DESCRIPTOR_SET})

# The generated code is compiled without the project warning set: protobuf 3.21
# emits code that trips -Wconversion, and turning the project warnings off for
# our own code to accommodate it would be the wrong trade.
add_library(p04_generated STATIC "${GENERATED_DIR}/market_data.pb.cc")
add_dependencies(p04_generated p04_schema)
target_include_directories(p04_generated PUBLIC "${GENERATED_DIR}")
target_include_directories(p04_generated SYSTEM PUBLIC "${PROTOBUF_INCLUDE_DIR}")
target_link_libraries(p04_generated PUBLIC "${PROTOBUF_LIBRARY}")
target_compile_options(p04_generated PRIVATE -w)
find_package(Threads REQUIRED)
target_link_libraries(p04_generated PUBLIC Threads::Threads)

# ---------------------------------------------------------------------------
# The core library: everything that is testable without a broker.
# ---------------------------------------------------------------------------
add_library(p04_core STATIC
  src/handoff/output_event.cpp
  src/ops/error_class.cpp
  src/ops/sha256.cpp
  src/ops/metrics.cpp
  src/ops/config.cpp
  src/ops/run_manifest.cpp
  src/routing/partition_policy.cpp
  src/routing/route_table.cpp
  src/policy/publish_gate.cpp
  src/queue/bounded_handoff.cpp
  src/serialization/marketdata_codec.cpp
  src/ledger/publish_status.cpp
  src/producer/kafka_transport.cpp
  src/producer/fake_transport.cpp
  src/source/p01_source.cpp
  src/source/p03_source.cpp
  src/app/publisher.cpp
)
target_include_directories(p04_core PUBLIC src "${GENERATED_DIR}")
target_link_libraries(p04_core PUBLIC p04_generated Threads::Threads)

# The librdkafka-dependent part, kept separate so the unit suite can link
# against p04_core alone and prove it needs no broker at all.
add_library(p04_kafka STATIC
  src/producer/rdkafka_producer.cpp
  src/ops/topic_guard.cpp
)
target_include_directories(p04_kafka PUBLIC src)
target_include_directories(p04_kafka SYSTEM PUBLIC "${RDKAFKA_INCLUDE_DIR}")
target_link_libraries(p04_kafka PUBLIC p04_core "${RDKAFKA_LIBRARY}")

add_executable(p04_publisher src/app/main.cpp)
target_link_libraries(p04_publisher PRIVATE p04_kafka)
set_target_properties(p04_publisher PROPERTIES
  BUILD_RPATH "${RDKAFKA_ROOT}/lib"
  INSTALL_RPATH "${RDKAFKA_ROOT}/lib")

add_executable(p04_group_verifier verifier/cpp_group_verifier/group_verifier_main.cpp)
target_link_libraries(p04_group_verifier PRIVATE p04_kafka)
set_target_properties(p04_group_verifier PROPERTIES
  BUILD_RPATH "${RDKAFKA_ROOT}/lib"
  INSTALL_RPATH "${RDKAFKA_ROOT}/lib")

# ---------------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------------
include(CTest)
option(P04_BUILD_TESTS "Build the unit and property suites" ON)
if(P04_BUILD_TESTS)
  find_package(GTest)
  if(GTest_FOUND)
    enable_testing()
    # CONFIGURE_DEPENDS so a newly added test file is picked up by the next build
    # rather than silently not running, which is how a test suite quietly
    # stops covering what it claims to cover.
    file(GLOB UNIT_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/tests/unit/*.cpp")
    file(GLOB PROPERTY_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/tests/property/*.cpp")
    add_executable(p04_unit_tests ${UNIT_TEST_SOURCES} ${PROPERTY_TEST_SOURCES})
    target_link_libraries(p04_unit_tests PRIVATE p04_kafka GTest::gtest GTest::gtest_main)
    target_include_directories(p04_unit_tests PRIVATE src tests)
    set_target_properties(p04_unit_tests PROPERTIES
      BUILD_RPATH "${RDKAFKA_ROOT}/lib")
    add_test(NAME unit COMMAND p04_unit_tests)
  else()
    message(WARNING "GTest not found; the C++ test suites will not be built")
  endif()
endif()
