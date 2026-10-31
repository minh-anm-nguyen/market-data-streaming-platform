cmake_minimum_required(VERSION 3.22)
project(md_adapter
        VERSION 0.1.0
        DESCRIPTION "Project 03 - C++20 FX/CFD Market Data Adapter (MDP1 / marketdata.v1)"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(cmake/CompilerWarnings.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/Protobuf.cmake)

option(MD_ADAPTER_BUILD_TESTS "Build the test suite" ON)
option(MD_ADAPTER_BUILD_FUZZERS "Build libFuzzer targets (clang only)" OFF)

# ---------------------------------------------------------------------------
# Generated protobuf contract (from the pinned P02 schema; never forked here).
# ---------------------------------------------------------------------------
md_adapter_generate_proto(
  PROTO       ${CMAKE_CURRENT_SOURCE_DIR}/schemas/market_data.proto
  OUT_DIR     ${CMAKE_CURRENT_BINARY_DIR}/generated
  SRC_VAR     MD_PROTO_SRCS
  HDR_VAR     MD_PROTO_HDRS
  DESC_VAR    MD_PROTO_DESC)

add_library(md_contract STATIC ${MD_PROTO_SRCS} ${MD_PROTO_HDRS})
target_include_directories(md_contract SYSTEM PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}/generated ${Protobuf_INCLUDE_DIRS})
target_link_libraries(md_contract PUBLIC ${Protobuf_LIBRARIES} Threads::Threads)
# Generated code is third-party output: do not apply the project warning set.

# ---------------------------------------------------------------------------
# Core adapter library
# ---------------------------------------------------------------------------
add_library(md_adapter_core STATIC
  src/status.cpp
  src/clock.cpp
  src/config.cpp
  src/toml_lite.cpp
  src/mdp1_framer.cpp
  src/protobuf_decoder.cpp
  src/structural_validator.cpp
  src/business_validator.cpp
  src/sequence_tracker.cpp
  src/quote_freshness.cpp
  src/classifier.cpp
  src/bounded_queue.cpp
  src/sink.cpp
  src/mdp1_file_source.cpp
  src/mdp1_tcp_client.cpp
  src/metrics.cpp
  src/report.cpp
  src/sha256.cpp
  src/pipeline.cpp
  src/runner.cpp)

target_include_directories(md_adapter_core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(md_adapter_core PUBLIC md_contract)
target_compile_definitions(md_adapter_core PUBLIC
  MD_ADAPTER_VERSION="${PROJECT_VERSION}"
  MD_PROTO_DESCRIPTOR_SHA256="${MD_PROTO_DESC}")
md_adapter_set_warnings(md_adapter_core)
md_adapter_apply_sanitizers(md_adapter_core)

add_executable(md_adapter src/main.cpp)
target_link_libraries(md_adapter PRIVATE md_adapter_core)
md_adapter_set_warnings(md_adapter)
md_adapter_apply_sanitizers(md_adapter)

add_executable(md_adapter_bench src/bench_main.cpp)
target_link_libraries(md_adapter_bench PRIVATE md_adapter_core)
md_adapter_set_warnings(md_adapter_bench)
md_adapter_apply_sanitizers(md_adapter_bench)

if(MD_ADAPTER_BUILD_TESTS AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt)
  enable_testing()
  add_subdirectory(tests)
endif()

if(MD_ADAPTER_BUILD_FUZZERS)
  add_subdirectory(tests/fuzz)
endif()

message(STATUS "md_adapter: build=${CMAKE_BUILD_TYPE} sanitizer=${MD_ADAPTER_SANITIZER} "
               "protoc=${Protobuf_PROTOC_EXECUTABLE} descriptor_sha256=${MD_PROTO_DESC}")
