# C++20 build for the Project 02 contract tools.
#
# Generated sources are NOT compiled from a glob: they are listed, and the
# generation itself is done by scripts/generate_proto.sh so that one pinned
# protoc produces both languages from one schema. A glob would happily compile a
# stale .pb.cc left over from an older schema, which is case E08.
cmake_minimum_required(VERSION 3.20)
project(md_contract_cpp CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(Protobuf CONFIG REQUIRED)
message(STATUS "protobuf version: ${Protobuf_VERSION}")

# The C++ runtime and the gencode must come from the same protobuf release;
# there is no ABI promise across releases, so a mismatch is a build stop rather
# than a warning (cases E05, E07, NFR-10).
if(DEFINED MD_EXPECTED_PROTOBUF_VERSION AND
   NOT Protobuf_VERSION VERSION_EQUAL MD_EXPECTED_PROTOBUF_VERSION)
  message(FATAL_ERROR
    "protobuf ${Protobuf_VERSION} does not match the pinned "
    "${MD_EXPECTED_PROTOBUF_VERSION} from toolchain/versions.lock")
endif()

set(MD_GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/generated/v1")
set(MD_GENERATED_SOURCES
  "${MD_GENERATED_DIR}/marketdata/v1/market_data.pb.cc")

if(NOT EXISTS "${MD_GENERATED_DIR}/marketdata/v1/market_data.pb.h")
  message(FATAL_ERROR
    "generated code missing; run scripts/generate_proto.sh first")
endif()

add_library(md_contract STATIC
  src/projection.cpp
  src/framing.cpp
  src/strict_json.cpp
  src/codec.cpp
  ${MD_GENERATED_SOURCES})

target_include_directories(md_contract PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
  "${MD_GENERATED_DIR}")

target_link_libraries(md_contract PUBLIC protobuf::libprotobuf)

if(MSVC)
  target_compile_options(md_contract PRIVATE /W4)
else()
  target_compile_options(md_contract PRIVATE -Wall -Wextra -Wconversion -Wshadow)
endif()

foreach(tool md_cpp_decode md_cpp_encode md_cpp_tcp_client)
  add_executable(${tool} src/${tool}.cpp)
  target_link_libraries(${tool} PRIVATE md_contract)
endforeach()

if(WIN32)
  target_link_libraries(md_cpp_tcp_client PRIVATE ws2_32)
endif()

enable_testing()
add_executable(md_cpp_tests tests/md_cpp_tests.cpp)
target_link_libraries(md_cpp_tests PRIVATE md_contract)
add_test(NAME md_cpp_tests COMMAND md_cpp_tests)
