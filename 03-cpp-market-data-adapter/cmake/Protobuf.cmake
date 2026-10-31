# Locates the pinned protobuf toolchain and generates the C++ contract.
#
# The toolchain is pinned, not discovered loosely: protoc and libprotobuf must
# come from the same install so generated code matches the runtime
# (protobuf cross-version runtime guarantee).
find_package(Threads REQUIRED)

set(P03_PROTOBUF_ROOT "" CACHE PATH
    "Root of a protobuf install (expects bin/protoc, include/, lib/)")

if(P03_PROTOBUF_ROOT)
  find_program(Protobuf_PROTOC_EXECUTABLE protoc
               HINTS ${P03_PROTOBUF_ROOT}/bin NO_DEFAULT_PATH REQUIRED)
  find_path(Protobuf_INCLUDE_DIRS google/protobuf/message.h
            HINTS ${P03_PROTOBUF_ROOT}/include NO_DEFAULT_PATH REQUIRED)
  # Prefer the static archive: a self-contained binary removes the need for
  # LD_LIBRARY_PATH at run time, which matters for CI and for the chaos
  # tests that re-exec the adapter under constrained environments.
  find_library(Protobuf_LIBRARIES NAMES libprotobuf.a protobuf
               HINTS ${P03_PROTOBUF_ROOT}/lib ${P03_PROTOBUF_ROOT}/lib/x86_64-linux-gnu
               NO_DEFAULT_PATH REQUIRED)
else()
  find_package(Protobuf REQUIRED)
endif()

# protoc 3.12 gates proto3 `optional` behind an experimental flag; >=3.15 does not.
execute_process(COMMAND ${Protobuf_PROTOC_EXECUTABLE} --version
                OUTPUT_VARIABLE _protoc_version_raw
                OUTPUT_STRIP_TRAILING_WHITESPACE)
string(REGEX MATCH "([0-9]+)\.([0-9]+)\.([0-9]+)" _m "${_protoc_version_raw}")
set(P03_PROTOC_VERSION "${CMAKE_MATCH_1}.${CMAKE_MATCH_2}.${CMAKE_MATCH_3}")
set(_proto3_optional_flag "")
if(CMAKE_MATCH_1 EQUAL 3 AND CMAKE_MATCH_2 LESS 15)
  set(_proto3_optional_flag "--experimental_allow_proto3_optional")
endif()

function(md_adapter_generate_proto)
  cmake_parse_arguments(A "" "PROTO;OUT_DIR;SRC_VAR;HDR_VAR;DESC_VAR" "" ${ARGN})
  get_filename_component(_dir ${A_PROTO} DIRECTORY)
  get_filename_component(_name ${A_PROTO} NAME_WE)
  file(MAKE_DIRECTORY ${A_OUT_DIR})
  set(_cc ${A_OUT_DIR}/${_name}.pb.cc)
  set(_h  ${A_OUT_DIR}/${_name}.pb.h)
  set(_desc ${A_OUT_DIR}/${_name}.desc)

  add_custom_command(
    OUTPUT ${_cc} ${_h} ${_desc}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE} ${_proto3_optional_flag}
            -I ${_dir} --cpp_out=${A_OUT_DIR}
            --descriptor_set_out=${_desc} ${A_PROTO}
    DEPENDS ${A_PROTO}
    COMMENT "protoc ${_name}.proto (protoc ${P03_PROTOC_VERSION})"
    VERBATIM)

  # Pin the descriptor digest at configure time when it already exists; the
  # runtime re-checks it against config, so a stale value fails loudly rather
  # than silently.
  set(_sha "UNPINNED")
  if(EXISTS ${_desc})
    file(SHA256 ${_desc} _sha)
  endif()

  set(${A_SRC_VAR} ${_cc} PARENT_SCOPE)
  set(${A_HDR_VAR} ${_h} PARENT_SCOPE)
  set(${A_DESC_VAR} ${_sha} PARENT_SCOPE)
endfunction()
