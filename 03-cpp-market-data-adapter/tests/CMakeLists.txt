find_package(GTest REQUIRED)
include(GoogleTest)

# Shared helpers for building wire bytes and configs inside tests.
add_library(md_test_support STATIC support/test_support.cpp)
target_include_directories(md_test_support PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/support)
target_link_libraries(md_test_support PUBLIC md_adapter_core GTest::gtest)
# Tests that shell out to the Python tooling need to find the source tree.
target_compile_definitions(md_test_support PUBLIC
  MD_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
md_adapter_apply_sanitizers(md_test_support)

function(md_add_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE
    md_test_support GTest::gtest GTest::gtest_main)
  md_adapter_set_warnings(${name})
  md_adapter_apply_sanitizers(${name})
  gtest_discover_tests(${name}
    PROPERTIES TIMEOUT 120
    DISCOVERY_TIMEOUT 60)
endfunction()

md_add_test(test_sha256         unit/test_sha256.cpp)
md_add_test(test_toml_lite      unit/test_toml_lite.cpp)
md_add_test(test_config         unit/test_config.cpp)
md_add_test(test_framer         unit/test_framer.cpp)
md_add_test(test_decoder        unit/test_decoder.cpp)
md_add_test(test_validators     unit/test_validators.cpp)
md_add_test(test_sequence       unit/test_sequence_tracker.cpp)
md_add_test(test_freshness      unit/test_freshness.cpp)
md_add_test(test_classifier     unit/test_classifier.cpp)
md_add_test(test_bounded_queue  unit/test_bounded_queue.cpp)
md_add_test(test_metrics        unit/test_metrics.cpp)
md_add_test(test_sink           unit/test_sink.cpp)
md_add_test(test_oracle_parity  unit/test_reference_oracle_parity.cpp)

md_add_test(test_chunk_invariance property/test_chunk_invariance.cpp)
md_add_test(test_state_bounds     property/test_state_bounds.cpp)

md_add_test(test_offline_e2e    integration/test_offline_e2e.cpp)
md_add_test(test_tcp_live       integration/test_tcp_live.cpp)

# The end-to-end matrix drives the real binary through the shell, including
# the cases that can only be observed from outside the process (exit codes,
# .part artifacts, signals, backpressure).
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
  add_test(NAME e2e_matrix
           COMMAND ${CMAKE_COMMAND} -E env
                   MD_ADAPTER_BIN=$<TARGET_FILE:md_adapter>
                   MD_BUILD_DIR=${CMAKE_BINARY_DIR}
                   # The matrix generates its fixtures with the same pinned
                   # protoc the build used, not whatever is on PATH.
                   MD_PROTOC=${Protobuf_PROTOC_EXECUTABLE}
                   ${Python3_EXECUTABLE}
                   ${CMAKE_CURRENT_SOURCE_DIR}/e2e/run_matrix.py
           WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  set_tests_properties(e2e_matrix PROPERTIES TIMEOUT 900)
endif()
