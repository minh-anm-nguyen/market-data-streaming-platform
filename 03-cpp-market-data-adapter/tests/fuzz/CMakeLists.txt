# libFuzzer targets. clang only; -fsanitize=fuzzer is not a GCC feature.
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(WARNING "MD_ADAPTER_BUILD_FUZZERS needs clang; skipping fuzz targets")
  return()
endif()

function(md_add_fuzzer name src)
  add_executable(${name} ${src})
  target_link_libraries(${name} PRIVATE md_adapter_core)
  target_compile_options(${name} PRIVATE -fsanitize=fuzzer)
  target_link_options(${name} PRIVATE -fsanitize=fuzzer)
  md_adapter_apply_sanitizers(${name})
endfunction()

md_add_fuzzer(fuzz_framer   fuzz_framer.cpp)
md_add_fuzzer(fuzz_decoder  fuzz_decoder.cpp)
md_add_fuzzer(fuzz_pipeline fuzz_pipeline.cpp)
md_add_fuzzer(fuzz_config   fuzz_config.cpp)
