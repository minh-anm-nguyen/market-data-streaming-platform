# Strict warning set. The adapter parses hostile bytes: arithmetic and
# conversion warnings are treated as defects, not style noise.
function(md_adapter_set_warnings target)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${target} PRIVATE
      -Wall -Wextra -Wpedantic
      -Wconversion -Wsign-conversion
      -Wshadow -Wnon-virtual-dtor -Woverloaded-virtual
      -Wcast-align -Wcast-qual -Wformat=2
      -Wdouble-promotion -Wimplicit-fallthrough
      -Wnull-dereference -Wold-style-cast
      -Wunused
      $<$<BOOL:${MD_ADAPTER_WERROR}>:-Werror>)
  endif()
endfunction()

option(MD_ADAPTER_WERROR "Treat warnings as errors" ON)
