#!/usr/bin/env bash
# Configure + build one flavour.
#
#   ./scripts/build.sh [release|perf|debug|asan|ubsan|tsan|fuzz] [extra cmake args]
#
# The compiler choice per flavour is deliberate:
#   * sanitizers use g++, whose ASan/UBSan/TSan work against the libstdc++
#     installed here;
#   * libFuzzer is clang-only, and this clang cannot parse the newer
#     libstdc++ headers that are also installed, so the fuzz build is pinned
#     to the GCC 11 standard headers explicitly.
set -euo pipefail
source "$(dirname "$0")/env.sh"

flavour="${1:-release}"
shift || true

cc=g++
cmake_args=()
extra_flags=""

case "$flavour" in
  release)  build_type=RelWithDebInfo ;;
  perf)     build_type=Release ;;
  debug)    build_type=Debug ;;
  asan)     build_type=Debug
            cmake_args+=(-DMD_ADAPTER_SANITIZER=address+undefined) ;;
  ubsan)    build_type=Debug
            cmake_args+=(-DMD_ADAPTER_SANITIZER=undefined) ;;
  # GCC's libtsan on this host reports bogus races for a plain
  # mutex + condition_variable::wait_for (reproduced with a 25-line program
  # that clang's TSan passes cleanly), so the thread sanitizer uses clang.
  # That means the same libstdc++ header pin the fuzz build needs.
  tsan)     build_type=Debug
            cc=clang++
            cmake_args+=(-DMD_ADAPTER_SANITIZER=thread)
            if [[ -d /usr/include/c++/11 ]]; then
              extra_flags="-nostdinc++ -isystem /usr/include/c++/11 -isystem /usr/include/x86_64-linux-gnu/c++/11"
            fi ;;
  fuzz)     build_type=Debug
            cc=clang++
            cmake_args+=(-DMD_ADAPTER_BUILD_FUZZERS=ON
                         -DMD_ADAPTER_BUILD_TESTS=OFF
                         -DMD_ADAPTER_SANITIZER=address+undefined)
            if [[ -d /usr/include/c++/11 ]]; then
              extra_flags="-nostdinc++ -isystem /usr/include/c++/11 -isystem /usr/include/x86_64-linux-gnu/c++/11"
            fi ;;
  *) echo "unknown flavour: $flavour" >&2; exit 2 ;;
esac

# ThreadSanitizer cannot cope with this kernel's ASLR entropy ("unexpected
# memory mapping"), and CMake runs each test binary at build time to discover
# its cases. Disabling randomisation for the build covers that; the test run
# needs the same wrapper. Neither needs root.
runner=()
if [[ "$flavour" == "tsan" ]]; then
  runner=(setarch "$(uname -m)" -R)
fi

build_dir="$P03_BUILD_ROOT/$flavour"
cmake -S "$P03_ROOT" -B "$build_dir" -G Ninja \
  -DCMAKE_BUILD_TYPE="$build_type" \
  -DCMAKE_CXX_COMPILER="$cc" \
  -DCMAKE_CXX_FLAGS="$extra_flags" \
  -DP03_PROTOBUF_ROOT="$P03_PROTOBUF_ROOT" \
  "${cmake_args[@]}" "$@" >/dev/null
"${runner[@]}" cmake --build "$build_dir" -j "$(nproc)"
echo "built: $build_dir"
