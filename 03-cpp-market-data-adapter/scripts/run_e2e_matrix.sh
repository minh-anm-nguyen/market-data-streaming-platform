#!/usr/bin/env bash
# Builds the release flavour and runs the full verification set:
#   unit + property + integration (ctest), then the subprocess e2e matrix.
#
# Prints a summary and exits non-zero if anything failed.
set -uo pipefail
source "$(dirname "$0")/env.sh"

fail=0
log_dir="${P03_LOG_DIR:-$HOME/p03logs}"
mkdir -p "$log_dir"

echo "== build (release) =="
if ! bash "$P03_ROOT/scripts/build.sh" release > "$log_dir/build.log" 2>&1; then
  echo "BUILD FAILED; see $log_dir/build.log"
  grep -E "error:" "$log_dir/build.log" | head -20
  exit 1
fi

build_dir="$P03_BUILD_ROOT/release"

echo "== ctest =="
if ctest --test-dir "$build_dir" --output-on-failure -E e2e_matrix \
        --timeout 300 > "$log_dir/ctest.log" 2>&1; then
  grep -E "tests passed" "$log_dir/ctest.log"
else
  fail=1
  echo "CTEST FAILED"
  sed -n '/The following tests FAILED/,$p' "$log_dir/ctest.log" | head -30
fi

echo "== e2e matrix =="
export MD_ADAPTER_BIN="$build_dir/md_adapter"
export MD_PROTOC="${MD_PROTOC:-$P03_PROTOBUF_ROOT/bin/protoc}"
if python3 "$P03_ROOT/tests/e2e/run_matrix.py" > "$log_dir/e2e.log" 2>&1; then
  tail -1 "$log_dir/e2e.log"
else
  fail=1
  echo "E2E MATRIX FAILED"
  sed -n '/failed cases/,$p' "$log_dir/e2e.log" | head -30
fi

echo
echo "logs in $log_dir"
exit $fail
