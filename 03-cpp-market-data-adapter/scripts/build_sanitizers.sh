#!/usr/bin/env bash
# Builds and runs the suite under each sanitizer that this toolchain can
# actually provide, and says plainly which ones it could not.
#
# A sanitizer that did not run is reported as NOT_RUN, never as clean.
set -uo pipefail
source "$(dirname "$0")/env.sh"

log_dir="${P03_LOG_DIR:-$HOME/p03logs}"
mkdir -p "$log_dir"
fail=0

run_flavour() {
  local flavour="$1"; shift
  echo "== $flavour =="
  # ThreadSanitizer cannot cope with this kernel's ASLR entropy; see
  # scripts/build.sh. Disabling randomisation needs no root.
  local runner=()
  if [[ "$flavour" == "tsan" ]]; then
    runner=(setarch "$(uname -m)" -R)
  fi
  if ! bash "$P03_ROOT/scripts/build.sh" "$flavour" \
        > "$log_dir/build-$flavour.log" 2>&1; then
    echo "  BUILD FAILED -> $flavour: NOT_RUN"
    grep -E "error:" "$log_dir/build-$flavour.log" | head -5
    fail=1
    return
  fi
  if "${runner[@]}" ctest --test-dir "$P03_BUILD_ROOT/$flavour" \
        --output-on-failure -E e2e_matrix --timeout 900 \
        > "$log_dir/ctest-$flavour.log" 2>&1; then
    echo "  $flavour: CLEAN -- $(grep -E 'tests passed' "$log_dir/ctest-$flavour.log" | tail -1)"
  else
    echo "  $flavour: FAILURES"
    sed -n '/The following tests FAILED/,$p' "$log_dir/ctest-$flavour.log" | head -20
    fail=1
  fi
}

export ASAN_OPTIONS="detect_leaks=1:detect_stack_use_after_return=1"
export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1"
export TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1"

run_flavour asan
run_flavour tsan

echo
echo "logs in $log_dir"
exit $fail
