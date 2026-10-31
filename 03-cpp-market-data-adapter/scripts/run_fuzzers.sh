#!/usr/bin/env bash
# Runs each libFuzzer target for a bounded time budget.
#
#   ./scripts/run_fuzzers.sh [seconds_per_target]
#
# Any crash leaves a minimised reproducer in $corpus_dir/crashes, which must
# be turned into a regression test before the finding is considered closed.
set -uo pipefail
source "$(dirname "$0")/env.sh"

secs="${1:-60}"
log_dir="${P03_LOG_DIR:-$HOME/p03logs}"
corpus_dir="${P03_CORPUS_DIR:-$HOME/p03corpus}"
mkdir -p "$log_dir" "$corpus_dir/crashes"
fail=0

if ! bash "$P03_ROOT/scripts/build.sh" fuzz > "$log_dir/build-fuzz.log" 2>&1; then
  echo "fuzz build FAILED -> fuzzing: NOT_RUN"
  grep -E "error:" "$log_dir/build-fuzz.log" | head -10
  exit 1
fi

build_dir="$P03_BUILD_ROOT/fuzz"
for target in fuzz_framer fuzz_decoder fuzz_pipeline fuzz_config; do
  bin="$build_dir/tests/fuzz/$target"
  [[ -x "$bin" ]] || bin="$build_dir/$target"
  if [[ ! -x "$bin" ]]; then
    echo "$target: NOT_RUN (binary not found)"
    fail=1
    continue
  fi
  mkdir -p "$corpus_dir/$target"
  echo "== $target (${secs}s) =="
  if "$bin" "$corpus_dir/$target" \
        -max_total_time="$secs" -max_len=65536 -rss_limit_mb=2048 \
        -artifact_prefix="$corpus_dir/crashes/${target}-" \
        > "$log_dir/$target.log" 2>&1; then
    echo "  clean: $(grep -oE 'Done [0-9]+ runs in [0-9]+ second' \
                      "$log_dir/$target.log" | tail -1)"
  else
    echo "  CRASH or timeout -- see $log_dir/$target.log"
    tail -20 "$log_dir/$target.log"
    fail=1
  fi
done

echo
echo "corpus in $corpus_dir, logs in $log_dir"
exit $fail
