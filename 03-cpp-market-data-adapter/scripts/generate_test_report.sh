#!/usr/bin/env bash
# Collects one verification report from everything that actually ran.
#
#   ./scripts/generate_test_report.sh [output.md]
#
# The rule it enforces: a lane that did not execute is written NOT_RUN, never
# omitted and never implied to be clean. A reader must be able to tell the
# difference between "passed" and "never tried".
set -uo pipefail
source "$(dirname "$0")/env.sh"

out="${1:-$P03_ROOT/reports/verification-$(date -u +%Y%m%dT%H%M%SZ).md}"
log_dir="${P03_LOG_DIR:-$HOME/p03logs}"
mkdir -p "$(dirname "$out")"

verdict_from_ctest() {
  local f="$1"
  if [[ ! -f "$f" ]]; then echo "NOT_RUN|no log at $f"; return; fi
  local line
  line=$(grep -E "tests passed" "$f" | tail -1)
  if [[ -z "$line" ]]; then echo "NOT_RUN|log has no ctest summary"; return; fi
  if grep -q "100% tests passed" "$f"; then echo "PASS|$line"
  else echo "FAIL|$line"; fi
}

verdict_from_e2e() {
  local f="$1"
  if [[ ! -f "$f" ]]; then echo "NOT_RUN|no log at $f"; return; fi
  local line
  line=$(grep -E "^matrix:" "$f" | tail -1)
  if [[ -z "$line" ]]; then echo "NOT_RUN|log has no matrix summary"; return; fi
  if echo "$line" | grep -q " 0 failed"; then echo "PASS|$line"
  else echo "FAIL|$line"; fi
}

row() {
  local name="$1" result="$2"
  printf '| %s | `%s` | %s |\n' "$name" "${result%%|*}" "${result#*|}"
}

{
  echo "# Verification report"
  echo
  echo "- generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- host: $(uname -srm)"
  echo "- cpu: $(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo)"
  echo "- cores: $(nproc)"
  echo "- compiler: $(g++ --version | head -1)"
  echo "- protoc: $("$P03_PROTOBUF_ROOT/bin/protoc" --version 2>/dev/null || echo UNKNOWN)"
  if [[ -x "$P03_BUILD_ROOT/release/md_adapter" ]]; then
    echo "- descriptor: $("$P03_BUILD_ROOT/release/md_adapter" --print-descriptor 2>/dev/null)"
  fi
  echo
  echo "## Lanes"
  echo
  echo "| Lane | Verdict | Detail |"
  echo "|---|---|---|"
  row "unit + property + integration (ctest)" "$(verdict_from_ctest "$log_dir/ctest.log")"
  row "end-to-end matrix (real binary)" "$(verdict_from_e2e "$log_dir/e2e.log")"
  row "AddressSanitizer + UBSan" "$(verdict_from_ctest "$log_dir/ctest-asan.log")"
  row "ThreadSanitizer" "$(verdict_from_ctest "$log_dir/ctest-tsan.log")"
  echo
  echo "## Fuzzing"
  echo
  echo "| Target | Verdict | Executions |"
  echo "|---|---|---|"
  for t in fuzz_framer fuzz_decoder fuzz_pipeline fuzz_config; do
    f="$log_dir/$t.log"
    if [[ ! -f "$f" ]]; then
      printf '| %s | `NOT_RUN` | — |\n' "$t"
    elif grep -qE "ERROR: libFuzzer|SUMMARY: .*Sanitizer" "$f"; then
      printf '| %s | `FAIL` | see %s |\n' "$t" "$f"
    else
      printf '| %s | `PASS` | %s |\n' "$t" \
        "$(grep -oE 'Done [0-9]+ runs' "$f" | tail -1 | grep -oE '[0-9]+' || echo '?')"
    fi
  done
  echo
  echo "## Not established by this report"
  echo
  echo "- Sustainable throughput. Benchmark lanes measure a run, not a"
  echo "  steady state; see docs/benchmarks.md."
  echo "- Live-stream completeness. Unprovable without source-side evidence."
  echo "- Absence of bugs. Sanitizers and fuzzers bound the search, not the"
  echo "  defect set."
} > "$out"

echo "wrote $out"
cat "$out"
