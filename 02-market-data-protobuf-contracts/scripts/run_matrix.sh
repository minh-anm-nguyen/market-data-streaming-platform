#!/usr/bin/env bash
# Run everything that produces evidence, in the order the gates depend on it.
#
# Fails fast on purpose: there is no value in benchmarking a build whose schema
# gate rejected the contract, and a report assembled from a partial run would be
# the "false success" the design argues against.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="/c/msys64/ucrt64/bin:$PATH"
export PYTHONPATH="$ROOT/python/src"
PY="${PYTHON:-python}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
SOURCE_RUN="${SOURCE_RUN:-../01-market-data-simulator/runs/smoke-rec-1}"
FAULT_RUN="${FAULT_RUN:-../01-market-data-simulator/runs/p02-business-faults}"

step() { printf '\n=== %s ===\n' "$1"; }

step "1. codegen from the single source schema"
bash scripts/generate_proto.sh >/dev/null
bash scripts/generate_proto.sh --check-stale

step "2. toolchain lock and artifact hashes"
"$PY" -m md_contract toolchain-check >/dev/null && echo "toolchain lock holds"

step "3. schema evolution gate"
"$PY" -m md_contract schema-gate --candidate build/descriptors/marketdata-v1_1.desc >/dev/null \
  && echo "v1.1 additive change: ALLOWED"
for variant in breaking_tag_reuse breaking_type_change breaking_unsigned_seq breaking_removed_field; do
  desc="build/descriptors/variants/$variant.desc"
  [[ -f "$desc" ]] || protoc -I "schemas/evolution/$variant" \
      --descriptor_set_out="$desc" marketdata/v1/market_data.proto
  # Inverted: the gate MUST reject these, so a zero exit here is the failure.
  if "$PY" -m md_contract schema-gate --candidate "$desc" >/dev/null 2>&1; then
    echo "GATE FAILURE: $variant was not blocked"; exit 1
  fi
  echo "$variant: BLOCKED as required"
done

step "4. C++ build and unit suite"
cmake -S cpp -B build/cpp -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_COMPILER=/c/msys64/ucrt64/bin/g++.exe \
  -DMD_EXPECTED_PROTOBUF_VERSION=35.1.0 >/dev/null
cmake --build build/cpp >/dev/null
mkdir -p reports
./build/cpp/md_cpp_tests.exe --json reports/cpp-cases.json | tail -2

step "5. source dataset verification"
"$PY" -m md_contract source-check --run-dir "$SOURCE_RUN" >/dev/null && echo "clean dataset verified"
"$PY" -m md_contract source-check --run-dir "$FAULT_RUN" >/dev/null && echo "fault dataset verified"

step "6. python + cross-language test suite"
"$PY" -m pytest tests -q

step "7. cross-language and version conformance matrix"
"$PY" -m md_contract cross-lang-conformance --source "$SOURCE_RUN/events.jsonl" \
  --work-dir build/conformance --limit 2000 --include-versions \
  --report reports/interop.json >/dev/null && echo "matrix written to reports/interop.json"

step "8. strict vs fault_passthrough on the fault dataset"
"$PY" -m md_contract encode-jsonl --run-dir "$FAULT_RUN" \
  --output build/reports/fault.pbstream --mode fault_passthrough >/dev/null \
  && echo "fault_passthrough preserved every manifest-backed anomaly"
# Inverted again: strict mode must reject them, so exit 0 would be the failure.
if "$PY" -m md_contract encode-jsonl --run-dir "$FAULT_RUN" \
     --output build/reports/strict.pbstream --mode strict_normal >/dev/null 2>&1; then
  echo "POLICY FAILURE: strict_normal accepted a business violation"; exit 1
fi
echo "strict_normal rejected them as required"

step "9. benchmark lanes and load ladder"
"$PY" -m md_contract benchmark --source "$SOURCE_RUN/events.jsonl" \
  --work-dir build/benchmark --lanes --ladder --duration-s 3 --sample-events 5000 \
  --report reports/benchmark.json >/dev/null && echo "written to reports/benchmark.json"

step "10. assemble the section 15 report"
"$PY" -m md_contract report --run-id "$RUN_ID" --output "reports/test-report.json" \
  --include reports/interop.json reports/benchmark.json \n  reports/what-if-cases.json reports/cpp-cases.json >/dev/null
"$PY" - <<'PYEOF'
import json, pathlib
d = json.loads(pathlib.Path("reports/test-report.json").read_text(encoding="utf-8"))
print(f"  status: {d['status']}   release_verdict: {d['release_verdict']}")
print(f"  what-if: {d['what_if_counts']}")
for blocker in d["release_blockers"]:
    print(f"  blocker: {blocker}")
PYEOF
