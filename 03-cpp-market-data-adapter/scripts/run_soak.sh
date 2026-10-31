#!/usr/bin/env bash
# Long-running soak against a live MDP1 feed.
#
#   ./scripts/run_soak.sh <hours> [events_per_sec]
#
# What it watches for is drift, not throughput: RSS that climbs, file
# descriptors that are never returned, a queue that never drains, counters
# that stop balancing. A soak that merely "did not crash" has not been read.
set -uo pipefail
source "$(dirname "$0")/env.sh"

hours="${1:-6}"
rate="${2:-10000}"
log_dir="${P03_LOG_DIR:-$HOME/p03logs}/soak-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$log_dir"

build_dir="$P03_BUILD_ROOT/perf"
if [[ ! -x "$build_dir/md_adapter" ]]; then
  bash "$P03_ROOT/scripts/build.sh" perf || exit 1
fi

fixture="${P03_SOAK_FIXTURE:-$P03_ROOT/fixtures/normal/golden.pbstream}"
if [[ ! -f "$fixture" ]]; then
  echo "no fixture at $fixture; run tools/make_fixtures.py first" >&2
  exit 2
fi

# Each session is one pass of the fixture; the feed loops for the duration.
sessions=$(( hours * 3600 * rate / 3000 + 1 ))
echo "soak: ${hours}h at ~${rate} events/s, $sessions sessions"
echo "logs: $log_dir"

python3 "$P03_ROOT/tools/tcp_chunker.py" --file "$fixture" --port 0 \
  --sessions "$sessions" --chunk 4096 \
  > "$log_dir/server.log" 2>&1 &
server_pid=$!
# The server prints "LISTENING <port>" before accepting.
for _ in $(seq 1 100); do
  port=$(awk '/^LISTENING/ {print $2; exit}' "$log_dir/server.log" 2>/dev/null)
  [[ -n "${port:-}" ]] && break
  sleep 0.1
done
if [[ -z "${port:-}" ]]; then
  echo "server did not start" >&2
  kill "$server_pid" 2>/dev/null
  exit 1
fi

"$build_dir/md_adapter" --config "$P03_ROOT/config/benchmark.toml" \
  --input-mode tcp --host 127.0.0.1 --port "$port" \
  --report "$log_dir/report.yaml" \
  > "$log_dir/adapter.log" 2>&1 &
adapter_pid=$!

# Sample resources on a fixed cadence so a trend is visible afterwards.
echo "unix_time,rss_kb,fds,threads" > "$log_dir/resources.csv"
deadline=$(( $(date +%s) + hours * 3600 ))
while kill -0 "$adapter_pid" 2>/dev/null && [[ $(date +%s) -lt $deadline ]]; do
  rss=$(awk '/VmRSS/ {print $2}' "/proc/$adapter_pid/status" 2>/dev/null)
  fds=$(ls "/proc/$adapter_pid/fd" 2>/dev/null | wc -l)
  thr=$(awk '/Threads/ {print $2}' "/proc/$adapter_pid/status" 2>/dev/null)
  echo "$(date +%s),${rss:-0},${fds:-0},${thr:-0}" >> "$log_dir/resources.csv"
  sleep 10
done

kill -TERM "$adapter_pid" 2>/dev/null
wait "$adapter_pid" 2>/dev/null
adapter_exit=$?
kill "$server_pid" 2>/dev/null
wait "$server_pid" 2>/dev/null

echo
echo "adapter exit: $adapter_exit"
python3 "$P03_ROOT/tools/verify_report.py" "$log_dir/report.yaml"
verify=$?

# Resource trend: compare the first and last tenth of the samples.
python3 - "$log_dir/resources.csv" <<'PY'
import statistics, sys, pathlib
rows = [r.split(",") for r in
        pathlib.Path(sys.argv[1]).read_text().splitlines()[1:] if r.strip()]
if len(rows) < 20:
    print("too few samples for a trend")
    raise SystemExit(0)
n = max(1, len(rows) // 10)
rss = [int(r[1]) for r in rows]
fds = [int(r[2]) for r in rows]
first, last = statistics.mean(rss[:n]), statistics.mean(rss[-n:])
growth = (last - first) / first * 100 if first else 0.0
print(f"samples: {len(rows)}")
print(f"rss first/last: {first/1024:.1f} MiB -> {last/1024:.1f} MiB "
      f"({growth:+.1f}%)")
print(f"fds min/max: {min(fds)} / {max(fds)}")
# These are observations, not a pass mark. A verdict needs a baseline for
# this workload on this machine.
if growth > 10:
    print("NOTE: RSS grew more than 10%; investigate before calling it clean")
if max(fds) > min(fds) + 8:
    print("NOTE: file-descriptor count drifted; check for leaks")
PY

exit $(( verify ))
