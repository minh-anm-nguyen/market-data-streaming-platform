#!/bin/bash
# E2E-14, the rate ladder (section 7.2, NFR-07, ADR-12).
#
# Two kinds of stage, and the difference matters for what may be claimed:
#
#   paced stages      an offered rate is requested and the question is whether
#                     the publisher sustains it. The publisher computes the
#                     performance verdict by comparing the sustained
#                     acknowledged rate against the request.
#   unpaced stage     no rate is requested, so there is nothing to sustain. The
#                     result is a ceiling observation for this host, this
#                     dataset and this single-broker profile. It is not a
#                     benchmark and the performance verdict stays NOT_RUN.
#
# Section 7.2 is explicit that a short peak is not a sustained rate and that a
# failure to reach a target must be reported as the maximum actually achieved,
# with the reason. This script prints both and never converts one into the other.
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

PROJECT_DIR=${P04_PROJECT_DIR:-/mnt/d/trading/prj-3/04-kafka-market-data-streaming}
BUILD_DIR=${P04_BUILD_DIR:-/root/p04-build}
DEPS=${P04_DEPS:-/opt/p04-deps}
SOURCE_RUN=${P04_SOURCE_RUN:-/mnt/d/trading/prj-3/01-market-data-simulator/runs/p04-smoke}
OUT_DIR=${P04_OUT_DIR:-/root/p04-runs}
BOOTSTRAP=${P04_BOOTSTRAP_HOST:-127.0.0.1:9092}
STAGES=${P04_STAGES:-"0 1000 5000 20000"}
MAX_EVENTS=${P04_LADDER_EVENTS:-20000}

cd "$PROJECT_DIR" || exit 3
mkdir -p "$OUT_DIR"

# Stage the source dataset onto the local filesystem before measuring.
#
# This is not an optimisation, it is a correctness fix for the measurement. With
# the dataset left on the 9p mount that exposes the Windows filesystem to WSL,
# the same 20000-event run took 32.1 s and reported 623 acknowledged events/s;
# with the dataset on ext4 it took 1.66 s and reported 12044/s. The first figure
# measures the 9p round trip per read, not the publisher, and reporting it as a
# throughput result would be measuring the harness.
#
# Section 7.1 makes the same point in general: a stage boundary has to be where
# you think it is before a number from it means anything.
STAGED_SOURCE="${P04_STAGED_SOURCE:-/root/p04-source/$(basename "$SOURCE_RUN")}"
if [ ! -f "$STAGED_SOURCE/events.jsonl" ] ||    [ "$SOURCE_RUN/events.jsonl" -nt "$STAGED_SOURCE/events.jsonl" ]; then
  mkdir -p "$(dirname "$STAGED_SOURCE")"
  cp -r "$SOURCE_RUN" "$(dirname "$STAGED_SOURCE")/" || exit 3
  echo "staged the source dataset to $STAGED_SOURCE"
fi
SOURCE_RUN="$STAGED_SOURCE"

docker compose -f deploy/docker-compose.local.yml up -d >/dev/null 2>&1
for _ in $(seq 1 40); do
  docker exec p04-kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 2
done
bash deploy/topic-create.sh >/dev/null || exit 3

echo "host: $(nproc) cpus, $(awk '/MemTotal/{printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
echo "profile: local-dev, 1 broker, RF=1, 8 partitions per topic, compression lz4"
echo "dataset: $MAX_EVENTS events from $(basename "$SOURCE_RUN"), staged on $(df --output=fstype "$SOURCE_RUN" | tail -1 | tr -d ' ')"
echo

printf "%-10s %-10s %-12s %-8s %-8s %-9s %-8s %-11s %s\n" \
  "requested" "sustained" "perf" "p50_ms" "p99_ms" "max_queue" "rss_mb" "safety" "wall_s"

for rate in $STAGES; do
  run_id="ladder-${rate}-$(date +%H%M%S)"
  "$BUILD_DIR/p04_publisher" \
    --config config/local-dev.example.toml \
    --set "run.source_run_dir=\"$SOURCE_RUN\"" \
    --set "run.output_dir=\"$OUT_DIR\"" \
    --set "kafka.bootstrap_servers=\"$BOOTSTRAP\"" \
    --set "run.max_events=$MAX_EVENTS" \
    --set "run.target_events_per_second=$rate" \
    --run-id "$run_id" >/dev/null 2>&1
  report="$OUT_DIR/$run_id/report.json"
  if [ ! -f "$report" ]; then
    printf "%-10s %s\n" "$rate" "NO REPORT"
    continue
  fi
  "$DEPS/venv/bin/python" - "$report" "$rate" <<'PYEOF'
import json, sys
report = json.load(open(sys.argv[1]))
requested = sys.argv[2]
def fmt(value, digits=1):
    return "null" if value is None else f"{value:.{digits}f}"
rss = report.get("max_process_rss_bytes")
print("%-10s %-10s %-12s %-8s %-8s %-9s %-8s %-11s %s" % (
    "unpaced" if requested == "0" else requested,
    fmt(report.get("actual_sustained_acked_events_per_second"), 0),
    report.get("performance_verdict"),
    fmt(report.get("p50_ack_ms"), 1),
    fmt(report.get("p99_ack_ms"), 1),
    report.get("max_app_queue_records"),
    fmt(rss / 1048576 if rss else None, 1),
    report.get("safety_verdict"),
    fmt(report.get("wall_seconds"), 2),
))
PYEOF
done

echo
echo "Reading these numbers:"
echo "  * sustained is acknowledged records divided by wall time, including"
echo "    startup, so a short run understates it."
echo "  * p50 and p99 are handoff-to-acknowledgement on the steady clock"
echo "    (NFR-08), bucketed, so they report a bucket upper bound rather than an"
echo "    interpolated value."
echo "  * the unpaced row has performance_verdict NOT_RUN by design: nothing was"
echo "    requested, so nothing was sustained (ADR-12)."
echo "  * a single broker with RF=1 is not a capacity result for the 120k/s"
echo "    target in section 7.2, which needs the reliability-lab profile."
echo "  * the dataset is staged on a local filesystem first. Reading it across"
echo "    the 9p Windows mount instead costs about 19x and the resulting number"
echo "    measures the mount, not the publisher."
