#!/bin/bash
# E2E-01: C++ producer -> one Kafka broker -> independent Python verifier
# (section 10.3, FR-01 to FR-16, NFR-01, NFR-02).
#
# The sequence is the one the design specifies, and the order matters:
#
#   1. a completed Project 01 dataset exists, with its COMPLETED marker and
#      digest, so there is a source oracle at all;
#   2. topics are provisioned through the Admin path with the pinned partition
#      count, replication factor and cleanup policy;
#   3. the publisher runs, polls every delivery report, and writes its ledger;
#   4. the source stops, offsets are captured, and the independent verifier
#      reads exactly the captured range;
#   5. only then is a verdict computed, from the verifier, not from the
#      publisher exit code alone.
#
# Exit code: 0 only when both the publisher and the verifier agree the run is
# clean. Anything else is non-zero, because a run that merely did not crash is
# not a pass (section 14).
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

PROJECT_DIR=${P04_PROJECT_DIR:-/mnt/d/trading/prj-3/04-kafka-market-data-streaming}
BUILD_DIR=${P04_BUILD_DIR:-/root/p04-build}
DEPS=${P04_DEPS:-/opt/p04-deps}
SOURCE_RUN=${P04_SOURCE_RUN:-/mnt/d/trading/prj-3/01-market-data-simulator/runs/p04-smoke}
RUN_ID=${P04_RUN_ID:-e2e01-$(date +%H%M%S)}
BOOTSTRAP=${P04_BOOTSTRAP_HOST:-127.0.0.1:9092}
# Run artifacts live on the Linux filesystem: /mnt/d is a 9p mount and the
# publisher fsyncs its report there, which is slow enough to distort a timing
# measurement.
OUT_DIR=${P04_OUT_DIR:-/root/p04-runs}

cd "$PROJECT_DIR" || exit 3
mkdir -p "$OUT_DIR"

step() { echo; echo "######## $* ########"; }

step "1. source dataset"
if [ ! -f "$SOURCE_RUN/COMPLETED" ]; then
  echo "FATAL: $SOURCE_RUN has no COMPLETED marker; it cannot be a completeness oracle"
  exit 3
fi
echo "source: $SOURCE_RUN"
grep -o '"dataset_id": *"[^"]*"' "$SOURCE_RUN/report.json" | head -1
wc -l < "$SOURCE_RUN/events.jsonl" | xargs echo "source events:"

step "2. kafka cluster"
docker compose -f deploy/docker-compose.local.yml up -d 2>&1 | tail -3
for attempt in $(seq 1 40); do
  if docker exec p04-kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "broker ready after ${attempt} checks"
    break
  fi
  sleep 2
done
docker exec p04-kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 >/dev/null 2>&1 || {
    echo "FATAL: broker never became ready"; docker compose -f deploy/docker-compose.local.yml logs --tail 30 kafka; exit 3; }

step "3. topic provisioning"
bash deploy/topic-create.sh || exit 3

step "4. capture begin offsets"
# Section 5.1: the scan must be bounded at both ends by explicitly captured
# offsets. The lower bound is taken here, before publishing, so the snapshot
# covers this run and not whatever an earlier run left on the topic.
mkdir -p "$OUT_DIR/$RUN_ID-pre"
"$DEPS/venv/bin/python" verifier/python_snapshot_verifier/snapshot_verifier.py   --bootstrap "$BOOTSTRAP"   --descriptor "$BUILD_DIR/generated/market_data.desc"   --source-run-dir "$SOURCE_RUN"   --capture-begin-offsets "$OUT_DIR/$RUN_ID-pre/begin-offsets.json" || exit 3

step "5. publish"
"$BUILD_DIR/p04_publisher" \
  --config config/local-dev.example.toml \
  --set "run.source_run_dir=\"$SOURCE_RUN\"" \
  --set "run.output_dir=\"$OUT_DIR\"" \
  --set "kafka.bootstrap_servers=\"$BOOTSTRAP\"" \
  --run-id "$RUN_ID"
PUBLISH_RC=$?
echo "publisher exit code: $PUBLISH_RC"
RUN_DIR="$OUT_DIR/$RUN_ID"
if [ -f "$RUN_DIR/report.json" ]; then
  echo "--- publisher report (selected fields) ---"
  "$DEPS/venv/bin/python" - "$RUN_DIR/report.json" <<'PYEOF'
import json, sys
report = json.load(open(sys.argv[1]))
for key in ("verdict", "safety_verdict", "performance_verdict", "upstream_accepted",
            "policy_suppressed", "route_rejected", "p04_enqueued_in_library",
            "p04_broker_acked", "p04_delivery_unknown", "p04_pending_at_shutdown",
            "p04_partition_mismatch", "conservation_check", "contract_error",
            "librdkafka_version", "protobuf_version", "kafka_version",
            "actual_sustained_acked_events_per_second", "p99_ack_ms",
            "max_app_queue_records", "max_process_rss_bytes"):
    print(f"  {key}: {report.get(key)}")
PYEOF
fi

step "6. independent snapshot verification"
"$DEPS/venv/bin/python" verifier/python_snapshot_verifier/snapshot_verifier.py \
  --bootstrap "$BOOTSTRAP" \
  --descriptor "$BUILD_DIR/generated/market_data.desc" \
  --source-run-dir "$SOURCE_RUN" \
  --ledger "$RUN_DIR/publish-ledger.jsonl" \
  --begin-offsets "$OUT_DIR/$RUN_ID-pre/begin-offsets.json" \
  --report "$RUN_DIR/snapshot-verifier-report.json"
VERIFY_RC=$?
echo "verifier exit code: $VERIFY_RC"

step "7. merged verdict"
# Two parties measured, so two documents exist. The merge never upgrades a
# verdict and never replaces a null with a zero; it reports the worst of the
# inputs and lists any disagreement between them (section 5.2, section 14).
"$DEPS/venv/bin/python" tools/summarize_run.py   --publisher-report "$RUN_DIR/report.json"   --verifier-report "$RUN_DIR/snapshot-verifier-report.json"   --out "$RUN_DIR/merged-verdict.json" | head -24
MERGE_RC=${PIPESTATUS[0]}
echo "merged verdict exit code: $MERGE_RC"

step "verdict"
echo "publisher_rc=$PUBLISH_RC verifier_rc=$VERIFY_RC artifacts=$RUN_DIR"
if [ "$PUBLISH_RC" -eq 0 ] && [ "$VERIFY_RC" -eq 0 ] && [ "${MERGE_RC:-1}" -eq 0 ]; then
  echo "E2E-01 PASS"
  exit 0
fi
echo "E2E-01 NOT PASS"
exit 1
