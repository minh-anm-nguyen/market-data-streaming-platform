#!/bin/bash
# Real-broker negative and fault suite.
#
# Every check here needs a live cluster, which is why it is not in the unit
# suite: each one asserts that the publisher refuses to start or refuses to
# claim success against a condition the broker actually presents. A check only
# counts when the stimulus is demonstrably applied, which is why each one prints
# what it did to the cluster before asserting (section 9, general principle 1).
#
# Exit codes the publisher uses, and what each check expects:
#   3  NO_GO before publishing  (config, schema pin, golden mapping, topics)
#   1  INCOMPLETE               (pending or unknown outcomes)
#   0  clean                    (every attempt terminal, counters balance)
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

PROJECT_DIR=${P04_PROJECT_DIR:-/mnt/d/trading/prj-3/04-kafka-market-data-streaming}
BUILD_DIR=${P04_BUILD_DIR:-/root/p04-build}
DEPS=${P04_DEPS:-/opt/p04-deps}
SOURCE_RUN=${P04_SOURCE_RUN:-/mnt/d/trading/prj-3/01-market-data-simulator/runs/p04-smoke}
OUT_DIR=${P04_OUT_DIR:-/root/p04-runs}
BOOTSTRAP=${P04_BOOTSTRAP_HOST:-127.0.0.1:9092}
COMPOSE="docker compose -f deploy/docker-compose.local.yml"
PUBLISHER="$BUILD_DIR/p04_publisher"

cd "$PROJECT_DIR" || exit 3
mkdir -p "$OUT_DIR"

PASSED=0
FAILED=0
INCONCLUSIVE=0
RESULTS=()

kt() { docker exec p04-kafka-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }

# Runs the publisher with overrides and compares the exit code to what the case
# requires. A case that expects a refusal and gets a success is a failure, and
# so is the reverse.
expect_exit() {
  local case_id="$1"; shift
  local expected="$1"; shift
  local description="$1"; shift
  local run_id="fault-${case_id}-$(date +%H%M%S)"
  local output
  output=$("$PUBLISHER" --config config/local-dev.example.toml \
      --set "run.source_run_dir=\"$SOURCE_RUN\"" \
      --set "run.output_dir=\"$OUT_DIR\"" \
      --set "kafka.bootstrap_servers=\"$BOOTSTRAP\"" \
      --set "run.max_events=2000" \
      --run-id "$run_id" "$@" 2>&1)
  local rc=$?
  if [ "$rc" -eq "$expected" ]; then
    PASSED=$((PASSED + 1))
    RESULTS+=("PASS $case_id exit=$rc  $description")
    echo "PASS $case_id (exit $rc): $description"
  else
    FAILED=$((FAILED + 1))
    RESULTS+=("FAIL $case_id exit=$rc expected=$expected  $description")
    echo "FAIL $case_id (exit $rc, expected $expected): $description"
  fi
  echo "$output" | grep -vE "^%4\||GETPID" | grep -E "NO_GO|verdict=|INCOMPLETE|refused|mismatch|FATAL" | head -4
  echo
}

echo "######## cluster ########"
$COMPOSE up -d 2>&1 | tail -2
for _ in $(seq 1 40); do
  docker exec p04-kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 2
done
bash deploy/topic-create.sh >/dev/null || exit 3
echo "cluster ready"
echo

echo "######## group A: producer configuration must fail fast (E-01..E-04) ########"
# The stimulus is a config value that contradicts the pinned idempotence policy.
# librdkafka itself refuses some of these at client creation; either way the
# process must not start publishing.
expect_exit "E-01" 3 "enable.idempotence=false is refused" \
  --set "producer.enable_idempotence=false"
expect_exit "E-02" 3 "acks=1 with idempotence required is refused" \
  --set "producer.acks=\"1\""
expect_exit "E-03" 3 "max.in.flight=10 is incompatible with idempotence" \
  --set "producer.max_in_flight_requests_per_connection=10"
expect_exit "E-04" 3 "retries=0 is refused under the pinned policy" \
  --set "producer.retries=0"
expect_exit "B-06" 3 "a non-murmur2 partitioner is refused" \
  --set "kafka.partitioner=\"random\""

echo "######## group B: topic contract must be verified before publishing ########"
# C-01: the topic simply does not exist. Auto-creation is off on both the broker
# and the client, so this must stop the run.
expect_exit "C-01" 3 "a missing topic stops the run before any record" \
  --set "kafka.ticks_topic=\"md.ticks.absent\""

# C-02: a topic that exists with the wrong partition count. The key-to-partition
# mapping is only stable at the pinned count, so this is a hard stop.
kt --delete --topic md.ticks.onepart >/dev/null 2>&1
kt --create --topic md.ticks.onepart --partitions 1 --replication-factor 1 >/dev/null 2>&1
echo "stimulus applied: md.ticks.onepart created with 1 partition"
expect_exit "C-02" 3 "a 1-partition topic is refused where 8 are pinned" \
  --set "kafka.ticks_topic=\"md.ticks.onepart\""

# C-03: compaction on a keyed event log would erase tick history.
kt --delete --topic md.ticks.compacted >/dev/null 2>&1
kt --create --topic md.ticks.compacted --partitions 8 --replication-factor 1 \
   --config cleanup.policy=compact >/dev/null 2>&1
echo "stimulus applied: md.ticks.compacted created with cleanup.policy=compact"
expect_exit "C-03" 3 "cleanup.policy=compact is refused for an event log" \
  --set "kafka.ticks_topic=\"md.ticks.compacted\""

# C-04 and C-05: the reliability-lab profile demands RF>=3 and minISR>=2. On a
# single-broker cluster that is unsatisfiable, and the run must say so rather
# than produce an HA verdict from one broker.
expect_exit "C-04" 3 "the HA profile is refused on a single-broker cluster" \
  --set "kafka.profile=\"reliability-lab\"" \
  --set "kafka.expected_replication_factor=3" \
  --set "kafka.expected_min_insync_replicas=2"

echo "######## group C: schema pin (D-08) ########"
expect_exit "D-08" 3 "a descriptor digest mismatch stops the run" \
  --set "run.proto_descriptor_sha256=\"$(printf 'f%.0s' {1..64})\""

echo "######## group D: broker unavailable (G-01, F-01) ########"
# The stimulus is the broker process being gone. produce() still succeeds into
# the client queue, which is exactly the trap FR-07 is about: the run must end
# INCOMPLETE with unknown outcomes, never with a success.
$COMPOSE stop kafka >/dev/null 2>&1
echo "stimulus applied: kafka container stopped"
RUN_ID="fault-G01-$(date +%H%M%S)"
OUTPUT=$("$PUBLISHER" --config config/local-dev.example.toml \
  --set "run.source_run_dir=\"$SOURCE_RUN\"" \
  --set "run.output_dir=\"$OUT_DIR\"" \
  --set "kafka.bootstrap_servers=\"$BOOTSTRAP\"" \
  --set "run.max_events=500" \
  --set "producer.message_timeout_ms=4000" \
  --set "shutdown.drain_deadline_ms=6000" \
  --run-id "$RUN_ID" 2>&1)
G01_RC=$?
echo "$OUTPUT" | grep -vE "^%[0-9]\||GETPID" | grep -E "verdict=|NO_GO" | head -3
G01_REPORT="$OUT_DIR/$RUN_ID/report.json"
if [ -f "$G01_REPORT" ]; then
  "$DEPS/venv/bin/python" - "$G01_REPORT" "$G01_RC" <<'PYEOF'
import json, sys
report = json.load(open(sys.argv[1]))
rc = int(sys.argv[2])
acked = report.get("p04_broker_acked") or 0
unknown = report.get("p04_delivery_unknown") or 0
definite = report.get("p04_delivery_failed_definite") or 0
problems = []
# Two safe outcomes exist and both are correct:
#
#   NO_GO (exit 3): the topic guard could not reach the cluster, so the run
#     stopped before publishing anything. Fail closed, which is what section 2.3
#     requires when the topic contract cannot be verified.
#   INCOMPLETE (exit 1): the broker went away mid-run, so attempts are pending
#     or unknown and completeness cannot be claimed.
#
# What is NOT acceptable is exit 0, a PASS verdict, or any acknowledged record.
if rc == 0:
    problems.append("exit code 0 with an unreachable broker")
if report.get("safety_verdict") not in ("INCOMPLETE", "FAIL", "NO_GO"):
    problems.append(f"safety verdict {report.get('safety_verdict')} with an unreachable broker")
if acked:
    problems.append(f"{acked} records reported as acknowledged with no broker")
if rc == 1 and unknown == 0 and definite == 0:
    problems.append("the run claimed INCOMPLETE but gave no attempt a terminal status")
print(json.dumps({
    "exit_code": rc,
    "safety_verdict": report.get("safety_verdict"),
    "acked": acked,
    "unknown": unknown,
    "failed_definite": definite,
    "conservation": report.get("conservation_check"),
    "problems": problems,
}, indent=2))
sys.exit(1 if problems else 0)
PYEOF
  if [ $? -eq 0 ]; then
    PASSED=$((PASSED + 1)); RESULTS+=("PASS G-01  broker down at startup: fail closed, nothing published")
    echo "PASS G-01: broker down at startup failed closed with no acknowledged record"
  else
    FAILED=$((FAILED + 1)); RESULTS+=("FAIL G-01  broker down produced an unsafe report")
    echo "FAIL G-01"
  fi
else
  FAILED=$((FAILED + 1)); RESULTS+=("FAIL G-01  no report written")
  echo "FAIL G-01: no report written"
fi
echo
$COMPOSE start kafka >/dev/null 2>&1
for _ in $(seq 1 40); do
  docker exec p04-kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 2
done
echo "broker restored"
echo

echo "######## group D2: broker lost mid-run (F-01, E-11) ########"
# The stimulus here is the broker disappearing *after* the topic contract was
# verified and records were already in flight. This is the path the startup
# guard cannot cover: the delivery reports have to resolve to UNKNOWN, and the
# run must end INCOMPLETE without a single false acknowledgement.
RUN_ID="fault-F01-$(date +%H%M%S)"
# The stimulus has to land mid-run, which a fixed sleep cannot guarantee:
# publisher startup includes metadata, DescribeConfigs and acquiring an
# idempotence producer id, and when that ran long the broker was stopped before
# the topic guard finished. The run then failed closed at startup with NO_GO,
# which is correct behaviour but tests G-01 over again rather than F-01.
#
# So the killer waits for evidence that records are actually flowing: the sum of
# end offsets across the market topics growing past a threshold. That makes the
# stimulus a consequence of observed progress instead of a guess about timing.
total_end_offsets() {
  # Asked through the verifier's own offset capture rather than
  # kafka-get-offsets.sh: that tool returned nothing when given two --topic
  # flags on Kafka 4.2, so the kill condition was never satisfied and the
  # stimulus silently never fired. A stimulus that does not fire turns a fault
  # test into a no-op that still prints a verdict, which is the worst outcome
  # available.
  local tmp
  tmp=$(mktemp)
  "$DEPS/venv/bin/python" verifier/python_snapshot_verifier/snapshot_verifier.py     --bootstrap "$BOOTSTRAP" --descriptor "$BUILD_DIR/generated/market_data.desc"     --source-run-dir "$SOURCE_RUN" --capture-begin-offsets "$tmp" >/dev/null 2>&1
  "$DEPS/venv/bin/python" -c "
import json, sys
try:
    print(sum(json.load(open(sys.argv[1])).values()))
except Exception:
    print(0)
" "$tmp"
  rm -f "$tmp"
}
BASELINE=$(total_end_offsets)
echo "baseline end offsets: $BASELINE"
STIMULUS_MARKER=$(mktemp)
(
  for _ in $(seq 1 60); do
    sleep 0.5
    current=$(total_end_offsets)
    if [ "${current:-0}" -ge $((BASELINE + 200)) ]; then
      docker compose -f deploy/docker-compose.local.yml stop kafka >/dev/null 2>&1
      echo "fired" > "$STIMULUS_MARKER"
      echo "stimulus applied: kafka stopped mid-run after $((current - BASELINE)) records landed"
      exit 0
    fi
  done
  echo "stimulus NOT applied: no record progress observed, F-01 is inconclusive"
) &
KILLER=$!
OUTPUT=$("$PUBLISHER" --config config/local-dev.example.toml   --set "run.source_run_dir=\"$SOURCE_RUN\""   --set "run.output_dir=\"$OUT_DIR\""   --set "kafka.bootstrap_servers=\"$BOOTSTRAP\""   --set "run.target_events_per_second=800"   --set "producer.message_timeout_ms=5000"   --set "shutdown.drain_deadline_ms=8000"   --run-id "$RUN_ID" 2>&1)
F01_RC=$?
wait $KILLER 2>/dev/null
echo "$OUTPUT" | grep -vE "^%[0-9]\||GETPID" | grep -E "verdict=" | head -2
F01_REPORT="$OUT_DIR/$RUN_ID/report.json"
F01_STIMULUS_FIRED=0
[ "$(cat "$STIMULUS_MARKER" 2>/dev/null)" = "fired" ] && F01_STIMULUS_FIRED=1
rm -f "$STIMULUS_MARKER"
if [ "$F01_STIMULUS_FIRED" -eq 0 ]; then
  INCONCLUSIVE=$((INCONCLUSIVE + 1))
  RESULTS+=("INCONCLUSIVE F-01  the broker was never stopped mid-run, so nothing was tested")
  echo "INCONCLUSIVE F-01: no record progress was observed, so the stimulus never fired"
elif [ -f "$F01_REPORT" ]; then
  "$DEPS/venv/bin/python" - "$F01_REPORT" "$F01_RC" <<'PYEOF2'
import json, sys
report = json.load(open(sys.argv[1]))
rc = int(sys.argv[2])
acked = report.get("p04_broker_acked") or 0
unknown = report.get("p04_delivery_unknown") or 0
definite = report.get("p04_delivery_failed_definite") or 0
pending = report.get("p04_pending_at_shutdown") or 0
problems = []
if rc == 0:
    problems.append("exit code 0 after the broker disappeared mid-run")
if acked == 0:
    problems.append("no record was acknowledged before the loss, so the broker "
                    "was not lost mid-run and this is G-01 again, not F-01")
if report.get("safety_verdict") not in ("INCOMPLETE", "FAIL"):
    problems.append(f"safety verdict {report.get('safety_verdict')} after losing the broker")
if unknown == 0:
    problems.append("no attempt ended UNKNOWN, so the ambiguity was not reported")
if pending:
    problems.append(f"{pending} attempts left pending instead of being finalised")
if report.get("conservation_check") != "ok":
    problems.append(f"counters did not balance: {report.get('conservation_check')}")
print(json.dumps({
    "exit_code": rc,
    "safety_verdict": report.get("safety_verdict"),
    "acked_before_the_loss": acked,
    "unknown": unknown,
    "failed_definite": definite,
    "pending_at_shutdown": pending,
    "conservation": report.get("conservation_check"),
    "problems": problems,
}, indent=2))
sys.exit(1 if problems else 0)
PYEOF2
  if [ $? -eq 0 ]; then
    PASSED=$((PASSED + 1)); RESULTS+=("PASS F-01  broker lost mid-run: UNKNOWN reported, nothing pending, no false ACK")
    echo "PASS F-01"
  else
    FAILED=$((FAILED + 1)); RESULTS+=("FAIL F-01  mid-run broker loss produced an unsafe report")
    echo "FAIL F-01"
  fi
else
  FAILED=$((FAILED + 1)); RESULTS+=("FAIL F-01  no report written")
fi
echo
$COMPOSE start kafka >/dev/null 2>&1
for _ in $(seq 1 40); do
  docker exec p04-kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh     --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 2
done
echo "broker restored"
echo

echo "######## group E: consumer group verifier (E2E-09, K-*) ########"
# Publishes a small batch, then runs the group verifier against it. The verifier
# asserts assignment exclusivity and that no checkpoint moved past the last
# contiguous processed offset.
RUN_ID="fault-K-$(date +%H%M%S)"
"$PUBLISHER" --config config/local-dev.example.toml \
  --set "run.source_run_dir=\"$SOURCE_RUN\"" \
  --set "run.output_dir=\"$OUT_DIR\"" \
  --set "kafka.bootstrap_servers=\"$BOOTSTRAP\"" \
  --set "run.max_events=3000" \
  --run-id "$RUN_ID" >/dev/null 2>&1
"$BUILD_DIR/p04_group_verifier" --bootstrap "$BOOTSTRAP" \
  --group "p04-fault-suite-$(date +%H%M%S)" \
  --topics "md.ticks.v1,md.quotes.v1" \
  --max-records 3000 --runtime-seconds 25 --commit-every 250 \
  --report "$OUT_DIR/$RUN_ID/group-verifier-report.json" > /tmp/group-verifier.out 2>&1
GROUP_RC=$?
grep -E "\"verdict\"|\"records_consumed\"|\"commits\"|\"concurrent_assignment_violations\"|\"decode_errors\"" /tmp/group-verifier.out | head -6
if [ "$GROUP_RC" -eq 0 ]; then
  PASSED=$((PASSED + 1)); RESULTS+=("PASS E2E-09  group verifier: exclusive assignment, contiguous checkpoints")
  echo "PASS E2E-09"
else
  FAILED=$((FAILED + 1)); RESULTS+=("FAIL E2E-09  group verifier rc=$GROUP_RC")
  echo "FAIL E2E-09 (rc=$GROUP_RC)"
fi
echo

echo "######## summary ########"
for line in "${RESULTS[@]}"; do echo "  $line"; done
echo
echo "passed=$PASSED failed=$FAILED inconclusive=$INCONCLUSIVE"
[ "$FAILED" -eq 0 ] || exit 1
exit 0
