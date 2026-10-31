"""The big project: run all eight components as one pipeline, in order.

Each project proved itself in isolation. This is where they run as one system,
and the stage list below is the assembly order the whole plan was split along:

    P01 record    -> sealed sim.v1 dataset
    P01 verify    -> the dataset is complete and its anomalies are explained
    P02 encode    -> marketdata.v1 protobuf framed as MDP1 (.pbstream)
    P02 verify    -> every encoded event matches the source, semantically
    P03 adapt     -> MDP1 in, per-symbol sequence/staleness policy, classified out
    P04 publish   -> Kafka, keyed by symbol, raw protobuf values
    P05 ingest    -> ClickHouse raw, offset committed only after the sink ACKs
    P06 build     -> canonical snapshot, 1m OHLC bars, READY only if proofs hold
    P07 reconcile -> independent oracle over the sealed source and the real cut

Every stage here was executed against the real components on this host before it
was written down. What that cost is worth recording, because none of it is
visible from the code:

* **A manifest shim between P02 and P03.** P02 writes
  ``<file>.pbstream.COMPLETED`` holding ``{bytes, frames, sha256,
  frame_protocol}``. P03 reads ``<file>.pbstream.manifest.json`` and requires
  ``status`` and ``dataset_id``. Different name, different schema, so P03
  reports ``SOURCE_UNVERIFIED`` and refuses the data. The shim derives P03's
  manifest from P02's marker plus P01's report. It is a **named shim, not a
  fix**: finding P09-M0-19 stays open until P02 emits the manifest its own
  consumer specified.
* **P09 seeds the consumer group.** P05 runs with ``auto_offset_reset="error"``
  by design, so a fresh group has no starting point and P05 stops. Choosing that
  starting point is an operator decision and P09 is the operator; the seeding
  tool commits the low watermark and refuses a group that already has offsets.
* **Absolute paths, always.** P04 resolves ``partition_golden_path`` relative to
  its own working directory, so the pipeline passes it absolutely.
* **A fresh run id per attempt.** P04 refuses to write into a non-empty run
  directory, which is right: a re-run must not overwrite the evidence of the
  last one.
* **The host network, and retries.** Kafka and ClickHouse are published on the
  WSL host's loopback. The Python stages therefore run with ``--network host``
  and reach the same ``127.0.0.1`` addresses the native C++ binaries use, which
  are the only ones reachable from both. This host also drops published-port
  connectivity across idle gaps, so the script runs end to end without pausing
  and probes the broker with retries instead of believing the first failure.
* **ClickHouse over HTTP.** The native port on 9000 flaps on this host. Nothing
  in the data path needs it -- P05 speaks HTTP on 8123 -- so the pipeline's own
  queries use HTTP too and never depend on the unreliable one.

As with the init jobs, this module renders and does not execute. The script runs
where the Docker daemon is, and judging what it produced is a separate step, so
a stage cannot mark its own work.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .deploy import InfraExpectation, _preamble


@dataclass(frozen=True)
class Stage:
    """One pipeline stage: who owns it, how it runs, what it leaves behind."""

    id: str
    project: str
    title: str
    runtime: str            # "container" | "native"
    produces: str
    notes: str = ""


#: The assembly order. Every stage is blocking on purpose: this pipeline fails
#: closed, because a later stage reading a partial artifact would report a
#: smaller dataset rather than an incomplete one.
STAGES: tuple[Stage, ...] = (
    Stage("p01-record", "P01", "generate a sealed sim.v1 dataset", "container",
          "runs/<id>/{events.jsonl,report.json,COMPLETED}"),
    Stage("p01-verify", "P01", "re-derive every anomaly and the digest", "container",
          "verdict COMPLETE"),
    Stage("p02-encode", "P02", "sim.v1 JSONL -> MDP1 .pbstream", "container",
          "p02/events.pbstream + .COMPLETED"),
    Stage("p02-verify", "P02", "semantic equality against the source", "container",
          "decoded == source, 0 mismatches"),
    Stage("p02-source-manifest", "P02", "the source manifest P03 reads", "container",
          "p02/events.pbstream.manifest.json",
          notes="P02 writes it directly now; the P09 shim that stood here is gone"),
    Stage("p03-adapt", "P03", "validate, classify, emit the P04 handoff", "native",
          "p03/handoff.ndjson + report.yaml"),
    Stage("p04-publish", "P04", "publish P03's classified events to Kafka", "native",
          "Kafka records + run manifest",
          notes="runs with upstream=p03_handoff, the real join (P09-M0-05)"),
    Stage("p05-seed-offsets", "P09", "commit the group's starting offsets", "container",
          "committed offsets for every partition",
          notes="P05 refuses auto_offset_reset=earliest by design (its I-11)"),
    Stage("p05-ingest", "P05", "Kafka -> ClickHouse raw, offset follows sink", "container",
          "md_ingest.market_events_raw rows"),
    Stage("seal-kafka-cut", "P09", "seal watermarks + the committed frontier", "container",
          "p05/cut.json",
          notes="P09 seals it: p05_ingest snapshot reads assignment() too early and "
                "records no committed frontier"),
    Stage("p06-build", "P06", "canonical snapshot + 1m OHLC bars + READY", "container",
          "md_analytics.canonical_ticks, bars_1m_state",
          notes="build-from-raw reads P05's table; plain `build` synthesises its own corpus"),
    Stage("p07-verify-source", "P07", "seal and verify the real P01 run", "container",
          "p07/verify-source.json",
          notes="the full scenario runner needs a lab profile; this is the part that reads this deployment"),
    Stage("p07-scan-live-cut", "P07", "classify the REAL Kafka cut", "container",
          "p07/live-cut.json",
          notes="same scan_cut classifier as the model profile; only the records are real"),
    Stage("p07-read-live-rows", "P07", "read the REAL ClickHouse raw rows", "container",
          "p07/live-rows.json",
          notes="physical rows and logical identities reported separately"),
)

_HEADER_NOTE = """
# GENERATED by `p09 render-pipeline` from contracts.lock.yaml. Do not hand-edit.
#
# Runs the whole platform once, end to end, and writes a JSON report per stage.
# Fails closed: the first failing stage stops the run, because a later stage
# reading a partial artifact would report a smaller dataset rather than an
# incomplete one.
#
# Run it where the Docker daemon is. The Python stages use --network host so
# they reach the same 127.0.0.1 addresses as the native C++ binaries; see
# p09_integration/pipeline.py for why that is the only set reachable from both.
"""


def _script(lock: dict[str, Any]) -> str:
    cross = lock.get("cross_project_identity") or {}
    kafka = cross.get("kafka") or {}
    clickhouse = cross.get("clickhouse") or {}
    golden = "$REPO/04-kafka-market-data-streaming/config/topic-partition-golden.json"

    return "\n".join([
        _preamble(lock).rstrip("\n"),
        _HEADER_NOTE.rstrip("\n"),
        "",
        'REPO="${REPO:-/mnt/d/trading/prj-3}"',
        'WORK="${WORK:-$HOME/p09-pipeline-$(date -u +%Y%m%dT%H%M%SZ)}"',
        'IMAGE="${IMAGE:-p09/python-tools:dev}"',
        'P03_BIN="${P03_BIN:-$HOME/.cache/p03-build/release/md_adapter}"',
        'P04_BIN="${P04_BIN:-$HOME/p04-build/p04_publisher}"',
        'BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"',
        'CH_HTTP="${CH_HTTP:-127.0.0.1:8123}"',
        'EVENTS="${EVENTS:-2000}"',
        'MAX_IDLE_STEPS="${MAX_IDLE_STEPS:-40}"',
        'RUN_ID="${RUN_ID:-chain-$(date -u +%Y%m%dT%H%M%SZ)}"',
        'TICKS="%s"' % kafka.get("ticks_topic", "md.ticks.v1"),
        'QUOTES="%s"' % kafka.get("quotes_topic", "md.quotes.v1"),
        'RAW_TABLE="%s"' % clickhouse.get("raw_table", "md_ingest.market_events_raw"),
        'GROUP="md-p05-$RUN_ID"',
        'TOPIC_GENERATION="%s"' % kafka.get("topic_generation", "unknown"),
        "",
        'mkdir -p "$WORK/cfg" "$WORK/runs" "$WORK/p02" "$WORK/p03" "$WORK/p04" \\',
        '  "$WORK/p05" "$WORK/p06" "$WORK/p07"',
        'REPORT="$WORK/pipeline-report.json"',
        "",
        'echo "P09 pipeline $RUN_ID"',
        'echo "  work dir $WORK"',
        "",
        "# Each stage appends its outcome before the next begins, so a crash still",
        "# leaves evidence of how far the pipeline got.",
        'printf \'{\\n  "run_id": "%s",\\n  "started_at_utc": "%s",\\n  "stages": [\' \\',
        '  "$RUN_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$REPORT"',
        "FIRST_STAGE=1",
        "REPORT_CLOSED=0",
        "",
        "record_stage() {",
        '  [ "$FIRST_STAGE" -eq 1 ] || printf \',\' >> "$REPORT"',
        "  FIRST_STAGE=0",
        '  printf \'\\n    {"id": "%s", "project": "%s", "exit_code": %s, "verdict": "%s"}\' \\',
        '    "$1" "$2" "$3" "$4" >> "$REPORT"',
        "}",
        "",
        "finish() {",
        "  REPORT_CLOSED=1",
        '  printf \'\\n  ],\\n  "finished_at_utc": "%s",\\n  "verdict": "%s"\\n}\\n\' \\',
        '    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" >> "$REPORT"',
        '  echo "report: $REPORT"',
        '  echo "verdict: $1"',
        '  [ "$1" = "PASS" ] || exit 1',
        "  exit 0",
        "}",
        "",
        "stage_failed() {",
        '  echo "  STAGE FAILED: $1 (exit $2)" >&2',
        '  record_stage "$1" "$3" "$2" FAIL',
        "  finish FAIL",
        "}",
        "",
        "# --network host, deliberately: see the module docstring. The locked",
        "# values are passed in as environment rather than interpolated into each",
        "# command, so a stage reads the same contract the lock holds.",
        "dr() {",
        '  workdir="$1"; shift',
        '  docker run --rm -i --network host \\',
        '    -e "RAW_TABLE=$RAW_TABLE" -e "CH_HTTP=$CH_HTTP" \\',
        '    -e "BOOTSTRAP=$BOOTSTRAP" -e "TICKS=$TICKS" -e "QUOTES=$QUOTES" \\',
        '    -v "$WORK:/work" -v "$REPO:/repo:ro" -w "$workdir" "$IMAGE" "$@"',
        "}",
        "",
        "# The script runs under `set -e`, so a stage that forgets its",
        "# `|| stage_failed` would abort the run and leave the report without a",
        "# verdict -- evidence that stops mid-sentence, which is worse than a",
        "# recorded failure. This closes the report on any exit path.",
        "on_exit() {",
        "  rc=$?",
        '  if [ "$rc" -ne 0 ] && [ "$REPORT_CLOSED" -eq 0 ]; then',
        '    echo "  PIPELINE ABORTED at an unguarded stage (exit $rc)" >&2',
        "    record_stage unguarded-stage P09 \"$rc\" FAIL",
        "    finish ABORTED",
        "  fi",
        "}",
        "trap on_exit EXIT",
        "",
        'echo "== 0. infrastructure health =="',
        "for i in $(seq 1 60); do",
        '  k=$(docker inspect -f "{{.State.Health.Status}}" p09-dev-kafka 2>/dev/null || true)',
        '  c=$(docker inspect -f "{{.State.Health.Status}}" p09-dev-clickhouse 2>/dev/null || true)',
        '  if [ "$k" = healthy ] && [ "$c" = healthy ]; then',
        '    echo "  both healthy"',
        "    break",
        "  fi",
        '  if [ "$i" -eq 60 ]; then',
        '    echo "  infrastructure not healthy: kafka=$k clickhouse=$c" >&2',
        "    record_stage infra-health P09 1 FAIL",
        "    finish FAIL",
        "  fi",
        "  sleep 2",
        "done",
        "record_stage infra-health P09 0 PASS",
        "",
        "# This host drops published-port connectivity across idle gaps, so the",
        "# broker is probed with retries rather than trusted on first contact.",
        'echo "== 0b. broker reachable =="',
        "dr /platform python - <<'PY' || stage_failed broker-reachable 1 P09",
        "import os, time",
        "from confluent_kafka.admin import AdminClient",
        "bootstrap = os.environ.get('BOOTSTRAP', '127.0.0.1:9092')",
        "last = None",
        "for attempt in range(1, 13):",
        "    try:",
        "        md = AdminClient({'bootstrap.servers': bootstrap,",
        "                          'socket.timeout.ms': 5000}).list_topics(timeout=8)",
        "        print('  reachable on attempt %d: %s' % (attempt, md.cluster_id))",
        "        raise SystemExit(0)",
        "    except SystemExit:",
        "        raise",
        "    except Exception as exc:",
        "        last = exc",
        "        time.sleep(3)",
        "print('  broker unreachable after 12 attempts:', last)",
        "raise SystemExit(1)",
        "PY",
        "record_stage broker-reachable P09 0 PASS",
        "",
        'echo "== 1. P01 record =="',
        "# A run-scoped config: output under the work dir, and TCP off for a",
        "# record-only run so nothing binds a port the pipeline does not use.",
        'sed -e "s|^root_dir = .*|root_dir = \\"/work/runs\\"|" \\',
        '    -e "/^\\[output.tcp\\]/,/^\\[/{s|^enabled = true|enabled = false|}" \\',
        '    "$REPO/01-market-data-simulator/config/example.toml" > "$WORK/cfg/p01.toml"',
        'dr /platform python -m market_sim record --config /work/cfg/p01.toml \\',
        '  --count "$EVENTS" --run-id p09run > "$WORK/p01-record.json" \\',
        "  || stage_failed p01-record $? P01",
        "record_stage p01-record P01 0 PASS",
        "",
        'echo "== 2. P01 verify =="',
        "dr /platform python -m market_sim verify --run-dir /work/runs/p09run \\",
        '  > "$WORK/p01-verify.json" || stage_failed p01-verify $? P01',
        "record_stage p01-verify P01 0 PASS",
        "",
        'echo "== 3. P02 encode =="',
        "dr /platform/02-market-data-protobuf-contracts python -m md_contract \\",
        "  encode-jsonl --run-dir /work/runs/p09run \\",
        "  --output /work/p02/events.pbstream --mode strict_normal \\",
        '  > "$WORK/p02-encode.json" || stage_failed p02-encode $? P02',
        "record_stage p02-encode P02 0 PASS",
        "",
        'echo "== 4. P02 verify =="',
        "dr /platform/02-market-data-protobuf-contracts python -m md_contract \\",
        "  verify-pbstream --input /work/p02/events.pbstream \\",
        "  --expected-jsonl /work/runs/p09run/events.jsonl \\",
        '  > "$WORK/p02-verify.json" || stage_failed p02-verify $? P02',
        "record_stage p02-verify P02 0 PASS",
        "",
        'echo "== 5. P03 source manifest present =="',
        "# No shim any more. P02 now writes <file>.pbstream.manifest.json itself,",
        "# with the status and dataset_id P03 specified, so finding P09-M0-19 is",
        "# closed at the source rather than papered over here. This stage only",
        "# asserts the artifact exists: a missing manifest must stop the run,",
        "# because P03 would otherwise report SOURCE_UNVERIFIED three lines later",
        "# and the cause would look like an adapter fault.",
        'if [ -s "$WORK/p02/events.pbstream.manifest.json" ]; then',
        '  echo "  manifest written by P02"',
        "else",
        '  echo "  P02 wrote no source manifest" >&2',
        "  stage_failed p02-source-manifest 1 P02",
        "fi",
        "record_stage p02-source-manifest P02 0 PASS",
        "",
        'echo "== 6. P03 adapt =="',
        'sed -e "s|^sink = .*|sink = \\"p04_stub\\"|" \\',
        '    -e "s|^file = .*|file = \\"$WORK/p03/handoff.ndjson\\"|" \\',
        '    "$REPO/03-cpp-market-data-adapter/config/example.toml" > "$WORK/cfg/p03.toml"',
        '"$P03_BIN" --config "$WORK/cfg/p03.toml" --input-mode pbstream \\',
        '  --input "$WORK/p02/events.pbstream" --report "$WORK/p03/report.yaml" \\',
        "  || stage_failed p03-adapt $? P03",
        'grep -E "^status:" "$WORK/p03/report.yaml" | head -1',
        "record_stage p03-adapt P03 0 PASS",
        "",
        'echo "== 7. P04 publish =="',
        "# upstream=p03_handoff is the real join: the classification and the",
        "# suppression set are P03's, not a re-implementation (finding P09-M0-05).",
        "# partition_golden_path must be absolute: P04 resolves it from its own cwd.",
        'sed -e "s|^source_run_dir = .*|source_run_dir = \\"$WORK/runs/p09run\\"|" \\',
        '    -e "s|^run_id = .*|run_id = \\"$RUN_ID\\"|" \\',
        '    -e "s|^output_dir = .*|output_dir = \\"$WORK/p04\\"|" \\',
        '    -e "s|^bootstrap_servers = .*|bootstrap_servers = \\"$BOOTSTRAP\\"|" \\',
        '    -e "s|^partition_golden_path = .*|partition_golden_path = \\"%s\\"|" \\' % golden,
        '    -e "s|^expected_source_schema = .*|expected_source_schema = \\"sim.v1\\"\\nupstream = \\"p03_handoff\\"\\np03_handoff_path = \\"$WORK/p03/handoff.ndjson\\"|" \\',
        '    "$REPO/04-kafka-market-data-streaming/config/local-dev.example.toml" \\',
        '    > "$WORK/cfg/p04.toml"',
        '"$P04_BIN" --config "$WORK/cfg/p04.toml" || stage_failed p04-publish $? P04',
        "record_stage p04-publish P04 0 PASS",
        "",
        'echo "== 8. P05 seed offsets =="',
        "# P05 refuses auto_offset_reset=earliest (its I-11), so the starting point",
        "# is an operator decision. This commits the low watermark and refuses a",
        "# group that already has committed offsets.",
        "dr /platform python \\",
        "  /repo/09-full-system-integration/compose/init/seed-consumer-offsets.py \\",
        '  "$BOOTSTRAP" "$GROUP" "$TICKS" "$QUOTES" \\',
        "  || stage_failed p05-seed-offsets $? P09",
        "record_stage p05-seed-offsets P09 0 PASS",
        "",
        'echo "== 9. P05 ingest =="',
        'sed -e "s|^bootstrap_servers = .*|bootstrap_servers = \\"$BOOTSTRAP\\"|" \\',
        '    -e "s|^group_id = .*|group_id = \\"$GROUP\\"|" \\',
        '    -e "s|^run_id = .*|run_id = \\"$RUN_ID\\"|" \\',
        '    -e "s|^source_manifest = .*|source_manifest = \\"/work/runs/p09run/report.json\\"|" \\',
        '    "$REPO/05-python-batch-ingestion/config/dev.toml" > "$WORK/cfg/p05.toml"',
        "# --max-idle-steps is required here, not optional. Without it P05 polls",
        "# forever waiting for records that a finite dataset will never send, and",
        "# the pipeline hangs rather than finishing. P05's own help is careful",
        "# about what it means: it stops after N consecutive empty polls and is",
        "# NOT an end-of-dataset detector, so the stage that follows still has to",
        "# reconcile rather than assume everything arrived.",
        "dr /platform/05-python-batch-ingestion python -m p05_ingest run \\",
        '  --config /work/cfg/p05.toml --report /work/p05/run.json \\',
        '  --max-idle-steps "$MAX_IDLE_STEPS" \\',
        "  || stage_failed p05-ingest $? P05",
        "record_stage p05-ingest P05 0 PASS",
        "",
        'echo "== 10. rows landed in ClickHouse =="',
        "dr /platform python - <<'PY'",
        "import os, urllib.parse, urllib.request",
        "query = ('SELECT event_kind, count(), '",
        "         'uniqExact((dataset_id,symbol,sequence)) '",
        "         'FROM ' + os.environ['RAW_TABLE'] +",
        "         ' GROUP BY event_kind ORDER BY event_kind FORMAT TSV')",
        "url = 'http://' + os.environ['CH_HTTP'] + '/?query=' + urllib.parse.quote(query)",
        "print(urllib.request.urlopen(url, timeout=30).read().decode().strip())",
        "PY",
        "RC=$?",
        '[ "$RC" -eq 0 ] || stage_failed raw-rows-landed "$RC" P09',
        "record_stage raw-rows-landed P09 0 PASS",
        "",
        'echo "== 11. seal the Kafka cut =="',
        "# P06 needs a bounded source, and the cut must carry BOTH the high-water",
        "# mark (which bounds it) and P05's committed frontier (which says how far",
        "# the sink actually acknowledged). TopicPartitionCut.covers_cut() is the",
        "# comparison between them.",
        "#",
        "# P09 seals it rather than `p05_ingest snapshot`, for two reasons found",
        "# by running it: that command reads assignment() immediately after an",
        "# asynchronous subscribe, so the cut came out empty; and it records only",
        "# watermarks, with no committed frontier to compare against.",
        "dr /platform python \\",
        "  /repo/09-full-system-integration/compose/init/seal-kafka-cut.py \\",
        '  --bootstrap "$BOOTSTRAP" --group "$GROUP" \\',
        '  --topics "$TICKS" "$QUOTES" \\',
        '  --run-id "$RUN_ID" --topic-generation "$TOPIC_GENERATION" \\',
        "  --out /work/p05/cut.json \\",
        "  || stage_failed seal-kafka-cut $? P09",
        "record_stage seal-kafka-cut P09 0 PASS",
        "",
        'echo "== 12. P06 build from the REAL raw table =="',
        "# build-from-raw, not build: the rows come from md_ingest.market_events_raw,",
        "# the expected set from P03's accepted-event ledger and the bound from P05's",
        "# cut. `build` would synthesise its own corpus and audit it against itself.",
        "# The dataset id is read from P01's own report rather than guessed: it is",
        "# the identity every downstream scope is stated against.",
        "DATASET=$(dr /platform python /repo/09-full-system-integration/"
        "compose/pipeline/read-dataset-id.py /work/runs/p09run/report.json"
        " | tr -d '\\r\\n')",
        'echo "  dataset $DATASET"',
        # --clickhouse-url is a GLOBAL option on md-analytics, so it precedes
        # the subcommand. Placing it after build-from-raw made argparse reject
        # it as unrecognised, which reads like a missing feature rather than a
        # word-order mistake.
        "# Run from the repository mount, not the installed package: P06's",
        "# SqlBank walks up from its own module file to find sql/, which sits",
        "# beside the package rather than inside it, so an installed copy raises",
        "# SQL_DIR_NOT_FOUND. The launcher sets the path; see its docstring.",
        "dr /repo/06-clickhouse-analytics python \\",
        "  /repo/09-full-system-integration/compose/pipeline/run-md-analytics.py \\",
        '  --clickhouse-url "http://$CH_HTTP" build-from-raw \\',
        "  --catalog /work/p06/catalog.json \\",
        '  --dataset-id "$DATASET" \\',
        "  --p03-handoff /work/p03/handoff.ndjson \\",
        "  --p05-cut /work/p05/cut.json \\",
        # The redirect runs in the HOST shell, which has no /work: only the
        # container has that mount. Container paths belong in arguments, host
        # paths in redirects, and mixing them created a file in a directory that
        # does not exist outside the container.
        '  --raw-table "$RAW_TABLE" > "$WORK/p06/build.json" \\',
        "  || stage_failed p06-build $? P06",
        "record_stage p06-build P06 0 PASS",
        "",
        'echo "== 13. P07 verify the sealed source =="',
        "# P07 against the real P01 run directory. Its full scenario runner needs a",
        "# lab profile; verify-source is the part that reads this deployment's",
        "# actual artifact and seals it, so it is what runs here.",
        "dr /platform/07-replay-and-recovery python -m p07 verify-source \\",
        '  /work/runs/p09run > "$WORK/p07/verify-source.json" \\',
        "  || stage_failed p07-verify-source $? P07",
        "record_stage p07-verify-source P07 0 PASS",
        "",
        'echo "== 14. P07 scans the REAL Kafka cut =="',
        "# This is what moves P07 off its UNIT/MODEL profile. The classifier is",
        "# scan_cut, unchanged: duplicate identities, identity conflicts, an MDP1",
        "# prefix inside a value and key/symbol disagreement are judged by exactly",
        "# the same code that judges the modelled broker. Only the records are",
        "# real now.",
        "#",
        "# --fenced is asserted here and nowhere else in the pipeline, because by",
        "# this point P04 has exited: there is no producer left to append, so the",
        "# window cannot move while it is read. Only the step that knows the",
        "# producers stopped may make that claim.",
        "dr /platform/07-replay-and-recovery python -m p07 scan-live-cut \\",
        '  --bootstrap "$BOOTSTRAP" --cut /work/p05/cut.json \\',
        '  --dataset-id "$DATASET" --fenced \\',
        '  > "$WORK/p07/live-cut.json" \\',
        "  || stage_failed p07-scan-live-cut $? P07",
        'cat "$WORK/p07/live-cut.json"',
        "record_stage p07-scan-live-cut P07 0 PASS",
        "",
        'echo "== 15. P07 reads the REAL ClickHouse rows =="',
        "# Physical rows and logical identities are reported as separate numbers.",
        "# The gap between them is the at-least-once duplication P05 documents,",
        "# not a defect, and one combined figure would invite the wrong reading.",
        "dr /platform/07-replay-and-recovery python -m p07 read-live-rows \\",
        '  --clickhouse-url "http://$CH_HTTP" \\',
        '  --dataset-id "$DATASET" --raw-table "$RAW_TABLE" \\',
        '  > "$WORK/p07/live-rows.json" \\',
        "  || stage_failed p07-read-live-rows $? P07",
        'cat "$WORK/p07/live-rows.json"',
        "record_stage p07-read-live-rows P07 0 PASS",
        "",
        "finish PASS",
        "",
    ])


def render_pipeline(lock: dict[str, Any], root: Path, out_dir: Path) -> list[Path]:
    """Write the pipeline runner and the stage inventory derived from ``lock``."""
    # Validates that the lock carries the topics and tables the script needs.
    InfraExpectation.from_lock(lock)
    out_dir.mkdir(parents=True, exist_ok=True)

    runner = out_dir / "run-pipeline.sh"
    runner.write_text(_script(lock), encoding="utf-8", newline="\n")

    rows = [
        "# Pipeline stages",
        "",
        "Generated by `p09 render-pipeline`: the assembly order of the platform.",
        "",
        "| # | stage | project | runtime | produces |",
        "|---|---|---|---|---|",
    ]
    for index, stage in enumerate(STAGES, start=1):
        rows.append(
            "| %d | `%s` | %s | %s | %s |"
            % (index, stage.id, stage.project, stage.runtime, stage.produces)
        )
    annotated = [s for s in STAGES if s.notes]
    if annotated:
        rows.extend(["", "## Notes", ""])
        rows.extend("* **`%s`** -- %s" % (s.id, s.notes) for s in annotated)

    inventory = out_dir / "STAGES.md"
    inventory.write_text("\n".join(rows) + "\n", encoding="utf-8", newline="\n")
    return [runner, inventory]
