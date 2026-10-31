#!/usr/bin/env python3
"""End-to-end matrix: drives the real md_adapter binary as a subprocess.

What this covers that the in-process tests cannot: exit codes, the artifacts
left on disk, config rejection at startup, signal handling, and the live TCP
path against an external fault server.

Every case states its expected outcome up front. A case whose condition
cannot actually be produced in this environment is reported SKIPPED with a
reason -- never PASS. Exit status is non-zero if any case fails.

Usage:
    MD_ADAPTER_BIN=/path/to/md_adapter python3 tests/e2e/run_matrix.py
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
TOOLS = ROOT / "tools"
sys.path.insert(0, str(TOOLS))

BIN = os.environ.get("MD_ADAPTER_BIN", "")
PROTOC = os.environ.get("MD_PROTOC", "protoc")

# Exit codes from md::ExitCode.
EXIT_OK = 0
EXIT_CONFIG_INVALID = 2
EXIT_SOURCE_UNVERIFIED = 3
EXIT_STREAM_FAILED = 4
EXIT_INCOMPLETE = 5
EXIT_BACKPRESSURE = 6
EXIT_SINK_IO = 8

results: list[tuple[str, str, str]] = []  # (id, verdict, detail)


def record(case_id: str, verdict: str, detail: str = "") -> None:
    results.append((case_id, verdict, detail))
    mark = {"PASS": "ok", "FAIL": "FAIL", "SKIPPED": "skip"}[verdict]
    print(f"[{mark:>4}] {case_id} {detail}".rstrip(), flush=True)


def check(case_id: str, ok: bool, detail: str = "") -> bool:
    record(case_id, "PASS" if ok else "FAIL", detail)
    return ok


# ---------------------------------------------------------------------------
# Running the adapter
# ---------------------------------------------------------------------------


class Run:
    def __init__(self, proc, report_path: pathlib.Path):
        self.code = proc.returncode
        self.stderr = proc.stderr or ""
        self.stdout = proc.stdout or ""
        self.report_path = report_path
        self.report = report_path.read_text() if report_path.exists() else ""

    def field(self, key: str):
        """Reads a scalar from the YAML report. The report is deliberately
        flat enough that a line match is honest here."""
        m = re.search(rf"^\s*{re.escape(key)}:\s*(.*)$", self.report, re.M)
        if not m:
            return None
        return m.group(1).strip().strip('"')

    def int_field(self, key: str):
        v = self.field(key)
        if v is None or v == "null":
            return None
        try:
            return int(v)
        except ValueError:
            return None


def run_adapter(workdir: pathlib.Path, config: pathlib.Path,
                extra=None, timeout: float = 60.0) -> Run:
    report = workdir / "report.yaml"
    if report.exists():
        report.unlink()
    cmd = [BIN, "--config", str(config), "--report", str(report)]
    cmd += extra or []
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                          cwd=workdir)
    return Run(proc, report)


def write_config(path: pathlib.Path, **overrides) -> pathlib.Path:
    """Writes a config from the shipped example with scalar overrides applied
    in place. Appending a second table would trip the duplicate-key rule,
    which is itself part of the contract under test.

    Keys are written as `section__name`.
    """
    text = (ROOT / "config" / "example.toml").read_text()
    # The example pins the digest of the build it shipped with; a test must
    # not fail merely because this toolchain produced a different one.
    text = re.sub(r'^expected_descriptor_sha256 = ".*"$',
                  'expected_descriptor_sha256 = "UNPINNED"', text, flags=re.M)

    for key, value in overrides.items():
        section, _, name = key.partition("__")
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, int):
            rendered = str(value)
        elif isinstance(value, str) and value.startswith("["):
            rendered = value
        else:
            rendered = f'"{value}"'

        header = f"[{section}]"
        at = text.index(header)
        nxt = text.find("\n[", at + 1)
        nxt = len(text) if nxt < 0 else nxt
        body = text[at:nxt]
        pat = re.compile(rf"^{re.escape(name)} = .*$", re.M)
        if pat.search(body):
            body = pat.sub(f"{name} = {rendered}", body, count=1)
        else:
            body = body.rstrip("\n") + f"\n{name} = {rendered}\n"
        text = text[:at] + body + text[nxt:]

    path.write_text(text)
    return path


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------


def case_config_rejection(tmp: pathlib.Path) -> None:
    """A-02, A-06, P-group: an impossible configuration stops startup."""
    bad = tmp / "bad.toml"

    write_config(bad, identity__mystery_knob=5)
    r = run_adapter(tmp, bad, ["--check-config"])
    check("CFG-01 unknown key rejected",
          r.code == EXIT_CONFIG_INVALID and "unknown configuration key" in r.stderr,
          f"exit={r.code}")

    write_config(bad, input__mode="tcp", input__port=9009,
                 freshness__clock_mode="monotonic_live")
    r = run_adapter(tmp, bad, ["--check-config"])
    check("CFG-02 port 9009 rejected",
          r.code == EXIT_CONFIG_INVALID and "9009" in r.stderr, f"exit={r.code}")

    write_config(bad, input__mode="tcp", input__host="203.0.113.9",
                 freshness__clock_mode="monotonic_live")
    r = run_adapter(tmp, bad, ["--check-config"])
    check("CFG-03 non-loopback host rejected",
          r.code == EXIT_CONFIG_INVALID and "loopback" in r.stderr,
          f"exit={r.code}")

    good = write_config(tmp / "good.toml")
    r = run_adapter(tmp, good, ["--check-config"])
    check("CFG-04 valid config accepted", r.code == EXIT_OK,
          f"exit={r.code} {r.stderr[-200:]}")

    # The documented CLI overrides rewrite keys the file already defines.
    # Appending them instead would hit the duplicate-key rule, so this
    # exercises the path the README tells operators to use.
    r = run_adapter(tmp, good, ["--check-config", "--input-mode", "pbstream",
                                "--input", "fixtures/normal/golden.pbstream",
                                "--sink", "metrics_only"])
    check("CFG-05 CLI overrides apply to existing keys", r.code == EXIT_OK,
          f"exit={r.code} {r.stderr[-200:]}")

    # An override still goes through the same validator as the file.
    r = run_adapter(tmp, good, ["--check-config", "--port", "9009",
                                "--input-mode", "tcp"])
    check("CFG-06 an override is validated like any other value",
          r.code == EXIT_CONFIG_INVALID and "9009" in r.stderr,
          f"exit={r.code}")


def case_golden_offline(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-01: a verified source, consumed end to end."""
    out = tmp / "out" / "golden.ndjson"
    cfg = write_config(tmp / "golden.toml",
                       input__file=str(fixtures / "normal" / "golden.pbstream"),
                       input__require_completed_manifest=True,
                       validation__mode="offline_verify",
                       output__sink="ndjson_test",
                       output__file=str(out))
    r = run_adapter(tmp, cfg, timeout=120)

    check("E2E-01 exit code", r.code == EXIT_OK,
          f"exit={r.code} {r.stderr[-300:]}")
    check("E2E-01 status COMPLETE_OFFLINE",
          r.field("status") == "COMPLETE_OFFLINE", str(r.field("status")))
    check("E2E-01 provenance verified",
          r.field("source_completeness") == "VERIFIED_COMPLETE_FILE")
    check("E2E-01 conservation holds",
          r.field("all_conservation_equations_hold") == "true")
    check("E2E-01 no unexpected rejects",
          r.int_field("structural_reject") == 0 and
          r.int_field("business_reject") == 0 and
          r.int_field("proto_decode_error") == 0)
    check("E2E-01 no false gaps", r.int_field("gap_incidents") == 0,
          str(r.int_field("gap_incidents")))
    check("E2E-01 artifact committed",
          out.exists() and out.with_suffix(".ndjson.complete").exists() and
          not out.with_suffix(".ndjson.part").exists())

    expected = json.loads(
        (fixtures / "expected" / "golden.expected.json").read_text())
    got = ([json.loads(line) for line in out.read_text().splitlines()]
           if out.exists() else [])
    if len(got) != len(expected):
        check("E2E-01 projection matches the oracle", False,
              f"{len(got)} rows vs {len(expected)} expected")
    else:
        mismatch = None
        for i, (g, e) in enumerate(zip(got, expected)):
            if any(g.get(k) != v for k, v in e.items()):
                mismatch = i
                break
        check("E2E-01 projection matches the oracle", mismatch is None,
              "" if mismatch is None else
              f"first mismatch at row {mismatch}: {got[mismatch]}")


def case_sequence_oracle(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-03 through the real binary."""
    exp = json.loads((fixtures / "expected" / "e2e03.expected.json").read_text())
    out = tmp / "out" / "seq.ndjson"
    cfg = write_config(
        tmp / "seq.toml",
        input__file=str(fixtures / "sequence_oracles" / "e2e03.pbstream"),
        input__require_completed_manifest=True,
        output__sink="ndjson_test", output__file=str(out))
    r = run_adapter(tmp, cfg)

    rows = ([json.loads(x) for x in out.read_text().splitlines()]
            if out.exists() else [])
    published = [x["sequence"] for x in rows]
    check("E2E-03 published sequences", published == exp["published_sequences"],
          f"{published} vs {exp['published_sequences']}")
    check("E2E-03 anomaly counts",
          r.int_field("duplicate_exact") == 1 and
          r.int_field("duplicate_conflict") == 1 and
          r.int_field("reorder_late") == 1 and
          r.int_field("gap_incidents") == 1,
          f"dup={r.int_field('duplicate_exact')} "
          f"conflict={r.int_field('duplicate_conflict')} "
          f"late={r.int_field('reorder_late')} "
          f"gap={r.int_field('gap_incidents')}")
    check("E2E-03 observed vs published continuity",
          r.int_field("contiguous_observed_valid") ==
          exp["contiguous_observed_valid"] and
          r.int_field("contiguous_published") == exp["contiguous_published"],
          f"observed={r.int_field('contiguous_observed_valid')} "
          f"published={r.int_field('contiguous_published')}")
    check("E2E-03 the gapped event carries DEGRADED",
          any(x["sequence"] == 4 and "DEGRADED" in x.get("quality_flags", [])
              for x in rows))


def case_malformed(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-06: every malformed input fails closed, with no partial output."""
    cases = [
        ("malformed_mdp1/bad_magic.pbstream", "BAD_MAGIC"),
        ("malformed_mdp1/wrong_protocol.pbstream", "BAD_MAGIC"),
        ("malformed_mdp1/no_magic.pbstream", "BAD_MAGIC"),
        ("malformed_mdp1/length_zero.pbstream", "INVALID_LENGTH"),
        ("malformed_mdp1/length_too_big.pbstream", "INVALID_LENGTH"),
        ("malformed_mdp1/length_uint32_max.pbstream", "INVALID_LENGTH"),
        ("malformed_mdp1/truncated_prefix.pbstream", "TRUNCATED_PREFIX"),
        ("malformed_mdp1/truncated_header.pbstream", "TRUNCATED_HEADER"),
        ("malformed_mdp1/truncated_body.pbstream", "TRUNCATED_BODY"),
        ("malformed_mdp1/second_magic_midstream.pbstream", "INVALID_LENGTH"),
        ("malformed_mdp1/trailing_garbage.pbstream", "TRUNCATED_HEADER"),
        ("malformed_protobuf/runaway_varint.pbstream", "PROTO_DECODE_ERROR"),
        ("malformed_protobuf/truncated_field.pbstream", "PROTO_DECODE_ERROR"),
        ("malformed_protobuf/reserved_wire_type.pbstream", "PROTO_DECODE_ERROR"),
        ("malformed_protobuf/tag_without_value.pbstream", "PROTO_DECODE_ERROR"),
        ("malformed_protobuf/missing_symbol.pbstream", "MISSING_SYMBOL"),
        ("malformed_protobuf/missing_sequence.pbstream", "MISSING_SEQUENCE"),
        ("malformed_protobuf/missing_event_time.pbstream", "MISSING_EVENT_TIME"),
        ("malformed_protobuf/missing_price_scale.pbstream",
         "MISSING_PRICE_SCALE"),
        ("malformed_protobuf/missing_version.pbstream",
         "MISSING_SOURCE_VERSION"),
        ("malformed_protobuf/no_oneof.pbstream", "MISSING_PAYLOAD_ONEOF"),
        ("malformed_protobuf/quote_without_bid.pbstream", "MISSING_QUOTE_FIELD"),
        ("malformed_protobuf/tick_without_price.pbstream", "MISSING_TICK_FIELD"),
        ("malformed_protobuf/wrong_schema_version.pbstream",
         "UNSUPPORTED_SCHEMA_VERSION"),
        ("malformed_protobuf/symbol_charset.pbstream", "SYMBOL_CHARSET"),
        ("malformed_protobuf/symbol_too_long.pbstream", "FIELD_TOO_LONG"),
        ("malformed_protobuf/empty_dataset_id.pbstream", "FIELD_TOO_LONG"),
    ]
    for rel, expected_reason in cases:
        name = pathlib.Path(rel).stem
        out = tmp / "out" / f"{name}.ndjson"
        cfg = write_config(tmp / f"{name}.toml",
                           input__file=str(fixtures / rel),
                           input__require_completed_manifest=False,
                           output__sink="ndjson_test",
                           output__file=str(out))
        r = run_adapter(tmp, cfg)
        reason = r.field("terminal_reason")
        ok = (r.code == EXIT_STREAM_FAILED and reason == expected_reason and
              not out.with_suffix(".ndjson.complete").exists())
        check(f"E2E-06 {name}", ok,
              f"exit={r.code} reason={reason} want={expected_reason}")


def case_provenance(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """H-group: completeness has to be proven, never assumed."""
    for name in ("no_manifest", "in_progress", "digest_mismatch"):
        cfg = write_config(tmp / f"prov_{name}.toml",
                           input__file=str(fixtures / "provenance" /
                                           f"{name}.pbstream"),
                           input__require_completed_manifest=True,
                           output__sink="metrics_only")
        r = run_adapter(tmp, cfg)
        check(f"H-{name} refused without proof",
              r.code == EXIT_SOURCE_UNVERIFIED, f"exit={r.code}")

    cfg = write_config(tmp / "prov_relaxed.toml",
                       input__file=str(fixtures / "provenance" /
                                       "no_manifest.pbstream"),
                       input__require_completed_manifest=False,
                       output__sink="metrics_only")
    r = run_adapter(tmp, cfg)
    check("H-relaxed processed but not COMPLETE",
          r.code == EXIT_SOURCE_UNVERIFIED and
          r.field("status") == "INCOMPLETE" and
          (r.int_field("business_ok") or 0) > 0,
          f"exit={r.code} status={r.field('status')} "
          f"ok={r.int_field('business_ok')}")
    check("H-relaxed reports unknown loss, not zero",
          r.field("no_silent_loss_when_full_source_verified") == "null")


def case_business_rejects(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E-group: an invalid event is skipped with a reason; the stream lives."""
    cfg = write_config(
        tmp / "biz.toml",
        input__file=str(fixtures / "sequence_oracles" /
                        "business_invalid.pbstream"),
        input__require_completed_manifest=True,
        output__sink="ndjson_test",
        output__file=str(tmp / "out" / "biz.ndjson"))
    r = run_adapter(tmp, cfg)
    check("E-biz stream survives business rejects", r.code == EXIT_OK,
          f"exit={r.code} {r.stderr[-200:]}")
    check("E-biz three events rejected", r.int_field("business_reject") == 3,
          str(r.int_field("business_reject")))
    reasons = r.field("business_reject_by_reason") or ""
    check("E-biz rejection reasons recorded",
          all(x in reasons for x in ("QUOTE_BID_NON_POSITIVE", "SCALE_MISMATCH",
                                     "INVALID_SEQUENCE")),
          reasons)
    check("E-biz conservation still holds",
          r.field("all_conservation_equations_hold") == "true")


def case_freshness(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-04: event-time staleness, and no false stale on a replay."""
    out = tmp / "out" / "stale.ndjson"
    cfg = write_config(
        tmp / "stale.toml",
        input__file=str(fixtures / "freshness_oracles" /
                        "event_time_old.pbstream"),
        input__require_completed_manifest=True,
        output__sink="ndjson_test", output__file=str(out))
    r = run_adapter(tmp, cfg)
    check("E2E-04 stale quote detected", r.int_field("event_time_old") == 1,
          str(r.int_field("event_time_old")))
    rows = ([json.loads(x) for x in out.read_text().splitlines()]
            if out.exists() else [])
    check("E2E-04 stale quote not published",
          [x["sequence"] for x in rows] == [1, 2, 4],
          str([x["sequence"] for x in rows]))

    cfg = write_config(
        tmp / "hist.toml",
        input__file=str(fixtures / "freshness_oracles" /
                        "historical_replay.pbstream"),
        input__require_completed_manifest=True,
        output__sink="metrics_only")
    r = run_adapter(tmp, cfg)
    check("E2E-04 historical replay is not stale",
          r.code == EXIT_OK and r.int_field("event_time_old") == 0,
          f"exit={r.code} stale={r.int_field('event_time_old')}")


def case_determinism(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-08: the same verified input must produce the same artifact."""
    digests = []
    for i, chunk in enumerate((7, 4096, 65536)):
        out = tmp / "out" / f"det{i}.ndjson"
        cfg = write_config(tmp / f"det{i}.toml",
                           input__file=str(fixtures / "normal" /
                                           "small.pbstream"),
                           input__require_completed_manifest=True,
                           input__read_chunk_bytes=chunk,
                           output__sink="ndjson_test", output__file=str(out))
        r = run_adapter(tmp, cfg)
        if r.code != EXIT_OK:
            check("E2E-08 determinism", False, f"run {i} exit={r.code}")
            return
        digests.append(r.field("artifact_sha256"))
    check("E2E-08 artifact digest is stable across read sizes",
          len(set(digests)) == 1, str(digests))


def case_p04_handoff(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-09: the Kafka-shaped value is raw protobuf, quality is separate."""
    out = tmp / "out" / "p04.ndjson"
    cfg = write_config(tmp / "p04.toml",
                       input__file=str(fixtures / "sequence_oracles" /
                                       "e2e03.pbstream"),
                       input__require_completed_manifest=True,
                       output__sink="p04_stub", output__file=str(out))
    r = run_adapter(tmp, cfg)
    if r.code != EXIT_OK or not out.exists():
        check("E2E-09 P04 handoff", False,
              f"exit={r.code} {r.stderr[-200:]}")
        return
    rows = [json.loads(x) for x in out.read_text().splitlines()]
    check("E2E-09 record.key is the symbol",
          all(row["key"] in ("EURUSD", "GBPUSD", "USDJPY") for row in rows))
    check("E2E-09 record.value carries no MDP1 framing",
          all(not bytes.fromhex(row["value_hex"]).startswith(b"MDP1")
              for row in rows))
    check("E2E-09 quality travels on a side channel",
          all("quality" in row and "flags" in row["quality"] for row in rows))

    import mdp1 as _mdp1
    mde = _mdp1.message_class(PROTOC)
    seqs = [mde.FromString(bytes.fromhex(row["value_hex"])).sequence
            for row in rows]
    check("E2E-09 values decode back to the published events",
          seqs == [1, 2, 4, 5], str(seqs))


def case_huge_gap(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """F-group: an absurd sequence jump is bounded work and bounded memory."""
    cfg = write_config(tmp / "huge.toml",
                       input__file=str(fixtures / "sequence_oracles" /
                                       "huge_gap.pbstream"),
                       input__require_completed_manifest=True,
                       output__sink="metrics_only")
    start = time.monotonic()
    r = run_adapter(tmp, cfg, timeout=30)
    elapsed = time.monotonic() - start
    check("F-huge gap completes quickly", r.code == EXIT_OK and elapsed < 10,
          f"exit={r.code} {elapsed:.2f}s")
    check("F-huge gap produces two events, not a loop",
          r.int_field("business_ok") == 2, str(r.int_field("business_ok")))
    rss = r.int_field("peak_rss_bytes") or 0
    check("F-huge gap keeps RSS bounded", 0 < rss < 512 * 1024 * 1024,
          f"rss={rss}")


def case_backpressure(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-07: a stalled sink fails the run instead of dropping events."""
    out = tmp / "out" / "bp.ndjson"
    cfg = write_config(tmp / "bp.toml",
                       input__file=str(fixtures / "normal" / "small.pbstream"),
                       input__require_completed_manifest=True,
                       output__sink="p04_stub", output__file=str(out),
                       output__stub_delay_us=300000,
                       output__max_queue_events=2,
                       output__output_stall_deadline_ms=100,
                       output__shutdown_drain_deadline_ms=100)
    r = run_adapter(tmp, cfg, timeout=120)
    check("E2E-07 stalled sink fails the run",
          r.code == EXIT_BACKPRESSURE and
          r.field("terminal_reason") == "BACKPRESSURE_TIMEOUT",
          f"exit={r.code} reason={r.field('terminal_reason')}")
    check("E2E-07 pending events are reported",
          (r.int_field("output_failed_or_pending") or 0) > 0,
          str(r.int_field("output_failed_or_pending")))
    check("E2E-07 artifact is left incomplete",
          not out.with_suffix(".ndjson.complete").exists() and
          out.with_suffix(".ndjson.part").exists())
    check("E2E-07 the throttle is disclosed in the report",
          r.int_field("sink_stub_delay_us") == 300000,
          str(r.int_field("sink_stub_delay_us")))


def case_sink_io(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """I-group: an unwritable sink is a hard failure, not a silent skip."""
    cfg = write_config(tmp / "sinkio.toml",
                       input__file=str(fixtures / "normal" / "small.pbstream"),
                       input__require_completed_manifest=True,
                       output__sink="ndjson_test",
                       output__file="out/nested/deep/out.ndjson")
    # A path the process genuinely cannot create: a file standing in for a
    # directory component.
    (tmp / "blocked").write_text("not a directory\n")
    cfg = write_config(tmp / "sinkio.toml",
                       input__file=str(fixtures / "normal" / "small.pbstream"),
                       input__require_completed_manifest=True,
                       output__sink="ndjson_test",
                       output__file=str(tmp / "blocked" / "out.ndjson"))
    r = run_adapter(tmp, cfg)
    check("I-unwritable sink fails at startup", r.code == EXIT_SINK_IO,
          f"exit={r.code}")


def case_fault_observe(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """Design section 5: the fault lane preserves every anomaly in arrival
    order so a detector can be scored, and tools/fault_sink.py scores it
    against the plan that produced the corpus. A flag the plan does not
    contain fails just as hard as a missed one."""
    out = tmp / "out" / "faults.ndjson"
    plan = fixtures / "fault_observe" / "plan.json"
    cfg = write_config(tmp / "faults.toml",
                       input__file=str(fixtures / "fault_observe" /
                                       "injected.pbstream"),
                       input__require_completed_manifest=True,
                       validation__mode="fault_observe",
                       validation__fault_manifest=str(plan),
                       output__sink="ndjson_test", output__file=str(out))
    r = run_adapter(tmp, cfg)
    if r.code != EXIT_OK or not out.exists():
        check("FAULT-01 fault_observe run completes", False,
              f"exit={r.code} {r.stderr[-200:]}")
        return
    check("FAULT-01 fault_observe run completes", True)

    rows = [json.loads(x) for x in out.read_text().splitlines()]
    # Everything business-valid is forwarded in arrival order, unlike strict.
    check("FAULT-02 every event is preserved in arrival order",
          [x["sequence"] for x in rows] ==
          [1, 2, 3, 5, 4, 6, 6, 7, 7, 8, 9],
          str([x["sequence"] for x in rows]))
    check("FAULT-03 events are marked test-only",
          all("FAULT_TEST_ONLY" in x.get("quality_flags", []) for x in rows))

    scored = subprocess.run(
        [sys.executable, str(TOOLS / "fault_sink.py"),
         "--plan", str(plan), "--events", str(out)],
        capture_output=True, text=True, timeout=60)
    check("FAULT-04 detected anomalies match the injected plan exactly",
          scored.returncode == 0,
          " | ".join(scored.stdout.split()))


def _serve(pbstream: pathlib.Path, extra):
    proc = subprocess.Popen(
        [sys.executable, str(TOOLS / "tcp_chunker.py"), "--file", str(pbstream),
         "--port", "0"] + list(extra),
        stdout=subprocess.PIPE, text=True)
    line = proc.stdout.readline().strip()
    return proc, int(line.split()[1])


def case_tcp_live(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-02 / A / B groups over a real socket."""
    pbstream = fixtures / "normal" / "small.pbstream"

    for label, extra in [("one-shot", []), ("1-byte chunks", ["--chunk", "1"]),
                         ("7-byte chunks", ["--chunk", "7"])]:
        srv, port = _serve(pbstream, extra)
        try:
            cfg = write_config(tmp / "live.toml", input__mode="tcp",
                               input__port=port,
                               freshness__clock_mode="monotonic_live",
                               output__sink="metrics_only",
                               reconnect__max_attempts=1)
            r = run_adapter(tmp, cfg, timeout=120)
            check(f"E2E-02 live {label}",
                  r.code == EXIT_OK and
                  r.int_field("frames_complete") == 100 and
                  r.field("status") == "PARTIAL_LIVE",
                  f"exit={r.code} frames={r.int_field('frames_complete')} "
                  f"status={r.field('status')}")
        finally:
            srv.wait(timeout=20)

    srv, port = _serve(pbstream, ["--silence-ms", "3000", "--hold-ms", "3000"])
    try:
        cfg = write_config(tmp / "silent.toml", input__mode="tcp",
                           input__port=port, input__prefix_deadline_ms=300,
                           freshness__clock_mode="monotonic_live",
                           output__sink="metrics_only",
                           reconnect__max_attempts=1)
        r = run_adapter(tmp, cfg, timeout=60)
        check("A-07 silent peer trips the prefix deadline",
              r.code == EXIT_STREAM_FAILED and
              r.field("terminal_reason") == "FRAME_TIMEOUT",
              f"exit={r.code} reason={r.field('terminal_reason')}")
    finally:
        srv.kill()
        srv.wait(timeout=20)

    jsonl = tmp / "jsonl.bin"
    jsonl.write_bytes(b'{"symbol":"EURUSD","sequence":1}\n' * 10)
    srv, port = _serve(jsonl, [])
    try:
        cfg = write_config(tmp / "jsonl.toml", input__mode="tcp",
                           input__port=port,
                           freshness__clock_mode="monotonic_live",
                           output__sink="metrics_only",
                           reconnect__max_attempts=1)
        r = run_adapter(tmp, cfg, timeout=60)
        check("A-01 JSONL on the MDP1 port fails closed",
              r.code == EXIT_STREAM_FAILED and
              r.field("terminal_reason") == "BAD_MAGIC",
              f"exit={r.code} reason={r.field('terminal_reason')}")
    finally:
        srv.wait(timeout=20)

    size = pbstream.stat().st_size
    srv, port = _serve(pbstream, ["--truncate-at", str(size - 5)])
    try:
        cfg = write_config(tmp / "trunc.toml", input__mode="tcp",
                           input__port=port,
                           freshness__clock_mode="monotonic_live",
                           output__sink="metrics_only",
                           reconnect__max_attempts=1)
        r = run_adapter(tmp, cfg, timeout=60)
        check("B-mid-frame close is reported as truncated",
              r.code == EXIT_STREAM_FAILED and
              r.field("terminal_reason") == "TRUNCATED_BODY",
              f"exit={r.code} reason={r.field('terminal_reason')}")
    finally:
        srv.wait(timeout=20)


def case_signals(tmp: pathlib.Path, fixtures: pathlib.Path) -> None:
    """E2E-10 / K-group: SIGTERM stops cleanly; SIGKILL leaves a .part."""
    pbstream = fixtures / "normal" / "golden.pbstream"

    srv, port = _serve(pbstream, ["--chunk", "64", "--delay-us", "2000"])
    out = tmp / "out" / "sigterm.ndjson"
    report = tmp / "sigterm.report.yaml"
    cfg = write_config(tmp / "sigterm.toml", input__mode="tcp",
                       input__port=port,
                       freshness__clock_mode="monotonic_live",
                       output__sink="ndjson_test", output__file=str(out),
                       reconnect__max_attempts=1)
    child = subprocess.Popen([BIN, "--config", str(cfg), "--report",
                              str(report)], cwd=tmp, stderr=subprocess.PIPE,
                             text=True)
    time.sleep(1.0)
    child.send_signal(signal.SIGTERM)
    try:
        child.wait(timeout=30)
        stopped = True
    except subprocess.TimeoutExpired:
        child.kill()
        stopped = False
    srv.kill()
    srv.wait(timeout=20)
    check("K-SIGTERM stops the run promptly", stopped,
          "" if stopped else "did not exit within 30 s")
    check("K-SIGTERM still writes a report", report.exists())

    srv, port = _serve(pbstream, ["--chunk", "64", "--delay-us", "2000"])
    out2 = tmp / "out" / "sigkill.ndjson"
    cfg = write_config(tmp / "sigkill.toml", input__mode="tcp",
                       input__port=port,
                       freshness__clock_mode="monotonic_live",
                       output__sink="ndjson_test", output__file=str(out2),
                       reconnect__max_attempts=1)
    child = subprocess.Popen([BIN, "--config", str(cfg)], cwd=tmp,
                             stderr=subprocess.DEVNULL)
    time.sleep(1.0)
    child.kill()
    child.wait(timeout=10)
    srv.kill()
    srv.wait(timeout=20)
    check("K-SIGKILL leaves an incomplete artifact",
          out2.with_suffix(".ndjson.part").exists() and
          not out2.with_suffix(".ndjson.complete").exists())


# ---------------------------------------------------------------------------


def main() -> int:
    if not BIN or not pathlib.Path(BIN).exists():
        print("MD_ADAPTER_BIN is not set to an existing binary", file=sys.stderr)
        return 2

    tmp = pathlib.Path(tempfile.mkdtemp(prefix="p03-e2e-"))
    (tmp / "out").mkdir(parents=True, exist_ok=True)
    fixtures = tmp / "fixtures"
    try:
        import make_fixtures
        make_fixtures.build(fixtures, PROTOC)
    except Exception as exc:  # noqa: BLE001
        # Without fixtures nothing below can run. Report it as a skip with a
        # reason rather than letting the matrix look green.
        record("FIXTURES", "SKIPPED", f"could not build fixtures: {exc}")
        shutil.rmtree(tmp, ignore_errors=True)
        return 2

    failed = 0
    try:
        case_config_rejection(tmp)
        case_golden_offline(tmp, fixtures)
        case_sequence_oracle(tmp, fixtures)
        case_malformed(tmp, fixtures)
        case_provenance(tmp, fixtures)
        case_business_rejects(tmp, fixtures)
        case_fault_observe(tmp, fixtures)
        case_freshness(tmp, fixtures)
        case_determinism(tmp, fixtures)
        case_p04_handoff(tmp, fixtures)
        case_huge_gap(tmp, fixtures)
        case_backpressure(tmp, fixtures)
        case_sink_io(tmp, fixtures)
        case_tcp_live(tmp, fixtures)
        case_signals(tmp, fixtures)
    finally:
        passed = sum(1 for _, v, _ in results if v == "PASS")
        failed = sum(1 for _, v, _ in results if v == "FAIL")
        skipped = sum(1 for _, v, _ in results if v == "SKIPPED")
        print(f"\nmatrix: {passed} passed, {failed} failed, {skipped} skipped")
        if failed:
            print("failed cases:")
            for cid, verdict, detail in results:
                if verdict == "FAIL":
                    print(f"  - {cid}: {detail}")
        if os.environ.get("MD_E2E_KEEP"):
            print(f"workdir kept at {tmp}")
        else:
            shutil.rmtree(tmp, ignore_errors=True)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
