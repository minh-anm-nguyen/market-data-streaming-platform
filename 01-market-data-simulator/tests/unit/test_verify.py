"""Verifier verdicts (section 5.1, cases F03, F06, F08, F17, F18, C04, C13).

The verifier is not allowed to trust the recorder, so these tests build run
directories by hand and then damage them in specific ways. Each verdict has to be
reachable, and each has to be unreachable by accident.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from market_sim.recorder import (
    COMPLETED_NAME,
    EVENTS_NAME,
    EVENTS_PART_NAME,
    FAULT_MANIFEST_NAME,
    MANIFEST_NAME,
    REPORT_NAME,
)
from market_sim.verify import Verdict, hash_file, iter_lines, verify_run

DATASET = "ds-0123456789abcdef"


def record(symbol="EURUSD", seq=1, bid=108346, ask=108348, t=1793322000000000000):
    return json.dumps(
        {
            "ask_ticks": ask,
            "bid_ticks": bid,
            "dataset_id": DATASET,
            "event_time_ns": t,
            "event_type": "quote",
            "price_scale": 100000,
            "schema_version": "sim.v1",
            "sequence": seq,
            "symbol": symbol,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def build_run(
    tmp_path,
    *,
    records=None,
    marker=True,
    report=True,
    manifest=True,
    part=False,
    faults=None,
    status="COMPLETED",
    exit_reason="slot_count_reached",
    slots=None,
    omissions=0,
    duplicates=0,
    override_sha=None,
    name="run-a",
):
    """Assemble a run directory, optionally damaged."""
    run = tmp_path / name
    run.mkdir(parents=True)
    if records is None:
        records = [record(seq=i) for i in range(1, 4)]
    body = "".join(line + "\n" for line in records).encode("utf-8")
    events = run / (EVENTS_PART_NAME if part else EVENTS_NAME)
    events.write_bytes(body)
    sha = override_sha or hashlib.sha256(body).hexdigest()
    lines = len(records)
    slots = lines if slots is None else slots

    if manifest:
        (run / MANIFEST_NAME).write_text(
            json.dumps({"dataset_id": DATASET, "mode": "record", "seed": 1}),
            encoding="utf-8",
        )
    if report:
        (run / REPORT_NAME).write_text(
            json.dumps(
                {
                    "status": status,
                    "exit_reason": exit_reason,
                    "mode": "record",
                    "planned_logical_slots": slots,
                    "counts": {
                        "logical_slots": slots,
                        "emitted_events": lines,
                        "intentional_omissions": omissions,
                        "intentional_duplicates": duplicates,
                        "recorded_events": lines,
                    },
                    "file": {"sha256": sha},
                }
            ),
            encoding="utf-8",
        )
    if marker:
        (run / COMPLETED_NAME).write_text(
            json.dumps(
                {"events_sha256": sha, "events_lines": lines, "events_bytes": len(body)}
            ),
            encoding="utf-8",
        )
    if faults is not None:
        (run / FAULT_MANIFEST_NAME).write_text(
            "".join(json.dumps(f) + "\n" for f in faults), encoding="utf-8"
        )
    return run


class TestComplete:
    def test_a_clean_committed_run_verifies(self, tmp_path):
        result = verify_run(build_run(tmp_path))
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.ok is True
        assert result.reasons == []
        assert result.lines == 3

    def test_reports_the_recomputed_hash(self, tmp_path):
        run = build_run(tmp_path)
        result = verify_run(run)
        assert result.sha256 == result.expected_sha256
        assert result.sha256 == hashlib.sha256((run / EVENTS_NAME).read_bytes()).hexdigest()

    def test_empty_dataset_is_complete(self, tmp_path):
        result = verify_run(build_run(tmp_path, records=[]))
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.lines == 0

    def test_per_symbol_observations_are_reported(self, tmp_path):
        records = [record(symbol="EURUSD", seq=i) for i in range(1, 4)] + [
            record(symbol="GBPUSD", seq=i) for i in range(1, 3)
        ]
        result = verify_run(build_run(tmp_path, records=records))
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.symbols["EURUSD"]["count"] == 3
        assert result.symbols["GBPUSD"]["last_sequence"] == 2

    def test_same_sequence_across_symbols_is_not_a_duplicate(self, tmp_path):
        # Case S05: dedup scope is (dataset_id, symbol, sequence).
        records = [record(symbol="EURUSD", seq=1), record(symbol="GBPUSD", seq=1)]
        result = verify_run(build_run(tmp_path, records=records))
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.totals["duplicates"] == 0


class TestIncomplete:
    def test_missing_directory(self, tmp_path):
        result = verify_run(tmp_path / "nope")
        assert result.verdict is Verdict.INCOMPLETE

    def test_no_events_file_at_all(self, tmp_path):
        run = tmp_path / "run-a"
        run.mkdir()
        result = verify_run(run)
        assert result.verdict is Verdict.INCOMPLETE

    def test_part_file_is_never_a_dataset(self, tmp_path):
        # Case F03: a killed run leaves .part behind; it is evidence, not data.
        result = verify_run(build_run(tmp_path, part=True, marker=False))
        assert result.verdict is Verdict.INCOMPLETE
        assert any(".part" in reason for reason in result.reasons)

    def test_events_jsonl_without_a_marker_is_incomplete(self, tmp_path):
        # Case F17: the crucial one. A plausible-looking file with no commit
        # marker must not be mistaken for a successful run.
        result = verify_run(build_run(tmp_path, marker=False))
        assert result.verdict is Verdict.INCOMPLETE
        assert any(COMPLETED_NAME in reason for reason in result.reasons)

    def test_missing_report_is_incomplete(self, tmp_path):
        result = verify_run(build_run(tmp_path, report=False))
        assert result.verdict is Verdict.INCOMPLETE

    def test_incomplete_is_not_downgraded_to_invalid(self, tmp_path):
        # "The run never finished" explains any reconciliation mismatch, so it
        # must win over "the data contradicts itself".
        result = verify_run(
            build_run(tmp_path, marker=False, report=False, part=True, slots=99)
        )
        assert result.verdict is Verdict.INCOMPLETE


class TestCorrupt:
    def test_hash_mismatch_after_a_byte_flip(self, tmp_path):
        # Case F18: the file is exactly the right size, so only recomputing the
        # hash can catch it. A filesize or line-count check would pass.
        run = build_run(tmp_path)
        raw = bytearray((run / EVENTS_NAME).read_bytes())
        index = raw.index(b"108346") + 5
        raw[index] = ord("7") if raw[index] == ord("6") else ord("6")
        (run / EVENTS_NAME).write_bytes(bytes(raw))
        result = verify_run(run)
        assert result.verdict is Verdict.CORRUPT
        assert any("SHA-256 mismatch" in reason for reason in result.reasons)

    def test_hash_mismatch_against_a_tampered_manifest(self, tmp_path):
        # Case F06: distinguishes corrupt/tampered from merely incomplete.
        result = verify_run(build_run(tmp_path, override_sha="0" * 64))
        assert result.verdict is Verdict.CORRUPT

    def test_unparseable_line(self, tmp_path):
        run = build_run(tmp_path, records=[record(seq=1), "{not json", record(seq=3)])
        result = verify_run(run)
        assert result.verdict is Verdict.CORRUPT
        assert any("invalid_json" in reason for reason in result.reasons)

    def test_line_with_an_unknown_field(self, tmp_path):
        bad = json.dumps({**json.loads(record()), "volume": 10}, sort_keys=True)
        result = verify_run(build_run(tmp_path, records=[bad]))
        assert result.verdict is Verdict.CORRUPT
        assert any("field_set_mismatch" in reason for reason in result.reasons)

    def test_torn_final_line_without_a_newline(self, tmp_path):
        # Case F08: never silently drop the last record.
        run = build_run(tmp_path)
        body = (run / EVENTS_NAME).read_bytes() + b'{"ask_ticks":1,"bid'
        (run / EVENTS_NAME).write_bytes(body)
        result = verify_run(run)
        assert result.verdict is Verdict.CORRUPT
        assert any("newline" in reason for reason in result.reasons)
        assert result.final_newline is False

    def test_unreadable_events_file(self, tmp_path):
        run = build_run(tmp_path)
        (run / EVENTS_NAME).unlink()
        (run / EVENTS_NAME).mkdir()
        result = verify_run(run)
        assert result.verdict in (Verdict.CORRUPT, Verdict.INCOMPLETE)


class TestInvalid:
    def test_report_line_count_disagrees_with_the_file(self, tmp_path):
        run = build_run(tmp_path)
        report = json.loads((run / REPORT_NAME).read_text())
        report["counts"]["recorded_events"] = 99
        (run / REPORT_NAME).write_text(json.dumps(report), encoding="utf-8")
        result = verify_run(run)
        assert result.verdict is Verdict.INVALID
        assert any("99 recorded events" in reason for reason in result.reasons)

    def test_counts_that_do_not_reconcile(self, tmp_path):
        # emitted == slots - omissions + duplicates, checked independently of
        # whatever the engine believed at the time.
        run = build_run(tmp_path, omissions=5)
        result = verify_run(run)
        assert result.verdict is Verdict.INVALID
        assert any("do not reconcile" in reason for reason in result.reasons)

    def test_a_run_claiming_it_reached_its_count_but_short_of_it_is_invalid(
        self, tmp_path
    ):
        # The report says "slot_count_reached", so producing fewer is a
        # self-contradiction, not an early stop.
        run = build_run(tmp_path, name="r2", exit_reason="slot_count_reached")
        report = json.loads((run / REPORT_NAME).read_text())
        report["planned_logical_slots"] = 10
        (run / REPORT_NAME).write_text(json.dumps(report), encoding="utf-8")
        result = verify_run(run)
        assert result.verdict is Verdict.INVALID
        assert any("reached its slot count" in r for r in result.reasons)

    def test_a_run_stopped_by_a_signal_may_legitimately_be_short(self, tmp_path):
        # NFR-05: a drained SIGTERM is a successful run that stopped early, and
        # the verifier must not call that a contradiction.
        run = build_run(tmp_path, name="r3", exit_reason="signal:SIGTERM")
        report = json.loads((run / REPORT_NAME).read_text())
        report["planned_logical_slots"] = 10
        (run / REPORT_NAME).write_text(json.dumps(report), encoding="utf-8")
        result = verify_run(run)
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert any("of 10 planned slots" in w for w in result.warnings)
        assert any("signal:SIGTERM" in w for w in result.warnings)

    def test_a_failed_run_short_of_its_plan_is_a_warning_not_a_contradiction(
        self, tmp_path
    ):
        run = build_run(tmp_path, status="FAILED", exit_reason="OUTPUT_DISK_FULL: ...")
        report = json.loads((run / REPORT_NAME).read_text())
        report["planned_logical_slots"] = 10
        (run / REPORT_NAME).write_text(json.dumps(report), encoding="utf-8")
        result = verify_run(run)
        assert any("of 10 planned slots" in w for w in result.warnings)

    def test_mixed_dataset_ids_in_one_file(self, tmp_path):
        other = json.loads(record(seq=4))
        other["dataset_id"] = "ds-ffffffffffffffff"
        records = [record(seq=i) for i in range(1, 4)] + [
            json.dumps(other, sort_keys=True, separators=(",", ":"))
        ]
        result = verify_run(build_run(tmp_path, records=records))
        assert result.verdict is Verdict.INVALID
        assert any("dataset ids" in reason for reason in result.reasons)

    def test_records_disagreeing_with_the_manifest_dataset_id(self, tmp_path):
        run = build_run(tmp_path)
        (run / MANIFEST_NAME).write_text(
            json.dumps({"dataset_id": "ds-aaaaaaaaaaaaaaaa", "mode": "record"}),
            encoding="utf-8",
        )
        result = verify_run(run)
        assert result.verdict is Verdict.INVALID


def fault_row(kind, sequence, applied=True, reason=None, affected=None, symbol="EURUSD"):
    """One fault-manifest row, in the exact shape the injector writes."""
    return {
        "fault_id": "f0001",
        "kind": kind,
        "target": {"symbol": symbol, "sequence": sequence},
        "planned": True,
        "applied": applied,
        "affected_sequences": affected if affected is not None else ([sequence] if applied else []),
        "output_impact": "see kind" if applied else "none",
        "reason_if_not_applied": reason,
        "channel": "payload",
    }


class TestAnomalyCrossCheck:
    """Case C04: an anomaly must be explained, and a fault must not be over-claimed."""

    def test_gap_without_a_fault_manifest_is_invalid(self, tmp_path):
        records = [record(seq=1), record(seq=2), record(seq=4)]
        result = verify_run(build_run(tmp_path, records=records))
        assert result.verdict is Verdict.INVALID
        assert any("no fault-manifest" in r for r in result.reasons)

    def test_gap_explained_by_an_applied_fault_is_complete(self, tmp_path):
        records = [record(seq=1), record(seq=2), record(seq=4)]
        result = verify_run(
            build_run(
                tmp_path,
                records=records,
                faults=[fault_row("missing_sequence", 3)],
                slots=4,
                omissions=1,
            )
        )
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.totals["gaps"] == 1
        assert result.fault_manifest["applied"] == 1

    def test_more_gaps_than_the_manifest_explains_is_invalid(self, tmp_path):
        records = [record(seq=1), record(seq=4), record(seq=8)]
        result = verify_run(
            build_run(tmp_path, records=records, faults=[fault_row("missing_sequence", 3)])
        )
        assert result.verdict is Verdict.INVALID
        assert any("sequence gap" in r for r in result.reasons)

    def test_duplicate_explained_by_the_manifest(self, tmp_path):
        records = [record(seq=1), record(seq=2), record(seq=2)]
        result = verify_run(
            build_run(
                tmp_path,
                records=records,
                faults=[fault_row("duplicate_exact", 2)],
                slots=2,
                duplicates=1,
            )
        )
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.totals["duplicates"] == 1
        assert result.totals["conflicting_duplicates"] == 0

    def test_conflicting_duplicate_is_reported_separately(self, tmp_path):
        # Case S03: same key, different bytes. Deduplicating by key would hide
        # this rather than resolve it, so it gets its own counter.
        records = [record(seq=1), record(seq=1, bid=999, ask=1000)]
        result = verify_run(
            build_run(
                tmp_path,
                records=records,
                faults=[fault_row("duplicate_conflict", 1)],
                slots=1,
                duplicates=1,
            )
        )
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.totals["conflicting_duplicates"] == 1

    def test_claimed_conflict_with_identical_payloads_is_invalid(self, tmp_path):
        # Over-claiming a fault is as bad as missing one.
        records = [record(seq=1), record(seq=1)]
        result = verify_run(
            build_run(
                tmp_path,
                records=records,
                faults=[fault_row("duplicate_conflict", 1)],
                slots=1,
                duplicates=1,
            )
        )
        assert result.verdict is Verdict.INVALID
        assert any("no conflicting payloads" in r for r in result.reasons)

    def test_out_of_order_explained_by_a_reorder_fault(self, tmp_path):
        records = [record(seq=1), record(seq=3), record(seq=2)]
        result = verify_run(
            build_run(
                tmp_path,
                records=records,
                faults=[fault_row("reorder_adjacent", 2, affected=[2, 3])],
            )
        )
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.totals["out_of_order"] == 1
        assert result.totals["gaps"] == 0

    def test_unapplied_faults_and_their_reasons_are_surfaced(self, tmp_path):
        result = verify_run(
            build_run(
                tmp_path,
                faults=[
                    fault_row(
                        "missing_sequence",
                        9999,
                        applied=False,
                        reason="target event never occurred in this run",
                    )
                ],
            )
        )
        assert result.verdict is Verdict.COMPLETE, result.reasons
        assert result.fault_manifest["not_applied"] == 1
        assert "never occurred" in result.fault_manifest["not_applied_reasons"][0]["reason"]


class TestStreamingLimits:
    def test_line_longer_than_the_limit_is_rejected(self, tmp_path):
        # Case C13: verifying a huge file must not be bounded by RAM.
        run = build_run(tmp_path)
        (run / EVENTS_NAME).write_bytes(b"x" * 5000 + b"\n")
        result = verify_run(run, max_line_bytes=1000)
        assert result.verdict is Verdict.CORRUPT

    def test_anomaly_tracking_can_be_switched_off(self, tmp_path):
        records = [record(seq=1), record(seq=5)]
        result = verify_run(build_run(tmp_path, records=records), check_anomalies=False)
        assert result.totals["gaps"] == 0
        assert result.symbols == {}

    def test_iter_lines_splits_across_chunk_boundaries(self, tmp_path):
        target = tmp_path / "big.jsonl"
        payload = b"".join(f'{{"n":{i}}}\n'.encode() for i in range(50_000))
        target.write_bytes(payload)
        lines = list(iter_lines(target, max_line_bytes=1000))
        assert len(lines) == 50_000
        assert all(had_newline for _, _, had_newline in lines)
        assert lines[0][1] == b'{"n":0}'
        assert lines[-1][1] == b'{"n":49999}'

    def test_iter_lines_flags_a_trailing_fragment(self, tmp_path):
        target = tmp_path / "torn.jsonl"
        target.write_bytes(b'{"a":1}\n{"a":2')
        lines = list(iter_lines(target, max_line_bytes=1000))
        assert [had_newline for _, _, had_newline in lines] == [True, False]

    def test_hash_file_matches_hashlib(self, tmp_path):
        target = tmp_path / "f.bin"
        payload = bytes(range(256)) * 10_000
        target.write_bytes(payload)
        digest, size = hash_file(target)
        assert digest == hashlib.sha256(payload).hexdigest()
        assert size == len(payload)


class TestServeMode:
    def test_serve_run_has_no_dataset_to_verify(self, tmp_path):
        run = tmp_path / "run-serve"
        run.mkdir()
        (run / MANIFEST_NAME).write_text(
            json.dumps({"mode": "serve", "dataset_id": DATASET}), encoding="utf-8"
        )
        (run / REPORT_NAME).write_text(json.dumps({"mode": "serve"}), encoding="utf-8")
        result = verify_run(run)
        assert result.verdict is Verdict.COMPLETE
        assert result.reasons == []
        assert any("no recorded dataset" in w for w in result.warnings)
        assert any("proven delivered" in w for w in result.warnings)

    def test_serve_run_without_a_report_is_incomplete(self, tmp_path):
        run = tmp_path / "run-serve"
        run.mkdir()
        (run / MANIFEST_NAME).write_text(
            json.dumps({"mode": "serve", "dataset_id": DATASET}), encoding="utf-8"
        )
        assert verify_run(run).verdict is Verdict.INCOMPLETE


class TestReorderDoesNotLookLikeAGap:
    def test_a_reorder_is_one_anomaly_not_two(self, tmp_path):
        # The regression this pins: computing gaps on arrival counted a reordered
        # event as both a gap and an out-of-order event.
        records = [record(seq=1), record(seq=3), record(seq=2), record(seq=4)]
        result = verify_run(
            build_run(
                tmp_path,
                records=records,
                faults=[fault_row("reorder_adjacent", 2, affected=[2, 3])],
            )
        )
        assert result.totals["gaps"] == 0
        assert result.totals["out_of_order"] == 1
        assert result.verdict is Verdict.COMPLETE, result.reasons

    def test_a_real_gap_is_still_detected_after_a_reorder(self, tmp_path):
        records = [record(seq=1), record(seq=3), record(seq=2), record(seq=6)]
        result = verify_run(build_run(tmp_path, records=records))
        assert result.symbols["EURUSD"]["gaps"] == [4, 5]


class TestSubstitutedLine:
    """F07: one line replaced by another *valid* record.

    The file still parses perfectly, so only the recomputed hash catches it.
    This is the case that separates "corrupt" from "unparseable".
    """

    def test_a_valid_but_different_line_is_caught_by_the_hash(self, tmp_path):
        run = build_run(tmp_path)
        lines = (run / EVENTS_NAME).read_bytes().splitlines(keepends=True)
        # Replace line 2 with a well-formed record carrying a different price.
        lines[1] = (record(seq=2, bid=111111, ask=111113) + "\n").encode("utf-8")
        (run / EVENTS_NAME).write_bytes(b"".join(lines))

        result = verify_run(run)
        assert result.verdict is Verdict.CORRUPT
        assert any("SHA-256 mismatch" in r for r in result.reasons)
        # Every line still parsed, which is exactly why the hash is the only
        # thing that could have detected this.
        assert result.lines == 3
        assert not any("invalid_json" in r for r in result.reasons)

    def test_a_reordered_file_is_also_caught_by_the_hash(self, tmp_path):
        run = build_run(tmp_path)
        lines = (run / EVENTS_NAME).read_bytes().splitlines(keepends=True)
        lines[0], lines[1] = lines[1], lines[0]
        (run / EVENTS_NAME).write_bytes(b"".join(lines))
        result = verify_run(run)
        assert result.verdict is Verdict.CORRUPT
