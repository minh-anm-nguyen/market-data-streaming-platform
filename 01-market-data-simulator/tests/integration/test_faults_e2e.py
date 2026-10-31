"""Fault-injected datasets end to end (E2E-02, E2E-03, E2E-09, cases S01-S04).

These are the fixtures Project 03's C++ adapter will be graded against, so the
assertions are about what a *receiver* can observe: a gap, a duplicate, an
out-of-order pair -- plus a manifest that explains each one and is kept out of
the event stream so the adapter cannot read the answer key.
"""

from __future__ import annotations

import json

import pytest

from conftest import read_events, sequences_by_symbol
from market_sim.engine import run_simulation
from market_sim.faults import parse_fault_plan
from market_sim.recorder import EVENTS_NAME, FAULT_MANIFEST_NAME
from market_sim.verify import Verdict, verify_run

pytestmark = pytest.mark.asyncio


async def run_with(make_config, faults, *, count=2_000, **kwargs):
    plan = parse_fault_plan({"fault": list(faults)})
    cfg = make_config(
        mode="record",
        rate=1_000_000,
        count=count,
        faults_enabled=True,
        plan_path="plan.toml",
        fault_plan_fingerprint=plan.fingerprint(),
        **kwargs,
    )
    result = await run_simulation(cfg, fault_plan=plan)
    return cfg, result


def manifest_rows(run_dir):
    text = (run_dir / FAULT_MANIFEST_NAME).read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


class TestE2E02GapFixture:
    async def test_the_targeted_sequence_is_absent_and_its_neighbours_are_not(
        self, make_config
    ):
        _, result = await run_with(
            make_config, [{"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100}]
        )
        assert result.ok
        seqs = sequences_by_symbol(read_events(result.run_dir / EVENTS_NAME))["EURUSD"]
        assert 100 not in seqs
        assert 99 in seqs and 101 in seqs
        assert seqs == [s for s in range(1, max(seqs) + 1) if s != 100]

    async def test_exactly_one_omission_is_recorded(self, make_config):
        _, result = await run_with(
            make_config, [{"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100}]
        )
        counts = result.report["counts"]
        assert counts["intentional_omissions"] == 1
        assert counts["emitted_events"] == counts["logical_slots"] - 1
        assert counts["reconciles"] is True

    async def test_the_manifest_explains_it_and_the_verifier_accepts_it(self, make_config):
        _, result = await run_with(
            make_config, [{"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100}]
        )
        rows = manifest_rows(result.run_dir)
        assert len(rows) == 1
        assert rows[0]["applied"] is True
        assert rows[0]["affected_sequences"] == [100]
        assert rows[0]["target"] == {"symbol": "EURUSD", "sequence": 100}
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert verdict.totals["gaps"] == 1

    async def test_the_answer_key_is_not_in_the_event_stream(self, make_config):
        # Section 7.1: the adapter has to detect the gap itself.
        _, result = await run_with(
            make_config, [{"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100}]
        )
        body = (result.run_dir / EVENTS_NAME).read_bytes()
        assert b"fault" not in body
        assert b"missing_sequence" not in body
        assert b"intentional" not in body

    async def test_a_faulted_dataset_has_a_different_id_from_a_clean_one(self, make_config):
        faulted, _ = await run_with(
            make_config, [{"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100}]
        )
        clean = make_config(mode="record", rate=1_000_000, count=2_000)
        assert faulted.dataset_id != clean.dataset_id


class TestE2E03DuplicateAndReorder:
    async def test_duplicate_appears_twice_with_identical_bytes(self, make_config):
        _, result = await run_with(
            make_config, [{"kind": "duplicate_exact", "symbol": "GBPUSD", "sequence": 200}]
        )
        lines = (result.run_dir / EVENTS_NAME).read_bytes().splitlines()
        matching = [
            line for line in lines if b'"sequence":200' in line and b"GBPUSD" in line
        ]
        assert len(matching) == 2
        assert matching[0] == matching[1]

    async def test_reorder_puts_two_events_of_one_symbol_out_of_order(self, make_config):
        _, result = await run_with(
            make_config, [{"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 401}]
        )
        seqs = sequences_by_symbol(read_events(result.run_dir / EVENTS_NAME))["EURUSD"]
        index = seqs.index(401)
        assert seqs[index - 1] == 402
        assert sorted(seqs) == list(range(1, len(seqs) + 1))

    async def test_both_together_with_the_counts_from_the_design_doc(self, make_config):
        # E2E-09: 1001 slots with one skip and one duplicate -- the file still
        # holds 1001 lines, but with a gap and a duplicate inside it.
        _, result = await run_with(
            make_config,
            [
                {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100},
                {"kind": "duplicate_exact", "symbol": "GBPUSD", "sequence": 200},
                {"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 401},
            ],
            count=1001,
        )
        counts = result.report["counts"]
        assert counts["logical_slots"] == 1001
        assert counts["intentional_omissions"] == 1
        assert counts["intentional_duplicates"] == 1
        assert counts["emitted_events"] == 1001
        assert len(read_events(result.run_dir / EVENTS_NAME)) == 1001

    async def test_verifier_matches_every_anomaly_to_a_fault(self, make_config):
        # Case C04 end to end.
        _, result = await run_with(
            make_config,
            [
                {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100},
                {"kind": "duplicate_exact", "symbol": "GBPUSD", "sequence": 200},
                {"kind": "reorder_adjacent", "symbol": "USDJPY", "sequence": 50},
            ],
        )
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert verdict.totals["gaps"] == 1
        assert verdict.totals["duplicates"] == 1
        assert verdict.totals["out_of_order"] == 1
        assert verdict.fault_manifest["applied"] == 3

    async def test_conflicting_duplicate_is_visible_as_two_different_payloads(
        self, make_config
    ):
        # Case S03: a consumer must be able to tell this apart from a harmless
        # duplicate, which is why it is a separate kind with its own counter.
        _, result = await run_with(
            make_config,
            [
                {
                    "kind": "duplicate_conflict",
                    "symbol": "EURUSD",
                    "sequence": 300,
                    "bid_delta_ticks": 11,
                }
            ],
        )
        lines = [
            line
            for line in (result.run_dir / EVENTS_NAME).read_bytes().splitlines()
            if b'"sequence":300' in line and b"EURUSD" in line
        ]
        assert len(lines) == 2 and lines[0] != lines[1]
        verdict = verify_run(result.run_dir)
        assert verdict.totals["conflicting_duplicates"] == 1
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons


class TestMutationFaults:
    async def test_stale_timestamp_moves_one_event_into_the_past(self, make_config):
        _, result = await run_with(
            make_config,
            [
                {
                    "kind": "stale_timestamp",
                    "symbol": "USDJPY",
                    "sequence": 300,
                    "offset_ms": 2000,
                }
            ],
        )
        by_seq = {
            e["sequence"]: e
            for e in read_events(result.run_dir / EVENTS_NAME)
            if e["symbol"] == "USDJPY"
        }
        assert by_seq[300]["event_time_ns"] < by_seq[299]["event_time_ns"]
        assert by_seq[301]["event_time_ns"] - by_seq[300]["event_time_ns"] > 2_000_000_000

    async def test_invalid_spread_crosses_a_quote_that_normal_mode_forbids(
        self, make_config
    ):
        _, result = await run_with(
            make_config, [{"kind": "invalid_spread", "symbol": "EURUSD", "sequence": 7}]
        )
        rows = manifest_rows(result.run_dir)
        if not rows[0]["applied"]:
            # That slot produced a tick, so there was no spread to cross. The
            # manifest says exactly that, which is the honest outcome.
            assert "produced a tick" in rows[0]["reason_if_not_applied"]
            return
        crossed = [
            e
            for e in read_events(result.run_dir / EVENTS_NAME)
            if e["event_type"] == "quote" and e["ask_ticks"] < e["bid_ticks"]
        ]
        assert [e["sequence"] for e in crossed] == [7]

    async def test_invalid_price_produces_a_non_positive_price(self, make_config):
        _, result = await run_with(
            make_config,
            [
                {
                    "kind": "invalid_price",
                    "symbol": "GBPUSD",
                    "sequence": 9,
                    "price_ticks": 0,
                }
            ],
        )
        bad = [
            e
            for e in read_events(result.run_dir / EVENTS_NAME)
            if e["symbol"] == "GBPUSD" and e["sequence"] == 9
        ]
        assert len(bad) == 1
        assert 0 in [v for k, v in bad[0].items() if k.endswith("_ticks")]

    async def test_mutations_do_not_change_the_line_count(self, make_config):
        _, result = await run_with(
            make_config,
            [
                {
                    "kind": "stale_timestamp",
                    "symbol": "EURUSD",
                    "sequence": 10,
                    "offset_ms": 5,
                },
                {
                    "kind": "timestamp_future",
                    "symbol": "EURUSD",
                    "sequence": 20,
                    "offset_ms": 5,
                },
                {"kind": "invalid_price", "symbol": "EURUSD", "sequence": 30},
            ],
        )
        counts = result.report["counts"]
        assert counts["emitted_events"] == counts["logical_slots"]
        assert counts["intentional_mutations"] >= 2


class TestManifestHonestyEndToEnd:
    async def test_a_fault_aimed_at_a_nonexistent_slot_is_not_claimed_as_applied(
        self, make_config
    ):
        # Case S11: the test that catches a system flattering itself.
        _, result = await run_with(
            make_config,
            [{"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 999_999}],
            count=500,
        )
        assert result.ok
        rows = manifest_rows(result.run_dir)
        assert rows[0]["planned"] is True
        assert rows[0]["applied"] is False
        assert "never occurred" in rows[0]["reason_if_not_applied"]
        assert result.report["counts"]["intentional_omissions"] == 0
        assert result.report["faults"]["applied"] == 0
        assert result.report["faults"]["not_applied"] == 1
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons
        assert verdict.totals["gaps"] == 0

    async def test_every_manifest_row_carries_the_full_audit_shape(self, make_config):
        _, result = await run_with(
            make_config,
            [
                {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10},
                {"kind": "duplicate_exact", "symbol": "GBPUSD", "sequence": 20},
            ],
        )
        for row in manifest_rows(result.run_dir):
            assert set(row) == {
                "fault_id",
                "kind",
                "target",
                "planned",
                "applied",
                "affected_sequences",
                "output_impact",
                "reason_if_not_applied",
                "channel",
            }

    async def test_the_fingerprint_is_recorded_in_the_manifest_and_report(
        self, make_config
    ):
        cfg, result = await run_with(
            make_config, [{"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10}]
        )
        manifest = json.loads((result.run_dir / "manifest.json").read_text())
        assert manifest["fault_plan_fingerprint"] == cfg.fault_plan_fingerprint
        assert manifest["faults_enabled"] is True
        assert result.report["faults"]["plan_fingerprint"] == cfg.fault_plan_fingerprint

    async def test_faulted_runs_are_deterministic_too(self, make_config):
        # Case S06 with faults on: the plan is addressed by (symbol, sequence),
        # never by wall-clock time, so repeating the run repeats the bytes.
        faults = [
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100},
            {"kind": "duplicate_exact", "symbol": "GBPUSD", "sequence": 200},
            {"kind": "reorder_adjacent", "symbol": "USDJPY", "sequence": 50},
        ]
        _, first = await run_with(make_config, faults)
        _, second = await run_with(make_config, faults)
        assert (first.run_dir / EVENTS_NAME).read_bytes() == (
            second.run_dir / EVENTS_NAME
        ).read_bytes()
