"""Fault plan parsing and injection (section 7, cases S01-S13, C04).

The point of every test here is that an injected anomaly is *explained*: the
manifest says what was planned, what was applied, which sequences it touched and
why anything was skipped. An unexplained anomaly is a bug; an unapplied fault
reported as applied is worse.
"""

from __future__ import annotations

import json

import pytest

from market_sim.errors import FaultPlanError
from market_sim.faults import (
    CLIENT_KINDS,
    MAX_REORDER_HOLD,
    SEMANTIC_KINDS,
    TRANSPORT_KINDS,
    FaultInjector,
    FaultPlan,
    parse_fault_plan,
)
from market_sim.generator import Generator
from market_sim.model import EVENT_QUOTE, EVENT_TICK, Event


def plan_of(*faults):
    return parse_fault_plan({"fault": list(faults)})


def ev(seq=1, symbol="EURUSD", **kw):
    base = dict(
        dataset_id="ds-1",
        event_type=EVENT_QUOTE,
        symbol=symbol,
        sequence=seq,
        event_time_ns=1_000_000_000,
        price_scale=100000,
        bid_ticks=108346,
        ask_ticks=108348,
    )
    base.update(kw)
    return Event(**base)


class TestPlanParsing:
    def test_empty_plan(self):
        assert len(parse_fault_plan({})) == 0
        assert len(FaultPlan.empty()) == 0

    def test_parses_the_shipped_example_shape(self):
        plan = plan_of(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100},
            {"kind": "duplicate_exact", "symbol": "GBPUSD", "sequence": 200},
            {"kind": "stale_timestamp", "symbol": "USDJPY", "sequence": 300, "offset_ms": 2000},
            {"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 401},
        )
        assert len(plan) == 4
        assert [s.fault_id for s in plan] == ["f0001", "f0002", "f0003", "f0004"]

    def test_unknown_kind_rejected(self):
        with pytest.raises(FaultPlanError) as exc:
            plan_of({"kind": "corrupt_everything", "symbol": "EURUSD", "sequence": 1})
        assert exc.value.code == "FAULT_PLAN_UNKNOWN_KIND"

    def test_unknown_key_rejected(self):
        with pytest.raises(FaultPlanError) as exc:
            plan_of(
                {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 1, "probability": 0.5}
            )
        assert exc.value.code == "FAULT_PLAN_UNKNOWN_KEY"

    def test_unknown_top_level_key_rejected(self):
        with pytest.raises(FaultPlanError) as exc:
            parse_fault_plan({"faults": []})
        assert exc.value.code == "FAULT_PLAN_UNKNOWN_KEY"

    def test_both_target_forms_is_ambiguous(self):
        with pytest.raises(FaultPlanError) as exc:
            plan_of(
                {
                    "kind": "missing_sequence",
                    "symbol": "EURUSD",
                    "sequence": 1,
                    "global_slot_index": 5,
                }
            )
        assert exc.value.code == "FAULT_PLAN_TARGET_INVALID"

    def test_no_target_is_rejected(self):
        with pytest.raises(FaultPlanError) as exc:
            plan_of({"kind": "missing_sequence"})
        assert exc.value.code == "FAULT_PLAN_TARGET_INVALID"

    @pytest.mark.parametrize("sequence", [0, -1])
    def test_sequence_must_be_positive(self, sequence):
        with pytest.raises(FaultPlanError) as exc:
            plan_of({"kind": "missing_sequence", "symbol": "EURUSD", "sequence": sequence})
        assert exc.value.code == "FAULT_PLAN_TARGET_INVALID"

    @pytest.mark.parametrize("kind", ["stale_timestamp", "timestamp_future"])
    def test_timestamp_faults_need_an_offset(self, kind):
        with pytest.raises(FaultPlanError) as exc:
            plan_of({"kind": kind, "symbol": "EURUSD", "sequence": 1})
        assert exc.value.code == "FAULT_PLAN_MISSING_PARAM"

    def test_float_offset_rejected(self):
        with pytest.raises(FaultPlanError) as exc:
            plan_of(
                {"kind": "stale_timestamp", "symbol": "EURUSD", "sequence": 1, "offset_ms": 1.5}
            )
        assert exc.value.code == "FAULT_PLAN_TYPE_ERROR"

    def test_two_payload_faults_on_one_target_conflict(self):
        # Case S12: guessing an order would make the dataset unexplainable.
        with pytest.raises(FaultPlanError) as exc:
            plan_of(
                {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10},
                {"kind": "duplicate_exact", "symbol": "EURUSD", "sequence": 10},
            )
        assert exc.value.code == "FAULT_PLAN_CONFLICT"

    def test_one_payload_plus_one_transport_fault_is_allowed(self):
        # Different channels, so there is nothing ambiguous about it.
        plan = plan_of(
            {"kind": "duplicate_exact", "symbol": "EURUSD", "sequence": 10},
            {"kind": "close_connection", "symbol": "EURUSD", "sequence": 10},
        )
        assert len(plan) == 2

    def test_same_sequence_on_different_symbols_is_not_a_conflict(self):
        plan = plan_of(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10},
            {"kind": "missing_sequence", "symbol": "GBPUSD", "sequence": 10},
        )
        assert len(plan) == 2

    def test_fingerprint_is_stable_and_order_sensitive(self):
        a = plan_of({"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 1})
        b = plan_of({"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 1})
        c = plan_of({"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 2})
        assert a.fingerprint() == b.fingerprint()
        assert a.fingerprint() != c.fingerprint()

    def test_empty_plan_fingerprint_differs_from_a_populated_one(self):
        assert FaultPlan.empty().fingerprint() != plan_of(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 1}
        ).fingerprint()

    def test_kind_sets_do_not_overlap(self):
        assert not (SEMANTIC_KINDS & TRANSPORT_KINDS)
        assert not (SEMANTIC_KINDS & CLIENT_KINDS)
        assert not (TRANSPORT_KINDS & CLIENT_KINDS)


class TestSemanticInjection:
    def test_disabled_injector_is_a_pass_through(self):
        injector = FaultInjector(FaultPlan.empty())
        items = injector.process(ev(1), 0)
        assert len(items) == 1 and items[0].event == ev(1)
        assert injector.enabled is False

    def test_missing_sequence_withholds_the_event(self):
        # Case S01: the receiver should see 99 -> 101.
        plan = plan_of({"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100})
        injector = FaultInjector(plan)
        assert injector.process(ev(99), 0) != []
        assert injector.process(ev(100), 1) == []
        assert injector.process(ev(101), 2) != []
        assert injector.omissions == 1
        record = injector.records[0]
        assert record.applied is True
        assert record.affected_sequences == [100]
        assert "gap" in record.output_impact

    def test_duplicate_exact_emits_identical_bytes_twice(self):
        # Case S02: same logical event id, so a consumer can dedupe safely.
        plan = plan_of({"kind": "duplicate_exact", "symbol": "EURUSD", "sequence": 5})
        injector = FaultInjector(plan)
        items = injector.process(ev(5), 0)
        assert len(items) == 2
        assert items[0].event == items[1].event
        assert items[1].is_duplicate is True
        assert injector.duplicates == 1

    def test_duplicate_conflict_emits_different_payloads_under_one_sequence(self):
        # Case S03: tracked separately, because deduplicating by key here would
        # hide a genuine inconsistency rather than resolve it.
        plan = plan_of(
            {
                "kind": "duplicate_conflict",
                "symbol": "EURUSD",
                "sequence": 5,
                "bid_delta_ticks": 7,
            }
        )
        injector = FaultInjector(plan)
        items = injector.process(ev(5), 0)
        assert len(items) == 2
        assert items[0].event.sequence == items[1].event.sequence == 5
        assert items[1].event.bid_ticks == items[0].event.bid_ticks + 7
        assert "flag the conflict" in injector.records[0].output_impact

    def test_duplicate_conflict_on_a_tick_moves_price_ticks(self):
        plan = plan_of({"kind": "duplicate_conflict", "symbol": "EURUSD", "sequence": 5})
        injector = FaultInjector(plan)
        tick = ev(
            5, event_type=EVENT_TICK, bid_ticks=None, ask_ticks=None, price_ticks=108347
        )
        items = injector.process(tick, 0)
        assert items[1].event.price_ticks == 108348

    def test_reorder_swaps_two_adjacent_events_of_one_symbol(self):
        plan = plan_of({"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 10})
        injector = FaultInjector(plan)
        assert injector.process(ev(10), 0) == []
        assert injector.held_count == 1
        items = injector.process(ev(11), 1)
        assert [i.event.sequence for i in items] == [11, 10]
        assert items[1].is_reordered is True
        assert injector.records[0].affected_sequences == [10, 11]

    def test_reorder_only_pairs_within_one_symbol(self):
        plan = plan_of({"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 10})
        injector = FaultInjector(plan)
        injector.process(ev(10), 0)
        other = injector.process(ev(4, symbol="GBPUSD"), 1)
        assert [i.event.symbol for i in other] == ["GBPUSD"]
        assert injector.held_count == 1

    def test_reorder_with_no_partner_is_drained_and_reported_unapplied(self):
        plan = plan_of({"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 10})
        injector = FaultInjector(plan)
        injector.process(ev(10), 0)
        drained = injector.drain()
        assert [i.event.sequence for i in drained] == [10]
        assert injector.held_count == 0
        record = injector.records[0]
        assert record.applied is False
        assert "nothing to swap with" in record.reason_if_not_applied

    def test_reorder_hold_is_bounded(self):
        # An unbounded hold would be a config-driven memory leak (case S04).
        faults = [
            {"kind": "reorder_adjacent", "symbol": f"SYM{i:04d}", "sequence": 1}
            for i in range(MAX_REORDER_HOLD + 5)
        ]
        injector = FaultInjector(plan_of(*faults))
        emitted_despite_hold = 0
        for i in range(MAX_REORDER_HOLD + 5):
            if injector.process(ev(1, symbol=f"SYM{i:04d}"), i):
                emitted_despite_hold += 1
        assert injector.held_count == MAX_REORDER_HOLD
        assert emitted_despite_hold == 5
        refused = [
            r for r in injector.records if "buffer full" in (r.reason_if_not_applied or "")
        ]
        assert len(refused) == 5

    def test_stale_timestamp_moves_event_time_backwards(self):
        plan = plan_of(
            {
                "kind": "stale_timestamp",
                "symbol": "EURUSD",
                "sequence": 3,
                "offset_ms": 2000,
            }
        )
        injector = FaultInjector(plan)
        item = injector.process(ev(3, event_time_ns=5_000_000_000), 0)[0]
        assert item.event.event_time_ns == 3_000_000_000
        assert item.is_mutated is True
        assert "stale" in injector.records[0].output_impact

    def test_stale_timestamp_never_goes_negative(self):
        plan = plan_of(
            {
                "kind": "stale_timestamp",
                "symbol": "EURUSD",
                "sequence": 3,
                "offset_ms": 10**6,
            }
        )
        injector = FaultInjector(plan)
        item = injector.process(ev(3, event_time_ns=5), 0)[0]
        assert item.event.event_time_ns == 0

    def test_timestamp_future_moves_event_time_forwards(self):
        # Case T07: a receiver must be able to detect forward clock skew, and
        # the manifest records the exact offset it was given.
        plan = plan_of(
            {
                "kind": "timestamp_future",
                "symbol": "EURUSD",
                "sequence": 3,
                "offset_ms": 1500,
            }
        )
        injector = FaultInjector(plan)
        item = injector.process(ev(3), 0)[0]
        assert item.event.event_time_ns == 1_000_000_000 + 1_500_000_000
        assert "clock skew" in injector.records[0].output_impact

    def test_invalid_spread_crosses_the_quote(self):
        plan = plan_of({"kind": "invalid_spread", "symbol": "EURUSD", "sequence": 3})
        injector = FaultInjector(plan)
        item = injector.process(ev(3), 0)[0]
        assert item.event.ask_ticks < item.event.bid_ticks

    def test_invalid_spread_can_force_a_width(self):
        plan = plan_of(
            {
                "kind": "invalid_spread",
                "symbol": "EURUSD",
                "sequence": 3,
                "spread_ticks": 9999,
            }
        )
        injector = FaultInjector(plan)
        item = injector.process(ev(3), 0)[0]
        assert item.event.spread_ticks == 9999

    def test_invalid_spread_aimed_at_a_tick_is_reported_not_applied(self):
        # The honest outcome: the slot produced a tick, so there was no spread to
        # break. Reporting it as applied would be a false positive (case C04).
        plan = plan_of({"kind": "invalid_spread", "symbol": "EURUSD", "sequence": 3})
        injector = FaultInjector(plan)
        tick = ev(3, event_type=EVENT_TICK, bid_ticks=None, ask_ticks=None, price_ticks=1)
        items = injector.process(tick, 0)
        assert items[0].event == tick
        assert items[0].is_mutated is False
        record = injector.records[0]
        assert record.applied is False
        assert "produced a tick" in record.reason_if_not_applied

    def test_invalid_price_forces_zero_by_default(self):
        plan = plan_of({"kind": "invalid_price", "symbol": "EURUSD", "sequence": 3})
        injector = FaultInjector(plan)
        item = injector.process(ev(3), 0)[0]
        assert item.event.bid_ticks == 0 and item.event.ask_ticks == 0

    def test_invalid_price_can_force_a_value(self):
        plan = plan_of(
            {
                "kind": "invalid_price",
                "symbol": "EURUSD",
                "sequence": 3,
                "price_ticks": -42,
            }
        )
        injector = FaultInjector(plan)
        item = injector.process(ev(3), 0)[0]
        assert item.event.bid_ticks == -42

    def test_global_slot_index_targeting(self):
        plan = plan_of({"kind": "missing_sequence", "global_slot_index": 7})
        injector = FaultInjector(plan)
        assert injector.process(ev(1), 6) != []
        assert injector.process(ev(2), 7) == []

    def test_priority_decides_between_two_slot_targets(self):
        plan = plan_of(
            {
                "kind": "missing_sequence",
                "symbol": "EURUSD",
                "sequence": 5,
                "priority": 1,
            },
            {"kind": "duplicate_exact", "global_slot_index": 0, "priority": 9},
        )
        injector = FaultInjector(plan)
        assert len(injector.process(ev(5), 0)) == 2  # the duplicate won


class TestManifestHonesty:
    def test_unreached_target_is_planned_not_applied(self):
        # Case S11: a fault aimed at a slot that never existed must never be
        # counted as a success.
        plan = plan_of({"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 9999})
        injector = FaultInjector(plan)
        injector.process(ev(1), 0)
        injector.finalize_unreached()
        record = injector.records[0]
        assert record.planned is True
        assert record.applied is False
        assert "never occurred" in record.reason_if_not_applied
        assert injector.counts() == {"planned": 1, "applied": 0, "not_applied": 1}

    def test_client_side_fault_is_never_claimed_as_applied(self):
        # throttle_client is driven by the consuming test client; the simulator
        # cannot apply it and must not pretend otherwise.
        plan = plan_of({"kind": "throttle_client", "symbol": "EURUSD", "sequence": 1})
        injector = FaultInjector(plan)
        injector.process(ev(1), 0)
        injector.finalize_unreached()
        record = injector.records[0]
        assert record.applied is False
        assert record.channel == "client"
        assert "client_side_fault" in record.reason_if_not_applied

    def test_transport_faults_are_marked_as_the_transport_channel(self):
        plan = plan_of({"kind": "close_connection", "symbol": "EURUSD", "sequence": 1})
        injector = FaultInjector(plan)
        items = injector.process(ev(1), 0)
        assert items[0].transport_fault is not None
        assert injector.records[0].channel == "transport"
        # Not applied until the sink actually closes the connection.
        assert injector.records[0].applied is False

    def test_transport_applied_and_skipped_notes(self):
        plan = plan_of({"kind": "close_connection", "symbol": "EURUSD", "sequence": 1})
        injector = FaultInjector(plan)
        spec = plan.specs[0]
        injector.note_transport_skipped(spec, "no client connected")
        assert injector.records[0].reason_if_not_applied == "no client connected"
        injector.note_transport_applied(spec, "closed after the record")
        assert injector.records[0].applied is True
        assert injector.records[0].output_impact == "closed after the record"

    def test_manifest_row_has_every_required_audit_field(self):
        # Section 7.2 names the required columns.
        plan = plan_of({"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 1})
        injector = FaultInjector(plan)
        injector.process(ev(1), 0)
        row = json.loads(injector.records[0].to_json_line())
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
        assert row["target"] == {"symbol": "EURUSD", "sequence": 1}

    def test_manifest_line_is_newline_terminated_json(self):
        plan = plan_of({"kind": "missing_sequence", "global_slot_index": 3})
        injector = FaultInjector(plan)
        line = injector.records[0].to_json_line()
        assert line.endswith(b"\n")
        assert json.loads(line)["target"] == {"global_slot_index": 3}


class TestCountAlgebra:
    """emitted == slots - omissions + duplicates, over a real generator."""

    def _run(self, cfg, plan, slots):
        injector = FaultInjector(plan)
        gen = Generator(cfg)
        emitted = 0
        for slot in range(slots):
            emitted += len(injector.process(gen.generate(slot), slot))
        emitted += len(injector.drain())
        injector.finalize_unreached()
        return injector, emitted

    def test_clean_run_emits_exactly_the_slot_count(self, make_config):
        cfg = make_config(count=1000)
        injector, emitted = self._run(cfg, FaultPlan.empty(), 1000)
        assert emitted == 1000
        assert injector.omissions == 0 and injector.duplicates == 0

    def test_one_drop_and_one_duplicate_cancel_out(self, make_config):
        # Straight from section 3.3: 1000 slots, one dropped, one duplicated, so
        # the file still has 1000 lines -- but with a gap and a duplicate in it.
        cfg = make_config(count=1000)
        plan = plan_of(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 100},
            {"kind": "duplicate_exact", "symbol": "GBPUSD", "sequence": 200},
        )
        injector, emitted = self._run(cfg, plan, 1000)
        assert injector.omissions == 1 and injector.duplicates == 1
        assert emitted == 1000 - 1 + 1 == 1000

    def test_reorder_does_not_change_the_count(self, make_config):
        cfg = make_config(count=1000)
        plan = plan_of({"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 50})
        injector, emitted = self._run(cfg, plan, 1000)
        assert emitted == 1000
        assert injector.reorders == 1

    def test_mutation_does_not_change_the_count(self, make_config):
        cfg = make_config(count=1000)
        plan = plan_of(
            {"kind": "stale_timestamp", "symbol": "EURUSD", "sequence": 50, "offset_ms": 5}
        )
        _, emitted = self._run(cfg, plan, 1000)
        assert emitted == 1000

    @pytest.mark.parametrize("slots", [0, 1, 2, 3])
    def test_edge_case_stream_lengths(self, make_config, slots):
        # Section 9.2 asks for the first/last-event and 0/1/2-event edges.
        cfg = make_config(count=max(slots, 1))
        plan = plan_of(
            {"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": 1},
            {"kind": "missing_sequence", "symbol": "GBPUSD", "sequence": 1},
        )
        injector, emitted = self._run(cfg, plan, slots)
        assert emitted == slots - injector.omissions + injector.duplicates
        assert injector.held_count == 0

    def test_algebra_holds_with_every_fault_kind_at_once(self, make_config):
        cfg = make_config(count=2000)
        plan = plan_of(
            {"kind": "missing_sequence", "symbol": "EURUSD", "sequence": 10},
            {"kind": "duplicate_exact", "symbol": "EURUSD", "sequence": 20},
            {"kind": "duplicate_conflict", "symbol": "GBPUSD", "sequence": 30},
            {"kind": "reorder_adjacent", "symbol": "GBPUSD", "sequence": 40},
            {"kind": "stale_timestamp", "symbol": "USDJPY", "sequence": 50, "offset_ms": 10},
            {"kind": "timestamp_future", "symbol": "USDJPY", "sequence": 60, "offset_ms": 10},
            {"kind": "invalid_price", "symbol": "EURUSD", "sequence": 70},
        )
        injector, emitted = self._run(cfg, plan, 2000)
        assert emitted == 2000 - injector.omissions + injector.duplicates
        assert injector.omissions == 1
        assert injector.duplicates == 2
        counts = injector.counts()
        assert counts["planned"] == 7
        assert counts["applied"] + counts["not_applied"] == 7
