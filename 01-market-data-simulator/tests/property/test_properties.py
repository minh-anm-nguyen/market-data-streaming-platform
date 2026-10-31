"""Property-based tests (section 9.2).

The example-based tests check the configurations we thought of. These check the
ones we did not: tens of thousands of randomly drawn valid price models, symbol
sets and fault plans, asserting the invariants that must hold for all of them.
"""

from __future__ import annotations

import json

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import config_dict
from market_sim.config import build_config
from market_sim.faults import FaultInjector, parse_fault_plan
from market_sim.generator import Generator, clamp_into, reflect_into
from market_sim.model import (
    EVENT_QUOTE,
    EVENT_TICK,
    Event,
    parse_canonical_line,
    reference_canonical_json,
    to_canonical_json,
)

PROFILE = settings(
    max_examples=150,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)

SYMBOL_NAMES = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "US500.CFD", "AUD_NZD"]


@st.composite
def symbol_models(draw, name="EURUSD"):
    """A randomly drawn but always *valid* per-symbol price model."""
    scale = draw(st.sampled_from([1, 10, 100, 1000, 100000, 1000000]))
    low = draw(st.integers(min_value=1, max_value=10**9))
    width = draw(st.integers(min_value=1, max_value=10**6))
    high = low + width
    initial = draw(st.integers(min_value=low, max_value=high))
    max_spread = draw(st.integers(min_value=0, max_value=min(width, 1000)))
    min_spread = draw(st.integers(min_value=0, max_value=max_spread))
    max_step = draw(st.integers(min_value=0, max_value=10**5))
    weight = draw(st.integers(min_value=1, max_value=16))
    return {
        "price_scale": scale,
        "initial_price_ticks": initial,
        "min_price_ticks": low,
        "max_price_ticks": high,
        "min_spread_ticks": min_spread,
        "max_spread_ticks": max_spread,
        "max_step_ticks": max_step,
        "weight": weight,
    }


class TestBoundFolding:
    @given(
        value=st.integers(min_value=-(10**12), max_value=10**12),
        low=st.integers(min_value=0, max_value=10**9),
        width=st.integers(min_value=0, max_value=10**6),
    )
    @PROFILE
    def test_reflect_always_lands_inside_the_band(self, value, low, width):
        high = low + width
        assert low <= reflect_into(value, low, high) <= high

    @given(
        value=st.integers(min_value=-(10**12), max_value=10**12),
        low=st.integers(min_value=0, max_value=10**9),
        width=st.integers(min_value=0, max_value=10**6),
    )
    @PROFILE
    def test_clamp_always_lands_inside_the_band(self, value, low, width):
        high = low + width
        assert low <= clamp_into(value, low, high) <= high

    @given(
        low=st.integers(min_value=0, max_value=10**6),
        width=st.integers(min_value=1, max_value=10**6),
        offset=st.integers(min_value=0, max_value=10**6),
    )
    @PROFILE
    def test_both_are_identities_inside_the_band(self, low, width, offset):
        # The value is derived to sit inside the band rather than filtered into
        # it, so Hypothesis is not left discarding most of what it generates.
        high = low + width
        value = low + offset % (width + 1)
        assert reflect_into(value, low, high) == value
        assert clamp_into(value, low, high) == value


class TestSerialization:
    @given(
        bid=st.integers(min_value=-(2**62), max_value=2**62),
        spread=st.integers(min_value=-(10**6), max_value=10**6),
        sequence=st.integers(min_value=0, max_value=2**53),
        event_time_ns=st.integers(min_value=0, max_value=2**62),
        scale=st.integers(min_value=1, max_value=10**12),
        symbol=st.sampled_from(SYMBOL_NAMES),
    )
    @PROFILE
    def test_quote_matches_the_json_oracle_for_any_integers(
        self, bid, spread, sequence, event_time_ns, scale, symbol
    ):
        # Faults produce negative, zero and crossed values, so the fast
        # serialiser has to agree with json.dumps across the whole integer range.
        event = Event(
            dataset_id="ds-0123456789abcdef",
            event_type=EVENT_QUOTE,
            symbol=symbol,
            sequence=sequence,
            event_time_ns=event_time_ns,
            price_scale=scale,
            bid_ticks=bid,
            ask_ticks=bid + spread,
        )
        assert to_canonical_json(event) == reference_canonical_json(event)

    @given(
        price=st.integers(min_value=-(2**62), max_value=2**62),
        sequence=st.integers(min_value=0, max_value=2**53),
        symbol=st.sampled_from(SYMBOL_NAMES),
    )
    @PROFILE
    def test_tick_matches_the_json_oracle_for_any_integers(
        self, price, sequence, symbol
    ):
        event = Event(
            dataset_id="ds-0123456789abcdef",
            event_type=EVENT_TICK,
            symbol=symbol,
            sequence=sequence,
            event_time_ns=1,
            price_scale=1000,
            price_ticks=price,
        )
        assert to_canonical_json(event) == reference_canonical_json(event)

    @given(
        bid=st.integers(min_value=1, max_value=10**9),
        spread=st.integers(min_value=0, max_value=10**4),
        sequence=st.integers(min_value=1, max_value=2**53),
    )
    @PROFILE
    def test_round_trip_preserves_every_integer_exactly(self, bid, spread, sequence):
        # No float ever touches these values, so nothing is rounded.
        event = Event(
            dataset_id="ds-0123456789abcdef",
            event_type=EVENT_QUOTE,
            symbol="EURUSD",
            sequence=sequence,
            event_time_ns=1793322000000000000 + sequence,
            price_scale=100000,
            bid_ticks=bid,
            ask_ticks=bid + spread,
        )
        assert parse_canonical_line(to_canonical_json(event)) == event


class TestGeneratorInvariants:
    """The headline property: no valid configuration can produce a bad event."""

    @given(model=symbol_models(), quote_pct=st.integers(0, 100), seed=st.integers(0, 2**32))
    @PROFILE
    def test_single_symbol_never_violates_its_invariants(
        self, tmp_path, model, quote_pct, seed
    ):
        # The Generator asserts on every event it produces, so completing the
        # loop *is* the assertion -- across a randomly drawn price model.
        cfg = build_config(
            config_dict(
                mode="record",
                count=300,
                rate=100_000,
                seed=seed,
                quote_ratio_pct=quote_pct,
                order=["EURUSD"],
                symbols={"EURUSD": model},
            ),
            base_dir=tmp_path,
        )
        events = list(Generator(cfg).iter_slots(300))
        assert len(events) == 300
        sym = cfg.symbol("EURUSD")
        for event in events:
            if event.event_type == EVENT_QUOTE:
                assert event.bid_ticks > 0
                assert event.ask_ticks >= event.bid_ticks
                assert sym.min_spread_ticks <= event.spread_ticks <= sym.max_spread_ticks
                assert event.bid_ticks >= sym.min_price_ticks
                assert event.ask_ticks <= sym.max_price_ticks
            else:
                assert sym.min_price_ticks <= event.price_ticks <= sym.max_price_ticks

    @given(
        names=st.lists(st.sampled_from(SYMBOL_NAMES), min_size=1, max_size=4, unique=True),
        seed=st.integers(0, 2**32),
    )
    @PROFILE
    def test_sequences_are_exact_per_symbol_for_any_symbol_set(
        self, tmp_path, names, seed
    ):
        import hashlib

        symbols = {}
        for index, name in enumerate(names):
            # Derive a deterministic but varied model per name.
            # Keep the nudge inside the band, or the validator (correctly)
            # rejects the config the test itself built.
            nudge = int(hashlib.sha256(name.encode()).hexdigest()[:4], 16) % 20_000
            symbols[name] = {
                "price_scale": 100000,
                "initial_price_ticks": 100_000 + nudge,
                "min_price_ticks": 90000,
                "max_price_ticks": 120000,
                "min_spread_ticks": 1,
                "max_spread_ticks": 5,
                "max_step_ticks": 8,
                "weight": index + 1,
            }
        cfg = build_config(
            config_dict(
                mode="record",
                count=400,
                rate=100_000,
                seed=seed,
                order=names,
                symbols=symbols,
            ),
            base_dir=tmp_path,
        )
        seen: dict[str, list[int]] = {}
        for event in Generator(cfg).iter_slots(400):
            seen.setdefault(event.symbol, []).append(event.sequence)
        assert set(seen) <= set(names)
        for name, seqs in seen.items():
            assert seqs == list(range(1, len(seqs) + 1)), name
        assert sum(len(v) for v in seen.values()) == 400

    @given(
        weights=st.lists(st.integers(min_value=1, max_value=8), min_size=1, max_size=3)
    )
    @PROFILE
    def test_weights_are_honoured_exactly_over_whole_cycles(self, tmp_path, weights):
        from market_sim.generator import WeightedRoundRobin

        names = tuple(SYMBOL_NAMES[: len(weights)])
        total = sum(weights)
        wrr = WeightedRoundRobin(names, tuple(weights))
        counts = dict.fromkeys(names, 0)
        cycles = 7
        for _ in range(total * cycles):
            counts[wrr.next_symbol()] += 1
        for name, weight in zip(names, weights):
            assert counts[name] == weight * cycles

    @given(seed=st.integers(0, 2**32), batch=st.sampled_from([1, 7, 100, 5000]))
    @PROFILE
    def test_determinism_survives_operational_changes(self, tmp_path, seed, batch):
        # Section 9.2: the recorded bytes must not depend on batching or queue
        # sizes, or the golden-hash comparison would only work on one machine.
        def run(**extra):
            cfg = build_config(
                config_dict(mode="record", count=200, rate=50_000, seed=seed, **extra),
                base_dir=tmp_path,
            )
            return b"".join(to_canonical_json(e) for e in Generator(cfg).iter_slots(200))

        assert run() == run(scheduler_batch_ms=batch % 100 + 1, max_burst_slots=batch)


class TestFaultAlgebra:
    """emitted == slots - omissions + duplicates, for any plan we can draw."""

    @given(
        kinds=st.lists(
            st.sampled_from(
                [
                    "missing_sequence",
                    "duplicate_exact",
                    "duplicate_conflict",
                    "reorder_adjacent",
                    "stale_timestamp",
                    "timestamp_future",
                    "invalid_price",
                    "invalid_spread",
                ]
            ),
            min_size=0,
            max_size=6,
        ),
        seed=st.integers(0, 2**32),
    )
    @PROFILE
    def test_the_count_algebra_holds_for_any_plan(self, tmp_path, kinds, seed):
        slots = 600
        # One fault per sequence so the plan can never be self-conflicting.
        faults = []
        for index, kind in enumerate(kinds):
            entry = {"kind": kind, "symbol": "EURUSD", "sequence": 10 + index * 7}
            if kind in ("stale_timestamp", "timestamp_future"):
                entry["offset_ms"] = 100
            faults.append(entry)
        plan = parse_fault_plan({"fault": faults})

        cfg = build_config(
            config_dict(
                mode="record",
                count=slots,
                rate=100_000,
                seed=seed,
                faults_enabled=True,
                plan_path="plan.toml",
            ),
            base_dir=tmp_path,
            fault_plan_fingerprint=plan.fingerprint(),
        )
        injector = FaultInjector(plan)
        generator = Generator(cfg)
        emitted = 0
        for slot in range(slots):
            emitted += len(injector.process(generator.generate(slot), slot))
        emitted += len(injector.drain())
        injector.finalize_unreached()

        assert emitted == slots - injector.omissions + injector.duplicates
        assert injector.held_count == 0
        counts = injector.counts()
        assert counts["planned"] == len(faults)
        assert counts["applied"] + counts["not_applied"] == len(faults)

    @given(
        slots=st.integers(min_value=0, max_value=5),
        target=st.integers(min_value=1, max_value=6),
    )
    @PROFILE
    def test_edge_case_stream_lengths(self, tmp_path, slots, target):
        # Section 9.2 asks explicitly for the first/last-event and 0/1/2-event
        # edges, where an off-by-one in the reorder buffer would show up.
        plan = parse_fault_plan(
            {
                "fault": [
                    {"kind": "reorder_adjacent", "symbol": "EURUSD", "sequence": target}
                ]
            }
        )
        cfg = build_config(
            config_dict(
                mode="record",
                count=max(slots, 1),
                rate=100_000,
                faults_enabled=True,
                plan_path="plan.toml",
            ),
            base_dir=tmp_path,
            fault_plan_fingerprint=plan.fingerprint(),
        )
        injector = FaultInjector(plan)
        generator = Generator(cfg)
        emitted = 0
        for slot in range(slots):
            emitted += len(injector.process(generator.generate(slot), slot))
        emitted += len(injector.drain())
        injector.finalize_unreached()
        assert emitted == slots
        assert injector.held_count == 0

    @given(
        offset_ms=st.integers(min_value=1, max_value=10**6),
        base_ns=st.integers(min_value=0, max_value=2**40),
    )
    @PROFILE
    def test_stale_and_future_are_exact_and_never_negative(self, offset_ms, base_ns):
        event = Event(
            dataset_id="ds-0",
            event_type=EVENT_TICK,
            symbol="EURUSD",
            sequence=1,
            event_time_ns=base_ns,
            price_scale=100000,
            price_ticks=1,
        )
        stale_plan = parse_fault_plan(
            {
                "fault": [
                    {
                        "kind": "stale_timestamp",
                        "symbol": "EURUSD",
                        "sequence": 1,
                        "offset_ms": offset_ms,
                    }
                ]
            }
        )
        stale = FaultInjector(stale_plan).process(event, 0)[0].event
        assert stale.event_time_ns == max(0, base_ns - offset_ms * 1_000_000)
        assert stale.event_time_ns >= 0

        future_plan = parse_fault_plan(
            {
                "fault": [
                    {
                        "kind": "timestamp_future",
                        "symbol": "EURUSD",
                        "sequence": 1,
                        "offset_ms": offset_ms,
                    }
                ]
            }
        )
        future = FaultInjector(future_plan).process(event, 0)[0].event
        assert future.event_time_ns == base_ns + offset_ms * 1_000_000


class TestManifestIsAlwaysSerialisable:
    @given(
        kinds=st.lists(
            st.sampled_from(sorted(["missing_sequence", "duplicate_exact", "throttle_client"])),
            min_size=1,
            max_size=4,
        )
    )
    @PROFILE
    def test_every_manifest_row_is_valid_json_with_the_full_shape(self, kinds):
        faults = [
            {"kind": kind, "symbol": "EURUSD", "sequence": 1 + index}
            for index, kind in enumerate(kinds)
        ]
        injector = FaultInjector(parse_fault_plan({"fault": faults}))
        injector.finalize_unreached()
        for record in injector.records:
            row = json.loads(record.to_json_line())
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
            # An unapplied fault must always carry a reason. Silence here is how
            # a system ends up implying something it never did.
            if not row["applied"]:
                assert row["reason_if_not_applied"]
