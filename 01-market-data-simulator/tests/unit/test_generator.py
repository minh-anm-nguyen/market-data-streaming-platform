"""Generator determinism and normal-mode invariants (cases D01-D12, S05-S08).

The generator is the only component allowed to decide what a correct event looks
like, so these tests are the backbone of the whole project: if a gap or a crossed
quote can appear here, no downstream gap-detection test means anything.
"""

from __future__ import annotations

import collections

import pytest

from conftest import symbol_dict
from market_sim.errors import InvariantViolation
from market_sim.generator import (
    Generator,
    WeightedRoundRobin,
    check_normal_invariants,
    clamp_into,
    reflect_into,
)
from market_sim.model import EVENT_QUOTE, EVENT_TICK, to_canonical_json


class TestWeightedRoundRobin:
    def test_single_symbol(self):
        wrr = WeightedRoundRobin(("EURUSD",), (1,))
        assert [wrr.next_symbol() for _ in range(3)] == ["EURUSD"] * 3

    def test_equal_weights_cycle_in_order(self):
        wrr = WeightedRoundRobin(("A1A", "B1B", "C1C"), (1, 1, 1))
        assert [wrr.next_symbol() for _ in range(6)] == [
            "A1A",
            "B1B",
            "C1C",
            "A1A",
            "B1B",
            "C1C",
        ]

    def test_share_is_exact_over_each_cycle_not_just_on_average(self):
        wrr = WeightedRoundRobin(("EURUSD", "GBPUSD", "USDJPY"), (3, 2, 1))
        counts = collections.Counter(wrr.next_symbol() for _ in range(6))
        assert counts == {"EURUSD": 3, "GBPUSD": 2, "USDJPY": 1}

    def test_exact_over_many_cycles(self):
        wrr = WeightedRoundRobin(("EURUSD", "GBPUSD", "USDJPY"), (3, 2, 1))
        counts = collections.Counter(wrr.next_symbol() for _ in range(60_000))
        assert counts == {"EURUSD": 30_000, "GBPUSD": 20_000, "USDJPY": 10_000}

    def test_consumes_no_randomness(self):
        # Two independent selectors agree forever, which is why changing weights
        # has an effect that is easy to reason about and to re-derive in C++.
        a = WeightedRoundRobin(("A1A", "B1B"), (5, 3))
        b = WeightedRoundRobin(("A1A", "B1B"), (5, 3))
        assert [a.next_symbol() for _ in range(100)] == [
            b.next_symbol() for _ in range(100)
        ]

    def test_order_breaks_ties(self):
        # Order is the tie-breaker, so with equal weights it fully determines the
        # cycle.
        a = WeightedRoundRobin(("A1A", "B1B"), (1, 1))
        b = WeightedRoundRobin(("B1B", "A1A"), (1, 1))
        assert [a.next_symbol() for _ in range(4)] == ["A1A", "B1B", "A1A", "B1B"]
        assert [b.next_symbol() for _ in range(4)] == ["B1B", "A1A", "B1B", "A1A"]

    def test_distinct_weights_make_order_irrelevant_to_the_sequence(self):
        # Worth pinning because it explains why dataset_id is deliberately
        # conservative: reordering [symbols].order always changes the id even
        # though, with distinct weights, the emitted interleaving can be
        # identical. Guessing whether the data "really" changed would be the
        # fragile option.
        a = WeightedRoundRobin(("A1A", "B1B"), (1, 2))
        b = WeightedRoundRobin(("B1B", "A1A"), (2, 1))
        assert [a.next_symbol() for _ in range(6)] == [b.next_symbol() for _ in range(6)]

    @pytest.mark.parametrize(
        "names,weights",
        [((), ()), (("A1A",), (0,)), (("A1A",), (-1,)), (("A1A", "B1B"), (1,))],
    )
    def test_rejects_invalid_input(self, names, weights):
        with pytest.raises(ValueError):
            WeightedRoundRobin(names, weights)


class TestBoundFolding:
    def test_clamp_saturates(self):
        assert clamp_into(5, 10, 20) == 10
        assert clamp_into(25, 10, 20) == 20
        assert clamp_into(15, 10, 20) == 15

    def test_reflect_turns_around_at_the_edge(self):
        assert reflect_into(21, 10, 20) == 19
        assert reflect_into(9, 10, 20) == 11
        assert reflect_into(15, 10, 20) == 15

    @pytest.mark.parametrize("value", range(-60, 120, 7))
    def test_reflect_always_lands_inside_the_band(self, value):
        assert 10 <= reflect_into(value, 10, 20) <= 20

    @pytest.mark.parametrize("value", [-10**9, 0, 10**9])
    def test_reflect_handles_far_out_values(self, value):
        # Case D03: an overflowing step must not escape the band.
        assert 10 <= reflect_into(value, 10, 20) <= 20

    def test_degenerate_band(self):
        assert reflect_into(99, 10, 10) == 10
        assert clamp_into(99, 10, 10) == 10


class TestNormalInvariants:
    def test_one_million_events_hold_every_invariant(self, make_config):
        # Case D01/D10: the Generator asserts on every event, so reaching the end
        # of this loop *is* the assertion. One million events across three
        # symbols with reflecting bounds exercises the band edges repeatedly.
        cfg = make_config(count=1_000_000, rate=1_000_000)
        gen = Generator(cfg)
        last = None
        for slot in range(1_000_000):
            last = gen.generate(slot)
        assert last is not None
        state = gen.state_snapshot()
        assert sum(s["emitted"] for s in state.values()) == 1_000_000

    def test_sequences_start_at_one_and_increase_by_one_per_symbol(self, make_config):
        cfg = make_config(count=600)
        gen = Generator(cfg)
        seqs: dict[str, list[int]] = {}
        for slot in range(600):
            event = gen.generate(slot)
            seqs.setdefault(event.symbol, []).append(event.sequence)
        for symbol, values in seqs.items():
            assert values == list(range(1, len(values) + 1)), symbol

    def test_sequence_numbers_repeat_across_symbols(self, make_config):
        # Case S05: sequence is scoped per symbol, so a global dedup key is wrong.
        cfg = make_config(count=60)
        gen = Generator(cfg)
        events = [gen.generate(i) for i in range(60)]
        ones = [e.symbol for e in events if e.sequence == 1]
        assert sorted(ones) == ["EURUSD", "GBPUSD", "USDJPY"]

    def test_event_time_never_decreases_per_symbol(self, make_config):
        cfg = make_config(count=5000, rate=10_000)
        gen = Generator(cfg)
        last: dict[str, int] = {}
        for slot in range(5000):
            event = gen.generate(slot)
            assert event.event_time_ns >= last.get(event.symbol, 0)
            last[event.symbol] = event.event_time_ns

    def test_price_scale_is_per_symbol(self, make_config):
        # Case D11: USDJPY uses a different scale in this simulation.
        cfg = make_config(count=300)
        gen = Generator(cfg)
        scales = {e.symbol: e.price_scale for e in (gen.generate(i) for i in range(300))}
        assert scales == {"EURUSD": 100000, "GBPUSD": 100000, "USDJPY": 1000}

    @pytest.mark.parametrize("pct,expected", [(0, {EVENT_TICK}), (100, {EVENT_QUOTE})])
    def test_quote_ratio_extremes(self, make_config, pct, expected):
        # Case D09: 0% and 100% are valid and produce exactly one event type.
        cfg = make_config(count=400, quote_ratio_pct=pct)
        gen = Generator(cfg)
        assert {gen.generate(i).event_type for i in range(400)} == expected

    def test_quote_ratio_is_approximately_honoured(self, make_config):
        cfg = make_config(count=20_000, quote_ratio_pct=80)
        gen = Generator(cfg)
        types = collections.Counter(gen.generate(i).event_type for i in range(20_000))
        assert 0.78 < types[EVENT_QUOTE] / 20_000 < 0.82

    def test_tick_carries_no_trade_fields(self, make_config):
        # Case D12: a tick is a synthetic price update, not a trade.
        cfg = make_config(count=200, quote_ratio_pct=0)
        gen = Generator(cfg)
        raw = to_canonical_json(gen.generate(0))
        assert b"volume" not in raw and b"trade" not in raw

    def test_narrow_band_still_satisfies_the_quote_invariants(self, make_config):
        # The tightest legal configuration: the band is barely wider than the
        # spread, so the quote has almost no room and clamping must be right.
        cfg = make_config(
            count=20_000,
            order=["EURUSD"],
            symbols={
                "EURUSD": symbol_dict(
                    low=100000, high=100010, initial=100005, min_spread=10, max_spread=10
                )
            },
        )
        gen = Generator(cfg)
        for slot in range(20_000):
            event = gen.generate(slot)
            if event.event_type == EVENT_QUOTE:
                assert event.bid_ticks == 100000 and event.ask_ticks == 100010

    def test_zero_step_keeps_the_price_still(self, make_config):
        cfg = make_config(
            count=100,
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict(max_step=0)},
            quote_ratio_pct=0,
        )
        gen = Generator(cfg)
        prices = {gen.generate(i).price_ticks for i in range(100)}
        assert prices == {108347}

    def test_clamp_policy_also_holds_the_invariants(self, make_config):
        cfg = make_config(count=50_000, bound_policy="clamp")
        gen = Generator(cfg)
        for slot in range(50_000):
            gen.generate(slot)

    def test_out_of_order_slot_is_rejected(self, make_config):
        gen = Generator(make_config())
        gen.generate(0)
        with pytest.raises(InvariantViolation):
            gen.generate(5)

    def test_iter_slots_matches_generate(self, make_config):
        cfg = make_config(count=50)
        a = [to_canonical_json(e) for e in Generator(cfg).iter_slots(50)]
        gen = Generator(cfg)
        b = [to_canonical_json(gen.generate(i)) for i in range(50)]
        assert a == b


class TestDeterminism:
    def test_same_config_same_bytes(self, make_config):
        # Case S06 at the generator level; the end-to-end hash check lives in
        # tests/integration/test_record_e2e.py.
        cfg = make_config(count=5000)
        first = b"".join(to_canonical_json(e) for e in Generator(cfg).iter_slots(5000))
        second = b"".join(to_canonical_json(e) for e in Generator(cfg).iter_slots(5000))
        assert first == second

    def test_two_config_objects_with_equal_values_agree(self, make_config):
        a = b"".join(to_canonical_json(e) for e in Generator(make_config(count=500)).iter_slots(500))
        b = b"".join(to_canonical_json(e) for e in Generator(make_config(count=500)).iter_slots(500))
        assert a == b

    @pytest.mark.parametrize(
        "change", [{"seed": 20261031}, {"start_time_ns": 1}, {"quote_ratio_pct": 50}]
    )
    def test_data_affecting_changes_change_the_output(self, make_config, change):
        base = b"".join(
            to_canonical_json(e) for e in Generator(make_config(count=500)).iter_slots(500)
        )
        other = b"".join(
            to_canonical_json(e)
            for e in Generator(make_config(count=500, **change)).iter_slots(500)
        )
        assert base != other

    @pytest.mark.parametrize(
        "change",
        [
            {"scheduler_batch_ms": 1},
            {"scheduler_batch_ms": 500},
            {"max_burst_slots": 1},
            {"max_queue_events": 1},
            {"fsync_every_events": 1},
        ],
    )
    def test_operational_knobs_do_not_change_the_output(self, make_config, change):
        # Section 9.2: determinism must survive changes to batching and queueing,
        # otherwise the golden-hash comparison would only work on one machine
        # under one configuration.
        base = b"".join(
            to_canonical_json(e) for e in Generator(make_config(count=500)).iter_slots(500)
        )
        other = b"".join(
            to_canonical_json(e)
            for e in Generator(make_config(count=500, **change)).iter_slots(500)
        )
        assert base == other

    def test_prng_streams_are_independent_per_symbol(self, make_config):
        # Dropping a symbol must not shift the others' price paths, because each
        # stream is seeded from SHA-256(seed || symbol).
        three = make_config(count=3000)
        gen = Generator(three)
        eur_from_three = [
            e.sequence for e in (gen.generate(i) for i in range(3000)) if e.symbol == "EURUSD"
        ]
        only_eur = make_config(
            count=1000, order=["EURUSD"], symbols={"EURUSD": symbol_dict(weight=3)}
        )
        gen2 = Generator(only_eur)
        eur_alone = [e.sequence for e in gen2.iter_slots(1000)]
        assert eur_from_three[:100] == eur_alone[:100]

    def test_state_snapshot_reports_prng_consumption(self, make_config):
        gen = Generator(make_config(count=300))
        for slot in range(300):
            gen.generate(slot)
        snapshot = gen.state_snapshot()
        for symbol, state in snapshot.items():
            # Three draws per event, plus any rejection re-draws.
            assert state["prng_draws"] >= 3 * state["emitted"], symbol


class TestInvariantChecker:
    """The checker itself must actually catch things, or it proves nothing."""

    def _sym(self, **kw):
        from market_sim.config import SymbolConfig

        base = dict(
            symbol="EURUSD",
            price_scale=100000,
            initial_price_ticks=108347,
            min_price_ticks=100000,
            max_price_ticks=120000,
            min_spread_ticks=1,
            max_spread_ticks=5,
            max_step_ticks=8,
            weight=1,
        )
        base.update(kw)
        return SymbolConfig(**base)

    def _quote(self, **kw):
        from market_sim.model import Event

        base = dict(
            dataset_id="ds-1",
            event_type=EVENT_QUOTE,
            symbol="EURUSD",
            sequence=1,
            event_time_ns=1,
            price_scale=100000,
            bid_ticks=108346,
            ask_ticks=108348,
        )
        base.update(kw)
        return Event(**base)

    def test_accepts_a_good_quote(self):
        check_normal_invariants(self._quote(), self._sym())

    @pytest.mark.parametrize(
        "change,fragment",
        [
            ({"bid_ticks": 0}, "bid_ticks must be > 0"),
            ({"bid_ticks": -1}, "bid_ticks must be > 0"),
            ({"bid_ticks": 108349}, "ask_ticks must be >= bid_ticks"),
            ({"ask_ticks": 108400}, "spread outside configured range"),
            ({"bid_ticks": 99999, "ask_ticks": 100001}, "price band"),
            ({"sequence": 0}, "sequence must start at 1"),
            ({"event_time_ns": -1}, "event_time_ns must be >= 0"),
            ({"price_scale": 1000}, "price_scale mismatch"),
            ({"symbol": "GBPUSD"}, "symbol mismatch"),
            ({"price_ticks": 5}, "quote must not carry price_ticks"),
            ({"event_type": "trade"}, "unknown event_type"),
        ],
    )
    def test_rejects_each_broken_quote(self, change, fragment):
        with pytest.raises(InvariantViolation) as exc:
            check_normal_invariants(self._quote(**change), self._sym())
        assert fragment in str(exc.value)

    @pytest.mark.parametrize(
        "change,fragment",
        [
            ({"price_ticks": 0}, "price_ticks must be > 0"),
            ({"price_ticks": -5}, "price_ticks must be > 0"),
            ({"price_ticks": 130000}, "price outside configured band"),
            ({"bid_ticks": 1}, "tick must not carry bid/ask"),
        ],
    )
    def test_rejects_each_broken_tick(self, change, fragment):
        kwargs = dict(
            event_type=EVENT_TICK, price_ticks=108347, bid_ticks=None, ask_ticks=None
        )
        kwargs.update(change)
        with pytest.raises(InvariantViolation) as exc:
            check_normal_invariants(self._quote(**kwargs), self._sym())
        assert fragment in str(exc.value)

    def test_dataset_id_mismatch_is_caught(self):
        with pytest.raises(InvariantViolation) as exc:
            check_normal_invariants(self._quote(), self._sym(), dataset_id="ds-other")
        assert "dataset_id mismatch" in str(exc.value)

    def test_crossed_quote_from_a_fault_would_be_caught_in_normal_mode(self):
        # This is the pairing that makes the design claim real: the checker runs
        # *before* the injector, so a crossed quote in a clean dataset is a bug.
        with pytest.raises(InvariantViolation):
            check_normal_invariants(
                self._quote(bid_ticks=108348, ask_ticks=108346), self._sym()
            )
