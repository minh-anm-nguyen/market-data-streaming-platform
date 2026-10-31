"""Open-loop schedules (sections 3.1, 4.3; ADR-01; case E2E-05).

The property that matters is in :class:`TestOpenLoopIndependence`: due times are
a function of the manifest alone. Everything else here checks that the shapes
section 3.1 names stay distinguishable in the output -- a burst must never be
readable as sustainable capacity, and a ramp must not flatten into its mean.
"""

from __future__ import annotations

import pytest

from p08_bench.errors import ConfigError
from p08_bench.scheduler import (
    NS_PER_S,
    RATE_LADDER_TICKS_PER_S,
    CompositeSchedule,
    PhasePlan,
    RateSchedule,
    SegmentShape,
    SlotKind,
    burst,
    constant,
    rate_ladder,
    ramp,
)


class TestSegments:
    def test_a_constant_segment_counts_rate_times_duration(self):
        assert constant(1000, 2.0).expected_slots == 2000

    def test_a_ramp_counts_the_area_under_the_curve(self):
        # 0 to 1000 over 2 s averages 500/s, so 1000 slots.
        assert ramp(0, 1000, 2.0).expected_slots == 1000

    def test_a_ramp_needs_an_end_rate(self):
        with pytest.raises(ConfigError, match="needs an end rate"):
            RateSchedule([constant(1, 1)]) and ramp(0, None, 1)  # type: ignore[arg-type]

    def test_a_constant_segment_may_not_carry_an_end_rate(self):
        from p08_bench.scheduler import Segment

        with pytest.raises(ConfigError, match="only a ramp"):
            Segment(SegmentShape.CONSTANT, 1.0, 100, end_rate_per_s=200)

    def test_a_zero_duration_segment_is_refused(self):
        with pytest.raises(ConfigError, match="duration must be positive"):
            constant(100, 0)

    def test_a_negative_rate_is_refused(self):
        with pytest.raises(ConfigError, match="may not be negative"):
            constant(-1, 1.0)

    def test_only_a_constant_segment_is_a_capacity_candidate(self):
        # Section 3.1: a burst is never a sustainable capacity.
        assert constant(100, 1).as_dict()["sustainable_capacity_candidate"] is True
        assert burst(400, 10).as_dict()["sustainable_capacity_candidate"] is False
        assert ramp(0, 100, 1).as_dict()["sustainable_capacity_candidate"] is False


class TestRateSchedule:
    def test_constant_slots_are_evenly_spaced(self):
        slots = list(RateSchedule.constant_rate(1000, 0.01).slots())
        assert len(slots) == 10
        gaps = {slots[i + 1].due_ns - slots[i].due_ns for i in range(len(slots) - 1)}
        assert gaps == {1_000_000}

    def test_the_first_slot_is_due_at_the_start(self):
        slots = list(RateSchedule.constant_rate(100, 1.0, start_ns=5_000).slots())
        assert slots[0].due_ns == 5_000

    def test_sequence_numbers_are_contiguous(self):
        slots = list(RateSchedule.constant_rate(100, 0.1).slots())
        assert [s.seq for s in slots] == list(range(len(slots)))

    def test_a_zero_rate_segment_contributes_no_slots_but_advances_time(self):
        schedule = RateSchedule([constant(0, 1.0, "gap"), constant(100, 1.0, "load")])
        slots = list(schedule.slots())
        assert len(slots) == 100
        # The load segment starts after the one-second gap.
        assert slots[0].due_ns == NS_PER_S

    def test_segments_run_back_to_back(self):
        schedule = RateSchedule([constant(100, 1.0), constant(100, 1.0)])
        slots = list(schedule.slots())
        assert len(slots) == 200
        assert slots[100].due_ns == NS_PER_S

    def test_a_ramp_accelerates_rather_than_spacing_evenly(self):
        # Under a rising rate the gaps must shrink; even spacing would make a
        # ramp indistinguishable from a constant segment at its mean.
        slots = list(RateSchedule([ramp(100, 1000, 1.0)]).slots())
        first_gap = slots[1].due_ns - slots[0].due_ns
        last_gap = slots[-1].due_ns - slots[-2].due_ns
        assert last_gap < first_gap / 2

    def test_ramp_slots_stay_inside_the_segment(self):
        slots = list(RateSchedule([ramp(100, 1000, 1.0)]).slots())
        assert all(0 <= s.due_ns <= NS_PER_S for s in slots)

    def test_an_empty_schedule_is_refused(self):
        with pytest.raises(ConfigError, match="at least one segment"):
            RateSchedule([])

    def test_peak_rate_covers_a_ramp_end(self):
        assert RateSchedule([ramp(100, 5000, 1.0)]).peak_rate_per_s == 5000

    def test_a_burst_is_visible_on_the_schedule(self):
        assert RateSchedule([burst(400, 10)]).has_burst
        assert not RateSchedule([constant(400, 10)]).has_burst

    def test_as_dict_reports_each_segment(self):
        payload = RateSchedule([constant(100, 1), burst(400, 2)]).as_dict()
        assert len(payload["segments"]) == 2
        assert payload["has_burst"] is True


class TestOpenLoopIndependence:
    def test_due_times_are_a_pure_function_of_the_manifest(self):
        # ADR-01: the schedule cannot be influenced by anything downstream, so
        # two independent generations must be identical.
        first = [s.due_ns for s in RateSchedule.constant_rate(1000, 0.05).slots()]
        second = [s.due_ns for s in RateSchedule.constant_rate(1000, 0.05).slots()]
        assert first == second

    def test_slots_are_generated_lazily(self):
        # A 120k/s half-hour schedule is 216 million slots and must never be
        # materialised; taking the first few must be cheap.
        schedule = RateSchedule.constant_rate(120_000, 1800)
        iterator = schedule.slots()
        assert [next(iterator).seq for _ in range(3)] == [0, 1, 2]

    def test_expected_slots_is_computable_without_generating_them(self):
        assert RateSchedule.constant_rate(120_000, 1800).expected_slots == 216_000_000


class TestCompositeSchedule:
    def test_tick_and_quote_rates_stay_exact_and_separate(self):
        # Case E2E-05: 100k ticks/s plus 25k quotes/s is not 125k ticks/s.
        composite = CompositeSchedule(
            [
                RateSchedule.constant_rate(100_000, 0.001, kind=SlotKind.TICK),
                RateSchedule.constant_rate(25_000, 0.001, kind=SlotKind.QUOTE),
            ]
        )
        assert composite.expected_slots(SlotKind.TICK) == 100
        assert composite.expected_slots(SlotKind.QUOTE) == 25
        assert composite.expected_slots() == 125
        assert composite.target_rate_per_s(SlotKind.TICK) == 100_000

    def test_merged_slots_are_in_due_order(self):
        composite = CompositeSchedule(
            [
                RateSchedule.constant_rate(100, 0.1, kind=SlotKind.TICK),
                RateSchedule.constant_rate(50, 0.1, kind=SlotKind.QUOTE),
            ]
        )
        dues = [s.due_ns for s in composite.slots()]
        assert dues == sorted(dues)

    def test_merged_slots_are_renumbered_across_kinds(self):
        composite = CompositeSchedule(
            [
                RateSchedule.constant_rate(100, 0.1, kind=SlotKind.TICK),
                RateSchedule.constant_rate(50, 0.1, kind=SlotKind.QUOTE),
            ]
        )
        seqs = [s.seq for s in composite.slots()]
        assert seqs == list(range(15))

    def test_both_kinds_survive_the_merge(self):
        composite = CompositeSchedule(
            [
                RateSchedule.constant_rate(100, 0.1, kind=SlotKind.TICK),
                RateSchedule.constant_rate(50, 0.1, kind=SlotKind.QUOTE),
            ]
        )
        kinds = {s.kind for s in composite.slots()}
        assert kinds == {SlotKind.TICK, SlotKind.QUOTE}

    def test_two_schedules_of_one_kind_are_refused(self):
        with pytest.raises(ConfigError, match="share a kind"):
            CompositeSchedule(
                [
                    RateSchedule.constant_rate(100, 1, kind=SlotKind.TICK),
                    RateSchedule.constant_rate(100, 1, kind=SlotKind.TICK),
                ]
            )

    def test_an_empty_composite_is_refused(self):
        with pytest.raises(ConfigError, match="at least one schedule"):
            CompositeSchedule([])

    def test_a_missing_kind_has_a_zero_target_not_an_error(self):
        composite = CompositeSchedule([RateSchedule.constant_rate(100, 1, kind=SlotKind.TICK)])
        assert composite.target_rate_per_s(SlotKind.QUOTE) == 0.0

    def test_as_dict_keeps_the_two_labels_apart(self):
        payload = CompositeSchedule(
            [
                RateSchedule.constant_rate(100_000, 0.001, kind=SlotKind.TICK),
                RateSchedule.constant_rate(25_000, 0.001, kind=SlotKind.QUOTE),
            ]
        ).as_dict()
        assert payload["tick_target_per_s"] == 100_000
        assert payload["quote_target_per_s"] == 25_000
        assert payload["expected_slots_by_kind"] == {"tick": 100, "quote": 25}


class TestPhasePlan:
    def test_the_steady_window_starts_after_warmup_and_ramp(self):
        # Case E2E-43: the window is data, so a one-minute steady phase cannot
        # be labelled as thirty.
        plan = PhasePlan(warmup_s=300, ramp_s=120, steady_s=1800, drain_s=120, tick_rate_per_s=1000)
        start, end = plan.steady_window_ns
        assert start == 420 * NS_PER_S
        assert end - start == 1800 * NS_PER_S

    def test_the_schedule_has_warmup_ramp_and_steady_segments(self):
        plan = PhasePlan(warmup_s=1, ramp_s=1, steady_s=1, drain_s=0, tick_rate_per_s=100)
        labels = [s.label for s in plan.schedule().schedules[0].segments]
        assert labels == ["warmup", "ramp", "steady"]

    def test_zero_length_phases_are_omitted(self):
        plan = PhasePlan(warmup_s=0, ramp_s=0, steady_s=1, drain_s=0, tick_rate_per_s=100)
        assert [s.label for s in plan.schedule().schedules[0].segments] == ["steady"]

    def test_quotes_get_their_own_schedule_only_when_requested(self):
        without = PhasePlan(0, 0, 1, 0, tick_rate_per_s=100)
        with_quotes = PhasePlan(0, 0, 1, 0, tick_rate_per_s=100, quote_rate_per_s=25)
        assert len(without.schedule().schedules) == 1
        assert len(with_quotes.schedule().schedules) == 2

    def test_a_zero_steady_phase_is_refused(self):
        with pytest.raises(ConfigError, match="steady_s must be positive"):
            PhasePlan(0, 0, 0, 0, tick_rate_per_s=100)

    def test_a_negative_phase_is_refused(self):
        with pytest.raises(ConfigError, match="may not be negative"):
            PhasePlan(-1, 0, 1, 0, tick_rate_per_s=100)


class TestRateLadder:
    def test_the_ladder_matches_nfr01(self):
        assert RATE_LADDER_TICKS_PER_S == (1_000, 10_000, 25_000, 50_000, 75_000, 100_000, 120_000)

    def test_each_level_is_its_own_labelled_segment(self):
        ladder = rate_ladder(seconds_per_level=1.0)
        assert len(ladder.segments) == 7
        assert ladder.segments[-1].label == "level-120000"

    def test_levels_must_ascend(self):
        with pytest.raises(ConfigError, match="must ascend"):
            rate_ladder([1000, 100], seconds_per_level=1.0)

    def test_an_empty_ladder_is_refused(self):
        with pytest.raises(ConfigError, match="at least one level"):
            rate_ladder([], seconds_per_level=1.0)

    def test_the_ladder_peak_is_the_top_level(self):
        assert rate_ladder(seconds_per_level=1.0).peak_rate_per_s == 120_000
