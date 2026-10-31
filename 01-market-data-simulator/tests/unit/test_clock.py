"""Clock and scheduler (section 4, cases T01-T14).

The scheduler is driven by an injected fake monotonic source, so these are real
assertions about pacing rather than timing-sensitive guesses.
"""

from __future__ import annotations

import pytest

from market_sim.clock import LogicalClock, RateScheduler, RateWindow


class FakeClock:
    """A monotonic source the test advances by hand."""

    def __init__(self, start: int = 1_000_000_000) -> None:
        self.now = start

    def __call__(self) -> int:
        return self.now

    def advance_ms(self, ms: float) -> None:
        self.now += int(ms * 1_000_000)


class TestLogicalClock:
    def test_slot_zero_is_t0(self):
        clock = LogicalClock(1793322000000000000, 1000)
        assert clock.event_time_ns(0) == 1793322000000000000

    def test_spacing_is_exact_integer_arithmetic(self):
        clock = LogicalClock(0, 1000)  # 1000/s -> 1 ms apart
        assert [clock.event_time_ns(i) for i in range(4)] == [
            0,
            1_000_000,
            2_000_000,
            3_000_000,
        ]

    def test_no_float_drift_over_a_long_run(self):
        # The whole reason for integer arithmetic: at 3 events/s, slot 3,000,000
        # must land exactly on the second, not 0.9999999 of it.
        clock = LogicalClock(0, 3)
        assert clock.event_time_ns(3_000_000) == 1_000_000_000_000_000

    def test_monotonically_non_decreasing(self):
        clock = LogicalClock(10**18, 7919)
        times = [clock.event_time_ns(i) for i in range(5000)]
        assert all(b >= a for a, b in zip(times, times[1:]))

    def test_very_high_rate_allows_equal_timestamps(self):
        # Case T11: above 1e9/s the quantum falls below 1 ns, so slots share a
        # timestamp and ordering has to come from sequence instead.
        clock = LogicalClock(0, 2_000_000_000)
        assert clock.event_time_ns(0) == clock.event_time_ns(1) == 0
        assert clock.event_time_ns(2) == 1

    @pytest.mark.parametrize("rate", [0, -1])
    def test_rejects_non_positive_rate(self, rate):
        with pytest.raises(ValueError):
            LogicalClock(0, rate)

    def test_rejects_negative_t0_and_slot(self):
        with pytest.raises(ValueError):
            LogicalClock(-1, 1000)
        with pytest.raises(ValueError):
            LogicalClock(0, 1000).event_time_ns(-1)

    def test_is_independent_of_wall_clock(self):
        # Cases T01/T09: two clocks built identically give identical timestamps
        # no matter what the system clock or timezone did in between.
        a = LogicalClock(1793322000000000000, 1000)
        b = LogicalClock(1793322000000000000, 1000)
        assert [a.event_time_ns(i) for i in range(100)] == [
            b.event_time_ns(i) for i in range(100)
        ]

    def test_slots_in_duration(self):
        assert LogicalClock(0, 1000).slots_in(2.5) == 2500


class TestRateScheduler:
    def _scheduler(self, rate=1000, batch_ms=10, burst=5000):
        fake = FakeClock()
        sched = RateScheduler(
            rate, batch_ms=batch_ms, max_burst_slots=burst, monotonic=fake
        )
        sched.start()
        return sched, fake

    def test_nothing_is_due_at_time_zero(self):
        sched, _ = self._scheduler()
        plan = sched.plan()
        assert plan.slots == 0 and plan.lag_ns == 0 and plan.sleep_s > 0

    def test_slots_accrue_with_elapsed_time(self):
        sched, fake = self._scheduler(rate=1000)
        fake.advance_ms(10)
        assert sched.plan().slots == 10

    def test_no_double_counting_after_production(self):
        sched, fake = self._scheduler(rate=1000)
        fake.advance_ms(10)
        sched.note_produced(sched.plan().slots)
        assert sched.plan().slots == 0

    def test_burst_is_capped_and_reported(self):
        # Case T05: a 500 ms stall must not release an unbounded burst.
        sched, fake = self._scheduler(rate=100_000, burst=1000)
        fake.advance_ms(500)
        plan = sched.plan()
        assert plan.slots == 1000
        assert plan.capped_by_burst is True

    def test_lag_is_reported_in_logical_time(self):
        sched, fake = self._scheduler(rate=1000, burst=10)
        fake.advance_ms(500)  # 500 slots due, only 10 may be produced
        plan = sched.plan()
        assert plan.lag_ns == 500 * 1_000_000
        assert plan.slots == 10

    def test_lag_does_not_shrink_just_because_the_burst_was_capped(self):
        # The honest number is the backlog, not what we chose to emit (case T04).
        sched, fake = self._scheduler(rate=1000, burst=1)
        fake.advance_ms(100)
        first = sched.plan()
        sched.note_produced(first.slots)
        assert sched.plan().lag_ns > 0

    def test_remaining_caps_the_last_batch(self):
        sched, fake = self._scheduler(rate=1000)
        fake.advance_ms(100)  # 100 due
        assert sched.plan(remaining=7).slots == 7

    def test_remaining_zero_yields_a_sleep_not_a_spin(self):
        sched, fake = self._scheduler(rate=1000)
        fake.advance_ms(100)
        plan = sched.plan(remaining=0)
        assert plan.slots == 0 and plan.sleep_s > 0

    def test_catch_up_batches_do_not_sleep(self):
        sched, fake = self._scheduler(rate=1000)
        fake.advance_ms(50)
        assert sched.plan().sleep_s == 0.0

    def test_low_rate_sleeps_at_most_one_batch_window(self):
        # Case T10: at 1 event/second with a 10 ms batch the loop still wakes
        # every window, so a stop request is noticed promptly.
        sched, _ = self._scheduler(rate=1, batch_ms=10)
        assert sched.plan().sleep_s == pytest.approx(0.010)

    def test_effective_rate_measures_real_production(self):
        sched, fake = self._scheduler(rate=1000)
        fake.advance_ms(1000)
        sched.note_produced(400)  # the machine only managed 400 of 1000
        assert sched.effective_rate() == pytest.approx(400.0, rel=1e-6)

    def test_elapsed_never_goes_backwards(self):
        sched, fake = self._scheduler()
        fake.advance_ms(100)
        fake.now -= 10**9  # a hostile source stepping backwards
        assert sched.elapsed_ns() == 0

    def test_plan_before_start_is_an_error(self):
        sched = RateScheduler(1000, batch_ms=10, max_burst_slots=100)
        with pytest.raises(RuntimeError):
            sched.plan()

    @pytest.mark.parametrize(
        "override",
        [
            {"rate_per_sec": 0},
            {"rate_per_sec": -1},
            {"batch_ms": 0},
            {"max_burst_slots": 0},
        ],
    )
    def test_rejects_invalid_parameters(self, override):
        args = {"rate_per_sec": 1000, "batch_ms": 10, "max_burst_slots": 100}
        args.update(override)
        rate = args.pop("rate_per_sec")
        with pytest.raises(ValueError):
            RateScheduler(rate, **args)

    def test_note_produced_rejects_negative(self):
        sched, _ = self._scheduler()
        with pytest.raises(ValueError):
            sched.note_produced(-1)


class TestRateWindow:
    def test_empty_window_is_zero(self):
        assert RateWindow(1.0, monotonic=FakeClock()).rate() == 0.0

    def test_counts_within_the_window(self):
        fake = FakeClock()
        window = RateWindow(1.0, monotonic=fake)
        for _ in range(10):
            window.add(100)
            fake.advance_ms(100)
        assert window.rate() == pytest.approx(1000.0, rel=0.2)

    def test_evicts_samples_older_than_the_window(self):
        fake = FakeClock()
        window = RateWindow(1.0, monotonic=fake)
        window.add(1000)
        fake.advance_ms(5000)
        assert window.rate() == 0.0

    def test_ignores_non_positive_counts(self):
        window = RateWindow(1.0, monotonic=FakeClock())
        window.add(0)
        window.add(-5)
        assert window.rate() == 0.0

    def test_rejects_non_positive_window(self):
        with pytest.raises(ValueError):
            RateWindow(0.0)
