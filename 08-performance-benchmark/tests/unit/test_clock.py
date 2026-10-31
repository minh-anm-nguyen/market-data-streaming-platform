"""Clock domains and sync accounting (section 4.3, INV-15, case E2E-42).

The headline test is :meth:`TestClockSync.test_300ms_skew_cannot_support_a_280ms_claim`:
E2E-42 requires a 300 ms skew between the source and ClickHouse to make a
one-way cross-host latency ``INCONCLUSIVE`` while leaving monotonic round-trip
measurements perfectly valid.
"""

from __future__ import annotations

import pytest

from p08_bench.clock import (
    ClockDomain,
    ClockSyncAssessment,
    ClockSyncQuality,
    FakeClock,
    MonotonicClock,
    Stopwatch,
    Timestamp,
    WallClockCorrelation,
)
from p08_bench.errors import ClockError


class TestClockDomain:
    def test_name_joins_host_and_process(self):
        assert ClockDomain("h1", "driver").name == "h1/driver"

    def test_two_processes_on_one_host_are_different_domains(self):
        # Section 4.3 scopes adapter_receive_to_emit to "the same process".
        assert ClockDomain("h1", "driver") != ClockDomain("h1", "consumer")

    def test_domains_are_hashable_so_they_can_key_an_assessment(self):
        assert len({ClockDomain("h1"), ClockDomain("h1")}) == 1


class TestTimestampSubtraction:
    def test_same_domain_gives_nanoseconds(self, domain):
        assert Timestamp(1_000, domain) - Timestamp(400, domain) == 600

    def test_cross_domain_subtraction_is_refused(self, domain, other_domain):
        with pytest.raises(ClockError, match="different clock domains"):
            Timestamp(1_000, domain) - Timestamp(400, other_domain)

    def test_a_backwards_interval_is_an_error_not_a_negative_latency(self, domain):
        with pytest.raises(ClockError, match="went backwards"):
            Timestamp(400, domain) - Timestamp(1_000, domain)

    def test_subtracting_a_non_timestamp_is_a_type_error(self, domain):
        with pytest.raises(TypeError):
            Timestamp(1, domain) - 1  # type: ignore[operator]

    def test_as_dict_names_the_domain(self, domain):
        assert Timestamp(5, domain).as_dict() == {"ns": 5, "clock_domain": "bench-host/driver"}


class TestMonotonicClock:
    def test_readings_are_tagged_with_the_domain(self, domain):
        clock = MonotonicClock(domain, source=iter([10, 20]).__next__)
        assert clock.now().domain == domain

    def test_a_source_that_goes_backwards_raises(self, domain):
        clock = MonotonicClock(domain, source=iter([100, 50]).__next__)
        clock.now()
        with pytest.raises(ClockError, match="lower value"):
            clock.now()

    def test_equal_readings_are_allowed(self, domain):
        # A coarse timer repeating a value is legal; only a decrease is not.
        clock = MonotonicClock(domain, source=iter([100, 100]).__next__)
        clock.now()
        assert clock.now().ns == 100

    def test_now_ns_is_a_plain_int(self, domain):
        clock = MonotonicClock(domain, source=lambda: 42)
        assert clock.now_ns() == 42


class TestFakeClock:
    def test_advance_moves_forward(self, fake_clock):
        before = fake_clock.now_ns()
        fake_clock.advance_s(1.5)
        assert fake_clock.now_ns() - before == 1_500_000_000

    def test_advance_refuses_a_negative_delta(self, fake_clock):
        with pytest.raises(ClockError, match="set_ns"):
            fake_clock.advance_ns(-1)

    def test_set_ns_can_move_backwards_so_guards_are_testable(self, fake_clock):
        fake_clock.set_ns(0)
        assert fake_clock.now_ns() == 0


class TestStopwatch:
    def test_elapsed_after_stop(self, fake_clock):
        watch = Stopwatch(fake_clock).start()
        fake_clock.advance_s(0.25)
        assert watch.stop() == 250_000_000
        assert watch.elapsed_ns == 250_000_000

    def test_stop_before_start_is_refused(self, fake_clock):
        with pytest.raises(ClockError, match="before it was started"):
            Stopwatch(fake_clock).stop()

    def test_elapsed_without_a_completed_interval_is_refused(self, fake_clock):
        watch = Stopwatch(fake_clock).start()
        with pytest.raises(ClockError, match="no completed interval"):
            _ = watch.elapsed_ns

    def test_restart_clears_the_previous_interval(self, fake_clock):
        watch = Stopwatch(fake_clock).start()
        fake_clock.advance_s(1)
        watch.stop()
        watch.start()
        with pytest.raises(ClockError):
            _ = watch.elapsed_ns


class TestClockSync:
    def test_unmeasured_sync_is_unknown_not_adequate(self, domain, other_domain):
        # Section 15: a missing measurement may not default to the passing value.
        assessment = ClockSyncAssessment.unmeasured(domain, other_domain)
        assert assessment.quality_for(280_000_000) is ClockSyncQuality.UNKNOWN
        assert not assessment.supports_one_way_latency(280_000_000)

    def test_300ms_skew_cannot_support_a_280ms_claim(self, domain, other_domain):
        # Case E2E-42, with the skew as uncertainty: 300 ms of unknown offset
        # cannot measure a 280 ms interval.
        assessment = ClockSyncAssessment(
            domain, other_domain,
            offset_ns=300_000_000, jitter_ns=5_000_000,
            uncertainty_ns=300_000_000, method="ntp",
        )
        assert assessment.quality_for(280_000_000) is ClockSyncQuality.INSUFFICIENT
        assert not assessment.supports_one_way_latency(280_000_000)

    def test_small_uncertainty_supports_the_claim(self, domain, other_domain):
        assessment = ClockSyncAssessment(
            domain, other_domain,
            offset_ns=300_000_000, jitter_ns=100_000,
            uncertainty_ns=1_000_000, method="ptp",
        )
        assert assessment.supports_one_way_latency(280_000_000)

    def test_the_fraction_is_configurable_and_recorded(self, domain, other_domain):
        assessment = ClockSyncAssessment(
            domain, other_domain, offset_ns=0, jitter_ns=0,
            uncertainty_ns=28_000_000, method="ntp",
        )
        # Exactly 10% of 280 ms passes at the default guardrail and fails at 5%.
        assert assessment.supports_one_way_latency(280_000_000)
        assert not assessment.supports_one_way_latency(280_000_000, max_fraction=0.05)

    def test_a_known_stable_offset_corrects_a_cross_host_interval(self, domain, other_domain):
        assessment = ClockSyncAssessment(
            domain, other_domain, offset_ns=1_000, jitter_ns=1,
            uncertainty_ns=2, method="ptp",
        )
        start = Timestamp(10_000, domain)
        end = Timestamp(12_000, other_domain)
        # start is on the left domain, so its reading is shifted by the offset.
        assert assessment.cross_host_interval_ns(start, end) == 1_000

    def test_the_correction_is_symmetric(self, domain, other_domain):
        assessment = ClockSyncAssessment(
            domain, other_domain, offset_ns=1_000, jitter_ns=1,
            uncertainty_ns=2, method="ptp",
        )
        start = Timestamp(12_000, other_domain)
        end = Timestamp(14_000, domain)
        assert assessment.cross_host_interval_ns(start, end) == 3_000

    def test_an_unmeasured_offset_cannot_produce_an_interval(self, domain, other_domain):
        assessment = ClockSyncAssessment.unmeasured(domain, other_domain)
        with pytest.raises(ClockError, match="measured offset"):
            assessment.cross_host_interval_ns(
                Timestamp(1, domain), Timestamp(2, other_domain)
            )

    def test_same_domain_interval_needs_no_correction(self, domain, other_domain):
        assessment = ClockSyncAssessment(
            domain, other_domain, offset_ns=9_999, jitter_ns=1,
            uncertainty_ns=1, method="ptp",
        )
        assert assessment.cross_host_interval_ns(
            Timestamp(100, domain), Timestamp(300, domain)
        ) == 200

    def test_an_assessment_for_other_domains_is_refused(self, domain, other_domain):
        third = ClockDomain("kafka-host")
        assessment = ClockSyncAssessment(
            domain, other_domain, offset_ns=0, jitter_ns=0,
            uncertainty_ns=0, method="ptp",
        )
        with pytest.raises(ClockError, match="does not cover"):
            assessment.cross_host_interval_ns(
                Timestamp(1, domain), Timestamp(2, third)
            )

    def test_a_non_positive_target_is_refused(self, domain, other_domain):
        assessment = ClockSyncAssessment(
            domain, other_domain, offset_ns=0, jitter_ns=0,
            uncertainty_ns=0, method="ptp",
        )
        with pytest.raises(ClockError, match="positive"):
            assessment.quality_for(0)

    def test_as_dict_records_all_three_numbers(self, domain, other_domain):
        payload = ClockSyncAssessment.unmeasured(domain, other_domain).as_dict()
        for key in ("offset_ns", "jitter_ns", "uncertainty_ns", "method"):
            assert key in payload


class TestWallClock:
    def test_correlation_is_marked_unusable_for_durations(self, fake_clock):
        # An NTP step would otherwise show up as a negative or inflated latency.
        correlation = WallClockCorrelation.capture(fake_clock, utc_iso="2026-10-30T10:20:00Z")
        payload = correlation.as_dict()
        assert payload["usable_for_durations"] is False
        assert payload["utc_iso"] == "2026-10-30T10:20:00Z"

    def test_it_carries_a_monotonic_anchor_for_joining(self, fake_clock):
        correlation = WallClockCorrelation.capture(fake_clock)
        assert correlation.monotonic.ns == fake_clock.now_ns()

    def test_there_is_no_duration_helper(self):
        assert not any(
            name for name in dir(WallClockCorrelation)
            if "duration" in name or "elapsed" in name
        )
