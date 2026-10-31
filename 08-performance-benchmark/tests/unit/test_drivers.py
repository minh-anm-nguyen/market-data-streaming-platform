"""Event driver, calibration and query driver (group G02; cases E2E-37, E2E-38, E2E-41).

Three documented failures are the spine of this file:

* **E2E-37** -- a driver configured for 120k that emits 70k. The capacity claim
  is INVALID and the actual issue series is kept.
* **E2E-38** -- a query driver returning empty results in 1 ms. Every latency
  gate passes; the oracle fails it anyway.
* **E2E-41** -- timeouts omitted from the input samples. The attempt registry
  keeps them in the denominator, so the omission is not possible.
"""

from __future__ import annotations

import pytest

from p08_bench.calibrator import Calibration, NullSink, calibrate
from p08_bench.clock import ClockDomain, FakeClock
from p08_bench.errors import ConfigError, MetrologyError, UpstreamUnavailable
from p08_bench.event_driver import EventDriver, default_payload_factory
from p08_bench.manifest import QueryCatalogEntry
from p08_bench.query_driver import (
    ExpectedResult,
    QueryBinding,
    QueryDriver,
    QueryFailure,
    QueryResponse,
    QueryResultOracle,
    uniform_bindings,
)
from p08_bench.scheduler import NS_PER_S, RateSchedule, SlotKind
from p08_bench.sut import Identity, Stage, SubmitOutcome, SubmitStatus
from p08_bench.sut_mock import MockConfig, MockPipeline
from p08_bench.units import Unit


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(ClockDomain("bench", "driver"))


def factory():
    return default_payload_factory("ds-test", ["EURUSD", "GBPUSD", "USDJPY"])


class _ShortSink(NullSink):
    """A sink that stops accepting after ``limit`` offers.

    Models case E2E-37 from the driver side: the schedule keeps asking and the
    driver has to record what it could not place rather than slowing down.
    """

    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit = limit
        self.seen = 0

    def submit(self, identity, payload, t_ns):  # type: ignore[override]
        self.seen += 1
        if self.seen > self.limit:
            return SubmitOutcome(SubmitStatus.DROPPED, identity, "sink refused")
        return super().submit(identity, payload, t_ns)


class TestEventDriverAccounting:
    def test_every_scheduled_slot_is_accounted(self, clock):
        schedule = RateSchedule.constant_rate(1000, 0.05)
        result = EventDriver(NullSink(), clock).drive(schedule, factory())
        assert result.counters.scheduled == 50
        assert result.counters.issued == 50
        assert result.counters.not_issued == 0

    def test_dropped_slots_are_counted_not_forgotten(self, clock):
        # Case E2E-37: the driver issued 30 of 50 and the record says so.
        schedule = RateSchedule.constant_rate(1000, 0.05)
        result = EventDriver(_ShortSink(30), clock).drive(schedule, factory())
        assert result.counters.scheduled == 50
        assert result.counters.issued == 30
        assert result.counters.not_issued == 20

    def test_an_under_issuing_driver_is_not_calibrated(self, clock):
        schedule = RateSchedule.constant_rate(1000, 0.05)
        result = EventDriver(_ShortSink(30), clock).drive(schedule, factory())
        assert not result.fidelity.calibrated
        assert result.fidelity.issue_fraction == pytest.approx(0.6)
        assert any("INVALID" in reason for reason in result.fidelity.reasons())

    def test_rejections_and_backpressure_are_counted_separately(self, clock):
        # A rejection is the pipeline applying policy; backpressure is a
        # capacity signal. Folding them would make a correct run look lossy.
        sut = MockPipeline(MockConfig(reject_fraction=1.0))
        result = EventDriver(sut, clock).drive(RateSchedule.constant_rate(1000, 0.01), factory())
        assert result.counters.rejected == 10
        assert result.counters.backpressured == 0
        assert result.counters.issued == 10

    def test_backpressure_is_recorded_when_a_queue_fills(self, clock):
        sut = MockPipeline(MockConfig(adapter_rate_per_s=1.0, adapter_queue_records=5))
        result = EventDriver(sut, clock).drive(RateSchedule.constant_rate(1000, 0.05), factory())
        assert result.counters.backpressured > 0
        assert result.counters.issued == result.counters.scheduled

    def test_per_kind_counts_are_kept_apart(self, clock):
        from p08_bench.scheduler import CompositeSchedule

        schedule = CompositeSchedule(
            [
                RateSchedule.constant_rate(1000, 0.01, kind=SlotKind.TICK),
                RateSchedule.constant_rate(250, 0.01, kind=SlotKind.QUOTE),
            ]
        )
        result = EventDriver(NullSink(), clock).drive(schedule, factory())
        assert result.counters.by_kind_issued == {"tick": 10, "quote": 2}

    def test_rates_are_reported_in_both_units(self, clock):
        from p08_bench.scheduler import CompositeSchedule

        schedule = CompositeSchedule(
            [
                RateSchedule.constant_rate(1000, 1.0, kind=SlotKind.TICK),
                RateSchedule.constant_rate(250, 1.0, kind=SlotKind.QUOTE),
            ]
        )
        result = EventDriver(NullSink(), clock).drive(schedule, factory())
        payload = result.as_dict()
        assert payload["issued_ticks_per_s"]["unit"] == "ticks/s"
        assert payload["issued_events_per_s"]["unit"] == "events/s"
        assert payload["issued_ticks_per_s"]["value"] < payload["issued_events_per_s"]["value"]

    def test_the_measured_denominator_is_the_schedule_span(self, clock):
        result = EventDriver(NullSink(), clock).drive(
            RateSchedule.constant_rate(1000, 1.0), factory()
        )
        # 1000 slots at 1 ms spacing spans 0.999 s between first and last due.
        assert result.measured_seconds == pytest.approx(0.999)

    def test_issued_rate_helper_uses_the_requested_unit(self, clock):
        result = EventDriver(NullSink(), clock).drive(
            RateSchedule.constant_rate(1000, 1.0), factory()
        )
        quantity = result.issued_rate(SlotKind.TICK, Unit.TICKS_PER_S)
        assert quantity.unit is Unit.TICKS_PER_S
        assert quantity.value == pytest.approx(1000, rel=0.01)

    def test_simulation_provenance_travels_with_the_result(self, clock):
        assert EventDriver(MockPipeline(), clock).drive(
            RateSchedule.constant_rate(100, 0.01), factory()
        ).sut_is_simulation


class TestEventDriverBounds:
    def test_the_driver_bounds_its_own_backlog(self, clock):
        # Case G02-09: a driver whose backlog grows without limit becomes a
        # second queue inside the measurement.
        sut = MockPipeline(MockConfig(adapter_rate_per_s=1.0, adapter_queue_records=1))
        driver = EventDriver(sut, clock, max_pending_slots=5)
        result = driver.drive(RateSchedule.constant_rate(1000, 0.05), factory())
        assert result.counters.scheduled == 50
        assert result.counters.issued + result.counters.not_issued == 50

    def test_invalid_driver_bounds_are_refused(self, clock):
        for kwargs in (
            {"max_pending_slots": 0},
            {"max_late_issue_ms": 0},
            {"tolerance_fraction": 1.0},
            {"sample_lag_every": 0},
        ):
            with pytest.raises(ConfigError):
                EventDriver(NullSink(), clock, **kwargs)

    def test_the_payload_factory_needs_symbols(self):
        with pytest.raises(ConfigError, match="at least one symbol"):
            default_payload_factory("ds", [])

    def test_payload_sequences_are_per_symbol_and_integer_priced(self, clock):
        from p08_bench.scheduler import Slot

        make = factory()
        first = make(Slot(0, 0, SlotKind.TICK))
        fourth = make(Slot(3, 0, SlotKind.TICK))
        assert first[0] == Identity("ds-test", "EURUSD", 1)
        # Slot 3 cycles back to EURUSD with the next sequence for that symbol.
        assert fourth[0] == Identity("ds-test", "EURUSD", 2)
        assert isinstance(first[1]["price_ticks"], int)


class TestCalibration:
    def test_a_clean_run_is_calibrated(self, clock):
        result = calibrate(RateSchedule.constant_rate(1000, 0.05), factory(), clock)
        assert result.verdict is Calibration.CALIBRATED
        assert result.supports_rate_claim

    def test_an_under_issuing_driver_is_generator_bound(self, clock):
        # Case G02-01 / E2E-37: the instrument is the limit, so no claim.
        result = calibrate(
            RateSchedule.constant_rate(1000, 0.05), factory(), clock, sink=_ShortSink(30)
        )
        assert result.verdict is Calibration.GENERATOR_BOUND
        assert not result.supports_rate_claim

    def test_the_achieved_rate_is_reported_in_both_units(self, clock):
        result = calibrate(RateSchedule.constant_rate(1000, 1.0), factory(), clock)
        assert result.achieved_ticks_per_s.unit is Unit.TICKS_PER_S
        assert result.achieved_events_per_s.unit is Unit.EVENTS_PER_S

    def test_a_delayed_null_sink_makes_the_baseline_suspect(self, clock):
        # Case G02-05: a slow null sink understates the driver.
        result = calibrate(
            RateSchedule.constant_rate(100, 0.1), factory(), clock,
            sink=NullSink(delay_ns=10_000_000),
        )
        assert result.sink_is_suspect
        assert not result.supports_rate_claim
        assert any("G02-05" in reason for reason in result.reasons)

    def test_the_target_is_recorded_alongside_the_achievement(self, clock):
        result = calibrate(RateSchedule.constant_rate(120_000, 0.001), factory(), clock)
        assert result.target_ticks_per_s == 120_000
        payload = result.as_dict()
        assert payload["target_ticks_per_s"] == 120_000
        assert payload["achieved_ticks_per_s"]["unit"] == "ticks/s"

    def test_the_issue_lag_distribution_is_published(self, clock):
        result = calibrate(RateSchedule.constant_rate(1000, 0.05), factory(), clock)
        summary = result.lag_summary()
        assert summary["n"] == 50
        assert "p95" in summary


class TestNullSinkStages:
    def test_a_null_sink_refuses_any_lane_beyond_calibration(self):
        # It is the absence of a pipeline, not a model of one.
        with pytest.raises(UpstreamUnavailable, match="cannot be exercised"):
            NullSink().require_stages([Stage.BROKER])

    def test_the_source_stage_is_available_for_the_calibration_lane(self):
        NullSink().require_stages([Stage.SOURCE])

    def test_the_stage_report_names_the_blocked_stages(self):
        report = NullSink().stage_report()
        assert Stage.BROKER.value in report["blocked_stages"]
        assert report["is_simulation"] is True


def _entry(timeout_ms: int = 2000) -> QueryCatalogEntry:
    return QueryCatalogEntry(
        qid="Q02", sql="SELECT symbol FROM canonical_ticks", binds={}, client_timeout_ms=timeout_ms
    )


ROWS = [{"symbol": "EURUSD", "price_ticks": 108347}]


class TestQueryOracle:
    def _oracle(self, **kwargs) -> QueryResultOracle:
        return QueryResultOracle({"b1": ExpectedResult.from_rows(ROWS, **kwargs)})

    def test_a_matching_response_is_correct(self):
        assert self._oracle().judge("b1", QueryResponse("q1", ROWS)).correct

    def test_an_empty_response_fails_however_fast_it_was(self):
        # Case E2E-38.
        verdict = self._oracle().judge("b1", QueryResponse("q1", []))
        assert not verdict.correct
        assert verdict.failure is QueryFailure.EMPTY_RESULT

    def test_an_expected_empty_response_is_allowed_when_declared(self):
        oracle = QueryResultOracle({"b1": ExpectedResult.from_rows([], allow_empty=True)})
        assert oracle.judge("b1", QueryResponse("q1", [])).correct

    def test_a_wrong_row_count_fails(self):
        verdict = self._oracle().judge("b1", QueryResponse("q1", ROWS * 2))
        assert verdict.failure is QueryFailure.WRONG_ROW_COUNT

    def test_the_right_number_of_wrong_rows_fails_on_the_digest(self):
        wrong = [{"symbol": "EURUSD", "price_ticks": 999}]
        verdict = self._oracle().judge("b1", QueryResponse("q1", wrong))
        assert verdict.failure is QueryFailure.WRONG_DIGEST

    def test_a_timeout_is_its_own_failure_kind(self):
        verdict = self._oracle().judge("b1", QueryResponse("q1", [], timed_out=True))
        assert verdict.failure is QueryFailure.TIMEOUT

    def test_an_error_is_its_own_failure_kind(self):
        verdict = self._oracle().judge("b1", QueryResponse("q1", [], error="connection reset"))
        assert verdict.failure is QueryFailure.ERROR

    def test_a_response_with_no_answer_key_cannot_be_judged(self):
        # INV-13: an unchecked result is an evidence gap, never a pass.
        with pytest.raises(MetrologyError, match="no expected result"):
            self._oracle().judge("unknown", QueryResponse("q1", ROWS))

    def test_an_empty_oracle_is_refused(self):
        with pytest.raises(ConfigError, match="at least one expected result"):
            QueryResultOracle({})


class TestQueryDriver:
    def _driver(self, clock, executor, **kwargs) -> QueryDriver:
        oracle = QueryResultOracle({"b1": ExpectedResult.from_rows(ROWS)})
        return QueryDriver(_entry(), oracle, executor, clock, **kwargs)

    def test_latency_is_measured_from_issue_to_verified_response(self, clock):
        def executor(entry, key, binds):
            clock.advance_ns(5_000_000)
            return QueryResponse("q1", ROWS, server_duration_ms=3.0)

        result = self._driver(clock, executor).run([QueryBinding("b1", 0)])
        assert result.registry.correct_completed == 1
        assert result.claim().result.value_ms == pytest.approx(5.0)

    def test_fast_empty_results_produce_no_latency_claim(self, clock):
        # Case E2E-38 end to end: 1 ms responses, all wrong, so the p95 has no
        # correct samples to be computed from.
        def executor(entry, key, binds):
            clock.advance_ns(1_000_000)
            return QueryResponse("q1", [])

        result = self._driver(clock, executor).run(uniform_bindings(["b1"] * 10, qps=100))
        assert result.registry.incorrect_completed == 10
        assert result.registry.correct_completed == 0
        assert result.claim().result.value_ms is None
        assert any("incorrect" in b for b in result.blockers())

    def test_timeouts_stay_in_the_denominator(self, clock):
        # Case E2E-41.
        def executor(entry, key, binds):
            clock.advance_ns(1_000_000)
            return QueryResponse("q1", [], timed_out=True)

        result = self._driver(clock, executor).run(uniform_bindings(["b1"] * 4, qps=100))
        assert result.registry.scheduled == 4
        assert result.registry.timeouts == 4
        assert result.registry.error_rate == 1.0

    def test_a_timeout_never_acquires_a_latency(self, clock):
        def executor(entry, key, binds):
            clock.advance_ns(1_000_000)
            return QueryResponse("q1", [], timed_out=True)

        result = self._driver(clock, executor).run([QueryBinding("b1", 0)])
        assert result.registry.completion_latencies_ms() == []

    def test_an_executor_exception_becomes_an_error_attempt(self, clock):
        def executor(entry, key, binds):
            raise RuntimeError("socket closed")

        result = self._driver(clock, executor).run([QueryBinding("b1", 0)])
        assert result.registry.errors == 1
        assert result.failures_by_kind["ERROR"] == 1

    def test_server_duration_is_kept_as_a_separate_diagnostic(self, clock):
        # INV-14: client RTT and server duration are not one distribution.
        def executor(entry, key, binds):
            clock.advance_ns(50_000_000)
            return QueryResponse("q1", ROWS, server_duration_ms=4.0)

        result = self._driver(clock, executor).run([QueryBinding("b1", 0)])
        payload = result.as_dict()
        assert payload["client_p95"]["value_ms"] == pytest.approx(50.0)
        assert payload["server_p95_secondary_diagnostic"]["value_ms"] == pytest.approx(4.0)

    def test_the_claim_carries_its_workload_labels(self, clock):
        def executor(entry, key, binds):
            clock.advance_ns(1_000_000)
            return QueryResponse("q1", ROWS)

        result = self._driver(
            clock, executor, concurrency=4, cache_profile="warm", dataset_build_id="BUILD-200M"
        ).run([QueryBinding("b1", 0)])
        assert result.claim().signature == ("Q02", 4, "warm", "BUILD-200M")

    def test_a_small_sample_set_blocks_the_claim(self, clock):
        def executor(entry, key, binds):
            clock.advance_ns(1_000_000)
            return QueryResponse("q1", ROWS)

        result = self._driver(clock, executor, min_complete_samples=100).run(
            [QueryBinding("b1", 0)]
        )
        assert not result.sample_count_adequate
        assert any("below the configured floor" in b for b in result.blockers())
        assert any("NFR-08" in b for b in result.blockers())

    def test_bindings_are_due_at_a_fixed_open_loop_cadence(self):
        bindings = uniform_bindings(["b1", "b1", "b1"], qps=4.0)
        assert [b.due_ns for b in bindings] == [0, NS_PER_S // 4, NS_PER_S // 2]

    def test_a_non_positive_qps_is_refused(self):
        with pytest.raises(ConfigError, match="qps must be positive"):
            uniform_bindings(["b1"], qps=0)

    def test_zero_concurrency_is_refused(self, clock):
        with pytest.raises(ConfigError, match="concurrency must be positive"):
            self._driver(clock, lambda *_: QueryResponse("q", ROWS), concurrency=0)
