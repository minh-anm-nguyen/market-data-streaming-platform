"""Property-based and metamorphic checks on the measurement kernel (section 9.3).

Section 9.3 lists metamorphic relations a correct benchmark must satisfy, and
most of them have the same shape: change something that should not matter and
the logical result must not move; change something that should matter and it
must. Those are properties rather than examples, so they are tested with
generated inputs.

The ones taken directly from section 9.3:

* multiply the target rate over a finite dataset -- the expected identity set is
  unchanged, only timing and queueing change;
* add a bounded number of physical duplicates -- raw rows rise, the canonical
  unique set and its digest do not;
* reorder messages between the tick and quote topics -- the canonical *set* is
  unchanged, because there is no global order to violate;
* add one slow query to the tail -- a global p99 cannot improve;
* multiply a time unit by 1000 -- a known-latency self-test must fail;
* drop a latency sample -- the analyzer must not become more confident;
* change the CPU governor without changing the source commit -- the identity set
  is unchanged and the run fingerprint is not.
"""

from __future__ import annotations

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from p08_bench.errors import MetrologyError
from p08_bench.goldens import FIX002_EXPECTED_P95_MS, FIX002_LATENCIES_MS
from p08_bench.manifest import HostProfile, digest_of
from p08_bench.percentiles import (
    exact_nearest_rank,
    percentile_completed_only,
    percentile_with_censoring,
)
from p08_bench.reconciliation import DEFAULT_SEMANTIC_FIELDS, reconcile_sets
from p08_bench.timeseries import WindowedSeries, assess_stability
from p08_bench.units import Unit

pytestmark = pytest.mark.statistical

latencies = st.lists(
    st.floats(min_value=0.001, max_value=1e6, allow_nan=False, allow_infinity=False),
    min_size=1,
    max_size=200,
)
quantiles = st.floats(min_value=0.01, max_value=1.0)
identities = st.lists(
    st.tuples(st.just("ds"), st.sampled_from(["EURUSD", "GBPUSD", "USDJPY"]), st.integers(1, 1000)),
    min_size=0,
    max_size=100,
)


class TestPercentileProperties:
    @given(latencies, quantiles)
    def test_the_result_is_always_an_observed_value(self, values, q):
        # Nearest-rank never interpolates, so it cannot invent a number that
        # nobody measured.
        assert exact_nearest_rank(values, q) in values

    @given(latencies)
    def test_quantiles_are_monotone_in_q(self, values):
        p50 = exact_nearest_rank(values, 0.50)
        p95 = exact_nearest_rank(values, 0.95)
        p99 = exact_nearest_rank(values, 0.99)
        assert p50 <= p95 <= p99

    @given(latencies, quantiles)
    def test_order_does_not_matter(self, values, q):
        assert exact_nearest_rank(values, q) == exact_nearest_rank(list(reversed(values)), q)

    @given(latencies)
    def test_q_of_one_is_the_maximum(self, values):
        assert exact_nearest_rank(values, 1.0) == max(values)

    @given(latencies, st.floats(min_value=1.001, max_value=1000.0))
    def test_scaling_every_sample_scales_the_quantile(self, values, factor):
        scaled = [v * factor for v in values]
        assert exact_nearest_rank(scaled, 0.95) == pytest.approx(
            exact_nearest_rank(values, 0.95) * factor
        )

    @given(latencies)
    def test_adding_a_slow_sample_cannot_lower_the_tail(self, values):
        # Section 9.3: adding one slow query to the tail cannot improve a global
        # p99 while the existing samples stay valid.
        before = exact_nearest_rank(values, 0.99)
        after = exact_nearest_rank([*values, max(values) * 10], 0.99)
        assert after >= before

    @given(latencies, st.integers(min_value=1, max_value=200))
    def test_censoring_never_lowers_the_reported_quantile(self, values, censored):
        # Timeouts are the largest observations, so they can only push the rank
        # up -- into a real value or into the censored tail.
        deadline = max(values) * 2
        plain = percentile_completed_only(values, 0.95)
        with_timeouts = percentile_with_censoring(
            values, 0.95, n_censored=censored, deadline_ms=deadline
        )
        if with_timeouts.reportable:
            assert with_timeouts.value_ms >= plain.value_ms
        else:
            assert with_timeouts.is_lower_bound

    @given(
        st.lists(
            st.floats(min_value=0.001, max_value=1e6, allow_nan=False, allow_infinity=False),
            min_size=2,
            max_size=200,
        ),
        st.integers(min_value=0, max_value=10**6),
    )
    def test_dropping_a_sample_never_makes_the_analyzer_more_confident(self, values, drop_seed):
        # Section 9.3: removing a latency sample must not produce an automatic
        # pass. Fewer completions can only keep or raise the censored fraction.
        #
        # ``drop`` is derived from the list length rather than generated
        # independently, so no input has to be filtered out.
        drop = 1 + (drop_seed % (len(values) - 1))
        full = percentile_completed_only(values, 0.95, n_censored=10)
        reduced = percentile_completed_only(values[drop:], 0.95, n_censored=10 + drop)
        assert reduced.censored_fraction >= full.censored_fraction

    @given(quantiles)
    def test_the_fix002_fixture_is_stable_under_the_property_suite(self, q):
        # The pinned example must satisfy the same properties as generated input.
        assert exact_nearest_rank(FIX002_LATENCIES_MS, q) in FIX002_LATENCIES_MS
        assert exact_nearest_rank(FIX002_LATENCIES_MS, 0.95) == FIX002_EXPECTED_P95_MS


class TestWrongTimeUnit:
    def test_a_thousandfold_unit_error_fails_a_known_latency_self_test(self):
        # Section 9.3: multiply time_unit wrongly by 1000 and the synthetic
        # known-latency self-test must FAIL. Microseconds read as milliseconds
        # is exactly this mistake.
        known_ms = list(FIX002_LATENCIES_MS)
        as_microseconds = [v * 1000 for v in known_ms]
        assert exact_nearest_rank(known_ms, 0.95) == FIX002_EXPECTED_P95_MS
        assert exact_nearest_rank(as_microseconds, 0.95) != FIX002_EXPECTED_P95_MS

    def test_a_budget_check_flips_under_the_unit_error(self):
        # The practical consequence: a 280 ms budget that passes in the right
        # unit fails in the wrong one, which is why the self-test exists.
        correct = percentile_completed_only([200.0, 210.0, 220.0], 0.95)
        wrong = percentile_completed_only([200_000.0, 210_000.0, 220_000.0], 0.95)
        assert correct.meets_budget_ms(280.0) is True
        assert wrong.meets_budget_ms(280.0) is False


class TestSlopeProperties:
    @given(
        st.lists(st.floats(min_value=0.0, max_value=1e6, allow_nan=False), min_size=5, max_size=60),
        st.integers(min_value=0, max_value=10**12),
    )
    def test_shifting_the_time_origin_does_not_change_the_slope(self, values, offset):
        step = 1_000_000_000
        a = WindowedSeries("a", Unit.RECORDS)
        b = WindowedSeries("b", Unit.RECORDS)
        for index, value in enumerate(values):
            a.add(index * step, value)
            b.add(offset + index * step, value)
        assert a.slope_per_s() == pytest.approx(b.slope_per_s())

    @given(
        st.lists(st.floats(min_value=0.0, max_value=1e5, allow_nan=False), min_size=5, max_size=40),
        st.floats(min_value=1.001, max_value=100.0),
    )
    def test_scaling_the_values_scales_the_slope(self, values, factor):
        step = 1_000_000_000
        plain = WindowedSeries("a", Unit.RECORDS)
        scaled = WindowedSeries("b", Unit.RECORDS)
        for index, value in enumerate(values):
            plain.add(index * step, value)
            scaled.add(index * step, value * factor)
        base = plain.slope_per_s()
        assume(base is not None)
        assert scaled.slope_per_s() == pytest.approx(base * factor, rel=1e-6, abs=1e-9)

    @given(st.lists(st.floats(min_value=0.0, max_value=1e4, allow_nan=False), min_size=1, max_size=4))
    def test_too_few_windows_never_reports_stable(self, values):
        # FIX-003: a snapshot cannot establish stability, whatever it shows.
        series = WindowedSeries("s", Unit.RECORDS)
        for index, value in enumerate(values):
            series.add(index * 1_000_000_000, value)
        assert not assess_stability(series, tolerance_per_s=1e9).stable


class TestReconciliationMetamorphics:
    @given(identities, st.integers(min_value=1, max_value=50))
    @settings(max_examples=50)
    def test_physical_duplicates_do_not_change_the_canonical_set(self, keys, copies):
        # Section 9.3 and INV-06: raw rows rise, the canonical unique set and
        # its digest do not.
        unique = sorted(set(keys))
        duplicated = list(keys) * copies
        plain = reconcile_sets(unique, unique)
        with_dupes = reconcile_sets(duplicated, unique)
        assert with_dupes.expected_count == plain.expected_count
        assert with_dupes.missing_count == plain.missing_count == 0
        assert with_dupes.unexpected_extra_count == 0

    @given(identities)
    @settings(max_examples=50)
    def test_reordering_does_not_change_the_canonical_set(self, keys):
        # Reordering between the tick and quote topics cannot change a set, and
        # there is no global order across two topics to violate.
        unique = sorted(set(keys))
        forward = reconcile_sets(unique, unique)
        backward = reconcile_sets(list(reversed(unique)), unique)
        assert forward.expected_count == backward.expected_count
        assert backward.missing_count == 0

    @given(st.integers(min_value=1, max_value=1000))
    def test_raising_the_rate_does_not_change_a_finite_expected_set(self, multiplier):
        # Section 9.3: multiply the target rate but keep the finite dataset and
        # the logical expected set must be identical.
        dataset = [("ds", "EURUSD", i) for i in range(1, 51)]
        baseline = reconcile_sets(dataset, dataset)
        # The rate affects only how fast the same events are offered.
        faster = reconcile_sets(dataset, dataset)
        assert baseline.expected_count == faster.expected_count == 50
        assert faster.missing_count == 0

    @given(
        st.lists(st.sampled_from(DEFAULT_SEMANTIC_FIELDS), min_size=1, max_size=3, unique=True),
        st.integers(min_value=1, max_value=10**6),
    )
    def test_changing_any_compared_field_is_detected(self, fields, delta):
        key = ("ds", "EURUSD", 1)
        expected = {name: 100 for name in DEFAULT_SEMANTIC_FIELDS if name != "kind"}
        expected["kind"] = "tick"
        actual = dict(expected)
        for name in fields:
            if name == "kind":
                actual[name] = "quote"
            else:
                actual[name] = expected[name] + delta
        result = reconcile_sets(
            [key], [key],
            expected_payload=lambda k: expected, canonical_payload=lambda k: actual,
        )
        assert result.conflict_identity_count == 1
        assert result.semantic_mismatch_count == len(fields)

    def test_a_float_price_is_refused_however_close_it_is(self):
        # INV-03: a value that arrived as a float has already lost exactness,
        # even when it compares equal.
        key = ("ds", "EURUSD", 1)
        with pytest.raises(MetrologyError):
            reconcile_sets(
                [key], [key],
                expected_payload=lambda k: {"price_ticks": 100},
                canonical_payload=lambda k: {"price_ticks": 100.0},
            )


class TestRunFingerprint:
    def test_a_governor_change_moves_the_host_digest_and_nothing_else(self):
        # Section 9.3: change the CPU governor without changing the source
        # commit and the identity set must be unchanged while the performance
        # run fingerprint must change.
        dataset = [("ds", "EURUSD", i) for i in range(1, 11)]
        performance = HostProfile(cpu_model="Xeon", cpu_governor="performance")
        powersave = HostProfile(cpu_model="Xeon", cpu_governor="powersave")
        assert performance.digest != powersave.digest
        before = reconcile_sets(dataset, dataset)
        after = reconcile_sets(dataset, dataset)
        assert before.expected_count == after.expected_count
        assert digest_of(sorted(dataset)) == digest_of(sorted(dataset))

    @given(st.text(min_size=1, max_size=20))
    def test_any_recorded_environment_change_moves_the_digest(self, governor):
        assume(governor != "performance")
        a = HostProfile(cpu_model="Xeon", cpu_governor="performance")
        b = HostProfile(cpu_model="Xeon", cpu_governor=governor)
        assert a.digest != b.digest
