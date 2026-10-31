"""Benchmark lanes and the load ladder: what-if group H.

These are measurements, not correctness assertions, and they are marked ``perf``
so they can be excluded from a fast run. What they *do* assert is the integrity
of the measurement itself, which is where section 10.3's list of common
benchmark mistakes applies:

* throughput counts completed decode+validate, never offered input;
* no rejection is swallowed, so a rate cannot be inflated by losing events;
* p50/p95/p99 are recorded, not just a mean;
* a target that was not reached is reported as DEGRADED, not rounded up to PASS.

The absolute numbers depend entirely on the machine, so the thresholds here are
deliberately loose; the report carries the real figures.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from md_contract.benchmark import (
    lane_b1_python_encode,
    lane_b2_python_decode,
    lane_b5_file_roundtrip,
    lane_b8_malformed,
    measure_ceiling,
    measure_sleep_granularity,
    run_all_lanes,
    run_ladder,
    run_ladder_step,
)
from md_contract.codec import encode_event
from md_contract.config import Config
from md_contract.conformance import Pipeline, decode_pbstream
from md_contract.strict_json import parse_sim_v1_line
from md_contract.validation import ValidationMode

pytestmark = pytest.mark.perf


@pytest.fixture(scope="module")
def events(clean_jsonl):
    out = []
    with clean_jsonl.open("rb") as handle:
        for raw in handle:
            if raw.strip():
                out.append(parse_sim_v1_line(raw))
    assert len(out) >= 1000
    return out


# ---------------------------------------------------------------------------
# lanes
# ---------------------------------------------------------------------------


@pytest.mark.cases("H01", "H15")
def test_encode_and_decode_lanes_report_complete_measurements(events) -> None:
    encode = lane_b1_python_encode(events[:5000])
    decode = lane_b2_python_decode([encode_event(e) for e in events[:5000]])

    for lane in (encode, decode):
        payload = lane.as_dict()
        assert lane.events == 5000
        assert lane.rejected == 0
        assert lane.events_per_sec > 0
        # The tail, not just the mean: GC pauses and allocator behaviour only
        # show up in p95/p99 (case H15).
        latency = payload["latency_us"]
        assert latency["p50"] is not None
        assert latency["p95"] is not None
        assert latency["p99"] is not None
        assert latency["exact"] is True
        assert latency["p99"] >= latency["p50"]

    # The payload size distribution is recorded, because a result from 60-byte
    # events says nothing about 4096-byte ones (case H06, section 10.3).
    sizes = encode.as_dict()["payload_bytes"]
    assert sizes["count"] == 5000
    assert sizes["p50"] is not None and sizes["max"] <= 4096

    print(
        f"\n  B1 encode: {encode.events_per_sec:,.0f} ev/s "
        f"(p50 {encode.latency.percentile(0.50):.2f}us, p99 {encode.latency.percentile(0.99):.2f}us)"
        f"\n  B2 decode: {decode.events_per_sec:,.0f} ev/s "
        f"(p50 {decode.latency.percentile(0.50):.2f}us, p99 {decode.latency.percentile(0.99):.2f}us)"
    )


@pytest.mark.cases("H01")
def test_the_end_to_end_lane_is_reported_separately_from_the_codec_lanes(
    clean_jsonl, tmp_path
) -> None:
    """B5 includes JSON parsing, file I/O and framing -- so it is a different number.

    Reporting an in-memory encode rate as end-to-end throughput is the first
    mistake section 10.3 lists, so the lane carries a note saying it is not
    comparable with B1/B2.
    """
    lane = lane_b5_file_roundtrip(clean_jsonl, tmp_path)
    assert lane.events == 5000
    assert lane.rejected == 0
    assert any("not comparable" in note for note in lane.notes)
    print(f"\n  B5 end-to-end: {lane.events_per_sec:,.0f} ev/s")


@pytest.mark.cases("H07")
def test_the_malformed_lane_counts_what_it_refused(events) -> None:
    valid = [encode_event(e) for e in events[:20]]
    malformed = [b"\x00", b"\x18\x80", b"\xff" * 40, b""]
    lane = lane_b8_malformed(malformed * 10 + valid, repeat=20)
    # Rejections are counted, not swallowed. A lane that hid them would report a
    # higher rate for having silently lost events.
    assert lane.rejected > 0
    assert lane.events == lane.rejected + 20 * 20


@pytest.mark.cases("H01", "H02")
@pytest.mark.slow
def test_run_all_lanes_produces_a_complete_report(clean_jsonl, tmp_path) -> None:
    report = run_all_lanes(clean_jsonl, tmp_path, sample_events=5000, trials=2)
    assert report["environment"]["python_protobuf_runtime_version"]
    assert len(report["lanes"]) == 4
    for lane in report["lanes"]:
        assert lane["events_completed"] > 0
        assert lane["duration_s"] > 0
        # The headline figure is a median over trials, with the spread beside
        # it, because a single run on this machine varies by more than 2x.
        assert lane["trials"] >= 2
        assert lane["events_per_sec_median"] > 0
        assert lane["events_per_sec_min"] <= lane["events_per_sec_median"]
        assert lane["events_per_sec_max"] >= lane["events_per_sec_median"]
        assert "events_per_sec" not in lane, "the single-run figure must not be quotable"
    assert "MEDIAN" in report["measurement_note"]
    # Lanes that were not run are named, not omitted: a report that quietly
    # dropped B3/B4/B6 would look complete when it is not (section 11.2).
    assert set(report["lanes_not_run"]) == {"B3", "B4", "B6", "B7"}


# ---------------------------------------------------------------------------
# the load ladder
# ---------------------------------------------------------------------------


@pytest.mark.cases("H01")
def test_a_low_rung_is_sustained_and_reports_the_achieved_rate(events) -> None:
    step = run_ladder_step(events, target_rate=1000, duration_s=2.0)
    assert step.verdict == "PASS", step.as_dict()
    assert step.events_rejected == 0
    # Both numbers appear: the target is what was asked for, the achieved rate
    # is the result, and reporting the former as the latter is the single most
    # common benchmark lie (section 10.3).
    payload = step.as_dict()
    assert payload["target_events_per_sec"] == 1000
    assert payload["achieved_events_per_sec"] > 0
    assert payload["achieved_events_per_sec"] >= 950
    assert payload["end_to_end_us"]["p99"] is not None
    print(f"\n  L0 1k/s: achieved {step.achieved_rate:,.0f} ev/s, verdict {step.verdict}")


@pytest.mark.cases("H03", "H04")
@pytest.mark.slow
def test_an_unreachable_target_is_reported_degraded_not_passed(events) -> None:
    """A rung the machine cannot sustain must not be reported as met.

    10 million events/s is beyond any Python pipeline, so this is a test of the
    *verdict logic*: the step has to come back DEGRADED with the rate it did
    achieve, rather than PASS because it tried.
    """
    step = run_ladder_step(events, target_rate=10_000_000, duration_s=1.0)
    assert step.verdict == "DEGRADED", step.as_dict()
    assert step.achieved_rate < 10_000_000
    assert step.achieved_rate > 0, "it still did real work and reported it"
    assert step.events_rejected == 0, "degraded, but nothing was lost"

    # And the open-loop ceiling explains *why* it degraded, which is what
    # separates "the pipeline is slow" from "this target was never reachable".
    ceiling = measure_ceiling(events, duration_s=1.0)
    assert ceiling.verdict == "MEASURED"
    assert ceiling.achieved_rate < 10_000_000
    assert step.achieved_rate <= ceiling.achieved_rate * 2


@pytest.mark.cases("H01", "H02", "H03", "H04")
@pytest.mark.slow
def test_the_ladder_stops_climbing_once_a_rung_fails(clean_jsonl) -> None:
    report = run_ladder(
        clean_jsonl, targets=[1000, 10_000, 5_000_000], duration_s=1.0, sample_events=5000
    )
    steps = report["steps"]
    # The climb stops at the first rung that is not sustained, and the targets
    # above it are listed as not run rather than guessed at (section 10.2).
    assert steps[0]["verdict"] == "PASS"
    assert steps[-1]["verdict"] in ("DEGRADED", "FAIL", "ABOVE_MEASURED_CEILING")
    assert report["max_sustained_events_per_sec"] in (1000, 10_000)
    assert report["max_sustained_verdict"] == "ESTABLISHED"
    assert "95%" in report["definition"]
    # The ceiling is reported alongside, and the targets that exceeded it are
    # named rather than just appearing as unexplained DEGRADED rows.
    assert report["open_loop_ceiling"]["achieved_events_per_sec"] > 0
    assert report["pacing"]["median_sleep_granularity_s"] > 0

    # Every target is accounted for in exactly one way, and the impossible one
    # is never reported as met: either the climb stopped before reaching it
    # (targets_not_run) or it was attempted and found to be above the measured
    # ceiling. Which of the two depends on whether the rung below held, so the
    # test asserts the accounting rather than one particular path.
    attempted = {step["target_events_per_sec"] for step in steps}
    accounted = (
        set(report["targets_not_run"])
        | set(report["targets_above_measured_ceiling"])
        | attempted
    )
    assert {1000, 10_000, 5_000_000} <= accounted
    assert 5_000_000 not in {
        step["target_events_per_sec"] for step in steps if step["verdict"] == "PASS"
    }
    for step in steps:
        assert step["events_rejected"] == 0
    print(
        f"\n  ladder: max sustained {report['max_sustained_events_per_sec']:,} ev/s; "
        f"not run: {report['targets_not_run']}"
    )


@pytest.mark.cases("H04")
def test_a_ladder_whose_first_rung_fails_reports_not_established(clean_jsonl) -> None:
    """No sustained rate must be reported as NOT_ESTABLISHED, not as zero.

    Section 10.3 is explicit about this: if no ceiling could be found, the
    report says so. Substituting 0, or the lowest target attempted, would read
    as a measurement when nothing was measured.
    """
    report = run_ladder(
        clean_jsonl, targets=[50_000_000], duration_s=0.5, sample_events=2000
    )
    assert report["max_sustained_events_per_sec"] is None
    assert report["max_sustained_verdict"] == "NOT_ESTABLISHED"
    assert len(report["steps"]) == 1
    # The attempt still reports the real rate it achieved, and says the target
    # was above the measured ceiling rather than blaming the pipeline.
    assert report["steps"][0]["achieved_events_per_sec"] > 0
    assert report["steps"][0]["verdict"] == "ABOVE_MEASURED_CEILING"


# ---------------------------------------------------------------------------
# resource behaviour
# ---------------------------------------------------------------------------


@pytest.mark.cases("H06")
def test_worst_case_payload_sizes_are_handled_and_measured(events) -> None:
    big = [e._replace(dataset_id="d" * 120) for e in events[:2000]]
    lane = lane_b1_python_encode(big)
    sizes = lane.as_dict()["payload_bytes"]
    assert lane.rejected == 0
    assert sizes["max"] <= 4096
    assert sizes["p50"] > 100, "this lane really is using larger payloads"


@pytest.mark.cases("H08")
@pytest.mark.slow
def test_a_sustained_run_does_not_grow_memory_without_bound(events) -> None:
    """A short soak: RSS must not trend upward across repeated batches.

    Short enough for CI, and not a substitute for the 60-minute soak the design
    asks for -- that is reported separately. What this catches is a per-event
    leak, which would show up even over 20 batches.
    """
    psutil = pytest.importorskip("psutil")
    import gc
    import os

    process = psutil.Process(os.getpid())
    payloads = [encode_event(e) for e in events[:2000]]

    gc.collect()
    samples = []
    for _ in range(20):
        lane = lane_b2_python_decode(payloads)
        assert lane.rejected == 0
        gc.collect()
        samples.append(process.memory_info().rss / (1024 * 1024))

    first_half = sum(samples[:10]) / 10
    second_half = sum(samples[10:]) / 10
    growth = second_half - first_half
    assert growth < 50, f"RSS grew {growth:.1f} MB across 20 batches: {samples}"
    print(f"\n  soak(short): RSS {samples[0]:.1f} -> {samples[-1]:.1f} MB")


@pytest.mark.cases("H17")
def test_measurements_use_a_monotonic_clock_so_a_clock_jump_cannot_distort_them(
    events,
) -> None:
    """Performance timing must not come from the wall clock.

    A wall-clock jump during a run would produce a negative or absurd duration.
    ``time.perf_counter`` is monotonic, so this asserts the measured duration is
    positive and plausible, and separately that the event payload's own
    ``event_time_ns`` is untouched by any of it.
    """
    step = run_ladder_step(events, target_rate=2000, duration_s=0.5)
    assert step.duration_s > 0
    assert 0.3 < step.duration_s < 3.0

    # The payload timestamp is data, never compared against a clock.
    before = events[0].event_time_ns
    from md_contract.codec import decode_payload

    assert decode_payload(encode_event(events[0])).event_time_ns == before


@pytest.mark.cases("H11", "H12")
def test_queue_budgets_hold_under_a_burst() -> None:
    """Burst at 3x: the queue stays inside both budgets or refuses (case H05)."""
    from md_contract.tcp_stream import BoundedFrameQueue, QueueBudgetExceeded

    queue = BoundedFrameQueue(max_events=256, max_bytes=256 * 1024)
    payload = b"x" * 512
    accepted = refused = 0
    for _ in range(3000):
        try:
            queue.put(payload, timeout=0.0)
            accepted += 1
        except QueueBudgetExceeded:
            refused += 1

    assert refused > 0, "a 3000-item burst into a 256-slot queue must refuse some"
    assert queue.depth <= 256
    assert queue.byte_depth <= 256 * 1024
    assert queue.peak_events <= 256
    assert queue.peak_bytes <= 256 * 1024
    # Nothing was dropped silently: every item was either queued or refused.
    assert accepted + refused == 3000
