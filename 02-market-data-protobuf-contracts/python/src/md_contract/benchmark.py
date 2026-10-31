"""Benchmark lanes B1-B8 and the load ladder (section 10).

The lanes exist so a number can be attributed to a cause. "The codec does 80k
events/s" is not a usable claim if the measurement included JSON parsing, file
I/O and framing: an encode-only lane and an end-to-end lane answer different
questions, and section 10.3 lists the mistakes that follow from mixing them.

Rules this module enforces on itself:

* Throughput is **completed** work. ``events_per_sec`` counts events that
  decoded *and* validated, not events offered. Reporting the input rate as
  throughput is how a target gets mistaken for a result.
* Every rejection is counted. There is no bare ``except: pass`` anywhere in a
  measured loop -- swallowing a parse failure inflates the rate and loses data.
* p50/p95/p99 are always reported, never just the mean. GC pauses, allocator
  behaviour and CPU throttling show up only in the tail.
* The payload size distribution is recorded, because a result from 60-byte
  events says little about 4096-byte ones.
* A target that was not reached is reported as ``NOT_ESTABLISHED``, not quietly
  rounded down.
"""

from __future__ import annotations

import gc
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Sequence

from .codec import decode_payload, encode_event, parse_payload
from .config import Config
from .conformance import Pipeline, decode_pbstream, environment_metadata
from .domain import DomainEvent
from .framing import BytesSource, FrameReader, encode_stream
from .metrics import Histogram, Metrics
from .strict_json import parse_sim_v1_line
from .validation import ValidationMode

try:  # psutil is optional; without it RSS is reported as unavailable, not zero.
    import psutil

    _PROCESS = psutil.Process(os.getpid())
except Exception:  # noqa: BLE001
    psutil = None  # type: ignore[assignment]
    _PROCESS = None


def peak_rss_mb() -> float | None:
    if _PROCESS is None:
        return None
    return _PROCESS.memory_info().rss / (1024 * 1024)


@dataclass
class LaneResult:
    """One lane's measurement, with everything needed to interpret it."""

    lane: str
    description: str
    events: int
    duration_s: float
    rejected: int = 0
    latency: Histogram | None = None
    payload_bytes: Histogram | None = None
    rss_mb_before: float | None = None
    rss_mb_after: float | None = None
    notes: list[str] = field(default_factory=list)
    #: 1 means every event was timed; N means 1 in N. Reported so the latency
    #: figures can be read correctly alongside the throughput figure.
    latency_sampled_every: int = 1

    @property
    def events_per_sec(self) -> float:
        return self.events / self.duration_s if self.duration_s > 0 else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "description": self.description,
            "events_completed": self.events,
            "events_rejected": self.rejected,
            "duration_s": round(self.duration_s, 6),
            "events_per_sec": round(self.events_per_sec, 1),
            "latency_us": self.latency.summary() if self.latency else None,
            "latency_sampled_every": self.latency_sampled_every,
            "payload_bytes": self.payload_bytes.summary() if self.payload_bytes else None,
            "rss_mb_before": self.rss_mb_before,
            "rss_mb_after": self.rss_mb_after,
            "rss_available": _PROCESS is not None,
            "notes": self.notes,
        }


def _load_events(source: str | Path, limit: int) -> list[DomainEvent]:
    events: list[DomainEvent] = []
    with open(source, "rb") as handle:
        for raw in handle:
            if len(events) >= limit:
                break
            if raw.strip():
                events.append(parse_sim_v1_line(raw))
    if not events:
        raise ValueError(f"no events parsed from {source}")
    return events


def _timed(
    lane: str,
    description: str,
    work: Callable[[], tuple[int, int]],
    *,
    latency: Histogram | None = None,
    payload_bytes: Histogram | None = None,
    notes: Sequence[str] = (),
) -> LaneResult:
    rss_before = peak_rss_mb()
    # One collection before the clock starts, then GC left enabled. Disabling it
    # would produce a prettier p99 that no production run would ever see
    # (case H15).
    gc.collect()
    start = time.perf_counter()
    events, rejected = work()
    duration = time.perf_counter() - start
    return LaneResult(
        lane=lane,
        description=description,
        events=events,
        duration_s=duration,
        rejected=rejected,
        latency=latency,
        payload_bytes=payload_bytes,
        rss_mb_before=rss_before,
        rss_mb_after=peak_rss_mb(),
        notes=list(notes),
    )


#: Time only every Nth event when measuring latency.
#:
#: This exists because measuring every event distorted the throughput badly.
#: Timing each decode -- two ``perf_counter`` calls and a histogram append --
#: plus the GC pressure of the extra samples put the measured decode rate at
#: ~11k events/s against ~88k when the same work ran uninstrumented: an 8x
#: understatement, with the instrumentation dominating the thing being measured.
#: Sampling keeps the latency distribution (the tail is what matters, and a
#: 1-in-16 sample of a uniform workload preserves it) while leaving throughput
#: close to the uninstrumented cost. Both numbers are reported, and
#: ``latency_sampled_every`` says which is which (section 10.3).
LATENCY_SAMPLE_EVERY: Final = 16


def lane_b1_python_encode(
    events: Sequence[DomainEvent], repeat: int = 1, *, sample_every: int = LATENCY_SAMPLE_EVERY
) -> LaneResult:
    latency = Histogram("encode_us")
    sizes = Histogram("payload_bytes")

    def work() -> tuple[int, int]:
        count = rejected = 0
        index = 0
        for _ in range(repeat):
            for event in events:
                timed = index % sample_every == 0
                index += 1
                t0 = time.perf_counter() if timed else 0.0
                try:
                    payload = encode_event(event)
                except Exception:  # noqa: BLE001 - counted, never swallowed
                    rejected += 1
                    continue
                if timed:
                    latency.observe((time.perf_counter() - t0) * 1e6)
                # Sizes are recorded for *every* event, not sampled: it is a
                # len() and an append with no clock call, and section 10.3 wants
                # the full size distribution -- a result from 60-byte events
                # says nothing about 4096-byte ones.
                sizes.observe(len(payload))
                count += 1
        return count, rejected

    result = _timed(
        "B1",
        "Python typed objects -> Protobuf encode (in memory)",
        work,
        latency=latency,
        payload_bytes=sizes,
    )
    result.latency_sampled_every = sample_every
    return result


def lane_b2_python_decode(
    payloads: Sequence[bytes], repeat: int = 1, *, sample_every: int = LATENCY_SAMPLE_EVERY
) -> LaneResult:
    latency = Histogram("decode_us")

    def work() -> tuple[int, int]:
        count = rejected = 0
        index = 0
        for _ in range(repeat):
            for payload in payloads:
                timed = index % sample_every == 0
                index += 1
                t0 = time.perf_counter() if timed else 0.0
                try:
                    decode_payload(payload)
                except Exception:  # noqa: BLE001 - counted, never swallowed
                    rejected += 1
                    continue
                if timed:
                    latency.observe((time.perf_counter() - t0) * 1e6)
                count += 1
        return count, rejected

    result = _timed(
        "B2",
        "Prebuilt Protobuf bytes -> Python decode + envelope check (in memory)",
        work,
        latency=latency,
    )
    result.latency_sampled_every = sample_every
    return result


def lane_b5_file_roundtrip(
    source: str | Path, work_dir: str | Path, *, config: Config | None = None
) -> LaneResult:
    """Python encode -> framed file -> Python decode, including file I/O."""
    config = config or Config()
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    stream = work / "bench_b5.pbstream"
    sizes = Histogram("payload_bytes")

    def work_fn() -> tuple[int, int]:
        pipeline = Pipeline(config=config, mode=ValidationMode.FAULT_PASSTHROUGH)
        encoded = pipeline.run_file(source, stream)
        decoded = decode_pbstream(stream, config=config)
        for sample in decoded.metrics.histograms.get("payload_bytes", Histogram("x")).samples:
            sizes.observe(sample)
        rejected = (
            encoded.metrics.get("json_rejected_total")
            + encoded.metrics.get("policy_rejected_total")
            + decoded.metrics.get("decode_rejected_total")
            + decoded.metrics.get("envelope_rejected_total")
        )
        return decoded.metrics.get("events_accepted_total"), rejected

    return _timed(
        "B5",
        "sim.v1 JSONL -> strict parse -> encode -> .pbstream -> decode + validate",
        work_fn,
        payload_bytes=sizes,
        notes=[
            "includes JSON parsing, file I/O and framing; not comparable with B1/B2"
        ],
    )


def lane_b8_malformed(payloads: Sequence[bytes], repeat: int = 1) -> LaneResult:
    """Worst-case validation work: a corpus that is mostly rejected.

    Rejection has to be *cheap*. If a malformed frame cost more than a valid
    one, a hostile sender could amplify load by sending garbage (case H07).
    """

    def work() -> tuple[int, int]:
        accepted = rejected = 0
        for _ in range(repeat):
            for payload in payloads:
                try:
                    decode_payload(payload)
                    accepted += 1
                except Exception:  # noqa: BLE001 - this lane is *about* rejections
                    rejected += 1
        return accepted + rejected, rejected

    return _timed(
        "B8",
        "Malformed/mixed corpus: cost of safely rejecting bad input",
        work,
        notes=["'events' counts frames examined; 'rejected' is the share refused"],
    )


@dataclass
class LadderStep:
    """One rung of the load ladder (section 10.2)."""

    target_rate: int
    duration_s: float
    achieved_rate: float
    events_completed: int
    events_rejected: int
    latency: Histogram
    queue_peak_events: int = 0
    queue_peak_bytes: int = 0
    rss_mb: float | None = None
    verdict: str = "NOT_RUN"

    def as_dict(self) -> dict[str, Any]:
        return {
            "target_events_per_sec": self.target_rate,
            "duration_s": round(self.duration_s, 3),
            # Both numbers, always. The achieved rate is the result; the target
            # is only what was asked for, and reporting the target as the result
            # is the single most common benchmark lie (section 10.3).
            "achieved_events_per_sec": round(self.achieved_rate, 1),
            "events_completed": self.events_completed,
            "events_rejected": self.events_rejected,
            "end_to_end_us": self.latency.summary(),
            "queue_peak_events": self.queue_peak_events,
            "queue_peak_bytes": self.queue_peak_bytes,
            "rss_mb": self.rss_mb,
            "verdict": self.verdict,
        }


#: Pacing tick. Batches are aligned to this, not individual events.
#:
#: Per-event pacing does not work at these rates. At 10k events/s the interval
#: is 100 us, far below the OS timer granularity (~15 ms on Windows by
#: default), so sleeping overshoots wildly and spinning makes the clock
#: arithmetic itself the bottleneck -- an earlier per-event version measured
#: 3.7k events/s against a 10k target while reaching 25k with no pacing at all,
#: which says everything about what was being measured. Pacing a batch per tick
#: keeps the average rate accurate and the overhead per event negligible.
PACING_TICK_S: Final = 0.05


def measure_ceiling(
    events: Sequence[DomainEvent],
    duration_s: float = 2.0,
    *,
    config: Config | None = None,
) -> LadderStep:
    """Open-loop: run encode+decode flat out and report the rate achieved.

    This is the honest "what can this machine do" number, and it has to be
    measured before the paced rungs mean anything: a paced run can only tell you
    whether a target *below* the ceiling is sustainable.
    """
    config = config or Config()
    latency = Histogram("end_to_end_us")
    max_payload = config.contract.max_payload_bytes
    start = time.perf_counter()
    deadline = start + duration_s
    completed = rejected = index = 0

    while time.perf_counter() < deadline:
        # A chunk per clock check, so the clock is not the thing being measured.
        for _ in range(256):
            event = events[index % len(events)]
            index += 1
            sample = index % LATENCY_SAMPLE_EVERY == 0
            t0 = time.perf_counter() if sample else 0.0
            try:
                decode_payload(
                    encode_event(event, max_payload_bytes=max_payload),
                    max_payload_bytes=max_payload,
                )
            except Exception:  # noqa: BLE001 - counted, never swallowed
                rejected += 1
                continue
            if sample:
                latency.observe((time.perf_counter() - t0) * 1e6)
            completed += 1

    elapsed = time.perf_counter() - start
    return LadderStep(
        target_rate=0,  # open loop: there was no target
        duration_s=elapsed,
        achieved_rate=completed / elapsed if elapsed > 0 else 0.0,
        events_completed=completed,
        events_rejected=rejected,
        latency=latency,
        rss_mb=peak_rss_mb(),
        verdict="FAIL" if rejected else "MEASURED",
    )


def measure_sleep_granularity(samples: int = 20) -> float:
    """The shortest sleep this OS actually delivers, in seconds.

    Needed to tell a pacing limit from a pipeline limit. Windows' default timer
    resolution is ~15 ms, so a request to sleep 7 ms can take 15 ms; a paced
    rung then lands at roughly half its target no matter how fast the codec is.
    Reporting that as DEGRADED would blame the pipeline for the harness, which
    is precisely the misattribution section 10.3 warns about.
    """
    # The median, not the worst case. A single long sample under load gave
    # 57 ms where the median was 1.6 ms, and basing the tick on that produced a
    # nonsensical 880 events/s pacing limit. This figure is reported as a
    # diagnostic; it is deliberately NOT used to decide a rung's verdict, which
    # is judged against the measured open-loop ceiling instead.
    observed = []
    for _ in range(samples):
        start = time.perf_counter()
        time.sleep(0.001)
        observed.append(time.perf_counter() - start)
    observed.sort()
    return observed[len(observed) // 2]


def run_ladder_step(
    events: Sequence[DomainEvent],
    target_rate: int,
    duration_s: float,
    *,
    config: Config | None = None,
    tick_s: float | None = None,
) -> LadderStep:
    """Drive encode+decode at a paced rate and measure what completed.

    Batches are paced against a monotonic clock. If the pipeline cannot keep up,
    the step records the rate it *did* achieve and is marked ``DEGRADED`` rather
    than being reported as having met the target.
    """
    config = config or Config()
    latency = Histogram("end_to_end_us")
    max_payload = config.contract.max_payload_bytes

    # The tick must be long enough that the OS timer's error is a small
    # fraction of it, otherwise the sleep overshoots and the measured rate
    # reflects the scheduler. run_ladder derives this from a measured
    # granularity; the default is only a floor.
    tick = tick_s if tick_s is not None else PACING_TICK_S
    # Events per tick, at least one. At 120k/s and a 30 ms tick that is 3600.
    per_tick = max(1, round(target_rate * tick))
    start = time.perf_counter()
    deadline = start + duration_s
    completed = rejected = index = 0
    ticks = 0

    while True:
        now = time.perf_counter()
        if now >= deadline:
            break
        # The tick's scheduled start, measured from the run's start so lateness
        # accumulates visibly instead of each wait hiding it.
        scheduled = start + ticks * tick
        if now < scheduled:
            time.sleep(scheduled - now)
        ticks += 1

        for position in range(per_tick):
            # The deadline is checked inside the batch, not only between ticks.
            # At a very high target a batch can hold hundreds of thousands of
            # events, and running it to completion would overshoot a 1-second
            # measurement by tens of seconds -- the run would then report a
            # duration nobody asked for.
            if position % 4096 == 0 and time.perf_counter() >= deadline:
                break
            event = events[index % len(events)]
            index += 1
            sample = index % LATENCY_SAMPLE_EVERY == 0
            t0 = time.perf_counter() if sample else 0.0
            try:
                decode_payload(
                    encode_event(event, max_payload_bytes=max_payload),
                    max_payload_bytes=max_payload,
                )
            except Exception:  # noqa: BLE001 - counted, never swallowed
                rejected += 1
                continue
            if sample:
                latency.observe((time.perf_counter() - t0) * 1e6)
            completed += 1

    elapsed = time.perf_counter() - start
    achieved = completed / elapsed if elapsed > 0 else 0.0
    # 95% of target is the line between "met it" and "could not". Stated here
    # rather than implied, so a report cannot be read as a pass at 60%.
    if rejected:
        verdict = "FAIL"
    elif achieved >= target_rate * 0.95:
        verdict = "PASS"
    else:
        verdict = "DEGRADED"

    return LadderStep(
        target_rate=target_rate,
        duration_s=elapsed,
        achieved_rate=achieved,
        events_completed=completed,
        events_rejected=rejected,
        latency=latency,
        rss_mb=peak_rss_mb(),
        verdict=verdict,
    )


#: How many times each lane is run before a rate is reported.
#:
#: One trial is not a measurement on this class of machine. Repeating a single
#: unchanged encode lane gave 65k, 88k, 91k, 134k, 139k and 149k events/s in one
#: sitting -- a 2.3x spread, and disabling the garbage collector did not change
#: it, so it is CPU frequency scaling and background load rather than anything
#: in the code. Reporting whichever number a single run happened to produce
#: would be reporting the machine's mood. The lanes therefore run several trials
#: and report the median with the full range beside it.
DEFAULT_TRIALS: Final = 5


@dataclass
class TrialSummary:
    """Several runs of one lane, reported as a median with its spread."""

    lane: str
    description: str
    rates: list[float]
    events: int
    best: LaneResult

    @property
    def median_rate(self) -> float:
        ordered = sorted(self.rates)
        middle = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) / 2

    @property
    def spread_ratio(self) -> float:
        low = min(self.rates)
        return max(self.rates) / low if low > 0 else 0.0

    def as_dict(self) -> dict[str, Any]:
        payload = self.best.as_dict()
        payload.update(
            {
                "trials": len(self.rates),
                # The median is the headline; the range is what says how much to
                # trust it. A spread_ratio near 1.0 means a stable machine, and
                # the 2.3x seen here means single-run figures are not citable.
                "events_per_sec_median": round(self.median_rate, 1),
                "events_per_sec_min": round(min(self.rates), 1),
                "events_per_sec_max": round(max(self.rates), 1),
                "events_per_sec_spread_ratio": round(self.spread_ratio, 2),
                "events_per_sec_all_trials": [round(rate, 1) for rate in self.rates],
            }
        )
        # Remove the single-run figure so it cannot be quoted as "the" rate.
        payload.pop("events_per_sec", None)
        return payload


def run_trials(
    lane: Callable[[], LaneResult], trials: int = DEFAULT_TRIALS
) -> TrialSummary:
    """Run a lane several times; keep the rates and the fastest trial's detail."""
    results = [lane() for _ in range(max(1, trials))]
    best = max(results, key=lambda r: r.events_per_sec)
    return TrialSummary(
        lane=best.lane,
        description=best.description,
        rates=[r.events_per_sec for r in results],
        events=best.events,
        best=best,
    )


def run_all_lanes(
    source: str | Path,
    work_dir: str | Path,
    *,
    config: Config | None = None,
    sample_events: int = 20000,
    repeat: int = 1,
    trials: int = DEFAULT_TRIALS,
) -> dict[str, Any]:
    """Run the in-process lanes and return a report fragment."""
    config = config or Config()
    events = _load_events(source, sample_events)
    payloads = [encode_event(event) for event in events]

    # A deliberately hostile mix for B8: truncated, zero-length-ish, bad tags
    # and oversize, alongside a minority of valid frames.
    malformed = [
        b"\x00",                       # field number 0 is never valid
        b"\x08\x80",                   # truncated varint
        b"\xff" * 64,                  # garbage
        b"\x62\x02\x08\x03",           # unknown payload variant (tag 12)
        b"",                           # empty
    ]
    mixed = (malformed * 4) + payloads[: max(1, len(payloads) // 10)]

    summaries = [
        run_trials(lambda: lane_b1_python_encode(events, repeat=repeat), trials),
        run_trials(lambda: lane_b2_python_decode(payloads, repeat=repeat), trials),
        # B5 writes and fsyncs a file per trial, so it gets fewer of them.
        run_trials(
            lambda: lane_b5_file_roundtrip(source, work_dir, config=config),
            max(2, trials // 2),
        ),
        run_trials(lambda: lane_b8_malformed(mixed, repeat=max(1, repeat * 20)), trials),
    ]
    return {
        "environment": environment_metadata(),
        "sample_events": len(events),
        "repeat": repeat,
        "trials": trials,
        "measurement_note": (
            "each lane's headline figure is the MEDIAN of its trials, with min, "
            "max and the spread ratio beside it. A spread ratio well above 1.0 "
            "means this machine's rate varies between identical runs, and a "
            "single-run number from it should not be quoted"
        ),
        "lanes": [summary.as_dict() for summary in summaries],
        "lanes_not_run": {
            "B3": "C++ encode in-memory -- measured by the C++ tools, not this runner",
            "B4": "C++ decode in-memory -- measured by the C++ tools, not this runner",
            "B6": "C++ encode -> framed TCP -> Python decode -- needs the TCP harness",
            "B7": "covered by B5 for the Python path",
        },
    }


def run_ladder(
    source: str | Path,
    *,
    config: Config | None = None,
    targets: Sequence[int] | None = None,
    duration_s: float = 2.0,
    sample_events: int = 20000,
) -> dict[str, Any]:
    """Climb the load ladder, stopping when a rung cannot be sustained.

    Stopping matters: section 10.2 says a higher rung is only meaningful if the
    one below it was stable, so a failed rung ends the climb and the remaining
    targets are reported ``NOT_RUN`` rather than guessed at.
    """
    config = config or Config()
    targets = list(targets or config.benchmark.target_events_per_sec)
    events = _load_events(source, sample_events)

    # Measured first, because a paced rung can only say whether a target *below*
    # the machine's ceiling is sustainable. Without this number, a DEGRADED rung
    # is ambiguous between "the pipeline is slow" and "the target was never
    # reachable here".
    ceiling_trials = [measure_ceiling(events, duration_s, config=config) for _ in range(3)]
    ceiling = max(ceiling_trials, key=lambda s: s.achieved_rate)
    ceiling_rates = sorted(s.achieved_rate for s in ceiling_trials)

    # Reported as a diagnostic only. A rung's verdict is decided by comparing
    # the target against the measured ceiling, which is an observation, rather
    # than against a predicted pacing limit, which proved unreliable.
    granularity = measure_sleep_granularity()
    best_ceiling = ceiling_rates[-1]

    steps: list[LadderStep] = []
    max_sustained: int | None = None
    above_ceiling: list[int] = []
    for target in targets:
        step = run_ladder_step(events, target, duration_s, config=config)
        if step.verdict == "DEGRADED" and target > best_ceiling:
            # Not a pipeline fault at all: the target is above what this machine
            # was measured to do unpaced, so it was never reachable here.
            # Naming that explicitly keeps "could not keep up" separate from
            # "was never possible" (section 10.3).
            step.verdict = "ABOVE_MEASURED_CEILING"
            above_ceiling.append(target)
        steps.append(step)
        if step.verdict == "PASS":
            max_sustained = target
        else:
            break

    not_run = targets[len(steps) :]
    return {
        "environment": environment_metadata(),
        "duration_s_per_step": duration_s,
        "open_loop_ceiling": {
            **ceiling.as_dict(),
            "trials": len(ceiling_rates),
            "achieved_events_per_sec_median": round(ceiling_rates[len(ceiling_rates) // 2], 1),
            "achieved_events_per_sec_min": round(ceiling_rates[0], 1),
            "achieved_events_per_sec_max": round(ceiling_rates[-1], 1),
        },
        "steps": [step.as_dict() for step in steps],
        "targets_not_run": not_run,
        "max_sustained_events_per_sec": max_sustained,
        "max_sustained_verdict": (
            "ESTABLISHED" if max_sustained is not None else "NOT_ESTABLISHED"
        ),
        "targets_above_measured_ceiling": above_ceiling,
        "pacing": {
            "tick_s": PACING_TICK_S,
            "median_sleep_granularity_s": round(granularity, 6),
            "note": (
                "batches are paced one per tick; the granularity figure is a "
                "diagnostic and is not used to decide any verdict"
            ),
        },
        "definition": (
            "max sustained rate = highest paced target where completed "
            "encode+decode+validate reached 95% of target with zero rejections; "
            "higher targets are not attempted once a rung fails. The open-loop "
            "ceiling is the unpaced rate on this machine and bounds every target."
        ),
        "lane_note": (
            "this ladder measures the in-process Python encode+decode+validate "
            "round trip only; it excludes JSON parsing, file I/O, framing and "
            "any network path, so it is not an end-to-end platform throughput "
            "figure"
        ),
    }
