"""Host, cgroup and process probes, and the honesty about what they cannot read.

Section 5.1 lists the environment facts a reproducible run needs, and then adds
the caveat that makes this module awkward: ``perf`` may be restricted by
``perf_event_paranoid``, PMU counters may be multiplexed, namespaces may hide
things, and lowering host security to obtain a metric is not allowed. Section
5.2 adds that the probes themselves must not be heavy enough to change the
thing being measured.

So every probe here returns a :class:`ProbeReading` that is either a value or a
stated reason for its absence. There is no fallback to zero. A missing
``cpu.stat`` means cgroup throttling is **unknown**, and unknown has to stay
unknown all the way into the verdict (section 15) -- a zero would read as
"measured, no throttling", which is the opposite claim.

The cgroup v2 files are Linux-only. On any other platform the readings come
back ``NOT_AVAILABLE`` with the platform as the reason, because a benchmark run
on Windows genuinely cannot evidence Linux cgroup limits and should say so
rather than omitting the question.

:func:`measure_probe_overhead` implements ADR-09: the observer measures its own
cost so a claim about low overhead is itself evidenced.
"""

from __future__ import annotations

import os
import platform
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from .clock import FakeClock, MonotonicClock
from .timeseries import WindowedSeries
from .units import Unit

#: cgroup v2 paths section 5.1 names. Read-only, and only on Linux.
CGROUP_V2_ROOT = Path("/sys/fs/cgroup")
CPU_STAT = "cpu.stat"
MEMORY_EVENTS = "memory.events"
MEMORY_CURRENT = "memory.current"
MEMORY_MAX = "memory.max"
IO_STAT = "io.stat"
PRESSURE_FILES = ("cpu.pressure", "memory.pressure", "io.pressure")

PERF_EVENT_PARANOID = Path("/proc/sys/kernel/perf_event_paranoid")


class Availability(str, Enum):
    """Why a reading is or is not present."""

    MEASURED = "MEASURED"
    #: The platform does not have this facility at all.
    NOT_AVAILABLE = "NOT_AVAILABLE"
    #: It exists but this process may not read it.
    PERMISSION_DENIED = "PERMISSION_DENIED"
    #: An optional dependency is absent.
    DEPENDENCY_MISSING = "DEPENDENCY_MISSING"
    #: The read was attempted and failed.
    READ_FAILED = "READ_FAILED"


@dataclass(frozen=True)
class ProbeReading:
    """One metric: a value, or a reason there is none.

    Never both, and never a default. ``value is None`` with
    ``availability is MEASURED`` is impossible by construction, so a consumer
    can rely on the pair.
    """

    name: str
    value: float | int | str | None
    unit: Unit
    availability: Availability
    reason: str = ""

    def __post_init__(self) -> None:
        if self.availability is Availability.MEASURED and self.value is None:
            raise ValueError(f"{self.name}: a measured reading needs a value")
        if self.availability is not Availability.MEASURED and self.value is not None:
            raise ValueError(f"{self.name}: an unavailable reading may not carry a value")

    @property
    def known(self) -> bool:
        return self.availability is Availability.MEASURED

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "unit": self.unit.value,
            "availability": self.availability.value,
            "reason": self.reason,
        }


def unavailable(
    name: str, unit: Unit, availability: Availability, reason: str
) -> ProbeReading:
    return ProbeReading(name, None, unit, availability, reason)


def _read_text(path: Path, name: str, unit: Unit) -> ProbeReading | str:
    """Read a file, or describe why not. Returns the text on success."""
    if not sys.platform.startswith("linux"):
        return unavailable(
            name, unit, Availability.NOT_AVAILABLE,
            f"{path} is a Linux cgroup/proc file; this host is {platform.system()}",
        )
    try:
        return path.read_text(encoding="utf-8")
    except PermissionError as exc:
        return unavailable(name, unit, Availability.PERMISSION_DENIED, str(exc))
    except FileNotFoundError:
        return unavailable(
            name, unit, Availability.NOT_AVAILABLE, f"{path} does not exist on this host"
        )
    except OSError as exc:
        return unavailable(name, unit, Availability.READ_FAILED, str(exc))


class CgroupProbe:
    """cgroup v2 readings: throttling, memory pressure, OOM events.

    ``root`` is injectable so tests can point it at a directory of fixture
    files. Without that the throttling tests would only run on Linux, and the
    "unknown stays unknown" behaviour is exactly what needs testing everywhere.
    """

    __slots__ = ("root", "_force_linux")

    def __init__(self, root: Path = CGROUP_V2_ROOT, *, assume_readable: bool = False) -> None:
        self.root = root
        self._force_linux = assume_readable

    def _read(self, filename: str, name: str, unit: Unit) -> ProbeReading | str:
        path = self.root / filename
        if self._force_linux:
            try:
                return path.read_text(encoding="utf-8")
            except PermissionError as exc:
                return unavailable(name, unit, Availability.PERMISSION_DENIED, str(exc))
            except FileNotFoundError:
                return unavailable(
                    name, unit, Availability.NOT_AVAILABLE, f"{path} does not exist"
                )
            except OSError as exc:
                return unavailable(name, unit, Availability.READ_FAILED, str(exc))
        return _read_text(path, name, unit)

    def cpu_throttled_usec(self) -> ProbeReading:
        """Microseconds this cgroup spent throttled (case E2E-16).

        The number that turns "CPU is at 90%" into a diagnosis. Section 12 is
        explicit that a CPU-bound conclusion without throttling data is not
        supported by the evidence.
        """
        name, unit = "cgroup_cpu_throttled_usec", Unit.COUNT
        text = self._read(CPU_STAT, name, unit)
        if isinstance(text, ProbeReading):
            return text
        for line in text.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == "throttled_usec":
                return ProbeReading(name, int(parts[1]), unit, Availability.MEASURED)
        return unavailable(
            name, unit, Availability.READ_FAILED, "cpu.stat has no throttled_usec line"
        )

    def oom_kill_count(self) -> ProbeReading:
        """OOM kills seen by this cgroup (NFR-07)."""
        name, unit = "cgroup_oom_kill_count", Unit.COUNT
        text = self._read(MEMORY_EVENTS, name, unit)
        if isinstance(text, ProbeReading):
            return text
        for line in text.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == "oom_kill":
                return ProbeReading(name, int(parts[1]), unit, Availability.MEASURED)
        return unavailable(
            name, unit, Availability.READ_FAILED, "memory.events has no oom_kill line"
        )

    def memory_current_bytes(self) -> ProbeReading:
        name, unit = "cgroup_memory_current_bytes", Unit.BYTES
        text = self._read(MEMORY_CURRENT, name, unit)
        if isinstance(text, ProbeReading):
            return text
        try:
            return ProbeReading(name, int(text.strip()), unit, Availability.MEASURED)
        except ValueError as exc:
            return unavailable(name, unit, Availability.READ_FAILED, str(exc))

    def memory_max_bytes(self) -> ProbeReading:
        """The cgroup memory limit, or ``max`` meaning no limit is set."""
        name, unit = "cgroup_memory_max_bytes", Unit.BYTES
        text = self._read(MEMORY_MAX, name, unit)
        if isinstance(text, ProbeReading):
            return text
        raw = text.strip()
        if raw == "max":
            return unavailable(
                name, unit, Availability.NOT_AVAILABLE,
                "memory.max is 'max': no cgroup memory limit is configured, so "
                "there is no budget to compare peak usage against",
            )
        try:
            return ProbeReading(name, int(raw), unit, Availability.MEASURED)
        except ValueError as exc:
            return unavailable(name, unit, Availability.READ_FAILED, str(exc))

    def pressure(self) -> dict[str, ProbeReading]:
        """PSI ``some avg10`` for cpu, memory and io."""
        out: dict[str, ProbeReading] = {}
        for filename in PRESSURE_FILES:
            name = f"psi_{filename.split('.')[0]}_some_avg10"
            text = self._read(filename, name, Unit.RATIO)
            if isinstance(text, ProbeReading):
                out[name] = text
                continue
            reading = unavailable(
                name, Unit.RATIO, Availability.READ_FAILED, f"no 'some' line in {filename}"
            )
            for line in text.splitlines():
                if line.startswith("some "):
                    for token in line.split()[1:]:
                        key, _, value = token.partition("=")
                        if key == "avg10":
                            reading = ProbeReading(
                                name, float(value), Unit.RATIO, Availability.MEASURED
                            )
                            break
            out[name] = reading
        return out

    def readings(self) -> list[ProbeReading]:
        return [
            self.cpu_throttled_usec(),
            self.oom_kill_count(),
            self.memory_current_bytes(),
            self.memory_max_bytes(),
            *self.pressure().values(),
        ]


class ProcessProbe:
    """Per-process RSS and CPU, via psutil when it is installed.

    psutil is an optional test dependency, so its absence is reported as
    ``DEPENDENCY_MISSING`` rather than crashing a run. A benchmark that cannot
    read memory should say the memory budget is unverified (NFR-07), not fail
    to start.
    """

    __slots__ = ("pid", "_process", "_import_error")

    def __init__(self, pid: int | None = None) -> None:
        self.pid = pid if pid is not None else os.getpid()
        self._process = None
        self._import_error = ""
        try:
            import psutil

            self._process = psutil.Process(self.pid)
        except ImportError as exc:
            self._import_error = str(exc)
        except Exception as exc:  # noqa: BLE001 - a dead pid is a datum
            self._import_error = f"{type(exc).__name__}: {exc}"

    def _missing(self, name: str, unit: Unit) -> ProbeReading:
        return unavailable(
            name, unit, Availability.DEPENDENCY_MISSING,
            self._import_error or "psutil is not installed",
        )

    def rss_bytes(self) -> ProbeReading:
        name, unit = "process_rss_bytes", Unit.BYTES
        if self._process is None:
            return self._missing(name, unit)
        try:
            return ProbeReading(
                name, int(self._process.memory_info().rss), unit, Availability.MEASURED
            )
        except Exception as exc:  # noqa: BLE001
            return unavailable(name, unit, Availability.READ_FAILED, str(exc))

    def cpu_times(self) -> dict[str, ProbeReading]:
        if self._process is None:
            return {
                "process_cpu_user_s": self._missing("process_cpu_user_s", Unit.SECONDS),
                "process_cpu_system_s": self._missing("process_cpu_system_s", Unit.SECONDS),
            }
        try:
            times = self._process.cpu_times()
            return {
                "process_cpu_user_s": ProbeReading(
                    "process_cpu_user_s", float(times.user), Unit.SECONDS, Availability.MEASURED
                ),
                "process_cpu_system_s": ProbeReading(
                    "process_cpu_system_s", float(times.system), Unit.SECONDS, Availability.MEASURED
                ),
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "process_cpu_user_s": unavailable(
                    "process_cpu_user_s", Unit.SECONDS, Availability.READ_FAILED, str(exc)
                ),
                "process_cpu_system_s": unavailable(
                    "process_cpu_system_s", Unit.SECONDS, Availability.READ_FAILED, str(exc)
                ),
            }

    def readings(self) -> list[ProbeReading]:
        return [self.rss_bytes(), *self.cpu_times().values()]


def perf_availability() -> ProbeReading:
    """Whether ``perf`` can read PMU counters here (section 5.1).

    A restricted ``perf_event_paranoid`` is a recorded limitation, not something
    to work around: section 5.1 forbids lowering host security to obtain a
    metric.
    """
    name, unit = "perf_event_paranoid", Unit.COUNT
    text = _read_text(PERF_EVENT_PARANOID, name, unit)
    if isinstance(text, ProbeReading):
        return text
    try:
        return ProbeReading(name, int(text.strip()), unit, Availability.MEASURED)
    except ValueError as exc:
        return unavailable(name, unit, Availability.READ_FAILED, str(exc))


@dataclass
class ProbeOverhead:
    """What the observer cost to run (ADR-09, group G28)."""

    scrapes: int
    total_ns: int
    clock_domain: str

    @property
    def mean_ns(self) -> float | None:
        return self.total_ns / self.scrapes if self.scrapes else None

    def fraction_of(self, window_ns: int) -> float | None:
        if window_ns <= 0:
            return None
        return self.total_ns / window_ns

    def as_dict(self) -> dict[str, Any]:
        return {
            "scrapes": self.scrapes,
            "total_ns": self.total_ns,
            "mean_ns": self.mean_ns,
            "clock_domain": self.clock_domain,
        }


def measure_probe_overhead(
    probe: Callable[[], Iterable[ProbeReading]],
    clock: MonotonicClock | FakeClock,
    *,
    scrapes: int = 10,
) -> ProbeOverhead:
    """Time ``scrapes`` calls to the probe so its cost is evidenced, not asserted."""
    total = 0
    for _ in range(max(scrapes, 0)):
        start = clock.now_ns()
        list(probe())
        total += clock.now_ns() - start
    return ProbeOverhead(scrapes=scrapes, total_ns=total, clock_domain=clock.domain.name)


@dataclass
class ResourceObserver:
    """Collects readings into series and records the gaps.

    A failed scrape increments ``failures`` and adds nothing to the series. That
    is the difference between a series with a hole in it and a series with a
    zero in it, and case E2E-44 is about exactly that difference.
    """

    cgroup: CgroupProbe = field(default_factory=CgroupProbe)
    process: ProcessProbe = field(default_factory=ProcessProbe)
    series: dict[str, WindowedSeries] = field(default_factory=dict)
    failures: dict[str, int] = field(default_factory=dict)
    unknown: dict[str, str] = field(default_factory=dict)

    def scrape(self, t_ns: int) -> list[ProbeReading]:
        readings = [*self.cgroup.readings(), *self.process.readings(), perf_availability()]
        for reading in readings:
            if reading.known and isinstance(reading.value, (int, float)):
                series = self.series.get(reading.name)
                if series is None:
                    series = WindowedSeries(reading.name, reading.unit)
                    self.series[reading.name] = series
                series.add(t_ns, float(reading.value))
            else:
                self.failures[reading.name] = self.failures.get(reading.name, 0) + 1
                self.unknown[reading.name] = (
                    f"{reading.availability.value}: {reading.reason}"
                )
        return readings

    def peak(self, name: str) -> float | None:
        series = self.series.get(name)
        if series is None or not len(series):
            return None
        return max(point.value for point in series.points)

    def budget_blockers(self) -> list[str]:
        """Resource budgets that cannot be evidenced on this host.

        NFR-07 asks for no OOM, no silent drops and no exceeded cgroup budget.
        Where the probe could not read the number, the budget is unverified and
        the run has to say so rather than claim it was met.
        """
        out: list[str] = []
        for name in ("cgroup_oom_kill_count", "cgroup_cpu_throttled_usec", "cgroup_memory_max_bytes"):
            if name in self.unknown:
                out.append(f"{name} is unknown ({self.unknown[name]}); budget unverified")
        return out

    def as_dict(self) -> dict[str, Any]:
        return {
            "series": {name: s.as_dict() for name, s in sorted(self.series.items())},
            "peaks": {name: self.peak(name) for name in sorted(self.series)},
            "scrape_failures": dict(sorted(self.failures.items())),
            "unknown_readings": dict(sorted(self.unknown.items())),
            "budget_blockers": self.budget_blockers(),
        }


def environment_limitations() -> list[str]:
    """Platform facts that bound what this host can evidence.

    Printed in every report. A P08 run on Windows cannot produce cgroup or perf
    evidence, and the honest move is to name that in the output rather than to
    leave the corresponding report fields quietly null.
    """
    notes: list[str] = []
    if not sys.platform.startswith("linux"):
        notes.append(
            f"host platform is {platform.system()}: cgroup v2, PSI and perf "
            "counters named in section 5.1 are not readable, so CPU throttling, "
            "OOM events and PMU evidence are unavailable"
        )
    try:
        import psutil  # noqa: F401
    except ImportError:
        notes.append("psutil is not installed: per-process RSS and CPU are unavailable")
    return notes
