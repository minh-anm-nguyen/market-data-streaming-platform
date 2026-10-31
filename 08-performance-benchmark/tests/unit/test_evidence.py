"""Evidence store, Kafka cuts and probe availability (sections 5, 15, 16; INV-02, INV-07).

Two refusals are the point of this file: an offset can never be asked for as an
identity (INV-02), and a probe that could not read something reports unknown
rather than zero (section 15). The second is tested on every platform by
pointing the cgroup probe at a fixture directory.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from p08_bench.collectors import (
    Availability,
    CgroupProbe,
    ProbeReading,
    ProcessProbe,
    ResourceObserver,
    environment_limitations,
    measure_probe_overhead,
)
from p08_bench.clock import ClockDomain, FakeClock
from p08_bench.errors import ManifestError, MetrologyError
from p08_bench.evidence import (
    REQUIRED_ARTIFACTS,
    ArtifactState,
    EvidenceGap,
    EvidenceStore,
    KafkaCut,
    PartitionCut,
)
from p08_bench.units import Unit


def cut(**overrides) -> PartitionCut:
    base = dict(
        topic="md.ticks.v1", partition=0, generation="gen-1",
        log_start_offset=0, log_end_offset=100, committed_next_offset=90,
    )
    base.update(overrides)
    return PartitionCut(**base)


class TestPartitionCut:
    def test_lag_is_log_end_minus_committed(self):
        assert cut().lag_records == 10

    def test_the_key_carries_the_generation(self):
        # INV-08 / case G01-06: a recreated topic invalidates a stored cut.
        assert cut().key == "md.ticks.v1-0@gen-1"

    def test_a_commit_beyond_the_log_end_is_refused(self):
        # INV-07: that would be progress past what was ever appended.
        with pytest.raises(ManifestError, match="exceeds log end"):
            cut(committed_next_offset=200)

    def test_a_commit_below_the_log_start_is_refused(self):
        # The consumer position refers to data retention already deleted.
        with pytest.raises(ManifestError, match="below log start"):
            cut(log_start_offset=50, committed_next_offset=10)

    def test_a_log_end_below_the_start_is_refused(self):
        with pytest.raises(ManifestError, match="precedes log start"):
            cut(log_start_offset=100, log_end_offset=50, committed_next_offset=100)

    def test_all_three_offsets_are_published(self):
        payload = cut().as_dict()
        for key in ("log_start_offset", "log_end_offset", "committed_next_offset"):
            assert key in payload

    def test_the_dict_states_that_offsets_are_not_identities(self):
        assert "never logical identities" in cut().as_dict()["note"]


class TestKafkaCut:
    def _cut(self) -> KafkaCut:
        sealed = KafkaCut(taken_at_ns=1_000)
        sealed.add(cut(partition=0, committed_next_offset=90))
        sealed.add(cut(partition=1, committed_next_offset=40))
        return sealed

    def test_total_lag_sums_the_partitions(self):
        assert self._cut().total_lag_records == 10 + 60

    def test_the_worst_partition_is_identified(self):
        # INV-09: decisions are taken on the worst partition.
        assert self._cut().worst_lag().partition == 1

    def test_an_empty_cut_has_no_worst_partition(self):
        assert KafkaCut(taken_at_ns=0).worst_lag() is None

    def test_adding_a_partition_twice_is_refused(self):
        sealed = KafkaCut(taken_at_ns=0)
        sealed.add(cut())
        with pytest.raises(ManifestError, match="already present"):
            sealed.add(cut())

    def test_asking_a_cut_for_an_identity_always_raises(self):
        # INV-02, made structural.
        with pytest.raises(MetrologyError, match="never a logical event identity"):
            self._cut().identity()

    def test_the_digest_changes_with_the_offsets(self):
        first = self._cut().digest
        other = KafkaCut(taken_at_ns=1_000)
        other.add(cut(partition=0, committed_next_offset=91))
        other.add(cut(partition=1, committed_next_offset=40))
        assert other.digest != first

    def test_generations_are_collected(self):
        sealed = KafkaCut(taken_at_ns=0)
        sealed.add(cut(partition=0, generation="gen-1"))
        sealed.add(cut(partition=1, generation="gen-2"))
        assert sealed.generations() == {"gen-1", "gen-2"}


class TestEvidenceStore:
    def test_writing_records_a_hash(self, tmp_path: Path):
        store = EvidenceStore(tmp_path / "ev")
        artifact = store.write_json("report", {"a": 1})
        assert len(artifact.sha256) == 64
        assert store.verify()["report"] is ArtifactState.OK

    def test_the_store_is_append_only_by_default(self, tmp_path: Path):
        # Overwriting silently would let a better-looking rerun replace the
        # evidence of an earlier one.
        store = EvidenceStore(tmp_path / "ev")
        store.write_json("report", {"a": 1})
        with pytest.raises(ManifestError, match="append-only"):
            store.write_json("report", {"a": 2})

    def test_overwrite_is_possible_when_asked_for_explicitly(self, tmp_path: Path):
        store = EvidenceStore(tmp_path / "ev")
        store.write_json("report", {"a": 1})
        store.write_json("report", {"a": 2}, overwrite=True)
        assert store.verify()["report"] is ArtifactState.OK

    def test_a_file_edited_after_the_run_reads_as_modified(self, tmp_path: Path):
        # A digest is a check, not a decoration (case G01-04 applied to the
        # evidence bundle itself).
        store = EvidenceStore(tmp_path / "ev")
        artifact = store.write_json("report", {"a": 1})
        Path(artifact.path).write_text('{"a": 2}\n', encoding="utf-8")
        assert store.verify()["report"] is ArtifactState.MODIFIED

    def test_a_deleted_file_reads_as_missing(self, tmp_path: Path):
        store = EvidenceStore(tmp_path / "ev")
        artifact = store.write_json("report", {"a": 1})
        Path(artifact.path).unlink()
        assert store.verify()["report"] is ArtifactState.MISSING

    def test_completeness_is_measured_against_a_requirement_list(self, tmp_path: Path):
        # Case G01-14: a bundle that lost artifacts stays INCOMPLETE, with no
        # reconstruction from a dashboard.
        store = EvidenceStore(tmp_path / "ev")
        store.write_json("report", {"a": 1})
        completeness = store.completeness()
        assert not completeness["complete"]
        assert "run_manifest" in completeness["missing"]

    def test_a_full_bundle_is_complete(self, tmp_path: Path):
        store = EvidenceStore(tmp_path / "ev")
        for name in REQUIRED_ARTIFACTS:
            store.write_json(name, {"name": name})
        assert store.completeness()["complete"]

    def test_a_damaged_artifact_breaks_completeness(self, tmp_path: Path):
        store = EvidenceStore(tmp_path / "ev")
        for name in REQUIRED_ARTIFACTS:
            store.write_json(name, {"name": name})
        Path(store.artifacts["report"].path).write_text("{}", encoding="utf-8")
        completeness = store.completeness()
        assert not completeness["complete"]
        assert "report" in completeness["damaged"]

    def test_a_recorded_gap_is_never_interpolated(self, tmp_path: Path):
        # Case E2E-44.
        store = EvidenceStore(tmp_path / "ev")
        store.record_gap(EvidenceGap("query_p95", 0, 5_000_000_000, "exporter crashed"))
        completeness = store.completeness()
        assert completeness["evidence_interval_has_gaps"]
        assert completeness["gaps"][0]["interpolated"] is False
        assert completeness["gaps"][0]["duration_s"] == pytest.approx(5.0)

    def test_a_cut_is_stored_as_its_own_artifact(self, tmp_path: Path):
        store = EvidenceStore(tmp_path / "ev")
        sealed = KafkaCut(taken_at_ns=0)
        sealed.add(cut())
        store.record_cut("steady_end", sealed)
        manifest = store.artifact_manifest()
        assert "cut_steady_end" in manifest["artifacts"]
        assert manifest["cuts"]["steady_end"] == sealed.digest

    def test_the_artifact_manifest_lists_hashes_and_states(self, tmp_path: Path):
        store = EvidenceStore(tmp_path / "ev")
        store.write_json("report", {"a": 1})
        manifest = store.artifact_manifest()
        assert manifest["artifacts"]["report"]["sha256"]
        assert manifest["states"]["report"] == "OK"


class TestProbeReadings:
    def test_a_measured_reading_needs_a_value(self):
        with pytest.raises(ValueError, match="needs a value"):
            ProbeReading("x", None, Unit.COUNT, Availability.MEASURED)

    def test_an_unavailable_reading_may_not_carry_a_value(self):
        # The pair (availability, value) must be consistent so consumers can
        # rely on it instead of re-checking.
        with pytest.raises(ValueError, match="may not carry a value"):
            ProbeReading("x", 1, Unit.COUNT, Availability.NOT_AVAILABLE, "nope")

    def test_known_follows_availability(self):
        assert ProbeReading("x", 1, Unit.COUNT, Availability.MEASURED).known
        assert not ProbeReading("x", None, Unit.COUNT, Availability.READ_FAILED, "r").known


class TestCgroupProbe:
    def test_a_missing_cpu_stat_is_unknown_not_zero(self, tmp_path: Path):
        # Section 15: an unknown may not default to the passing value. Zero
        # throttling would be a claim nobody measured.
        probe = CgroupProbe(tmp_path, assume_readable=True)
        reading = probe.cpu_throttled_usec()
        assert not reading.known
        assert reading.value is None
        assert reading.availability is Availability.NOT_AVAILABLE

    def test_throttling_is_parsed_when_present(self, tmp_path: Path):
        (tmp_path / "cpu.stat").write_text(
            "usage_usec 100\nnr_throttled 3\nthrottled_usec 4200\n", encoding="utf-8"
        )
        reading = CgroupProbe(tmp_path, assume_readable=True).cpu_throttled_usec()
        assert reading.known
        assert reading.value == 4200

    def test_a_cpu_stat_without_the_line_is_a_read_failure(self, tmp_path: Path):
        (tmp_path / "cpu.stat").write_text("usage_usec 100\n", encoding="utf-8")
        reading = CgroupProbe(tmp_path, assume_readable=True).cpu_throttled_usec()
        assert reading.availability is Availability.READ_FAILED

    def test_oom_kills_are_parsed(self, tmp_path: Path):
        (tmp_path / "memory.events").write_text("low 0\noom 2\noom_kill 1\n", encoding="utf-8")
        reading = CgroupProbe(tmp_path, assume_readable=True).oom_kill_count()
        assert reading.value == 1

    def test_an_unlimited_memory_max_is_reported_as_no_budget(self, tmp_path: Path):
        # "max" means there is no limit to compare peak usage against, which is
        # different from a limit this probe failed to read.
        (tmp_path / "memory.max").write_text("max\n", encoding="utf-8")
        reading = CgroupProbe(tmp_path, assume_readable=True).memory_max_bytes()
        assert not reading.known
        assert "no cgroup memory limit" in reading.reason

    def test_a_numeric_memory_max_is_parsed(self, tmp_path: Path):
        (tmp_path / "memory.max").write_text("2147483648\n", encoding="utf-8")
        assert CgroupProbe(tmp_path, assume_readable=True).memory_max_bytes().value == 2147483648

    def test_psi_pressure_is_parsed(self, tmp_path: Path):
        (tmp_path / "cpu.pressure").write_text(
            "some avg10=1.25 avg60=0.50 avg300=0.10 total=123\n", encoding="utf-8"
        )
        readings = CgroupProbe(tmp_path, assume_readable=True).pressure()
        assert readings["psi_cpu_some_avg10"].value == pytest.approx(1.25)

    def test_every_reading_is_produced_even_when_all_are_unknown(self, tmp_path: Path):
        readings = CgroupProbe(tmp_path, assume_readable=True).readings()
        assert len(readings) == 7
        assert all(not r.known for r in readings)


class TestObserver:
    def test_unknown_readings_leave_holes_not_zeros(self, tmp_path: Path):
        observer = ResourceObserver(cgroup=CgroupProbe(tmp_path, assume_readable=True))
        observer.scrape(0)
        assert "cgroup_cpu_throttled_usec" not in observer.series
        assert observer.failures["cgroup_cpu_throttled_usec"] == 1

    def test_unverifiable_budgets_are_reported_as_blockers(self, tmp_path: Path):
        # NFR-07 asks for no OOM and no exceeded budget; where the probe could
        # not read it, the budget is unverified rather than met.
        observer = ResourceObserver(cgroup=CgroupProbe(tmp_path, assume_readable=True))
        observer.scrape(0)
        blockers = observer.budget_blockers()
        assert any("cgroup_oom_kill_count is unknown" in b for b in blockers)

    def test_measured_readings_build_a_series(self, tmp_path: Path):
        (tmp_path / "memory.current").write_text("1000\n", encoding="utf-8")
        observer = ResourceObserver(cgroup=CgroupProbe(tmp_path, assume_readable=True))
        observer.scrape(0)
        (tmp_path / "memory.current").write_text("2000\n", encoding="utf-8")
        observer.scrape(1_000_000_000)
        assert observer.peak("cgroup_memory_current_bytes") == 2000
        assert len(observer.series["cgroup_memory_current_bytes"]) == 2

    def test_the_observer_measures_its_own_cost(self):
        # ADR-09: a low-overhead claim has to be evidenced.
        clock = FakeClock(ClockDomain("bench", "observer"))

        def probe():
            clock.advance_ns(1_000)
            return []

        overhead = measure_probe_overhead(probe, clock, scrapes=5)
        assert overhead.total_ns == 5_000
        assert overhead.mean_ns == 1_000
        assert overhead.fraction_of(1_000_000) == pytest.approx(0.005)

    def test_overhead_fraction_of_a_zero_window_is_undefined(self):
        clock = FakeClock(ClockDomain("bench", "observer"))
        overhead = measure_probe_overhead(lambda: [], clock, scrapes=1)
        assert overhead.fraction_of(0) is None

    def test_as_dict_carries_unknowns_and_blockers(self, tmp_path: Path):
        observer = ResourceObserver(cgroup=CgroupProbe(tmp_path, assume_readable=True))
        observer.scrape(0)
        payload = observer.as_dict()
        assert payload["unknown_readings"]
        assert payload["budget_blockers"]


class TestProcessProbeAndPlatform:
    def test_the_process_probe_either_measures_or_explains(self):
        readings = ProcessProbe().readings()
        for reading in readings:
            assert reading.known or reading.reason

    def test_platform_limitations_are_stated_rather_than_omitted(self):
        # A run on a non-Linux host cannot evidence cgroup or perf facts, and
        # the report says so instead of leaving the fields quietly null.
        import sys

        notes = environment_limitations()
        if not sys.platform.startswith("linux"):
            assert any("cgroup v2" in note for note in notes)
        else:
            assert isinstance(notes, list)
