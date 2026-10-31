"""Provenance, seals and drift (sections 3.1, 5.1; group G01).

Each class here maps onto specific G01 cases: a flipped byte (G01-04), a
``.part`` source (G01-03), a reused ``run_id`` (G01-02), a repointed image tag
(G01-09), a producer config changed between A and B (G01-08), and a topic
recreated under the same name (G01-06).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from p08_bench.errors import ManifestError
from p08_bench.manifest import (
    Drift,
    DriftSeverity,
    HostProfile,
    QueryCatalogEntry,
    RunIdRegistry,
    RunManifest,
    SealState,
    SourceSeal,
    TopicGeneration,
    canonical_json,
    digest_of,
    query_catalog_digest,
    sha256_bytes,
    sha256_file,
    sha256_text,
)


def write_p01_run(root: Path, *, events: str = '{"a":1}\n', completed: bool = True,
                  part_only: bool = False, hash_override: str | None = None,
                  count: int | None = 1, marker_bytes: int | None = None) -> Path:
    """Build a P01-shaped run directory.

    Mirrors the real layout, including the distinction that matters: the
    manifest carries ``data_sha256`` (P01's *config* digest, which ``dataset_id``
    derives from) while the ``COMPLETED`` marker carries ``events_sha256`` --
    the hash of the bytes -- plus the committed line and byte counts.
    """
    root.mkdir(parents=True, exist_ok=True)
    name = "events.jsonl.part" if part_only else "events.jsonl"
    # Bytes, not text: write_text would translate newlines on Windows and the
    # recorded hash would never match the file.
    payload = events.encode("utf-8")
    (root / name).write_bytes(payload)
    manifest = {
        "dataset_id": "ds-test",
        # A config digest, deliberately unrelated to the file contents.
        "data_sha256": "c0" * 32,
        "planned_logical_slots": count,
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    if completed and not part_only:
        marker = {
            "run_instance_id": root.name,
            "events_sha256": hash_override or sha256_bytes(payload),
            "events_lines": count,
            "events_bytes": marker_bytes if marker_bytes is not None else len(payload),
            "committed_at_utc": "2026-10-30T10:20:00Z",
        }
        (root / "COMPLETED").write_text(json.dumps(marker), encoding="utf-8")
    return root


class TestHashing:
    def test_canonical_json_sorts_keys(self):
        assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'

    def test_key_order_does_not_change_the_digest(self):
        assert digest_of({"a": 1, "b": 2}) == digest_of({"b": 2, "a": 1})

    def test_a_value_change_does_change_the_digest(self):
        assert digest_of({"a": 1}) != digest_of({"a": 2})

    def test_sha256_of_bytes_and_text_agree(self):
        assert sha256_bytes(b"abc") == sha256_text("abc")

    def test_file_hashing_streams(self, tmp_path: Path):
        path = tmp_path / "big.bin"
        path.write_bytes(b"x" * (3 << 20))
        assert sha256_file(path) == sha256_bytes(b"x" * (3 << 20))


class TestSourceSeal:
    def test_a_committed_run_is_sealed(self, tmp_path: Path):
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "run"))
        assert seal.state is SealState.SEALED
        assert seal.sealed
        assert seal.usable_for_completeness_claim

    def test_a_part_file_is_incomplete_not_corrupt(self, tmp_path: Path):
        # Case G01-03: a generator stopped early leaves a .part file, and the
        # expected set derived from it would be short.
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "run", part_only=True))
        assert seal.state is SealState.INCOMPLETE
        assert not seal.usable_for_completeness_claim
        assert "never committed" in seal.reasons[0]

    def test_no_completed_marker_is_incomplete(self, tmp_path: Path):
        # The bytes may be fine and the writer may still have had buffered
        # events; P01 commits with the marker, so no marker is no dataset.
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "run", completed=False))
        assert seal.state is SealState.INCOMPLETE
        assert "COMPLETED marker" in seal.reasons[0]

    def test_a_flipped_byte_is_corrupt(self, tmp_path: Path):
        # Case G01-04: one byte changed after the seal.
        run = write_p01_run(tmp_path / "run", hash_override="0" * 64)
        seal = SourceSeal.from_p01_run_dir(run)
        assert seal.state is SealState.CORRUPT
        assert not seal.usable_for_completeness_claim
        assert "does not match" in seal.reasons[0]

    def test_a_truncated_file_is_detected_through_the_hash(self, tmp_path: Path):
        # Case G01-11: removing the last sealed event keeps the file valid JSONL
        # and breaks the committed hash, which is the point of hashing it.
        run = write_p01_run(tmp_path / "run", events='{"a":1}\n{"b":2}\n', count=2)
        (run / "events.jsonl").write_bytes(b'{"a":1}\n')
        assert SourceSeal.from_p01_run_dir(run).state is SealState.CORRUPT

    def test_a_missing_manifest_is_missing(self, tmp_path: Path):
        (tmp_path / "empty").mkdir()
        seal = SourceSeal.from_p01_run_dir(tmp_path / "empty")
        assert seal.state is SealState.MISSING

    def test_an_unreadable_manifest_is_corrupt(self, tmp_path: Path):
        run = tmp_path / "run"
        run.mkdir()
        (run / "manifest.json").write_text("{not json", encoding="utf-8")
        assert SourceSeal.from_p01_run_dir(run).state is SealState.CORRUPT

    def test_a_missing_events_file_is_missing(self, tmp_path: Path):
        run = write_p01_run(tmp_path / "run")
        (run / "events.jsonl").unlink()
        assert SourceSeal.from_p01_run_dir(run).state is SealState.MISSING

    def test_a_sealed_source_without_a_count_cannot_anchor_completeness(self, tmp_path: Path):
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "run", count=None))
        assert seal.sealed
        assert not seal.usable_for_completeness_claim

    def test_the_file_hash_and_the_dataset_digest_are_separate(self, tmp_path: Path):
        # P01 keeps these apart and so must P08: data_sha256 is the config
        # digest dataset_id derives from, events_sha256 is the bytes. Comparing
        # the file against the config digest fails every honest run.
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "run"))
        assert seal.sealed
        assert seal.dataset_digest == "c0" * 32
        assert seal.sha256 != seal.dataset_digest

    def test_the_event_count_comes_from_the_commit_marker(self, tmp_path: Path):
        # Section 4.1: the manifest records what was asked for, the marker
        # records what was durably written, and only the second can anchor a
        # completeness claim.
        run = write_p01_run(tmp_path / "run", events='{"a":1}\n{"b":2}\n', count=2)
        seal = SourceSeal.from_p01_run_dir(run)
        assert seal.event_count == 2
        assert seal.bytes_sealed == 16

    def test_an_unreadable_commit_marker_is_corrupt(self, tmp_path: Path):
        run = write_p01_run(tmp_path / "run")
        (run / "COMPLETED").write_text("{not json", encoding="utf-8")
        assert SourceSeal.from_p01_run_dir(run).state is SealState.CORRUPT

    def test_a_marker_without_a_hash_cannot_seal(self, tmp_path: Path):
        run = write_p01_run(tmp_path / "run")
        (run / "COMPLETED").write_text(json.dumps({"events_lines": 1}), encoding="utf-8")
        seal = SourceSeal.from_p01_run_dir(run)
        assert seal.state is SealState.INCOMPLETE
        assert "no events_sha256" in seal.reasons[0]

    def test_a_byte_count_disagreement_is_corrupt(self, tmp_path: Path):
        run = write_p01_run(tmp_path / "run", marker_bytes=999)
        seal = SourceSeal.from_p01_run_dir(run)
        assert seal.state is SealState.CORRUPT
        assert "byte count" in seal.reasons[0]


class TestTopicGeneration:
    def test_the_key_includes_the_generation(self):
        # INV-08 / case G01-06: a recreated topic has offsets that mean nothing
        # relative to the old one.
        assert TopicGeneration("md.ticks.v1", "gen-2", 8).key == "md.ticks.v1@gen-2"

    def test_two_generations_of_one_topic_are_different_keys(self):
        a = TopicGeneration("md.ticks.v1", "gen-1", 8)
        b = TopicGeneration("md.ticks.v1", "gen-2", 8)
        assert a.key != b.key

    def test_an_empty_generation_is_refused(self):
        with pytest.raises(ManifestError, match="generation"):
            TopicGeneration("md.ticks.v1", "", 8)

    def test_zero_partitions_is_refused(self):
        with pytest.raises(ManifestError, match="partitions"):
            TopicGeneration("md.ticks.v1", "gen-1", 0)


class TestQueryCatalog:
    def _entry(self, sql: str = "SELECT 1", qid: str = "Q02") -> QueryCatalogEntry:
        return QueryCatalogEntry(qid=qid, sql=sql, binds={"symbol": "EURUSD"}, client_timeout_ms=2000)

    def test_the_digest_covers_the_sql(self):
        # Case G01-13: Q02 selected, Q01 executed.
        assert self._entry("SELECT 1").digest != self._entry("SELECT 2").digest

    def test_the_digest_covers_the_binds(self):
        a = QueryCatalogEntry("Q02", "SELECT 1", {"symbol": "EURUSD"}, 2000)
        b = QueryCatalogEntry("Q02", "SELECT 1", {"symbol": "GBPUSD"}, 2000)
        assert a.digest != b.digest

    def test_the_digest_covers_the_settings(self):
        a = QueryCatalogEntry("Q02", "SELECT 1", {}, 2000, settings={"max_threads": 4})
        b = QueryCatalogEntry("Q02", "SELECT 1", {}, 2000, settings={"max_threads": 8})
        assert a.digest != b.digest

    def test_catalog_digest_is_order_independent(self):
        a, b = self._entry(qid="Q01"), self._entry(qid="Q02")
        assert query_catalog_digest([a, b]) == query_catalog_digest([b, a])

    def test_catalog_digest_changes_when_an_entry_changes(self):
        base = [self._entry(qid="Q01"), self._entry(qid="Q02")]
        changed = [self._entry(qid="Q01"), self._entry("SELECT 3", qid="Q02")]
        assert query_catalog_digest(base) != query_catalog_digest(changed)

    def test_the_sql_text_is_kept_for_the_report(self):
        assert self._entry().sql == "SELECT 1"


class TestHostProfile:
    def test_unknown_fields_default_to_none_and_are_listed(self):
        # Case E2E-45 needs the governor to be a recorded field; an unknown one
        # has to read as unknown rather than as a plausible default.
        profile = HostProfile(cpu_model="Xeon")
        assert profile.cpu_governor is None
        assert "cpu_governor" in profile.unknown_fields

    def test_a_profile_with_holes_still_has_a_stable_digest(self):
        assert HostProfile(cpu_model="Xeon").digest == HostProfile(cpu_model="Xeon").digest

    def test_changing_the_governor_changes_the_digest(self):
        a = HostProfile(cpu_model="Xeon", cpu_governor="performance")
        b = HostProfile(cpu_model="Xeon", cpu_governor="powersave")
        assert a.digest != b.digest

    def test_the_digest_is_not_part_of_the_hashed_payload(self):
        payload = HostProfile().as_dict(include_digest=False)
        assert "digest" not in payload


class TestRunManifestDrift:
    def _manifest(self, tmp_path: Path, **overrides) -> RunManifest:
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "src"))
        defaults = dict(
            run_id="P08-A", test_id="E2E-02", lane="L01", seed=1,
            profile="LAB_CAPACITY", created_at_utc="2026-10-30T10:20:00Z",
            workload_sha256="w" * 64, config_sha256="c" * 64,
            dataset_seal=seal, host_profile_sha256="h" * 64,
            topic_generations=(TopicGeneration("md.ticks.v1", "gen-1", 8),),
            query_catalog_sha256="q" * 64,
            image_digests={"clickhouse": "sha256:aaa"},
            source_commit="abc123",
        )
        defaults.update(overrides)
        return RunManifest(**defaults)

    def test_an_identical_manifest_has_no_drift(self, tmp_path: Path):
        a, b = self._manifest(tmp_path), self._manifest(tmp_path)
        assert a.drift_against(b) == []
        assert a.comparable_with(b)

    def test_a_changed_dataset_is_invalidating(self, tmp_path: Path):
        # Case G01-01: dataset_id edited between warmup and steady.
        other_seal = SourceSeal.from_p01_run_dir(
            write_p01_run(tmp_path / "src2", events='{"other":1}\n')
        )
        drifts = self._manifest(tmp_path).drift_against(
            self._manifest(tmp_path, dataset_seal=other_seal)
        )
        severities = {d.field_name: d.severity for d in drifts}
        assert severities["dataset_sha256"] is DriftSeverity.INVALIDATING

    def test_a_changed_seed_is_invalidating(self, tmp_path: Path):
        drifts = self._manifest(tmp_path).drift_against(self._manifest(tmp_path, seed=2))
        assert drifts[0].severity is DriftSeverity.INVALIDATING

    def test_a_changed_query_catalog_is_invalidating(self, tmp_path: Path):
        drifts = self._manifest(tmp_path).drift_against(
            self._manifest(tmp_path, query_catalog_sha256="z" * 64)
        )
        assert drifts[0].field_name == "query_catalog_sha256"
        assert drifts[0].severity is DriftSeverity.INVALIDATING

    def test_a_recreated_topic_is_invalidating(self, tmp_path: Path):
        drifts = self._manifest(tmp_path).drift_against(
            self._manifest(
                tmp_path, topic_generations=(TopicGeneration("md.ticks.v1", "gen-2", 8),)
            )
        )
        assert drifts[0].field_name == "topic_generations"
        assert drifts[0].severity is DriftSeverity.INVALIDATING

    def test_a_changed_producer_config_is_incomparable_not_invalid(self, tmp_path: Path):
        # Case G01-08: both runs are individually fine; they are not an A/B pair.
        drifts = self._manifest(tmp_path).drift_against(
            self._manifest(tmp_path, config_sha256="d" * 64)
        )
        assert drifts[0].severity is DriftSeverity.INCOMPARABLE

    def test_a_repointed_image_tag_is_incomparable(self, tmp_path: Path):
        # Case G01-09: same tag, different digest.
        drifts = self._manifest(tmp_path).drift_against(
            self._manifest(tmp_path, image_digests={"clickhouse": "sha256:bbb"})
        )
        assert drifts[0].field_name == "image_digests"
        assert drifts[0].severity is DriftSeverity.INCOMPARABLE

    def test_a_different_host_profile_confounds_an_ab_pair(self, tmp_path: Path):
        # Case E2E-45: the governor swapped for the B arm only.
        drifts = self._manifest(tmp_path).drift_against(
            self._manifest(tmp_path, host_profile_sha256="x" * 64)
        )
        assert drifts[0].severity is DriftSeverity.INCOMPARABLE
        assert not self._manifest(tmp_path).comparable_with(
            self._manifest(tmp_path, host_profile_sha256="x" * 64)
        )

    def test_a_different_lane_is_only_informational(self, tmp_path: Path):
        a = self._manifest(tmp_path)
        b = self._manifest(tmp_path, lane="L04")
        assert a.drift_against(b)[0].severity is DriftSeverity.INFORMATIONAL
        assert a.comparable_with(b)

    def test_drift_is_sorted_and_serialisable(self, tmp_path: Path):
        drifts = self._manifest(tmp_path).drift_against(
            self._manifest(tmp_path, seed=2, lane="L04")
        )
        assert [d.field_name for d in drifts] == sorted(d.field_name for d in drifts)
        assert Drift("f", 1, 2, DriftSeverity.INFORMATIONAL, "d").as_dict()["severity"] == "INFORMATIONAL"


class TestRunManifestValidity:
    def test_a_sealed_source_leaves_no_blockers(self, tmp_path: Path):
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "src"))
        manifest = RunManifest(
            run_id="r", test_id="t", lane="L01", seed=1, profile="LOCAL_SMALL",
            created_at_utc="2026-10-30T10:20:00Z", workload_sha256="w",
            config_sha256="c", dataset_seal=seal, host_profile_sha256="h",
        )
        assert manifest.validity_blockers() == []

    def test_an_unsealed_source_blocks_the_run(self, tmp_path: Path):
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "src", part_only=True))
        manifest = RunManifest(
            run_id="r", test_id="t", lane="L01", seed=1, profile="LOCAL_SMALL",
            created_at_utc="2026-10-30T10:20:00Z", workload_sha256="w",
            config_sha256="c", dataset_seal=seal, host_profile_sha256="h",
        )
        blockers = manifest.validity_blockers()
        assert any("INCOMPLETE" in b for b in blockers)

    def test_a_missing_host_profile_blocks_reproducibility(self, tmp_path: Path):
        # FR-15: artifacts must be enough to rerun.
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "src"))
        manifest = RunManifest(
            run_id="r", test_id="t", lane="L01", seed=1, profile="LOCAL_SMALL",
            created_at_utc="2026-10-30T10:20:00Z", workload_sha256="w",
            config_sha256="c", dataset_seal=seal, host_profile_sha256="",
        )
        assert any("host profile" in b for b in manifest.validity_blockers())

    def test_an_empty_run_id_is_refused(self, tmp_path: Path):
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "src"))
        with pytest.raises(ManifestError, match="run_id"):
            RunManifest(
                run_id=" ", test_id="t", lane="L01", seed=1, profile="LOCAL_SMALL",
                created_at_utc="now", workload_sha256="w", config_sha256="c",
                dataset_seal=seal, host_profile_sha256="h",
            )

    def test_an_empty_test_id_is_refused(self, tmp_path: Path):
        seal = SourceSeal.from_p01_run_dir(write_p01_run(tmp_path / "src"))
        with pytest.raises(ManifestError, match="test_id"):
            RunManifest(
                run_id="r", test_id="", lane="L01", seed=1, profile="LOCAL_SMALL",
                created_at_utc="now", workload_sha256="w", config_sha256="c",
                dataset_seal=seal, host_profile_sha256="h",
            )


class TestRunIdRegistry:
    def test_a_fresh_run_id_is_reserved(self, tmp_path: Path):
        registry = RunIdRegistry(tmp_path / "runs")
        path = registry.reserve("P08-A")
        assert path.is_dir()
        assert registry.is_reserved("P08-A")

    def test_reusing_a_run_id_is_refused(self, tmp_path: Path):
        # Case G01-02: two concurrent workloads must not share a directory.
        registry = RunIdRegistry(tmp_path / "runs")
        registry.reserve("P08-A")
        with pytest.raises(ManifestError, match="already in use"):
            registry.reserve("P08-A")

    def test_two_registry_objects_still_collide(self, tmp_path: Path):
        # The reservation is a directory, so it survives a second process.
        RunIdRegistry(tmp_path / "runs").reserve("P08-A")
        with pytest.raises(ManifestError, match="already in use"):
            RunIdRegistry(tmp_path / "runs").reserve("P08-A")

    def test_a_path_traversing_run_id_is_refused(self, tmp_path: Path):
        registry = RunIdRegistry(tmp_path / "runs")
        with pytest.raises(ManifestError, match="usable directory name"):
            registry.reserve("../escape")

    def test_an_empty_run_id_is_refused(self, tmp_path: Path):
        with pytest.raises(ManifestError, match="usable directory name"):
            RunIdRegistry(tmp_path / "runs").reserve("")

    def test_an_unreserved_id_reads_as_free(self, tmp_path: Path):
        assert not RunIdRegistry(tmp_path / "runs").is_reserved("P08-B")
