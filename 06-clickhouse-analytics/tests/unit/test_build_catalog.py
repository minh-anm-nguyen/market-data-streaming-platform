"""The publication gate: CAS, locking, atomicity, immutability (group G, F-12)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from _coverage import covers
from md_analytics.build_catalog import (
    ACTIVE_POINTER,
    NO_ACTIVE_BUILD,
    BuildCatalog,
    BuildManifest,
)
from md_analytics.canonical_builder import BuildState
from md_analytics.errors import PublishRejected, Verdict


def manifest(build_id="build-1", **overrides) -> BuildManifest:
    kwargs = dict(
        build_id=build_id,
        dataset_id="DS-1",
        state=BuildState.READY,
        manifest_sha256="m" * 64,
        cut_digest="c" * 64,
        schema_sha256="s" * 64,
        sql_bank_digest="q" * 64,
        canonical_policy_digest="p" * 64,
        builder_version="canonical-builder.1",
        canonical_count=10,
        bar_count=2,
        bar_tick_count=10,
        raw_physical_rows=12,
        raw_duplicate_row_count=2,
        audit_verdict=Verdict.PASS,
        bars_verdict=Verdict.PASS,
        evidence_class="MODEL",
        server_version=None,
        published_at_ns=1_793_361_600_000_000_000,
        environment={"platform": "test"},
        counters={"reference_tick_count": 10},
    )
    kwargs.update(overrides)
    return BuildManifest(**kwargs)


class TestProofGate:
    @covers("P06-G-11", "P06-AA-03")
    def test_a_ready_manifest_with_a_failed_audit_is_not_publishable(self, catalog):
        bad = manifest(audit_verdict=Verdict.NO_GO)
        assert "raw audit verdict is NO_GO" in bad.proof_gaps()
        with pytest.raises(PublishRejected) as exc:
            catalog.publish(bad)
        assert exc.value.code == "PROOFS_INCOMPLETE"

    @covers("P06-G-11")
    def test_a_manifest_that_is_not_ready_is_not_publishable(self, catalog):
        with pytest.raises(PublishRejected):
            catalog.publish(manifest(state=BuildState.DIRTY))

    @covers("P06-G-11")
    def test_a_failed_bar_verdict_blocks_publication(self, catalog):
        with pytest.raises(PublishRejected):
            catalog.publish(manifest(bars_verdict=Verdict.FAIL))

    @covers("P06-AD-05")
    def test_missing_provenance_hashes_block_publication(self, catalog):
        for field in ("manifest_sha256", "cut_digest", "schema_sha256", "sql_bank_digest"):
            with pytest.raises(PublishRejected) as exc:
                catalog.publish(manifest(**{field: ""}))
            assert field in str(exc.value)

    def test_an_empty_build_is_not_publishable(self, catalog):
        with pytest.raises(PublishRejected):
            catalog.publish(manifest(canonical_count=0))

    @covers("P06-H-05")
    def test_a_tick_count_disagreeing_with_the_reference_blocks_publication(self, catalog):
        with pytest.raises(PublishRejected):
            catalog.publish(manifest(bar_tick_count=9))


class TestCompareAndSwap:
    def test_the_first_publish_requires_no_active_build(self, catalog):
        pointer = catalog.publish(manifest(), expected_current=NO_ACTIVE_BUILD)
        assert pointer["pointer_version"] == 1
        assert pointer["superseded_build_id"] is None
        assert catalog.active_build_id() == "build-1"

    @covers("P06-F-12", "P06-G-10")
    def test_publishing_over_an_unexpected_pointer_is_refused(self, catalog):
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        with pytest.raises(PublishRejected) as exc:
            catalog.publish(manifest("build-2"), expected_current=NO_ACTIVE_BUILD)
        assert exc.value.code == "POINTER_CAS_FAILED"
        assert catalog.active_build_id() == "build-1", "the pointer did not move"

    @covers("P06-F-12")
    def test_a_correct_expectation_supersedes_cleanly(self, catalog):
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        pointer = catalog.publish(manifest("build-2"), expected_current="build-1")
        assert pointer["pointer_version"] == 2
        assert pointer["superseded_build_id"] == "build-1"
        assert catalog.active_build_id() == "build-2"

    @covers("P06-F-12", "P06-S-03")
    def test_two_racing_publishers_leave_exactly_one_winner(self, catalog):
        catalog.publish(manifest("build-0"), expected_current=NO_ACTIVE_BUILD)
        # Both believe build-0 is current. The first wins; the second's CAS fails
        # rather than overwriting a pointer it has not seen.
        catalog.publish(manifest("build-a"), expected_current="build-0")
        with pytest.raises(PublishRejected) as exc:
            catalog.publish(manifest("build-b"), expected_current="build-0")
        assert exc.value.code == "POINTER_CAS_FAILED"
        assert catalog.active_build_id() == "build-a"
        assert sorted(catalog.list_builds()) == ["build-0", "build-a"], (
            "the loser's manifest was never recorded, so no partial build is "
            "discoverable"
        )


class TestAtomicity:
    @covers("P06-G-10")
    def test_a_crash_before_the_rename_leaves_the_previous_pointer(self, catalog):
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        catalog.fault_crash_before_rename = True
        with pytest.raises(PublishRejected) as exc:
            catalog.publish(manifest("build-2"), expected_current="build-1")
        assert exc.value.code == "INJECTED_PUBLISH_CRASH"
        assert catalog.active_build_id() == "build-1", "no torn pointer"

        pointer = json.loads(catalog.pointer_path.read_text(encoding="utf-8"))
        assert pointer["build_id"] == "build-1"
        temps = list(catalog.root.glob(f".{ACTIVE_POINTER}.*.tmp"))
        assert temps, "the abandoned temp file is left behind, not the live pointer"

    @covers("P06-G-10")
    def test_the_lock_is_released_after_a_crash(self, catalog):
        catalog.fault_crash_before_rename = True
        with pytest.raises(PublishRejected):
            catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        catalog.fault_crash_before_rename = False
        assert not catalog.lock_path.exists()
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        assert catalog.active_build_id() == "build-1"

    def test_a_held_lock_blocks_a_second_writer(self, catalog):
        os.close(os.open(catalog.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
        try:
            with pytest.raises(PublishRejected) as exc:
                catalog.publish(manifest(), expected_current=NO_ACTIVE_BUILD)
            assert exc.value.code == "CATALOG_LOCKED"
        finally:
            catalog.lock_path.unlink()

    def test_the_pointer_file_is_valid_json_with_a_trailing_newline(self, catalog):
        catalog.publish(manifest(), expected_current=NO_ACTIVE_BUILD)
        text = catalog.pointer_path.read_text(encoding="utf-8")
        assert text.endswith("\n")
        assert json.loads(text)["build_id"] == "build-1"


class TestImmutability:
    @covers("P06-G-11")
    def test_recording_different_content_under_one_build_id_is_refused(self, catalog):
        catalog.record_manifest(manifest("build-1", canonical_count=10))
        with pytest.raises(PublishRejected) as exc:
            catalog.record_manifest(manifest("build-1", canonical_count=11))
        assert exc.value.code == "MANIFEST_IMMUTABLE"

    def test_republishing_identical_content_is_idempotent(self, catalog):
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        first = catalog.read_manifest("build-1").published_at_ns
        # Same snapshot, published again later: a retry, not a correction.
        catalog.publish(
            manifest("build-1", published_at_ns=first + 10_000),
            expected_current="build-1",
        )
        assert catalog.read_manifest("build-1").published_at_ns == first, (
            "the original publication time stays on file"
        )

    @covers("P06-G-11", "P06-Y-07")
    def test_an_edited_manifest_file_is_detected_on_read(self, catalog):
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        path = catalog.builds_dir / "build-1.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["payload"]["bar_tick_count"] = 999
        path.write_text(json.dumps(document), encoding="utf-8")

        with pytest.raises(PublishRejected) as exc:
            catalog.read_manifest("build-1")
        assert exc.value.code == "MANIFEST_DIGEST_MISMATCH"

    @covers("P06-Y-07")
    def test_a_signature_is_verified_when_a_key_is_configured(self, signed_catalog):
        signed_catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        path = signed_catalog.builds_dir / "build-1.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        assert document["signature_hmac_sha256"], "a key was configured"
        document["signature_hmac_sha256"] = "0" * 64
        path.write_text(json.dumps(document), encoding="utf-8")

        with pytest.raises(PublishRejected) as exc:
            signed_catalog.read_manifest("build-1")
        assert exc.value.code == "MANIFEST_SIGNATURE_INVALID"

    @covers("P06-Y-01")
    @pytest.mark.parametrize("build_id", ["../escape", "a/b", "a\\b", ".", ".."])
    def test_a_build_id_cannot_escape_the_catalog_directory(self, catalog, build_id):
        with pytest.raises(PublishRejected) as exc:
            catalog.record_manifest(manifest(build_id))
        assert exc.value.code == "BUILD_ID_UNSAFE"


class TestServing:
    @covers("P06-R-06")
    def test_require_ready_refuses_a_dirty_build(self, catalog):
        catalog.record_manifest(manifest("build-x", state=BuildState.DIRTY))
        with pytest.raises(PublishRejected) as exc:
            catalog.require_ready("build-x")
        assert exc.value.code == "BUILD_NOT_READY"

    @covers("P06-R-06")
    def test_require_ready_refuses_an_unknown_build(self, catalog):
        with pytest.raises(PublishRejected) as exc:
            catalog.require_ready("nope")
        assert exc.value.code == "MANIFEST_NOT_FOUND"

    @covers("P06-G-12")
    def test_a_revoked_build_stops_serving_but_keeps_its_evidence(self, catalog):
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        catalog.revoke("build-1", reason="late correction found")
        assert catalog.active_build_id() is None
        assert "build-1" in catalog.list_builds(), "the record is kept for audit"
        revoked = catalog.read_manifest("build-1")
        assert revoked.state is BuildState.NO_GO
        assert any("REVOKED" in note for note in revoked.notes)
        with pytest.raises(PublishRejected):
            catalog.require_ready("build-1")

    @covers("P06-G-12")
    def test_an_old_ready_build_stays_servable_while_a_new_one_fails(self, catalog):
        catalog.publish(manifest("build-1"), expected_current=NO_ACTIVE_BUILD)
        with pytest.raises(PublishRejected):
            catalog.publish(
                manifest("build-2", bars_verdict=Verdict.FAIL), expected_current="build-1"
            )
        assert catalog.require_ready("build-1").build_id == "build-1"
        assert catalog.active_build_id() == "build-1"
        assert "build-2" not in catalog.list_builds()
