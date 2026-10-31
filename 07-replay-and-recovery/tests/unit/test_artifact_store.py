"""The evidence bundle: atomic writes, a quota that refuses, and no escapes."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from p07.evidence.artifact_store import ArtifactStore, QuotaExceeded


@pytest.fixture
def store(tmp_path: Path) -> ArtifactStore:
    return ArtifactStore(tmp_path, scenario_id="sc", run_id="r1", fsync=False)


def test_layout_follows_the_design(store: ArtifactStore) -> None:
    store.write_json("kafka/cut.json", {"a": 1})
    store.write_json("analytics/bars_diff.json", {"b": 2})
    assert (store.run_dir / "kafka" / "cut.json").is_file()
    assert (store.run_dir / "analytics" / "bars_diff.json").is_file()
    assert store.run_dir.name == "r1"
    assert store.run_dir.parent.name == "sc"


def test_json_is_canonical_and_newline_terminated(store: ArtifactStore) -> None:
    """Canonical, because the bundle's own SHA256SUMS must be reproducible."""
    store.write_json("x.json", {"b": 1, "a": [2, 3]})
    raw = (store.run_dir / "x.json").read_bytes()
    assert raw == b'{"a":[2,3],"b":1}\n'


def test_no_temporary_file_is_left_behind(store: ArtifactStore) -> None:
    store.write_json("x.json", {"a": 1})
    assert list(store.run_dir.glob("*.tmp")) == []


def test_digest_and_size_are_recorded(store: ArtifactStore) -> None:
    artifact = store.write_text("note.txt", "hello")
    assert artifact.bytes_written == 5
    assert len(artifact.sha256) == 64
    assert store.inventory()["note.txt"].sha256 == artifact.sha256


@pytest.mark.whatif("P07-G01-13", "P07-G21-06")
def test_quota_refuses_before_writing_anything(tmp_path: Path) -> None:
    """Design 8: stop the run, never truncate a report into something parseable."""
    small = ArtifactStore(tmp_path, scenario_id="sc", run_id="r", quota_bytes=40, fsync=False)
    small.write_text("a.txt", "x" * 30)
    with pytest.raises(QuotaExceeded) as exc:
        small.write_text("b.txt", "y" * 30)
    assert exc.value.path == "b.txt"
    assert not (small.run_dir / "b.txt").exists()
    assert small.bytes_remaining == 10


def test_quota_also_applies_to_appends(tmp_path: Path) -> None:
    small = ArtifactStore(tmp_path, scenario_id="sc", run_id="r", quota_bytes=20, fsync=False)
    with pytest.raises(QuotaExceeded):
        small.append_jsonl("journal.jsonl", {"padding": "x" * 50})
    assert not (small.run_dir / "journal.jsonl").exists()


@pytest.mark.whatif("P07-G28-03")
@pytest.mark.parametrize("hostile", ["../escape.json", "a/../../escape.json", "/abs.json"])
def test_path_traversal_is_refused(store: ArtifactStore, hostile: str) -> None:
    """An artifact name from a scenario file must not write outside the bundle."""
    with pytest.raises((ValueError, OSError)):
        store.write_json(hostile, {"a": 1})
    assert not (store.run_dir.parent.parent / "escape.json").exists()


def test_jsonl_appends_accumulate_in_order(store: ArtifactStore) -> None:
    for index in range(3):
        store.append_jsonl("orchestration/journal.jsonl", {"i": index})
    lines = [json.loads(line) for line in store.iter_lines("orchestration/journal.jsonl")]
    assert [item["i"] for item in lines] == [0, 1, 2]


def test_sha256sums_covers_everything_stored(store: ArtifactStore) -> None:
    store.write_json("a.json", {"a": 1})
    store.write_json("b/c.json", {"b": 2})
    sums = store.finalise()
    text = (store.run_dir / "SHA256SUMS").read_text(encoding="utf-8")
    assert "a.json" in text and "b/c.json" in text
    # The sums file hashes the others but obviously not itself.
    assert "SHA256SUMS" not in text
    assert len(sums.sha256) == 64


def test_verify_inventory_detects_tampering_and_deletion(store: ArtifactStore) -> None:
    store.write_json("a.json", {"a": 1})
    store.write_json("b.json", {"b": 2})
    assert store.verify_inventory() == []
    (store.run_dir / "a.json").write_text("{}", encoding="utf-8")
    (store.run_dir / "b.json").unlink()
    assert sorted(store.verify_inventory()) == ["a.json", "b.json"]


def test_durability_is_reported_not_assumed(tmp_path: Path) -> None:
    """A bundle must say whether it was flushed, because it is often not."""
    unflushed = ArtifactStore(tmp_path, scenario_id="s", run_id="a", fsync=False)
    assert unflushed.durability()["fsync_enabled"] is False
    flushed = ArtifactStore(tmp_path, scenario_id="s", run_id="b", fsync=True)
    assert flushed.durability()["fsync_enabled"] is True
    assert flushed.durability()["directory_fsync_supported"] is (os.name == "posix")


def test_the_fsync_path_works(tmp_path: Path) -> None:
    """Exercised once here; the rest of the suite runs unflushed for speed."""
    store = ArtifactStore(tmp_path, scenario_id="s", run_id="fs", fsync=True)
    artifact = store.write_json("a.json", {"a": 1})
    assert (store.run_dir / "a.json").read_bytes() == b'{"a":1}\n'
    assert artifact.bytes_written == 8
    store.append_jsonl("j.jsonl", {"x": 1})
    store.finalise()
    assert store.verify_inventory() == []


def test_read_json_round_trips(store: ArtifactStore) -> None:
    store.write_json("round.json", {"a": [1, 2], "b": "x"})
    assert store.read_json("round.json") == {"a": [1, 2], "b": "x"}


def test_iter_lines_of_a_missing_file_is_empty(store: ArtifactStore) -> None:
    assert list(store.iter_lines("nothing.jsonl")) == []
