"""The run directory: artifacts, hashes and a quota that fails loudly.

Layout is design 11.1 verbatim, so a bundle from this harness can be read by
anyone who has the document::

    runs/<scenario_id>/<run_id>/
      environment.json  config.effective.json  source/manifest.json
      fault/intent.yaml fault/receipts.jsonl   orchestration/journal.jsonl
      kafka/cut.json    kafka/record_inventory.json
      ingestion/frontier.json  clickhouse/raw_diff.json
      analytics/canonical_diff.json  analytics/bars_diff.json
      metrics/timeseries/  backup/restore_rehearsal.json
      reports/results.json  reports/summary.md  SHA256SUMS

Two properties this class is responsible for.

**A quota that stops the run instead of truncating a report.** Design 8 requires
that running out of artifact space end the test as ``INCONCLUSIVE``; a half
written diff that still parses is worse than no diff, because it looks complete.
:meth:`ArtifactStore.write_json` raises :class:`QuotaExceeded` before writing
anything that would cross the limit.

**Writes that are atomic enough to survive a kill.** Every file goes to a
temporary name, is flushed and ``fsync``-ed, then renamed. On POSIX the parent
directory is ``fsync``-ed too; on Windows that is not possible, and
:attr:`ArtifactStore.directory_fsync_supported` reports ``False`` rather than
letting the bundle imply a durability it does not have -- the same honesty P01's
recorder applies to its own commit protocol.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

#: Design 8's suggested evidence cap: 1 GiB per run.
DEFAULT_QUOTA_BYTES = 1 << 30


class QuotaExceeded(Exception):
    """The artifact quota would be exceeded. Nothing was written."""

    def __init__(self, path: str, needed: int, remaining: int) -> None:
        super().__init__(f"{path}: needs {needed}B, {remaining}B left in quota")
        self.path = path
        self.needed = needed
        self.remaining = remaining


def directory_fsync_supported() -> bool:
    """Whether this platform can ``fsync`` a directory handle. POSIX can."""
    return os.name == "posix"


@dataclass(frozen=True, slots=True)
class StoredArtifact:
    relative_path: str
    sha256: str
    bytes_written: int


class ArtifactStore:
    """One run's artifact directory."""

    def __init__(
        self,
        root: str | Path,
        *,
        scenario_id: str,
        run_id: str,
        quota_bytes: int = DEFAULT_QUOTA_BYTES,
        fsync: bool = True,
    ) -> None:
        self.run_dir = Path(root) / scenario_id / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.quota_bytes = quota_bytes
        self._bytes_used = 0
        self._artifacts: dict[str, StoredArtifact] = {}
        self.directory_fsync_supported = directory_fsync_supported()
        #: Whether each write is flushed to the device. On by default, because a
        #: bundle meant to survive the crash it is documenting has to be on disk.
        #: Turned off by the test suite, where ``FlushFileBuffers`` costs over a
        #: second per call on some Windows volumes and would make the suite
        #: unrunnable. :meth:`durability` reports which it was, so a bundle never
        #: implies a durability it did not pay for.
        self.fsync_enabled = fsync

    @property
    def bytes_used(self) -> int:
        return self._bytes_used

    @property
    def bytes_remaining(self) -> int:
        return max(0, self.quota_bytes - self._bytes_used)

    def path_for(self, relative_path: str) -> Path:
        """Resolve ``relative_path`` inside the run directory, refusing escapes.

        ``..`` and absolute paths are rejected rather than normalised: a
        traversal in an artifact name is case ``P07-G28-*`` and must not be able
        to write outside the bundle even when the name came from a scenario
        file.
        """
        candidate = (self.run_dir / relative_path).resolve()
        root = self.run_dir.resolve()
        if candidate != root and root not in candidate.parents:
            raise ValueError(f"artifact path escapes the run directory: {relative_path!r}")
        return candidate

    def write_bytes(self, relative_path: str, payload: bytes) -> StoredArtifact:
        """Atomically write ``payload``. Raises before writing if over quota."""
        if len(payload) > self.bytes_remaining:
            raise QuotaExceeded(relative_path, len(payload), self.bytes_remaining)
        target = self.path_for(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            if self.fsync_enabled:
                os.fsync(handle.fileno())
        temporary.replace(target)
        if self.fsync_enabled and self.directory_fsync_supported:
            fd = os.open(str(target.parent), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        digest = hashlib.sha256(payload).hexdigest()
        artifact = StoredArtifact(relative_path, digest, len(payload))
        self._artifacts[relative_path] = artifact
        self._bytes_used += len(payload)
        return artifact

    def write_json(self, relative_path: str, obj: Any) -> StoredArtifact:
        """Write canonical JSON: sorted keys, no spaces, trailing newline.

        Canonical because the bundle's own hashes go into ``SHA256SUMS`` and a
        re-run has to be able to reproduce them.
        """
        payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return self.write_bytes(relative_path, payload + b"\n")

    def write_text(self, relative_path: str, text: str) -> StoredArtifact:
        return self.write_bytes(relative_path, text.encode("utf-8"))

    def append_jsonl(self, relative_path: str, obj: Any) -> None:
        """Append one JSON line. Used for the journal and fault receipts.

        Appends are not atomic the way :meth:`write_bytes` is -- a kill mid-line
        can leave a partial record. The reader
        (:meth:`p07.evidence.proof_ledger.ProofLedger.verify`) treats a trailing
        partial line as a torn tail and says so, instead of discarding it
        silently.
        """
        line = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str) + "\n"
        payload = line.encode("utf-8")
        if len(payload) > self.bytes_remaining:
            raise QuotaExceeded(relative_path, len(payload), self.bytes_remaining)
        target = self.path_for(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("ab") as handle:
            handle.write(payload)
            handle.flush()
            if self.fsync_enabled:
                os.fsync(handle.fileno())
        self._bytes_used += len(payload)

    def read_json(self, relative_path: str) -> Any:
        return json.loads(self.path_for(relative_path).read_text(encoding="utf-8"))

    def iter_lines(self, relative_path: str) -> Iterator[str]:
        path = self.path_for(relative_path)
        if not path.exists():
            return
        with path.open("r", encoding="utf-8") as handle:
            yield from handle

    def inventory(self) -> Mapping[str, StoredArtifact]:
        return dict(self._artifacts)

    def finalise(self) -> StoredArtifact:
        """Write ``SHA256SUMS`` over everything stored. Returns its own entry.

        The sums file hashes the other artifacts but obviously not itself, and a
        reader with write access to the bundle could regenerate it. Design 5.1
        says this plainly: a hash chain detects tampering only when an anchor is
        kept somewhere else. :meth:`finalise` returns its digest so a caller can
        push that anchor outside the failure domain.
        """
        lines = [
            f"{artifact.sha256}  {path}"
            for path, artifact in sorted(self._artifacts.items())
        ]
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        target = self.path_for("SHA256SUMS")
        with target.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            if self.fsync_enabled:
                os.fsync(handle.fileno())
        return StoredArtifact("SHA256SUMS", hashlib.sha256(payload).hexdigest(), len(payload))

    def durability(self) -> dict[str, object]:
        """What this bundle's writes actually guarantee.

        Reported rather than assumed. ``fsync=False`` means the artifacts are in
        the OS page cache and a power loss can lose the tail, so a run that
        claims to have survived a crash must have been written with
        ``fsync=True``. Windows cannot fsync a directory handle at all, so a
        rename is not as durable there as on POSIX -- the same caveat P01's
        recorder publishes about its own commit protocol.
        """
        return {
            "fsync_enabled": self.fsync_enabled,
            "directory_fsync_supported": self.directory_fsync_supported,
            "note": (
                "Without fsync these artifacts are only in the page cache; without directory "
                "fsync a rename is not durable. Neither is claimed when not performed."
            ),
        }

    def verify_inventory(self) -> list[str]:
        """Re-hash every stored artifact. Returns the paths that no longer match."""
        damaged: list[str] = []
        for path, artifact in sorted(self._artifacts.items()):
            target = self.path_for(path)
            if not target.exists():
                damaged.append(path)
                continue
            if hashlib.sha256(target.read_bytes()).hexdigest() != artifact.sha256:
                damaged.append(path)
        return damaged
