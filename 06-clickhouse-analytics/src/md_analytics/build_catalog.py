"""The publication gate: a single-writer catalog with a compare-and-swap pointer.

Section 5.1 S5 and matrix group G/F-12. The design is explicit that a single
``SELECT`` must not stand in for a publication gate, so the pointer lives
outside ClickHouse and moves only under two conditions:

* **A lock is held.** One writer at a time, via ``O_CREAT | O_EXCL``. Two
  builders promoting at once must not interleave (case F-12, E2E-26).
* **The caller named the build it expects to replace.** A blind write would let
  a slow builder overwrite a newer READY build with an older one (case G-12).

Durability details that matter more than they look:

* The pointer is written to a temp file in the **same directory** and moved with
  :func:`os.replace`, which is atomic on both POSIX and Windows. A reader
  therefore sees the old pointer or the new one, never a torn one -- a crash
  halfway through writing leaves a stray temp file and the previous pointer
  intact (case G-10).
* ``fsync`` is called on the file and then on the directory, because a rename
  that is not durable can be lost even after the data is.
* Manifests are **write-once**. Re-publishing an existing build id with
  different content is rejected rather than merged: a published snapshot is
  immutable and a correction is a new build (ADR-007, case G-11).

The MVP shape is a local filesystem with one writer. That is honest for a single
host and is *not* multi-host HA -- two machines sharing a directory over NFS do
not get these guarantees, and section 5.1 S5 says so.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
import time
from contextlib import contextmanager
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Final

from .canonical_builder import BuildState
from .errors import PublishRejected
from .errors import Verdict

ACTIVE_POINTER: Final = "active_build.json"
BUILDS_DIR: Final = "builds"
LOCK_FILE: Final = ".catalog.lock"

#: Sentinel for "there must be no active build yet", distinct from ``None``
#: meaning "do not care". Publishing the first build and publishing over an
#: unknown current build are different intentions and must not share a spelling.
NO_ACTIVE_BUILD: Final = "__NO_ACTIVE_BUILD__"


@dataclass(frozen=True, slots=True)
class BuildManifest:
    """Everything needed to re-derive and re-check a published snapshot (S5)."""

    build_id: str
    dataset_id: str
    state: BuildState
    manifest_sha256: str
    cut_digest: str
    schema_sha256: str
    sql_bank_digest: str
    canonical_policy_digest: str
    builder_version: str
    canonical_count: int
    bar_count: int
    bar_tick_count: int
    raw_physical_rows: int
    raw_duplicate_row_count: int
    audit_verdict: Verdict
    bars_verdict: Verdict
    evidence_class: str
    server_version: str | None
    published_at_ns: int
    environment: dict[str, str] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def payload(self) -> dict[str, Any]:
        """The signed content. Excludes the signature itself."""
        return {
            "build_id": self.build_id,
            "dataset_id": self.dataset_id,
            "state": str(self.state),
            "manifest_sha256": self.manifest_sha256,
            "cut_digest": self.cut_digest,
            "schema_sha256": self.schema_sha256,
            "sql_bank_digest": self.sql_bank_digest,
            "canonical_policy_digest": self.canonical_policy_digest,
            "builder_version": self.builder_version,
            "canonical_count": self.canonical_count,
            "bar_count": self.bar_count,
            "bar_tick_count": self.bar_tick_count,
            "raw_physical_rows": self.raw_physical_rows,
            "raw_duplicate_row_count": self.raw_duplicate_row_count,
            "audit_verdict": str(self.audit_verdict),
            "bars_verdict": str(self.bars_verdict),
            "evidence_class": self.evidence_class,
            "server_version": self.server_version,
            "published_at_ns": self.published_at_ns,
            "environment": dict(sorted(self.environment.items())),
            "counters": dict(sorted(self.counters.items())),
            "notes": list(self.notes),
        }

    def content_digest(self) -> str:
        """Digest of the whole document, including when it was published.

        This is the tamper evidence: :meth:`BuildCatalog.read_manifest`
        recomputes it, so any later edit to any field is detected.
        """
        return hashlib.sha256(_canonical_json(self.payload())).hexdigest()

    #: Fields that describe *this publication act* rather than the snapshot.
    _VOLATILE_FIELDS = ("published_at_ns", "environment")

    def substantive_payload(self) -> dict[str, Any]:
        """The snapshot's content, without the circumstances of publishing it."""
        return {
            k: v for k, v in self.payload().items() if k not in self._VOLATILE_FIELDS
        }

    def substantive_digest(self) -> str:
        """Digest of what the snapshot *says*, for the immutability check.

        Immutability (ADR-007) is about the data a consumer reads: counts,
        hashes, verdicts, policy. Publishing the identical snapshot again after a
        lost acknowledgement is a retry, not a correction, and must be idempotent
        -- so the wall clock and the host fingerprint are excluded here while
        remaining covered by :meth:`content_digest`.
        """
        return hashlib.sha256(_canonical_json(self.substantive_payload())).hexdigest()

    def is_ready(self) -> bool:
        return (
            self.state is BuildState.READY
            and self.audit_verdict is Verdict.PASS
            and self.bars_verdict is Verdict.PASS
        )

    def proof_gaps(self) -> list[str]:
        """Why this manifest may *not* be published, if anything.

        INV-10/INV-13: READY has to mean the proofs exist and passed. A manifest
        that names a READY state while carrying a failed verdict is the
        ``build_ready_without_proof`` alert of section 11, and it is blocked here
        rather than merely counted.
        """
        gaps: list[str] = []
        if self.state is not BuildState.READY:
            gaps.append(f"state is {self.state}, not READY")
        if self.audit_verdict is not Verdict.PASS:
            gaps.append(f"raw audit verdict is {self.audit_verdict}")
        if self.bars_verdict is not Verdict.PASS:
            gaps.append(f"bar verification verdict is {self.bars_verdict}")
        if self.canonical_count <= 0:
            gaps.append("canonical_count is not positive")
        if self.bar_tick_count != self.counters.get("reference_tick_count", self.bar_tick_count):
            gaps.append("bar tick count disagrees with the reference tick count")
        for name, value in (
            ("manifest_sha256", self.manifest_sha256),
            ("cut_digest", self.cut_digest),
            ("schema_sha256", self.schema_sha256),
            ("sql_bank_digest", self.sql_bank_digest),
        ):
            if not value:
                gaps.append(f"{name} is empty")
        return gaps


def _canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _atomic_write(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically, durably, in the same directory.

    The temp file must share the directory so the rename stays within one
    filesystem; ``os.replace`` across devices is not atomic (and on Windows,
    not permitted).
    """
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=directory, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        # Leave nothing half-written behind on the way out.
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise
    _fsync_dir(directory)


def _fsync_dir(directory: Path) -> None:
    """Make the rename itself durable. A no-op where the platform forbids it."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return  # Windows cannot open a directory this way; the replace is still atomic.
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class BuildCatalog:
    """Filesystem build catalog with a single-writer, CAS-guarded pointer."""

    def __init__(self, root: Path, *, signing_key: bytes | None = None) -> None:
        self.root = Path(root)
        self.builds_dir = self.root / BUILDS_DIR
        self.pointer_path = self.root / ACTIVE_POINTER
        self.lock_path = self.root / LOCK_FILE
        self._signing_key = signing_key
        self.root.mkdir(parents=True, exist_ok=True)
        self.builds_dir.mkdir(parents=True, exist_ok=True)

        #: Set by a test to abandon a publish *after* the temp file is written
        #: but before the rename, which is the crash window of case G-10.
        self.fault_crash_before_rename = False

    # ------------------------------------------------------------------- lock

    @contextmanager
    def _writer_lock(self, *, timeout_s: float = 5.0) -> Iterator[None]:
        deadline = time.monotonic() + timeout_s
        fd: int | None = None
        while True:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except FileExistsError:
                if time.monotonic() >= deadline:
                    raise PublishRejected(
                        f"another writer holds {self.lock_path} after {timeout_s}s; "
                        f"publication is serialized by design",
                        code="CATALOG_LOCKED",
                    ) from None
                time.sleep(0.01)
        try:
            os.write(fd, str(os.getpid()).encode("ascii"))
            os.close(fd)
            fd = None
            yield
        finally:
            if fd is not None:
                os.close(fd)
            try:
                os.unlink(self.lock_path)
            except FileNotFoundError:
                pass

    # -------------------------------------------------------------- manifests

    def _manifest_path(self, build_id: str) -> Path:
        if "/" in build_id or "\\" in build_id or build_id in (".", ".."):
            raise PublishRejected(
                f"refusing a build id that would escape the catalog: {build_id!r}",
                code="BUILD_ID_UNSAFE",
            )
        return self.builds_dir / f"{build_id}.json"

    def _signature(self, digest: str) -> str | None:
        if self._signing_key is None:
            return None
        return hmac.new(self._signing_key, digest.encode("ascii"), hashlib.sha256).hexdigest()

    def record_manifest(self, manifest: BuildManifest) -> str:
        """Write a build manifest once, with tamper evidence. Returns its digest.

        Re-recording the same content is idempotent -- a retried publish after a
        lost acknowledgement must not be a failure -- but re-recording *different*
        content under the same build id is refused.
        """
        digest = manifest.content_digest()
        document = {
            "payload": manifest.payload(),
            "content_digest": digest,
            "substantive_digest": manifest.substantive_digest(),
            "signature_hmac_sha256": self._signature(digest),
        }
        path = self._manifest_path(manifest.build_id)
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing.get("substantive_digest") != manifest.substantive_digest():
                raise PublishRejected(
                    f"build {manifest.build_id} is already recorded with different "
                    f"content; a published snapshot is immutable and a correction is "
                    f"a new build",
                    code="MANIFEST_IMMUTABLE",
                    build_id=manifest.build_id,
                )
            # Identical snapshot, published again: a retry. Keep the original
            # record rather than restamping it, so the first publication time
            # stays the one on file.
            return str(existing["content_digest"])
        _atomic_write(path, _canonical_json(document) + b"\n")
        return digest

    def read_manifest(self, build_id: str) -> BuildManifest:
        path = self._manifest_path(build_id)
        if not path.exists():
            raise PublishRejected(
                f"no manifest recorded for build {build_id}",
                code="MANIFEST_NOT_FOUND",
                build_id=build_id,
            )
        document = json.loads(path.read_text(encoding="utf-8"))
        payload = document["payload"]
        rebuilt = BuildManifest(
            build_id=payload["build_id"],
            dataset_id=payload["dataset_id"],
            state=BuildState(payload["state"]),
            manifest_sha256=payload["manifest_sha256"],
            cut_digest=payload["cut_digest"],
            schema_sha256=payload["schema_sha256"],
            sql_bank_digest=payload["sql_bank_digest"],
            canonical_policy_digest=payload["canonical_policy_digest"],
            builder_version=payload["builder_version"],
            canonical_count=payload["canonical_count"],
            bar_count=payload["bar_count"],
            bar_tick_count=payload["bar_tick_count"],
            raw_physical_rows=payload["raw_physical_rows"],
            raw_duplicate_row_count=payload["raw_duplicate_row_count"],
            audit_verdict=Verdict(payload["audit_verdict"]),
            bars_verdict=Verdict(payload["bars_verdict"]),
            evidence_class=payload["evidence_class"],
            server_version=payload["server_version"],
            published_at_ns=payload["published_at_ns"],
            environment=dict(payload.get("environment", {})),
            counters=dict(payload.get("counters", {})),
            notes=tuple(payload.get("notes", ())),
        )
        if rebuilt.content_digest() != document.get("content_digest"):
            raise PublishRejected(
                f"manifest for {build_id} has been modified since it was recorded",
                code="MANIFEST_DIGEST_MISMATCH",
                build_id=build_id,
            )
        expected_signature = self._signature(rebuilt.content_digest())
        if expected_signature is not None and not hmac.compare_digest(
            expected_signature, document.get("signature_hmac_sha256") or ""
        ):
            raise PublishRejected(
                f"manifest signature for {build_id} does not verify",
                code="MANIFEST_SIGNATURE_INVALID",
                build_id=build_id,
            )
        return rebuilt

    # ---------------------------------------------------------------- pointer

    def read_active(self) -> dict[str, Any] | None:
        if not self.pointer_path.exists():
            return None
        return json.loads(self.pointer_path.read_text(encoding="utf-8"))

    def active_build_id(self) -> str | None:
        pointer = self.read_active()
        return None if pointer is None else str(pointer["build_id"])

    def publish(
        self,
        manifest: BuildManifest,
        *,
        expected_current: str | None = NO_ACTIVE_BUILD,
    ) -> dict[str, Any]:
        """Record the manifest and move the pointer, under lock and CAS.

        ``expected_current`` must name the build the caller believes is active,
        or :data:`NO_ACTIVE_BUILD` for the first publish. Passing ``None`` means
        "whatever is there" and is accepted, but it is the unsafe option and the
        concurrent-publisher test relies on callers not using it.
        """
        gaps = manifest.proof_gaps()
        if gaps:
            raise PublishRejected(
                f"build {manifest.build_id} is not publishable: {'; '.join(gaps)}",
                code="PROOFS_INCOMPLETE",
                build_id=manifest.build_id,
                gaps=gaps,
            )

        with self._writer_lock():
            current = self.active_build_id()
            if expected_current is not None:
                wanted = None if expected_current == NO_ACTIVE_BUILD else expected_current
                if current != wanted:
                    raise PublishRejected(
                        f"compare-and-swap failed: expected active build "
                        f"{wanted!r}, found {current!r}",
                        code="POINTER_CAS_FAILED",
                        build_id=manifest.build_id,
                        expected=wanted,
                        found=current,
                    )
            # Use the digest actually on file: an idempotent retry keeps the
            # original record, so the pointer must name that one.
            recorded_digest = self.record_manifest(manifest)

            previous = self.read_active()
            pointer = {
                "build_id": manifest.build_id,
                "dataset_id": manifest.dataset_id,
                "content_digest": recorded_digest,
                "pointer_version": 1 if previous is None else int(previous["pointer_version"]) + 1,
                "superseded_build_id": current,
                "published_at_ns": manifest.published_at_ns,
            }
            if self.fault_crash_before_rename:
                # Write the temp file, then abandon: exactly the window case
                # G-10 cares about. The pointer on disk must still be the old one.
                fd, temp_name = tempfile.mkstemp(
                    dir=self.root, prefix=f".{ACTIVE_POINTER}.", suffix=".tmp"
                )
                with os.fdopen(fd, "wb") as handle:
                    handle.write(_canonical_json(pointer))
                raise PublishRejected(
                    f"injected crash before pointer rename (temp file {temp_name})",
                    code="INJECTED_PUBLISH_CRASH",
                    build_id=manifest.build_id,
                )
            _atomic_write(self.pointer_path, _canonical_json(pointer) + b"\n")
            return pointer

    def revoke(self, build_id: str, *, reason: str) -> dict[str, Any]:
        """Mark a recorded build REVOKED without deleting its evidence.

        Deleting the manifest would destroy the only record of what was served;
        a revocation has to be auditable (section 11's ``revoke`` gate).
        """
        manifest = self.read_manifest(build_id)
        revoked = replace(
            manifest,
            state=BuildState.NO_GO,
            notes=(*manifest.notes, f"REVOKED: {reason}"),
        )
        path = self._manifest_path(build_id)
        digest = revoked.content_digest()
        _atomic_write(
            path,
            _canonical_json(
                {
                    "payload": revoked.payload(),
                    "content_digest": digest,
                    "substantive_digest": revoked.substantive_digest(),
                    "signature_hmac_sha256": self._signature(digest),
                }
            )
            + b"\n",
        )
        with self._writer_lock():
            pointer = self.read_active()
            if pointer is not None and pointer["build_id"] == build_id:
                self.pointer_path.unlink()
        return {"build_id": build_id, "state": str(BuildState.NO_GO), "reason": reason}

    def list_builds(self) -> list[str]:
        return sorted(path.stem for path in self.builds_dir.glob("*.json"))

    def require_ready(self, build_id: str) -> BuildManifest:
        """Resolve a build for serving, or refuse. No fallback to raw (FR-09)."""
        manifest = self.read_manifest(build_id)
        if not manifest.is_ready():
            raise PublishRejected(
                f"build {build_id} is {manifest.state} and is not servable",
                code="BUILD_NOT_READY",
                build_id=build_id,
                state=str(manifest.state),
            )
        return manifest
