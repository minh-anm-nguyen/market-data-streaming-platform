"""Provenance: seals, digests, run identity and drift detection (sections 3.1, 5.1; group G01).

Group G01 is sixteen variations on one theme: the numbers were fine, but they
describe a different run than the one being claimed. A ``dataset_id`` edited
between warmup and steady (G01-01), a ``run_id`` reused by two concurrent
workloads (G01-02), a source that only ever reached ``.part`` (G01-03), one
byte flipped in a sealed file (G01-04), a Docker tag repointed at a new digest
(G01-09), a ``query_id`` bound to different SQL than the catalog says (G01-13).

None of those are performance problems, and none of them can be caught after
the fact by looking at a dashboard. They are caught by pinning everything that
could change, hashing it, and comparing before and after -- which is what this
module does.

Two deliberate asymmetries:

* **A seal is a claim about durability, not about bytes.** A file whose hash
  matches but which has no ``COMPLETED`` marker is *not* sealed, because the
  writer may still have had buffered events (P01's contract). So
  :class:`SourceSeal` carries ``sealed`` separately from ``sha256``.
* **Drift is classified, not just detected.** Changing ``linger.ms`` between an
  A and a B run makes those two runs incomparable (G01-08) but does not make
  either one invalid. Changing the dataset does make them invalid. The two get
  different severities.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import ManifestError

#: Read size for streaming hashes: large enough to be cheap, small enough that
#: a 200 GB dataset never costs more than this much resident memory.
READ_CHUNK = 1 << 20

#: P01 run-directory layout, mirrored here so P08 can read a sealed dataset
#: without importing the simulator package.
P01_EVENTS_NAME = "events.jsonl"
P01_EVENTS_PART_NAME = "events.jsonl.part"
P01_MANIFEST_NAME = "manifest.json"
P01_COMPLETED_NAME = "COMPLETED"


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str:
    """Streaming SHA-256 so a huge dataset does not have to fit in memory."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(payload: Any) -> str:
    """Stable JSON for hashing: sorted keys, no incidental whitespace.

    Two configs that differ only in key order must produce the same digest, or
    every rerun would look like a drift.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest_of(payload: Any) -> str:
    return sha256_text(canonical_json(payload))


class SealState(str, Enum):
    """Whether a source can anchor a completeness claim."""

    SEALED = "SEALED"
    #: Bytes exist, the writer never committed (a ``.part`` file, or no marker).
    INCOMPLETE = "INCOMPLETE"
    #: Bytes disagree with the recorded hash.
    CORRUPT = "CORRUPT"
    MISSING = "MISSING"


@dataclass(frozen=True)
class SourceSeal:
    """A sealed dataset: its identity, its bytes and whether it was committed.

    Two digests, deliberately separate, because P01 keeps them separate:

    ``sha256``
        The hash of the events file as it sits on disk. This is what detects a
        flipped byte or a truncation (cases G01-04, G01-11).
    ``dataset_digest``
        P01's ``data_sha256``: a hash of the data-generating configuration, and
        the thing ``dataset_id`` is derived from. Two runs of the same config
        share it; it says nothing about the bytes that were written.

    Conflating them is a real hazard -- they are both 64 hex characters in the
    same manifest -- and comparing the file against the config digest would fail
    every honest run.

    ``event_count`` is the count P01 *committed*, read from the ``COMPLETED``
    marker, not ``planned_logical_slots`` from the manifest. Section 4.1 keeps
    those apart: the manifest records what was asked for, the marker records
    what was durably written, and only the second can anchor a completeness
    claim.
    """

    dataset_id: str
    path: str
    sha256: str | None
    event_count: int | None
    state: SealState
    reasons: tuple[str, ...] = ()
    dataset_digest: str | None = None
    bytes_sealed: int | None = None

    @property
    def sealed(self) -> bool:
        return self.state is SealState.SEALED

    @property
    def usable_for_completeness_claim(self) -> bool:
        """Only a sealed source can anchor "every expected event arrived".

        Case G01-03: a generator stopped early leaves a ``.part`` file, and the
        expected set derived from it would be missing events that the pipeline
        was never asked to carry. That has to block the claim, not shrink the
        denominator.
        """
        return self.sealed and self.event_count is not None and self.sha256 is not None

    @classmethod
    def from_p01_run_dir(cls, run_dir: Path) -> "SourceSeal":
        """Read a P01 run directory and judge its seal.

        The checks are in the order that produces the most specific reason:
        missing, then uncommitted, then hash mismatch.
        """
        reasons: list[str] = []
        manifest_path = run_dir / P01_MANIFEST_NAME
        events_path = run_dir / P01_EVENTS_NAME
        part_path = run_dir / P01_EVENTS_PART_NAME
        completed_path = run_dir / P01_COMPLETED_NAME

        if not manifest_path.is_file():
            return cls(
                dataset_id="unknown", path=str(run_dir), sha256=None, event_count=None,
                state=SealState.MISSING, reasons=("no manifest.json in the run directory",),
            )
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return cls(
                dataset_id="unknown", path=str(run_dir), sha256=None, event_count=None,
                state=SealState.CORRUPT, reasons=(f"manifest.json unreadable: {exc}",),
            )

        dataset_id = str(manifest.get("dataset_id", "unknown"))
        # ``data_sha256`` is P01's config digest, not the file hash. The file
        # hash lives in the COMPLETED marker, which is also the only thing that
        # proves the writer committed.
        dataset_digest = manifest.get("data_sha256")

        if part_path.is_file() and not events_path.is_file():
            reasons.append("source is still a .part file; the writer never committed")
            return cls(
                dataset_id=dataset_id, path=str(part_path), sha256=None,
                event_count=None, state=SealState.INCOMPLETE, reasons=tuple(reasons),
                dataset_digest=dataset_digest,
            )
        if not events_path.is_file():
            reasons.append("events.jsonl is absent")
            return cls(
                dataset_id=dataset_id, path=str(events_path), sha256=None,
                event_count=None, state=SealState.MISSING, reasons=tuple(reasons),
                dataset_digest=dataset_digest,
            )
        if not completed_path.is_file():
            reasons.append("no COMPLETED marker; the dataset was not committed")
            return cls(
                dataset_id=dataset_id, path=str(events_path), sha256=None,
                event_count=None, state=SealState.INCOMPLETE, reasons=tuple(reasons),
                dataset_digest=dataset_digest,
            )

        try:
            marker = json.loads(completed_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            reasons.append(f"COMPLETED marker is unreadable: {exc}")
            return cls(
                dataset_id=dataset_id, path=str(events_path), sha256=None,
                event_count=None, state=SealState.CORRUPT, reasons=tuple(reasons),
                dataset_digest=dataset_digest,
            )
        if not isinstance(marker, dict):
            reasons.append("COMPLETED marker is not an object")
            return cls(
                dataset_id=dataset_id, path=str(events_path), sha256=None,
                event_count=None, state=SealState.CORRUPT, reasons=tuple(reasons),
                dataset_digest=dataset_digest,
            )

        committed_sha = marker.get("events_sha256")
        committed_lines = marker.get("events_lines")
        committed_bytes = marker.get("events_bytes")
        actual_sha = sha256_file(events_path)
        actual_bytes = events_path.stat().st_size

        if committed_sha is None:
            reasons.append("COMPLETED marker carries no events_sha256")
            return cls(
                dataset_id=dataset_id, path=str(events_path), sha256=actual_sha,
                event_count=None, state=SealState.INCOMPLETE, reasons=tuple(reasons),
                dataset_digest=dataset_digest, bytes_sealed=actual_bytes,
            )
        if actual_sha != committed_sha:
            reasons.append(
                f"committed sha256 {str(committed_sha)[:16]}... does not match the "
                f"file ({actual_sha[:16]}...)"
            )
            return cls(
                dataset_id=dataset_id, path=str(events_path), sha256=actual_sha,
                event_count=None, state=SealState.CORRUPT, reasons=tuple(reasons),
                dataset_digest=dataset_digest, bytes_sealed=actual_bytes,
            )
        if committed_bytes is not None and int(committed_bytes) != actual_bytes:
            # A size mismatch with a matching hash should be impossible; if it
            # happens, something is wrong with the marker rather than the data.
            reasons.append(
                f"committed byte count {committed_bytes} does not match the file "
                f"({actual_bytes})"
            )
            return cls(
                dataset_id=dataset_id, path=str(events_path), sha256=actual_sha,
                event_count=None, state=SealState.CORRUPT, reasons=tuple(reasons),
                dataset_digest=dataset_digest, bytes_sealed=actual_bytes,
            )

        return cls(
            dataset_id=dataset_id,
            path=str(events_path),
            sha256=actual_sha,
            event_count=int(committed_lines) if committed_lines is not None else None,
            state=SealState.SEALED,
            dataset_digest=dataset_digest,
            bytes_sealed=actual_bytes,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "path": self.path,
            "sha256": self.sha256,
            "event_count": self.event_count,
            # Kept distinct from sha256: this is the config digest P01 derives
            # dataset_id from, not a hash of the bytes.
            "dataset_digest": self.dataset_digest,
            "bytes_sealed": self.bytes_sealed,
            "state": self.state.value,
            "sealed": self.sealed,
            "usable_for_completeness_claim": self.usable_for_completeness_claim,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class TopicGeneration:
    """A Kafka topic plus the generation that distinguishes two incarnations.

    Case G01-06: a topic deleted and recreated under the same display name has
    offsets that mean nothing in terms of the old one. The generation is what
    makes a stored cut comparable, so it is part of the identity (INV-08).
    """

    topic: str
    generation: str
    partitions: int

    def __post_init__(self) -> None:
        if self.partitions <= 0:
            raise ManifestError("partitions must be positive", topic=self.topic)
        if not self.generation:
            raise ManifestError("topic generation may not be empty", topic=self.topic)

    @property
    def key(self) -> str:
        return f"{self.topic}@{self.generation}"

    def as_dict(self) -> dict[str, Any]:
        return {"topic": self.topic, "generation": self.generation, "partitions": self.partitions}


@dataclass(frozen=True)
class QueryCatalogEntry:
    """A pinned query: the ID, the exact SQL and the exact binds.

    Case G01-13 is someone selecting Q02 while the script runs Q01. The digest
    of (sql, binds) is what catches it, so both are hashed, and the SQL text is
    kept so a report can show what actually ran.
    """

    qid: str
    sql: str
    binds: Mapping[str, Any]
    client_timeout_ms: int
    settings: Mapping[str, Any] = field(default_factory=dict)

    @property
    def sql_sha256(self) -> str:
        return sha256_text(self.sql)

    @property
    def digest(self) -> str:
        return digest_of(
            {
                "qid": self.qid,
                "sql_sha256": self.sql_sha256,
                "binds": dict(self.binds),
                "client_timeout_ms": self.client_timeout_ms,
                "settings": dict(self.settings),
            }
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "qid": self.qid,
            "sql_sha256": self.sql_sha256,
            "binds": dict(self.binds),
            "client_timeout_ms": self.client_timeout_ms,
            "settings": dict(self.settings),
            "digest": self.digest,
        }


def query_catalog_digest(entries: Iterable[QueryCatalogEntry]) -> str:
    return digest_of(sorted(entry.digest for entry in entries))


@dataclass(frozen=True)
class HostProfile:
    """The environment snapshot section 5.1 requires.

    Every field is optional and defaults to ``None`` rather than to a plausible
    value: an unknown CPU governor has to read as unknown, because case E2E-45
    is a governor changed for one arm of an A/B and nothing else. A profile with
    holes in it still produces a stable digest, and the holes are visible.
    """

    cpu_model: str | None = None
    cpu_cores: int | None = None
    cpu_threads: int | None = None
    smt_enabled: bool | None = None
    numa_nodes: int | None = None
    ram_bytes: int | None = None
    swap_bytes: int | None = None
    storage_model: str | None = None
    filesystem: str | None = None
    mount_options: str | None = None
    kernel: str | None = None
    platform: str | None = None
    cpu_governor: str | None = None
    turbo_enabled: bool | None = None
    cgroup_version: str | None = None
    cpu_quota: str | None = None
    memory_limit_bytes: int | None = None
    nic_speed_bits_per_s: int | None = None
    mtu: int | None = None
    perf_event_paranoid: int | None = None
    container_image_digests: Mapping[str, str] = field(default_factory=dict)
    python_version: str | None = None
    package_hash: str | None = None
    notes: tuple[str, ...] = ()

    @property
    def digest(self) -> str:
        return digest_of(self.as_dict(include_digest=False))

    @property
    def unknown_fields(self) -> list[str]:
        return sorted(
            name for name, value in self.as_dict(include_digest=False).items()
            if value is None
        )

    def as_dict(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "cpu_model": self.cpu_model,
            "cpu_cores": self.cpu_cores,
            "cpu_threads": self.cpu_threads,
            "smt_enabled": self.smt_enabled,
            "numa_nodes": self.numa_nodes,
            "ram_bytes": self.ram_bytes,
            "swap_bytes": self.swap_bytes,
            "storage_model": self.storage_model,
            "filesystem": self.filesystem,
            "mount_options": self.mount_options,
            "kernel": self.kernel,
            "platform": self.platform,
            "cpu_governor": self.cpu_governor,
            "turbo_enabled": self.turbo_enabled,
            "cgroup_version": self.cgroup_version,
            "cpu_quota": self.cpu_quota,
            "memory_limit_bytes": self.memory_limit_bytes,
            "nic_speed_bits_per_s": self.nic_speed_bits_per_s,
            "mtu": self.mtu,
            "perf_event_paranoid": self.perf_event_paranoid,
            "container_image_digests": dict(self.container_image_digests),
            "python_version": self.python_version,
            "package_hash": self.package_hash,
            "notes": list(self.notes),
        }
        if include_digest:
            payload["digest"] = self.digest
            payload["unknown_fields"] = self.unknown_fields
        return payload


class DriftSeverity(str, Enum):
    """What a changed field does to a comparison.

    ``INVALIDATING`` means the run no longer measures what it claims to (the
    dataset changed under it). ``INCOMPARABLE`` means both runs are individually
    fine but may not be compared as an A/B pair (G01-08, E2E-45).
    """

    INVALIDATING = "INVALIDATING"
    INCOMPARABLE = "INCOMPARABLE"
    INFORMATIONAL = "INFORMATIONAL"


@dataclass(frozen=True)
class Drift:
    field_name: str
    before: Any
    after: Any
    severity: DriftSeverity
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "field": self.field_name,
            "before": self.before,
            "after": self.after,
            "severity": self.severity.value,
            "detail": self.detail,
        }


#: How each pinned field is classified when it changes. The split is the point:
#: a different dataset invalidates, a different producer config merely makes two
#: runs incomparable, and a note is informational.
_DRIFT_RULES: dict[str, tuple[DriftSeverity, str]] = {
    "dataset_sha256": (DriftSeverity.INVALIDATING, "the sealed source changed under the run"),
    "dataset_id": (DriftSeverity.INVALIDATING, "a different dataset is being measured"),
    "workload_sha256": (DriftSeverity.INVALIDATING, "the workload manifest changed mid-run"),
    "query_catalog_sha256": (DriftSeverity.INVALIDATING, "the query family is not the one pinned"),
    "topic_generations": (DriftSeverity.INVALIDATING, "stored offsets refer to another topic incarnation"),
    "seed": (DriftSeverity.INVALIDATING, "a different seed produces a different expected set"),
    "config_sha256": (DriftSeverity.INCOMPARABLE, "configuration differs; label as a separate revision"),
    "image_digests": (DriftSeverity.INCOMPARABLE, "a tag was repointed at a different image digest"),
    "host_profile_sha256": (DriftSeverity.INCOMPARABLE, "the environment differs; A/B pair is confounded"),
    "source_commit": (DriftSeverity.INCOMPARABLE, "different source build"),
    "profile": (DriftSeverity.INCOMPARABLE, "cross-profile results are not one experiment"),
    "lane": (DriftSeverity.INFORMATIONAL, "different lane"),
    "test_id": (DriftSeverity.INFORMATIONAL, "different case id"),
}


@dataclass(frozen=True)
class RunManifest:
    """Everything that must be identical for two measurements to be the same thing."""

    run_id: str
    test_id: str
    lane: str
    seed: int
    profile: str
    created_at_utc: str
    workload_sha256: str
    config_sha256: str
    dataset_seal: SourceSeal
    host_profile_sha256: str
    manifest_version: str = "p08.v1"
    topic_generations: tuple[TopicGeneration, ...] = ()
    query_catalog_sha256: str | None = None
    image_digests: Mapping[str, str] = field(default_factory=dict)
    source_commit: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ManifestError("run_id may not be empty")
        if not self.test_id.strip():
            raise ManifestError("test_id may not be empty", run_id=self.run_id)

    # -- identity ---------------------------------------------------------
    def pinned_fields(self) -> dict[str, Any]:
        """The fields drift is computed over."""
        return {
            "dataset_id": self.dataset_seal.dataset_id,
            "dataset_sha256": self.dataset_seal.sha256,
            "workload_sha256": self.workload_sha256,
            "config_sha256": self.config_sha256,
            "query_catalog_sha256": self.query_catalog_sha256,
            "topic_generations": [t.key for t in self.topic_generations],
            "seed": self.seed,
            "image_digests": dict(self.image_digests),
            "host_profile_sha256": self.host_profile_sha256,
            "source_commit": self.source_commit,
            "profile": self.profile,
            "lane": self.lane,
            "test_id": self.test_id,
        }

    @property
    def digest(self) -> str:
        return digest_of(self.pinned_fields())

    # -- comparison -------------------------------------------------------
    def drift_against(self, other: "RunManifest") -> list[Drift]:
        """Every pinned field that differs, classified by what it breaks."""
        mine, theirs = self.pinned_fields(), other.pinned_fields()
        drifts: list[Drift] = []
        for name in sorted(set(mine) | set(theirs)):
            before, after = mine.get(name), theirs.get(name)
            if before == after:
                continue
            severity, detail = _DRIFT_RULES.get(
                name, (DriftSeverity.INFORMATIONAL, "unclassified field changed")
            )
            drifts.append(Drift(name, before, after, severity, detail))
        return drifts

    def comparable_with(self, other: "RunManifest") -> bool:
        """True only when no drift would invalidate or confound a comparison."""
        return not any(
            d.severity in (DriftSeverity.INVALIDATING, DriftSeverity.INCOMPARABLE)
            for d in self.drift_against(other)
        )

    def validity_blockers(self) -> list[str]:
        """Reasons this manifest cannot anchor a measurement at all."""
        blockers: list[str] = []
        if not self.dataset_seal.sealed:
            blockers.append(
                f"source is {self.dataset_seal.state.value}: "
                + "; ".join(self.dataset_seal.reasons)
            )
        elif not self.dataset_seal.usable_for_completeness_claim:
            blockers.append("sealed source is missing its hash or event count")
        if not self.host_profile_sha256:
            blockers.append("no host profile digest; the run is not reproducible (FR-15)")
        return blockers

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "run_id": self.run_id,
            "test_id": self.test_id,
            "lane": self.lane,
            "seed": self.seed,
            "profile": self.profile,
            "created_at_utc": self.created_at_utc,
            "workload_sha256": self.workload_sha256,
            "config_sha256": self.config_sha256,
            "query_catalog_sha256": self.query_catalog_sha256,
            "host_profile_sha256": self.host_profile_sha256,
            "dataset_seal": self.dataset_seal.as_dict(),
            "topic_generations": [t.as_dict() for t in self.topic_generations],
            "image_digests": dict(self.image_digests),
            "source_commit": self.source_commit,
            "manifest_digest": self.digest,
            "validity_blockers": self.validity_blockers(),
        }


class RunIdRegistry:
    """Reserve run ids so two workloads cannot share one (case G01-02).

    Backed by a directory: reservation is "create this directory exclusively",
    which is atomic on every filesystem P08 runs on. An in-memory set would not
    survive two processes, and two processes is exactly the case.
    """

    __slots__ = ("root",)

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def reserve(self, run_id: str) -> Path:
        if not run_id or any(ch in run_id for ch in '/\\:*?"<>|'):
            raise ManifestError("run_id is not a usable directory name", run_id=run_id)
        target = self.root / run_id
        try:
            target.mkdir(parents=False, exist_ok=False)
        except FileExistsError as exc:
            raise ManifestError(
                "run_id is already in use; artifacts from two runs may not share "
                "a directory",
                run_id=run_id,
                path=str(target),
            ) from exc
        return target

    def is_reserved(self, run_id: str) -> bool:
        return (self.root / run_id).exists()
