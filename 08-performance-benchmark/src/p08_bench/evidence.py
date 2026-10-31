"""The evidence store: artifacts, cuts, gaps and their hashes (sections 15, 16, 25).

Section 16 has a sentence that drives this module: links and digests must exist
and be checked, not pasted. And section 25's report template is a list of
``null`` fields with the instruction not to replace them with made-up numbers.

So the store is append-only and self-hashing. Every artifact written gets a
SHA-256 recorded at write time, :meth:`EvidenceStore.verify` re-reads the files
and recomputes, and :meth:`EvidenceStore.completeness` answers "is the bundle
whole" against a declared requirement list rather than against whatever happens
to be present.

:class:`KafkaCut` is the other half. Offsets are the natural way to describe
what reached a broker, and INV-02 forbids them from being identities. The class
therefore exposes ``lag`` and refuses ``identity`` outright, and it carries the
topic generation because a recreated topic makes a stored cut meaningless
(INV-08, case G01-06).

:class:`EvidenceGap` exists for case E2E-44: the exporter disappearing during
the busiest five minutes is a recorded hole, never an interpolated green.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import ManifestError, MetrologyError
from .manifest import digest_of, sha256_file


@dataclass(frozen=True)
class Artifact:
    """One stored file with the hash it had when it was written."""

    name: str
    path: str
    sha256: str
    bytes_written: int
    kind: str = "json"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "sha256": self.sha256,
            "bytes": self.bytes_written,
            "kind": self.kind,
        }


class ArtifactState(str, Enum):
    OK = "OK"
    MISSING = "MISSING"
    MODIFIED = "MODIFIED"


@dataclass(frozen=True)
class EvidenceGap:
    """A window with no observation, and why (case E2E-44)."""

    series: str
    start_ns: int
    end_ns: int
    reason: str

    @property
    def duration_s(self) -> float:
        return (self.end_ns - self.start_ns) / 1e9

    def as_dict(self) -> dict[str, Any]:
        return {
            "series": self.series,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "duration_s": self.duration_s,
            "reason": self.reason,
            "interpolated": False,
        }


@dataclass(frozen=True)
class PartitionCut:
    """Log start, log end and committed offset for one topic-partition.

    All three, because section 4.1 requires it: ``log_end - committed`` is the
    lag, and without ``log_start`` there is no way to tell a drained backlog
    from one that retention deleted (FIX-003).
    """

    topic: str
    partition: int
    generation: str
    log_start_offset: int
    log_end_offset: int
    committed_next_offset: int

    def __post_init__(self) -> None:
        if self.log_end_offset < self.log_start_offset:
            raise ManifestError(
                "log end precedes log start", topic=self.topic, partition=self.partition
            )
        if self.committed_next_offset < self.log_start_offset:
            raise ManifestError(
                "committed offset is below log start; the consumer position "
                "refers to data that no longer exists",
                topic=self.topic,
                partition=self.partition,
            )
        if self.committed_next_offset > self.log_end_offset:
            # INV-07: a commit beyond the log end means the consumer claimed
            # progress past what was ever appended.
            raise ManifestError(
                "committed offset exceeds log end offset",
                topic=self.topic,
                partition=self.partition,
            )

    @property
    def key(self) -> str:
        return f"{self.topic}-{self.partition}@{self.generation}"

    @property
    def lag_records(self) -> int:
        """``log_end - committed``, in offsets.

        An offset count, not a duration and not a number of logical events
        (section 4.1). The name says ``records`` for that reason.
        """
        return self.log_end_offset - self.committed_next_offset

    def as_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "partition": self.partition,
            "generation": self.generation,
            "log_start_offset": self.log_start_offset,
            "log_end_offset": self.log_end_offset,
            "committed_next_offset": self.committed_next_offset,
            "lag_records": self.lag_records,
            "note": "offsets are physical positions, never logical identities (INV-02)",
        }


@dataclass
class KafkaCut:
    """A sealed set of per-partition offsets taken at one instant."""

    taken_at_ns: int
    partitions: dict[str, PartitionCut] = field(default_factory=dict)

    def add(self, cut: PartitionCut) -> None:
        if cut.key in self.partitions:
            raise ManifestError("partition already present in this cut", key=cut.key)
        self.partitions[cut.key] = cut

    @property
    def total_lag_records(self) -> int:
        return sum(cut.lag_records for cut in self.partitions.values())

    def worst_lag(self) -> PartitionCut | None:
        """The partition that governs the verdict (INV-09)."""
        if not self.partitions:
            return None
        return max(self.partitions.values(), key=lambda cut: cut.lag_records)

    def generations(self) -> set[str]:
        return {cut.generation for cut in self.partitions.values()}

    def identity(self) -> None:
        """Always raises. Offsets are not identities (INV-02)."""
        raise MetrologyError(
            "a Kafka offset is a position in one incarnation of one partition "
            "and is never a logical event identity (INV-02)"
        )

    @property
    def digest(self) -> str:
        return digest_of(
            {
                "taken_at_ns": self.taken_at_ns,
                "partitions": {k: v.as_dict() for k, v in sorted(self.partitions.items())},
            }
        )

    def as_dict(self) -> dict[str, Any]:
        worst = self.worst_lag()
        return {
            "taken_at_ns": self.taken_at_ns,
            "partitions": {k: v.as_dict() for k, v in sorted(self.partitions.items())},
            "total_lag_records": self.total_lag_records,
            "worst_partition": worst.key if worst else None,
            "generations": sorted(self.generations()),
            "digest": self.digest,
        }


#: Artifacts a complete bundle must contain (section 25). Declared as a list so
#: "complete" is checked against a requirement, not against what happens to
#: have been written.
REQUIRED_ARTIFACTS: tuple[str, ...] = (
    "run_manifest",
    "host_profile",
    "workload_config",
    "stage_counters",
    "latency_samples",
    "reconciliation",
    "report",
)


class EvidenceStore:
    """Append-only artifact directory with a hash manifest."""

    __slots__ = ("root", "_artifacts", "_gaps", "_cuts", "_write_failures")

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._artifacts: dict[str, Artifact] = {}
        self._gaps: list[EvidenceGap] = []
        self._cuts: dict[str, KafkaCut] = {}
        self._write_failures: list[dict[str, str]] = []

    # -- writing ----------------------------------------------------------
    def write_json(self, name: str, payload: Any, *, overwrite: bool = False) -> Artifact:
        """Write a JSON artifact and record its hash.

        Append-only by default. Overwriting an artifact silently would let a
        later, better-looking run replace the evidence of an earlier one.
        """
        if name in self._artifacts and not overwrite:
            raise ManifestError(
                "artifact already written; the evidence store is append-only",
                name=name,
            )
        path = self.root / f"{name}.json"
        text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        try:
            path.write_text(text, encoding="utf-8")
        except OSError as exc:
            self._write_failures.append({"name": name, "error": str(exc)})
            raise ManifestError("could not write artifact", name=name, error=str(exc)) from exc
        artifact = Artifact(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            bytes_written=len(text.encode("utf-8")),
        )
        self._artifacts[name] = artifact
        return artifact

    def record_gap(self, gap: EvidenceGap) -> None:
        self._gaps.append(gap)

    def record_cut(self, name: str, cut: KafkaCut) -> None:
        self._cuts[name] = cut
        self.write_json(f"cut_{name}", cut.as_dict())

    # -- reading ----------------------------------------------------------
    @property
    def artifacts(self) -> dict[str, Artifact]:
        return dict(self._artifacts)

    @property
    def gaps(self) -> list[EvidenceGap]:
        return list(self._gaps)

    @property
    def write_failures(self) -> list[dict[str, str]]:
        return list(self._write_failures)

    def verify(self) -> dict[str, ArtifactState]:
        """Re-read every artifact and recompare its hash.

        This is what makes a digest a check rather than a decoration: a file
        edited after the run shows up as ``MODIFIED`` (case G01-04 applied to
        the evidence itself).
        """
        states: dict[str, ArtifactState] = {}
        for name, artifact in self._artifacts.items():
            path = Path(artifact.path)
            if not path.is_file():
                states[name] = ArtifactState.MISSING
            elif sha256_file(path) != artifact.sha256:
                states[name] = ArtifactState.MODIFIED
            else:
                states[name] = ArtifactState.OK
        return states

    def completeness(
        self, required: Iterable[str] = REQUIRED_ARTIFACTS
    ) -> dict[str, Any]:
        """Which required artifacts are present, and what that means.

        Case G01-14: a run whose artifacts were lost to a host reboot stays
        ``INCOMPLETE``. There is no reconstruction from a dashboard.
        """
        required_list = list(required)
        present = [name for name in required_list if name in self._artifacts]
        missing = [name for name in required_list if name not in self._artifacts]
        states = self.verify()
        damaged = sorted(k for k, v in states.items() if v is not ArtifactState.OK)
        return {
            "required": required_list,
            "present": present,
            "missing": missing,
            "damaged": damaged,
            "gaps": [gap.as_dict() for gap in self._gaps],
            "write_failures": self.write_failures,
            "complete": not missing and not damaged and not self._write_failures,
            "evidence_interval_has_gaps": bool(self._gaps),
        }

    def artifact_manifest(self) -> dict[str, Any]:
        """The SHA-256 manifest section 25 requires in every report."""
        return {
            "root": str(self.root),
            "artifacts": {
                name: artifact.as_dict() for name, artifact in sorted(self._artifacts.items())
            },
            "cuts": {name: cut.digest for name, cut in sorted(self._cuts.items())},
            "states": {name: state.value for name, state in sorted(self.verify().items())},
        }


def host_profile_from_platform(extra_notes: Iterable[str] = ()) -> Mapping[str, Any]:
    """Best-effort host facts from the standard library.

    Deliberately partial. Section 5.1 asks for CPU governor, NUMA topology, IRQ
    placement and more, none of which the standard library exposes portably.
    The fields this cannot determine stay absent so
    :attr:`~p08_bench.manifest.HostProfile.unknown_fields` lists them, rather
    than being filled with a guess that would make the profile look complete.
    """
    import platform
    import sys

    return {
        "cpu_model": platform.processor() or None,
        "platform": platform.platform(),
        "kernel": platform.release(),
        "python_version": sys.version.split()[0],
        "notes": tuple(extra_notes),
    }
