"""Verification of the Project 01 dataset this codec is fed (cases L02, L03).

Two separate questions live here.

**Is the source trustworthy?** Project 01 writes ``events.jsonl`` through a
``.part`` file, renames it, and only then writes a ``COMPLETED`` marker carrying
the line count and SHA-256. A run killed with ``SIGKILL`` leaves the ``.part``
file and no marker. So a directory with events but no marker is an *incomplete*
dataset, and converting it while reporting success would launder a truncated
file into an apparently verified Protobuf stream. ``require_completed=True``
refuses it (L02). The hash is re-computed, not trusted.

**Which anomalies were deliberate?** ``fault-manifest.jsonl`` is Project 01's
audit trail, kept deliberately outside the event stream. ``fault_passthrough``
mode consults it to decide whether a crossed quote is an injected fault or real
corruption. Without it, the mode refuses to run rather than waving every
anomaly through (L03) -- the entire value of the mode is that it distinguishes
the two, and a mode that cannot distinguish them is worse than no mode.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, Iterable

from .errors import ConfigError, SourceArtifactError

EVENTS_NAME: Final = "events.jsonl"
MANIFEST_NAME: Final = "manifest.json"
COMPLETED_NAME: Final = "COMPLETED"
FAULT_MANIFEST_NAME: Final = "fault-manifest.jsonl"

#: Which Project 01 fault kinds explain which observed business anomaly.
#:
#: This mapping is the crux of fault_passthrough. An anomaly is preserved only
#: when the manifest shows a fault whose kind *could have produced it* at that
#: exact ``(symbol, sequence)``. A negative price covered by a `stale_timestamp`
#: fault is still a rejection: the fault was real but it does not explain this
#: anomaly, and accepting it would mean any manifest entry licenses any
#: corruption on that event.
ANOMALY_TO_FAULT_KINDS: Final[dict[str, frozenset[str]]] = {
    "NONPOSITIVE_PRICE": frozenset({"invalid_price"}),
    "CROSSED_QUOTE": frozenset({"invalid_spread"}),
    # P01 clamps a stale shift at zero, so a large enough backward shift can
    # legitimately produce event_time_ns == 0.
    "NONPOSITIVE_TIME": frozenset({"stale_timestamp"}),
    # Project 01 has no fault that produces either of these, so no manifest
    # entry can ever excuse them -- they are always treated as real corruption.
    "NONPOSITIVE_SEQUENCE": frozenset(),
    "NONPOSITIVE_SCALE": frozenset(),
}


@dataclass(frozen=True)
class FaultRecord:
    """One row of ``fault-manifest.jsonl``."""

    fault_id: str
    kind: str
    applied: bool
    symbol: str | None
    sequences: tuple[int, ...]
    output_impact: str
    channel: str


@dataclass
class FaultIndex:
    """Applied payload faults, indexed by ``(symbol, sequence)``."""

    records: tuple[FaultRecord, ...] = ()
    _by_key: dict[tuple[str, int], tuple[FaultRecord, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        index: dict[tuple[str, int], list[FaultRecord]] = {}
        for record in self.records:
            # Only applied payload faults can explain a value in the data. A
            # planned-but-not-applied fault changed nothing, and a transport
            # fault corrupts bytes on Project 01's TCP path without touching
            # the recorded file.
            if not record.applied or record.channel != "payload":
                continue
            if record.symbol is None:
                continue
            for sequence in record.sequences:
                index.setdefault((record.symbol, sequence), []).append(record)
        self._by_key = {key: tuple(value) for key, value in index.items()}

    @classmethod
    def from_lines(cls, lines: Iterable[bytes | str]) -> "FaultIndex":
        records: list[FaultRecord] = []
        for raw in lines:
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            text = text.strip()
            if not text:
                continue
            row = json.loads(text)
            target = row.get("target") or {}
            sequences = tuple(int(s) for s in row.get("affected_sequences") or ())
            if not sequences and target.get("sequence") is not None:
                sequences = (int(target["sequence"]),)
            records.append(
                FaultRecord(
                    fault_id=str(row["fault_id"]),
                    kind=str(row["kind"]),
                    applied=bool(row.get("applied")),
                    symbol=target.get("symbol"),
                    sequences=sequences,
                    output_impact=str(row.get("output_impact") or ""),
                    channel=str(row.get("channel") or "payload"),
                )
            )
        return cls(records=tuple(records))

    @classmethod
    def load(cls, path: Path) -> "FaultIndex":
        if not path.exists():
            return cls()
        with path.open("rb") as handle:
            return cls.from_lines(handle)

    def explains(self, symbol: str | None, sequence: int | None, anomaly: str) -> FaultRecord | None:
        """The fault record licensing ``anomaly`` on this event, if any."""
        if symbol is None or sequence is None:
            return None
        allowed = ANOMALY_TO_FAULT_KINDS.get(anomaly, frozenset())
        if not allowed:
            return None
        for record in self._by_key.get((symbol, sequence), ()):
            if record.kind in allowed:
                return record
        return None

    @property
    def applied_count(self) -> int:
        return sum(1 for r in self.records if r.applied)

    def fault_ids(self) -> tuple[str, ...]:
        return tuple(r.fault_id for r in self.records)


@dataclass(frozen=True)
class SourceDataset:
    """A verified Project 01 run directory."""

    run_dir: Path
    events_path: Path
    dataset_id: str
    schema_version: str
    events_lines: int
    events_sha256: str
    faults_enabled: bool
    fault_index: FaultIndex
    completed: bool

    @classmethod
    def load(
        cls,
        run_dir: str | Path,
        *,
        require_completed: bool = True,
        verify_hash: bool = True,
    ) -> "SourceDataset":
        """Load and verify a run directory.

        ``require_completed=False`` exists for the negative test that asserts the
        default refuses an incomplete source; it is not a normal operating mode.
        """
        run_dir = Path(run_dir)
        events_path = run_dir / EVENTS_NAME
        manifest_path = run_dir / MANIFEST_NAME
        completed_path = run_dir / COMPLETED_NAME

        if not events_path.exists():
            raise SourceArtifactError(f"no {EVENTS_NAME} in {run_dir}")
        if not manifest_path.exists():
            raise SourceArtifactError(f"no {MANIFEST_NAME} in {run_dir}")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        completed = completed_path.exists()
        if require_completed and not completed:
            raise SourceArtifactError(
                f"{run_dir} has no {COMPLETED_NAME} marker: the source run did not "
                "commit, so it cannot be used as a verified source"
            )

        declared_lines = -1
        declared_sha = ""
        if completed:
            marker = json.loads(completed_path.read_text(encoding="utf-8"))
            declared_lines = int(marker.get("events_lines", -1))
            declared_sha = str(marker.get("events_sha256", ""))

        actual_sha = ""
        if verify_hash:
            actual_sha = _sha256_file(events_path)
            if declared_sha and actual_sha != declared_sha:
                raise SourceArtifactError(
                    f"{EVENTS_NAME} sha256 {actual_sha} does not match the "
                    f"COMPLETED marker's {declared_sha}; the file changed after commit"
                )

        return cls(
            run_dir=run_dir,
            events_path=events_path,
            dataset_id=str(manifest.get("dataset_id", "")),
            schema_version=str(manifest.get("schema_version", "")),
            events_lines=declared_lines,
            events_sha256=actual_sha or declared_sha,
            faults_enabled=bool(manifest.get("faults_enabled", False)),
            fault_index=FaultIndex.load(run_dir / FAULT_MANIFEST_NAME),
            completed=completed,
        )

    def require_fault_manifest(self) -> FaultIndex:
        """The fault index, or a refusal if this dataset has no audit trail.

        Called when entering ``fault_passthrough``: running that mode against a
        dataset with no manifest would accept every anomaly unchecked, which
        defeats the point of having the mode at all (L03).
        """
        if not self.fault_index.records:
            raise ConfigError(
                f"fault_passthrough requires a verified {FAULT_MANIFEST_NAME}, "
                f"but {self.run_dir} has none; refusing to treat every anomaly "
                "as intentional"
            )
        return self.fault_index


def _sha256_file(path: Path, *, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(chunk):
            digest.update(block)
    return digest.hexdigest()


def sha256_file(path: str | Path) -> str:
    """Public helper: artifact hashes go into every report (FR-10)."""
    return _sha256_file(Path(path))
