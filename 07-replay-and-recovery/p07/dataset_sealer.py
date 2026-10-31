"""Turn a P01 run directory into a sealed, verified source of truth.

Nothing downstream in P07 may read the source until it has been through here.
That is the whole reason this module exists: every expected set is derived from
the source, so if the source is unproven, every verdict built on it is unproven
too (design 3.3 step 1).

What "sealed" means, concretely, against P01's commit protocol:

* ``events.jsonl.part`` must be **absent**. A ``.part`` file is an open writer's
  buffer, never a dataset (case ``P07-G01-01``).
* ``COMPLETED`` and ``report.json`` must both exist. P01 writes the marker last,
  so its presence is what distinguishes a committed file from one that merely
  got renamed before a crash.
* The SHA-256 of ``events.jsonl`` is **recomputed here** and compared with
  ``report.json``. Reading the recorded hash and printing it back would prove
  nothing.
* The line count is recomputed and compared with the report, which catches a
  truncated tail that still hashes as a valid file prefix would not
  (case ``P07-G01-02``).

And the limit worth being honest about: if an attacker can rewrite both
``events.jsonl`` and ``report.json``, self-consistency proves nothing. That is
what ``anchor`` is for -- a digest pinned somewhere outside the run directory.
Without one, :func:`seal_dataset` still seals, but it records
``anchor_verified=False`` and the gate downgrades any claim that depends on
source integrity to ``INCONCLUSIVE`` rather than pretending otherwise
(case ``P07-G01-03``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

from .contracts import SOURCE_SCHEMA_VERSION, EventKind, SemanticCore, SourceIdentity
from .evidence.verdicts import Reason

EVENTS_NAME = "events.jsonl"
EVENTS_PART_NAME = "events.jsonl.part"
MANIFEST_NAME = "manifest.json"
REPORT_NAME = "report.json"
COMPLETED_NAME = "COMPLETED"
FAULT_MANIFEST_NAME = "fault-manifest.jsonl"

#: Read the source in chunks rather than slurping it. A capacity fixture is
#: allowed to be larger than RAM, and hashing is the one place that would
#: otherwise force the whole file into memory.
_HASH_CHUNK = 1 << 20


class SealError(Exception):
    """A source that cannot be trusted. Carries a :class:`Reason` code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class SymbolPolicy:
    """Per-symbol limits, read from the sealed P01 config.

    The acceptance oracle needs these to decide business validity, and taking
    them from the manifest rather than from a P07 constant is what makes the
    oracle track the dataset it is judging.
    """

    symbol: str
    price_scale: int
    min_price_ticks: int
    max_price_ticks: int
    min_spread_ticks: int
    max_spread_ticks: int
    max_step_ticks: int


@dataclass(frozen=True, slots=True)
class FaultRecord:
    """One entry of P01's ``fault-manifest.jsonl``.

    Kept apart from the event stream on purpose: P03 and the oracles must detect
    an anomaly themselves rather than be handed the answer key (P01 section 7).
    P07 reads it only to tell an *intentional* anomaly from transport loss
    (``INV-15``).
    """

    fault_id: str
    kind: str
    applied: bool
    symbol: str | None
    sequence: int | None
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SealedDataset:
    """A source proven to be complete, with the metadata the oracles need."""

    run_dir: Path
    events_path: Path
    dataset_id: str
    schema_version: str
    seed: int
    logical_start_time_ns: int
    planned_logical_slots: int
    events_sha256: str
    events_bytes: int
    event_lines: int
    config_sha256: str
    fault_plan_fingerprint: str
    faults_enabled: bool
    symbols: Mapping[str, SymbolPolicy]
    symbol_order: tuple[str, ...]
    fault_records: tuple[FaultRecord, ...]
    anchor_verified: bool
    runtime: Mapping[str, Any]

    def iter_raw_lines(self) -> Iterator[tuple[int, str]]:
        """Yield ``(line_number, text)`` for every record, 1-based.

        A generator rather than a list: the capacity profiles in design 13 run
        to tens of millions of events and the diff machinery is built to stream.
        """
        with self.events_path.open("r", encoding="utf-8", newline="") as handle:
            for number, line in enumerate(handle, start=1):
                yield number, line.rstrip("\n")

    def seal_receipt(self) -> dict[str, Any]:
        """Machine-readable proof of what was sealed, for the evidence bundle."""
        return {
            "dataset_id": self.dataset_id,
            "run_dir": str(self.run_dir),
            "events_sha256": self.events_sha256,
            "events_bytes": self.events_bytes,
            "event_lines": self.event_lines,
            "config_sha256": self.config_sha256,
            "schema_version": self.schema_version,
            "seed": self.seed,
            "logical_start_time_ns": self.logical_start_time_ns,
            "planned_logical_slots": self.planned_logical_slots,
            "fault_plan_fingerprint": self.fault_plan_fingerprint,
            "faults_enabled": self.faults_enabled,
            "faults_declared": len(self.fault_records),
            "faults_applied": sum(1 for record in self.fault_records if record.applied),
            "symbols": sorted(self.symbols),
            "anchor_verified": self.anchor_verified,
            "source_runtime": dict(self.runtime),
        }


def sha256_file(path: Path) -> tuple[str, int]:
    """``(hex_digest, byte_count)`` for ``path``, read in chunks."""
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _load_json(path: Path, reason: str) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SealError(reason, f"{path.name}: {exc}") from exc
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SealError(reason, f"{path.name}: {exc.msg}") from exc
    if not isinstance(obj, dict):
        raise SealError(reason, f"{path.name}: not an object")
    return obj


def _require_int(obj: Mapping[str, Any], key: str, *, where: str) -> int:
    value = obj.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, f"{where}.{key}={value!r}")
    return value


def _parse_symbols(manifest: Mapping[str, Any]) -> dict[str, SymbolPolicy]:
    config = manifest.get("config_resolved")
    if not isinstance(config, dict):
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, "config_resolved missing")
    data = config.get("data")
    if not isinstance(data, dict):
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, "config_resolved.data missing")
    entries = data.get("symbols")
    if not isinstance(entries, list) or not entries:
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, "no symbols in config")
    out: dict[str, SymbolPolicy] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise SealError(Reason.SOURCE_MANIFEST_INVALID, "symbol entry not an object")
        name = entry.get("symbol")
        if not isinstance(name, str) or not name:
            raise SealError(Reason.SOURCE_MANIFEST_INVALID, f"bad symbol name {name!r}")
        if name in out:
            raise SealError(Reason.SOURCE_MANIFEST_INVALID, f"duplicate symbol {name}")
        where = f"symbols[{name}]"
        policy = SymbolPolicy(
            symbol=name,
            price_scale=_require_int(entry, "price_scale", where=where),
            min_price_ticks=_require_int(entry, "min_price_ticks", where=where),
            max_price_ticks=_require_int(entry, "max_price_ticks", where=where),
            min_spread_ticks=_require_int(entry, "min_spread_ticks", where=where),
            max_spread_ticks=_require_int(entry, "max_spread_ticks", where=where),
            max_step_ticks=_require_int(entry, "max_step_ticks", where=where),
        )
        if policy.price_scale <= 0:
            raise SealError(Reason.SOURCE_MANIFEST_INVALID, f"{where}.price_scale<=0")
        out[name] = policy
    return out


def _parse_fault_manifest(path: Path) -> tuple[FaultRecord, ...]:
    if not path.exists():
        return ()
    records: list[FaultRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SealError(
                    Reason.SOURCE_MANIFEST_INVALID,
                    f"{FAULT_MANIFEST_NAME}:{number}: {exc.msg}",
                ) from exc
            if not isinstance(obj, dict):
                raise SealError(
                    Reason.SOURCE_MANIFEST_INVALID, f"{FAULT_MANIFEST_NAME}:{number}: not an object"
                )
            kind = obj.get("kind")
            if not isinstance(kind, str):
                raise SealError(
                    Reason.SOURCE_MANIFEST_INVALID, f"{FAULT_MANIFEST_NAME}:{number}: no kind"
                )
            sequence = obj.get("sequence")
            records.append(
                FaultRecord(
                    fault_id=str(obj.get("fault_id", f"line-{number}")),
                    kind=kind,
                    # P01 records planned-but-not-applied faults explicitly; a
                    # fault aimed at a slot that never existed must not be read
                    # as an anomaly that happened (P01 cases S11, C04).
                    applied=bool(obj.get("applied", False)),
                    symbol=obj.get("symbol") if isinstance(obj.get("symbol"), str) else None,
                    sequence=sequence if isinstance(sequence, int) else None,
                    detail={k: v for k, v in obj.items() if k not in {"fault_id", "kind", "applied"}},
                )
            )
    return tuple(records)


def seal_dataset(
    run_dir: str | Path,
    *,
    anchor: str | None = None,
    require_fault_plan: bool = False,
    expect_schema_version: str = SOURCE_SCHEMA_VERSION,
) -> SealedDataset:
    """Verify and seal the P01 run directory at ``run_dir``.

    ``anchor``
        A SHA-256 of ``events.jsonl`` held outside the run directory. When
        given it is checked and a mismatch raises
        :data:`~p07.evidence.verdicts.Reason.SOURCE_ANCHOR_MISMATCH`; when
        omitted the dataset is sealed with ``anchor_verified=False``.
    ``require_fault_plan``
        Set by a fault-replay scenario. A run whose
        ``fault_plan_fingerprint`` is ``"none"`` cannot support a deterministic
        fault-replay claim, so sealing refuses it (case ``P07-G01-14``).

    Raises :class:`SealError` on anything unproven. It never repairs, trims or
    rewrites the source -- a source that needs fixing is quarantined, because
    mutating it to make a test pass is the failure mode the design calls out by
    name in the ``dataset_sealer`` row of section 2.1.
    """
    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        raise SealError(Reason.SOURCE_UNAVAILABLE, f"{run_dir} is not a directory")

    if (run_dir / EVENTS_PART_NAME).exists():
        raise SealError(Reason.SOURCE_PART_FILE_PRESENT, EVENTS_PART_NAME)
    if not (run_dir / COMPLETED_NAME).exists():
        raise SealError(Reason.SOURCE_NOT_COMPLETED, f"no {COMPLETED_NAME} marker")

    events_path = run_dir / EVENTS_NAME
    if not events_path.is_file():
        raise SealError(Reason.SOURCE_UNAVAILABLE, EVENTS_NAME)

    manifest = _load_json(run_dir / MANIFEST_NAME, Reason.SOURCE_MANIFEST_INVALID)
    report = _load_json(run_dir / REPORT_NAME, Reason.SOURCE_NOT_COMPLETED)

    commit = report.get("file", {})
    commit = commit.get("commit", {}) if isinstance(commit, dict) else {}
    if not isinstance(commit, dict) or not commit.get("completed_marker"):
        raise SealError(Reason.SOURCE_NOT_COMPLETED, "report.file.commit incomplete")

    actual_sha, actual_bytes = sha256_file(events_path)
    recorded_sha = commit.get("events_sha256")
    if not isinstance(recorded_sha, str) or not recorded_sha:
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, "report has no events_sha256")
    if actual_sha != recorded_sha:
        raise SealError(
            Reason.SOURCE_HASH_MISMATCH, f"recomputed {actual_sha} != reported {recorded_sha}"
        )

    recorded_lines = commit.get("events_lines")
    if not isinstance(recorded_lines, int) or isinstance(recorded_lines, bool):
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, "report has no events_lines")
    actual_lines = sum(1 for _ in events_path.open("r", encoding="utf-8", newline=""))
    if actual_lines != recorded_lines:
        raise SealError(
            Reason.SOURCE_COUNT_MISMATCH, f"counted {actual_lines} != reported {recorded_lines}"
        )

    schema_version = manifest.get("schema_version")
    if schema_version != expect_schema_version:
        raise SealError(
            Reason.SOURCE_SCHEMA_SKEW, f"manifest {schema_version!r} != {expect_schema_version!r}"
        )

    dataset_id = manifest.get("dataset_id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, f"dataset_id={dataset_id!r}")

    fingerprint = manifest.get("fault_plan_fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint:
        raise SealError(Reason.SOURCE_MANIFEST_INVALID, "fault_plan_fingerprint missing")
    if require_fault_plan and fingerprint == "none":
        raise SealError(Reason.SOURCE_FAULT_PLAN_UNHASHED, "fault replay needs a hashed plan")

    anchor_verified = False
    if anchor is not None:
        if anchor.lower() != actual_sha:
            raise SealError(Reason.SOURCE_ANCHOR_MISMATCH, f"anchor {anchor} != file {actual_sha}")
        anchor_verified = True

    symbols = _parse_symbols(manifest)
    config = manifest["config_resolved"]["data"]
    order = config.get("symbol_order")
    symbol_order = tuple(order) if isinstance(order, list) else tuple(sorted(symbols))

    runtime = manifest.get("runtime")
    return SealedDataset(
        run_dir=run_dir,
        events_path=events_path,
        dataset_id=dataset_id,
        schema_version=str(schema_version),
        seed=_require_int(manifest, "seed", where="manifest"),
        logical_start_time_ns=_require_int(manifest, "logical_start_time_ns", where="manifest"),
        planned_logical_slots=_require_int(manifest, "planned_logical_slots", where="manifest"),
        events_sha256=actual_sha,
        events_bytes=actual_bytes,
        event_lines=actual_lines,
        config_sha256=str(manifest.get("config_sha256", "")),
        fault_plan_fingerprint=fingerprint,
        faults_enabled=bool(manifest.get("faults_enabled", False)),
        symbols=symbols,
        symbol_order=symbol_order,
        fault_records=_parse_fault_manifest(run_dir / FAULT_MANIFEST_NAME),
        anchor_verified=anchor_verified,
        runtime=runtime if isinstance(runtime, dict) else {},
    )


def check_dataset_id_collision(datasets: list[SealedDataset]) -> list[tuple[str, str, str]]:
    """Find fixtures that share a ``dataset_id`` but not their contents.

    Two sealed sources with the same ``dataset_id`` and different event hashes
    would make ``(dataset_id, symbol, sequence)`` ambiguous and let a diff
    silently merge two datasets. Returns ``(dataset_id, sha_a, sha_b)`` triples
    so the caller can fail with
    :data:`~p07.evidence.verdicts.Reason.SOURCE_DATASET_ID_COLLISION`
    (case ``P07-G01-04``).
    """
    seen: dict[str, SealedDataset] = {}
    collisions: list[tuple[str, str, str]] = []
    for dataset in datasets:
        other = seen.get(dataset.dataset_id)
        if other is None:
            seen[dataset.dataset_id] = dataset
        elif other.events_sha256 != dataset.events_sha256:
            collisions.append((dataset.dataset_id, other.events_sha256, dataset.events_sha256))
    return collisions


def semantic_core_from_source(obj: Mapping[str, Any]) -> SemanticCore:
    """Project one parsed P01 record onto a :class:`SemanticCore`.

    ``bid``/``ask``/``price`` keep their ``None`` when P01 omitted them, which
    is what carries field presence into the digest. Validation is *not* done
    here -- a fault-injected dataset is expected to contain records that violate
    business rules, and deciding what to do about that is the acceptance
    oracle's job, not the projection's.
    """
    return SemanticCore(
        dataset_id=obj["dataset_id"],
        symbol=obj["symbol"],
        sequence=obj["sequence"],
        event_time_ns=obj["event_time_ns"],
        price_scale=obj["price_scale"],
        event_kind=EventKind.from_wire_name(obj["event_type"]),
        source_schema_version=obj["schema_version"],
        bid_ticks=obj.get("bid_ticks"),
        ask_ticks=obj.get("ask_ticks"),
        price_ticks=obj.get("price_ticks"),
    )


def identity_of(obj: Mapping[str, Any]) -> SourceIdentity:
    return SourceIdentity(obj["dataset_id"], obj["symbol"], obj["sequence"])
