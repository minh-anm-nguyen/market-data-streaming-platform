"""Independent verifier for a recorded run (sections 5.1, 9.1).

The verifier is deliberately *not* allowed to trust the recorder. It re-reads the
file from disk, recomputes the SHA-256, re-parses every line and re-derives the
anomalies, then compares all of that against the report and the fault manifest.
A recorder bug that wrote a wrong hash or a wrong count has to show up here.

Four verdicts, kept distinct because they call for different actions:

``COMPLETE``
    Marker, report, recomputed hash and line count all agree, and every observed
    anomaly is explained by the fault manifest.
``INCOMPLETE``
    The run did not finish committing: a ``.part`` file, or ``events.jsonl``
    without a ``COMPLETED`` marker (case F17). Not corrupt -- just not a dataset.
``CORRUPT``
    The file exists and claims to be complete, but the bytes disagree with the
    recorded hash, a line does not parse, or the final line is torn.
``INVALID``
    The dataset parses but contradicts itself: counts that do not reconcile, or
    anomalies with no entry in the fault manifest (case C04).

Memory is O(max line) for the hash and the parse. The anomaly tracking keeps one
small record per symbol plus the set of sequences seen, which is what makes gap
and duplicate detection exact; ``--no-anomalies`` skips that for a huge file
(case C13).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterator

from .model import RecordParseError, parse_canonical_line
from .recorder import (
    COMPLETED_NAME,
    EVENTS_NAME,
    EVENTS_PART_NAME,
    FAULT_MANIFEST_NAME,
    MANIFEST_NAME,
    REPORT_NAME,
)

#: Read size for the streaming hash. Large enough to be cheap, small enough that
#: a 200 GB file never costs more than this much resident memory (case C13).
READ_CHUNK = 1 << 20


class Verdict(str, Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    CORRUPT = "CORRUPT"
    INVALID = "INVALID"


#: Escalation order for :meth:`VerifyResult.escalate`.
_SEVERITY = {
    Verdict.COMPLETE: 0,
    Verdict.INVALID: 1,
    Verdict.INCOMPLETE: 2,
    Verdict.CORRUPT: 3,
}


@dataclass
class SymbolObservation:
    """What the file actually shows for one symbol."""

    symbol: str
    first_sequence: int | None = None
    last_sequence: int | None = None
    count: int = 0
    sequences_seen: set[int] = field(default_factory=set)
    gaps: list[int] = field(default_factory=list)
    duplicates: list[int] = field(default_factory=list)
    out_of_order: list[tuple[int, int]] = field(default_factory=list)
    time_regressions: list[tuple[int, int]] = field(default_factory=list)
    conflicting_duplicates: list[int] = field(default_factory=list)
    _payload_by_seq: dict[int, bytes] = field(default_factory=dict, repr=False)
    _last_event_time_ns: int | None = field(default=None, repr=False)

    def finalize(self) -> None:
        """Derive the gaps once the whole file has been read.

        A gap is a sequence that *never arrived*, not one that was skipped at the
        moment of arrival. Computing it on arrival would count every reordered
        event twice -- once as a gap and once as out-of-order -- which would make
        a reorder fault look like two different anomalies.
        """
        if self.first_sequence is None or self.last_sequence is None:
            return
        expected = range(self.first_sequence, self.last_sequence + 1)
        self.gaps = [seq for seq in expected if seq not in self.sequences_seen]

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "count": self.count,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "gaps": sorted(self.gaps),
            "duplicates": sorted(self.duplicates),
            "conflicting_duplicates": sorted(self.conflicting_duplicates),
            "out_of_order": [list(pair) for pair in self.out_of_order],
            "event_time_regressions": [list(pair) for pair in self.time_regressions],
        }


@dataclass
class VerifyResult:
    verdict: Verdict
    run_dir: Path
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    events_path: Path | None = None
    lines: int = 0
    bytes_read: int = 0
    sha256: str | None = None
    expected_sha256: str | None = None
    final_newline: bool = True
    symbols: dict[str, dict[str, Any]] = field(default_factory=dict)
    totals: dict[str, Any] = field(default_factory=dict)
    fault_manifest: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.verdict is Verdict.COMPLETE

    def escalate(self, verdict: Verdict) -> None:
        """Move to a more specific verdict, never back to a milder one.

        An incomplete file will usually *also* fail a reconciliation check, but
        "the run never finished" already explains that, so it must not be
        downgraded to "the data contradicts itself". Severity order: COMPLETE,
        INVALID, INCOMPLETE, CORRUPT.
        """
        if _SEVERITY[verdict] > _SEVERITY[self.verdict]:
            self.verdict = verdict

    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "run_dir": str(self.run_dir),
            "events_path": str(self.events_path) if self.events_path else None,
            "reasons": self.reasons,
            "warnings": self.warnings,
            "lines": self.lines,
            "bytes_read": self.bytes_read,
            "sha256": self.sha256,
            "expected_sha256": self.expected_sha256,
            "final_newline": self.final_newline,
            "totals": self.totals,
            "symbols": self.symbols,
            "fault_manifest": self.fault_manifest,
        }


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def iter_lines(path: Path, *, max_line_bytes: int) -> Iterator[tuple[int, bytes, bool]]:
    """Yield ``(line_number, line_without_newline, had_newline)``.

    Streams by chunk so memory stays bounded, and enforces ``max_line_bytes`` so
    a file of one enormous line cannot exhaust RAM (case N15 on disk).
    """
    line_number = 0
    buffer = bytearray()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(READ_CHUNK)
            if not chunk:
                break
            buffer.extend(chunk)
            start = 0
            while True:
                idx = buffer.find(b"\n", start)
                if idx == -1:
                    break
                line_number += 1
                yield line_number, bytes(buffer[start:idx]), True
                start = idx + 1
            del buffer[:start]
            if len(buffer) > max_line_bytes:
                line_number += 1
                raise RecordParseError(
                    "line_exceeds_max_bytes",
                    f"line {line_number} exceeds {max_line_bytes} bytes",
                )
    if buffer:
        # A trailing fragment with no newline: the writer was interrupted
        # mid-record, or the file was truncated (case F08).
        line_number += 1
        yield line_number, bytes(buffer), False


def hash_file(path: Path) -> tuple[str, int]:
    """Streaming SHA-256 and byte count. O(READ_CHUNK) memory."""
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(READ_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def verify_run(
    run_dir: str | Path,
    *,
    max_line_bytes: int = 1 << 20,
    check_anomalies: bool = True,
    keep_payloads: bool = True,
) -> VerifyResult:
    """Verify one run directory. Never raises for bad data -- it reports."""
    path = Path(run_dir).resolve()
    result = VerifyResult(verdict=Verdict.COMPLETE, run_dir=path)

    if not path.is_dir():
        result.escalate(Verdict.INCOMPLETE)
        result.reasons.append(f"run directory does not exist: {path}")
        return result

    manifest = _read_json(path / MANIFEST_NAME)
    report = _read_json(path / REPORT_NAME)
    marker = _read_json(path / COMPLETED_NAME)
    events = path / EVENTS_NAME
    part = path / EVENTS_PART_NAME

    if manifest is None:
        result.reasons.append(f"{MANIFEST_NAME} missing or unreadable")

    mode = (manifest or {}).get("mode") or (report or {}).get("mode")
    if mode == "serve":
        # serve mode records nothing by design; there is no dataset to verify.
        if report is None:
            result.escalate(Verdict.INCOMPLETE)
            result.reasons.append(f"{REPORT_NAME} missing or unreadable")
        # A warning, not a reason: reasons are failures, and serve mode recording
        # nothing is the design, not a fault.
        result.warnings.append(
            "mode=serve produces no recorded dataset; only the run report is "
            "verifiable, and no TCP event is proven delivered"
        )
        result.totals = {"mode": "serve", "report_present": report is not None}
        return result

    if events.is_file():
        result.events_path = events
    elif part.is_file():
        result.events_path = part
        result.escalate(Verdict.INCOMPLETE)
        result.reasons.append(
            f"{EVENTS_PART_NAME} is still present: the run never finalised, so this "
            "is not a dataset"
        )
    else:
        result.escalate(Verdict.INCOMPLETE)
        result.reasons.append("no events file found (neither .jsonl nor .jsonl.part)")
        return result

    if marker is None:
        result.reasons.append(
            f"{COMPLETED_NAME} marker missing: a committed run writes it last, so "
            "its absence means the run did not finish committing"
        )
        result.escalate(Verdict.INCOMPLETE)
    if report is None:
        result.reasons.append(f"{REPORT_NAME} missing or unreadable")
        result.escalate(Verdict.INCOMPLETE)

    try:
        actual_sha, size = hash_file(result.events_path)
    except OSError as exc:
        result.escalate(Verdict.CORRUPT)
        result.reasons.append(f"cannot read events file: {exc}")
        return result
    result.sha256 = actual_sha
    result.bytes_read = size

    expected_sha = (marker or {}).get("events_sha256") or (
        (report or {}).get("file", {}) or {}
    ).get("sha256")
    result.expected_sha256 = expected_sha
    if expected_sha and expected_sha != actual_sha:
        result.escalate(Verdict.CORRUPT)
        result.reasons.append(
            f"SHA-256 mismatch: recomputed {actual_sha}, recorded {expected_sha}. "
            "The bytes on disk are not the bytes that were committed."
        )

    parse_errors: list[str] = []
    observations: dict[str, SymbolObservation] = {}
    dataset_ids: set[str] = set()
    schema_versions: set[str] = set()
    type_counts: dict[str, int] = {}
    lines = 0
    final_newline = True

    try:
        for line_number, raw, had_newline in iter_lines(
            result.events_path, max_line_bytes=max_line_bytes
        ):
            lines += 1
            final_newline = had_newline
            if not had_newline:
                result.reasons.append(
                    f"line {line_number} has no terminating newline: the file ends "
                    "mid-record"
                )
            if not raw.strip():
                parse_errors.append(f"line {line_number}: empty line")
                continue
            try:
                event = parse_canonical_line(raw, max_bytes=max_line_bytes)
            except RecordParseError as exc:
                parse_errors.append(f"line {line_number}: {exc.reason} {exc.detail}")
                continue
            dataset_ids.add(event.dataset_id)
            schema_versions.add(event.schema_version)
            type_counts[event.event_type] = type_counts.get(event.event_type, 0) + 1
            if check_anomalies:
                _observe(observations, event, raw, keep_payloads=keep_payloads)
    except RecordParseError as exc:
        result.escalate(Verdict.CORRUPT)
        result.reasons.append(f"{exc.reason}: {exc.detail}")
        return result
    except OSError as exc:
        result.escalate(Verdict.CORRUPT)
        result.reasons.append(f"read error while parsing: {exc}")
        return result

    result.lines = lines
    result.final_newline = final_newline
    if parse_errors:
        result.escalate(Verdict.CORRUPT)
        result.reasons.extend(parse_errors[:20])
        if len(parse_errors) > 20:
            result.reasons.append(f"... and {len(parse_errors) - 20} more parse errors")

    if len(dataset_ids) > 1:
        result.escalate(Verdict.INVALID)
        result.reasons.append(
            f"file mixes {len(dataset_ids)} dataset ids {sorted(dataset_ids)}; a run "
            "directory must hold exactly one dataset"
        )
    if manifest and dataset_ids and manifest.get("dataset_id") not in dataset_ids:
        result.escalate(Verdict.INVALID)
        result.reasons.append(
            f"records carry dataset_id {sorted(dataset_ids)} but the manifest says "
            f"{manifest.get('dataset_id')!r}"
        )

    totals = {
        "lines": lines,
        "dataset_ids": sorted(dataset_ids),
        "schema_versions": sorted(schema_versions),
        "event_types": dict(sorted(type_counts.items())),
        "gaps": 0,
        "duplicates": 0,
        "conflicting_duplicates": 0,
        "out_of_order": 0,
        "event_time_regressions": 0,
    }
    for obs in observations.values():
        obs.finalize()
        totals["gaps"] += len(obs.gaps)
        totals["duplicates"] += len(obs.duplicates)
        totals["conflicting_duplicates"] += len(obs.conflicting_duplicates)
        totals["out_of_order"] += len(obs.out_of_order)
        totals["event_time_regressions"] += len(obs.time_regressions)
        result.symbols[obs.symbol] = obs.as_dict()
    result.totals = totals

    if report is not None:
        _cross_check_report(result, report, totals, lines)
    if check_anomalies:
        _cross_check_faults(result, path, totals)

    # COMPLETE is the starting point, so reaching here with no reason recorded
    # means every check passed. The marker and report are still required: a clean
    # file that was never committed is INCOMPLETE, not COMPLETE (case F17).
    if marker is None or report is None:
        result.escalate(Verdict.INCOMPLETE)
    return result


def _observe(
    observations: dict[str, SymbolObservation],
    event: Any,
    raw: bytes,
    *,
    keep_payloads: bool,
) -> None:
    obs = observations.get(event.symbol)
    if obs is None:
        obs = SymbolObservation(symbol=event.symbol)
        observations[event.symbol] = obs

    seq = event.sequence
    obs.count += 1
    if obs.first_sequence is None:
        obs.first_sequence = seq

    if seq in obs.sequences_seen:
        obs.duplicates.append(seq)
        previous = obs._payload_by_seq.get(seq)
        if keep_payloads and previous is not None and previous != raw:
            # Same (dataset, symbol, sequence), different bytes. Deduplicating by
            # key would *hide* this; it has to be reported as a conflict (S03).
            obs.conflicting_duplicates.append(seq)
    else:
        obs.sequences_seen.add(seq)
        if keep_payloads:
            obs._payload_by_seq[seq] = raw
        if obs.last_sequence is not None and seq < obs.last_sequence:
            obs.out_of_order.append((obs.last_sequence, seq))
    if obs.last_sequence is None or seq > obs.last_sequence:
        obs.last_sequence = seq

    if obs._last_event_time_ns is not None and event.event_time_ns < obs._last_event_time_ns:
        obs.time_regressions.append((obs._last_event_time_ns, event.event_time_ns))
    obs._last_event_time_ns = max(
        event.event_time_ns, obs._last_event_time_ns or event.event_time_ns
    )


def _cross_check_report(
    result: VerifyResult, report: dict[str, Any], totals: dict[str, Any], lines: int
) -> None:
    counts = report.get("counts") or {}
    recorded = counts.get("recorded_events")
    if recorded is not None and recorded != lines:
        result.escalate(Verdict.INVALID)
        result.reasons.append(
            f"report claims {recorded} recorded events but the file holds {lines} "
            "lines"
        )
    slots = counts.get("logical_slots")
    emitted = counts.get("emitted_events")
    omissions = counts.get("intentional_omissions")
    duplicates = counts.get("intentional_duplicates")
    if None not in (slots, emitted, omissions, duplicates):
        expected = slots - omissions + duplicates
        if emitted != expected:
            result.escalate(Verdict.INVALID)
            result.reasons.append(
                f"counts do not reconcile: emitted={emitted} but "
                f"slots - omissions + duplicates = {expected}"
            )
    planned = report.get("planned_logical_slots")
    if planned is not None and slots is not None and planned != slots:
        # Falling short of the plan is only a contradiction when the run also
        # claims it ran to the end. A run stopped by a signal or an explicit stop
        # request completes cleanly and legitimately short (NFR-05), and says so
        # in exit_reason -- so the verifier holds the report to its own statement
        # rather than assuming COMPLETED means "reached the count".
        exit_reason = report.get("exit_reason") or ""
        claims_full_count = exit_reason == "slot_count_reached"
        if report.get("status") == "COMPLETED" and claims_full_count:
            result.escalate(Verdict.INVALID)
            result.reasons.append(
                f"the report says the run reached its slot count, but it produced "
                f"{slots} logical slots against a plan of {planned}"
            )
        else:
            result.warnings.append(
                f"run produced {slots} of {planned} planned slots "
                f"(status={report.get('status')}, exit_reason={exit_reason!r})"
            )


def _cross_check_faults(
    result: VerifyResult, run_dir: Path, totals: dict[str, Any]
) -> None:
    """Every observed anomaly must be explained by an applied fault, and vice versa."""
    manifest_path = run_dir / FAULT_MANIFEST_NAME
    if not manifest_path.is_file():
        unexplained = (
            totals["gaps"]
            + totals["duplicates"]
            + totals["out_of_order"]
            + totals["event_time_regressions"]
        )
        if unexplained:
            result.escalate(Verdict.INVALID)
            result.reasons.append(
                f"{unexplained} anomalies in the data but no {FAULT_MANIFEST_NAME}: "
                "in normal mode the dataset must be clean"
            )
        return

    records: list[dict[str, Any]] = []
    try:
        for raw in manifest_path.read_text(encoding="utf-8").splitlines():
            if raw.strip():
                records.append(json.loads(raw))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        result.escalate(Verdict.INVALID)
        result.reasons.append(f"cannot read {FAULT_MANIFEST_NAME}: {exc}")
        return

    applied = [r for r in records if r.get("applied")]
    by_kind: dict[str, int] = {}
    for record in applied:
        by_kind[record["kind"]] = by_kind.get(record["kind"], 0) + 1
    result.fault_manifest = {
        "planned": len(records),
        "applied": len(applied),
        "not_applied": len(records) - len(applied),
        "applied_by_kind": dict(sorted(by_kind.items())),
        "not_applied_reasons": [
            {"fault_id": r["fault_id"], "kind": r["kind"], "reason": r.get("reason_if_not_applied")}
            for r in records
            if not r.get("applied")
        ],
    }

    expected_gaps = by_kind.get("missing_sequence", 0)
    expected_dupes = by_kind.get("duplicate_exact", 0) + by_kind.get(
        "duplicate_conflict", 0
    )
    expected_reorders = by_kind.get("reorder_adjacent", 0)
    expected_regressions = by_kind.get("stale_timestamp", 0)

    if totals["gaps"] != expected_gaps:
        result.escalate(Verdict.INVALID)
        result.reasons.append(
            f"observed {totals['gaps']} sequence gap(s) but the fault manifest "
            f"applied {expected_gaps} missing_sequence fault(s)"
        )
    if totals["duplicates"] != expected_dupes:
        result.escalate(Verdict.INVALID)
        result.reasons.append(
            f"observed {totals['duplicates']} duplicate(s) but the fault manifest "
            f"applied {expected_dupes} duplicate fault(s)"
        )
    if totals["out_of_order"] != expected_reorders:
        result.escalate(Verdict.INVALID)
        result.reasons.append(
            f"observed {totals['out_of_order']} out-of-order event(s) but the fault "
            f"manifest applied {expected_reorders} reorder fault(s)"
        )
    if totals["event_time_regressions"] > expected_regressions + expected_reorders:
        result.warnings.append(
            f"observed {totals['event_time_regressions']} event-time regressions "
            f"against {expected_regressions} stale_timestamp and "
            f"{expected_reorders} reorder faults; a reorder also moves timestamps"
        )
    if by_kind.get("duplicate_conflict", 0) and not totals["conflicting_duplicates"]:
        result.escalate(Verdict.INVALID)
        result.reasons.append(
            "manifest applied duplicate_conflict but no conflicting payloads were "
            "found in the file"
        )
