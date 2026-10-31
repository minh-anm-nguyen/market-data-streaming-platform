"""Deterministic fault injection and its audit manifest (section 7).

Design constraints that drive the shape of this module:

* Faults are addressed by ``(symbol, sequence)`` or ``global_slot_index`` --
  **never** by wall-clock time, so the same plan produces the same dataset on a
  fast machine and on a slow one.
* The audit trail goes to ``fault-manifest.jsonl``, which is **not** part of the
  event stream. Project 03's C++ adapter has to detect the anomaly itself; it
  must not be handed the answer key (section 7.1).
* Semantic faults alter the payload, so they affect every sink. Transport faults
  (truncation, malformed bytes, mid-stream close) are applied **only on the TCP
  path**, so the recorded golden dataset stays well-formed (invariant 8).
* ``planned`` and ``applied`` are tracked separately. A fault aimed at a slot
  that never existed is reported as planned-but-not-applied with a reason -- it
  is never silently counted as a success (cases S11, C04).

Emitted-count algebra, which the verifier re-checks::

    emitted == slots - omissions + duplicates

Reordering moves events without changing the count; mutations change bytes
without changing the count.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Iterable, Iterator

from .errors import FaultPlanError
from .model import EVENT_QUOTE, Event

#: Faults that rewrite or withhold the payload itself. They reach every sink.
SEMANTIC_KINDS: Final = frozenset(
    {
        "missing_sequence",
        "duplicate_exact",
        "duplicate_conflict",
        "reorder_adjacent",
        "stale_timestamp",
        "timestamp_future",
        "invalid_spread",
        "invalid_price",
    }
)

#: Faults that corrupt the byte stream or the connection. TCP path only -- mixing
#: these into the recorder would destroy the golden dataset we compare against.
TRANSPORT_KINDS: Final = frozenset({"truncated_line", "malformed_json", "close_connection"})

#: Driven by the test client, not by the server. Accepted in a plan so a scenario
#: can be described in one file, but recorded as not-applied by the injector.
CLIENT_KINDS: Final = frozenset({"throttle_client"})

ALL_KINDS: Final = SEMANTIC_KINDS | TRANSPORT_KINDS | CLIENT_KINDS

_FAULT_KEYS: Final = {
    "kind",
    "symbol",
    "sequence",
    "global_slot_index",
    "offset_ms",
    "priority",
    "bid_delta_ticks",
    "price_ticks",
    "spread_ticks",
    "truncate_bytes",
    "payload",
}

#: Cap on how many events the reorder buffer may hold at once. Reordering is the
#: only fault that needs to retain an event, and an unbounded hold would be an
#: unbounded memory leak driven by config (case S04).
MAX_REORDER_HOLD: Final = 1024


@dataclass(frozen=True, slots=True)
class FaultSpec:
    """One planned fault, fully resolved."""

    fault_id: str
    kind: str
    symbol: str | None = None
    sequence: int | None = None
    global_slot_index: int | None = None
    offset_ms: int = 0
    priority: int = 0
    bid_delta_ticks: int | None = None
    price_ticks: int | None = None
    spread_ticks: int | None = None
    truncate_bytes: int | None = None
    payload: str | None = None

    @property
    def is_semantic(self) -> bool:
        return self.kind in SEMANTIC_KINDS

    @property
    def is_transport(self) -> bool:
        return self.kind in TRANSPORT_KINDS

    @property
    def is_client_side(self) -> bool:
        return self.kind in CLIENT_KINDS

    @property
    def target_key(self) -> tuple[str, object]:
        if self.global_slot_index is not None:
            return ("slot", self.global_slot_index)
        return ("seq", (self.symbol, self.sequence))

    def target_dict(self) -> dict[str, Any]:
        if self.global_slot_index is not None:
            return {"global_slot_index": self.global_slot_index}
        return {"symbol": self.symbol, "sequence": self.sequence}


@dataclass
class FaultRecord:
    """An audit row for ``fault-manifest.jsonl``."""

    fault_id: str
    kind: str
    target: dict[str, Any]
    planned: bool = True
    applied: bool = False
    affected_sequences: list[int] = field(default_factory=list)
    output_impact: str = "none"
    reason_if_not_applied: str | None = None
    channel: str = "payload"

    def as_dict(self) -> dict[str, Any]:
        return {
            "fault_id": self.fault_id,
            "kind": self.kind,
            "target": self.target,
            "planned": self.planned,
            "applied": self.applied,
            "affected_sequences": self.affected_sequences,
            "output_impact": self.output_impact,
            "reason_if_not_applied": self.reason_if_not_applied,
            "channel": self.channel,
        }

    def to_json_line(self) -> bytes:
        return (
            json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")


class FaultPlan:
    """A validated, immutable collection of :class:`FaultSpec`."""

    def __init__(self, specs: Iterable[FaultSpec], *, source: Path | None = None) -> None:
        self._specs = tuple(specs)
        self.source = source
        by_target: dict[tuple[str, object], list[FaultSpec]] = {}
        for spec in self._specs:
            by_target.setdefault(spec.target_key, []).append(spec)

        # Two faults on one target are allowed only when they act on different
        # channels (one payload, one transport). Anything else is ambiguous, and
        # guessing an order would make the dataset unexplainable (case S12).
        for key, group in by_target.items():
            if len(group) == 1:
                continue
            semantic = [s for s in group if s.is_semantic]
            transport = [s for s in group if s.is_transport or s.is_client_side]
            if len(semantic) > 1 or len(transport) > 1:
                raise FaultPlanError(
                    "conflicting faults target the same event: "
                    + ", ".join(f"{s.fault_id}({s.kind})" for s in group),
                    code="FAULT_PLAN_CONFLICT",
                    target=str(key),
                    kinds=[s.kind for s in group],
                )
        self._by_target = by_target

    def __len__(self) -> int:
        return len(self._specs)

    def __iter__(self) -> Iterator[FaultSpec]:
        return iter(self._specs)

    @property
    def specs(self) -> tuple[FaultSpec, ...]:
        return self._specs

    def semantic_for(self, symbol: str, sequence: int, slot_index: int) -> FaultSpec | None:
        return self._lookup(symbol, sequence, slot_index, SEMANTIC_KINDS)

    def transport_for(self, symbol: str, sequence: int, slot_index: int) -> FaultSpec | None:
        return self._lookup(symbol, sequence, slot_index, TRANSPORT_KINDS)

    def _lookup(
        self, symbol: str, sequence: int, slot_index: int, kinds: frozenset[str]
    ) -> FaultSpec | None:
        candidates: list[FaultSpec] = []
        for spec in self._by_target.get(("seq", (symbol, sequence)), ()):
            if spec.kind in kinds:
                candidates.append(spec)
        for spec in self._by_target.get(("slot", slot_index), ()):
            if spec.kind in kinds:
                candidates.append(spec)
        if not candidates:
            return None
        # Highest priority wins; ties fall back to plan order via fault_id.
        return max(candidates, key=lambda s: (s.priority, s.fault_id))

    def fingerprint(self) -> str:
        """Stable hash of the plan, folded into ``dataset_id``.

        Two runs that differ only in their fault plan must not share a dataset
        id, or a gap in one would look like a contradiction in the other.
        """
        payload = json.dumps(
            [
                {
                    "fault_id": s.fault_id,
                    "kind": s.kind,
                    "symbol": s.symbol,
                    "sequence": s.sequence,
                    "global_slot_index": s.global_slot_index,
                    "offset_ms": s.offset_ms,
                    "priority": s.priority,
                    "bid_delta_ticks": s.bid_delta_ticks,
                    "price_ticks": s.price_ticks,
                    "spread_ticks": s.spread_ticks,
                    "truncate_bytes": s.truncate_bytes,
                    "payload": s.payload,
                }
                for s in self._specs
            ],
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    @staticmethod
    def empty() -> "FaultPlan":
        return FaultPlan(())


def _as_int(value: Any, key: str, fault_id: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FaultPlanError(
            f"{fault_id}: '{key}' must be an integer, got {type(value).__name__}",
            code="FAULT_PLAN_TYPE_ERROR",
            fault_id=fault_id,
            key=key,
        )
    return value


def parse_fault_plan(raw: dict[str, Any], *, source: Path | None = None) -> FaultPlan:
    """Validate a parsed fault-plan mapping."""
    if not isinstance(raw, dict):
        raise FaultPlanError(
            "fault plan root must be a table", code="FAULT_PLAN_TYPE_ERROR"
        )
    unknown = sorted(set(raw) - {"fault"})
    if unknown:
        raise FaultPlanError(
            f"unknown key(s) in fault plan: {', '.join(unknown)}",
            code="FAULT_PLAN_UNKNOWN_KEY",
            unknown=unknown,
        )
    entries = raw.get("fault", [])
    if not isinstance(entries, list):
        raise FaultPlanError(
            "[[fault]] must be an array of tables", code="FAULT_PLAN_TYPE_ERROR"
        )

    specs: list[FaultSpec] = []
    for index, entry in enumerate(entries):
        fault_id = f"f{index + 1:04d}"
        if not isinstance(entry, dict):
            raise FaultPlanError(
                f"{fault_id}: each [[fault]] entry must be a table",
                code="FAULT_PLAN_TYPE_ERROR",
                fault_id=fault_id,
            )
        extra = sorted(set(entry) - _FAULT_KEYS)
        if extra:
            raise FaultPlanError(
                f"{fault_id}: unknown key(s) {', '.join(extra)}",
                code="FAULT_PLAN_UNKNOWN_KEY",
                fault_id=fault_id,
                unknown=extra,
            )
        kind = entry.get("kind")
        if kind not in ALL_KINDS:
            raise FaultPlanError(
                f"{fault_id}: unsupported fault kind {kind!r}; supported kinds are "
                f"{sorted(ALL_KINDS)}",
                code="FAULT_PLAN_UNKNOWN_KIND",
                fault_id=fault_id,
                kind=str(kind),
            )

        has_seq_target = "symbol" in entry and "sequence" in entry
        has_slot_target = "global_slot_index" in entry
        if has_seq_target == has_slot_target:
            raise FaultPlanError(
                f"{fault_id}: specify exactly one target -- either "
                "(symbol, sequence) or global_slot_index",
                code="FAULT_PLAN_TARGET_INVALID",
                fault_id=fault_id,
            )

        symbol: str | None = None
        sequence: int | None = None
        slot: int | None = None
        if has_seq_target:
            symbol = entry["symbol"]
            if not isinstance(symbol, str) or not symbol:
                raise FaultPlanError(
                    f"{fault_id}: 'symbol' must be a non-empty string",
                    code="FAULT_PLAN_TYPE_ERROR",
                    fault_id=fault_id,
                )
            sequence = _as_int(entry["sequence"], "sequence", fault_id)
            if sequence < 1:
                raise FaultPlanError(
                    f"{fault_id}: 'sequence' must be >= 1, got {sequence}",
                    code="FAULT_PLAN_TARGET_INVALID",
                    fault_id=fault_id,
                )
        else:
            slot = _as_int(entry["global_slot_index"], "global_slot_index", fault_id)
            if slot < 0:
                raise FaultPlanError(
                    f"{fault_id}: 'global_slot_index' must be >= 0, got {slot}",
                    code="FAULT_PLAN_TARGET_INVALID",
                    fault_id=fault_id,
                )

        offset_ms = _as_int(entry.get("offset_ms", 0), "offset_ms", fault_id)
        if kind in ("stale_timestamp", "timestamp_future") and offset_ms <= 0:
            raise FaultPlanError(
                f"{fault_id}: {kind} needs a positive 'offset_ms'",
                code="FAULT_PLAN_MISSING_PARAM",
                fault_id=fault_id,
            )
        truncate_bytes = entry.get("truncate_bytes")
        if truncate_bytes is not None:
            truncate_bytes = _as_int(truncate_bytes, "truncate_bytes", fault_id)
            if truncate_bytes < 1:
                raise FaultPlanError(
                    f"{fault_id}: 'truncate_bytes' must be >= 1",
                    code="FAULT_PLAN_MISSING_PARAM",
                    fault_id=fault_id,
                )

        specs.append(
            FaultSpec(
                fault_id=fault_id,
                kind=kind,
                symbol=symbol,
                sequence=sequence,
                global_slot_index=slot,
                offset_ms=offset_ms,
                priority=_as_int(entry.get("priority", 0), "priority", fault_id),
                bid_delta_ticks=(
                    _as_int(entry["bid_delta_ticks"], "bid_delta_ticks", fault_id)
                    if "bid_delta_ticks" in entry
                    else None
                ),
                price_ticks=(
                    _as_int(entry["price_ticks"], "price_ticks", fault_id)
                    if "price_ticks" in entry
                    else None
                ),
                spread_ticks=(
                    _as_int(entry["spread_ticks"], "spread_ticks", fault_id)
                    if "spread_ticks" in entry
                    else None
                ),
                truncate_bytes=truncate_bytes,
                payload=entry.get("payload"),
            )
        )
    return FaultPlan(specs, source=source)


def load_fault_plan(path: str | Path) -> FaultPlan:
    plan_path = Path(path).resolve()
    if not plan_path.is_file():
        raise FaultPlanError(
            f"fault plan not found: {plan_path}",
            code="FAULT_PLAN_NOT_FOUND",
            path=str(plan_path),
        )
    try:
        raw = tomllib.loads(plan_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise FaultPlanError(
            f"invalid TOML in fault plan {plan_path}: {exc}",
            code="FAULT_PLAN_SYNTAX",
            path=str(plan_path),
        ) from exc
    return parse_fault_plan(raw, source=plan_path)


@dataclass(frozen=True, slots=True)
class EmitItem:
    """One thing to push downstream.

    ``transport_fault`` carries a TCP-only directive that the recorder ignores,
    which is how a corrupt-bytes test runs without corrupting the golden file.
    """

    event: Event
    is_duplicate: bool = False
    is_reordered: bool = False
    is_mutated: bool = False
    transport_fault: FaultSpec | None = None


class FaultInjector:
    """Streaming transform from generated events to emitted items.

    Call :meth:`process` once per generated event, in order, then :meth:`drain`
    exactly once at the end to release anything the reorder buffer still holds.
    Skipping ``drain()`` would silently lose an event, so the engine asserts that
    nothing is left behind.
    """

    def __init__(self, plan: FaultPlan, *, enabled: bool = True) -> None:
        self._plan = plan
        self._enabled = enabled and len(plan) > 0
        self._records: dict[str, FaultRecord] = {}
        for spec in plan:
            channel = (
                "payload"
                if spec.is_semantic
                else ("transport" if spec.is_transport else "client")
            )
            record = FaultRecord(
                fault_id=spec.fault_id,
                kind=spec.kind,
                target=spec.target_dict(),
                planned=True,
                applied=False,
                channel=channel,
            )
            if spec.is_client_side:
                record.reason_if_not_applied = (
                    "client_side_fault: driven by the consuming test client, not "
                    "by the simulator"
                )
            self._records[spec.fault_id] = record
        # Pending reorder holds: at most one per symbol, bounded overall.
        self._held: dict[str, tuple[Event, FaultSpec]] = {}
        # Omissions are counted here rather than by the engine: a withheld event
        # produces no item downstream, so there is nothing for the engine to see.
        self.omissions = 0
        self.duplicates = 0
        self.reorders = 0
        self.mutations = 0

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def records(self) -> list[FaultRecord]:
        return [self._records[spec.fault_id] for spec in self._plan]

    @property
    def held_count(self) -> int:
        return len(self._held)

    def counts(self) -> dict[str, int]:
        applied = sum(1 for r in self._records.values() if r.applied)
        return {
            "planned": len(self._records),
            "applied": applied,
            "not_applied": len(self._records) - applied,
        }

    def process(self, event: Event, slot_index: int) -> list[EmitItem]:
        if not self._enabled:
            return [EmitItem(event)]

        # A held event is released as soon as the next event of the same symbol
        # arrives: the pair comes out swapped, which is exactly "adjacent
        # reorder" as a receiver sees it.
        held = self._held.pop(event.symbol, None)
        if held is not None:
            held_event, held_spec = held
            record = self._records[held_spec.fault_id]
            record.applied = True
            record.output_impact = (
                "two events of this symbol emitted out of sequence order"
            )
            record.affected_sequences = sorted({held_event.sequence, event.sequence})
            self.reorders += 1
            return [EmitItem(event), EmitItem(held_event, is_reordered=True)]

        spec = self._plan.semantic_for(event.symbol, event.sequence, slot_index)
        transport = self._plan.transport_for(event.symbol, event.sequence, slot_index)

        if spec is None:
            return [EmitItem(event, transport_fault=self._mark_transport(transport, event))]

        record = self._records[spec.fault_id]
        kind = spec.kind

        if kind == "missing_sequence":
            record.applied = True
            record.output_impact = "event withheld; receiver observes a sequence gap"
            record.affected_sequences = [event.sequence]
            self.omissions += 1
            return []

        if kind == "duplicate_exact":
            record.applied = True
            record.output_impact = "same payload emitted twice with the same sequence"
            record.affected_sequences = [event.sequence]
            self.duplicates += 1
            return [
                EmitItem(event, transport_fault=self._mark_transport(transport, event)),
                EmitItem(event, is_duplicate=True),
            ]

        if kind == "duplicate_conflict":
            # A *different* payload under the same sequence. Tracked apart from
            # duplicate_exact because deduplicating by key would hide a real
            # inconsistency here rather than resolve it (case S03).
            delta = spec.bid_delta_ticks if spec.bid_delta_ticks is not None else 1
            if event.event_type == EVENT_QUOTE:
                conflicting = event.replace(
                    bid_ticks=(event.bid_ticks or 0) + delta,
                    ask_ticks=(event.ask_ticks or 0) + delta,
                )
            else:
                conflicting = event.replace(price_ticks=(event.price_ticks or 0) + delta)
            record.applied = True
            record.output_impact = (
                "two events share (dataset_id, symbol, sequence) with different "
                "prices; a consumer must flag the conflict, not silently dedupe"
            )
            record.affected_sequences = [event.sequence]
            self.duplicates += 1
            self.mutations += 1
            return [
                EmitItem(event, transport_fault=self._mark_transport(transport, event)),
                EmitItem(conflicting, is_duplicate=True, is_mutated=True),
            ]

        if kind == "reorder_adjacent":
            if len(self._held) >= MAX_REORDER_HOLD:
                record.applied = False
                record.reason_if_not_applied = (
                    f"reorder buffer full ({MAX_REORDER_HOLD} held); refusing to "
                    "grow it without bound"
                )
                return [
                    EmitItem(event, transport_fault=self._mark_transport(transport, event))
                ]
            self._held[event.symbol] = (event, spec)
            return []

        mutated = self._mutate(event, spec, record)
        if record.applied:
            self.mutations += 1
        return [
            EmitItem(
                mutated,
                # Not every mutation applies: invalid_spread aimed at a slot that
                # produced a tick leaves the event untouched, and flagging it as
                # mutated would overstate what happened.
                is_mutated=record.applied,
                transport_fault=self._mark_transport(transport, event),
            )
        ]

    def _mutate(self, event: Event, spec: FaultSpec, record: FaultRecord) -> Event:
        kind = spec.kind
        record.applied = True
        record.affected_sequences = [event.sequence]

        if kind == "stale_timestamp":
            shifted = event.event_time_ns - spec.offset_ms * 1_000_000
            record.output_impact = (
                f"event_time_ns moved {spec.offset_ms} ms into the past; a receiver "
                "should flag it as stale against its own threshold"
            )
            return event.replace(event_time_ns=max(0, shifted))

        if kind == "timestamp_future":
            record.output_impact = (
                f"event_time_ns moved {spec.offset_ms} ms into the future; a "
                "receiver should flag clock skew"
            )
            return event.replace(
                event_time_ns=event.event_time_ns + spec.offset_ms * 1_000_000
            )

        if kind == "invalid_spread":
            if event.event_type != EVENT_QUOTE:
                record.applied = False
                record.reason_if_not_applied = (
                    "invalid_spread targets a quote, but this slot produced a tick"
                )
                record.affected_sequences = []
                return event
            bid, ask = event.bid_ticks or 0, event.ask_ticks or 0
            if spec.spread_ticks is not None:
                record.output_impact = f"spread forced to {spec.spread_ticks} ticks"
                return event.replace(ask_ticks=bid + spec.spread_ticks)
            record.output_impact = "bid and ask swapped, so ask < bid (crossed quote)"
            return event.replace(bid_ticks=ask, ask_ticks=bid)

        if kind == "invalid_price":
            forced = spec.price_ticks if spec.price_ticks is not None else 0
            record.output_impact = f"price forced to {forced}, outside the valid band"
            if event.event_type == EVENT_QUOTE:
                return event.replace(bid_ticks=forced, ask_ticks=forced)
            return event.replace(price_ticks=forced)

        record.applied = False
        record.reason_if_not_applied = f"no mutation implemented for kind {kind!r}"
        record.affected_sequences = []
        return event

    def _mark_transport(self, spec: FaultSpec | None, event: Event) -> FaultSpec | None:
        if spec is None:
            return None
        # Whether a transport fault was *applied* is decided by the TCP sink, the
        # only component that can actually corrupt or close the stream. Here we
        # record only that the targeted event was reached.
        self._records[spec.fault_id].affected_sequences = [event.sequence]
        return spec

    def note_transport_applied(self, spec: FaultSpec, impact: str) -> None:
        record = self._records[spec.fault_id]
        record.applied = True
        record.output_impact = impact

    def note_transport_skipped(self, spec: FaultSpec, reason: str) -> None:
        record = self._records[spec.fault_id]
        if not record.applied:
            record.reason_if_not_applied = reason

    def drain(self) -> list[EmitItem]:
        """Release held events at end of stream.

        A hold that never found a partner is a planned-but-not-applied reorder:
        the event is still emitted, in order, and the manifest says why the swap
        did not happen, so the count algebra stays exact.
        """
        out: list[EmitItem] = []
        for symbol in sorted(self._held):
            event, spec = self._held[symbol]
            record = self._records[spec.fault_id]
            record.applied = False
            record.reason_if_not_applied = (
                "no following event for this symbol before end of stream, so there "
                "was nothing to swap with"
            )
            record.affected_sequences = [event.sequence]
            out.append(EmitItem(event))
        self._held.clear()
        return out

    def finalize_unreached(self) -> None:
        """Mark faults whose target never occurred (case S11)."""
        for spec in self._plan:
            record = self._records[spec.fault_id]
            if record.applied or record.reason_if_not_applied:
                continue
            record.reason_if_not_applied = (
                "target event never occurred in this run (check symbol, sequence or "
                "global_slot_index against the actual slot count)"
            )
