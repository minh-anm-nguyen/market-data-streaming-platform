"""An independent re-implementation of P03's acceptance policy.

This is the expected-set generator the whole harness rests on. Design 2.1 is
blunt about the thing it must not do: *"copy output P03 làm expected mà không
independently validate"*. So nothing here reads P03's counters, logs or output.
It reads the sealed JSONL and applies the policy from P03's design sections 3.4,
3.5, 4.2, 5 and 6 to produce three sets:

``accepted``
    what a correct P03 in ``strict_monitor`` mode would publish downstream --
    one entry per :class:`~p07.contracts.SourceIdentity`, which is the set ``A``
    that Kafka, the raw table, the canonical build and the bars all get
    compared against.
``suppressed``
    business-valid but deliberately not published: exact duplicates, late
    arrivals, event-time-stale quotes. Their absence downstream is *correct*,
    and conflating it with transport loss is the mistake ``INV-15`` names.
``rejected``
    structurally or business-invalid, with the P03 reason code.

Stage order is fixed by P03's implementation invariant and is load-bearing:
``structural -> business -> sequence -> freshness -> output policy``. A
business-invalid event does **not** update sequence state, so a bad seq 42
leaves seq 43 reporting a gap -- reordering these stages silently changes the
expected set.

Two deliberate policy choices, both configurable and both documented because a
reader will otherwise assume they were accidents:

* **Price-step violations flag rather than reject by default.** P03 case E-09
  allows either. Rejecting would couple this oracle to P01's generator
  internals (its step bound applies to the mid, while a quote carries bid/ask),
  and a false reject here would corrupt every downstream verdict. ``flag`` adds
  a ``PRICE_STEP`` quality flag and keeps the event; ``reject`` is available for
  scenarios that mean to test it.
* **Event-time-stale quotes are suppressed, not published with a flag.** P03's
  taxonomy says ``EVENT_TIME_OLD`` is "reject as current quote", so the strict
  publication policy here withholds it. The flag is recorded either way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final, Iterable, Mapping, MutableMapping

from ..contracts import SOURCE_SCHEMA_VERSION, EventKind, SemanticCore, SourceIdentity
from ..dataset_sealer import SealedDataset, SymbolPolicy
from .semantic_digest import digest_core

#: Bump this when any rule below changes. An expected set is only comparable to
#: another expected set from the same policy version (design 1.1 FR-05).
POLICY_VERSION: Final = "p03.policy.v1"

#: P03 section 5: the bounded window within which a sequence can still be
#: classified. Older than this is ``TOO_OLD_UNVERIFIABLE`` -- not "duplicate",
#: because the tracker genuinely cannot tell any more.
DEFAULT_SEQUENCE_WINDOW: Final = 4096

#: P03 section 6 MVP policy. Two seconds of event-time regression against the
#: per-symbol quote watermark. Not a universal broker threshold; it is the
#: fixture's threshold and it travels in the receipt.
DEFAULT_EVENT_TIME_STALE_NS: Final = 2_000_000_000

#: Field allowlists, re-derived here from the ``sim.v1`` contract rather than
#: imported from P01, so a change in P01's writer cannot silently redefine what
#: the oracle accepts.
_QUOTE_FIELDS: Final = frozenset(
    {
        "ask_ticks",
        "bid_ticks",
        "dataset_id",
        "event_time_ns",
        "event_type",
        "price_scale",
        "schema_version",
        "sequence",
        "symbol",
    }
)
_TICK_FIELDS: Final = frozenset(
    {
        "dataset_id",
        "event_time_ns",
        "event_type",
        "price_scale",
        "price_ticks",
        "schema_version",
        "sequence",
        "symbol",
    }
)


class P03Reason:
    """P03's error taxonomy (its design section 4.2), as constants."""

    INVALID_JSON: Final = "INVALID_JSON"
    FIELD_SET_MISMATCH: Final = "FIELD_SET_MISMATCH"
    UNSUPPORTED_SCHEMA: Final = "UNSUPPORTED_SCHEMA"
    UNSUPPORTED_EVENT_KIND: Final = "UNSUPPORTED_EVENT_KIND"
    STRUCTURAL_REJECT: Final = "STRUCTURAL_REJECT"
    FIELD_NOT_INTEGER: Final = "FIELD_NOT_INTEGER"
    WRONG_DATASET: Final = "WRONG_DATASET"
    UNKNOWN_SYMBOL: Final = "UNKNOWN_SYMBOL"
    INVALID_SEQUENCE: Final = "INVALID_SEQUENCE"
    INVALID_EVENT_TIME: Final = "INVALID_EVENT_TIME"
    BAD_SCALE: Final = "BAD_SCALE"
    INVALID_BID: Final = "INVALID_BID"
    INVALID_ASK: Final = "INVALID_ASK"
    SPREAD_OUT_OF_RANGE: Final = "SPREAD_OUT_OF_RANGE"
    INVALID_PRICE: Final = "INVALID_PRICE"
    PRICE_OUT_OF_RANGE: Final = "PRICE_OUT_OF_RANGE"
    PRICE_STEP: Final = "PRICE_STEP"
    SEQUENCE_GAP: Final = "SEQUENCE_GAP"
    DUPLICATE_EXACT: Final = "DUPLICATE_EXACT"
    DUPLICATE_CONFLICT: Final = "DUPLICATE_CONFLICT"
    REORDER_LATE: Final = "REORDER_LATE"
    TOO_OLD_UNVERIFIABLE: Final = "TOO_OLD_UNVERIFIABLE"
    EVENT_TIME_OLD: Final = "EVENT_TIME_OLD"
    SEQUENCE_ROLLBACK: Final = "SEQUENCE_ROLLBACK"


class Disposition(str, Enum):
    """What a correct P03 does with the event."""

    ACCEPTED = "ACCEPTED"
    SUPPRESSED = "SUPPRESSED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class ClassifiedEvent:
    """One source record, classified. ``core`` is ``None`` when unparseable."""

    line_number: int
    disposition: Disposition
    reason: str | None
    identity: SourceIdentity | None
    core: SemanticCore | None
    flags: frozenset[str] = frozenset()

    def as_dict(self) -> dict[str, Any]:
        return {
            "line": self.line_number,
            "disposition": self.disposition.value,
            "reason": self.reason,
            "identity": list(self.identity) if self.identity else None,
            "flags": sorted(self.flags),
        }


@dataclass(frozen=True, slots=True)
class PolicyConfig:
    """Knobs that change the expected set. Travels into the receipt verbatim."""

    sequence_window: int = DEFAULT_SEQUENCE_WINDOW
    event_time_stale_ns: int = DEFAULT_EVENT_TIME_STALE_NS
    price_step_policy: str = "flag"  # "flag" | "reject"
    require_sequence_starts_at_one: bool = False

    def __post_init__(self) -> None:
        if self.sequence_window <= 0:
            raise ValueError("sequence_window must be positive")
        if self.event_time_stale_ns < 0:
            raise ValueError("event_time_stale_ns must not be negative")
        if self.price_step_policy not in {"flag", "reject"}:
            raise ValueError("price_step_policy must be 'flag' or 'reject'")


@dataclass(slots=True)
class _SymbolState:
    """Per ``(dataset_id, symbol)`` tracker state, bounded by the window.

    ``cores_in_window`` keeps the typed projection rather than a short hash:
    P03 section 5 forbids concluding ``DUPLICATE_EXACT`` from a truncated hash,
    because a collision would turn a conflict into a benign duplicate.
    """

    highest_seen_valid: int | None = None
    seen_valid: dict[int, SemanticCore] = field(default_factory=dict)
    known_missing: set[int] = field(default_factory=set)
    quote_watermark_ns: int | None = None
    last_mid_ticks: int | None = None
    gap_count: int = 0

    def prune(self, window: int) -> None:
        """Drop state older than the window, so memory stays bounded.

        Without this, a 50M-event capacity fixture would hold 50M cores -- the
        unbounded-state failure mode of case ``P07-G24-*``.
        """
        if self.highest_seen_valid is None:
            return
        floor = self.highest_seen_valid - window
        if floor <= 0:
            return
        for store in (self.seen_valid, self.known_missing):
            stale = [seq for seq in store if seq < floor]
            for seq in stale:
                if isinstance(store, dict):
                    store.pop(seq, None)
                else:
                    store.discard(seq)


@dataclass(frozen=True, slots=True)
class AcceptanceExpectation:
    """The expected sets, plus everything needed to defend them."""

    policy_version: str
    config: PolicyConfig
    dataset_id: str
    accepted: Mapping[SourceIdentity, SemanticCore]
    accepted_digests: Mapping[SourceIdentity, str]
    publication_order: Mapping[str, tuple[int, ...]]
    suppressed: tuple[ClassifiedEvent, ...]
    rejected: tuple[ClassifiedEvent, ...]
    source_identity_conflicts: tuple[tuple[SourceIdentity, tuple[str, ...]], ...]
    untracked_anomalies: tuple[ClassifiedEvent, ...]
    unapplied_declared_faults: tuple[str, ...]
    symbols_without_events: tuple[str, ...]
    counts: Mapping[str, int]

    def expected_identities(self) -> frozenset[SourceIdentity]:
        return frozenset(self.accepted)

    def receipt(self) -> dict[str, Any]:
        """What the evidence bundle stores about this expected set."""
        return {
            "policy_version": self.policy_version,
            "dataset_id": self.dataset_id,
            "config": {
                "sequence_window": self.config.sequence_window,
                "event_time_stale_ns": self.config.event_time_stale_ns,
                "price_step_policy": self.config.price_step_policy,
                "require_sequence_starts_at_one": self.config.require_sequence_starts_at_one,
            },
            "counts": dict(self.counts),
            "source_identity_conflicts": [
                {"identity": list(identity), "fields": list(fields)}
                for identity, fields in self.source_identity_conflicts
            ],
            "untracked_anomalies": [event.as_dict() for event in self.untracked_anomalies[:50]],
            "untracked_anomaly_count": len(self.untracked_anomalies),
            "unapplied_declared_faults": list(self.unapplied_declared_faults),
            "symbols_without_events": list(self.symbols_without_events),
        }


#: Anomaly reasons the fault manifest is expected to explain. A gap, duplicate,
#: conflict or stale quote that no *applied* fault accounts for means the
#: fixture is not what it claims (cases ``P07-G01-05``, ``P07-G01-06``).
_ANOMALY_REASONS: Final = frozenset(
    {
        P03Reason.SEQUENCE_GAP,
        P03Reason.DUPLICATE_EXACT,
        P03Reason.DUPLICATE_CONFLICT,
        P03Reason.REORDER_LATE,
        P03Reason.TOO_OLD_UNVERIFIABLE,
        P03Reason.EVENT_TIME_OLD,
        P03Reason.INVALID_BID,
        P03Reason.INVALID_ASK,
        P03Reason.INVALID_PRICE,
        P03Reason.SPREAD_OUT_OF_RANGE,
        P03Reason.PRICE_OUT_OF_RANGE,
        P03Reason.BAD_SCALE,
        P03Reason.INVALID_SEQUENCE,
    }
)

#: P01 fault kinds mapped to the P03 classifications they should produce. Used
#: only to decide whether an observed anomaly was declared, never to relax a
#: rule: an undeclared anomaly still gets its real classification.
_FAULT_KIND_EFFECTS: Final[Mapping[str, frozenset[str]]] = {
    "missing_sequence": frozenset({P03Reason.SEQUENCE_GAP}),
    "duplicate_exact": frozenset({P03Reason.DUPLICATE_EXACT}),
    "duplicate_conflict": frozenset({P03Reason.DUPLICATE_CONFLICT}),
    "reorder_adjacent": frozenset(
        {P03Reason.REORDER_LATE, P03Reason.SEQUENCE_GAP, P03Reason.TOO_OLD_UNVERIFIABLE}
    ),
    "stale_timestamp": frozenset({P03Reason.EVENT_TIME_OLD}),
    "timestamp_future": frozenset({P03Reason.INVALID_EVENT_TIME, P03Reason.EVENT_TIME_OLD}),
    "invalid_spread": frozenset(
        {P03Reason.SPREAD_OUT_OF_RANGE, P03Reason.INVALID_ASK, P03Reason.INVALID_BID}
    ),
    "invalid_price": frozenset(
        {P03Reason.INVALID_PRICE, P03Reason.PRICE_OUT_OF_RANGE, P03Reason.INVALID_BID}
    ),
}


def _parse_line(text: str) -> tuple[Mapping[str, Any] | None, str | None]:
    """Strict ``sim.v1`` parse. Returns ``(obj, reason)``; one is always None."""
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return None, P03Reason.INVALID_JSON
    if not isinstance(obj, dict):
        return None, P03Reason.STRUCTURAL_REJECT
    if obj.get("schema_version") != SOURCE_SCHEMA_VERSION:
        return None, P03Reason.UNSUPPORTED_SCHEMA
    kind = obj.get("event_type")
    if kind not in {"tick", "quote"}:
        return None, P03Reason.UNSUPPORTED_EVENT_KIND
    expected = _QUOTE_FIELDS if kind == "quote" else _TICK_FIELDS
    if set(obj) != expected:
        return None, P03Reason.FIELD_SET_MISMATCH
    for name in ("sequence", "event_time_ns", "price_scale"):
        value = obj[name]
        if not isinstance(value, int) or isinstance(value, bool):
            return None, P03Reason.FIELD_NOT_INTEGER
    tick_fields = ("bid_ticks", "ask_ticks") if kind == "quote" else ("price_ticks",)
    for name in tick_fields:
        value = obj[name]
        # ``None`` here is absence, which for a required payload field is a
        # structural reject: P03 rule 3 forbids treating a default as present.
        if value is None:
            return None, P03Reason.STRUCTURAL_REJECT
        if not isinstance(value, int) or isinstance(value, bool):
            return None, P03Reason.FIELD_NOT_INTEGER
    if not isinstance(obj.get("symbol"), str) or not isinstance(obj.get("dataset_id"), str):
        return None, P03Reason.STRUCTURAL_REJECT
    return obj, None


def _business_check(
    core: SemanticCore, policy: SymbolPolicy, state: _SymbolState, config: PolicyConfig
) -> tuple[str | None, frozenset[str]]:
    """Business validity for one event. Returns ``(reject_reason, flags)``."""
    flags: set[str] = set()
    if core.sequence <= 0:
        return P03Reason.INVALID_SEQUENCE, frozenset()
    if core.event_time_ns < 0:
        return P03Reason.INVALID_EVENT_TIME, frozenset()
    if core.price_scale <= 0 or core.price_scale != policy.price_scale:
        return P03Reason.BAD_SCALE, frozenset()

    if core.event_kind is EventKind.QUOTE:
        bid, ask = core.bid_ticks, core.ask_ticks
        assert bid is not None and ask is not None  # structural stage guaranteed presence
        if bid <= 0:
            return P03Reason.INVALID_BID, frozenset()
        if ask < bid:
            return P03Reason.INVALID_ASK, frozenset()
        spread = ask - bid
        if not policy.min_spread_ticks <= spread <= policy.max_spread_ticks:
            return P03Reason.SPREAD_OUT_OF_RANGE, frozenset()
        if not policy.min_price_ticks <= bid <= policy.max_price_ticks:
            return P03Reason.PRICE_OUT_OF_RANGE, frozenset()
        if not policy.min_price_ticks <= ask <= policy.max_price_ticks:
            return P03Reason.PRICE_OUT_OF_RANGE, frozenset()
        mid = (bid + ask) // 2
    else:
        price = core.price_ticks
        assert price is not None
        if price <= 0:
            return P03Reason.INVALID_PRICE, frozenset()
        if not policy.min_price_ticks <= price <= policy.max_price_ticks:
            return P03Reason.PRICE_OUT_OF_RANGE, frozenset()
        mid = price

    # Price-step policy. The tolerance adds the spread band because the mid of a
    # quote is recovered by integer halving, so comparing a tick price with a
    # quote mid carries up to half a spread of estimation error. A tighter bound
    # would reject valid P01 output; see the module docstring.
    if state.last_mid_ticks is not None and policy.max_step_ticks > 0:
        tolerance = policy.max_step_ticks + policy.max_spread_ticks
        if abs(mid - state.last_mid_ticks) > tolerance:
            if config.price_step_policy == "reject":
                return P03Reason.PRICE_STEP, frozenset()
            flags.add(P03Reason.PRICE_STEP)
    return None, frozenset(flags)


def _sequence_check(
    core: SemanticCore, state: _SymbolState, config: PolicyConfig
) -> tuple[str | None, frozenset[str], tuple[int, int] | None]:
    """P03 section 5 sequence classification.

    Returns ``(reason, flags, missing_range)``. ``reason`` of ``None`` means
    in-order and new. ``missing_range`` is the ``[floor, seq)`` interval a gap
    exposed, which the caller needs in order to decide whether the *fault
    manifest explains those specific sequences* -- a gap is only "declared" if
    every sequence it skipped has a declared fault.
    """
    seq = core.sequence
    if state.highest_seen_valid is None:
        if config.require_sequence_starts_at_one and seq != 1:
            return None, frozenset({P03Reason.SEQUENCE_GAP}), (1, seq)
        return None, frozenset(), None
    high = state.highest_seen_valid
    if seq > high:
        if seq > high + 1:
            # Record the gap as an interval's worth of missing sequences, but
            # only those inside the window: materialising a billion missing
            # slots is the memory blow-up P03 section 5 warns about.
            floor = max(high + 1, seq - config.sequence_window)
            state.known_missing.update(range(floor, seq))
            state.gap_count += 1
            return None, frozenset({P03Reason.SEQUENCE_GAP}), (floor, seq)
        return None, frozenset(), None
    if seq >= high - config.sequence_window:
        previous = state.seen_valid.get(seq)
        if previous is not None:
            if previous == core:
                return P03Reason.DUPLICATE_EXACT, frozenset({P03Reason.DUPLICATE_EXACT}), None
            return P03Reason.DUPLICATE_CONFLICT, frozenset({P03Reason.DUPLICATE_CONFLICT}), None
        if seq in state.known_missing:
            return P03Reason.REORDER_LATE, frozenset({P03Reason.REORDER_LATE}), None
    return P03Reason.TOO_OLD_UNVERIFIABLE, frozenset({P03Reason.TOO_OLD_UNVERIFIABLE}), None


def _freshness_check(
    core: SemanticCore, state: _SymbolState, config: PolicyConfig
) -> tuple[str | None, frozenset[str]]:
    """Event-time staleness for quotes, against the per-symbol watermark.

    A tick never refreshes and is never judged by the quote watermark (P03
    section 6: "Tick không refresh quote timer"). Feed *inactivity* is a
    wall-clock/monotonic concern of the live adapter and has no bearing on an
    offline expected set, so it is deliberately not modelled here.
    """
    if core.event_kind is not EventKind.QUOTE:
        return None, frozenset()
    watermark = state.quote_watermark_ns
    if watermark is None:
        return None, frozenset()
    if watermark - core.event_time_ns > config.event_time_stale_ns:
        return P03Reason.EVENT_TIME_OLD, frozenset({P03Reason.EVENT_TIME_OLD})
    return None, frozenset()


def _declared_fault_index(
    dataset: SealedDataset,
) -> tuple[dict[tuple[str, int], set[str]], set[tuple[str, int]], tuple[str, ...]]:
    """Index applied faults by their exact ``(symbol, sequence)`` target.

    Three results: the reasons each target is expected to produce, the set of
    targets that have *any* applied fault, and the ids of faults that were
    planned but never applied.

    The exactness matters. An earlier version also indexed ``(symbol, None)``
    as a fallback, which made any anomaly on a symbol with one declared fault
    look declared -- so an undeclared conflict two thousand sequences away would
    have gone unreported. The "any applied fault at this sequence" set exists
    because a *gap* is explained by whatever happened to the missing sequence,
    whether that was an omission, a business reject or a suppressed stale quote.
    """
    index: dict[tuple[str, int], set[str]] = {}
    any_fault: set[tuple[str, int]] = set()
    unapplied: list[str] = []
    for record in dataset.fault_records:
        if not record.applied:
            unapplied.append(record.fault_id)
            continue
        if record.symbol is None or record.sequence is None:
            continue
        key = (record.symbol, record.sequence)
        index.setdefault(key, set()).update(_FAULT_KIND_EFFECTS.get(record.kind, frozenset()))
        any_fault.add(key)
    return index, any_fault, tuple(unapplied)


def build_expectation(
    dataset: SealedDataset,
    *,
    config: PolicyConfig | None = None,
) -> AcceptanceExpectation:
    """Apply the independent P03 policy to ``dataset`` and return the sets.

    One pass over the file, streaming, with per-symbol state bounded by the
    sequence window. The accepted map is the only thing that grows with the
    dataset, which is unavoidable -- it *is* the expected set -- and
    :mod:`p07.reconcile` has a sharded path for fixtures too large to hold.
    """
    config = config or PolicyConfig()
    states: MutableMapping[str, _SymbolState] = {}
    accepted: dict[SourceIdentity, SemanticCore] = {}
    digests: dict[SourceIdentity, str] = {}
    order: dict[str, list[int]] = {}
    suppressed: list[ClassifiedEvent] = []
    rejected: list[ClassifiedEvent] = []
    conflicts: list[tuple[SourceIdentity, tuple[str, ...]]] = []
    untracked: list[ClassifiedEvent] = []
    seen_symbols: set[str] = set()
    counts: dict[str, int] = {
        "source_lines": 0,
        "accepted": 0,
        "suppressed": 0,
        "rejected": 0,
        "gaps_detected": 0,
        "flagged_degraded": 0,
    }

    fault_index, fault_targets, unapplied = _declared_fault_index(dataset)

    def is_declared(symbol: str, sequence: int, reason: str) -> bool:
        """Whether a declared fault at this exact sequence predicts ``reason``."""
        return reason in fault_index.get((symbol, sequence), frozenset())

    def gap_declared(symbol: str, missing_range: tuple[int, int]) -> bool:
        """Whether every sequence the gap skipped has a declared fault.

        One undeclared sequence inside the interval makes the whole gap
        undeclared: the fixture contains an absence nothing accounts for, and
        that is the finding.
        """
        floor, ceiling = missing_range
        return all((symbol, seq) in fault_targets for seq in range(floor, ceiling))

    for line_number, text in dataset.iter_raw_lines():
        counts["source_lines"] += 1
        obj, parse_reason = _parse_line(text)
        if obj is None:
            rejected.append(ClassifiedEvent(line_number, Disposition.REJECTED, parse_reason, None, None))
            counts["rejected"] += 1
            continue

        symbol = obj["symbol"]
        seen_symbols.add(symbol)
        identity = SourceIdentity(obj["dataset_id"], symbol, obj["sequence"])

        if obj["dataset_id"] != dataset.dataset_id:
            rejected.append(
                ClassifiedEvent(line_number, Disposition.REJECTED, P03Reason.WRONG_DATASET, identity, None)
            )
            counts["rejected"] += 1
            continue
        policy = dataset.symbols.get(symbol)
        if policy is None:
            rejected.append(
                ClassifiedEvent(line_number, Disposition.REJECTED, P03Reason.UNKNOWN_SYMBOL, identity, None)
            )
            counts["rejected"] += 1
            continue

        core = SemanticCore(
            dataset_id=obj["dataset_id"],
            symbol=symbol,
            sequence=obj["sequence"],
            event_time_ns=obj["event_time_ns"],
            price_scale=obj["price_scale"],
            event_kind=EventKind.from_wire_name(obj["event_type"]),
            source_schema_version=obj["schema_version"],
            bid_ticks=obj.get("bid_ticks"),
            ask_ticks=obj.get("ask_ticks"),
            price_ticks=obj.get("price_ticks"),
        )
        state = states.setdefault(symbol, _SymbolState())

        # Stage 2: business. A reject here must not touch sequence state.
        reject_reason, flags = _business_check(core, policy, state, config)
        if reject_reason is not None:
            rejected.append(
                ClassifiedEvent(line_number, Disposition.REJECTED, reject_reason, identity, core, flags)
            )
            counts["rejected"] += 1
            if not is_declared(symbol, core.sequence, reject_reason):
                untracked.append(
                    ClassifiedEvent(
                        line_number, Disposition.REJECTED, reject_reason, identity, core, flags
                    )
                )
            continue

        # Stage 3: sequence.
        seq_reason, seq_flags, missing_range = _sequence_check(core, state, config)
        flags = flags | seq_flags
        if seq_reason == P03Reason.DUPLICATE_CONFLICT:
            previous = state.seen_valid.get(core.sequence)
            if previous is not None:
                conflicts.append((identity, previous.differing_fields(core)))
            rejected.append(
                ClassifiedEvent(line_number, Disposition.REJECTED, seq_reason, identity, core, flags)
            )
            counts["rejected"] += 1
            if not is_declared(symbol, core.sequence, seq_reason):
                untracked.append(
                    ClassifiedEvent(line_number, Disposition.REJECTED, seq_reason, identity, core, flags)
                )
            continue
        if seq_reason in {P03Reason.DUPLICATE_EXACT, P03Reason.REORDER_LATE}:
            # Business-valid, observed, deliberately not published.
            if seq_reason == P03Reason.REORDER_LATE:
                state.seen_valid.setdefault(core.sequence, core)
                state.known_missing.discard(core.sequence)
            suppressed.append(
                ClassifiedEvent(line_number, Disposition.SUPPRESSED, seq_reason, identity, core, flags)
            )
            counts["suppressed"] += 1
            if not is_declared(symbol, core.sequence, seq_reason):
                untracked.append(
                    ClassifiedEvent(line_number, Disposition.SUPPRESSED, seq_reason, identity, core, flags)
                )
            continue
        if seq_reason == P03Reason.TOO_OLD_UNVERIFIABLE:
            rejected.append(
                ClassifiedEvent(line_number, Disposition.REJECTED, seq_reason, identity, core, flags)
            )
            counts["rejected"] += 1
            if not is_declared(symbol, core.sequence, seq_reason):
                untracked.append(
                    ClassifiedEvent(line_number, Disposition.REJECTED, seq_reason, identity, core, flags)
                )
            continue

        # Stage 4: freshness.
        fresh_reason, fresh_flags = _freshness_check(core, state, config)
        flags = flags | fresh_flags
        if fresh_reason is not None:
            suppressed.append(
                ClassifiedEvent(line_number, Disposition.SUPPRESSED, fresh_reason, identity, core, flags)
            )
            counts["suppressed"] += 1
            if not is_declared(symbol, core.sequence, fresh_reason):
                untracked.append(
                    ClassifiedEvent(
                        line_number, Disposition.SUPPRESSED, fresh_reason, identity, core, flags
                    )
                )
            # A stale quote does not advance the watermark or the last-good
            # price: P03 section 6, "không cập nhật last-good quote".
            continue

        # Stage 5: output policy -- this event is published.
        if identity in accepted:
            # Only reachable when the sequence window already forgot the first
            # occurrence, so the tracker could not classify it as a duplicate.
            # Treat it as a conflict rather than overwriting: the oracle must
            # not pick a winner (design 7.3).
            conflicts.append((identity, accepted[identity].differing_fields(core)))
            rejected.append(
                ClassifiedEvent(
                    line_number, Disposition.REJECTED, P03Reason.DUPLICATE_CONFLICT, identity, core, flags
                )
            )
            counts["rejected"] += 1
            continue

        accepted[identity] = core
        digests[identity] = digest_core(core)
        order.setdefault(symbol, []).append(core.sequence)
        counts["accepted"] += 1
        if flags:
            counts["flagged_degraded"] += 1
        if (
            P03Reason.SEQUENCE_GAP in flags
            and missing_range is not None
            and not gap_declared(symbol, missing_range)
        ):
            untracked.append(
                ClassifiedEvent(line_number, Disposition.ACCEPTED, P03Reason.SEQUENCE_GAP, identity, core, flags)
            )

        state.seen_valid[core.sequence] = core
        if state.highest_seen_valid is None or core.sequence > state.highest_seen_valid:
            state.highest_seen_valid = core.sequence
        if core.event_kind is EventKind.QUOTE:
            bid, ask = core.bid_ticks, core.ask_ticks
            assert bid is not None and ask is not None
            state.quote_watermark_ns = max(state.quote_watermark_ns or 0, core.event_time_ns)
            state.last_mid_ticks = (bid + ask) // 2
        else:
            state.last_mid_ticks = core.price_ticks
        state.prune(config.sequence_window)

    counts["gaps_detected"] = sum(state.gap_count for state in states.values())
    missing_symbols = tuple(sorted(set(dataset.symbols) - seen_symbols))

    return AcceptanceExpectation(
        policy_version=POLICY_VERSION,
        config=config,
        dataset_id=dataset.dataset_id,
        accepted=accepted,
        accepted_digests=digests,
        publication_order={symbol: tuple(seqs) for symbol, seqs in order.items()},
        suppressed=tuple(suppressed),
        rejected=tuple(rejected),
        source_identity_conflicts=tuple(conflicts),
        untracked_anomalies=tuple(untracked),
        unapplied_declared_faults=unapplied,
        symbols_without_events=missing_symbols,
        counts=counts,
    )


def accepted_ticks(expectation: AcceptanceExpectation) -> Iterable[SemanticCore]:
    """Accepted tick events only -- the input the bars oracle is allowed.

    Quotes are excluded because P06 section 3 keeps quote OHLC a separate
    product; folding bid/ask into price bars would invent data.
    """
    return (core for core in expectation.accepted.values() if core.event_kind is EventKind.TICK)
