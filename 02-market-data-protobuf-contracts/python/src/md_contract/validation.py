"""Business policy: layer B of section 3.3, the only layer a mode can change.

The structural layer (framing, Protobuf parse, presence, byte budgets) is
enforced unconditionally in :mod:`md_contract.framing` and
:mod:`md_contract.codec`. Nothing here can relax it. What this module decides is
narrower and entirely about *values*:

``strict_normal``
    A negative price, a crossed quote, a non-positive sequence, time or scale is
    a rejection with a reason. The value is never repaired. Rewriting ``bid=-1``
    to ``bid=0`` and reporting success would be the worst possible outcome,
    because the operator then has neither the data nor the error.

``fault_passthrough``
    The same anomaly is preserved *exactly* -- same sign, same magnitude -- but
    only when Project 01's fault manifest shows a fault that explains it at that
    ``(symbol, sequence)``. An anomaly with no matching manifest entry is
    rejected as ``FAULT_NOT_IN_MANIFEST``, so a genuine corruption cannot hide
    inside a run that happens to have faults enabled.

Sequence gaps, duplicates and reordering are deliberately **not** judged here.
They are properties of a *stream*, not of an event, and detecting them is
Project 03's job (section 3.3). A codec that rejected them per-event would be
unable to forward an intentionally gapped dataset at all; one that "filled in"
the gap would be fabricating market data. So both modes forward them untouched
and the event count is preserved (cases F01-F03).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from .domain import PAYLOAD_QUOTE, PAYLOAD_TICK, SOURCE_SCHEMA_VERSION, DomainEvent
from .errors import PolicyError, Reason
from .source import FaultIndex, FaultRecord


class ValidationMode(str, Enum):
    STRICT_NORMAL = "strict_normal"
    FAULT_PASSTHROUGH = "fault_passthrough"


#: Anomaly identifiers, matching the keys of
#: :data:`md_contract.source.ANOMALY_TO_FAULT_KINDS`.
ANOMALY_NONPOSITIVE_PRICE: Final = "NONPOSITIVE_PRICE"
ANOMALY_CROSSED_QUOTE: Final = "CROSSED_QUOTE"
ANOMALY_NONPOSITIVE_SEQUENCE: Final = "NONPOSITIVE_SEQUENCE"
ANOMALY_NONPOSITIVE_TIME: Final = "NONPOSITIVE_TIME"
ANOMALY_NONPOSITIVE_SCALE: Final = "NONPOSITIVE_SCALE"


@dataclass(frozen=True)
class Anomaly:
    """One business-invariant violation found on a single event."""

    kind: str
    detail: str


@dataclass(frozen=True)
class Verdict:
    """The outcome of business validation for one event.

    ``preserved_faults`` names the manifest entries that licensed the anomalies.
    It is metadata *about* the event and is deliberately kept outside the
    Protobuf payload: Project 03 has to detect the anomaly itself and must not be
    handed the answer key (ADR-006).
    """

    accepted: bool
    anomalies: tuple[Anomaly, ...] = ()
    preserved_faults: tuple[FaultRecord, ...] = ()

    @property
    def fault_ids(self) -> tuple[str, ...]:
        return tuple(r.fault_id for r in self.preserved_faults)


def find_anomalies(event: DomainEvent) -> tuple[Anomaly, ...]:
    """Every business-invariant violation on this event, regardless of mode.

    Assumes the envelope check already passed, so the core scalars and the
    payload prices are present. Returns *all* violations rather than the first,
    because fault_passthrough has to confirm that each one is separately
    explained by the manifest.
    """
    found: list[Anomaly] = []

    if event.sequence is not None and event.sequence <= 0:
        found.append(
            Anomaly(ANOMALY_NONPOSITIVE_SEQUENCE, f"sequence={event.sequence}")
        )
    if event.event_time_ns is not None and event.event_time_ns <= 0:
        found.append(
            Anomaly(ANOMALY_NONPOSITIVE_TIME, f"event_time_ns={event.event_time_ns}")
        )
    if event.price_scale is not None and event.price_scale <= 0:
        found.append(
            Anomaly(ANOMALY_NONPOSITIVE_SCALE, f"price_scale={event.price_scale}")
        )

    if event.payload_case == PAYLOAD_QUOTE:
        for name in ("bid_ticks", "ask_ticks"):
            value = getattr(event, name)
            if value is not None and value <= 0:
                found.append(Anomaly(ANOMALY_NONPOSITIVE_PRICE, f"{name}={value}"))
        if (
            event.bid_ticks is not None
            and event.ask_ticks is not None
            and event.bid_ticks > event.ask_ticks
        ):
            found.append(
                Anomaly(
                    ANOMALY_CROSSED_QUOTE,
                    f"bid_ticks={event.bid_ticks} > ask_ticks={event.ask_ticks}",
                )
            )
    elif event.payload_case == PAYLOAD_TICK:
        if event.price_ticks is not None and event.price_ticks <= 0:
            found.append(
                Anomaly(ANOMALY_NONPOSITIVE_PRICE, f"price_ticks={event.price_ticks}")
            )

    if event.source_schema_version not in (None, SOURCE_SCHEMA_VERSION):
        found.append(
            Anomaly(
                "UNSUPPORTED_SOURCE_VERSION",
                f"source_schema_version={event.source_schema_version!r}",
            )
        )

    return tuple(found)


def validate_business(
    event: DomainEvent,
    *,
    mode: ValidationMode = ValidationMode.STRICT_NORMAL,
    fault_index: FaultIndex | None = None,
) -> Verdict:
    """Apply the mode's value policy. Raises :class:`PolicyError` on rejection.

    Note what is *not* here: no clamping, no sign flipping, no substitution of a
    default. An event either passes with its values byte-for-byte intact, or it
    is rejected. There is no third path that emits a modified event.
    """
    anomalies = find_anomalies(event)
    if not anomalies:
        return Verdict(accepted=True)

    if mode is ValidationMode.STRICT_NORMAL:
        raise PolicyError(
            Reason.BUSINESS_VIOLATION,
            "; ".join(f"{a.kind}({a.detail})" for a in anomalies),
            anomalies=[a.kind for a in anomalies],
            event_id=list(event.event_id),
        )

    if fault_index is None:
        raise PolicyError(
            Reason.FAULT_NOT_IN_MANIFEST,
            "fault_passthrough without a fault manifest cannot distinguish an "
            "intentional fault from corruption",
        )

    licensed: list[FaultRecord] = []
    unexplained: list[Anomaly] = []
    for anomaly in anomalies:
        record = fault_index.explains(event.symbol, event.sequence, anomaly.kind)
        if record is None:
            unexplained.append(anomaly)
        else:
            licensed.append(record)

    if unexplained:
        # Even in fault_passthrough. This is the case that keeps the mode
        # honest: anomalies the manifest does not account for are real failures
        # of the source or the pipeline, and calling them intentional would be
        # the "false success" the design forbids (section 3.3, case L03).
        raise PolicyError(
            Reason.FAULT_NOT_IN_MANIFEST,
            "; ".join(f"{a.kind}({a.detail})" for a in unexplained),
            anomalies=[a.kind for a in unexplained],
            event_id=list(event.event_id),
        )

    return Verdict(
        accepted=True, anomalies=anomalies, preserved_faults=tuple(dict.fromkeys(licensed))
    )
