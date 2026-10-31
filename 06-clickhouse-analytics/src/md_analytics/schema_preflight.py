"""Pin the raw and analytics schemas, and refuse to run against drift.

Matrix group B. P06 reads ``md_ingest.market_events_raw`` and must not change it
(FR-01), which makes the schema an interface -- and an interface that changes
under you silently is the problem this module exists to prevent.

Three habits it enforces:

* **Compare against ``system.columns``, not against ``CREATE TABLE`` success.**
  ``CREATE TABLE IF NOT EXISTS`` happily succeeds against a pre-existing table
  with a different definition, so "the DDL ran" proves nothing (case B-09).
* **Never ALTER to fix a mismatch.** A mismatch stops the build and asks for a
  human and a migration. Auto-ALTER on a landing table owned by another service
  is how one team's preflight becomes another team's outage.
* **Types matter down to nullability and width.** ``Int64`` becoming ``Float64``
  destroys nanosecond precision; ``Nullable(Int64)`` becoming ``Decimal64``
  changes arithmetic. Both are refusals, not warnings (case B-01, B-02, B-07).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final

from .errors import SchemaDriftError


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    name: str
    type: str
    position: int


#: ``md_ingest.market_events_raw`` as section 0.1 records it. Owned by P05; this
#: is the shape P06 is written against, not a shape P06 may impose.
EXPECTED_RAW_COLUMNS: Final = (
    ColumnSpec("dataset_id", "String", 1),
    ColumnSpec("symbol", "LowCardinality(String)", 2),
    ColumnSpec("sequence", "Int64", 3),
    ColumnSpec("event_kind", "LowCardinality(String)", 4),
    ColumnSpec("event_time_ns", "Int64", 5),
    ColumnSpec("event_date", "Date", 6),
    ColumnSpec("price_scale", "Int64", 7),
    ColumnSpec("bid_ticks", "Nullable(Int64)", 8),
    ColumnSpec("ask_ticks", "Nullable(Int64)", 9),
    ColumnSpec("price_ticks", "Nullable(Int64)", 10),
    ColumnSpec("source_schema_version", "LowCardinality(String)", 11),
    ColumnSpec("payload_digest", "FixedString(64)", 12),
    ColumnSpec("kafka_topic", "LowCardinality(String)", 13),
    ColumnSpec("kafka_partition", "Int32", 14),
    ColumnSpec("kafka_offset", "Int64", 15),
    ColumnSpec("topic_generation", "Int64", 16),
    ColumnSpec("ingest_run_id", "String", 17),
    ColumnSpec("ingested_at", "DateTime64(3, 'UTC')", 18),
)

EXPECTED_CANONICAL_COLUMNS: Final = (
    ColumnSpec("build_id", "String", 1),
    ColumnSpec("dataset_id", "String", 2),
    ColumnSpec("symbol", "LowCardinality(String)", 3),
    ColumnSpec("sequence", "Int64", 4),
    ColumnSpec("event_time_ns", "Int64", 5),
    ColumnSpec("event_date", "Date", 6),
    ColumnSpec("price_scale", "Int64", 7),
    ColumnSpec("price_ticks", "Int64", 8),
    ColumnSpec("payload_digest", "FixedString(64)", 9),
    ColumnSpec("source_schema_version", "LowCardinality(String)", 10),
)

#: The raw table is append-only ``MergeTree`` by P05's own decision (ADR-001).
#: Finding ``ReplacingMergeTree`` instead would mean rows are being collapsed by
#: a rule P06 has not verified, so the dedup it performs cannot be trusted to
#: match the business identity (case B-04).
EXPECTED_RAW_ENGINE: Final = "MergeTree"
EXPECTED_CANONICAL_ENGINE: Final = "MergeTree"
EXPECTED_BARS_ENGINE: Final = "AggregatingMergeTree"

#: Columns whose type must never widen or narrow silently, with the reason, so
#: an error message says what breaks rather than only that something changed.
_CRITICAL_TYPES: Final = {
    "event_time_ns": "nanosecond precision and ordering",
    "sequence": "tie-break ordering and overflow behaviour",
    "price_ticks": "fixed-point price exactness",
    "bid_ticks": "fixed-point price exactness",
    "ask_ticks": "fixed-point price exactness",
    "price_scale": "price scaling",
    "payload_digest": "digest comparison",
    "event_date": "UTC partitioning",
}


def schema_digest(columns: Iterable[ColumnSpec], *, engine: str = "") -> str:
    """Stable hash over names, types, positions and the engine."""
    h = hashlib.sha256()
    h.update(engine.encode("utf-8"))
    h.update(b"\0")
    for column in sorted(columns, key=lambda c: c.position):
        h.update(f"{column.position}:{column.name}:{column.type}".encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def _normalise(type_text: str) -> str:
    """Collapse whitespace so ``DateTime64(3, 'UTC')`` compares equal to itself.

    Quote style is *not* normalised: a type text differing by more than spacing
    is a different type as far as this check is concerned, and a preflight that
    guesses equivalences is the one that lets a real change through.
    """
    return " ".join(type_text.replace(", ", ",").split())


def preflight_columns(
    observed: Sequence[ColumnSpec],
    *,
    expected: Sequence[ColumnSpec] = EXPECTED_RAW_COLUMNS,
    table: str = "md_ingest.market_events_raw",
    require_exact_positions: bool = True,
) -> str:
    """Compare a live column list against the pinned one. Returns its digest.

    Raises :class:`SchemaDriftError` naming every difference at once: a preflight
    that stops at the first mismatch makes an operator run it N times to learn
    what a migration has to cover.
    """
    observed_by_name = {c.name: c for c in observed}
    expected_by_name = {c.name: c for c in expected}

    missing = [name for name in expected_by_name if name not in observed_by_name]
    unexpected = [name for name in observed_by_name if name not in expected_by_name]
    type_changes: list[str] = []
    position_changes: list[str] = []

    for name, want in expected_by_name.items():
        got = observed_by_name.get(name)
        if got is None:
            continue
        if _normalise(got.type) != _normalise(want.type):
            reason = _CRITICAL_TYPES.get(name)
            suffix = f" (breaks {reason})" if reason else ""
            type_changes.append(f"{name}: expected {want.type}, found {got.type}{suffix}")
        if require_exact_positions and got.position != want.position:
            position_changes.append(
                f"{name}: expected position {want.position}, found {got.position}"
            )

    problems: list[str] = []
    if missing:
        problems.append(f"missing columns: {sorted(missing)}")
    if unexpected:
        # An added column is drift, not an error in itself -- but P06 must not
        # guess what a new field means, so it stops and asks (case A-11, B-10).
        problems.append(f"unexpected columns: {sorted(unexpected)}")
    if type_changes:
        problems.append("type changes: " + "; ".join(sorted(type_changes)))
    if position_changes:
        problems.append("position changes: " + "; ".join(sorted(position_changes)))

    if problems:
        raise SchemaDriftError(
            f"{table} does not match the pinned schema: " + " | ".join(problems),
            table=table,
            missing=sorted(missing),
            unexpected=sorted(unexpected),
            type_changes=sorted(type_changes),
            position_changes=sorted(position_changes),
        )
    return schema_digest(observed)


def preflight_engine(observed_engine: str, *, expected: str, table: str) -> None:
    if observed_engine != expected:
        raise SchemaDriftError(
            f"{table} uses engine {observed_engine!r}, expected {expected!r}; "
            f"a different engine changes what a row means and whether rows are "
            f"collapsed behind P06's back",
            code="ENGINE_MISMATCH",
            table=table,
            observed_engine=observed_engine,
            expected_engine=expected,
        )


def preflight_raw(
    *,
    columns: Sequence[ColumnSpec],
    engine: str,
    pinned_digest: str | None = None,
) -> str:
    """Full raw-table preflight: engine, columns and an optional pinned digest.

    ``pinned_digest`` is the stronger check: columns and engine can match while
    something else about the definition moved, and a recorded digest from the
    last verified run catches that. A mismatch here blocks the build (case B-13).
    """
    preflight_engine(engine, expected=EXPECTED_RAW_ENGINE, table="md_ingest.market_events_raw")
    digest = preflight_columns(columns, expected=EXPECTED_RAW_COLUMNS)
    full = schema_digest(columns, engine=engine)
    if pinned_digest is not None and full != pinned_digest:
        raise SchemaDriftError(
            f"raw schema digest {full} does not match the pinned {pinned_digest}",
            code="SCHEMA_DIGEST_MISMATCH",
            observed=full,
            pinned=pinned_digest,
        )
    return full


def expected_raw_digest() -> str:
    """Digest of the pinned raw schema, for configs and manifests."""
    return schema_digest(EXPECTED_RAW_COLUMNS, engine=EXPECTED_RAW_ENGINE)


def expected_canonical_digest() -> str:
    return schema_digest(EXPECTED_CANONICAL_COLUMNS, engine=EXPECTED_CANONICAL_ENGINE)
