"""The real ClickHouse behind the row shapes the reconcilers already consume.

The companion to :mod:`p07.replay.live_kafka`. Together they are what moves this
project off the ``UNIT/MODEL`` profile: the Kafka adapter lets the cut scanner
see real records, and this lets
:func:`p07.reconcile.raw_reconciler.reconcile_raw` and
:func:`p07.reconcile.canonical_reconciler.reconcile_canonical` see real rows.

As with the broker, nothing in the reconcilers changes. ``reconcile_raw`` takes
an iterable of ``RawRow``; where those rows came from was never its business.
That is why P09's finding called this wiring rather than redesign.

Three choices worth stating:

* **HTTP, not the native protocol.** P05 writes over HTTP on 8123, so the data
  path never needs port 9000 -- and on this host the native listener has been
  observed to stop accepting connections while the container still reports
  healthy. An evidence plane should not depend on the flakier of two paths.
* **Rows are read in stored order, duplicates and all.** At-least-once delivery
  means the raw table legitimately holds several physical rows per logical
  identity. Collapsing them here would hide both the duplication P05 documents
  and any genuine conflict between two rows under one identity.
* **A column that cannot be parsed raises.** The reconcilers compare digests and
  provenance; a row assembled with a defaulted field would compare as a
  mismatch and be reported as a data defect, when the defect was in the reader.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterator, Sequence

from ..dataplane.clickhouse_fake import RawRow

#: Columns requested explicitly. ``SELECT *`` would let a column added upstream
#: shift every value after it, and the rows would still parse.
RAW_COLUMNS: tuple[str, ...] = (
    "dataset_id",
    "symbol",
    "sequence",
    "event_time_ns",
    "price_scale",
    "event_kind",
    "source_schema_version",
    "bid_ticks",
    "ask_ticks",
    "price_ticks",
    "payload_digest",
    "topic_generation",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
)

CANONICAL_COLUMNS: tuple[str, ...] = (
    "build_id",
    "dataset_id",
    "symbol",
    "sequence",
    "event_time_ns",
    "price_scale",
    "price_ticks",
    "payload_digest",
    "source_schema_version",
)


class LiveClickHouseUnavailable(RuntimeError):
    """The server could not be read, so nothing may be concluded."""


def _quote(value: str) -> str:
    """Single-quote a string literal for ClickHouse, escaping what matters."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


class LiveClickHouse:
    """Read-only HTTP client. Issues no DDL and writes nothing."""

    def __init__(self, url: str, *, timeout_s: float = 30.0) -> None:
        self._url = url.rstrip("/")
        self._timeout_s = timeout_s

    def query_tsv(self, sql: str) -> list[list[str]]:
        """Run a read query and return rows of raw strings.

        TSV rather than JSON on purpose: ClickHouse renders Int64 in JSON as a
        number, and values above 2^53 lose precision in a JSON parser. Prices
        here are scaled integers and the corpus deliberately contains large
        ones, so they are parsed from text straight into ``int``.
        """
        url = self._url + "/?query=" + urllib.parse.quote(sql)
        try:
            with urllib.request.urlopen(url, timeout=self._timeout_s) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:400]
            raise LiveClickHouseUnavailable(
                "ClickHouse rejected the query: %s" % detail
            ) from exc
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise LiveClickHouseUnavailable(
                "cannot reach ClickHouse at %s: %s" % (self._url, exc)
            ) from exc

        return [line.split("\t") for line in body.split("\n") if line]

    def version(self) -> str:
        rows = self.query_tsv("SELECT version()")
        return rows[0][0] if rows else "unknown"


def _as_int(value: str, field: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise LiveClickHouseUnavailable(
            "%s is not an integer: %r" % (field, value)
        ) from exc


def _as_optional_int(value: str, field: str) -> int | None:
    # ClickHouse renders NULL as \N in TSV. Treated as absent, which differs
    # from zero: a quote with bid_ticks absent and one with bid_ticks=0 are
    # different events, and the digest distinguishes them.
    if value in ("\\N", ""):
        return None
    return _as_int(value, field)


def read_raw_rows(
    client: LiveClickHouse,
    *,
    dataset_id: str,
    table: str = "md_ingest.market_events_raw",
) -> Iterator[RawRow]:
    """Stream every physical row of one dataset, in stored order.

    Ordered by the Kafka coordinate so a re-read is reproducible and so each
    row's provenance can be matched against the scanned cut.
    """
    if not dataset_id:
        raise LiveClickHouseUnavailable(
            "a dataset_id is required; refusing to read the whole table"
        )
    sql = (
        "SELECT %s FROM %s WHERE dataset_id = %s "
        "ORDER BY kafka_topic, kafka_partition, kafka_offset FORMAT TSV"
        % (", ".join(RAW_COLUMNS), table, _quote(dataset_id))
    )
    for values in client.query_tsv(sql):
        if len(values) != len(RAW_COLUMNS):
            raise LiveClickHouseUnavailable(
                "expected %d columns, got %d; the raw table shape has changed"
                % (len(RAW_COLUMNS), len(values))
            )
        record = dict(zip(RAW_COLUMNS, values))
        yield RawRow(
            dataset_id=record["dataset_id"],
            symbol=record["symbol"],
            sequence=_as_int(record["sequence"], "sequence"),
            event_time_ns=_as_int(record["event_time_ns"], "event_time_ns"),
            price_scale=_as_int(record["price_scale"], "price_scale"),
            event_kind=record["event_kind"],
            source_schema_version=record["source_schema_version"],
            bid_ticks=_as_optional_int(record["bid_ticks"], "bid_ticks"),
            ask_ticks=_as_optional_int(record["ask_ticks"], "ask_ticks"),
            price_ticks=_as_optional_int(record["price_ticks"], "price_ticks"),
            payload_digest=record["payload_digest"],
            topic_generation=record["topic_generation"],
            topic=record["kafka_topic"],
            partition=_as_int(record["kafka_partition"], "kafka_partition"),
            offset=_as_int(record["kafka_offset"], "kafka_offset"),
        )


def read_canonical_rows(
    client: LiveClickHouse,
    *,
    dataset_id: str,
    build_id: str | None = None,
    table: str = "md_analytics.canonical_ticks",
) -> list[dict[str, Any]]:
    """Read the canonical ticks of one build.

    ``build_id`` is required in practice: querying across builds mixes two
    immutable snapshots into one answer, which P06 forbids. It is optional here
    only so a caller can discover which builds exist first.
    """
    predicate = "dataset_id = %s" % _quote(dataset_id)
    if build_id:
        predicate += " AND build_id = %s" % _quote(build_id)
    sql = "SELECT %s FROM %s WHERE %s ORDER BY symbol, sequence FORMAT TSV" % (
        ", ".join(CANONICAL_COLUMNS),
        table,
        predicate,
    )
    rows: list[dict[str, Any]] = []
    for values in client.query_tsv(sql):
        if len(values) != len(CANONICAL_COLUMNS):
            raise LiveClickHouseUnavailable(
                "expected %d canonical columns, got %d"
                % (len(CANONICAL_COLUMNS), len(values))
            )
        record = dict(zip(CANONICAL_COLUMNS, values))
        rows.append(
            {
                "build_id": record["build_id"],
                "dataset_id": record["dataset_id"],
                "symbol": record["symbol"],
                "sequence": _as_int(record["sequence"], "sequence"),
                "event_time_ns": _as_int(record["event_time_ns"], "event_time_ns"),
                "price_scale": _as_int(record["price_scale"], "price_scale"),
                "price_ticks": _as_optional_int(record["price_ticks"], "price_ticks"),
                "payload_digest": record["payload_digest"],
                "source_schema_version": record["source_schema_version"],
            }
        )
    return rows


def raw_row_summary(rows: Sequence[RawRow]) -> dict[str, Any]:
    """Counts a caller reports, kept apart deliberately.

    Physical rows and logical identities are different numbers, and the gap
    between them is the at-least-once duplication P05 documents rather than a
    defect. Reporting either one alone invites the wrong conclusion.
    """
    identities = {(r.dataset_id, r.symbol, r.sequence) for r in rows}
    by_kind: dict[str, int] = {}
    for row in rows:
        by_kind[row.event_kind] = by_kind.get(row.event_kind, 0) + 1
    return {
        "physical_rows": len(rows),
        "logical_identities": len(identities),
        "physical_rows_by_kind": by_kind,
        "duplication_factor": (
            round(len(rows) / len(identities), 4) if identities else None
        ),
        "profile": "LIVE",
    }


def as_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True)
