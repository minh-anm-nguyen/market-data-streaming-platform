"""Golden corpora and a fixture builder for the test matrix.

Section 14.1. A corpus is a *source* manifest plus the raw rows P05 would have
landed, plus a sealed cut -- the three things the audit compares. Building them
through one API keeps the expected set and the rows in step: a fixture that
forgets to put an injected event in the ledger produces an "extra" finding that
belongs to the fixture rather than to the system under test.

The injectors are named for what they inject, and each one is deliberately
distinguishable from its neighbours:

* :meth:`CorpusBuilder.duplicate` -- same identity, same semantic core, new Kafka
  offset. At-least-once delivery. Must collapse to one canonical row.
* :meth:`CorpusBuilder.conflict` -- same identity, *different* core. Must stop
  the build.
* :meth:`CorpusBuilder.suppress_from_raw` -- in the accepted ledger, absent from
  raw. Must be reported missing.
* :meth:`CorpusBuilder.unledgered` -- in raw, absent from the ledger. Must be
  reported extra.
* :meth:`CorpusBuilder.after_cut` -- in raw at an offset past the seal. Must be
  out of scope, which is neither missing nor extra.

The last two are easy to conflate and the matrix separates them (case F-08 vs
E-12), so the builder does too.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final

from .contracts import (
    EVENT_QUOTE,
    EVENT_TICK,
    NS_PER_MINUTE,
    SourceIdentity,
    payload_digest,
)
from .oracle import Bar, BarKey
from .rows import RawRow, TopicPartition, make_raw_row
from .snapshot_cutter import (
    AcceptedEvent,
    SnapshotCut,
    SourceManifest,
    TopicPartitionCut,
    seal_snapshot,
)

TICKS_TOPIC: Final = "md.ticks.v1"
QUOTES_TOPIC: Final = "md.quotes.v1"

#: 8 partitions per topic, matching the P04 benchmark profile. The matrix is
#: explicit that this is 8 *per topic* and not 8 in total (case A-14).
PARTITIONS_PER_TOPIC: Final = 8

SCHEMA_VERSION: Final = "marketdata.v1"

#: 2026-10-30T12:00:00Z, chosen so the golden corpus sits on a minute boundary
#: and the arithmetic in assertions is readable.
GOLDEN_BASE_NS: Final = 1_793_361_600_000_000_000


def partition_for(symbol: str) -> int:
    """Stable partition assignment, as a keyed producer would do.

    Uses a fixed FNV-1a rather than :func:`hash`, whose string seed changes per
    process: a fixture whose partition layout moved between runs would make the
    cut non-reproducible.
    """
    h = 0x811C9DC5
    for byte in symbol.encode("utf-8"):
        h = ((h ^ byte) * 0x01000193) & 0xFFFFFFFF
    return h % PARTITIONS_PER_TOPIC


@dataclass(slots=True)
class Corpus:
    """A sealed fixture: manifest, raw rows and the cut that bounds them."""

    dataset_id: str
    manifest: SourceManifest
    raw_rows: list[RawRow]
    cut: SnapshotCut

    def ticks_in_scope(self) -> list[RawRow]:
        return [
            row
            for row in self.raw_rows
            if row.event_kind == EVENT_TICK
            and (bound := self.cut.offset_bound(row.topic_partition)) is not None
            and row.kafka_offset < bound
        ]

    def accepted_tick_identities(self) -> frozenset[SourceIdentity]:
        return self.manifest.accepted_tick_identities()


class CorpusBuilder:
    """Accumulates source events and the raw rows that carry them."""

    def __init__(
        self,
        dataset_id: str,
        *,
        price_scale: int = 100_000,
        schema_version: str = SCHEMA_VERSION,
        topic_generation: int = 1,
        ingest_run_id: str = "p05-run-1",
    ) -> None:
        self.dataset_id = dataset_id
        self.price_scale = price_scale
        self.schema_version = schema_version
        self.topic_generation = topic_generation
        self.ingest_run_id = ingest_run_id

        self._rows: list[RawRow] = []
        self._accepted: list[AcceptedEvent] = []
        self._rejected: list[tuple[SourceIdentity, str]] = []
        self._offsets: dict[TopicPartition, int] = {}
        self._cut_bounds: dict[TopicPartition, int] | None = None
        self._ingest_clock = 0

    # ----------------------------------------------------------- bookkeeping

    def _topic_partition(self, event_kind: str, symbol: str) -> TopicPartition:
        topic = TICKS_TOPIC if event_kind == EVENT_TICK else QUOTES_TOPIC
        return TopicPartition(topic, partition_for(symbol), self.topic_generation)

    def _next_offset(self, tp: TopicPartition) -> int:
        offset = self._offsets.get(tp, 0)
        self._offsets[tp] = offset + 1
        return offset

    def _next_ingest_time(self) -> int:
        self._ingest_clock += 1
        return self._ingest_clock

    def _append_row(
        self,
        *,
        symbol: str,
        sequence: int,
        event_kind: str,
        event_time_ns: int,
        price_ticks: int | None,
        bid_ticks: int | None,
        ask_ticks: int | None,
        price_scale: int | None,
        schema_version: str | None,
        event_date_override=None,
        digest_override: str | None = None,
        topic_generation: int | None = None,
    ) -> RawRow:
        tp = self._topic_partition(event_kind, symbol)
        if topic_generation is not None:
            tp = TopicPartition(tp.topic, tp.partition, topic_generation)
        row = make_raw_row(
            dataset_id=self.dataset_id,
            symbol=symbol,
            sequence=sequence,
            event_kind=event_kind,
            event_time_ns=event_time_ns,
            price_scale=self.price_scale if price_scale is None else price_scale,
            price_ticks=price_ticks,
            bid_ticks=bid_ticks,
            ask_ticks=ask_ticks,
            source_schema_version=(
                self.schema_version if schema_version is None else schema_version
            ),
            kafka_topic=tp.topic,
            kafka_partition=tp.partition,
            kafka_offset=self._next_offset(tp),
            topic_generation=tp.topic_generation,
            ingest_run_id=self.ingest_run_id,
            ingested_at=self._next_ingest_time(),
            event_date=event_date_override,
            payload_digest_override=digest_override,
        )
        self._rows.append(row)
        return row

    def _accept(self, row: RawRow) -> None:
        self._accepted.append(
            AcceptedEvent(
                identity=row.identity,
                event_kind=row.event_kind,
                payload_digest=row.recomputed_digest(),
            )
        )

    # -------------------------------------------------------------- injectors

    def tick(
        self,
        symbol: str,
        sequence: int,
        event_time_ns: int,
        price_ticks: int,
        *,
        price_scale: int | None = None,
        accepted: bool = True,
        schema_version: str | None = None,
        event_date_override=None,
        digest_override: str | None = None,
    ) -> RawRow:
        row = self._append_row(
            symbol=symbol,
            sequence=sequence,
            event_kind=EVENT_TICK,
            event_time_ns=event_time_ns,
            price_ticks=price_ticks,
            bid_ticks=None,
            ask_ticks=None,
            price_scale=price_scale,
            schema_version=schema_version,
            event_date_override=event_date_override,
            digest_override=digest_override,
        )
        if accepted:
            self._accept(row)
        return row

    def quote(
        self,
        symbol: str,
        sequence: int,
        event_time_ns: int,
        bid_ticks: int,
        ask_ticks: int,
        *,
        price_scale: int | None = None,
        accepted: bool = True,
    ) -> RawRow:
        row = self._append_row(
            symbol=symbol,
            sequence=sequence,
            event_kind=EVENT_QUOTE,
            event_time_ns=event_time_ns,
            price_ticks=None,
            bid_ticks=bid_ticks,
            ask_ticks=ask_ticks,
            price_scale=price_scale,
            schema_version=None,
        )
        if accepted:
            self._accept(row)
        return row

    def duplicate(self, row: RawRow, *, times: int = 1) -> list[RawRow]:
        """Re-land an identical event at new Kafka offsets.

        The duplicate is byte-identical in every semantic field and differs only
        in transport coordinates and ingest clock, which is what a retry
        produces. It is *not* added to the ledger again: one source event.
        """
        out: list[RawRow] = []
        for _ in range(times):
            copy = row._replace(
                kafka_offset=self._next_offset(row.topic_partition),
                ingested_at=self._next_ingest_time(),
            )
            self._rows.append(copy)
            out.append(copy)
        return out

    def conflict(self, row: RawRow, **changes: object) -> RawRow:
        """Re-land the same identity with a *different* semantic core.

        The digest is recomputed from the changed fields, so the conflicting row
        is internally consistent -- the audit has to notice that two internally
        consistent rows disagree, which is harder than noticing a bad digest.
        """
        tp = row.topic_partition
        mutated = row._replace(
            kafka_offset=self._next_offset(tp),
            ingested_at=self._next_ingest_time(),
            **changes,  # type: ignore[arg-type]
        )
        mutated = mutated._replace(payload_digest=mutated.recomputed_digest())
        self._rows.append(mutated)
        return mutated

    def suppress_from_raw(self, row: RawRow) -> None:
        """Drop a row from raw while leaving it in the accepted ledger."""
        self._rows.remove(row)

    def unledgered(self, row: RawRow) -> None:
        """Remove a row from the accepted ledger while leaving it in raw."""
        self._accepted = [e for e in self._accepted if e.identity != row.identity]

    def reject_at_source(self, identity: SourceIdentity, reason: str) -> None:
        """Record that P03 refused to publish an event (case A-04)."""
        self._rejected.append((identity, reason))

    # --------------------------------------------------------------- sealing

    def seal(
        self,
        *,
        p05_state: str = "DRAINED",
        seal_reason: str = "durable_sink_frontier",
        captured_at_ns: int = GOLDEN_BASE_NS + 10 * NS_PER_MINUTE,
        frontier_offset_by: int = 0,
        earliest_available_offset: int = 0,
    ) -> Corpus:
        """Capture the cut at the current offsets and return the corpus.

        ``frontier_offset_by`` makes the consumer frontier lag the cut, which is
        how case F-01/A-12 is produced: everything else looks finished, but the
        sink has not acknowledged up to the bound.
        """
        bounds = dict(self._offsets)
        self._cut_bounds = bounds
        cuts = [
            TopicPartitionCut(
                topic_partition=tp,
                end_offset=end,
                consumer_frontier=end - frontier_offset_by,
                earliest_available_offset=earliest_available_offset,
            )
            for tp, end in sorted(bounds.items(), key=lambda kv: (kv[0].topic, kv[0].partition))
        ]
        manifest = self.manifest()
        cut = seal_snapshot(
            manifest=manifest,
            cuts=cuts,
            p05_state=p05_state,
            seal_reason=seal_reason,
            captured_at_ns=captured_at_ns,
        )
        return Corpus(
            dataset_id=self.dataset_id,
            manifest=manifest,
            raw_rows=list(self._rows),
            cut=cut,
        )

    def manifest(self) -> SourceManifest:
        """Build the manifest and stamp it with its own content digest."""
        draft = SourceManifest(
            dataset_id=self.dataset_id,
            accepted=tuple(self._accepted),
            rejected=tuple(self._rejected),
            generator_version="fixtures.1",
        )
        return SourceManifest(
            dataset_id=draft.dataset_id,
            accepted=draft.accepted,
            rejected=draft.rejected,
            declared_sha256=draft.content_digest(),
            generator_version=draft.generator_version,
        )

    def after_cut(
        self,
        symbol: str,
        sequence: int,
        event_time_ns: int,
        price_ticks: int,
        *,
        accepted: bool = False,
    ) -> RawRow:
        """Land a row past the sealed bound (case E-12).

        Must be called after :meth:`seal`, and the resulting row is out of scope
        for the sealed cut: it belongs to the next build, not to this one.
        """
        if self._cut_bounds is None:
            raise RuntimeError("after_cut() requires a prior seal()")
        return self.tick(
            symbol, sequence, event_time_ns, price_ticks, accepted=accepted
        )

    def rows(self) -> list[RawRow]:
        return list(self._rows)


# --------------------------------------------------------------------- golden


@dataclass(frozen=True, slots=True)
class GoldenCase:
    corpus: Corpus
    expected_bars: dict[BarKey, Bar]
    description: str = ""


def golden_six_event(dataset_id: str = "GOLDEN-01", build_id: str = "") -> GoldenCase:
    """The section 16.1 corpus: 4 ticks and 2 quotes over two minutes.

    Transcribed from the design document, including its expected bars, so the
    first assertion in the suite is one a reader can check against the spec by
    eye:

        bar [12:00, 12:01) OPEN=100000 HIGH=100200 LOW=99900 CLOSE=99900 count=3
        bar [12:01, 12:02) OPEN=100100 HIGH=100100 LOW=100100 CLOSE=100100 count=1

    Note CLOSE=99900 for the first bar: ticks 3 and 4 share a timestamp and the
    sequence breaks the tie, so the later *sequence* wins -- not the higher price
    (case H-11).
    """
    base = GOLDEN_BASE_NS
    builder = CorpusBuilder(dataset_id, price_scale=100_000)
    builder.tick("EURUSD", 1, base, 100_000)
    builder.quote("EURUSD", 2, base + 20 * 1_000_000_000, 100_010, 100_012)
    builder.tick("EURUSD", 3, base + 30 * 1_000_000_000, 100_200)
    builder.tick("EURUSD", 4, base + 30 * 1_000_000_000, 99_900)
    builder.quote("EURUSD", 5, base + 45 * 1_000_000_000, 99_950, 99_952)
    builder.tick("EURUSD", 6, base + NS_PER_MINUTE, 100_100)
    corpus = builder.seal()

    expected = {
        BarKey(build_id, dataset_id, "EURUSD", 100_000, base): Bar(
            open_ticks=100_000,
            high_ticks=100_200,
            low_ticks=99_900,
            close_ticks=99_900,
            tick_count=3,
        ),
        BarKey(build_id, dataset_id, "EURUSD", 100_000, base + NS_PER_MINUTE): Bar(
            open_ticks=100_100,
            high_ticks=100_100,
            low_ticks=100_100,
            close_ticks=100_100,
            tick_count=1,
        ),
    }
    return GoldenCase(corpus, expected, "section 16.1 six-event golden corpus")


def rebase_expected(
    expected: dict[BarKey, Bar], *, build_id: str
) -> dict[BarKey, Bar]:
    """Re-stamp expected bar keys with the build id a test actually produced."""
    return {key._replace(build_id=build_id): bar for key, bar in expected.items()}


def synthetic_corpus(
    *,
    dataset_id: str,
    symbols: Sequence[str],
    ticks_per_symbol: int,
    base_ns: int = GOLDEN_BASE_NS,
    step_ns: int = 1_000_000_000,
    start_price: int = 100_000,
    price_scale: int = 100_000,
    quote_every: int = 0,
    duplicate_every: int = 0,
) -> Corpus:
    """A deterministic corpus of arbitrary size, with a reproducible manifest.

    Prices follow a fixed integer sawtooth rather than a PRNG so a failure is
    reproducible from the parameters alone, and so the expected OHLC of any
    window can be derived by hand when a test needs to be checked by a human.
    ``quote_every`` interleaves quotes, which must never reach a tick bar;
    ``duplicate_every`` re-lands physical duplicates.
    """
    builder = CorpusBuilder(dataset_id, price_scale=price_scale)
    for symbol_index, symbol in enumerate(symbols):
        sequence = 0
        for i in range(ticks_per_symbol):
            sequence += 1
            price = start_price + (i % 97) * 10 - (i % 13) * 7 + symbol_index * 1_000
            row = builder.tick(symbol, sequence, base_ns + i * step_ns, price)
            if duplicate_every and (i % duplicate_every == 0):
                builder.duplicate(row)
            if quote_every and (i % quote_every == 0):
                sequence += 1
                builder.quote(
                    symbol,
                    sequence,
                    base_ns + i * step_ns + step_ns // 2,
                    price - 5,
                    price + 5,
                )
    return builder.seal()


def digest_of(row: RawRow) -> str:
    """Convenience for tests that need the digest a row *should* carry."""
    return payload_digest(row.semantic_core())
