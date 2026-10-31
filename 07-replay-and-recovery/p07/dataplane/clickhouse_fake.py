"""P05's raw sink and P06's canonical/bar/catalog store, as a state machine.

Four behaviours are modelled because the recovery protocol turns on them:

**An INSERT can commit and then lose its reply.** ``ack_loss_once`` makes
:meth:`FakeClickHouse.insert_raw` write the rows and raise
:class:`SinkAckUnknown`. The client cannot distinguish that from a write that
never landed, and design 5.3 forbids treating the timeout as either outcome. The
retry then produces a second *physical* row -- allowed -- while the canonical
projection must still hold exactly one logical row per identity.

**Raw is physical, canonical is logical.** ``MergeTree`` enforces no uniqueness
(P06 section 4.1), so :attr:`FakeClickHouse.raw_rows` is a list and duplicates
live in it happily. Uniqueness is a property the builder must establish and an
independent verifier must confirm -- never something the storage does for you.

**Publication is a compare-and-swap on a pointer.** :meth:`publish_ready` takes
the build it expects to be current and refuses if that changed, which is what
makes two racing builders safe (case ``E2E-31``). Crashing between the swap and
the client's acknowledgement is survivable precisely because the pointer can be
read back (case ``E2E-30``).

**TTL deletes rows the audit still needs.** :meth:`ttl_expire_raw` removes raw
rows by ingestion time. An audit that then finds fewer rows must report
``RAW_ROWS_EXPIRED`` and stop, not conclude the data was never there
(case ``E2E-33``).

A published build is immutable here by construction: :meth:`insert_canonical`
and :meth:`insert_bars` refuse to touch a build that is ``READY``, so
``INV-13`` cannot be violated by accident -- only by code that deliberately
reaches past the API, which is what the mutation tests do.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterator, Mapping, Sequence

from ..contracts import EventKind, SemanticCore, SourceIdentity


class SinkError(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


class SinkUnavailable(SinkError):
    """The server is not accepting queries at all."""


class DiskFull(SinkError):
    """Out of space. The rows did not land."""


class SinkAckUnknown(SinkError):
    """The rows landed; the reply did not arrive.

    ``rows_written`` is lab-only knowledge, kept so a scenario can later state
    what an independent scan *should* find. Ingestion code must not read it.
    """

    def __init__(self, rows_written: int) -> None:
        super().__init__("SINK_ACK_UNKNOWN", f"{rows_written} rows already committed")
        self.rows_written = rows_written


class BuildStatus(str, Enum):
    """P06's build lifecycle. ``READY`` is reached only through the catalog."""

    STAGING = "STAGING"
    CANDIDATE = "CANDIDATE"
    VERIFIED = "VERIFIED"
    READY = "READY"
    DIRTY = "DIRTY"


@dataclass(frozen=True, slots=True)
class RawRow:
    """One physical row of ``md_ingest.market_events_raw``.

    Provenance is stored so every row can be tied back to the Kafka record that
    produced it. A row with no verifiable provenance is an ``extra`` in the
    reconciliation, not a curiosity (design 7.2).
    """

    dataset_id: str
    symbol: str
    sequence: int
    event_time_ns: int
    price_scale: int
    event_kind: str
    source_schema_version: str
    bid_ticks: int | None
    ask_ticks: int | None
    price_ticks: int | None
    payload_digest: str
    topic_generation: str
    topic: str
    partition: int
    offset: int
    batch_id: str
    ingested_at_ns: int

    @property
    def identity(self) -> SourceIdentity:
        return SourceIdentity(self.dataset_id, self.symbol, self.sequence)

    def semantic_core(self) -> SemanticCore:
        """Rebuild the core from stored columns, presence included.

        Note it does **not** use ``payload_digest``: the reconciler recomputes
        the digest from these columns so a stored digest cannot vouch for itself
        (design 3.1).
        """
        return SemanticCore(
            dataset_id=self.dataset_id,
            symbol=self.symbol,
            sequence=self.sequence,
            event_time_ns=self.event_time_ns,
            price_scale=self.price_scale,
            event_kind=EventKind.from_wire_name(self.event_kind),
            source_schema_version=self.source_schema_version,
            bid_ticks=self.bid_ticks,
            ask_ticks=self.ask_ticks,
            price_ticks=self.price_ticks,
        )


@dataclass(frozen=True, slots=True)
class CanonicalRow:
    """One logical row of a canonical build."""

    build_id: str
    core: SemanticCore
    payload_digest: str


@dataclass(frozen=True, slots=True)
class BarRow:
    """One materialised 1-minute bar of a build."""

    build_id: str
    dataset_id: str
    symbol: str
    price_scale: int
    bar_start_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    tick_count: int


@dataclass(slots=True)
class CatalogEntry:
    """What the catalog records about a build."""

    build_id: str
    status: BuildStatus
    source_manifest_sha256: str
    expected_identity_count: int
    evidence_refs: tuple[str, ...] = ()
    published_at_ns: int | None = None


class FakeClickHouse:
    """Raw table, per-build canonical and bar tables, and the READY catalog."""

    def __init__(self, *, disk_free_bytes: int = 1 << 30, bytes_per_row: int = 128) -> None:
        self.raw_rows: list[RawRow] = []
        self._canonical: dict[str, dict[SourceIdentity, CanonicalRow]] = {}
        self._bars: dict[str, dict[tuple[str, str, int, int], BarRow]] = {}
        self._catalog: dict[str, CatalogEntry] = {}
        self._ready_build: str | None = None
        self._disk_free_bytes = disk_free_bytes
        self._bytes_per_row = bytes_per_row
        self.available = True
        #: One-shot fault switches, set by the ClickHouse injector.
        self.ack_loss_once = False
        self.fail_before_commit_once = False
        self.insert_calls = 0
        self.ready_pointer_writes = 0

    # -- raw table -------------------------------------------------------

    def insert_raw(self, rows: Sequence[RawRow]) -> int:
        """Insert physical rows. Returns how many landed.

        Order of checks is deliberate: availability, then the injected
        pre-commit failure, then space, then the write, then the injected lost
        ACK. Each earlier stage means "nothing was written"; only the last means
        "written but unacknowledged", and conflating the two is the ambiguity
        this whole module exists to reproduce.
        """
        self.insert_calls += 1
        if not self.available:
            raise SinkUnavailable("SINK_UNAVAILABLE", "server down")
        if self.fail_before_commit_once:
            self.fail_before_commit_once = False
            raise SinkError("SINK_IO_ERROR", "injected failure before commit")
        needed = len(rows) * self._bytes_per_row
        if needed > self._disk_free_bytes:
            raise DiskFull("SINK_DISK_FULL", f"need {needed}B, have {self._disk_free_bytes}B")
        self._disk_free_bytes -= needed
        self.raw_rows.extend(rows)
        if self.ack_loss_once:
            self.ack_loss_once = False
            raise SinkAckUnknown(len(rows))
        return len(rows)

    def scan_raw(self, *, dataset_id: str | None = None) -> Iterator[RawRow]:
        """Read-only scan. The evidence plane's only way in."""
        if not self.available:
            raise SinkUnavailable("SINK_UNAVAILABLE", "server down")
        for row in self.raw_rows:
            if dataset_id is None or row.dataset_id == dataset_id:
                yield row

    def raw_row_count(self, *, dataset_id: str | None = None) -> int:
        return sum(1 for _ in self.scan_raw(dataset_id=dataset_id))

    def ttl_expire_raw(self, *, before_ingested_at_ns: int) -> int:
        """Delete raw rows ingested before the cutoff. Returns the count."""
        before = len(self.raw_rows)
        self.raw_rows = [row for row in self.raw_rows if row.ingested_at_ns >= before_ingested_at_ns]
        removed = before - len(self.raw_rows)
        self._disk_free_bytes += removed * self._bytes_per_row
        return removed

    # -- builds -----------------------------------------------------------

    def open_build(
        self, build_id: str, *, source_manifest_sha256: str, expected_identity_count: int
    ) -> CatalogEntry:
        if build_id in self._catalog:
            raise SinkError("BUILD_EXISTS", build_id)
        entry = CatalogEntry(
            build_id=build_id,
            status=BuildStatus.STAGING,
            source_manifest_sha256=source_manifest_sha256,
            expected_identity_count=expected_identity_count,
        )
        self._catalog[build_id] = entry
        self._canonical[build_id] = {}
        self._bars[build_id] = {}
        return entry

    def _mutable_build(self, build_id: str) -> CatalogEntry:
        entry = self._catalog.get(build_id)
        if entry is None:
            raise SinkError("UNKNOWN_BUILD", build_id)
        if entry.status is BuildStatus.READY or build_id == self._ready_build:
            # P06 NFR-10: a published snapshot is immutable. Correcting history
            # means a new build, never an edit in place.
            raise SinkError("READY_IMMUTABILITY_VIOLATED", build_id)
        return entry

    def insert_canonical(self, build_id: str, rows: Sequence[CanonicalRow]) -> int:
        """Insert canonical rows into a staging build.

        Last write wins *per identity* inside a build, which models a staging
        table that gets replaced rather than appended to (P06 4.1). It is not a
        dedup guarantee: the builder still has to put exactly one row per
        identity in, and the verifier still has to confirm it did.
        """
        self._mutable_build(build_id)
        table = self._canonical[build_id]
        for row in rows:
            table[row.core.identity] = row
        return len(rows)

    def insert_bars(self, build_id: str, rows: Sequence[BarRow]) -> int:
        self._mutable_build(build_id)
        table = self._bars[build_id]
        for row in rows:
            table[(row.dataset_id, row.symbol, row.price_scale, row.bar_start_ns)] = row
        return len(rows)

    def scan_canonical(self, build_id: str) -> Iterator[CanonicalRow]:
        if not self.available:
            raise SinkUnavailable("SINK_UNAVAILABLE", "server down")
        yield from self._canonical.get(build_id, {}).values()

    def scan_bars(self, build_id: str) -> Iterator[BarRow]:
        if not self.available:
            raise SinkUnavailable("SINK_UNAVAILABLE", "server down")
        yield from self._bars.get(build_id, {}).values()

    def set_build_status(self, build_id: str, status: BuildStatus) -> None:
        entry = self._catalog.get(build_id)
        if entry is None:
            raise SinkError("UNKNOWN_BUILD", build_id)
        if entry.status is BuildStatus.READY and status is not BuildStatus.READY:
            raise SinkError("READY_IMMUTABILITY_VIOLATED", build_id)
        entry.status = status

    def build_status(self, build_id: str) -> BuildStatus | None:
        entry = self._catalog.get(build_id)
        return entry.status if entry else None

    def builds(self) -> Mapping[str, CatalogEntry]:
        return dict(self._catalog)

    # -- catalog ----------------------------------------------------------

    @property
    def ready_build(self) -> str | None:
        """The published build, read back from the pointer.

        Reading back is how a crash between the swap and the acknowledgement is
        resolved: the pointer is the truth, the client's missing reply is not
        (case ``E2E-30``).
        """
        return self._ready_build

    def publish_ready(
        self,
        build_id: str,
        *,
        expected_current: str | None,
        evidence_refs: Sequence[str] = (),
        now_ns: int = 0,
    ) -> bool:
        """Compare-and-swap the READY pointer. ``False`` means the race was lost.

        Requires the build to be ``VERIFIED``. A ``CANDIDATE`` or ``DIRTY``
        build cannot be published, which is the gate behind ``INV-05``: no false
        READY, ever.
        """
        entry = self._catalog.get(build_id)
        if entry is None:
            raise SinkError("UNKNOWN_BUILD", build_id)
        if self._ready_build == build_id and entry.status is BuildStatus.READY:
            # Already published. A retry of the *same* build is idempotent,
            # which is what makes case E2E-30 survivable: the pointer moved, the
            # client never learned it, and re-issuing the publish must confirm
            # rather than fail.
            return True
        if entry.status is not BuildStatus.VERIFIED:
            raise SinkError("BUILD_NOT_VERIFIED", f"{build_id} is {entry.status.value}")
        if self._ready_build != expected_current:
            return False
        self._ready_build = build_id
        entry.status = BuildStatus.READY
        entry.evidence_refs = tuple(evidence_refs)
        entry.published_at_ns = now_ns
        self.ready_pointer_writes += 1
        return True

    # -- lab operations ---------------------------------------------------

    @property
    def disk_free_bytes(self) -> int:
        return self._disk_free_bytes

    def set_disk_free_bytes(self, value: int) -> None:
        self._disk_free_bytes = max(0, value)

    def backup(self) -> dict[str, Any]:
        """A deep snapshot. Taking one is not proof it can be restored."""
        return {
            "raw_rows": list(self.raw_rows),
            "canonical": copy.deepcopy(self._canonical),
            "bars": copy.deepcopy(self._bars),
            "catalog": copy.deepcopy(self._catalog),
            "ready_build": self._ready_build,
        }

    def restore(self, snapshot: Mapping[str, Any]) -> None:
        """Restore a snapshot into this server, replacing everything.

        ``INV-18``: a backup counts as viable only after a restore rehearsal
        whose restored identities and bars match the oracle. Calling this is the
        rehearsal; the comparison is the proof.
        """
        self.raw_rows = list(snapshot["raw_rows"])
        self._canonical = copy.deepcopy(snapshot["canonical"])
        self._bars = copy.deepcopy(snapshot["bars"])
        self._catalog = copy.deepcopy(snapshot["catalog"])
        self._ready_build = snapshot["ready_build"]

    def drop_everything(self) -> None:
        """Lose the volume. Models disaster, not a delete API."""
        self.raw_rows = []
        self._canonical = {}
        self._bars = {}
        self._catalog = {}
        self._ready_build = None

    def describe(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "raw_rows": len(self.raw_rows),
            "insert_calls": self.insert_calls,
            "disk_free_bytes": self._disk_free_bytes,
            "ready_build": self._ready_build,
            "ready_pointer_writes": self.ready_pointer_writes,
            "builds": {
                build_id: {
                    "status": entry.status.value,
                    "expected_identity_count": entry.expected_identity_count,
                    "canonical_rows": len(self._canonical.get(build_id, {})),
                    "bar_rows": len(self._bars.get(build_id, {})),
                    "published_at_ns": entry.published_at_ns,
                }
                for build_id, entry in sorted(self._catalog.items())
            },
        }
