"""Tests for reading the real P05 landing table (P09 finding P09-M0-20).

The module's job is largely to refuse bad input, so most of these feed something
wrong and assert it raises. The one that matters most is the last: an expected
set derived from the rows being audited would make a missing row invisible, so
the ledger has to come from P03 and these tests have to show that it does.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from md_analytics.contracts import EVENT_QUOTE, EVENT_TICK
from md_analytics.raw_source import (
    RAW_COLUMNS,
    RAW_SELECT,
    RawSourceError,
    accepted_from_p03_handoff,
    corpus_from_raw,
    cut_from_p05_snapshot,
    load_raw_rows,
)

DATASET = "ds-test"


def _values(**overrides) -> list:
    """One raw-table row in the pinned column order."""
    row = {
        "dataset_id": DATASET,
        "symbol": "EURUSD",
        "sequence": 1,
        "event_kind": EVENT_TICK,
        "event_time_ns": 1793322000000000000,
        "event_date": "2026-10-31",
        "price_scale": 100000,
        "bid_ticks": None,
        "ask_ticks": None,
        "price_ticks": 108347,
        "source_schema_version": "sim.v1",
        "payload_digest": "a" * 64,
        "kafka_topic": "md.ticks.v1",
        "kafka_partition": 3,
        "kafka_offset": 17,
        "topic_generation": "md-v1-generation-a",
        "ingest_run_id": "run-1",
        # Milliseconds: toUnixTimestamp64Milli is what RAW_SELECT asks for, and
        # scale 3 is all the column stores. The first version of this fixture
        # held nanoseconds, which the server can neither store nor return.
        "ingested_at": 1793322000111,
    }
    row.update(overrides)
    return [row[name] for name in RAW_COLUMNS]


class FakeClient:
    """Stands in for the ClickHouse HTTP client; records the SQL it was given."""

    def __init__(self, rows, *, fail: Exception | None = None) -> None:
        self.rows = rows
        self.fail = fail
        self.sql: str | None = None
        self.params: dict | None = None

    def query_rows(self, sql, *, params=None):
        if self.fail is not None:
            raise self.fail
        self.sql = sql
        self.params = params
        return self.rows


class TypedFakeClient:
    """A fake that answers in ClickHouse's types rather than Python's.

    :class:`FakeClient` is handed a finished list of values, so it agrees with
    the reader by construction. That is what let this bug ship: the fixture
    gave ``ingested_at`` an ``int``, but the column is
    ``DateTime64(3, 'UTC')`` and the server returns ``'2026-10-31
    05:35:29.843'``. The reader called ``int()`` on it, and the pipeline
    refused the real table at stage 12 with 32 tests still green.

    This one reads the SELECT list out of the SQL and answers each expression
    the way the server would, so removing the conversion from
    :data:`RAW_SELECT` fails here instead of in a deployment.
    """

    #: What the server returns for each expression this module may ask for.
    RESPONSES = {
        "ingested_at": "2026-10-31 05:35:29.843",
        "toUnixTimestamp64Milli(ingested_at)": 1793322000111,
    }

    def __init__(self, row: dict | None = None) -> None:
        self.row = dict(row or {})
        self.sql: str | None = None
        self.params: dict | None = None

    def query_rows(self, sql, *, params=None):
        self.sql = sql
        self.params = params
        select = sql.split("SELECT ", 1)[1].split(" FROM ", 1)[0]
        base = dict(zip(RAW_COLUMNS, _values()))
        out = []
        for expr in (part.strip() for part in select.split(",")):
            if expr in self.RESPONSES:
                out.append(self.RESPONSES[expr])
            elif expr in base:
                out.append(self.row.get(expr, base[expr]))
            else:
                raise AssertionError(
                    "the reader asked for %r; this fake does not know what "
                    "ClickHouse would answer, so the test cannot be trusted"
                    % expr
                )
        return [out]


# --------------------------------------------------------------------------
# load_raw_rows
# --------------------------------------------------------------------------

def test_reads_rows_in_the_pinned_column_order():
    rows = load_raw_rows(FakeClient([_values()]), dataset_id=DATASET)
    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == "EURUSD"
    assert row.sequence == 1
    assert row.price_ticks == 108347
    assert row.bid_ticks is None
    assert row.event_date == date(2026, 10, 31)
    assert row.kafka_offset == 17
    # Carried as P05 stores it, not coerced (finding P09-M0-21).
    assert row.topic_generation == "md-v1-generation-a"


def test_selects_named_columns_and_scopes_to_one_dataset():
    client = FakeClient([_values()])
    load_raw_rows(client, dataset_id=DATASET)
    # SELECT * would let an upstream column addition shift every value.
    assert "SELECT *" not in client.sql
    for name in RAW_COLUMNS:
        assert name in client.sql
    assert client.params == {"dataset_id": DATASET}
    # Ordered by the transport coordinate, so a re-read is reproducible.
    assert "ORDER BY kafka_topic, kafka_partition, kafka_offset" in client.sql


def test_refuses_to_read_the_whole_table():
    with pytest.raises(RawSourceError, match="dataset_id is required"):
        load_raw_rows(FakeClient([_values()]), dataset_id="")


def test_a_changed_column_count_raises_rather_than_misparsing():
    with pytest.raises(RawSourceError, match="shape has changed"):
        load_raw_rows(FakeClient([_values()[:-1]]), dataset_id=DATASET)


def test_an_unknown_event_kind_raises():
    with pytest.raises(RawSourceError, match="unknown event_kind"):
        load_raw_rows(FakeClient([_values(event_kind="trade")]), dataset_id=DATASET)


@pytest.mark.parametrize(
    "overrides",
    [
        {"sequence": "not-a-number"},
        {"event_time_ns": None},
        {"event_date": "31/10/2026"},
        {"price_scale": True},
    ],
)
def test_malformed_scalars_raise(overrides):
    with pytest.raises(RawSourceError):
        load_raw_rows(FakeClient([_values(**overrides)]), dataset_id=DATASET)


def test_a_driver_failure_is_reported_not_swallowed():
    client = FakeClient([], fail=RuntimeError("connection reset"))
    with pytest.raises(RawSourceError, match="connection reset"):
        load_raw_rows(client, dataset_id=DATASET)


def test_a_quote_row_keeps_both_sides_and_no_price():
    client = FakeClient(
        [
            _values(
                event_kind=EVENT_QUOTE,
                bid_ticks=108346,
                ask_ticks=108348,
                price_ticks=None,
                kafka_topic="md.quotes.v1",
            )
        ]
    )
    row = load_raw_rows(client, dataset_id=DATASET)[0]
    assert row.event_kind == EVENT_QUOTE
    assert (row.bid_ticks, row.ask_ticks) == (108346, 108348)
    assert row.price_ticks is None


# --------------------------------------------------------------------------
# accepted_from_p03_handoff
# --------------------------------------------------------------------------

def _handoff_line(sequence: int, reason: str = "none", symbol: str = "EURUSD") -> str:
    return json.dumps(
        {
            "key": symbol,
            "value_hex": "0a01",
            "value_bytes": 2,
            "quality": {
                "flags": [],
                "feed_state": "HEALTHY",
                "primary_reason": reason,
                "arrival_index": sequence,
                # The fields P03's append_core really writes.
                "event_id": {
                    "dataset_id": DATASET,
                    "symbol": symbol,
                    "sequence": sequence,
                    "event_time_ns": 1793322000000000000 + sequence,
                    "price_scale": 100000,
                    "source_schema_version": "sim.v1",
                    "kind": EVENT_TICK,
                    "price_ticks": 108347,
                },
            },
        }
    )


def test_reads_the_accepted_ledger(tmp_path):
    path = tmp_path / "handoff.ndjson"
    path.write_text(
        "\n".join(_handoff_line(n) for n in (1, 2, 3)) + "\n", encoding="utf-8"
    )
    accepted = accepted_from_p03_handoff(path)
    assert [a.identity.sequence for a in accepted] == [1, 2, 3]
    assert all(a.identity.dataset_id == DATASET for a in accepted)


def test_a_gap_reason_is_still_an_accepted_forward(tmp_path):
    # P03's ADR-07: a discontinuity is a property of the series, so the event
    # was forwarded and must be expected in raw.
    path = tmp_path / "handoff.ndjson"
    path.write_text(_handoff_line(5, reason="SEQUENCE_GAP") + "\n", encoding="utf-8")
    assert len(accepted_from_p03_handoff(path)) == 1


def test_a_suppressed_reason_is_not_expected_in_raw(tmp_path):
    # Demanding a suppressed event appear in raw would manufacture a
    # missing-row report (P06 cases A-04, F-03).
    path = tmp_path / "handoff.ndjson"
    path.write_text(
        _handoff_line(1) + "\n" + _handoff_line(2, reason="DUPLICATE_EXACT") + "\n",
        encoding="utf-8",
    )
    assert [a.identity.sequence for a in accepted_from_p03_handoff(path)] == [1]


def test_an_empty_ledger_is_refused_rather_than_passing_trivially(tmp_path):
    path = tmp_path / "handoff.ndjson"
    path.write_text("", encoding="utf-8")
    with pytest.raises(RawSourceError, match="empty expectation"):
        accepted_from_p03_handoff(path)


def test_a_ledger_line_without_an_identity_raises(tmp_path):
    path = tmp_path / "handoff.ndjson"
    path.write_text(
        json.dumps({"key": "EURUSD", "quality": {"primary_reason": "none"}}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RawSourceError, match="event_id"):
        accepted_from_p03_handoff(path)


def test_a_malformed_ledger_line_raises(tmp_path):
    path = tmp_path / "handoff.ndjson"
    path.write_text("not json\n", encoding="utf-8")
    with pytest.raises(RawSourceError, match="not JSON"):
        accepted_from_p03_handoff(path)


def test_a_missing_ledger_raises(tmp_path):
    with pytest.raises(RawSourceError, match="cannot read"):
        accepted_from_p03_handoff(tmp_path / "absent.ndjson")


# --------------------------------------------------------------------------
# cut_from_p05_snapshot
# --------------------------------------------------------------------------

def _cut_file(tmp_path, **overrides):
    payload = {
        "run_id": "r1",
        "topic_generation": "md-v1-generation-a",
        # A cut may only be sealed from a terminal P05 state.
        "p05_state": "DRAINED",
        "seal_reason": "p05-snapshot",
        "captured_at_ns": 1793322000999000000,
        "cut": {
            "md.ticks.v1:0": {"log_start": 0, "end": 250},
            "md.quotes.v1:0": {"log_start": 0, "end": 900},
        },
    }
    payload.update(overrides)
    path = tmp_path / "cut.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_reads_the_cut_per_topic_partition(tmp_path):
    cut = cut_from_p05_snapshot(
        _cut_file(tmp_path), dataset_id=DATASET, manifest_sha256="b" * 64
    )
    assert cut.dataset_id == DATASET
    assert len(cut.cuts) == 2
    by_topic = {c.topic_partition.topic: c for c in cut.cuts}
    assert by_topic["md.ticks.v1"].end_offset == 250
    assert by_topic["md.quotes.v1"].end_offset == 900
    assert (
        by_topic["md.ticks.v1"].topic_partition.topic_generation
        == "md-v1-generation-a"
    )


def test_a_committed_frontier_below_the_end_does_not_cover_the_cut(tmp_path):
    path = _cut_file(tmp_path, committed={"md.ticks.v1:0": 100})
    cut = cut_from_p05_snapshot(path, dataset_id=DATASET, manifest_sha256="b" * 64)
    ticks = next(c for c in cut.cuts if c.topic_partition.topic == "md.ticks.v1")
    assert ticks.consumer_frontier == 100
    assert ticks.covers_cut() is False


def test_an_empty_cut_is_refused(tmp_path):
    path = _cut_file(tmp_path, cut={})
    with pytest.raises(RawSourceError, match="no topic-partition entries"):
        cut_from_p05_snapshot(path, dataset_id=DATASET, manifest_sha256="b" * 64)


@pytest.mark.parametrize("state", ["RUNNING", "", "INCOMPLETE", "DRAINING"])
def test_a_cut_from_a_non_terminal_p05_state_is_refused(tmp_path, state):
    """A cut taken while the consumer was still moving bounds nothing."""
    path = _cut_file(tmp_path, p05_state=state)
    with pytest.raises(RawSourceError, match="not terminal"):
        cut_from_p05_snapshot(path, dataset_id=DATASET, manifest_sha256="b" * 64)


def test_a_malformed_partition_key_is_refused(tmp_path):
    path = _cut_file(tmp_path, cut={"md.ticks.v1": {"end": 1}})
    with pytest.raises(RawSourceError, match="malformed topic-partition key"):
        cut_from_p05_snapshot(path, dataset_id=DATASET, manifest_sha256="b" * 64)


# --------------------------------------------------------------------------
# corpus_from_raw
# --------------------------------------------------------------------------

def _corpus(tmp_path, rows):
    ledger = tmp_path / "handoff.ndjson"
    ledger.write_text(_handoff_line(1) + "\n", encoding="utf-8")
    return corpus_from_raw(
        dataset_id=DATASET,
        raw_rows=rows,
        accepted=accepted_from_p03_handoff(ledger),
        cut=cut_from_p05_snapshot(
            _cut_file(tmp_path), dataset_id=DATASET, manifest_sha256="b" * 64
        ),
    )


def test_assembles_a_corpus_whose_rows_came_from_the_table(tmp_path):
    rows = load_raw_rows(FakeClient([_values()]), dataset_id=DATASET)
    corpus = _corpus(tmp_path, rows)
    assert corpus.dataset_id == DATASET
    assert corpus.raw_rows == rows
    # The expected set is P03's, not a recomputation of the audited rows.
    assert corpus.manifest.generator_version == "p05-raw-table"
    assert len(corpus.manifest.accepted) == 1


def test_an_empty_read_is_refused(tmp_path):
    with pytest.raises(RawSourceError, match="no raw rows"):
        _corpus(tmp_path, [])


def test_rows_from_another_dataset_are_refused(tmp_path):
    client = FakeClient([_values(), _values(dataset_id="ds-someone-else")])
    rows = load_raw_rows(client, dataset_id=DATASET)
    with pytest.raises(RawSourceError, match="leaked into the read"):
        _corpus(tmp_path, rows)


def test_the_expected_set_is_independent_of_the_rows(tmp_path):
    """The property that makes the audit able to fail at all.

    The ledger says sequence 1 was accepted. If the raw table does not contain
    it, the corpus still expects it -- which is how a missing row becomes
    visible instead of agreeing with itself.
    """
    rows = load_raw_rows(FakeClient([_values(sequence=99)]), dataset_id=DATASET)
    corpus = _corpus(tmp_path, rows)
    expected = {a.identity.sequence for a in corpus.manifest.accepted}
    present = {row.sequence for row in corpus.raw_rows}
    assert expected == {1}
    assert present == {99}
    assert expected - present == {1}


# --------------------------------------------------------------------------
# sqlbank: a semicolon inside a comment must not become a statement
# --------------------------------------------------------------------------

def test_a_semicolon_inside_a_comment_does_not_become_a_statement():
    """The bug this prevents reached a real ClickHouse and got a syntax error.

    002_canonical_ticks.sql cites "006_audit_queries.sql; this DDL only makes
    the proof cheap to run." Splitting on ';' before stripping comments cut that
    line in half and sent the prose tail to the server:

        Code: 62. DB::Exception: Syntax error: failed at position 1 (this)

    The model backend never parsed SQL, so no existing test could catch it.
    """
    from md_analytics.sqlbank import _split_statements

    text = (
        "-- a comment citing other_file.sql; and continuing after the semicolon.\n"
        "CREATE TABLE IF NOT EXISTS t (a Int64) ENGINE = MergeTree ORDER BY a;\n"
    )
    statements = _split_statements(text)
    assert len(statements) == 1, statements
    assert statements[0].startswith("CREATE TABLE")
    assert "continuing after the semicolon" not in statements[0]


def test_every_shipped_ddl_file_splits_into_sql_only():
    """No shipped DDL file may yield a fragment that is not a statement."""
    import pathlib

    from md_analytics.sqlbank import _split_statements

    keywords = (
        "CREATE", "ALTER", "DROP", "INSERT", "SELECT", "SET", "OPTIMIZE",
        "SYSTEM", "TRUNCATE", "WITH",
    )
    sql_dir = pathlib.Path(__file__).resolve().parents[1] / "sql"
    checked = 0
    for path in sorted(sql_dir.glob("0*.sql")):
        for statement in _split_statements(path.read_text(encoding="utf-8")):
            checked += 1
            head = statement.lstrip().upper()
            assert head.startswith(keywords), "%s -> %r" % (path.name, statement[:80])
    assert checked > 0


# --------------------------------------------------------------------------
# DateTime64 conversion (pipeline stage 12, run 20261031T091246Z)
# --------------------------------------------------------------------------

def test_the_server_converts_ingested_at_rather_than_python():
    """The conversion belongs where the column's type and timezone are known."""
    assert "toUnixTimestamp64Milli(ingested_at)" in RAW_SELECT
    assert "ingested_at" not in RAW_SELECT
    # Milli, not Nano: widening scale 3 would fabricate precision.
    assert not any("Nano" in expr or "Micro" in expr for expr in RAW_SELECT)
    # Every other column is still read as itself, in the pinned order.
    assert len(RAW_SELECT) == len(RAW_COLUMNS)
    for name, expr in zip(RAW_COLUMNS, RAW_SELECT):
        assert name in expr


def test_reads_a_row_from_a_server_typed_fake():
    client = TypedFakeClient()
    rows = load_raw_rows(client, dataset_id=DATASET)
    assert len(rows) == 1
    assert rows[0].ingested_at == 1793322000111
    assert rows[0].symbol == "EURUSD"


def test_datetime64_text_is_refused_rather_than_silently_zeroed():
    """The failure mode this fix removes, kept as a statement of what went wrong.

    If the server-side conversion is ever dropped, the reader sees text. It must
    refuse: a ``try/except`` that fell back to ``0`` would make an unreadable
    ingest clock look like the epoch.
    """
    with pytest.raises(RawSourceError, match="ingested_at is not an integer"):
        load_raw_rows(
            FakeClient([_values(ingested_at="2026-10-31 05:35:29.843")]),
            dataset_id=DATASET,
        )
