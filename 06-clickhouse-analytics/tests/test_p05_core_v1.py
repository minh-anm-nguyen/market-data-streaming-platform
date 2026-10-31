"""The two ``payload_digest`` encodings, and the audit that has to tell them apart.

P05 fills ``md_ingest.market_events_raw.payload_digest`` with a SHA-256 over
its own length-prefixed binary layout. P06 hashes canonical JSON. Both
functions were called ``payload_digest``, both suites were green, and nothing
compared them until P06 read P05's live table: 85,520 digest mismatches and
2,000 "missing" identities for a dataset that was entirely present.

These tests are the comparison that was missing. The first pins P05's layout
against a digest taken off a live server; the rest show that recomputing under
the wrong encoding fails loudly in-process, which is what it never did before.
"""

from __future__ import annotations

import pytest

from md_analytics import p05_core_v1
from md_analytics.contracts import payload_digest as p06_json_digest
from md_analytics.errors import ContractError
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder
from md_analytics.raw_auditor import audit_raw
from md_analytics.rows import DIGEST_ENCODINGS, P06_JSON_V1
from md_analytics.snapshot_cutter import AcceptedEvent, SourceManifest

BASE = GOLDEN_BASE_NS
P05_CORE_V1 = p05_core_v1.ENCODING


def _as_p05(corpus):
    """The same corpus with every digest rewritten the way P05 would write it."""
    rows = [
        row._replace(payload_digest=p05_core_v1.payload_digest(row.semantic_core()))
        for row in corpus.raw_rows
    ]
    by_identity = {row.identity: row for row in rows}
    accepted = tuple(
        AcceptedEvent(
            identity=event.identity,
            event_kind=event.event_kind,
            payload_digest=by_identity[event.identity].payload_digest,
        )
        for event in corpus.manifest.accepted
    )
    manifest = SourceManifest(
        dataset_id=corpus.manifest.dataset_id,
        accepted=accepted,
        rejected=corpus.manifest.rejected,
        generator_version=corpus.manifest.generator_version,
    )
    return rows, manifest


def _corpus():
    b = CorpusBuilder("DS-ENC")
    b.tick("EURUSD", 1, BASE, 108347)
    b.quote("EURUSD", 2, BASE + 1_000_000, 108342, 108349)
    return b.seal()


# --------------------------------------------------------------------------
# The layout itself
# --------------------------------------------------------------------------

def test_matches_a_digest_read_off_a_live_server():
    """The known answer. Evidence, not a fixture -- see VERIFIED_VECTOR."""
    core, stored = p05_core_v1.VERIFIED_VECTOR
    assert p05_core_v1.payload_digest(core) == stored


def test_the_two_encodings_genuinely_disagree():
    """Stated as a test so nobody 'simplifies' one into the other."""
    core, _ = p05_core_v1.VERIFIED_VECTOR
    assert p06_json_digest(core) != p05_core_v1.payload_digest(core)


def test_the_domain_separator_is_part_of_the_digest():
    core, _ = p05_core_v1.VERIFIED_VECTOR
    encoded = p05_core_v1.canonical_bytes(core)
    assert encoded.startswith(b"P05-CORE-V1\x00")


def test_an_absent_optional_field_is_not_a_zero():
    """The presence flags exist so these three cannot collide."""
    quote = {
        "dataset_id": "ds-1",
        "symbol": "EURUSD",
        "sequence": 1,
        "event_kind": "quote",
        "event_time_ns": BASE,
        "price_scale": 100000,
        "source_schema_version": "sim.v1",
        "bid_ticks": 1,
        "ask_ticks": 2,
    }
    absent = p05_core_v1.payload_digest(quote)
    explicit_none = p05_core_v1.payload_digest({**quote, "price_ticks": None})
    zero = p05_core_v1.payload_digest({**quote, "price_ticks": 0})
    assert absent == explicit_none
    assert absent != zero


def test_a_tick_and_a_quote_never_collide():
    common = {
        "dataset_id": "ds-1",
        "symbol": "EURUSD",
        "sequence": 1,
        "event_time_ns": BASE,
        "price_scale": 100000,
        "source_schema_version": "sim.v1",
    }
    tick = p05_core_v1.payload_digest({**common, "event_kind": "tick", "price_ticks": 7})
    quote = p05_core_v1.payload_digest(
        {**common, "event_kind": "quote", "bid_ticks": 7, "ask_ticks": 7}
    )
    assert tick != quote


def test_an_unencodable_core_raises_rather_than_hashing_something_else():
    core, _ = p05_core_v1.VERIFIED_VECTOR
    with pytest.raises(ContractError):
        p05_core_v1.payload_digest({**core, "sequence": "2"})
    with pytest.raises(ContractError):
        p05_core_v1.payload_digest({**core, "event_kind": "trade"})
    with pytest.raises(ContractError):
        p05_core_v1.payload_digest({**core, "sequence": 2**63})


# --------------------------------------------------------------------------
# The audit has to use the writer's encoding
# --------------------------------------------------------------------------

def test_the_registry_holds_both_encodings():
    assert set(DIGEST_ENCODINGS) == {P06_JSON_V1, P05_CORE_V1}


def test_an_unknown_encoding_is_refused():
    row = _corpus().raw_rows[0]
    with pytest.raises(ContractError, match="unknown digest encoding"):
        row.recomputed_digest(encoding="P05-CORE-V2")


def test_p05_rows_audit_clean_under_p05_and_fail_under_p06():
    """The production failure, reproduced and then fixed, in one test.

    Rows carrying P05 digests audited as canonical JSON mismatch *every* row --
    that is the shape of the stage 12 failure. The only change between the two
    halves is which encoding the audit is told to use.
    """
    corpus = _corpus()
    rows, manifest = _as_p05(corpus)

    wrong = audit_raw(rows, manifest=manifest, cut=corpus.cut)
    assert wrong.counters()["raw_digest_mismatch_count"] == len(rows)
    assert wrong.counters()["raw_distinct_identities"] == 0

    right = audit_raw(
        rows, manifest=manifest, cut=corpus.cut, digest_encoding=P05_CORE_V1
    )
    assert right.counters()["raw_digest_mismatch_count"] == 0
    assert right.counters()["missing_ids"] == 0
    assert right.counters()["raw_distinct_identities"] == len(rows)


def test_a_corpus_from_the_raw_table_declares_p05s_encoding():
    """The encoding travels with the rows; nobody downstream has to guess."""
    from md_analytics.raw_source import corpus_from_raw

    corpus = _corpus()
    rows, manifest = _as_p05(corpus)
    built = corpus_from_raw(
        dataset_id=corpus.dataset_id,
        raw_rows=rows,
        accepted=manifest.accepted,
        cut=corpus.cut,
    )
    assert built.digest_encoding == P05_CORE_V1


def test_a_corpus_built_in_process_still_defaults_to_p06s_encoding():
    """Changing the raw path must not relabel P06's own fixtures."""
    assert _corpus().digest_encoding == P06_JSON_V1
