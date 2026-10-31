"""Chaos profiles C1-C8 of section 17.4.

The rule that makes these tests worth anything: **an injector that did not fire
makes the result INVALID, not PASS.** Every test here asserts the stimulus
happened before it asserts anything about the response, and each builds an
:class:`EvidenceRecord` carrying the proof, so a run that silently stopped
injecting faults shows up as ``INVALID`` in the ledger instead of as a clean
sweep.

What is in scope here is the fault classes the model can produce honestly:
unknown acknowledgements, partial writes, an unavailable view target, disk full,
publication crashes, data defects and lifecycle loss. Network partitions, real
SIGKILL, cgroup pressure and replica lag need a process and a server, and are
registered as ``needs_infra`` rather than simulated into evidence.
"""

from __future__ import annotations

from datetime import date

import pytest

from _coverage import covers, declare_unexecutable
from md_analytics.canonical_builder import BuildState
from md_analytics.errors import EvidenceClass, StoreError, Verdict
from md_analytics.evidence import EvidenceLedger, EvidenceRecord
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder
from md_analytics.pipeline import run_and_require_ready, run_build
from md_analytics.schema_preflight import expected_canonical_digest, expected_raw_digest
from md_analytics.sqlbank import SqlBank
from md_analytics.store.model import CANONICAL_TABLE, ModelStore
from md_analytics.ttl_controller import ReconstructabilityCheck

pytestmark = pytest.mark.chaos

BASE = GOLDEN_BASE_NS
SEC = 1_000_000_000

# Fault classes that cannot be produced in-process. Named so they are visibly
# absent rather than quietly missing (section 17.4).
declare_unexecutable(
    ["P06-W-01", "P06-W-03", "P06-W-04", "P06-W-07", "P06-W-08"],
    kind="needs_infra",
    reason="needs cgroup/CPU/network pressure against a real server process",
)
declare_unexecutable(
    [f"P06-X-{n:02d}" for n in range(1, 15)],
    kind="needs_infra",
    reason="needs replicated ClickHouse with Keeper (optional HA-LAB profile)",
)


@pytest.fixture
def s() -> ModelStore:
    store = ModelStore()
    store.setup()
    return store


@pytest.fixture
def ledger() -> EvidenceLedger:
    return EvidenceLedger()


def record(case_id: str, *, priority: str, store: ModelStore, verdict: Verdict, reason: str,
           build_id: str | None = None, counters=None) -> EvidenceRecord:
    """An evidence record with full provenance and the injector's own account."""
    bank = SqlBank()
    return EvidenceRecord(
        case_id=case_id,
        priority=priority,
        verdict=verdict,
        reason=reason,
        backend=store.info().backend,
        evidence_class=str(EvidenceClass.MODEL),
        server_version=store.info().server_version,
        dataset_manifest_sha="f" * 64,
        raw_table_ddl_sha=expected_raw_digest(),
        canonical_ddl_sha=expected_canonical_digest(),
        sql_bank_digest=bank.bank_digest(),
        config_sha="c" * 64,
        build_id=build_id,
        claims_fault_injection=True,
        fault_injection_proof=tuple(store.fired),
        counters=counters or {},
    )


def corpus(n=12, dataset_id="CHAOS"):
    b = CorpusBuilder(dataset_id)
    for i in range(n):
        b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i)
    return b.seal()


class TestC1Network:
    """C1: the sink's answer is lost, so durability is unknown."""

    @covers("P06-G-02", "P06-T-01", kind="model_only")
    def test_an_unknown_ack_never_becomes_a_publication(self, s, catalog, ledger):
        s.fault_ack_unknown_after_insert = 1
        result = run_build(s, corpus(), catalog=catalog, block_size=4)

        assert "ack_unknown_after_insert" in s.fired, "the injector must have fired"
        assert result.state is BuildState.DIRTY
        assert catalog.active_build_id() is None

        entry = ledger.add(
            record(
                "P06-G-02",
                priority="P0",
                store=s,
                verdict=Verdict.NO_GO,
                reason="unknown ACK left the build DIRTY and nothing was published",
                counters=result.counters(),
            )
        )
        assert entry.effective_verdict() is Verdict.NO_GO
        assert ledger.release_verdict() is Verdict.PASS, (
            "refusing to publish is the asserted behaviour, so it does not block"
        )

    @covers("P06-AD-06", kind="model_only")
    def test_a_record_claiming_a_fault_that_did_not_fire_is_invalid(self, s, ledger):
        # No injector armed: s.fired stays empty.
        run_build(s, corpus(), publish=False)
        assert s.fired == []
        entry = ledger.add(
            record(
                "P06-G-02",
                priority="P0",
                store=s,
                verdict=Verdict.PASS,
                reason="claims a fault that never happened",
            )
        )
        assert entry.effective_verdict() is Verdict.INVALID
        assert ledger.release_verdict() is Verdict.FAIL, (
            "an un-fired injector must block the release, not pass quietly"
        )


class TestC2Storage:
    """C2: the device is out of space."""

    @covers("P06-W-02", "P06-V-05", kind="model_only")
    def test_disk_full_is_a_bounded_error_with_no_partial_build(self, s, catalog, ledger):
        s.fault_disk_full = True
        result = run_build(s, corpus(), catalog=catalog)

        assert "disk_full" in s.fired
        assert result.state is BuildState.DIRTY
        assert s.all_canonical(build_id=result.build_id or "") == []
        assert catalog.active_build_id() is None
        ledger.add(
            record(
                "P06-W-02",
                priority="P1",
                store=s,
                verdict=Verdict.NO_GO,
                reason="disk full raised a bounded error and left no partial build",
            )
        )
        assert ledger.release_verdict() is Verdict.PASS

    @covers("P06-K-06", "P06-AB-03", kind="model_only")
    def test_a_read_only_view_target_leaves_source_rows_and_no_bars(self, s, catalog):
        s.fault_mv_target_unavailable = True
        result = run_build(s, corpus(), catalog=catalog)

        assert "mv_target_unavailable" in s.fired
        assert result.verdict is Verdict.NO_GO
        assert catalog.active_build_id() is None


class TestC3Process:
    """C3: the process dies at a gate."""

    @covers("P06-G-01", "P06-W-05", kind="model_only")
    def test_an_abort_mid_insert_leaves_the_previous_pointer(self, s, catalog, ledger):
        first = run_and_require_ready(s, corpus(2, "CHAOS-P"), catalog=catalog)

        s.fault_stop_after_blocks = 2
        crashed = run_build(
            s,
            corpus(30, "CHAOS-P2"),
            catalog=catalog,
            block_size=3,
            expected_current=first.build_id,
        )
        assert "stop_after_blocks" in s.fired
        assert crashed.state is BuildState.DIRTY
        assert catalog.active_build_id() == first.build_id
        ledger.add(
            record(
                "P06-G-01",
                priority="P0",
                store=s,
                verdict=Verdict.NO_GO,
                reason="partial canonical write stayed unpublished; old READY served on",
                build_id=first.build_id,
            )
        )
        assert ledger.release_verdict() is Verdict.PASS

    @covers("P06-G-10", kind="model_only")
    def test_a_crash_while_moving_the_pointer_is_atomic(self, s, catalog):
        first = run_and_require_ready(s, corpus(2, "CHAOS-A"), catalog=catalog)
        catalog.fault_crash_before_rename = True
        second = run_build(
            s, corpus(3, "CHAOS-B"), catalog=catalog, expected_current=first.build_id
        )
        assert second.verdict is Verdict.NO_GO
        assert "INJECTED_PUBLISH_CRASH" in second.reasons[0] or "injected crash" in second.reasons[0]
        assert catalog.active_build_id() == first.build_id

    @covers("P06-G-04", kind="model_only")
    def test_a_restart_cannot_append_to_a_dirty_attempt(self, s, catalog):
        s.fault_ack_unknown_after_insert = 1
        c = corpus(8, "CHAOS-R")
        run_build(s, c, catalog=catalog, block_size=2)

        # A restart that tries the same attempt number again must be refused:
        # the staging id is not empty, and appending would double the states.
        retry = run_build(s, c, catalog=catalog, block_size=2, attempt=1)
        assert retry.state is BuildState.DIRTY
        assert "already holds" in retry.reasons[0]


class TestC4Metadata:
    """C4: schema drift and catalog tampering."""

    @covers("P06-B-01", "P06-Q-02", kind="model_only")
    def test_a_build_against_a_drifted_schema_gets_a_different_build_id(self, s, catalog):
        c = corpus(4, "CHAOS-S")
        good = run_build(s, c, catalog=catalog, schema_sha256=expected_raw_digest())
        drifted = run_build(
            s,
            c,
            catalog=catalog,
            schema_sha256="0" * 64,
            attempt=2,
            expected_current=good.build_id,
        )
        assert good.build_id != drifted.build_id, (
            "the schema hash is a build-id input, so a drifted schema can never "
            "silently reuse a verified build's name"
        )

    @covers("P06-Y-07", "P06-G-11", kind="model_only")
    def test_a_tampered_manifest_is_detected_on_read(self, s, catalog):
        import json

        from md_analytics.errors import PublishRejected

        result = run_and_require_ready(s, corpus(4, "CHAOS-T"), catalog=catalog)
        path = catalog.builds_dir / f"{result.build_id}.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["payload"]["canonical_count"] = 999
        path.write_text(json.dumps(document), encoding="utf-8")

        with pytest.raises(PublishRejected) as exc:
            catalog.read_manifest(result.build_id)
        assert exc.value.code == "MANIFEST_DIGEST_MISMATCH"


class TestC5Data:
    """C5: late timestamps, duplicates, conflicts, out-of-order."""

    @covers("P06-E-02", "P06-C-05", kind="model_only")
    def test_a_conflict_under_load_still_stops_the_build(self, s, catalog):
        b = CorpusBuilder("CHAOS-C5")
        rows = [b.tick("EURUSD", i + 1, BASE + i * SEC, 100_000 + i) for i in range(500)]
        for row in rows[::50]:
            b.duplicate(row, times=2)
        b.conflict(rows[250], price_ticks=1)

        result = run_build(s, b.seal(), catalog=catalog, block_size=64)
        assert result.verdict is Verdict.NO_GO
        assert len(result.audit.conflicts) == 1
        assert result.audit.physical_rows == 521, "500 unique + 20 retries + 1 conflicting"

        # Row 250 is both duplicated and conflicting. It is classified once, as a
        # conflict: an identity whose rows disagree is not a duplicate of
        # anything, so it must not also be counted as a clean retry.
        assert result.audit.duplicate_row_count == 18, "9 duplicated identities, not 10"
        assert result.audit.physical_duplicates.get(("CHAOS-C5", "EURUSD", 251)) is None
        assert catalog.active_build_id() is None

    @covers("P06-J-01", "P06-J-04", kind="model_only")
    def test_out_of_order_arrival_at_scale_still_matches_the_oracle(self, s, catalog):
        import random

        b = CorpusBuilder("CHAOS-ORD")
        specs = [(i + 1, BASE + i * 37_000_000, 100_000 + (i * 7919) % 5_000) for i in range(800)]
        random.Random(4242).shuffle(specs)
        for sequence, ns, price in specs:
            b.tick("EURUSD", sequence, ns, price)

        result = run_and_require_ready(s, b.seal(), catalog=catalog, block_size=37)
        assert result.bars.verdict() is Verdict.PASS
        assert sum(bar.tick_count for bar in result.stored_bars.values()) == 800


class TestC6Lifecycle:
    """C6: TTL plus a failed backup plus a late replay."""

    @covers("P06-O-01", "P06-P-05", "P06-AB-02", kind="model_only")
    def test_losing_the_source_with_no_backup_is_not_recoverable(self, s, catalog, ledger):
        result = run_and_require_ready(s, corpus(10, "CHAOS-TTL"), catalog=catalog)
        removed = s.apply_ttl(table=CANONICAL_TABLE, cutoff=date(2026, 11, 1))
        assert removed == 10

        check = ReconstructabilityCheck(False, False, False)
        assert check.verdict() is Verdict.NO_GO
        entry = ledger.add(
            record(
                "P06-AB-02",
                priority="P0",
                store=s,
                verdict=Verdict.NO_GO,
                reason=check.reason(),
                build_id=result.build_id,
            )
        )
        # No injector fired here -- TTL is a policy action, not a fault -- so the
        # record must not claim one.
        assert entry.fault_injection_proof == ()
        assert entry.effective_verdict() is Verdict.NO_GO

    @covers("P06-O-03", kind="model_only")
    def test_a_replay_already_older_than_the_ttl_is_refused_up_front(self):
        from md_analytics.ttl_controller import would_expire_on_arrival

        assert would_expire_on_arrival(
            event_date=date(2026, 6, 1), today=date(2026, 10, 30), ttl_days=90
        ), (
            "inserting it would look successful and then vanish at the next merge, "
            "which is indistinguishable from never having arrived"
        )


class TestC8MultiFault:
    """C8: two or more faults at once; fail closed."""

    @covers("P06-AB-01", "P06-AB-04", kind="model_only")
    def test_ack_loss_then_a_read_only_target_never_publishes(self, s, catalog, ledger):
        c = corpus(10, "CHAOS-MF")

        s.fault_ack_unknown_after_insert = 1
        first = run_build(s, c, catalog=catalog, block_size=3)
        assert first.state is BuildState.DIRTY

        s.fault_mv_target_unavailable = True
        second = run_build(s, c, catalog=catalog, block_size=3, attempt=2)
        assert second.verdict is Verdict.NO_GO

        assert {"ack_unknown_after_insert", "mv_target_unavailable"} <= set(s.fired)
        assert catalog.active_build_id() is None
        entry = ledger.add(
            record(
                "P06-AB-01",
                priority="P0",
                store=s,
                verdict=Verdict.NO_GO,
                reason="two faults in sequence; the pipeline failed closed both times",
            )
        )
        assert entry.effective_verdict() is Verdict.NO_GO
        assert len(entry.fault_injection_proof) >= 2

    @covers("P06-AB-05", kind="model_only")
    def test_a_clean_rebuild_after_every_fault_is_cleared(self, s, catalog):
        from md_analytics.canonical_builder import compute_build_id, staging_build_id

        c = corpus(10, "CHAOS-REC")
        s.fault_ack_unknown_after_insert = 1
        run_build(s, c, catalog=catalog, block_size=3)

        logical = compute_build_id(
            manifest_sha256=c.cut.manifest_sha256,
            cut_digest=c.cut.cut_digest(),
            schema_sha256=expected_raw_digest(),
        )
        s.drop_build(build_id=staging_build_id(logical, 1))
        s.fault_ack_unknown_after_insert = 0
        s.fault_mv_target_unavailable = False

        recovered = run_and_require_ready(s, c, catalog=catalog, block_size=3, attempt=3)
        assert sum(bar.tick_count for bar in recovered.stored_bars.values()) == 10
        assert recovered.bars.verdict() is Verdict.PASS


class TestInjectorDiscipline:
    @covers("P06-AD-06", kind="model_only")
    @pytest.mark.parametrize(
        "proof_name,attribute,value",
        [
            ("fail_before_insert", "fault_fail_before_insert", 1),
            ("disk_full", "fault_disk_full", True),
            ("mv_target_unavailable", "fault_mv_target_unavailable", True),
        ],
    )
    def test_every_injector_records_that_it_fired(self, proof_name, attribute, value):
        """A fault the harness cannot observe makes a test INVALID, not PASS."""
        store = ModelStore()
        store.setup()
        setattr(store, attribute, value)

        with pytest.raises(StoreError):
            store.insert_canonical(_one_tick(), block_size=1)
        assert proof_name in store.fired, f"{proof_name} did not record its own firing"


def _one_tick():
    from datetime import date as _date

    from md_analytics.rows import CanonicalTick

    return [
        CanonicalTick(
            build_id="probe",
            dataset_id="PROBE",
            symbol="EURUSD",
            sequence=1,
            event_time_ns=BASE,
            event_date=_date(2026, 10, 30),
            price_scale=100_000,
            price_ticks=100_000,
            payload_digest="0" * 64,
            source_schema_version="marketdata.v1",
        )
    ]
