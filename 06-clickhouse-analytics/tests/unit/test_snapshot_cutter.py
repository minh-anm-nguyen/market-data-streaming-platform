"""Sealing a finite source snapshot (group F, A-12)."""

from __future__ import annotations

import pytest

from _coverage import covers
from md_analytics.contracts import SourceIdentity
from md_analytics.errors import SnapshotCutError
from md_analytics.fixtures import GOLDEN_BASE_NS, CorpusBuilder
from md_analytics.rows import TopicPartition
from md_analytics.snapshot_cutter import (
    AcceptedEvent,
    SourceManifest,
    TopicPartitionCut,
    seal_snapshot,
)

BASE = GOLDEN_BASE_NS


def manifest(dataset_id="DS-1", n=3) -> SourceManifest:
    accepted = tuple(
        AcceptedEvent(
            identity=SourceIdentity(dataset_id, "EURUSD", i + 1),
            event_kind="tick",
            payload_digest=f"{i:064d}",
        )
        for i in range(n)
    )
    draft = SourceManifest(dataset_id=dataset_id, accepted=accepted)
    return SourceManifest(
        dataset_id=dataset_id,
        accepted=accepted,
        declared_sha256=draft.content_digest(),
        generator_version=draft.generator_version,
    )


def cut(**overrides) -> TopicPartitionCut:
    kwargs = dict(
        topic_partition=TopicPartition("md.ticks.v1", 0, 1),
        end_offset=100,
        consumer_frontier=100,
        earliest_available_offset=0,
    )
    kwargs.update(overrides)
    return TopicPartitionCut(**kwargs)


def seal(**overrides):
    kwargs = dict(
        manifest=manifest(),
        cuts=[cut()],
        p05_state="DRAINED",
        seal_reason="durable_sink_frontier",
        captured_at_ns=BASE,
    )
    kwargs.update(overrides)
    return seal_snapshot(**kwargs)


class TestSealReasons:
    @covers("P06-F-01")
    def test_consumer_lag_zero_is_explicitly_not_a_cut(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(seal_reason="consumer_lag_zero")
        assert exc.value.code == "SEAL_REASON_REJECTED"
        assert "durably acknowledged" in str(exc.value)

    @covers("P06-F-01")
    @pytest.mark.parametrize("reason", ["row_count_match", "wall_clock", "operator_judgement"])
    def test_other_tempting_reasons_are_rejected_by_name(self, reason):
        with pytest.raises(SnapshotCutError) as exc:
            seal(seal_reason=reason)
        assert exc.value.code == "SEAL_REASON_REJECTED"

    def test_an_unknown_reason_is_distinguished_from_a_rejected_one(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(seal_reason="because_i_said_so")
        assert exc.value.code == "SEAL_REASON_UNKNOWN"

    def test_the_durable_frontier_is_accepted(self):
        assert seal().seal_reason == "durable_sink_frontier"


class TestFrontier:
    @covers("P06-F-01", "P06-A-12")
    def test_a_frontier_behind_the_cut_blocks_sealing(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(cuts=[cut(consumer_frontier=99)])
        assert exc.value.code == "CUT_FRONTIER_BEHIND"

    @covers("P06-F-05")
    def test_a_frontier_ahead_of_the_cut_is_fine(self):
        assert seal(cuts=[cut(consumer_frontier=120)]) is not None

    @covers("P06-F-07")
    def test_an_unbounded_stream_cannot_be_sealed(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(cuts=[])
        assert exc.value.code == "CUT_EMPTY"
        assert "unbounded live stream" in str(exc.value)


class TestTopologyAndGenerations:
    @covers("P06-F-11", "P06-E-11")
    def test_a_missing_topic_generation_blocks_sealing(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(cuts=[cut(topic_partition=TopicPartition("md.ticks.v1", 0, 0))])
        assert exc.value.code == "CUT_MISSING_GENERATION"

    @covers("P06-F-04", "P06-F-13")
    def test_each_topic_partition_carries_its_own_bound(self):
        cuts = [
            cut(
                topic_partition=TopicPartition("md.ticks.v1", p, 1),
                end_offset=10 * p + 1,
                consumer_frontier=10 * p + 1,
            )
            for p in range(8)
        ]
        sealed = seal(cuts=cuts)
        assert len(sealed.cuts) == 8
        assert sealed.offset_bound(TopicPartition("md.ticks.v1", 3, 1)) == 31

    @covers("P06-A-14", "P06-F-13")
    def test_a_declared_topology_catches_a_partition_absent_from_the_cut(self):
        expected = [TopicPartition("md.ticks.v1", p, 1) for p in range(8)]
        cuts = [cut(topic_partition=tp) for tp in expected[:7]]
        with pytest.raises(SnapshotCutError) as exc:
            seal(cuts=cuts, expected_topic_partitions=expected)
        assert exc.value.code == "CUT_TOPOLOGY_MISMATCH"
        assert exc.value.context["missing"] == ["md.ticks.v1/7@gen1"]

    @covers("P06-A-14")
    def test_eight_partitions_per_topic_is_sixteen_in_total(self):
        expected = [
            TopicPartition(topic, p, 1)
            for topic in ("md.ticks.v1", "md.quotes.v1")
            for p in range(8)
        ]
        sealed = seal(
            cuts=[cut(topic_partition=tp) for tp in expected],
            expected_topic_partitions=expected,
        )
        assert len(sealed.cuts) == 16, "8 per topic, not 8 in total"

    def test_a_duplicate_topic_partition_is_rejected(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(cuts=[cut(), cut()])
        assert exc.value.code == "CUT_DUPLICATE_TP"

    @covers("P06-A-03")
    def test_the_cut_digest_changes_with_the_topology(self):
        one = seal(cuts=[cut()])
        two = seal(
            cuts=[
                cut(end_offset=50, consumer_frontier=50),
                cut(
                    topic_partition=TopicPartition("md.ticks.v1", 1, 1),
                    end_offset=50,
                    consumer_frontier=50,
                ),
            ]
        )
        assert one.cut_digest() != two.cut_digest()


class TestRetentionAndState:
    @covers("P06-F-02")
    def test_retention_past_the_cut_makes_completeness_unprovable(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(cuts=[cut(earliest_available_offset=5)])
        assert exc.value.code == "CUT_RETENTION_LOST"
        assert "unprovable" in str(exc.value)

    @covers("P06-F-06", "P06-A-12")
    @pytest.mark.parametrize("state", ["CONSUMING", "REBALANCING", "CRASHED", ""])
    def test_a_non_terminal_p05_state_blocks_sealing(self, state):
        with pytest.raises(SnapshotCutError) as exc:
            seal(p05_state=state)
        assert exc.value.code == "P05_NOT_TERMINAL"

    def test_stopped_at_offsets_is_terminal(self):
        assert seal(p05_state="STOPPED_AT_OFFSETS") is not None

    def test_negative_offsets_are_rejected(self):
        with pytest.raises(SnapshotCutError) as exc:
            seal(cuts=[cut(end_offset=-1, consumer_frontier=-1)])
        assert exc.value.code == "CUT_NEGATIVE_OFFSET"


class TestManifestIntegrity:
    @covers("P06-F-14")
    def test_a_tampered_manifest_blocks_sealing(self):
        good = manifest()
        tampered = SourceManifest(
            dataset_id=good.dataset_id,
            accepted=good.accepted[:-1],  # one event removed, declared SHA left alone
            rejected=good.rejected,
            declared_sha256=good.declared_sha256,
            generator_version=good.generator_version,
        )
        with pytest.raises(SnapshotCutError) as exc:
            seal(manifest=tampered)
        assert exc.value.code == "MANIFEST_TAMPERED"

    @covers("P06-A-10", "P06-A-06")
    def test_a_manifest_without_a_declared_sha_is_inconclusive_not_trusted(self):
        bare = SourceManifest(dataset_id="DS-1", accepted=manifest().accepted)
        with pytest.raises(SnapshotCutError) as exc:
            seal(manifest=bare)
        assert exc.value.code == "MANIFEST_SHA_MISSING"
        assert "INCONCLUSIVE" in str(exc.value)

    @covers("P06-G-06")
    def test_the_manifest_digest_covers_the_generator_version(self):
        a = manifest()
        b = SourceManifest(
            dataset_id=a.dataset_id,
            accepted=a.accepted,
            rejected=a.rejected,
            generator_version="fixtures.2",
        )
        assert a.content_digest() != b.content_digest()


class TestSealedCorpus:
    @covers("P06-F-01")
    def test_the_fixture_builder_can_produce_a_lagging_frontier(self):
        b = CorpusBuilder("DS-LAG")
        b.tick("EURUSD", 1, BASE, 100)
        b.tick("EURUSD", 2, BASE + 1, 101)
        with pytest.raises(SnapshotCutError) as exc:
            b.seal(frontier_offset_by=1)
        assert exc.value.code == "CUT_FRONTIER_BEHIND"

    def test_a_sealed_cut_serialises_every_coordinate(self):
        b = CorpusBuilder("DS-SER")
        b.tick("EURUSD", 1, BASE, 100)
        b.quote("EURUSD", 2, BASE + 1, 99, 101)
        payload = b.seal().cut.as_dict()
        assert {c["topic"] for c in payload["cuts"]} == {"md.ticks.v1", "md.quotes.v1"}
        assert all(c["topic_generation"] == 1 for c in payload["cuts"])
        assert payload["seal_reason"] == "durable_sink_frontier"
        assert len(payload["cut_digest"]) == 64
