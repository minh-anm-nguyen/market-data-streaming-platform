"""Replay (E2E-10, cases S14, C03, C07, F15).

The contract is one sentence: replay changes timing, never payload. So the
assertions are mostly byte equality plus "the speed knob moved the clock and
nothing else".
"""

from __future__ import annotations

import json

import pytest

from market_sim.engine import run_simulation
from market_sim.errors import ReplayError
from market_sim.recorder import COMPLETED_NAME, EVENTS_NAME
from market_sim.replay import open_replay_source, replay_dataset
from market_sim.verify import Verdict, verify_run

pytestmark = pytest.mark.asyncio


async def make_source(make_config, count=400, rate=20_000):
    cfg = make_config(mode="record", count=count, rate=rate)
    result = await run_simulation(cfg)
    assert result.ok
    return result


class TestSourceValidation:
    async def test_a_committed_run_directory_is_accepted(self, make_config):
        result = await make_source(make_config)
        source = open_replay_source(result.run_dir)
        assert source.lines == 400
        assert source.dataset_id == result.report["dataset_id"]
        assert source.sha256 == result.report["file"]["sha256"]

    async def test_an_events_file_with_its_marker_is_accepted(self, make_config):
        result = await make_source(make_config)
        assert open_replay_source(result.run_dir / EVENTS_NAME).lines == 400

    async def test_a_run_without_a_marker_is_refused(self, make_config):
        result = await make_source(make_config)
        (result.run_dir / COMPLETED_NAME).unlink()
        with pytest.raises(ReplayError) as exc:
            open_replay_source(result.run_dir)
        assert exc.value.code == "REPLAY_SOURCE_NOT_COMPLETE"

    async def test_an_uncommitted_run_can_be_replayed_with_an_explicit_opt_in(
        self, make_config
    ):
        result = await make_source(make_config)
        (result.run_dir / COMPLETED_NAME).unlink()
        source = open_replay_source(result.run_dir, require_completed=False)
        assert source.lines == 400

    async def test_a_corrupt_record_is_refused_with_its_line_number(self, make_config):
        # Case C07: a strict parser, and an error that says where.
        result = await make_source(make_config)
        path = result.run_dir / EVENTS_NAME
        lines = path.read_bytes().splitlines(keepends=True)
        lines[9] = b"{not json}\n"
        path.write_bytes(b"".join(lines))
        with pytest.raises(ReplayError) as exc:
            open_replay_source(result.run_dir, require_completed=False)
        assert exc.value.code == "REPLAY_RECORD_INVALID"
        assert exc.value.context["line"] == 10

    async def test_a_torn_final_line_is_refused(self, make_config):
        result = await make_source(make_config)
        path = result.run_dir / EVENTS_NAME
        path.write_bytes(path.read_bytes() + b'{"ask_ticks":1,"bid')
        with pytest.raises(ReplayError) as exc:
            open_replay_source(result.run_dir, require_completed=False)
        assert exc.value.code == "REPLAY_TORN_FINAL_LINE"

    async def test_a_missing_input_is_refused(self, tmp_path):
        with pytest.raises(ReplayError) as exc:
            open_replay_source(tmp_path / "nope.jsonl")
        assert exc.value.code == "REPLAY_INPUT_NOT_FOUND"

    async def test_an_unsupported_schema_version_is_refused(self, make_config):
        # Case C03: never guess at fields for a version we do not implement.
        result = await make_source(make_config)
        path = result.run_dir / EVENTS_NAME
        path.write_bytes(path.read_bytes().replace(b'"sim.v1"', b'"sim.v9"'))
        with pytest.raises(ReplayError) as exc:
            open_replay_source(result.run_dir, require_completed=False)
        assert exc.value.code == "REPLAY_RECORD_INVALID"
        assert exc.value.context["reason"] == "unsupported_schema_version"

    async def test_mixed_dataset_ids_are_refused(self, make_config):
        result = await make_source(make_config)
        path = result.run_dir / EVENTS_NAME
        lines = path.read_bytes().splitlines(keepends=True)
        lines[5] = lines[5].replace(b"ds-", b"dx-")
        path.write_bytes(b"".join(lines))
        with pytest.raises(ReplayError) as exc:
            open_replay_source(result.run_dir, require_completed=False)
        assert exc.value.code == "REPLAY_MIXED_DATASETS"


class TestE2E10SpeedDoesNotChangeContent:
    async def test_speed_1_and_speed_10_produce_identical_files(self, make_config):
        source_run = await make_source(make_config, count=300, rate=10_000)
        cfg = make_config(mode="replay", count=300)

        slow = await replay_dataset(source_run.run_dir, cfg, speed=1.0)
        fast = await replay_dataset(source_run.run_dir, cfg, speed=10.0)
        assert slow.ok and fast.ok

        original = (source_run.run_dir / EVENTS_NAME).read_bytes()
        assert (slow.run_dir / EVENTS_NAME).read_bytes() == original
        assert (fast.run_dir / EVENTS_NAME).read_bytes() == original

    async def test_a_faster_replay_really_is_faster(self, make_config):
        # The knob has to do something, or the byte-equality test above would be
        # vacuous. The logical span has to be long enough to dominate the fixed
        # per-run overhead (directory creation, hashing, commit): 1000 records at
        # 1000/s spans a full second, so pacing is the signal rather than noise.
        source_run = await make_source(make_config, count=1_000, rate=1_000)
        cfg = make_config(mode="replay", count=1_000)
        slow = await replay_dataset(source_run.run_dir, cfg, speed=1.0)
        fast = await replay_dataset(source_run.run_dir, cfg, speed=20.0)

        logical_span_s = 1_000 / 1_000
        assert slow.report["duration_s"] > logical_span_s * 0.8, (
            "speed=1.0 should take about as long as the recorded span"
        )
        assert fast.report["duration_s"] < slow.report["duration_s"] / 2

    async def test_unpaced_replay_is_also_byte_identical(self, make_config):
        source_run = await make_source(make_config, count=300, rate=10_000)
        cfg = make_config(mode="replay", count=300)
        result = await replay_dataset(source_run.run_dir, cfg, speed=0.0)
        assert result.ok
        assert (result.run_dir / EVENTS_NAME).read_bytes() == (
            source_run.run_dir / EVENTS_NAME
        ).read_bytes()

    async def test_event_times_and_sequences_are_untouched(self, make_config):
        source_run = await make_source(make_config, count=200)
        cfg = make_config(mode="replay", count=200)
        result = await replay_dataset(source_run.run_dir, cfg, speed=5.0)
        original = [
            json.loads(line)
            for line in (source_run.run_dir / EVENTS_NAME).read_text().splitlines()
        ]
        replayed = [
            json.loads(line)
            for line in (result.run_dir / EVENTS_NAME).read_text().splitlines()
        ]
        assert [(e["symbol"], e["sequence"], e["event_time_ns"]) for e in original] == [
            (e["symbol"], e["sequence"], e["event_time_ns"]) for e in replayed
        ]

    async def test_the_replayed_dataset_verifies(self, make_config):
        source_run = await make_source(make_config, count=200)
        cfg = make_config(mode="replay", count=200)
        result = await replay_dataset(source_run.run_dir, cfg, speed=0.0)
        verdict = verify_run(result.run_dir)
        assert verdict.verdict is Verdict.COMPLETE, verdict.reasons

    async def test_counts_reconcile_and_claim_nothing_extra(self, make_config):
        source_run = await make_source(make_config, count=200)
        cfg = make_config(mode="replay", count=200)
        result = await replay_dataset(source_run.run_dir, cfg, speed=0.0)
        counts = result.report["counts"]
        assert counts["source_lines"] == counts["emitted_events"] == 200
        assert counts["reconciles"] is True
        assert counts["intentional_omissions"] == 0

    async def test_the_report_records_the_source_identity(self, make_config):
        source_run = await make_source(make_config, count=100)
        cfg = make_config(mode="replay", count=100)
        result = await replay_dataset(source_run.run_dir, cfg, speed=0.0)
        source = result.report["source"]
        assert source["sha256"] == source_run.report["file"]["sha256"]
        assert source["dataset_id"] == source_run.report["dataset_id"]
        assert result.report["mode"] == "replay"
        assert any("accelerated replay" in note for note in result.report["known_limitations"])

    async def test_a_source_changed_mid_read_is_rejected(self, make_config):
        # Case F15: verify the hash before and after, and refuse a file that
        # moved under us rather than publish a half-replayed dataset.
        import asyncio

        source_run = await make_source(make_config, count=4_000, rate=4_000)
        cfg = make_config(mode="replay", count=4_000)
        path = source_run.run_dir / EVENTS_NAME
        original = path.read_bytes()

        async def tamper():
            # Well inside the replay window: 4000 records at 4000/s replayed at
            # speed 1.0 takes about a second.
            await asyncio.sleep(0.3)
            path.write_bytes(original + original[:200])

        task = asyncio.create_task(tamper())
        result = await replay_dataset(source_run.run_dir, cfg, speed=1.0)
        await task
        assert not result.ok
        assert result.error.code == "REPLAY_SOURCE_MUTATED"
        assert result.report["status"] == "FAILED"
