"""Named what-if scenarios, each with an explicit PASS oracle.

Spec 9.1 fixes the order a scenario must run in::

    preflight -> baseline snapshot -> arm fault -> verify the fault occurred
    -> run workload -> capture cut -> wait for terminal status
    -> independent reconcile -> resource check -> verdict

The fourth step is what makes the rest mean anything. A scenario whose fault
never fired is reported ``NOT_RUN``, never ``PASS`` -- so each scenario
declares how its fault is *confirmed*, and the runner refuses a verdict without
that confirmation.

Usage::

    python tools/run_scenarios.py --list
    python tools/run_scenarios.py --cases P05-C-01,P05-H-01
    python tools/run_scenarios.py --all --out artifacts/scenarios.json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from p05_ingest.canonical_digest import payload_digest  # noqa: E402
from p05_ingest.config import AppConfig  # noqa: E402
from p05_ingest.contract_validator import ContractValidator  # noqa: E402
from p05_ingest.metrics import REBALANCES  # noqa: E402
from p05_ingest.model import TopicPartition  # noqa: E402
from p05_ingest.reconciler import (  # noqa: E402
    EvidenceGaps,
    Verdict,
    observed_from_kafka_scan,
    observed_from_sink_rows,
    reconcile,
)
from p05_ingest.testkit import (  # noqa: E402
    TICKS_TOPIC,
    Behaviour,
    CommitFault,
    FakeClock,
    conflict_pair,
    make_tick,
)
from p05_ingest.testkit.workload import (  # noqa: E402
    manifest_as_expected,
    produce_market_workload,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEV_CONFIG = PROJECT_ROOT / "config" / "dev.toml"
TP0 = TopicPartition(TICKS_TOPIC, 0)

TICKS_ONLY = {"kafka": {"topics": ["md.ticks.v1"]}}


@dataclass
class ScenarioResult:
    case_id: str
    title: str
    verdict: str
    fault_confirmed: bool
    notes: list[str] = field(default_factory=list)
    reconciliation: dict | None = None

    def to_dict(self) -> dict:
        return {
            "case_id": self.case_id,
            "title": self.title,
            "verdict": self.verdict,
            "fault_confirmed": self.fault_confirmed,
            "notes": self.notes,
            "reconciliation": self.reconciliation,
        }


def _verdict(*, fault_confirmed: bool, passed: bool) -> str:
    """The three-way verdict. An unconfirmed fault can never be a PASS."""
    if not fault_confirmed:
        return "NOT_RUN"
    return "PASS" if passed else "FAIL"


def _harness(**kwargs):
    """Build the standard harness. Imported here to keep the CLI light."""
    from tests.conftest import build_harness, make_config

    base = AppConfig.from_toml(DEV_CONFIG)
    overrides = kwargs.pop("config_overrides", {})
    cfg = make_config(base, **overrides) if overrides else base
    return build_harness(cfg, clock=FakeClock(), **kwargs)


def _reconcile(harness, *, expected=None, gaps: EvidenceGaps | None = None):
    validator = ContractValidator(harness.cfg.contracts, harness.cfg.project)
    kafka_observed, refused = observed_from_kafka_scan(
        harness.cluster.scan(), validator=validator, mapper_digest=payload_digest
    )
    report = reconcile(
        expected=expected if expected is not None else kafka_observed,
        kafka_observed=kafka_observed,
        sink_observed=observed_from_sink_rows(harness.sink.rows),
        gaps=gaps,
    )
    return report, refused


def _produce_ticks(harness, count: int, *, start: int = 0) -> None:
    for sequence in range(start, start + count):
        harness.cluster.produce(
            TICKS_TOPIC,
            0,
            key=b"EURUSD",
            value=make_tick(sequence=sequence).SerializeToString(),
        )


def _committed(harness, tp: TopicPartition = TP0) -> int:
    return harness.cluster.committed(harness.cfg.kafka.group_id, tp) or 0


# --------------------------------------------------------------- scenarios


def scenario_a_01() -> ScenarioResult:
    """A-01: the clean handoff. No fault, so the oracle is exact equality."""
    harness = _harness(
        partitions=4, config_overrides={"batch": {"per_partition_max_rows": 100}}
    )
    manifest = produce_market_workload(
        harness.cluster, events=800, partitions=4, seed=101
    )
    status = harness.finish()
    report, refused = _reconcile(harness, expected=manifest_as_expected(manifest))

    notes = [status.reason]
    if refused:
        notes.append(f"records the verifier could not parse: {sorted(set(refused))}")
    return ScenarioResult(
        case_id="P05-A-01",
        title="Clean upstream handoff, no injected fault",
        # Nothing is armed, so there is nothing to confirm.
        verdict=_verdict(
            fault_confirmed=True, passed=status.complete and report.is_go
        ),
        fault_confirmed=True,
        notes=notes,
        reconciliation=report.to_dict(),
    )


def scenario_c_01() -> ScenarioResult:
    """C-01: commit response lost. The position must become UNKNOWN."""
    harness = _harness(
        partitions=1,
        config_overrides={**TICKS_ONLY, "batch": {"per_partition_max_rows": 4}},
    )
    _produce_ticks(harness, 20)

    harness.consumer.commit_fault = CommitFault.LOST_RESPONSE
    harness.consumer.commit_fault_remaining = 1

    fault_confirmed = False
    for _ in range(200):
        report = harness.engine.step()
        if report.commits_unknown:
            fault_confirmed = True
        if report.idle:
            if harness.engine.batcher.open_partitions:
                harness.tick_timer()
                continue
            break
    status = harness.engine.shutdown()
    recon, _ = _reconcile(harness)

    return ScenarioResult(
        case_id="P05-C-01",
        title="Kafka commit response lost; committed position is UNKNOWN",
        verdict=_verdict(
            fault_confirmed=fault_confirmed,
            passed=status.complete and recon.is_go,
        ),
        fault_confirmed=fault_confirmed,
        notes=[
            status.reason,
            "the broker applied the commit but the client was not told, so the "
            "engine retried rather than claiming success",
        ],
        reconciliation=recon.to_dict(),
    )


def scenario_g_01() -> ScenarioResult:
    """G-01: an identity conflict must end in NO-GO with no winner chosen."""
    harness = _harness(
        partitions=1,
        config_overrides={**TICKS_ONLY, "batch": {"per_partition_max_rows": 2}},
    )
    for entry in conflict_pair():
        harness.cluster.produce(TICKS_TOPIC, 0, key=entry.key, value=entry.value)
    harness.finish()
    recon, _ = _reconcile(harness)

    fault_confirmed = bool(recon.conflicts)
    return ScenarioResult(
        case_id="P05-G-01",
        title="Identity conflict detected and refused",
        verdict=_verdict(
            fault_confirmed=fault_confirmed,
            passed=recon.verdict is Verdict.NO_GO,
        ),
        fault_confirmed=fault_confirmed,
        notes=[
            "a conflict must be reported, never resolved by picking a row",
            f"conflicting identities: {len(recon.conflicts)}",
        ],
        reconciliation=recon.to_dict(),
    )


def scenario_h_01() -> ScenarioResult:
    """H-01: ClickHouse wrote the rows and lost the acknowledgement."""
    harness = _harness(
        partitions=1,
        sink_script=(Behaviour.WRITE_THEN_LOSE_ACK,),
        config_overrides={**TICKS_ONLY, "batch": {"per_partition_max_rows": 5}},
    )
    _produce_ticks(harness, 10)
    status = harness.finish()

    fault_confirmed = any(
        a.behaviour is Behaviour.WRITE_THEN_LOSE_ACK and a.applied
        for a in harness.sink.attempts
    )
    recon, _ = _reconcile(harness)
    return ScenarioResult(
        case_id="P05-H-01",
        title="ClickHouse ACK lost after the write landed",
        verdict=_verdict(
            fault_confirmed=fault_confirmed,
            passed=status.complete and recon.is_go,
        ),
        fault_confirmed=fault_confirmed,
        notes=[
            status.reason,
            f"insert attempts: {len(harness.sink.attempts)}; "
            f"physical rows: {harness.sink.row_count}",
        ],
        reconciliation=recon.to_dict(),
    )


def scenario_k_01() -> ScenarioResult:
    """K-01: a poison record stops its partition under fail-closed policy."""
    harness = _harness(
        partitions=1,
        config_overrides={**TICKS_ONLY, "batch": {"per_partition_max_rows": 1}},
    )
    _produce_ticks(harness, 5)
    poison_offset = harness.cluster.produce(
        TICKS_TOPIC, 0, key=b"EURUSD", value=b"\xff\xff\xff\xff"
    )
    _produce_ticks(harness, 5, start=5)

    status = harness.finish()
    committed = _committed(harness)

    fault_confirmed = bool(harness.engine.blocked_partitions)
    stopped_at_poison = committed == poison_offset
    no_invalid_rows = all(
        r["kafka_offset"] != poison_offset for r in harness.sink.rows
    )

    return ScenarioResult(
        case_id="P05-K-01",
        title="Poison record halts the partition; nothing is skipped",
        verdict=_verdict(
            fault_confirmed=fault_confirmed,
            passed=(
                not status.complete and stopped_at_poison and no_invalid_rows
            ),
        ),
        fault_confirmed=fault_confirmed,
        notes=[
            status.reason,
            f"poison at offset {poison_offset}; committed next offset {committed}",
        ],
    )


def scenario_v_01() -> ScenarioResult:
    """V-01: lost ACK during a rebalance, the combined case."""
    harness = _harness(
        partitions=1,
        sink_script=(Behaviour.WRITE_THEN_LOSE_ACK, Behaviour.ACK),
        dedup_window=0,
        config_overrides={
            **TICKS_ONLY,
            "batch": {"per_partition_max_rows": 4},
            "retry": {"max_attempts": 3, "overall_deadline_s": 10},
        },
    )
    _produce_ticks(harness, 12)

    saw_attempt = False
    for _ in range(60):
        harness.engine.step()
        if harness.sink.attempts:
            saw_attempt = True
            break
        harness.tick_timer()

    harness.consumer.lose([TP0])
    harness.engine.step()
    harness.consumer.assign([TP0])
    harness.drain()
    status = harness.engine.shutdown()

    recon, _ = _reconcile(harness)
    fault_confirmed = saw_attempt and harness.metrics.counter_total(REBALANCES) > 0
    return ScenarioResult(
        case_id="P05-V-01",
        title="Lost ClickHouse ACK during a rebalance",
        verdict=_verdict(
            fault_confirmed=fault_confirmed,
            passed=not recon.conflicts and not recon.missing_from_sink,
        ),
        fault_confirmed=fault_confirmed,
        notes=[
            status.reason,
            "the run is INCOMPLETE by design because ownership was lost; the "
            "data must still reconcile with no missing events and no conflicts",
            f"physical rows: {harness.sink.row_count}, "
            f"logical: {len(harness.logical_identities())}",
        ],
        reconciliation=recon.to_dict(),
    )


SCENARIOS: dict[str, Callable[[], ScenarioResult]] = {
    "P05-A-01": scenario_a_01,
    "P05-C-01": scenario_c_01,
    "P05-G-01": scenario_g_01,
    "P05-H-01": scenario_h_01,
    "P05-K-01": scenario_k_01,
    "P05-V-01": scenario_v_01,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default="", help="comma-separated case ids")
    parser.add_argument("--all", action="store_true", help="run every scenario")
    parser.add_argument("--list", action="store_true", help="list the case ids")
    parser.add_argument("--out", default=None, help="write a JSON report here")
    parser.add_argument(
        "--profile",
        default="simulated",
        choices=["simulated"],
        help=(
            "only 'simulated' is implemented. The reliability_lab and "
            "capacity_lab profiles need live infrastructure and are NOT_RUN "
            "until it exists"
        ),
    )
    args = parser.parse_args(argv)

    if args.list:
        for case_id, fn in sorted(SCENARIOS.items()):
            first_line = (fn.__doc__ or "").strip().splitlines()[0]
            print(f"{case_id}  {first_line}")
        return 0

    selected = (
        sorted(SCENARIOS)
        if args.all
        else [c.strip() for c in args.cases.split(",") if c.strip()]
    )
    if not selected:
        parser.error("pass --cases, --all or --list")

    unknown = [c for c in selected if c not in SCENARIOS]
    if unknown:
        parser.error(f"unknown case id(s): {', '.join(unknown)}")

    results = []
    for case_id in selected:
        result = SCENARIOS[case_id]()
        results.append(result)
        flag = "" if result.fault_confirmed else "  [fault NOT confirmed]"
        print(f"[{result.verdict:<8}] {result.case_id}  {result.title}{flag}")
        for note in result.notes:
            print(f"             {note}")

    failed = [r for r in results if r.verdict == "FAIL"]
    not_run = [r for r in results if r.verdict == "NOT_RUN"]

    print()
    print(
        f"{len(results)} scenario(s): "
        f"{len(results) - len(failed) - len(not_run)} PASS, "
        f"{len(failed)} FAIL, {len(not_run)} NOT_RUN"
    )
    if not_run:
        print(
            "NOT_RUN means the intended fault was not confirmed to have "
            "happened. Those cases prove nothing and must not be counted as "
            "passes (spec 9.1)."
        )

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(
                {
                    "profile": args.profile,
                    "scenarios": [r.to_dict() for r in results],
                    "summary": {
                        "total": len(results),
                        "fail": len(failed),
                        "not_run": len(not_run),
                    },
                },
                indent=2,
                sort_keys=True,
                default=str,
            ),
            encoding="utf-8",
        )
        print(f"report: {out}")

    return 1 if (failed or not_run) else 0


if __name__ == "__main__":
    sys.exit(main())
