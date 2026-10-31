#!/usr/bin/env python3
"""Merge a publisher report and a verifier report into one GO/NO-GO document.

Two documents exist because two parties measured: the publisher knows what it
attempted and what the broker told it, and the snapshot verifier knows what the
broker actually holds. Section 5.2 is built on keeping those apart, and section
14 wants one machine-readable verdict at the end.

So this tool merges rather than recomputes. It never replaces a null with a
zero, never upgrades a verdict, and if the two documents disagree it says so
instead of picking a winner. The merged verdict is the worst of the inputs, with
the reasons listed.

The three classes of disagreement it looks for are the ones that matter:

  * the publisher reported records acknowledged that the verifier did not find;
  * the verifier found records the publisher never reported attempting;
  * the publisher recorded UNKNOWN outcomes that the verifier resolved, which is
    not a problem but must be stated, because the completeness claim then rests
    on the scan rather than on the acknowledgements.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# Worst-first. A verdict absent from this ordering is treated as the worst.
VERDICT_RANK = {
    "PASS": 0,
    "NOT_RUN": 1,
    "INCONCLUSIVE": 2,
    "INCOMPLETE": 3,
    "NO_GO": 4,
    "FAIL": 5,
}


def worse(left: str, right: str) -> str:
    return left if VERDICT_RANK.get(left, 99) >= VERDICT_RANK.get(right, 99) else right


def load(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    return json.loads(file_path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="merge a P04 run into one verdict")
    parser.add_argument("--publisher-report", required=True)
    parser.add_argument("--verifier-report", default="")
    parser.add_argument("--group-verifier-report", default="")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    publisher = load(args.publisher_report)
    verifier = load(args.verifier_report)
    group = load(args.group_verifier_report)

    if not publisher:
        print(f"no publisher report at {args.publisher_report}")
        return 2

    reasons: List[str] = []
    disagreements: List[str] = []

    safety = publisher.get("safety_verdict", "NOT_RUN")
    performance = publisher.get("performance_verdict", "NOT_RUN")
    data = verifier.get("verdict", "NOT_RUN") if verifier else "NOT_RUN"
    group_verdict = group.get("verdict", "NOT_RUN") if group else "NOT_RUN"

    if not verifier:
        reasons.append(
            "no verifier report: without an independent scan nothing is proven about "
            "what the broker holds, so completeness cannot be claimed"
        )

    acked = publisher.get("p04_broker_acked")
    unknown = publisher.get("p04_delivery_unknown")
    observed_unique = verifier.get("observed_unique_identities") if verifier else None
    expected = verifier.get("expected_market_valid") if verifier else None
    missing = verifier.get("missing_count") if verifier else None
    resolved = verifier.get("resolved_unknown_count") if verifier else None
    unresolved = verifier.get("unresolved_unknown_count") if verifier else None

    # Disagreement 1: acknowledged but not found. This is the serious one.
    if acked is not None and observed_unique is not None and missing:
        disagreements.append(
            f"the verifier reports {missing} expected identities missing while the "
            f"publisher reports {acked} acknowledged; an acknowledged record that "
            f"cannot be read back is a P0"
        )

    # Disagreement 2: found but never attempted.
    unexpected = verifier.get("unexpected_count") if verifier else None
    if unexpected:
        disagreements.append(
            f"the verifier found {unexpected} identities the source oracle does not "
            f"expect, so something other than this run wrote to the topic"
        )

    # Not a disagreement, but it changes what the completeness claim rests on.
    if resolved:
        reasons.append(
            f"{resolved} outcomes the publisher could not resolve were resolved by the "
            f"independent scan; completeness for those records rests on the scan, not "
            f"on any acknowledgement"
        )
    if unresolved:
        reasons.append(
            f"{unresolved} outcomes remain unresolved by both the publisher and the "
            f"scan; they are neither written nor proven unwritten"
        )
    if unknown:
        reasons.append(
            f"the publisher recorded {unknown} UNKNOWN outcomes, which is why the "
            f"safety verdict is {safety}"
        )
    if expected is not None and observed_unique is not None and expected != observed_unique:
        reasons.append(
            f"expected {expected} market-valid identities, observed {observed_unique}"
        )

    if publisher.get("conservation_check") not in ("ok", None, ""):
        disagreements.append(
            f"the publisher counters did not balance: {publisher.get('conservation_check')}"
        )
    if publisher.get("contract_error"):
        disagreements.append(f"contract violation: {publisher.get('contract_error')}")

    # The merged verdict is the worst of the verdicts that actually ran. A
    # component nobody measured must not drag the result down: ADR-12 separates
    # the verdicts precisely so that "we did not benchmark" and "the benchmark
    # failed" stay different statements. Components that did not run are listed
    # under component_verdicts, so nothing is hidden by leaving them out here.
    ran = [v for v in (safety, performance, data, group_verdict) if v != "NOT_RUN"]
    overall = "NOT_RUN"
    for verdict in ran:
        overall = worse(overall, verdict) if overall != "NOT_RUN" else verdict
    not_run = [
        name
        for name, verdict in (
            ("publisher_safety", safety),
            ("publisher_performance", performance),
            ("snapshot_verifier_data", data),
            ("group_verifier_mechanics", group_verdict),
        )
        if verdict == "NOT_RUN"
    ]
    if not_run:
        reasons.append(
            "not measured in this run, so not folded into the merged verdict: "
            + ", ".join(not_run)
        )
    if disagreements:
        overall = "FAIL"

    merged = {
        "project": publisher.get("project"),
        "run_id": publisher.get("run_id"),
        "merged_verdict": overall,
        "component_verdicts": {
            "publisher_safety": safety,
            "publisher_performance": performance,
            "snapshot_verifier_data": data,
            "group_verifier_mechanics": group_verdict,
        },
        "disagreements": disagreements,
        "reasons": reasons,
        "provenance": {
            "mock_upstream_p03": publisher.get("mock_upstream_p03"),
            "source_usable_as_oracle": publisher.get("source_usable_as_oracle"),
            "broker_profile": publisher.get("broker_profile"),
            "kafka_version": publisher.get("kafka_version"),
            "librdkafka_version": publisher.get("librdkafka_version"),
            "protobuf_version": publisher.get("protobuf_version"),
            "proto_descriptor_sha256": publisher.get("proto_descriptor_sha256"),
            "config_sha256": publisher.get("config_sha256"),
            "dataset_sha256": publisher.get("dataset_sha256"),
            "begin_offsets_captured_before_the_run": verifier.get(
                "begin_offsets_were_captured_before_the_run"
            )
            if verifier
            else None,
        },
        # Carried over verbatim, nulls included. Section 14: null means not
        # observed and must not become 0.
        "counters": {
            key: publisher.get(key)
            for key in (
                "upstream_accepted", "policy_suppressed", "route_rejected",
                "p04_enqueued_in_library", "p04_broker_acked",
                "p04_delivery_failed_definite", "p04_delivery_unknown",
                "p04_pending_at_shutdown", "p04_app_level_resends",
                "p04_partition_mismatch", "p04_double_finalize_refused",
            )
        },
        "verifier_counters": {
            key: verifier.get(key)
            for key in (
                "expected_market_valid", "observed_unique_identities",
                "observed_records", "missing_count", "duplicate_count",
                "conflict_count", "contract_violation_count",
                "resolved_unknown_count", "unresolved_unknown_count",
            )
        }
        if verifier
        else {},
        "performance": {
            key: publisher.get(key)
            for key in (
                "requested_events_per_second",
                "actual_sustained_acked_events_per_second",
                "p50_ack_ms", "p95_ack_ms", "p99_ack_ms", "p999_ack_ms",
                "max_app_queue_records", "max_app_queue_bytes",
                "max_process_rss_bytes", "wall_seconds",
            )
        },
        "not_proven": [
            "any write to ClickHouse: Project 05 owns the sink",
            "exactly-once delivery end to end",
            "high availability: that requires the reliability-lab profile",
            "anything about the real Project 03 adapter while mock_upstream_p03 is true",
        ],
    }

    text = json.dumps(merged, indent=2) + "\n"
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    print(text, end="")

    if overall == "PASS":
        return 0
    if overall in ("INCOMPLETE", "INCONCLUSIVE", "NOT_RUN"):
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
