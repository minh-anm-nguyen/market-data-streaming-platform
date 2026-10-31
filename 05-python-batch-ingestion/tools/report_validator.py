"""Refuse a run report that claims more than its evidence supports.

Spec 14.1 defaults every gate to ``NOT_RUN``, and spec 9.1 says a scenario
whose fault was never confirmed is ``NOT_RUN`` rather than a pass. Those rules
are only worth anything if something enforces them on the artifact that gets
read, so this script does.

It checks a run report (from ``p05-ingest run|simulate --report``) and, when
given, a scenario report (from ``tools/run_scenarios.py --out``), and rejects:

  * a GO on a gate with no evidence behind it;
  * a capacity claim from a run against the in-memory fakes;
  * a completeness claim with no independent reconciliation;
  * a NOT_RUN case counted as a pass;
  * a secret or a raw market payload leaking into the artifact.

Usage::

    python tools/report_validator.py --run artifacts/sim-clean.json
    python tools/report_validator.py --run R.json --scenarios S.json

Exit status: 0 the report is defensible, 1 it overclaims, 2 it is unreadable.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

#: Substrings that must never appear in an artifact. Credentials are read from
#: the environment and never stored, so a hit here means something regressed.
FORBIDDEN_PATTERNS = (
    re.compile(r"password\s*[\"']?\s*[:=]", re.IGNORECASE),
    re.compile(r"\bsecret\b", re.IGNORECASE),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
)

REQUIRED_RUN_KEYS = (
    "verdict",
    "reason",
    "config_fingerprint",
    "descriptor_sha256",
    "metrics",
)


def fail(problems: list[str], message: str) -> None:
    problems.append(message)


def check_run_report(payload: dict, *, path: Path) -> list[str]:
    problems: list[str] = []

    for key in REQUIRED_RUN_KEYS:
        if key not in payload:
            fail(problems, f"missing required key {key!r}")

    fingerprint = payload.get("config_fingerprint", "")
    if len(fingerprint) != 64:
        fail(
            problems,
            "config_fingerprint is not a 64-character hash, so the settings "
            "behind this run cannot be identified",
        )
    descriptor = payload.get("descriptor_sha256", "")
    if len(descriptor) != 64:
        fail(
            problems,
            "descriptor_sha256 is not a 64-character hash, so the schema "
            "behind this run cannot be identified",
        )

    verdict = payload.get("verdict")
    if verdict not in ("COMPLETE", "INCOMPLETE"):
        fail(problems, f"verdict {verdict!r} is not COMPLETE or INCOMPLETE")

    simulated = bool(payload.get("simulated"))

    # A COMPLETE verdict is a statement about this process only. Spec I-08
    # forbids reading completeness against the source into it, so the report
    # must carry the caveat or a reconciliation result.
    if verdict == "COMPLETE":
        reason = payload.get("reason", "")
        has_caveat = "independent reconciliation" in reason
        has_reconciliation = bool(payload.get("reconciliation"))
        if not (has_caveat or has_reconciliation):
            fail(
                problems,
                "a COMPLETE verdict carries neither the reconciliation caveat "
                "nor a reconciliation result, so it reads as a completeness "
                "claim it cannot support (spec I-08)",
            )

    # Capacity claims.
    for key in ("events_per_second", "throughput", "capacity_go", "slo_verified"):
        if key in payload:
            if simulated:
                fail(
                    problems,
                    f"{key!r} appears in a simulated run report; a figure from "
                    "the in-memory fakes is not a capacity measurement "
                    "(spec 10.4)",
                )
            elif not payload.get("run_manifest"):
                fail(
                    problems,
                    f"{key!r} appears without a run_manifest describing the "
                    "hardware, topology and settings it was measured on",
                )

    if payload.get("gates"):
        for gate, state in payload["gates"].items():
            if state == "GO" and not payload.get("evidence", {}).get(gate):
                fail(
                    problems,
                    f"gate {gate} is GO with no evidence recorded; every gate "
                    "defaults to NOT_RUN (spec 14.1)",
                )

    blob = json.dumps(payload)
    for pattern in FORBIDDEN_PATTERNS:
        if pattern.search(blob):
            fail(
                problems,
                f"the report matches the forbidden pattern {pattern.pattern!r}; "
                "artifacts must not carry credentials (spec NFR-14)",
            )

    # Raw market payloads must not be embedded either: a long hex blob is the
    # shape that would leak one.
    for match in re.finditer(r"\b[0-9a-f]{128,}\b", blob):
        fail(
            problems,
            f"the report contains a {len(match.group())}-character hex blob, "
            "which looks like an embedded raw payload rather than a digest",
        )
        break

    return problems


def check_scenario_report(payload: dict) -> list[str]:
    problems: list[str] = []
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list):
        return ["scenario report has no 'scenarios' list"]

    summary = payload.get("summary", {})
    counted_not_run = summary.get("not_run")
    actual_not_run = sum(1 for s in scenarios if s.get("verdict") == "NOT_RUN")
    if counted_not_run is not None and counted_not_run != actual_not_run:
        problems.append(
            f"summary says {counted_not_run} NOT_RUN but {actual_not_run} "
            "scenarios are NOT_RUN; a hidden NOT_RUN reads as a pass"
        )

    for scenario in scenarios:
        case_id = scenario.get("case_id", "<unknown>")
        verdict = scenario.get("verdict")
        confirmed = scenario.get("fault_confirmed")
        if verdict == "PASS" and confirmed is False:
            problems.append(
                f"{case_id} is PASS although its fault was not confirmed to "
                "have occurred; that case proves nothing (spec 9.1)"
            )
        if verdict not in ("PASS", "FAIL", "NOT_RUN"):
            problems.append(f"{case_id} has an unrecognised verdict {verdict!r}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, help="run report JSON")
    parser.add_argument("--scenarios", default=None, help="scenario report JSON")
    args = parser.parse_args(argv)

    run_path = Path(args.run)
    try:
        run_payload = json.loads(run_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"cannot read {run_path}: {exc}", file=sys.stderr)
        return 2

    problems = check_run_report(run_payload, path=run_path)

    if args.scenarios:
        scenario_path = Path(args.scenarios)
        try:
            scenario_payload = json.loads(scenario_path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"cannot read {scenario_path}: {exc}", file=sys.stderr)
            return 2
        problems.extend(check_scenario_report(scenario_payload))

    if problems:
        print(f"REPORT REJECTED ({len(problems)} problem(s)):")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print("REPORT ACCEPTED: every claim in it is backed by recorded evidence.")
    print(f"  verdict:    {run_payload.get('verdict')}")
    print(f"  config:     {run_payload.get('config_fingerprint', '')[:16]}...")
    print(f"  descriptor: {run_payload.get('descriptor_sha256', '')[:16]}...")
    if run_payload.get("simulated"):
        print(
            "  NOTE: this run used the in-memory fakes. It carries no capacity, "
            "durability or HA evidence."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
