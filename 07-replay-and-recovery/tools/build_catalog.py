"""Extract the what-if matrix and the E2E list from the design document.

The 448 cases exist in exactly one place -- section 10 of
``PROJECT_07_Deterministic_Replay_and_Failure_Recovery_System_Design.md`` -- and
transcribing them by hand into code would guarantee drift. This parses the
tables instead, so ``catalog/what_if_matrix.json`` is derived from the document
and a change there shows up as a change here.

It also refuses to pretend. The parser asserts the totals the design states
(448 cases, 32 groups, 128 P0, 40 E2E); a mismatch is an error, because a
catalog that silently lost 30 rows would understate the denominator in every
coverage number that follows.

Usage::

    python tools/build_catalog.py ../docs/PROJECT_07_*.md --out catalog/what_if_matrix.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

#: ``### G01. 01 -- Source manifest, dataset seal & provenance``
_GROUP = re.compile(r"^###\s+(?P<group>G\d{2})\.\s*\d+\s*[-—]+\s*(?P<title>.+?)\s*$")
#: ``| `P07-G01-01` | **P0** | what if | expected | evidence |``
_CASE = re.compile(
    r"^\|\s*`(?P<case_id>P07-G\d{2}-\d{2})`\s*\|\s*\**(?P<priority>P[012])\**\s*\|"
    r"\s*(?P<what_if>.*?)\s*\|\s*(?P<expected>.*?)\s*\|\s*(?P<evidence>.*?)\s*\|\s*$"
)
#: ``| `E2E-01` | fixture | fault | expected |``
_E2E = re.compile(
    r"^\|\s*`(?P<case>E2E-\d{2})`\s*\|\s*(?P<fixture>.*?)\s*\|\s*(?P<fault>.*?)\s*\|"
    r"\s*(?P<expected>.*?)\s*\|\s*$"
)

EXPECTED_CASES = 448
EXPECTED_GROUPS = 32
EXPECTED_P0 = 128
EXPECTED_E2E = 40


def parse(document: str) -> dict[str, Any]:
    """Parse the design document into a catalog dictionary."""
    cases: list[dict[str, Any]] = []
    e2e: list[dict[str, Any]] = []
    groups: dict[str, str] = {}
    current_group = ""

    for line in document.splitlines():
        group_match = _GROUP.match(line)
        if group_match:
            current_group = group_match.group("group")
            groups[current_group] = group_match.group("title")
            continue
        case_match = _CASE.match(line)
        if case_match:
            case_id = case_match.group("case_id")
            group = case_id.split("-")[1]
            cases.append(
                {
                    "case_id": case_id,
                    "group": group,
                    "group_title": groups.get(group, ""),
                    "priority": case_match.group("priority"),
                    "what_if": case_match.group("what_if"),
                    "expected_safe_behavior": case_match.group("expected"),
                    "independent_evidence": case_match.group("evidence"),
                }
            )
            continue
        e2e_match = _E2E.match(line)
        if e2e_match:
            e2e.append(
                {
                    "case": e2e_match.group("case"),
                    "fixture": e2e_match.group("fixture"),
                    "fault": e2e_match.group("fault"),
                    "expected_proof": e2e_match.group("expected"),
                }
            )

    seen = {case["case_id"] for case in cases}
    if len(seen) != len(cases):
        raise ValueError("duplicate case ids in the matrix")

    return {
        "source_document": "PROJECT_07_Deterministic_Replay_and_Failure_Recovery_System_Design.md",
        "totals": {
            "cases": len(cases),
            "groups": len(groups),
            "p0": sum(1 for case in cases if case["priority"] == "P0"),
            "p1": sum(1 for case in cases if case["priority"] == "P1"),
            "p2": sum(1 for case in cases if case["priority"] == "P2"),
            "e2e": len(e2e),
        },
        "groups": [{"group": key, "title": value} for key, value in sorted(groups.items())],
        "cases": cases,
        "e2e": e2e,
        "note": (
            "This inventory is the set of cases the design enumerates. It is NOT a set of "
            "results: a case with no execution is NOT_RUN."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("design_doc")
    parser.add_argument("--out", default="catalog/what_if_matrix.json")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="skip the total assertions (for parsing a draft document)",
    )
    args = parser.parse_args(argv)

    text = Path(args.design_doc).read_text(encoding="utf-8")
    catalog = parse(text)
    totals = catalog["totals"]

    if not args.allow_partial:
        problems = []
        for name, expected in (
            ("cases", EXPECTED_CASES),
            ("groups", EXPECTED_GROUPS),
            ("p0", EXPECTED_P0),
            ("e2e", EXPECTED_E2E),
        ):
            if totals[name] != expected:
                problems.append(f"{name}: parsed {totals[name]}, design states {expected}")
        if problems:
            print("catalog does not match the design totals:", file=sys.stderr)
            for problem in problems:
                print(f"  - {problem}", file=sys.stderr)
            return 1

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(catalog, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(totals, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
