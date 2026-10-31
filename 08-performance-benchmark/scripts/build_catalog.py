"""Generate ``specs/test-catalog.csv`` from the PROJECT_08 design document.

The 512 what-if cases and 48 end-to-end tests are defined in markdown tables in
``docs/PROJECT_08_*.md``. Retyping them into a CSV would create a second source
of truth that drifts, so the catalog is *derived* from the document and the
derivation is re-runnable:

    python scripts/build_catalog.py --doc ../docs/PROJECT_08_...md

The script asserts the inventory the document states in section 17 -- 32 groups
of 16 cases, 242 P0, 206 P1, 64 P2, 512 total -- and 48 E2E tests across four
sections. A mismatch means either the document changed or the parser is wrong,
and either way the catalog must not be written.

Every row lands with ``status=NOT_RUN``. Section 17 is explicit that this is the
state of all 512 cases, and nothing in this repository is allowed to upgrade a
status without evidence.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Any

#: ``### G01 — Dataset manifest, provenance và run control (16 cases)``
GROUP_HEADING = re.compile(r"^###\s+(G\d{2})\s+[—-]\s+(.+?)\s+\((\d+)\s+cases\)\s*$")

#: ``### 18.1. E2E-A — Measurement foundation và normal pipeline (12 tests)``
E2E_HEADING = re.compile(r"^###\s+18\.\d+\.\s+(E2E-[A-D])\s+[—-]\s+(.+?)\s+\((\d+)\s+tests\)\s*$")

#: ``| `P08-G01-01` | P0 | what if | stimulus | expected |``
WHATIF_ROW = re.compile(
    r"^\|\s*`(P08-G\d{2}-\d{2})`\s*\|\s*(P[012])\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*$"
)

#: ``| E2E-01 | setup | oracle |``
E2E_ROW = re.compile(r"^\|\s*(E2E-\d{2})\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*$")

FIELDNAMES = (
    "id",
    "kind",
    "group",
    "group_title",
    "priority",
    "what_if",
    "stimulus",
    "expected_evidence",
    "status",
)

#: The inventory section 17 declares. Checked, not assumed.
EXPECTED_WHATIF_TOTAL = 512
EXPECTED_GROUPS = 32
EXPECTED_CASES_PER_GROUP = 16
EXPECTED_PRIORITY_COUNTS = {"P0": 242, "P1": 206, "P2": 64}
EXPECTED_E2E_TOTAL = 48
EXPECTED_E2E_SECTIONS = 4


def parse(doc: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    group = group_title = ""
    e2e_section = e2e_title = ""
    groups_seen: list[str] = []
    e2e_sections_seen: list[str] = []

    for line in doc.splitlines():
        heading = GROUP_HEADING.match(line)
        if heading:
            group, group_title, declared = heading.group(1), heading.group(2), int(heading.group(3))
            if declared != EXPECTED_CASES_PER_GROUP:
                raise SystemExit(f"{group} declares {declared} cases, expected {EXPECTED_CASES_PER_GROUP}")
            groups_seen.append(group)
            continue

        heading = E2E_HEADING.match(line)
        if heading:
            e2e_section, e2e_title = heading.group(1), heading.group(2)
            e2e_sections_seen.append(e2e_section)
            continue

        match = WHATIF_ROW.match(line)
        if match:
            case_id, priority, what_if, stimulus, expected = match.groups()
            if not case_id.startswith(f"P08-{group}-"):
                raise SystemExit(f"{case_id} appeared under heading {group!r}")
            rows.append(
                {
                    "id": case_id,
                    "kind": "WHAT_IF",
                    "group": group,
                    "group_title": group_title,
                    "priority": priority,
                    "what_if": what_if,
                    "stimulus": stimulus,
                    "expected_evidence": expected,
                    "status": "NOT_RUN",
                }
            )
            continue

        match = E2E_ROW.match(line)
        if match and e2e_section:
            case_id, setup, oracle = match.groups()
            rows.append(
                {
                    "id": case_id,
                    "kind": "E2E",
                    "group": e2e_section,
                    "group_title": e2e_title,
                    # Section 18 does not assign P-levels per test; inventing one
                    # here would put a priority in the catalog that the document
                    # never set.
                    "priority": "UNSET",
                    "what_if": "",
                    "stimulus": setup,
                    "expected_evidence": oracle,
                    "status": "NOT_RUN",
                }
            )

    _check(rows, groups_seen, e2e_sections_seen)
    return rows


def _check(rows: list[dict[str, Any]], groups: list[str], e2e_sections: list[str]) -> None:
    what_if = [r for r in rows if r["kind"] == "WHAT_IF"]
    e2e = [r for r in rows if r["kind"] == "E2E"]

    problems: list[str] = []
    if len(groups) != EXPECTED_GROUPS:
        problems.append(f"{len(groups)} group headings, expected {EXPECTED_GROUPS}")
    if len(what_if) != EXPECTED_WHATIF_TOTAL:
        problems.append(f"{len(what_if)} what-if rows, expected {EXPECTED_WHATIF_TOTAL}")
    if len(e2e) != EXPECTED_E2E_TOTAL:
        problems.append(f"{len(e2e)} E2E rows, expected {EXPECTED_E2E_TOTAL}")
    if len(e2e_sections) != EXPECTED_E2E_SECTIONS:
        problems.append(f"{len(e2e_sections)} E2E sections, expected {EXPECTED_E2E_SECTIONS}")

    counts: dict[str, int] = {}
    for row in what_if:
        counts[row["priority"]] = counts.get(row["priority"], 0) + 1
    if counts != EXPECTED_PRIORITY_COUNTS:
        problems.append(f"priority counts {counts}, expected {EXPECTED_PRIORITY_COUNTS}")

    per_group: dict[str, int] = {}
    for row in what_if:
        per_group[row["group"]] = per_group.get(row["group"], 0) + 1
    uneven = {g: n for g, n in per_group.items() if n != EXPECTED_CASES_PER_GROUP}
    if uneven:
        problems.append(f"groups without {EXPECTED_CASES_PER_GROUP} cases: {uneven}")

    ids = [row["id"] for row in rows]
    if len(set(ids)) != len(ids):
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        problems.append(f"duplicate ids: {duplicates}")

    if problems:
        raise SystemExit("catalog does not match the document inventory:\n  - " + "\n  - ".join(problems))


def main(argv: list[str] | None = None) -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--doc",
        type=Path,
        default=here.parents[1] / "docs" / "PROJECT_08_High_Throughput_Benchmark_and_Optimization_System_Design.md",
    )
    parser.add_argument("--out", type=Path, default=here.parent / "specs" / "test-catalog.csv")
    args = parser.parse_args(argv)

    if not args.doc.is_file():
        raise SystemExit(f"design document not found: {args.doc}")
    rows = parse(args.doc.read_text(encoding="utf-8"))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {len(rows)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
