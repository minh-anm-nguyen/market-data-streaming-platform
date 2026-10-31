"""Extract the §15 what-if matrix and §16 E2E table from the system design doc.

The registry is *generated*, never hand-maintained: a hand-typed copy of 420
rows drifts from the document it claims to cover, and a coverage report built
on a drifted copy is worse than no report at all.

Run:
    python scripts/extract_matrix.py ../docs/PROJECT_06_*.md tests/registry
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

CASE_RE = re.compile(
    r"^\|\s*(P06-[A-Z]{1,2}-\d{2})\s*\|\s*(P[0-2])\s*\|(.+?)\|(.+?)\|(.+?)\|\s*$"
)
E2E_RE = re.compile(r"^\|\s*(E2E-\d{2})\s*—\s*(.+?)\s*\|(.+?)\|(.+?)\|(.+?)\|\s*$")
GROUP_RE = re.compile(r"^####\s+([A-Z]{1,2})\.\s+(.+?)\s+\(\d+ case\)\s*$")


def _clean(cell: str) -> str:
    return " ".join(cell.strip().split())


def extract(doc: str) -> tuple[list[dict], list[dict]]:
    cases: list[dict] = []
    e2es: list[dict] = []
    group_key = group_title = ""
    for line in doc.splitlines():
        if (m := GROUP_RE.match(line)) is not None:
            group_key, group_title = m.group(1), _clean(m.group(2))
            continue
        if (m := CASE_RE.match(line)) is not None:
            case_id = m.group(1)
            key = case_id.split("-")[1]
            cases.append(
                {
                    "case_id": case_id,
                    "group": key,
                    "group_title": group_title if key == group_key else "",
                    "priority": m.group(2),
                    "stimulus": _clean(m.group(3)),
                    "required_behaviour": _clean(m.group(4)),
                    "oracle": _clean(m.group(5)),
                }
            )
            continue
        if (m := E2E_RE.match(line)) is not None:
            e2es.append(
                {
                    "e2e_id": m.group(1),
                    "title": _clean(m.group(2)),
                    "input_fault": _clean(m.group(3)),
                    "pass_condition": _clean(m.group(4)),
                    "related_cases": [
                        f"P06-{t}" for t in _clean(m.group(5)).replace(",", " ").split() if re.fullmatch(r"[A-Z]{1,2}-\d{2}", t)
                    ],
                }
            )
    return cases, e2es


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    doc = Path(argv[1]).read_text(encoding="utf-8")
    out = Path(argv[2])
    out.mkdir(parents=True, exist_ok=True)
    cases, e2es = extract(doc)

    # Fill group titles for rows that appear before/after a heading hiccup.
    titles = {c["group"]: c["group_title"] for c in cases if c["group_title"]}
    for c in cases:
        c["group_title"] = titles.get(c["group"], c["group_title"])

    (out / "whatif_matrix.jsonl").write_text(
        "".join(json.dumps(c, ensure_ascii=False, sort_keys=True) + "\n" for c in cases),
        encoding="utf-8",
    )
    (out / "e2e_matrix.jsonl").write_text(
        "".join(json.dumps(e, ensure_ascii=False, sort_keys=True) + "\n" for e in e2es),
        encoding="utf-8",
    )
    print(f"cases={len(cases)} e2e={len(e2es)} groups={len(titles)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
