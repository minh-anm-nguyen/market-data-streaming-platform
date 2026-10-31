#!/usr/bin/env python3
"""Scores a fault_observe run against the fault plan that produced it.

`fault_observe` mode forwards every business-valid event with its quality
flags into a test-only sink. That is only evidence if the anomalies the
adapter reported are the anomalies the source deliberately injected. This
tool compares the two, and is strict in both directions:

  * an injected fault the adapter did not flag is a **miss**;
  * a flag the adapter raised that the plan does not contain is an
    **unexpected fault** -- a failure of the oracle, not a bonus.

Neither may be silently absorbed, because "we saw more anomalies than
expected" is exactly how a detector with a false-positive problem looks
successful.

Fault plan format:

```json
{
  "dataset_id": "ds-fault",
  "faults": [
    {"symbol": "EURUSD", "sequence": 42, "kind": "GAP"},
    {"symbol": "EURUSD", "sequence": 77, "kind": "DUPLICATE_EXACT"}
  ]
}
```

Usage:
    python3 tools/fault_sink.py --plan plan.json --events out/adapter.ndjson
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import sys

# Quality flags that correspond to a deliberately injectable anomaly. Flags
# outside this set (DEGRADED, FAULT_TEST_ONLY, ...) are consequences rather
# than injections and are not scored.
SCORED_FLAGS = {
    "GAP",
    "DUPLICATE_EXACT",
    "DUPLICATE_CONFLICT",
    "REORDER_LATE",
    "TOO_OLD_UNVERIFIABLE",
    "EVENT_TIME_OLD",
    "SEQUENCE_ROLLBACK",
    "INITIAL_GAP",
}


def load_events(path: pathlib.Path) -> list:
    out = []
    for line_no, raw in enumerate(path.read_text().splitlines(), 1):
        if not raw.strip():
            continue
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{path}:{line_no}: not valid NDJSON ({exc})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True, type=pathlib.Path)
    ap.add_argument("--events", required=True, type=pathlib.Path)
    ap.add_argument("--json", action="store_true",
                    help="machine-readable output")
    args = ap.parse_args()

    plan = json.loads(args.plan.read_text())
    expected = {
        (f["symbol"], int(f["sequence"]), f["kind"])
        for f in plan.get("faults", [])
    }
    events = load_events(args.events)

    observed = set()
    flag_counts: collections.Counter = collections.Counter()
    for e in events:
        for flag in e.get("quality_flags", []):
            flag_counts[flag] += 1
            if flag in SCORED_FLAGS:
                observed.add((e["symbol"], int(e["sequence"]), flag))

    missed = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    matched = sorted(expected & observed)

    result = {
        "events": len(events),
        "expected_faults": len(expected),
        "matched": len(matched),
        "missed": [list(x) for x in missed],
        "unexpected": [list(x) for x in unexpected],
        "flag_counts": dict(flag_counts),
        # A detector is credited only when both directions are clean.
        "verdict": "PASS" if not missed and not unexpected else "FAIL",
    }

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"events scored:  {result['events']}")
        print(f"faults in plan: {result['expected_faults']}")
        print(f"matched:        {result['matched']}")
        print(f"missed:         {len(missed)}")
        for m in missed:
            print(f"  - not detected: {m[0]} seq {m[1]} {m[2]}")
        print(f"unexpected:     {len(unexpected)}")
        for u in unexpected:
            print(f"  - not in plan:  {u[0]} seq {u[1]} {u[2]}")
        print(f"verdict:        {result['verdict']}")

    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
