#!/usr/bin/env python3
"""Independently re-checks an adapter run report.

The adapter writes its own verdict. This tool re-derives the parts of that
verdict that can be checked from the report alone, so a reviewer does not
have to take the binary's word for it:

  * the conservation equations really balance;
  * COMPLETE_OFFLINE is only claimed with a verified source;
  * nothing that was merely unknown has been reported as zero;
  * a committed artifact exists and matches the digest in the report.

Exit status: 0 when every check passes, 1 otherwise.

Usage:
    python3 tools/verify_report.py reports/offline.yaml
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import re
import sys


def parse_scalars(text: str) -> dict[str, str]:
    """Flat key -> value map. The report nests only one level and never
    repeats a leaf name across sections it matters for, so this stays
    honest; `want_int` guards against a missing key being read as zero."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$", line)
        if not m:
            continue
        key, value = m.group(1), m.group(2).strip()
        if value == "" or value.startswith(("-", "{", "[")) and value != "{}":
            # Section header or a list item; nothing scalar to record.
            if value == "":
                continue
        out.setdefault(key, value.strip('"'))
    return out


class Checker:
    def __init__(self, scalars: dict[str, str], report_path: pathlib.Path):
        self.s = scalars
        self.path = report_path
        self.failures: list[str] = []
        self.checks = 0

    def want_int(self, key: str):
        v = self.s.get(key)
        if v is None or v == "null":
            return None
        try:
            return int(v)
        except ValueError:
            return None

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.checks += 1
        if not ok:
            self.failures.append(f"{name}{': ' + detail if detail else ''}")

    def run(self) -> None:
        g = self.want_int

        # --- conservation ---------------------------------------------------
        pairs = [
            ("frames_complete == proto_decode_error + frames_decoded",
             g("frames_complete"),
             (g("proto_decode_error"), g("frames_decoded"))),
            ("frames_decoded == structural_reject + structural_ok",
             g("frames_decoded"), (g("structural_reject"), g("structural_ok"))),
            ("structural_ok == business_reject + business_ok",
             g("structural_ok"), (g("business_reject"), g("business_ok"))),
            ("business_ok == policy_suppressed + classified_output_attempted",
             g("business_ok"),
             (g("policy_suppressed"), g("classified_output_attempted"))),
            ("classified_output_attempted == output_success + "
             "output_failed_or_pending",
             g("classified_output_attempted"),
             (g("output_success"), g("output_failed_or_pending"))),
        ]
        for name, lhs, rhs in pairs:
            if lhs is None or any(r is None for r in rhs):
                self.check(name, False, "a counter is missing from the report")
                continue
            self.check(name, lhs == sum(rhs), f"{lhs} != {sum(rhs)}")

        self.check("the adapter also says conservation holds",
                   self.s.get("all_conservation_equations_hold") == "true",
                   str(self.s.get("all_conservation_equations_hold")))

        # --- completeness claims ---------------------------------------------
        status = self.s.get("status")
        provenance = self.s.get("source_completeness")
        if status == "COMPLETE_OFFLINE":
            self.check("COMPLETE_OFFLINE requires a verified source",
                       provenance == "VERIFIED_COMPLETE_FILE", str(provenance))
            self.check("COMPLETE_OFFLINE requires a clean terminal reason",
                       self.s.get("terminal_reason") == "none",
                       str(self.s.get("terminal_reason")))
        if provenance != "VERIFIED_COMPLETE_FILE":
            self.check("an unverified run must not claim zero loss",
                       self.s.get("no_silent_loss_when_full_source_verified")
                       in (None, "null", "false"),
                       str(self.s.get(
                           "no_silent_loss_when_full_source_verified")))
        if status == "PARTIAL_LIVE":
            self.check("a live run must not claim a verified source",
                       provenance in ("LIVE_UNVERIFIED",
                                      "LIVE_WITH_SOURCE_EVIDENCE"),
                       str(provenance))

        # --- things a single run may never assert ----------------------------
        self.check("sustainable throughput is not asserted by a single run",
                   self.s.get("sustainable_throughput_verified") == "false",
                   str(self.s.get("sustainable_throughput_verified")))
        self.check("the release verdict is not self-awarded",
                   self.s.get("release_verdict") == "NOT_EVALUATED",
                   str(self.s.get("release_verdict")))

        # --- artifact ---------------------------------------------------------
        artifact = self.s.get("artifact", "")
        digest = self.s.get("artifact_sha256", "NONE")
        if artifact and digest not in ("", "NONE"):
            p = pathlib.Path(artifact)
            if not p.is_absolute():
                p = self.path.parent / p
            if not p.exists():
                self.check("the committed artifact exists", False, str(p))
            else:
                actual = hashlib.sha256(p.read_bytes()).hexdigest()
                self.check("the artifact matches the digest in the report",
                           actual == digest, f"{actual} != {digest}")
                marker = p.with_name(p.name + ".complete")
                self.check("a committed artifact has a completion marker",
                           marker.exists(), str(marker))
        if artifact.endswith(".part"):
            self.check("a .part artifact is never reported as complete",
                       status != "COMPLETE_OFFLINE", str(status))

        # --- descriptor pinning ------------------------------------------------
        if self.s.get("descriptor_pinned") == "true":
            self.check("the pinned descriptor matched",
                       self.s.get("descriptor_matches") == "true")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("report", type=pathlib.Path, nargs="+")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    bad = 0
    for path in args.report:
        if not path.exists():
            print(f"{path}: missing", file=sys.stderr)
            bad += 1
            continue
        c = Checker(parse_scalars(path.read_text()), path)
        c.run()
        if c.failures:
            bad += 1
            print(f"{path}: {len(c.failures)} of {c.checks} checks FAILED")
            for f in c.failures:
                print(f"  - {f}")
        elif not args.quiet:
            print(f"{path}: {c.checks} checks passed")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
