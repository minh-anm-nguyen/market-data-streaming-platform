"""Render a run report as Markdown, and aggregate many runs into a release gate.

Two renderers:

:func:`render_summary`
    one run, for a human. It leads with the verdict and the profile caveat,
    because a reader who stops after two lines must not come away thinking a
    model-lab pass says anything about a real broker.

:func:`render_release_report`
    many runs plus the what-if catalog, which is design 16.2's release document.
    Its hard rule: a mandatory ``P0`` case that is ``INVALID``, ``INCONCLUSIVE``
    or ``NOT_RUN`` is ``NO-GO``. Unrun cases are counted as unrun, never
    quietly dropped from the denominator -- "448 designed" and "n executed" are
    different numbers and both are printed.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from ..evidence.verdicts import Verdict

_VERDICT_ORDER = ("PASS", "FAIL", "INCONCLUSIVE", "INVALID", "NOT_RUN")


def _fmt(value: Any) -> str:
    if value is None:
        return "not measured"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _diff_line(label: str, diff: Mapping[str, Any] | None) -> str:
    if not diff:
        return f"| {label} | not run | - | - | - |"
    return (
        f"| {label} | {_fmt(diff.get('equal'))} | {diff.get('missing_count', 0)} | "
        f"{diff.get('extra_count', 0)} | {diff.get('mismatched_count', 0)} |"
    )


def render_summary(report: Mapping[str, Any]) -> str:
    """Markdown for one run."""
    stages = report.get("stages") or {}
    raw = (stages.get("raw") or {}).get("diff")
    canonical = (stages.get("canonical") or {}).get("diff")
    transport = (stages.get("transport") or {}).get("diff")
    bars = stages.get("bars") or {}
    faults = report.get("faults") or {}
    recovery = report.get("recovery") or {}
    evidence = report.get("evidence") or {}
    expected = report.get("expected") or {}

    lines: list[str] = [
        f"# {report.get('scenario_id', 'scenario')} / {report.get('run_id', 'run')}",
        "",
        f"**Verdict: {report.get('verdict')}** (`{report.get('primary_reason')}`) · "
        f"release-blocking: {_fmt(report.get('release_blocking'))}",
        "",
        f"> **Profile `{report.get('profile')}`.** {report.get('profile_note')}",
        "",
        "## Reconciliation",
        "",
        "| Boundary | Sets equal | Missing | Extra | Conflicting |",
        "|---|---|---:|---:|---:|",
        _diff_line("Kafka cut (L2)", transport),
        _diff_line("ClickHouse raw (L3)", raw),
        _diff_line("Canonical build (L4)", canonical),
        "",
    ]

    if bars:
        lines += [
            "### Bars (L5)",
            "",
            f"- expected bars: {bars.get('expected_bars')}, observed: {bars.get('observed_bars')}",
            f"- expected tick total: {bars.get('expected_tick_total')}, "
            f"observed: {bars.get('observed_tick_total')}",
            f"- mismatched buckets: {bars.get('mismatched_count')}, "
            f"missing: {bars.get('missing_count')}, extra: {bars.get('extra_count')}",
            "",
        ]

    lines += [
        "## Expected set (independent P03 policy oracle)",
        "",
        f"- policy version: `{expected.get('policy_version', 'n/a')}`",
        f"- counts: {expected.get('counts')}",
        f"- untracked anomalies: {expected.get('untracked_anomaly_count', 0)}",
        "",
        "## Faults",
        "",
        f"- injected: {faults.get('faults_total', 0)}, confirmed: {faults.get('faults_confirmed', 0)}, "
        f"unconfirmed: {faults.get('faults_unconfirmed', 0)}",
        f"- trigger success ratio: {_fmt(faults.get('trigger_success_ratio'))}",
        "",
        "## Recovery",
        "",
        f"- RTO-SERVICE: {_fmt(recovery.get('rto_service_ns'))} ns",
        f"- RTO-CORRECT: {_fmt(recovery.get('rto_correct_ns'))} ns",
        f"- RPO events unrecoverable: {_fmt(recovery.get('rpo_events'))} "
        f"(zero claimable: {_fmt(recovery.get('rpo_zero_claimable'))})",
        "",
        "## Evidence",
        "",
        f"- journal entries verified: {(evidence.get('journal') or {}).get('entries_verified')}, "
        f"chain intact: {_fmt((evidence.get('journal') or {}).get('chain_intact'))}",
        f"- artifact bytes: {evidence.get('artifact_bytes')}",
        f"- anchor: `{(evidence.get('anchor') or {}).get('head_sha256', '')[:16]}...` "
        "(store this outside the lab's failure domain or the chain proves nothing)",
        "",
    ]

    findings = report.get("findings") or []
    if findings:
        lines += ["## Findings", "", "| Reason | Detail |", "|---|---|"]
        for finding in findings[:25]:
            detail = str(finding.get("detail", {}))
            if len(detail) > 160:
                detail = detail[:157] + "..."
            lines.append(f"| `{finding.get('reason')}` | {detail} |")
        if len(findings) > 25:
            lines.append(f"| ... | {len(findings) - 25} more in `reports/results.json` |")
        lines.append("")
    else:
        lines += ["## Findings", "", "None. Every check completed with no contradiction.", ""]

    lines += [
        "## Limitations",
        "",
        "- A pass here is scoped to this profile, this dataset and these faults.",
        "- No throughput or latency figure is published unless a perf run measured it.",
        "- Source integrity is only as strong as the external anchor; without one the",
        "  bundle is self-consistent, which is not the same as unmodified.",
        "",
    ]
    return "\n".join(lines)


def tally_verdicts(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """Count verdicts, with every bucket present even at zero."""
    counts = {name: 0 for name in _VERDICT_ORDER}
    for row in rows:
        verdict = str(row.get("verdict", Verdict.NOT_RUN.value))
        counts[verdict] = counts.get(verdict, 0) + 1
    return counts


def render_release_report(
    *,
    catalog: Sequence[Mapping[str, Any]],
    executed: Sequence[Mapping[str, Any]],
    e2e: Sequence[Mapping[str, Any]] = (),
) -> str:
    """The release document from design 16.2.

    ``catalog`` is the full 448-case inventory; ``executed`` is what actually
    ran, keyed by ``case_id``. Cases in the catalog with no execution are
    ``NOT_RUN``, and that is reported as a number rather than omitted -- the
    design's own warning is "không đánh đồng 448 thiết kế với 448 lần chạy
    thành công".
    """
    executed_by_case: dict[str, Mapping[str, Any]] = {}
    for row in executed:
        for case_id in row.get("cases", ()):
            existing = executed_by_case.get(case_id)
            # Worst verdict wins, so one failing test cannot be hidden by a
            # passing one that happens to cover the same case.
            if existing is None or _VERDICT_ORDER.index(str(row["verdict"])) > _VERDICT_ORDER.index(
                str(existing["verdict"])
            ):
                executed_by_case[case_id] = row

    rows: list[dict[str, Any]] = []
    for case in catalog:
        case_id = str(case["case_id"])
        run = executed_by_case.get(case_id)
        rows.append(
            {
                "case_id": case_id,
                "priority": case.get("priority", "P?"),
                "group": case.get("group", ""),
                "verdict": str(run["verdict"]) if run else Verdict.NOT_RUN.value,
                "covered_by": run.get("test") if run else None,
            }
        )

    totals = tally_verdicts(rows)
    p0 = [row for row in rows if row["priority"] == "P0"]
    p0_totals = tally_verdicts(p0)
    p0_blocking = [row for row in p0 if row["verdict"] != Verdict.PASS.value]
    decision = "NO-GO" if p0_blocking else "GO (for the tested profile and scope)"

    lines = [
        "# P07 release report",
        "",
        f"**Decision: {decision}**",
        "",
        f"- what-if cases designed: {len(rows)}",
        f"- P0 cases: {len(p0)}",
        "",
        "## Case inventory",
        "",
        "| Verdict | All cases | P0 cases |",
        "|---|---:|---:|",
    ]
    for name in _VERDICT_ORDER:
        lines.append(f"| {name} | {totals.get(name, 0)} | {p0_totals.get(name, 0)} |")
    lines += [
        "",
        "A mandatory P0 case that is INVALID, INCONCLUSIVE or NOT_RUN is NO-GO: "
        "unproven is not the same as fine (design 9.3).",
        "",
    ]

    if e2e:
        lines += ["## Mandatory end-to-end cases", "", "| Case | Verdict | Covered by |", "|---|---|---|"]
        for row in e2e:
            lines.append(
                f"| {row.get('case')} | {row.get('verdict')} | {row.get('test') or 'not run'} |"
            )
        lines.append("")

    if p0_blocking:
        lines += ["## Blocking P0 cases", "", "| Case | Group | Verdict |", "|---|---|---|"]
        for row in p0_blocking[:60]:
            lines.append(f"| {row['case_id']} | {row['group']} | {row['verdict']} |")
        if len(p0_blocking) > 60:
            lines.append(f"| ... | | {len(p0_blocking) - 60} more |")
        lines.append("")

    lines += [
        "## What this report does not claim",
        "",
        "- No real Kafka broker or ClickHouse server was exercised, so integration and HA",
        "  behaviour is NOT VALIDATED (design 16.1, row \"Only mock tests run\").",
        "- No throughput figure is asserted. Capacity profiles S1-S6 are NOT_RUN.",
        "- Cases needing cgroups, a real network stack, replica placement or a materialised-view",
        "  partial-state model are NOT_RUN rather than passed.",
        "",
    ]
    return "\n".join(lines)
