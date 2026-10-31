"""``p09`` -- the integration and release command line.

Exit codes are the interface a CI job asserts on, so they are part of the
contract rather than a side effect of how the code happens to return:

==== ========================================================================
0    the command ran and found nothing outstanding
1    the command ran and something is outstanding (findings, a non-PASS audit)
2    the request was refused: bad arguments, or no repository to look at
3    the stored lock disagrees with the tree, or is stale
==== ========================================================================

A non-zero code never means "the tool broke". An internal failure raises and is
visible as a traceback, because a crash reported as a clean FAIL would be
indistinguishable from a real finding.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import audit as audit_mod
from . import deploy, lockfile, pipeline, probes, release, schemasync

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_REFUSED = 2
EXIT_DRIFT = 3

_SEVERITY_ORDER = {"BLOCKER": 0, "HIGH": 1, "MEDIUM": 2, "INFO": 3}


def _default_repo_root() -> Path:
    """The repository root, assuming this package lives in ``09-*/src/p09_integration``."""
    return Path(__file__).resolve().parents[3]


def _project_dir() -> Path:
    """The P09 project directory, where ``contracts/`` and ``reports/`` live."""
    return Path(__file__).resolve().parents[2]


def _resolve_root(args: argparse.Namespace) -> Path | None:
    root = Path(args.repo_root).resolve() if args.repo_root else _default_repo_root()
    if not root.is_dir():
        print("refused: repository root does not exist: %s" % root, file=sys.stderr)
        return None
    present = [d for d in probes.PROJECT_DIRS.values() if (root / d).is_dir()]
    if not present:
        print(
            "refused: %s contains none of the expected project directories" % root,
            file=sys.stderr,
        )
        return None
    return root


def _run_audit(root: Path) -> audit_mod.AuditResult:
    return audit_mod.run_audit(root, probes.probe_all(root))


def _lock_path(value: str | None) -> Path:
    if value:
        return Path(value)
    return _project_dir() / "contracts" / lockfile.LOCK_FILENAME


def _write_report(result: audit_mod.AuditResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(result.as_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _without_timestamp(text: str) -> str:
    """The lock minus its generation time, so a re-cut is not false drift."""
    return "\n".join(
        line for line in text.splitlines() if not line.startswith("generated_at_utc:")
    )


def _print_audit(result: audit_mod.AuditResult, verbose: bool) -> None:
    counts = result.as_dict()["counts"]
    unresolved = counts["unresolved_by_severity"]
    print("P09 M0 contract audit")
    print("  repo      %s" % result.repo_root)
    print("  generated %s" % result.generated_at_utc)
    print("  verdict   %s" % result.verdict)
    print(
        "  checks    %d run, %d agreed, %d outstanding"
        % (counts["checks"], counts["passed"], counts["checks"] - counts["passed"])
    )
    print(
        "  severity  BLOCKER=%d HIGH=%d MEDIUM=%d"
        % (unresolved["BLOCKER"], unresolved["HIGH"], unresolved["MEDIUM"])
    )
    print()

    outstanding = sorted(
        (f for f in result.findings if f.verdict != "PASS"),
        key=lambda f: (_SEVERITY_ORDER[f.severity], f.id),
    )
    if outstanding:
        print("OUTSTANDING")
        for finding in outstanding:
            print(
                "  %-8s %-12s %s/%s  %s"
                % (
                    finding.severity,
                    finding.id,
                    finding.gate,
                    finding.milestone,
                    finding.title,
                )
            )
            print("      %s" % finding.detail)
            if finding.remediation:
                print("      fix: %s" % finding.remediation)
            if verbose:
                for line in finding.evidence:
                    print("      | %s" % line)
            print()

    agreed = [f for f in result.findings if f.verdict == "PASS"]
    if agreed:
        print("AGREED (%d)" % len(agreed))
        for finding in agreed:
            print("  %-12s %s" % (finding.id, finding.title))
            if verbose:
                for line in finding.evidence:
                    print("      | %s" % line)


def cmd_audit(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    result = _run_audit(root)
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        _print_audit(result, verbose=args.verbose)
    if args.report:
        path = Path(args.report)
        _write_report(result, path)
        if not args.json:
            print("\nreport written: %s" % path)
    return EXIT_OK if result.verdict == "M0_PASS" else EXIT_FINDINGS


def cmd_contracts_lock(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    result = _run_audit(root)
    document = lockfile.build_lock(result)
    out = _lock_path(args.out)

    if args.check:
        if not out.exists():
            print("refused: no lock at %s to check against" % out, file=sys.stderr)
            return EXIT_REFUSED
        current = out.read_text(encoding="utf-8")
        proposed = lockfile.render_lock(document)
        if _without_timestamp(current) == _without_timestamp(proposed):
            print("lock is current: %s" % out)
            return EXIT_OK
        print("lock is stale: %s" % out, file=sys.stderr)
        print("run `p09 contracts-lock` to regenerate it", file=sys.stderr)
        return EXIT_DRIFT

    lockfile.write_lock(out, document)
    schema = document["cross_project_identity"]["wire_schema"]
    print("wrote %s" % out)
    print("  audit verdict   %s" % document["audit_verdict"])
    print("  schema identity %s" % schema["normalized_sha256"])
    print("  open findings   %d" % len(document["open_findings"]))
    if args.report:
        path = Path(args.report)
        _write_report(result, path)
        print("  report written  %s" % path)
    return EXIT_OK if not document["open_findings"] else EXIT_FINDINGS


def cmd_contracts_verify(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    path = _lock_path(args.lock)
    if not path.exists():
        print("refused: no lock at %s" % path, file=sys.stderr)
        print("run `p09 contracts-lock` first", file=sys.stderr)
        return EXIT_REFUSED
    try:
        locked = lockfile.load_lock(path)
    except ValueError as exc:
        print("refused: %s" % exc, file=sys.stderr)
        return EXIT_REFUSED

    result = _run_audit(root)
    drifted, notes = lockfile.verify_lock(locked, result)

    print("P09 contract verification against %s" % path)
    print("  lock cut at   %s" % locked.get("generated_at_utc"))
    print("  lock verdict  %s" % locked.get("audit_verdict"))
    print("  fresh verdict %s" % result.verdict)
    print()
    if notes:
        print("NOTES (per-build values, expected to vary)")
        for note in notes:
            print("  - %s" % note)
        print()
    if drifted:
        print("DRIFT in cross-project identity (%d)" % len(drifted))
        for line in drifted:
            print("  ! %s" % line)
        print()
        print("A cross-project identity moved. Either the change is intended, in")
        print("which case re-cut the lock and record it in the release, or a project")
        print("has broken the contract and must be fixed before anything is deployed.")
        return EXIT_DRIFT
    print("no drift: every cross-project identity matches the lock")
    return EXIT_OK if result.verdict == "M0_PASS" else EXIT_FINDINGS


def cmd_facts(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    facts = probes.probe_all(root)
    if args.json:
        print(
            json.dumps(
                {k: v.as_dict() for k, v in facts.items()}, indent=2, sort_keys=True
            )
        )
        return EXIT_OK
    unknown_total = 0
    for project, pf in facts.items():
        print("%s  %s" % (project, pf.directory))
        print("     %s" % pf.role)
        if not pf.present:
            print("     ABSENT")
            print()
            continue
        for key in sorted(pf.facts):
            fact = pf.facts[key]
            if fact.known:
                print("     %-28s %r" % (key, fact.value))
            else:
                unknown_total += 1
                print("     %-28s UNKNOWN -- %s" % (key, fact.unknown_reason))
        for error in pf.errors:
            print("     ERROR %s" % error)
        print()
    print("%d facts could not be established" % unknown_total)
    return EXIT_OK if unknown_total == 0 else EXIT_FINDINGS


def _load_lock_or_refuse(path: Path) -> dict[str, object] | None:
    if not path.exists():
        print("refused: no lock at %s" % path, file=sys.stderr)
        print("run `p09 contracts-lock` first", file=sys.stderr)
        return None
    try:
        return lockfile.load_lock(path)
    except ValueError as exc:
        print("refused: %s" % exc, file=sys.stderr)
        return None


def cmd_render_init(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    locked = _load_lock_or_refuse(_lock_path(args.lock))
    if locked is None:
        return EXIT_REFUSED
    out_dir = Path(args.out) if args.out else _project_dir() / "compose" / "init"
    try:
        written = deploy.render_init(locked, root, out_dir)
    except deploy.DeployError as exc:
        print("refused: %s" % exc, file=sys.stderr)
        return EXIT_REFUSED
    print("rendered %d init artifacts into %s" % (len(written), out_dir))
    for path in written:
        print("  %s" % path.name)
    print()
    print("Run them where the Docker daemon is; see %s" % (out_dir / "README.md"))
    return EXIT_OK


def cmd_verify_infra(args: argparse.Namespace) -> int:
    locked = _load_lock_or_refuse(_lock_path(args.lock))
    if locked is None:
        return EXIT_REFUSED
    source = Path(args.observed)
    if not source.exists():
        print("refused: no observation at %s" % source, file=sys.stderr)
        print("run init/03-verify.sh and redirect its output here", file=sys.stderr)
        return EXIT_REFUSED
    try:
        observed = deploy.parse_verify_report(source.read_text(encoding="utf-8"))
        report = deploy.infra_report(locked, observed)
    except deploy.DeployError as exc:
        print("refused: %s" % exc, file=sys.stderr)
        return EXIT_REFUSED

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print("P09 G2 infrastructure check")
        print("  lock cut at   %s" % report["lock_cut_at_utc"])
        print("  observed at   %s" % report["observed_at_utc"])
        print("  cluster id    %s" % (report["cluster_id"] or "unknown"))
        print("  clickhouse    %s" % (report["clickhouse_version"] or "unknown"))
        print("  verdict       %s" % report["verdict"])
        print()
        for finding in report["findings"]:
            print("  %-12s %-42s %s" % (finding["verdict"], finding["subject"], finding["detail"]))
    if args.report:
        path = Path(args.report)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        if not args.json:
            print("\nreport written: %s" % path)
    return EXIT_OK if report["verdict"] == "G2_PASS" else EXIT_FINDINGS


def cmd_render_pipeline(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    locked = _load_lock_or_refuse(_lock_path(args.lock))
    if locked is None:
        return EXIT_REFUSED
    out_dir = Path(args.out) if args.out else _project_dir() / "compose" / "pipeline"
    try:
        written = pipeline.render_pipeline(locked, root, out_dir)
    except deploy.DeployError as exc:
        print("refused: %s" % exc, file=sys.stderr)
        return EXIT_REFUSED
    print("rendered the pipeline into %s" % out_dir)
    for path in written:
        print("  %s" % path.name)
    print()
    print("%d stages, P01 through P07. Run it where the Docker daemon is:" % len(pipeline.STAGES))
    print("  sh %s" % (out_dir / "run-pipeline.sh"))
    return EXIT_OK


def cmd_sync_schema(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    try:
        if args.write:
            written = schemasync.write_copies(root)
            print("synced %d consumer copies from %s" % (len(written), schemasync.CANONICAL))
            for state in written:
                print("  %-4s %s" % (state.project, state.path))
            return EXIT_OK
        states, expected = schemasync.check_copies(root)
    except schemasync.SchemaSyncError as exc:
        print("refused: %s" % exc, file=sys.stderr)
        return EXIT_REFUSED

    print("canonical %s" % schemasync.CANONICAL)
    print("  sha256  %s" % expected)
    print()
    drifted = [s for s in states if not s.matches]
    for state in states:
        print("  %-9s %-4s %s" % (state.status, state.project, state.path))
    if not drifted:
        print()
        print("every consumer copy is byte-identical to the canonical schema")
        return EXIT_OK
    print()
    print("%d copy/copies have drifted from the canonical schema." % len(drifted))
    print("Edit only %s, then run `p09 sync-schema --write`." % schemasync.CANONICAL)
    return EXIT_DRIFT


def cmd_preflight(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    locked = _load_lock_or_refuse(_lock_path(args.lock))
    if locked is None:
        return EXIT_REFUSED
    result = release.preflight(root, _project_dir(), locked)

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        print("P09 release preflight")
        print("  G0 build/provenance  %s" % result.g0)
        print("  contract lock        %s" % result.lock_verdict)
        print("  schema identity      %s" % (result.schema_identity or "unknown"))
        print("  open findings        %d" % result.open_findings)
        print()
        print("IMAGES")
        for image in result.images:
            print("  %-11s %-44s pinned by %s" % (image.service, image.ref, image.pinned_by))
            print("      digest %s (%s)" % (image.digest or "UNKNOWN", image.digest_source))
        print()
        print("NOTES")
        for note in result.notes:
            print("  - %s" % note)
    return EXIT_OK if result.g0 == "GO" else EXIT_FINDINGS


def cmd_release_manifest(args: argparse.Namespace) -> int:
    root = _resolve_root(args)
    if root is None:
        return EXIT_REFUSED
    locked = _load_lock_or_refuse(_lock_path(args.lock))
    if locked is None:
        return EXIT_REFUSED

    # Gate values are read from the evidence that exists, never defaulted to a
    # pass: a missing report leaves its gate at NOT_RUN.
    infra_verdict = None
    pipeline_verdict = None
    reports = _project_dir() / "reports"
    infra = reports / "infra.json"
    if infra.exists():
        try:
            infra_verdict = json.loads(infra.read_text(encoding="utf-8")).get("verdict")
        except (OSError, json.JSONDecodeError):
            infra_verdict = None
    if args.pipeline_report:
        path = Path(args.pipeline_report)
        if path.exists():
            try:
                pipeline_verdict = json.loads(path.read_text(encoding="utf-8")).get("verdict")
            except (OSError, json.JSONDecodeError):
                pipeline_verdict = None

    result = release.preflight(root, _project_dir(), locked)
    document = release.build_release_manifest(
        result, locked, infra_verdict=infra_verdict, pipeline_verdict=pipeline_verdict
    )
    out = Path(args.out) if args.out else _project_dir() / "releases" / "release-manifest.yaml"
    release.write_release_manifest(out, document)
    print("wrote %s" % out)
    print("  verdict %s" % document["verdict"])
    for gate, value in document["gates"].items():
        print("    %-26s %s" % (gate, value))
    return EXIT_OK if document["verdict"] == "GO" else EXIT_FINDINGS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p09",
        description=(
            "Full-system integration for the market data platform: audits the "
            "contracts between projects 01-08 and pins them."
        ),
    )
    parser.add_argument(
        "--repo-root",
        help="repository root holding 01-*..08-* (default: inferred from this package)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_audit = sub.add_parser("audit", help="run the M0 cross-project contract audit")
    p_audit.add_argument(
        "-v", "--verbose", action="store_true", help="show the evidence per finding"
    )
    p_audit.add_argument("--json", action="store_true", help="emit the full result as JSON")
    p_audit.add_argument("--report", help="also write the JSON report to this path")
    p_audit.set_defaults(func=cmd_audit)

    p_lock = sub.add_parser("contracts-lock", help="generate contracts.lock.yaml")
    p_lock.add_argument("--out", help="lock path (default: contracts/contracts.lock.yaml)")
    p_lock.add_argument("--report", help="also write the JSON audit report here")
    p_lock.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit 3 if the stored lock is stale (for CI)",
    )
    p_lock.set_defaults(func=cmd_contracts_lock)

    p_verify = sub.add_parser(
        "contracts-verify", help="check the tree against a stored lock"
    )
    p_verify.add_argument(
        "--lock", help="lock path (default: contracts/contracts.lock.yaml)"
    )
    p_verify.set_defaults(func=cmd_contracts_verify)

    p_facts = sub.add_parser("facts", help="dump the probed facts for every project")
    p_facts.add_argument("--json", action="store_true", help="emit JSON")
    p_facts.set_defaults(func=cmd_facts)

    p_render = sub.add_parser(
        "render-init", help="generate the Kafka/ClickHouse init jobs from the lock"
    )
    p_render.add_argument("--lock", help="lock path (default: contracts/contracts.lock.yaml)")
    p_render.add_argument("--out", help="output directory (default: compose/init)")
    p_render.set_defaults(func=cmd_render_init)

    p_infra = sub.add_parser(
        "verify-infra", help="judge an init/03-verify.sh observation against the lock"
    )
    p_infra.add_argument(
        "--observed", required=True, help="JSON file produced by init/03-verify.sh"
    )
    p_infra.add_argument("--lock", help="lock path (default: contracts/contracts.lock.yaml)")
    p_infra.add_argument("--json", action="store_true", help="emit JSON")
    p_infra.add_argument("--report", help="also write the JSON report here")
    p_infra.set_defaults(func=cmd_verify_infra)

    p_pipe = sub.add_parser(
        "render-pipeline", help="generate the end-to-end pipeline runner from the lock"
    )
    p_pipe.add_argument("--lock", help="lock path (default: contracts/contracts.lock.yaml)")
    p_pipe.add_argument("--out", help="output directory (default: compose/pipeline)")
    p_pipe.set_defaults(func=cmd_render_pipeline)

    p_sync = sub.add_parser(
        "sync-schema",
        help="check (or regenerate) the consumer schema copies from P02's canonical one",
    )
    p_sync.add_argument(
        "--write",
        action="store_true",
        help="overwrite the consumer copies with the canonical bytes",
    )
    p_sync.set_defaults(func=cmd_sync_schema)

    p_pre = sub.add_parser(
        "preflight", help="what a release would consist of, and what it cannot claim"
    )
    p_pre.add_argument("--lock", help="lock path (default: contracts/contracts.lock.yaml)")
    p_pre.add_argument("--json", action="store_true", help="emit JSON")
    p_pre.set_defaults(func=cmd_preflight)

    p_rel = sub.add_parser(
        "release-manifest", help="cut the release record, with every gate at its measured value"
    )
    p_rel.add_argument("--lock", help="lock path (default: contracts/contracts.lock.yaml)")
    p_rel.add_argument("--out", help="output path (default: releases/release-manifest.yaml)")
    p_rel.add_argument(
        "--pipeline-report", help="pipeline-report.json, to fill G3 from real evidence"
    )
    p_rel.set_defaults(func=cmd_release_manifest)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
