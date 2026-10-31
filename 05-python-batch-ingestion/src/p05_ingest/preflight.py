"""Startup gates. Everything that must be true before the first poll.

Spec FR-18 and section 12.1. The rule is that a misconfiguration fails the
start, never gets quietly repaired. Each check returns a ``CheckResult`` so
``cli doctor`` can print the whole picture instead of stopping at the first
problem, but ``run`` refuses to proceed if any required check failed.

Deliberately included: the descriptor pin, the effective-settings check, the
retention headroom estimate and the fault-lane isolation check. Deliberately
*excluded*: creating or altering the ClickHouse table. A program that can
reshape its own sink can hide a schema mismatch from the operator.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .config import MARKET_TABLE, AppConfig
from .errors import PreflightError
from .proto_schema import descriptor_sha256, schema_package, verify_descriptor_pin


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    ok: bool
    detail: str
    required: bool = True

    @property
    def status(self) -> str:
        if self.ok:
            return "PASS"
        return "FAIL" if self.required else "WARN"


@dataclass(slots=True)
class PreflightReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.results if r.required)

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.required and not r.ok]

    def render(self) -> str:
        width = max((len(r.name) for r in self.results), default=0)
        lines = [
            f"[{r.status}] {r.name.ljust(width)}  {r.detail}" for r in self.results
        ]
        lines.append("")
        lines.append("PREFLIGHT " + ("OK" if self.ok else "FAILED"))
        return "\n".join(lines)

    def raise_if_failed(self) -> None:
        if self.ok:
            return
        detail = "; ".join(f"{r.name}: {r.detail}" for r in self.failures)
        raise PreflightError(f"preflight failed -- {detail}")


def check_schema_pin(cfg: AppConfig) -> CheckResult:
    try:
        verify_descriptor_pin(
            cfg.contracts.protobuf_descriptor_sha256,
            expected_package=cfg.contracts.protobuf_package,
        )
    except PreflightError as exc:
        return CheckResult("protobuf descriptor pin", False, str(exc).splitlines()[0])
    return CheckResult(
        "protobuf descriptor pin",
        True,
        f"{schema_package()} @ {descriptor_sha256()[:16]}...",
    )


def check_sink_schema(cfg: AppConfig, sink) -> CheckResult:
    """Ask the sink to validate its own destination."""
    if sink is None:
        return CheckResult(
            "clickhouse table schema",
            False,
            "no sink available to check",
            required=False,
        )
    try:
        sink.preflight()
    except Exception as exc:
        return CheckResult("clickhouse table schema", False, str(exc))
    return CheckResult("clickhouse table schema", True, cfg.clickhouse.table_fqn)


def check_insert_mode(cfg: AppConfig) -> CheckResult:
    """Spec NFR-09: an ACK must mean stored, not buffered."""
    ch = cfg.clickhouse
    if not ch.async_insert:
        return CheckResult(
            "insert acknowledgement mode",
            True,
            "async_insert=0: the ACK means the insert completed",
        )
    if ch.wait_for_async_insert:
        return CheckResult(
            "insert acknowledgement mode",
            True,
            "async_insert=1 with wait_for_async_insert=1",
            required=True,
        )
    return CheckResult(  # pragma: no cover - config.py rejects this first
        "insert acknowledgement mode",
        False,
        "async_insert=1 with wait_for_async_insert=0 would ACK on buffering",
    )


def check_offset_policy(cfg: AppConfig) -> CheckResult:
    """Spec I-11: no silent reset when the committed offset is unusable."""
    return CheckResult(
        "offset reset policy",
        cfg.kafka.auto_offset_reset in ("error", "none"),
        f"auto.offset.reset={cfg.kafka.auto_offset_reset}",
    )


def check_fault_lane(cfg: AppConfig) -> CheckResult:
    """Spec I-12: deliberately anomalous data must not enter the market table."""
    if not cfg.project.is_fault_lane:
        return CheckResult(
            "fault lane isolation",
            True,
            f"mode=market_valid writing to {cfg.clickhouse.table_fqn}",
        )
    return CheckResult(
        "fault lane isolation",
        cfg.clickhouse.table != MARKET_TABLE,
        f"mode=isolated_fault_test writing to {cfg.clickhouse.table_fqn}",
    )


def check_retention_headroom(cfg: AppConfig, source=None) -> CheckResult:
    """Spec NFR-12: can the group still recover from its current position?

    Reports a WARN rather than a FAIL when the broker cannot be queried: a
    headroom estimate needs live watermarks, and claiming a PASS without them
    would be exactly the kind of unverified assertion the spec forbids.
    """
    if source is None:
        return CheckResult(
            "kafka retention headroom",
            False,
            "not evaluated: no live broker to read log-start and end offsets from",
            required=False,
        )
    worst: tuple[str, int] | None = None
    try:
        for tp in source.assignment():
            low, high = source.watermarks(tp)
            committed = source.committed_next(tp)
            margin = committed - low
            if worst is None or margin < worst[1]:
                worst = (str(tp), margin)
            if committed < low:
                return CheckResult(
                    "kafka retention headroom",
                    False,
                    f"{tp}: committed next offset {committed} is below log start "
                    f"offset {low}; required data has been deleted",
                )
    except Exception as exc:
        return CheckResult(
            "kafka retention headroom", False, f"could not read watermarks: {exc}",
            required=False,
        )
    if worst is None:
        return CheckResult(
            "kafka retention headroom", True, "no partitions assigned yet",
            required=False,
        )
    return CheckResult(
        "kafka retention headroom",
        True,
        f"smallest margin above log start: {worst[1]} offsets on {worst[0]}",
    )


def check_quarantine_policy(cfg: AppConfig) -> CheckResult:
    """Spec FR-12/I-07: skipping a poison record needs a durable ACK."""
    if cfg.quarantine.mode == "stop":
        return CheckResult(
            "poison policy", True, "fail-closed: a poison record stops its partition"
        )
    return CheckResult(
        "poison policy",
        cfg.quarantine.require_ack and bool(cfg.quarantine.topic),
        f"durable DLQ to {cfg.quarantine.topic!r}, ACK required="
        f"{cfg.quarantine.require_ack}",
    )


def check_budget_sanity(cfg: AppConfig) -> CheckResult:
    """A single maximum-size record must fit, with room for a whole batch."""
    batch = cfg.batch
    if batch.per_partition_max_serialized_bytes > batch.app_buffer_budget_bytes:
        return CheckResult(  # pragma: no cover - config.py rejects this first
            "memory budget", False, "a single batch cannot fit in the global budget"
        )
    return CheckResult(
        "memory budget",
        True,
        f"{batch.app_buffer_budget_bytes} B global, "
        f"{batch.per_partition_max_serialized_bytes} B per batch, pause at "
        f"{batch.pause_high_watermark:.0%} / resume at "
        f"{batch.resume_low_watermark:.0%}. Tracked bytes only -- this is not "
        "an RSS bound",
    )


def run_preflight(cfg: AppConfig, *, sink=None, source=None) -> PreflightReport:
    """Run every gate and return the whole picture."""
    report = PreflightReport()
    report.add(check_schema_pin(cfg))
    report.add(check_insert_mode(cfg))
    report.add(check_offset_policy(cfg))
    report.add(check_fault_lane(cfg))
    report.add(check_quarantine_policy(cfg))
    report.add(check_budget_sanity(cfg))
    report.add(check_sink_schema(cfg, sink))
    report.add(check_retention_headroom(cfg, source))
    return report
