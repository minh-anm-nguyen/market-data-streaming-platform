"""Strict TOML configuration, matching section 19.2.

Following the P01 house rule: **unknown keys are errors, at every level.** A typo
in a safety flag must not fall back to a permissive default -- which is exactly
what would happen with ``config.get("allow_publish_with_conflicts", False)`` if
the key were misspelled in the file. The whole point of the flag is that someone
can see it is off.

Every number in the shipped configs is a *dev-lab assumption*, as the design says
in so many words. ``max_execution_time_seconds = 5`` is not a claim about any
host; it is a value that must be re-pinned per machine and recorded in the
benchmark report (NFR-07, NFR-12).
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from .errors import ConfigError

PROFILES: Final = ("dev", "integration", "capacity", "ha-lab")

_SECTIONS: Final = {
    "project": {
        "project_id",
        "profile",
        "contract_version",
        "raw_table",
        "canonical_table",
        "bars_1m_table",
        "require_manifest_sha256",
        "require_p05_offset_cut",
        "require_independent_reconcile",
        "allow_direct_raw_to_mv",
        "allow_publish_on_unknown_ack",
        "allow_publish_with_conflicts",
    },
    "time": {
        "timezone",
        "unit",
        "bar_interval_ns",
        "reject_unsupported_timestamp_range",
    },
    "queries": {
        "require_ready_build",
        "require_dataset_scope",
        "require_symbol_scope",
        "require_minute_alignment",
        "max_bars_returned",
        "max_query_window_minutes",
        "max_execution_time_seconds",
        "max_concurrent_interactive_queries",
        "max_canonical_rows",
    },
    "canonical": {
        "require_unique_source_identity",
        "require_identical_semantic_digest_on_retries",
        "publish_mode",
        "manual_rebuild_on_late_corrections",
        "insert_block_size",
    },
    "retention": {
        "enabled",
        "raw_ttl_days_candidate",
        "require_restore_drill",
        "recovery_window_days",
        "audit_window_days",
        "rebuild_window_days",
        "late_arrival_window_days",
        "backup_max_age_days",
    },
    "benchmark": {
        "query_repetitions",
        "query_concurrency",
        "dataset_rows_targets",
        "offered_rates_events_s",
    },
    "clickhouse": {"url", "user", "password", "database", "timeout_s"},
}

#: Flags that must stay ``False``. The design does not offer these as options:
#: publishing with conflicts or on an unknown ACK is the failure mode the whole
#: gate exists to prevent, and a config file must not be able to turn it on
#: (section 5.2, NFR-06).
_MUST_BE_FALSE: Final = (
    ("project", "allow_direct_raw_to_mv"),
    ("project", "allow_publish_on_unknown_ack"),
    ("project", "allow_publish_with_conflicts"),
)

#: Flags that must stay ``True`` for a run to mean anything.
_MUST_BE_TRUE: Final = (
    ("project", "require_manifest_sha256"),
    ("project", "require_p05_offset_cut"),
    ("project", "require_independent_reconcile"),
    ("canonical", "require_unique_source_identity"),
    ("canonical", "require_identical_semantic_digest_on_retries"),
)


def _reject_unknown(table: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise ConfigError(
            f"unknown configuration key(s) in [{where}]: {', '.join(unknown)}",
            code="CONFIG_UNKNOWN_KEY",
            section=where,
            unknown=unknown,
            allowed=sorted(allowed),
        )


def _require(table: dict[str, Any], key: str, where: str) -> Any:
    if key not in table:
        raise ConfigError(
            f"missing required key '{key}' in [{where}]",
            code="CONFIG_MISSING_KEY",
            section=where,
            key=key,
        )
    return table[key]


def _as_bool(value: Any, key: str, where: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(
            f"'{key}' in [{where}] must be a boolean", code="CONFIG_TYPE_ERROR", key=key
        )
    return value


def _as_int(value: Any, key: str, where: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(
            f"'{key}' in [{where}] must be an integer, got {type(value).__name__}",
            code="CONFIG_TYPE_ERROR",
            key=key,
        )
    if minimum is not None and value < minimum:
        raise ConfigError(
            f"'{key}' in [{where}] must be >= {minimum}, got {value}",
            code="CONFIG_RANGE_ERROR",
            key=key,
        )
    return value


@dataclass(frozen=True, slots=True)
class Config:
    """A validated configuration plus the hash of its resolved form."""

    profile: str
    project: dict[str, Any]
    time: dict[str, Any]
    queries: dict[str, Any]
    canonical: dict[str, Any]
    retention: dict[str, Any]
    benchmark: dict[str, Any]
    clickhouse: dict[str, Any] = field(default_factory=dict)
    source_path: str = ""

    def resolved(self) -> dict[str, Any]:
        """The whole config, with the password reduced to a digest, in a stable order.

        A constant ``<redacted>`` would make :meth:`sha256` blind to a credential
        change, and which server a run authenticated against is part of what the
        run was. A truncated digest keeps the hash sensitive without putting the
        secret in an evidence file.
        """
        clickhouse = {k: v for k, v in self.clickhouse.items() if k != "password"}
        if "password" in self.clickhouse:
            password = str(self.clickhouse["password"]).encode("utf-8")
            fingerprint = hashlib.sha256(password).hexdigest()[:16]
            clickhouse["password"] = f"<redacted:sha256:{fingerprint}>"
        return {
            "benchmark": dict(sorted(self.benchmark.items())),
            "canonical": dict(sorted(self.canonical.items())),
            "clickhouse": dict(sorted(clickhouse.items())),
            "project": dict(sorted(self.project.items())),
            "queries": dict(sorted(self.queries.items())),
            "retention": dict(sorted(self.retention.items())),
            "time": dict(sorted(self.time.items())),
        }

    def sha256(self) -> str:
        """``config_sha`` of an evidence record.

        Computed over the resolved config with the password redacted, so the hash
        is shareable and still changes when any behaviour-affecting value does.
        """
        return hashlib.sha256(
            json.dumps(self.resolved(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def query_limits(self):  # -> QueryLimits, imported lazily to avoid a cycle
        from .query_service import QueryLimits

        return QueryLimits(
            max_bars_returned=self.queries["max_bars_returned"],
            max_window_minutes=self.queries["max_query_window_minutes"],
            max_canonical_rows=self.queries.get("max_canonical_rows", 100_000),
            require_ready_build=self.queries["require_ready_build"],
            require_minute_alignment=self.queries["require_minute_alignment"],
        )

    def retention_policy(self):  # -> RetentionPolicy
        from .ttl_controller import RetentionPolicy

        return RetentionPolicy(
            raw_ttl_days=self.retention["raw_ttl_days_candidate"],
            recovery_window_days=self.retention["recovery_window_days"],
            audit_window_days=self.retention["audit_window_days"],
            rebuild_window_days=self.retention["rebuild_window_days"],
            late_arrival_window_days=self.retention["late_arrival_window_days"],
            restore_drill_passed=False,  # never asserted by a config file
            backup_age_days=None,  # measured, not declared
            backup_max_age_days=self.retention["backup_max_age_days"],
            enabled=self.retention["enabled"],
        )


def load_config(path: str | Path) -> Config:
    """Parse, validate and hash a config file."""
    path = Path(path)
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {path}", code="CONFIG_NOT_FOUND") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}", code="CONFIG_INVALID_TOML") from exc

    _reject_unknown(raw, set(_SECTIONS), "<root>")
    for section in ("project", "time", "queries", "canonical", "retention", "benchmark"):
        if section not in raw:
            raise ConfigError(
                f"missing required section [{section}] in {path}",
                code="CONFIG_MISSING_SECTION",
                section=section,
            )
    for section, allowed in _SECTIONS.items():
        if section in raw:
            if not isinstance(raw[section], dict):
                raise ConfigError(
                    f"[{section}] must be a table", code="CONFIG_TYPE_ERROR", section=section
                )
            _reject_unknown(raw[section], allowed, section)

    project = raw["project"]
    profile = _require(project, "profile", "project")
    if profile not in PROFILES:
        raise ConfigError(
            f"profile must be one of {list(PROFILES)}, got {profile!r}",
            code="CONFIG_BAD_PROFILE",
        )

    for section, key in _MUST_BE_FALSE:
        if _as_bool(_require(raw[section], key, section), key, section):
            raise ConfigError(
                f"[{section}].{key} must be false: the design does not permit it, and a "
                f"config file is not the place to override a correctness gate",
                code="CONFIG_FORBIDDEN_FLAG",
                section=section,
                key=key,
            )
    for section, key in _MUST_BE_TRUE:
        if not _as_bool(_require(raw[section], key, section), key, section):
            raise ConfigError(
                f"[{section}].{key} must be true; without it a build's verdict is "
                f"not evidence of anything",
                code="CONFIG_REQUIRED_FLAG",
                section=section,
                key=key,
            )

    time_table = raw["time"]
    if _require(time_table, "timezone", "time") != "UTC":
        raise ConfigError(
            "[time].timezone must be UTC; every bucket and partition in this "
            "design is defined in UTC",
            code="CONFIG_BAD_TIMEZONE",
        )
    bar_interval = _as_int(
        _require(time_table, "bar_interval_ns", "time"), "bar_interval_ns", "time", minimum=1
    )
    if bar_interval != 60_000_000_000:
        raise ConfigError(
            f"[time].bar_interval_ns must be 60000000000 for the 1m product, got "
            f"{bar_interval}",
            code="CONFIG_BAD_BAR_INTERVAL",
        )

    queries = raw["queries"]
    for key in (
        "max_bars_returned",
        "max_query_window_minutes",
        "max_execution_time_seconds",
        "max_concurrent_interactive_queries",
    ):
        _as_int(_require(queries, key, "queries"), key, "queries", minimum=1)
    for key in (
        "require_ready_build",
        "require_dataset_scope",
        "require_symbol_scope",
        "require_minute_alignment",
    ):
        if not _as_bool(_require(queries, key, "queries"), key, "queries"):
            raise ConfigError(
                f"[queries].{key} must be true: an unscoped or unaligned query has "
                f"no well-defined answer",
                code="CONFIG_REQUIRED_FLAG",
                key=key,
            )

    canonical = raw["canonical"]
    publish_mode = _require(canonical, "publish_mode", "canonical")
    if publish_mode != "sealed_snapshot_only":
        raise ConfigError(
            f"[canonical].publish_mode must be 'sealed_snapshot_only' in the MVP; "
            f"a live certified mode is a separate workstream (ADR-002), got "
            f"{publish_mode!r}",
            code="CONFIG_BAD_PUBLISH_MODE",
        )
    _as_int(
        canonical.get("insert_block_size", 5_000), "insert_block_size", "canonical", minimum=1
    )

    retention = raw["retention"]
    for key in (
        "raw_ttl_days_candidate",
        "recovery_window_days",
        "audit_window_days",
        "rebuild_window_days",
        "late_arrival_window_days",
        "backup_max_age_days",
    ):
        _as_int(_require(retention, key, "retention"), key, "retention", minimum=0)
    _as_bool(_require(retention, "enabled", "retention"), "enabled", "retention")

    benchmark = raw["benchmark"]
    for key in ("query_concurrency", "dataset_rows_targets", "offered_rates_events_s"):
        value = _require(benchmark, key, "benchmark")
        if not isinstance(value, list) or not value or not all(
            isinstance(v, int) and not isinstance(v, bool) and v > 0 for v in value
        ):
            raise ConfigError(
                f"[benchmark].{key} must be a non-empty list of positive integers",
                code="CONFIG_TYPE_ERROR",
                key=key,
            )
    _as_int(
        _require(benchmark, "query_repetitions", "benchmark"),
        "query_repetitions",
        "benchmark",
        minimum=1,
    )

    return Config(
        profile=profile,
        project=dict(project),
        time=dict(time_table),
        queries=dict(queries),
        canonical=dict(canonical),
        retention=dict(retention),
        benchmark=dict(benchmark),
        clickhouse=dict(raw.get("clickhouse", {})),
        source_path=str(path),
    )
