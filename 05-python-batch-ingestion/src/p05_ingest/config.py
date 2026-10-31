"""Typed configuration, validation and the fail-closed startup gates.

Three jobs, in order:

  1. parse ``config/*.toml`` into frozen dataclasses with no silent defaults for
     anything safety-relevant;
  2. refuse to start on a configuration that is syntactically valid but unsafe
     -- spec NFR-09 (``async_insert=1`` with ``wait_for_async_insert=0``),
     I-11 (``auto.offset.reset`` drifting to ``latest``), and the descriptor
     placeholder gate in section 12.1;
  3. emit an *effective* configuration fingerprint with secrets redacted, so a
     test report can prove which settings a run actually used.

Unknown keys are errors, not warnings. A typo in a budget or a timeout would
otherwise silently fall back to a default and invalidate every number in a
benchmark report.
"""

from __future__ import annotations

import hashlib
import json
import os
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .errors import ConfigError, PreflightError

#: Values accepted for ``[kafka].auto_offset_reset``. ``latest``/``earliest``
#: are rejected outright for a production-like group: both silently paper over
#: an expired or absent committed offset, which is the exact data-loss mode
#: spec I-11 exists to prevent.
SAFE_OFFSET_RESET = frozenset({"error", "none"})

#: Placeholder shipped in the example config. Starting with it still in place
#: would mean running against an unpinned schema.
DESCRIPTOR_PLACEHOLDER = "REPLACE_WITH_ACTUAL_SHA256"

RUN_MODES = frozenset({"market_valid", "isolated_fault_test"})
QUARANTINE_MODES = frozenset({"stop", "durable_dlq"})

#: The market-valid destination. Named as a constant rather than read off
#: ``ClickHouseConfig.table``: these dataclasses use ``slots=True``, so the
#: class attribute is a slot descriptor and comparing a string to it silently
#: never matches -- which would have made the I-12 isolation gate a no-op.
MARKET_TABLE = "market_events_raw"


def _require(section: dict[str, Any], key: str, where: str) -> Any:
    if key not in section:
        raise ConfigError(f"missing required key [{where}].{key}")
    return section[key]


def _reject_unknown(section: dict[str, Any], known: frozenset[str], where: str) -> None:
    extra = set(section) - set(known)
    if extra:
        raise ConfigError(
            f"unknown key(s) in [{where}]: {', '.join(sorted(extra))} "
            "(a typo here would silently use a default and invalidate the run)"
        )


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    project: str
    mode: str
    run_id: str
    topic_generation: str
    source_manifest: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in RUN_MODES:
            raise ConfigError(
                f"[project].mode must be one of {sorted(RUN_MODES)}, got {self.mode!r}"
            )
        if not self.run_id:
            raise ConfigError("[project].run_id must be non-empty")
        if not self.topic_generation:
            raise ConfigError("[project].topic_generation must be non-empty")

    @property
    def is_fault_lane(self) -> bool:
        """True in the isolated fault namespace (spec I-12)."""
        return self.mode == "isolated_fault_test"


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    bootstrap_servers: str
    group_id: str
    topics: tuple[str, ...]
    enable_auto_commit: bool = False
    enable_auto_offset_store: bool = False
    auto_offset_reset: str = "error"
    allow_auto_create_topics: bool = False
    isolation_level: str = "read_committed"
    session_timeout_ms: int = 10_000
    max_poll_interval_ms: int = 300_000
    queued_max_messages_kbytes: int = 16_384
    poll_timeout_ms: int = 100
    cluster_id: str = "local-dev"

    def __post_init__(self) -> None:
        if self.enable_auto_commit:
            raise ConfigError(
                "[kafka].enable_auto_commit must be false: automatic commits can "
                "advance past records with no sink proof (spec FR-02, I-01)"
            )
        if self.enable_auto_offset_store:
            raise ConfigError(
                "[kafka].enable_auto_offset_store must be false: storing offsets "
                "on poll decouples the commit from the sink decision"
            )
        if self.auto_offset_reset not in SAFE_OFFSET_RESET:
            raise ConfigError(
                "[kafka].auto_offset_reset must be one of "
                f"{sorted(SAFE_OFFSET_RESET)}, got {self.auto_offset_reset!r}: "
                "'latest'/'earliest' silently skip or re-read data when the "
                "committed offset is gone (spec I-11)"
            )
        if self.allow_auto_create_topics:
            raise ConfigError(
                "[kafka].allow_auto_create_topics must be false: a typo in a "
                "topic name would create an empty topic and read nothing"
            )
        if not self.topics:
            raise ConfigError("[kafka].topics must list at least one topic")
        if len(set(self.topics)) != len(self.topics):
            raise ConfigError("[kafka].topics contains duplicates")
        if self.poll_timeout_ms <= 0 or self.poll_timeout_ms > 1000:
            raise ConfigError(
                "[kafka].poll_timeout_ms must be in 1..1000: a long poll timeout "
                "starves the completion drain and the commit path"
            )


@dataclass(frozen=True, slots=True)
class ContractsConfig:
    protobuf_package: str
    protobuf_descriptor_sha256: str
    max_record_value_bytes: int = 4096
    max_key_bytes: int = 32
    max_headers_total_bytes: int = 512
    max_dataset_id_bytes: int = 128
    strict_symbol_ascii: bool = True
    strict_market_validation: bool = True
    allowed_source_schema_versions: tuple[str, ...] = ()
    ticks_topic: str = "md.ticks.v1"
    quotes_topic: str = "md.quotes.v1"

    def __post_init__(self) -> None:
        if self.protobuf_descriptor_sha256 == DESCRIPTOR_PLACEHOLDER:
            raise ConfigError(
                "[contracts].protobuf_descriptor_sha256 is still the placeholder; "
                "pin the real descriptor hash before starting (spec section 12.1)"
            )
        if len(self.protobuf_descriptor_sha256) != 64:
            raise ConfigError(
                "[contracts].protobuf_descriptor_sha256 must be 64 hex chars"
            )
        try:
            int(self.protobuf_descriptor_sha256, 16)
        except ValueError as exc:
            raise ConfigError(
                "[contracts].protobuf_descriptor_sha256 is not hexadecimal"
            ) from exc
        for name in ("max_record_value_bytes", "max_key_bytes", "max_dataset_id_bytes"):
            if getattr(self, name) <= 0:
                raise ConfigError(f"[contracts].{name} must be positive")

    @property
    def topic_to_kind(self) -> dict[str, str]:
        """Topic allowlist mapped to the oneof variant it must carry."""
        return {self.ticks_topic: "tick", self.quotes_topic: "quote"}


@dataclass(frozen=True, slots=True)
class BatchConfig:
    per_partition_max_rows: int = 5000
    per_partition_max_serialized_bytes: int = 4 * 1024 * 1024
    max_wait_ms: int = 2000
    max_inflight_batches_per_partition: int = 1
    max_inflight_batches_global: int = 4
    app_buffer_budget_bytes: int = 64 * 1024 * 1024
    pause_high_watermark: float = 0.80
    resume_low_watermark: float = 0.50

    def __post_init__(self) -> None:
        if self.per_partition_max_rows <= 0:
            raise ConfigError("[batch].per_partition_max_rows must be positive")
        if self.max_wait_ms <= 0:
            raise ConfigError("[batch].max_wait_ms must be positive")
        if self.max_inflight_batches_per_partition != 1:
            raise ConfigError(
                "[batch].max_inflight_batches_per_partition must be 1 in this "
                "MVP: more than one in-flight batch per partition invalidates "
                "the contiguous-frontier argument (spec ADR-003)"
            )
        if self.max_inflight_batches_global < 1:
            raise ConfigError("[batch].max_inflight_batches_global must be >= 1")
        if not 0.0 < self.resume_low_watermark < self.pause_high_watermark < 1.0:
            raise ConfigError(
                "[batch] watermarks must satisfy "
                "0 < resume_low_watermark < pause_high_watermark < 1 "
                "(equal watermarks cause pause/resume flapping)"
            )
        if self.per_partition_max_serialized_bytes > self.app_buffer_budget_bytes:
            raise ConfigError(
                "[batch].per_partition_max_serialized_bytes exceeds "
                "app_buffer_budget_bytes: a single partition could never seal"
            )


@dataclass(frozen=True, slots=True)
class ClickHouseConfig:
    host: str = "localhost"
    port: int = 8123
    database: str = "md_ingest"
    table: str = MARKET_TABLE
    user_env: str = "P05_CH_USER"
    password_env: str = "P05_CH_PASSWORD"
    secure: bool = False
    connect_timeout_s: int = 3
    insert_timeout_s: int = 10
    async_insert: bool = False
    wait_for_async_insert: bool = True
    deduplicate_insert: str = "enable"
    schema_version: str = "raw-v1"

    def __post_init__(self) -> None:
        if self.async_insert and not self.wait_for_async_insert:
            raise ConfigError(
                "[clickhouse] async_insert=true with wait_for_async_insert=false "
                "is forbidden: the ACK would only prove server-side buffering, so "
                "committing on it can lose data (spec NFR-09, section 6.4)"
            )
        if self.insert_timeout_s <= 0:
            raise ConfigError("[clickhouse].insert_timeout_s must be positive")

    @property
    def table_fqn(self) -> str:
        return f"{self.database}.{self.table}"

    @property
    def insert_mode(self) -> str:
        """Part of the insert token: de-duplication must not span modes."""
        if not self.async_insert:
            return "sync"
        return "async_wait" if self.wait_for_async_insert else "async_nowait"

    def credentials(self) -> tuple[str, str]:
        """Read credentials from the environment; never from the TOML file."""
        user = os.environ.get(self.user_env, "")
        password = os.environ.get(self.password_env, "")
        if self.secure and not user:
            raise PreflightError(
                f"secure=true requires a username in ${self.user_env}"
            )
        return user or "default", password


@dataclass(frozen=True, slots=True)
class RetryConfig:
    max_attempts: int = 5
    initial_backoff_ms: int = 100
    max_backoff_ms: int = 3000
    overall_deadline_s: float = 30.0
    jitter_seed: int = 1337

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ConfigError("[retry].max_attempts must be >= 1")
        if self.initial_backoff_ms <= 0 or self.max_backoff_ms < self.initial_backoff_ms:
            raise ConfigError(
                "[retry] backoff must satisfy 0 < initial_backoff_ms <= max_backoff_ms"
            )
        if self.overall_deadline_s <= 0:
            raise ConfigError(
                "[retry].overall_deadline_s must be positive: unbounded retry "
                "outlives the ClickHouse de-duplication window (spec section 5.2)"
            )


@dataclass(frozen=True, slots=True)
class QuarantineConfig:
    mode: str = "stop"
    topic: str | None = None
    require_ack: bool = True

    def __post_init__(self) -> None:
        if self.mode not in QUARANTINE_MODES:
            raise ConfigError(
                f"[quarantine].mode must be one of {sorted(QUARANTINE_MODES)}"
            )
        if self.mode == "durable_dlq":
            if not self.topic:
                raise ConfigError(
                    "[quarantine].topic is required when mode='durable_dlq'"
                )
            if not self.require_ack:
                raise ConfigError(
                    "[quarantine].require_ack cannot be false: skipping a poison "
                    "record without a durable DLQ ACK is a silent drop (spec I-07)"
                )


@dataclass(frozen=True, slots=True)
class ShutdownConfig:
    drain_timeout_s: float = 20.0
    hard_exit_on_incomplete: bool = True


@dataclass(frozen=True, slots=True)
class TestsConfig:
    fault_plan: str | None = None
    require_independent_reconciliation_for_go: bool = True


@dataclass(frozen=True, slots=True)
class AppConfig:
    project: ProjectConfig
    kafka: KafkaConfig
    contracts: ContractsConfig
    batch: BatchConfig
    clickhouse: ClickHouseConfig
    retry: RetryConfig
    quarantine: QuarantineConfig
    shutdown: ShutdownConfig
    tests: TestsConfig
    source_path: str | None = None

    def __post_init__(self) -> None:
        self._check_topic_allowlist()
        self._check_fault_lane_isolation()
        self._check_poll_interval_headroom()

    def _check_topic_allowlist(self) -> None:
        allowed = set(self.contracts.topic_to_kind)
        unknown = set(self.kafka.topics) - allowed
        if unknown:
            raise ConfigError(
                f"[kafka].topics contains topic(s) outside the contract "
                f"allowlist: {', '.join(sorted(unknown))} (spec FR-01)"
            )

    def _check_fault_lane_isolation(self) -> None:
        """Spec I-12: verified-fault data must not reach the market table."""
        if self.project.is_fault_lane:
            if self.clickhouse.table == MARKET_TABLE:
                raise ConfigError(
                    "mode='isolated_fault_test' must not write to the market "
                    f"table {MARKET_TABLE!r}: deliberately anomalous events "
                    "would pollute analytics (spec I-12)"
                )
        elif self.quarantine.mode == "durable_dlq":
            raise ConfigError(
                "[quarantine].mode='durable_dlq' is only permitted in an "
                "explicitly isolated profile (mode='isolated_fault_test')"
            )

    def _check_poll_interval_headroom(self) -> None:
        """Spec NFR-08: keep >= 3x margin between a stall and eviction.

        The worst case gap between two polls is one full insert deadline, since
        the owner may be waiting on a worker slot. If that is close to
        ``max.poll.interval.ms`` the group will rebalance under mere slowness.
        """
        worst_stall_ms = (self.retry.overall_deadline_s * 1000) + (
            self.clickhouse.insert_timeout_s * 1000
        )
        if worst_stall_ms * 3 > self.kafka.max_poll_interval_ms:
            raise ConfigError(
                "[kafka].max_poll_interval_ms="
                f"{self.kafka.max_poll_interval_ms} leaves less than 3x margin "
                f"over the worst-case sink stall of {worst_stall_ms:.0f} ms "
                "(retry deadline + insert timeout); the group would rebalance "
                "on slowness alone (spec NFR-08)"
            )

    # ---------------------------------------------------------------- loading

    @classmethod
    def from_toml(cls, path: str | Path) -> AppConfig:
        p = Path(path)
        try:
            raw = tomllib.loads(p.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ConfigError(f"config file not found: {p}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"invalid TOML in {p}: {exc}") from exc
        return cls.from_dict(raw, source_path=str(p))

    @classmethod
    def from_dict(cls, raw: dict[str, Any], *, source_path: str | None = None) -> AppConfig:
        _reject_unknown(
            raw,
            frozenset(
                {
                    "project",
                    "kafka",
                    "contracts",
                    "batch",
                    "clickhouse",
                    "retry",
                    "quarantine",
                    "shutdown",
                    "tests",
                }
            ),
            "root",
        )
        sections = {
            "project": (ProjectConfig, True),
            "kafka": (KafkaConfig, True),
            "contracts": (ContractsConfig, True),
            "batch": (BatchConfig, False),
            "clickhouse": (ClickHouseConfig, False),
            "retry": (RetryConfig, False),
            "quarantine": (QuarantineConfig, False),
            "shutdown": (ShutdownConfig, False),
            "tests": (TestsConfig, False),
        }
        built: dict[str, Any] = {}
        for name, (klass, required) in sections.items():
            body = raw.get(name)
            if body is None:
                if required:
                    raise ConfigError(f"missing required section [{name}]")
                built[name] = klass()
                continue
            if not isinstance(body, dict):
                raise ConfigError(f"[{name}] must be a table")
            known = frozenset(f.name for f in _fields_of(klass))
            _reject_unknown(body, known, name)
            built[name] = klass(**_coerce_tuples(klass, body))
        return cls(**built, source_path=source_path)

    # ------------------------------------------------------------ fingerprint

    def effective_dict(self, *, redact: bool = True) -> dict[str, Any]:
        """Serializable view of the effective config.

        Secret *values* never live in the config object -- only the names of
        the environment variables holding them -- so redaction only has to
        normalise those names away so that the fingerprint is stable across
        machines that use different variable names for the same role.
        """
        data = {
            "project": asdict(self.project),
            "kafka": asdict(self.kafka),
            "contracts": asdict(self.contracts),
            "batch": asdict(self.batch),
            "clickhouse": asdict(self.clickhouse),
            "retry": asdict(self.retry),
            "quarantine": asdict(self.quarantine),
            "shutdown": asdict(self.shutdown),
            "tests": asdict(self.tests),
        }
        if redact:
            data["clickhouse"]["user_env"] = "<redacted-env-name>"
            data["clickhouse"]["password_env"] = "<redacted-env-name>"
        return data

    def fingerprint(self) -> str:
        """SHA-256 over the canonically ordered, redacted effective config."""
        blob = json.dumps(
            self.effective_dict(redact=True),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()


def _fields_of(klass: type) -> tuple[Any, ...]:
    return tuple(klass.__dataclass_fields__.values())


def _coerce_tuples(klass: type, body: dict[str, Any]) -> dict[str, Any]:
    """TOML arrays decode to lists; frozen dataclasses want hashable tuples."""
    out = dict(body)
    for f in _fields_of(klass):
        if f.name in out and isinstance(out[f.name], list):
            out[f.name] = tuple(out[f.name])
    return out
