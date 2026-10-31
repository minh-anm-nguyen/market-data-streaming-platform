"""Configuration, validated strictly (section 12.1).

Unknown keys are rejected rather than ignored. A config file that silently
tolerates ``max_payload_byte = 4096`` (singular, a typo) would run with the
default 4096 and look fine -- until the day the intended value mattered. One
file, fully validated, is auditable; a file with ignored keys is not.

The checks below are the ones listed in section 12.1, and they are refusals
rather than warnings: a non-loopback bind without explicit approval, an infinite
deadline, a queue budget of zero, ``fault_passthrough`` without a manifest
requirement. Each of those is a configuration that would weaken a guarantee the
design states, so the right time to fail is at load.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Final

from .errors import ConfigError
from .validation import ValidationMode

#: Loopback only unless explicitly overridden. MDP1 carries no authentication
#: and no transport encryption, so binding it to a routable address would expose
#: an unauthenticated feed (case J09, section 7).
LOOPBACK_HOSTS: Final = frozenset({"127.0.0.1", "::1", "localhost"})


def _check_unknown(section: str, provided: dict[str, Any], known: frozenset[str]) -> None:
    unknown = sorted(set(provided) - known)
    if unknown:
        raise ConfigError(f"[{section}] has unknown keys: {unknown}", section=section)


def _positive(section: str, name: str, value: Any, *, maximum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigError(f"[{section}].{name} must be a positive integer, got {value!r}")
    if maximum is not None and value > maximum:
        raise ConfigError(f"[{section}].{name} must be <= {maximum}, got {value}")
    return value


@dataclass(frozen=True)
class ContractConfig:
    proto_package: str = "marketdata.v1"
    source_schema_version: str = "sim.v1"
    frame_protocol: str = "MDP1"
    max_json_line_bytes: int = 4096
    max_payload_bytes: int = 4096
    max_symbol_bytes: int = 32
    max_dataset_id_bytes: int = 128
    max_source_version_bytes: int = 32


@dataclass(frozen=True)
class ValidationConfig:
    mode: ValidationMode = ValidationMode.STRICT_NORMAL
    fail_on_unknown_json_keys: bool = True
    require_all_core_fields: bool = True
    allow_only_manifest_faults: bool = True


@dataclass(frozen=True)
class StreamConfig:
    host: str = "127.0.0.1"
    port: int = 9010
    max_clients: int = 1
    max_queue_events: int = 2048
    max_queue_bytes: int = 8 * 1024 * 1024
    prefix_header_deadline_ms: int = 2000
    payload_deadline_ms: int = 2000
    on_queue_budget_exceeded: str = "fail_connection"
    allow_public_bind: bool = False


@dataclass(frozen=True)
class ArtifactsConfig:
    require_completed_source_manifest: bool = True
    strict_pbstream_verification: bool = True
    hash_algorithm: str = "sha256"


@dataclass(frozen=True)
class BenchmarkConfig:
    target_events_per_sec: tuple[int, ...] = (1000, 10000, 50000, 120000)
    snapshot_interval_s: float = 1.0
    record_payload_size_histogram: bool = True
    duration_s: float = 2.0


@dataclass(frozen=True)
class Config:
    contract: ContractConfig = field(default_factory=ContractConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)
    artifacts: ArtifactsConfig = field(default_factory=ArtifactsConfig)
    benchmark: BenchmarkConfig = field(default_factory=BenchmarkConfig)
    source_path: Path | None = None

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        path = Path(path)
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ConfigError(f"config not found: {path}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"{path} is not valid TOML: {exc}") from exc
        return cls.from_dict(raw, source_path=path)

    @classmethod
    def from_dict(cls, raw: dict[str, Any], *, source_path: Path | None = None) -> "Config":
        _check_unknown(
            "<root>",
            raw,
            frozenset({"contract", "validation", "stream", "artifacts", "benchmark"}),
        )

        contract = cls._section("contract", raw, ContractConfig)
        validation_raw = dict(raw.get("validation", {}))
        _check_unknown(
            "validation",
            validation_raw,
            frozenset(f.name for f in fields(ValidationConfig)),
        )
        mode_name = validation_raw.pop("mode", ValidationMode.STRICT_NORMAL.value)
        try:
            mode = ValidationMode(mode_name)
        except ValueError as exc:
            raise ConfigError(
                f"[validation].mode must be one of "
                f"{[m.value for m in ValidationMode]}, got {mode_name!r}"
            ) from exc
        validation = ValidationConfig(mode=mode, **validation_raw)

        stream = cls._section("stream", raw, StreamConfig)
        artifacts = cls._section("artifacts", raw, ArtifactsConfig)

        benchmark_raw = dict(raw.get("benchmark", {}))
        _check_unknown(
            "benchmark", benchmark_raw, frozenset(f.name for f in fields(BenchmarkConfig))
        )
        if "target_events_per_sec" in benchmark_raw:
            benchmark_raw["target_events_per_sec"] = tuple(
                benchmark_raw["target_events_per_sec"]
            )
        benchmark = BenchmarkConfig(**benchmark_raw)

        config = cls(
            contract=contract,
            validation=validation,
            stream=stream,
            artifacts=artifacts,
            benchmark=benchmark,
            source_path=source_path,
        )
        config.validate()
        return config

    @staticmethod
    def _section(name: str, raw: dict[str, Any], klass: type) -> Any:
        provided = dict(raw.get(name, {}))
        _check_unknown(name, provided, frozenset(f.name for f in fields(klass)))
        try:
            return klass(**provided)
        except TypeError as exc:
            raise ConfigError(f"[{name}] is invalid: {exc}") from exc

    def with_mode(self, mode: ValidationMode | str) -> "Config":
        """A copy running in a different validation mode, re-validated.

        Re-validated rather than just substituted, because some combinations are
        only invalid together -- fault_passthrough with
        allow_only_manifest_faults=false, for one.
        """
        mode = ValidationMode(mode) if isinstance(mode, str) else mode
        updated = replace(self, validation=replace(self.validation, mode=mode))
        updated.validate()
        return updated

    def validate(self) -> None:
        c, s, b = self.contract, self.stream, self.benchmark

        if c.frame_protocol != "MDP1":
            raise ConfigError(
                f"frame_protocol {c.frame_protocol!r} is not supported; an "
                "incompatible framing change must be a new protocol name on a "
                "different port, not a reconfiguration of this one"
            )
        if c.source_schema_version != "sim.v1":
            raise ConfigError(
                f"no mapper exists for source_schema_version "
                f"{c.source_schema_version!r}"
            )
        for name in (
            "max_json_line_bytes",
            "max_payload_bytes",
            "max_symbol_bytes",
            "max_dataset_id_bytes",
            "max_source_version_bytes",
        ):
            _positive("contract", name, getattr(c, name))

        # Port 0 is allowed and means "let the OS pick an ephemeral port". That
        # is what makes two instances -- or two test runs -- able to coexist
        # without fighting over 9010; MdpServer reports the resolved port back.
        if not isinstance(s.port, int) or isinstance(s.port, bool) or not 0 <= s.port <= 65535:
            raise ConfigError(f"[stream].port must be 0..65535, got {s.port!r}")
        _positive("stream", "max_clients", s.max_clients)
        _positive("stream", "max_queue_events", s.max_queue_events)
        _positive("stream", "max_queue_bytes", s.max_queue_bytes)
        # An infinite deadline is how a slowloris client holds a connection
        # forever, so zero and negative values are refused rather than treated
        # as "no limit" (cases D12, I14).
        _positive("stream", "prefix_header_deadline_ms", s.prefix_header_deadline_ms)
        _positive("stream", "payload_deadline_ms", s.payload_deadline_ms)

        if s.on_queue_budget_exceeded not in ("fail_connection", "block_with_timeout"):
            raise ConfigError(
                "[stream].on_queue_budget_exceeded must be 'fail_connection' or "
                f"'block_with_timeout', got {s.on_queue_budget_exceeded!r}"
            )

        # The byte budget has to be able to hold at least one maximum-size
        # frame, or the queue would reject every large event regardless of depth.
        min_bytes = c.max_payload_bytes + 4
        if s.max_queue_bytes < min_bytes:
            raise ConfigError(
                f"[stream].max_queue_bytes {s.max_queue_bytes} cannot hold even one "
                f"maximum frame ({min_bytes} bytes)"
            )

        if s.host not in LOOPBACK_HOSTS and not s.allow_public_bind:
            raise ConfigError(
                f"[stream].host {s.host!r} is not loopback and "
                "allow_public_bind is false; MDP1 has no authentication or "
                "transport encryption, so a non-loopback bind needs an explicit "
                "security decision"
            )

        if (
            self.validation.mode is ValidationMode.FAULT_PASSTHROUGH
            and not self.validation.allow_only_manifest_faults
        ):
            raise ConfigError(
                "fault_passthrough with allow_only_manifest_faults=false would "
                "accept any anomaly as intentional, which defeats the purpose of "
                "the mode"
            )

        if b.snapshot_interval_s <= 0:
            raise ConfigError("[benchmark].snapshot_interval_s must be positive")
        for rate in b.target_events_per_sec:
            _positive("benchmark", "target_events_per_sec", rate)

    def as_dict(self) -> dict[str, Any]:
        """Resolved config for the report; every run records what it actually ran with."""
        return {
            "contract": {f.name: getattr(self.contract, f.name) for f in fields(ContractConfig)},
            "validation": {
                **{
                    f.name: getattr(self.validation, f.name)
                    for f in fields(ValidationConfig)
                    if f.name != "mode"
                },
                "mode": self.validation.mode.value,
            },
            "stream": {f.name: getattr(self.stream, f.name) for f in fields(StreamConfig)},
            "artifacts": {
                f.name: getattr(self.artifacts, f.name) for f in fields(ArtifactsConfig)
            },
            "benchmark": {
                **{
                    f.name: getattr(self.benchmark, f.name)
                    for f in fields(BenchmarkConfig)
                    if f.name != "target_events_per_sec"
                },
                "target_events_per_sec": list(self.benchmark.target_events_per_sec),
            },
            "source_path": str(self.source_path) if self.source_path else None,
        }
