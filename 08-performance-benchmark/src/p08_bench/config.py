"""Workload configuration: strict parsing and a hash (sections 3.1, 21.1).

The parser rejects both unknown and missing keys. That is not pedantry: section
3.1 requires the manifest to be fixed *before* a run and section 4.4 requires
A/B pairs to differ in exactly one variable, and neither is checkable if a
config file can silently fall back to defaults. A config here is always a
complete, auditable statement of the run -- one file, one resolved config, one
digest, no include mechanism.

The config also answers questions about what a run is *allowed to claim*, which
keeps those judgements out of the reporting code:

* :attr:`QuerySection.supports_latency_claim` -- a lane with ``query.enabled =
  false`` cannot produce a p95, so section 21.1's note becomes a property.
* :attr:`KafkaSection.durability_contract_intact` -- case E2E-47 is someone
  swapping ``acks=all`` for ``acks=0`` and reporting the resulting speedup, so
  the durability contract is part of the config surface.
* :attr:`LoadSection.meets_steady_protocol` -- NFR-05 proposes 30 minutes; a
  shorter run is allowed and simply cannot call itself a capacity claim
  (case E2E-43).
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .errors import ConfigError
from .manifest import digest_of
from .percentiles import MIN_COMPLETED_SAMPLES_FOR_P95

#: NFR-05: 30 minutes of steady state is the proposed protocol for a capacity
#: claim. Proposed, not universal -- the config records what it actually ran.
PROTOCOL_STEADY_SECONDS = 1800

#: NFR-02: the CV-reference lane uses 8 partitions *per topic*.
CV_REFERENCE_PARTITIONS_PER_TOPIC = 8


class ArrivalModel(str, Enum):
    """The four arrival modes of section 3.1."""

    OPEN_LOOP = "open_loop"
    OPEN_LOOP_BURST = "open_loop_burst"
    CLOSED_LOOP = "closed_loop"
    FINITE_REPLAY = "finite_replay"

    @property
    def valid_for_primary_latency(self) -> bool:
        """ADR-01: open-loop is primary; closed-loop is complementary only."""
        return self in (ArrivalModel.OPEN_LOOP, ArrivalModel.OPEN_LOOP_BURST, ArrivalModel.FINITE_REPLAY)


class Profile(str, Enum):
    """Deployment profiles of section 2.1. Results from two profiles are never
    one experiment (ADR-10)."""

    LOCAL_SMALL = "LOCAL_SMALL"
    LAB_CAPACITY = "LAB_CAPACITY"
    LAB_RECOVERY = "LAB_RECOVERY"
    QUERY_ONLY = "QUERY_ONLY"
    MIXED = "MIXED"


class SymbolDistribution(str, Enum):
    UNIFORM = "uniform"
    SKEW_80_20 = "skew_80_20"
    HOT_SYMBOL_95 = "hot_symbol_95"


class CacheProfile(str, Enum):
    WARM = "warm"
    COLD = "cold"


def _section(raw: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = raw.get(name)
    if value is None:
        raise ConfigError(f"missing [{name}] section")
    if not isinstance(value, Mapping):
        raise ConfigError(f"[{name}] must be a table", section=name)
    return value


def _strict(section: Mapping[str, Any], name: str, expected: set[str]) -> None:
    """Reject unknown and missing keys, naming both sets.

    A typo in a key would otherwise leave the intended setting at its default
    and the run would quietly measure something else.
    """
    present = set(section)
    unknown = sorted(present - expected)
    missing = sorted(expected - present)
    problems = []
    if unknown:
        problems.append(f"unknown keys: {unknown}")
    if missing:
        problems.append(f"missing keys: {missing}")
    if problems:
        raise ConfigError(f"[{name}] is not a complete config: " + "; ".join(problems), section=name)


def _typed(section: Mapping[str, Any], name: str, key: str, kind: type) -> Any:
    value = section[key]
    # bool is a subclass of int, so an int field would happily accept `true`.
    if kind is int and isinstance(value, bool):
        raise ConfigError(f"{name}.{key} must be an integer, not a boolean", value=value)
    if not isinstance(value, kind):
        raise ConfigError(
            f"{name}.{key} must be {kind.__name__}, got {type(value).__name__}",
            value=value,
        )
    return value


def _enum(section: Mapping[str, Any], name: str, key: str, enum: type[Enum]) -> Any:
    raw = section[key]
    try:
        return enum(raw)
    except ValueError as exc:
        allowed = sorted(member.value for member in enum)
        raise ConfigError(
            f"{name}.{key} must be one of {allowed}", value=raw
        ) from exc


def _positive(value: int | float, name: str, key: str) -> Any:
    if value <= 0:
        raise ConfigError(f"{name}.{key} must be positive", value=value)
    return value


def _non_negative(value: int | float, name: str, key: str) -> Any:
    if value < 0:
        raise ConfigError(f"{name}.{key} may not be negative", value=value)
    return value


@dataclass(frozen=True)
class RunSection:
    id: str
    seed: int
    manifest_version: str
    profile: Profile
    expected_oracle: str
    lane: str

    KEYS = {"id", "seed", "manifest_version", "profile", "expected_oracle", "lane"}

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "RunSection":
        _strict(raw, "run", cls.KEYS)
        run_id = _typed(raw, "run", "id", str)
        if not run_id.strip():
            raise ConfigError("run.id may not be empty")
        return cls(
            id=run_id,
            seed=_typed(raw, "run", "seed", int),
            manifest_version=_typed(raw, "run", "manifest_version", str),
            profile=_enum(raw, "run", "profile", Profile),
            expected_oracle=_typed(raw, "run", "expected_oracle", str),
            lane=_typed(raw, "run", "lane", str),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "seed": self.seed,
            "manifest_version": self.manifest_version,
            "profile": self.profile.value,
            "expected_oracle": self.expected_oracle,
            "lane": self.lane,
        }


@dataclass(frozen=True)
class DatasetSection:
    dataset_id: str
    schema: str
    symbol_count: int
    symbol_distribution: SymbolDistribution
    tick_to_quote_ratio: str
    source_seal_required: bool

    KEYS = {
        "dataset_id", "schema", "symbol_count", "symbol_distribution",
        "tick_to_quote_ratio", "source_seal_required",
    }

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "DatasetSection":
        _strict(raw, "dataset", cls.KEYS)
        ratio = _typed(raw, "dataset", "tick_to_quote_ratio", str)
        cls._parse_ratio(ratio)
        return cls(
            dataset_id=_typed(raw, "dataset", "dataset_id", str),
            schema=_typed(raw, "dataset", "schema", str),
            symbol_count=_positive(_typed(raw, "dataset", "symbol_count", int), "dataset", "symbol_count"),
            symbol_distribution=_enum(raw, "dataset", "symbol_distribution", SymbolDistribution),
            tick_to_quote_ratio=ratio,
            source_seal_required=_typed(raw, "dataset", "source_seal_required", bool),
        )

    @staticmethod
    def _parse_ratio(ratio: str) -> tuple[int, int]:
        parts = ratio.split(":")
        if len(parts) != 2:
            raise ConfigError("dataset.tick_to_quote_ratio must look like '4:1'", value=ratio)
        try:
            ticks, quotes = int(parts[0]), int(parts[1])
        except ValueError as exc:
            raise ConfigError("dataset.tick_to_quote_ratio parts must be integers", value=ratio) from exc
        if ticks <= 0 or quotes < 0:
            raise ConfigError("dataset.tick_to_quote_ratio is not usable", value=ratio)
        return ticks, quotes

    @property
    def quotes_per_tick(self) -> float:
        """The measured mix, for :meth:`Quantity.to_events_per_s`.

        Declared in the manifest before the run so the ticks/s to events/s
        conversion of section 1.2 uses a stated ratio rather than an assumed one.
        """
        ticks, quotes = self._parse_ratio(self.tick_to_quote_ratio)
        return quotes / ticks

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "schema": self.schema,
            "symbol_count": self.symbol_count,
            "symbol_distribution": self.symbol_distribution.value,
            "tick_to_quote_ratio": self.tick_to_quote_ratio,
            "quotes_per_tick": self.quotes_per_tick,
            "source_seal_required": self.source_seal_required,
        }


@dataclass(frozen=True)
class KafkaSection:
    tick_topic: str
    quote_topic: str
    partitions_per_topic: int
    producer_acks: str
    producer_idempotent: bool
    topic_generation: str

    KEYS = {
        "tick_topic", "quote_topic", "partitions_per_topic", "producer_acks",
        "producer_idempotent", "topic_generation",
    }
    ALLOWED_ACKS = ("all", "1", "0")

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "KafkaSection":
        _strict(raw, "kafka", cls.KEYS)
        acks = _typed(raw, "kafka", "producer_acks", str)
        if acks not in cls.ALLOWED_ACKS:
            raise ConfigError(
                f"kafka.producer_acks must be one of {list(cls.ALLOWED_ACKS)}", value=acks
            )
        tick = _typed(raw, "kafka", "tick_topic", str)
        quote = _typed(raw, "kafka", "quote_topic", str)
        if tick == quote:
            raise ConfigError("tick and quote topics must differ (P04 contract)", value=tick)
        return cls(
            tick_topic=tick,
            quote_topic=quote,
            partitions_per_topic=_positive(
                _typed(raw, "kafka", "partitions_per_topic", int), "kafka", "partitions_per_topic"
            ),
            producer_acks=acks,
            producer_idempotent=_typed(raw, "kafka", "producer_idempotent", bool),
            topic_generation=_typed(raw, "kafka", "topic_generation", str),
        )

    @property
    def durability_contract_intact(self) -> bool:
        """``acks=all`` with an idempotent producer, per the P04 contract.

        Case E2E-47: dropping to ``acks=0`` makes the pipeline faster and the
        durability claim false, so an optimization that flips this cannot be
        released on throughput grounds.
        """
        return self.producer_acks == "all" and self.producer_idempotent

    @property
    def cv_reference_topology(self) -> bool:
        """Whether this is the 8-partitions-per-topic topology of NFR-02.

        A different topology is allowed; it just has to be labelled and judged
        separately (ADR-03), so this is a property rather than a validation.
        """
        return self.partitions_per_topic == CV_REFERENCE_PARTITIONS_PER_TOPIC

    def as_dict(self) -> dict[str, Any]:
        return {
            "tick_topic": self.tick_topic,
            "quote_topic": self.quote_topic,
            "partitions_per_topic": self.partitions_per_topic,
            "producer_acks": self.producer_acks,
            "producer_idempotent": self.producer_idempotent,
            "topic_generation": self.topic_generation,
            "durability_contract_intact": self.durability_contract_intact,
            "cv_reference_topology": self.cv_reference_topology,
        }


@dataclass(frozen=True)
class LoadSection:
    model: ArrivalModel
    market_tick_target_per_s: int
    quote_target_per_s: int
    ramp_up_seconds: int
    warmup_seconds: int
    steady_seconds: int
    drain_seconds: int
    max_late_issue_ms: int

    KEYS = {
        "model", "market_tick_target_per_s", "quote_target_per_s", "ramp_up_seconds",
        "warmup_seconds", "steady_seconds", "drain_seconds", "max_late_issue_ms",
    }

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "LoadSection":
        _strict(raw, "load", cls.KEYS)
        ticks = _non_negative(
            _typed(raw, "load", "market_tick_target_per_s", int), "load", "market_tick_target_per_s"
        )
        quotes = _non_negative(
            _typed(raw, "load", "quote_target_per_s", int), "load", "quote_target_per_s"
        )
        if ticks == 0 and quotes == 0:
            raise ConfigError("load targets are both zero; nothing would be issued")
        return cls(
            model=_enum(raw, "load", "model", ArrivalModel),
            market_tick_target_per_s=ticks,
            quote_target_per_s=quotes,
            ramp_up_seconds=_non_negative(_typed(raw, "load", "ramp_up_seconds", int), "load", "ramp_up_seconds"),
            warmup_seconds=_non_negative(_typed(raw, "load", "warmup_seconds", int), "load", "warmup_seconds"),
            steady_seconds=_positive(_typed(raw, "load", "steady_seconds", int), "load", "steady_seconds"),
            drain_seconds=_non_negative(_typed(raw, "load", "drain_seconds", int), "load", "drain_seconds"),
            max_late_issue_ms=_positive(
                _typed(raw, "load", "max_late_issue_ms", int), "load", "max_late_issue_ms"
            ),
        )

    @property
    def total_event_target_per_s(self) -> int:
        """Ticks plus quotes. A different number from the tick target (INV-12)."""
        return self.market_tick_target_per_s + self.quote_target_per_s

    @property
    def meets_steady_protocol(self) -> bool:
        """Whether the steady phase is long enough for NFR-05's protocol.

        A one-minute run labelled as thirty is case E2E-43; keeping this as a
        property means the label and the duration cannot disagree silently.
        """
        return self.steady_seconds >= PROTOCOL_STEADY_SECONDS

    def as_dict(self) -> dict[str, Any]:
        return {
            "model": self.model.value,
            "market_tick_target_per_s": self.market_tick_target_per_s,
            "quote_target_per_s": self.quote_target_per_s,
            "total_event_target_per_s": self.total_event_target_per_s,
            "ramp_up_seconds": self.ramp_up_seconds,
            "warmup_seconds": self.warmup_seconds,
            "steady_seconds": self.steady_seconds,
            "drain_seconds": self.drain_seconds,
            "max_late_issue_ms": self.max_late_issue_ms,
            "meets_steady_protocol": self.meets_steady_protocol,
            "valid_for_primary_latency": self.model.valid_for_primary_latency,
        }


@dataclass(frozen=True)
class QuerySection:
    enabled: bool
    family: str
    concurrency: int
    min_complete_samples: int
    client_p95_budget_ms: int
    cache_profile: CacheProfile
    deadline_ms: int

    KEYS = {
        "enabled", "family", "concurrency", "min_complete_samples",
        "client_p95_budget_ms", "cache_profile", "deadline_ms",
    }

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "QuerySection":
        _strict(raw, "query", cls.KEYS)
        return cls(
            enabled=_typed(raw, "query", "enabled", bool),
            family=_typed(raw, "query", "family", str),
            concurrency=_positive(_typed(raw, "query", "concurrency", int), "query", "concurrency"),
            min_complete_samples=_positive(
                _typed(raw, "query", "min_complete_samples", int), "query", "min_complete_samples"
            ),
            client_p95_budget_ms=_positive(
                _typed(raw, "query", "client_p95_budget_ms", int), "query", "client_p95_budget_ms"
            ),
            cache_profile=_enum(raw, "query", "cache_profile", CacheProfile),
            deadline_ms=_positive(_typed(raw, "query", "deadline_ms", int), "query", "deadline_ms"),
        )

    @property
    def supports_latency_claim(self) -> bool:
        """Section 21.1: ``enabled = false`` means this lane proves no p95."""
        return self.enabled

    @property
    def sample_floor_is_protocol_grade(self) -> bool:
        """Whether the sample floor reaches NFR-08's 10k completed requests."""
        return self.min_complete_samples >= MIN_COMPLETED_SAMPLES_FOR_P95

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "family": self.family,
            "concurrency": self.concurrency,
            "min_complete_samples": self.min_complete_samples,
            "client_p95_budget_ms": self.client_p95_budget_ms,
            "cache_profile": self.cache_profile.value,
            "deadline_ms": self.deadline_ms,
            "supports_latency_claim": self.supports_latency_claim,
            "sample_floor_is_protocol_grade": self.sample_floor_is_protocol_grade,
        }


@dataclass(frozen=True)
class EvidenceSection:
    require_ready_snapshot: bool
    require_identity_diff: bool
    require_semantic_match: bool
    require_fault_receipt_if_chaos: bool
    fail_on_missing_metrics: bool

    KEYS = {
        "require_ready_snapshot", "require_identity_diff", "require_semantic_match",
        "require_fault_receipt_if_chaos", "fail_on_missing_metrics",
    }

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "EvidenceSection":
        _strict(raw, "evidence", cls.KEYS)
        return cls(**{key: _typed(raw, "evidence", key, bool) for key in sorted(cls.KEYS)})

    def as_dict(self) -> dict[str, Any]:
        return {
            "require_ready_snapshot": self.require_ready_snapshot,
            "require_identity_diff": self.require_identity_diff,
            "require_semantic_match": self.require_semantic_match,
            "require_fault_receipt_if_chaos": self.require_fault_receipt_if_chaos,
            "fail_on_missing_metrics": self.fail_on_missing_metrics,
        }


@dataclass(frozen=True)
class SafetySection:
    lab_only: bool
    dry_run_destructive_actions: bool
    max_test_volume_fill_percent: int
    allow_host_root_path: bool

    KEYS = {
        "lab_only", "dry_run_destructive_actions", "max_test_volume_fill_percent",
        "allow_host_root_path",
    }

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "SafetySection":
        _strict(raw, "safety", cls.KEYS)
        fill = _typed(raw, "safety", "max_test_volume_fill_percent", int)
        if not (1 <= fill <= 99):
            raise ConfigError(
                "safety.max_test_volume_fill_percent must leave headroom (1-99)", value=fill
            )
        return cls(
            lab_only=_typed(raw, "safety", "lab_only", bool),
            dry_run_destructive_actions=_typed(raw, "safety", "dry_run_destructive_actions", bool),
            max_test_volume_fill_percent=fill,
            allow_host_root_path=_typed(raw, "safety", "allow_host_root_path", bool),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "lab_only": self.lab_only,
            "dry_run_destructive_actions": self.dry_run_destructive_actions,
            "max_test_volume_fill_percent": self.max_test_volume_fill_percent,
            "allow_host_root_path": self.allow_host_root_path,
        }


@dataclass(frozen=True)
class WorkloadConfig:
    """One resolved workload. The digest is over the resolved values."""

    run: RunSection
    dataset: DatasetSection
    kafka: KafkaSection
    load: LoadSection
    query: QuerySection
    evidence: EvidenceSection
    safety: SafetySection
    source_path: str | None = None
    SECTIONS = ("run", "dataset", "kafka", "load", "query", "evidence", "safety")

    @property
    def sha256(self) -> str:
        return digest_of(self.resolved())

    def resolved(self) -> dict[str, Any]:
        """The canonical form the digest is taken over.

        Derived properties are excluded: they are functions of the inputs, and
        including them would make the digest change when a derivation changes
        rather than when the configuration does.
        """
        return {
            "run": self.run.as_dict(),
            "dataset": {
                k: v for k, v in self.dataset.as_dict().items() if k != "quotes_per_tick"
            },
            "kafka": {
                k: v for k, v in self.kafka.as_dict().items()
                if k not in ("durability_contract_intact", "cv_reference_topology")
            },
            "load": {
                k: v for k, v in self.load.as_dict().items()
                if k not in ("total_event_target_per_s", "meets_steady_protocol", "valid_for_primary_latency")
            },
            "query": {
                k: v for k, v in self.query.as_dict().items()
                if k not in ("supports_latency_claim", "sample_floor_is_protocol_grade")
            },
            "evidence": self.evidence.as_dict(),
            "safety": self.safety.as_dict(),
        }

    def claim_limitations(self) -> list[str]:
        """What this configuration, by construction, cannot prove.

        Collected here so a report never has to infer it, and so a reviewer can
        see the gap between the lane as configured and the claim it is cited for.
        """
        limits: list[str] = []
        if not self.query.supports_latency_claim:
            limits.append("query lane disabled: this run proves no query p95")
        elif not self.query.sample_floor_is_protocol_grade:
            limits.append(
                f"min_complete_samples={self.query.min_complete_samples} is below the "
                f"{MIN_COMPLETED_SAMPLES_FOR_P95}-sample guardrail; p95 is discovery-grade"
            )
        if not self.load.meets_steady_protocol:
            limits.append(
                f"steady_seconds={self.load.steady_seconds} is below the "
                f"{PROTOCOL_STEADY_SECONDS}s protocol; not a sustainable-capacity claim"
            )
        if not self.kafka.cv_reference_topology:
            limits.append(
                f"partitions_per_topic={self.kafka.partitions_per_topic} is not the "
                f"{CV_REFERENCE_PARTITIONS_PER_TOPIC}-per-topic reference topology"
            )
        if not self.kafka.durability_contract_intact:
            limits.append(
                f"producer_acks={self.kafka.producer_acks!r} with "
                f"idempotent={self.kafka.producer_idempotent}: durability contract "
                "is weaker than P04, so throughput is not comparable to a durable run"
            )
        if not self.load.model.valid_for_primary_latency:
            limits.append(
                f"arrival model {self.load.model.value} is complementary only (ADR-01); "
                "it cannot provide the primary latency distribution"
            )
        if self.load.quote_target_per_s and self.run.lane == "L07":
            limits.append(
                "L07 is the ticks-only rate-identity lane; quotes here make the "
                "ticks/s label ambiguous"
            )
        return limits

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_path": self.source_path,
            "config_sha256": self.sha256,
            "claim_limitations": self.claim_limitations(),
            "run": self.run.as_dict(),
            "dataset": self.dataset.as_dict(),
            "kafka": self.kafka.as_dict(),
            "load": self.load.as_dict(),
            "query": self.query.as_dict(),
            "evidence": self.evidence.as_dict(),
            "safety": self.safety.as_dict(),
        }


def build_config(raw: Mapping[str, Any], *, source_path: str | None = None) -> WorkloadConfig:
    """Validate a mapping into a :class:`WorkloadConfig`."""
    unknown = sorted(set(raw) - set(WorkloadConfig.SECTIONS))
    if unknown:
        raise ConfigError("unknown top-level sections", sections=unknown)
    return WorkloadConfig(
        run=RunSection.parse(_section(raw, "run")),
        dataset=DatasetSection.parse(_section(raw, "dataset")),
        kafka=KafkaSection.parse(_section(raw, "kafka")),
        load=LoadSection.parse(_section(raw, "load")),
        query=QuerySection.parse(_section(raw, "query")),
        evidence=EvidenceSection.parse(_section(raw, "evidence")),
        safety=SafetySection.parse(_section(raw, "safety")),
        source_path=source_path,
    )


def load_config(path: Path) -> WorkloadConfig:
    """Read and validate a workload TOML file."""
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError("config file not found", path=str(path)) from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config is not valid TOML: {exc}", path=str(path)) from exc
    return build_config(raw, source_path=str(path))
