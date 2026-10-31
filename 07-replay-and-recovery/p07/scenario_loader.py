"""Load, validate and schedule a scenario file.

The loader is fail-closed in three ways design 2.1 asks for.

**Unknown keys are errors.** A typo in a scenario would otherwise disable a
check silently -- ``require_zero_conflicts`` misspelled is a test that no longer
tests anything. Every mapping is validated against an explicit key set.

**Fault types are a closed enum with a fixed application point.** A scenario
cannot name a command, a path or a host; it names one of the fault types in
:data:`FAULT_TYPES`, and the runner knows at which lifecycle stage that type is
applied. There is no hook for arbitrary code, by construction.

**Targets must be on the lab allowlist.** Checked at load time so a scenario
aimed at something outside the lab is rejected before anything starts, which is
the ``--dry-run`` guarantee design 14.2 wants.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Mapping

from .injectors.base import DEFAULT_ALLOWLIST, LabAllowlist
from .oracle.acceptance_oracle import PolicyConfig
from .yamlite import load_path

SCENARIO_VERSION: Final = "p07.scenario.v1"

#: Fault type -> the lifecycle stage at which the runner applies it. The stage
#: is part of the contract, not a hint: a retention expiry applied before the
#: cut is a different test from one applied after it.
FAULT_TYPES: Final[Mapping[str, str]] = {
    "transport_truncate": "replay",
    "transport_close": "replay",
    "transport_malformed": "replay",
    "transport_duplicate": "replay",
    "transport_bad_magic": "replay",
    "producer_transient": "pre_replay",
    "kafka_ack_loss": "pre_replay",
    "kafka_broker_stop": "pre_replay",
    "kafka_isr_degrade": "pre_replay",
    "kafka_retention_expire": "post_cut",
    "consumer_rebalance": "pre_ingest",
    "ch_ack_loss": "pre_ingest",
    "ch_fail_before_commit": "pre_ingest",
    "ch_disk_full": "pre_ingest",
    "ch_server_down": "pre_ingest",
    "ch_raw_ttl": "post_ingest",
    "ch_volume_loss": "post_ingest",
    "ingest_crash": "pre_ingest",
    "build_crash": "pre_build",
}

_SCENARIO_KEYS: Final = frozenset(
    {"version", "scenario_id", "test_case_id", "description", "profile", "source", "lab", "replay", "faults", "expect", "limits"}
)
_SOURCE_KEYS: Final = frozenset({"run_dir", "anchor_sha256", "require_fault_plan"})
_LAB_KEYS: Final = frozenset(
    {
        "partitions",
        "brokers",
        "replication_factor",
        "min_insync_replicas",
        "group",
        "batch_max_records",
        "producer_idempotent",
        "kafka_idempotent_dedupe",
        "strict_poison",
        "sequence_window",
        "event_time_stale_ns",
        "price_step_policy",
    }
)
_REPLAY_KEYS: Final = frozenset({"chunk_sizes"})
_FAULT_KEYS: Final = frozenset(
    {"type", "target", "broker_id", "topic", "partition", "new_log_start", "stage", "after_calls", "free_bytes", "count", "frame_index", "records", "before_ingested_at_ns", "confirm"}
)
_EXPECT_KEYS: Final = frozenset(
    {"verdict", "allow_physical_duplicates", "require_zero_conflicts", "require_published", "require_fault_confirmed"}
)
_LIMITS_KEYS: Final = frozenset({"max_scenario_duration_s", "evidence_max_bytes", "max_parallel_faults"})


class ScenarioError(ValueError):
    """An invalid scenario. The message names the offending key."""


def _require_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ScenarioError(f"{where} must be a mapping, got {type(value).__name__}")
    return value


def _check_keys(obj: Mapping[str, Any], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise ScenarioError(f"{where} has unknown keys {unknown}; allowed {sorted(allowed)}")


@dataclass(frozen=True, slots=True)
class FaultSpec:
    """One declared fault, with its application stage resolved."""

    type: str
    stage: str
    target: str
    options: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"type": self.type, "stage": self.stage, "target": self.target, **dict(self.options)}


@dataclass(frozen=True, slots=True)
class Expectations:
    """What the scenario asserts about its own outcome."""

    verdict: str | None = None
    allow_physical_duplicates: bool = True
    require_zero_conflicts: bool = True
    require_published: bool | None = None
    require_fault_confirmed: bool = True


@dataclass(frozen=True, slots=True)
class Scenario:
    """A validated scenario, ready for the runner."""

    scenario_id: str
    test_case_id: str
    description: str
    profile: str
    source_run_dir: str
    source_anchor: str | None
    require_fault_plan: bool
    lab_overrides: Mapping[str, Any]
    policy: PolicyConfig
    chunk_sizes: tuple[int, ...]
    faults: tuple[FaultSpec, ...]
    expect: Expectations
    max_duration_s: int
    evidence_max_bytes: int

    def faults_at(self, stage: str) -> tuple[FaultSpec, ...]:
        return tuple(fault for fault in self.faults if fault.stage == stage)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "test_case_id": self.test_case_id,
            "description": self.description,
            "profile": self.profile,
            "source": {
                "run_dir": self.source_run_dir,
                "anchor_sha256": self.source_anchor,
                "require_fault_plan": self.require_fault_plan,
            },
            "lab": dict(self.lab_overrides),
            "replay": {"chunk_sizes": list(self.chunk_sizes)},
            "faults": [fault.as_dict() for fault in self.faults],
            "expect": {
                "verdict": self.expect.verdict,
                "allow_physical_duplicates": self.expect.allow_physical_duplicates,
                "require_zero_conflicts": self.expect.require_zero_conflicts,
                "require_published": self.expect.require_published,
                "require_fault_confirmed": self.expect.require_fault_confirmed,
            },
            "limits": {
                "max_scenario_duration_s": self.max_duration_s,
                "evidence_max_bytes": self.evidence_max_bytes,
            },
        }


def load_scenario(
    path: str | Path,
    *,
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
) -> Scenario:
    """Load and validate the scenario at ``path``."""
    document = load_path(path)
    return parse_scenario(document, allowlist=allowlist, origin=str(path))


def parse_scenario(
    document: Any,
    *,
    allowlist: LabAllowlist = DEFAULT_ALLOWLIST,
    origin: str = "<memory>",
) -> Scenario:
    """Validate an already-parsed scenario document."""
    doc = _require_mapping(document, origin)
    _check_keys(doc, _SCENARIO_KEYS, origin)

    version = doc.get("version")
    if version != SCENARIO_VERSION:
        raise ScenarioError(f"{origin}: version must be {SCENARIO_VERSION!r}, got {version!r}")
    scenario_id = doc.get("scenario_id")
    if not isinstance(scenario_id, str) or not scenario_id:
        raise ScenarioError(f"{origin}: scenario_id is required")

    source = _require_mapping(doc.get("source"), f"{origin}.source")
    _check_keys(source, _SOURCE_KEYS, f"{origin}.source")
    run_dir = source.get("run_dir")
    if not isinstance(run_dir, str) or not run_dir:
        raise ScenarioError(f"{origin}.source.run_dir is required")
    anchor = source.get("anchor_sha256")
    if anchor is not None and (not isinstance(anchor, str) or len(anchor) != 64):
        raise ScenarioError(f"{origin}.source.anchor_sha256 must be a 64-character hex digest")

    lab = _require_mapping(doc.get("lab") or {}, f"{origin}.lab")
    _check_keys(lab, _LAB_KEYS, f"{origin}.lab")
    policy = PolicyConfig(
        sequence_window=int(lab.get("sequence_window", PolicyConfig().sequence_window)),
        event_time_stale_ns=int(lab.get("event_time_stale_ns", PolicyConfig().event_time_stale_ns)),
        price_step_policy=str(lab.get("price_step_policy", PolicyConfig().price_step_policy)),
    )

    replay = _require_mapping(doc.get("replay") or {}, f"{origin}.replay")
    _check_keys(replay, _REPLAY_KEYS, f"{origin}.replay")
    # An *absent* ``chunk_sizes`` means "use the default"; an explicitly empty
    # list is a mistake, and `or [4096]` would have silently accepted it.
    raw_chunks = replay["chunk_sizes"] if "chunk_sizes" in replay else [4096]
    if not isinstance(raw_chunks, list) or not raw_chunks:
        raise ScenarioError(f"{origin}.replay.chunk_sizes must be a non-empty list")
    chunk_sizes: list[int] = []
    for size in raw_chunks:
        if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
            raise ScenarioError(f"{origin}.replay.chunk_sizes must contain positive integers")
        chunk_sizes.append(size)

    faults: list[FaultSpec] = []
    for index, entry in enumerate(doc.get("faults") or []):
        where = f"{origin}.faults[{index}]"
        spec = _require_mapping(entry, where)
        _check_keys(spec, _FAULT_KEYS, where)
        fault_type = spec.get("type")
        if fault_type not in FAULT_TYPES:
            raise ScenarioError(
                f"{where}.type {fault_type!r} is not one of {sorted(FAULT_TYPES)}"
            )
        target = spec.get("target")
        if not isinstance(target, str) or not target:
            raise ScenarioError(f"{where}.target is required")
        try:
            allowlist.check(target)
        except Exception as exc:  # TargetNotAllowed
            raise ScenarioError(f"{where}: {exc}") from exc
        options = {key: value for key, value in spec.items() if key not in {"type", "target", "stage"}}
        faults.append(FaultSpec(str(fault_type), FAULT_TYPES[str(fault_type)], target, options))

    limits = _require_mapping(doc.get("limits") or {}, f"{origin}.limits")
    _check_keys(limits, _LIMITS_KEYS, f"{origin}.limits")
    max_parallel = int(limits.get("max_parallel_faults", 2))
    if len(faults) > max_parallel:
        # Design 8 caps concurrent faults per failure domain. A scenario that
        # exceeds its own declared cap is a configuration error, not something
        # to run anyway and explain later.
        raise ScenarioError(
            f"{origin}: {len(faults)} faults exceeds max_parallel_faults={max_parallel}"
        )

    expect_doc = _require_mapping(doc.get("expect") or {}, f"{origin}.expect")
    _check_keys(expect_doc, _EXPECT_KEYS, f"{origin}.expect")
    verdict = expect_doc.get("verdict")
    if verdict is not None and verdict not in {"PASS", "FAIL", "INCONCLUSIVE", "INVALID"}:
        raise ScenarioError(f"{origin}.expect.verdict {verdict!r} is not a terminal verdict")

    return Scenario(
        scenario_id=scenario_id,
        test_case_id=str(doc.get("test_case_id", "")),
        description=str(doc.get("description", "")),
        profile=str(doc.get("profile", "unit-model")),
        source_run_dir=run_dir,
        source_anchor=anchor,
        require_fault_plan=bool(source.get("require_fault_plan", False)),
        lab_overrides={key: value for key, value in lab.items() if key in _LAB_KEYS},
        policy=policy,
        chunk_sizes=tuple(chunk_sizes),
        faults=tuple(faults),
        expect=Expectations(
            verdict=verdict,
            allow_physical_duplicates=bool(expect_doc.get("allow_physical_duplicates", True)),
            require_zero_conflicts=bool(expect_doc.get("require_zero_conflicts", True)),
            require_published=expect_doc.get("require_published"),
            require_fault_confirmed=bool(expect_doc.get("require_fault_confirmed", True)),
        ),
        max_duration_s=int(limits.get("max_scenario_duration_s", 600)),
        evidence_max_bytes=int(limits.get("evidence_max_bytes", 1 << 30)),
    )


_LAB_CONFIG_KEYS: Final = frozenset({"version", "lab", "source", "contracts", "limits", "reconciliation", "safety"})


def load_lab_config(path: str | Path) -> Mapping[str, Any]:
    """Load and shallow-validate ``config/replay-lab.yaml``.

    Validation is deliberately shallow: the file documents the lab's intent and
    most of its keys are consumed by the report rather than by control flow.
    What *is* checked are the keys that would weaken a gate if mis-set, listed in
    :func:`lab_config_findings`.
    """
    document = load_path(path)
    doc = _require_mapping(document, str(path))
    _check_keys(doc, _LAB_CONFIG_KEYS, str(path))
    if doc.get("version") != "p07.scenario.v1":
        raise ScenarioError(f"{path}: version must be 'p07.scenario.v1'")
    return doc


def lab_config_findings(config: Mapping[str, Any]) -> list[str]:
    """Settings that would make a pass meaningless. Returns human-readable notes.

    These are the switches design 14.1 sets and then warns about: a lab that
    accepts sample-only passes, tolerates semantic conflicts, allows missing
    identities or resets offsets automatically is not testing what P07 is for.
    """
    notes: list[str] = []
    reconciliation = config.get("reconciliation") or {}
    safety = config.get("safety") or {}
    if reconciliation.get("allow_sample_only_pass"):
        notes.append("reconciliation.allow_sample_only_pass is true: a sampled pass is not a proof")
    if reconciliation.get("allow_semantic_conflicts"):
        notes.append("reconciliation.allow_semantic_conflicts is true: conflicts are P0, never allowed")
    if reconciliation.get("allow_missing"):
        notes.append("reconciliation.allow_missing is true: missing identities cannot be tolerated")
    if reconciliation.get("allow_extra"):
        notes.append("reconciliation.allow_extra is true: extra identities cannot be tolerated")
    if not reconciliation.get("full_identity_scan", True):
        notes.append("reconciliation.full_identity_scan is false: no exact diff would be performed")
    if not reconciliation.get("require_source_policy_oracle", True):
        notes.append("reconciliation.require_source_policy_oracle is false: the oracle becomes circular")
    if str(safety.get("auto_offset_reset", "none")) != "none":
        notes.append("safety.auto_offset_reset is not 'none': an implicit reset can skip data (INV-08)")
    if not safety.get("publishing_requires_ready_catalog", True):
        notes.append("safety.publishing_requires_ready_catalog is false: a false READY becomes possible")
    return notes
