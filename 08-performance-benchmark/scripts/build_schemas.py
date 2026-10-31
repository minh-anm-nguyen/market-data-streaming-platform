"""Generate ``specs/*.schema.json`` from the code that enforces them.

Section 21 lists ``workload.schema.json``, ``metric.schema.json`` and
``report.schema.json`` as published contracts. Hand-writing them next to a
strict parser creates two sources of truth that drift apart, and the drift is
silent: the schema says one thing, the parser accepts another, and whichever one
a reader consults is the one they will be wrong about.

So the schemas are derived:

* the workload schema from the section classes in :mod:`p08_bench.config`,
  whose ``KEYS`` sets are exactly what the parser requires;
* the metric schema from the stage counters, run counters and label rules in
  :mod:`p08_bench.metrics`;
* the report schema from :func:`p08_bench.report.blank_report`, which is the
  section 25 template itself.

Run after changing any of those::

    python scripts/build_schemas.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from p08_bench import MANIFEST_VERSION, REPORT_VERSION  # noqa: E402
from p08_bench.config import (  # noqa: E402
    CV_REFERENCE_PARTITIONS_PER_TOPIC,
    PROTOCOL_STEADY_SECONDS,
    ArrivalModel,
    CacheProfile,
    DatasetSection,
    EvidenceSection,
    KafkaSection,
    LoadSection,
    Profile,
    QuerySection,
    RunSection,
    SafetySection,
    SymbolDistribution,
    WorkloadConfig,
)
from p08_bench.metrics import (  # noqa: E402
    ALLOWED_LABEL_KEYS,
    FORBIDDEN_LABEL_KEYS,
    RUN_COUNTERS,
    STAGE_COUNTER_SCOPES,
    StageCounterName,
)
from p08_bench.report import (  # noqa: E402
    CAPACITY_EVIDENCE_FIELDS,
    CORRECTNESS_EVIDENCE_FIELDS,
    LATENCY_EVIDENCE_FIELDS,
    blank_report,
)
from p08_bench.percentiles import MIN_COMPLETED_SAMPLES_FOR_P95  # noqa: E402

DRAFT = "https://json-schema.org/draft/2020-12/schema"

#: Per-key types and constraints. Only the ones the parser enforces beyond
#: presence; everything else is described by its type.
_WORKLOAD_PROPERTIES: dict[str, dict[str, Any]] = {
    "run.id": {"type": "string", "minLength": 1},
    "run.seed": {"type": "integer"},
    "run.manifest_version": {"type": "string", "const": MANIFEST_VERSION},
    "run.profile": {"type": "string", "enum": [p.value for p in Profile]},
    "run.expected_oracle": {"type": "string"},
    "run.lane": {"type": "string"},
    "dataset.dataset_id": {"type": "string"},
    "dataset.schema": {"type": "string"},
    "dataset.symbol_count": {"type": "integer", "minimum": 1},
    "dataset.symbol_distribution": {
        "type": "string", "enum": [d.value for d in SymbolDistribution]
    },
    "dataset.tick_to_quote_ratio": {
        "type": "string",
        "pattern": r"^[0-9]+:[0-9]+$",
        "description": "ticks:quotes. The measured mix; ticks/s is never events/s (INV-12).",
    },
    "dataset.source_seal_required": {"type": "boolean"},
    "kafka.tick_topic": {"type": "string"},
    "kafka.quote_topic": {"type": "string"},
    "kafka.partitions_per_topic": {
        "type": "integer",
        "minimum": 1,
        "description": (
            f"per topic, not in total. {CV_REFERENCE_PARTITIONS_PER_TOPIC} is the "
            "CV-reference topology (NFR-02); another value is allowed and judged separately."
        ),
    },
    "kafka.producer_acks": {"type": "string", "enum": list(KafkaSection.ALLOWED_ACKS)},
    "kafka.producer_idempotent": {"type": "boolean"},
    "kafka.topic_generation": {
        "type": "string",
        "description": "distinguishes two incarnations of one topic name (INV-08).",
    },
    "load.model": {"type": "string", "enum": [m.value for m in ArrivalModel]},
    "load.market_tick_target_per_s": {"type": "integer", "minimum": 0},
    "load.quote_target_per_s": {"type": "integer", "minimum": 0},
    "load.ramp_up_seconds": {"type": "integer", "minimum": 0},
    "load.warmup_seconds": {"type": "integer", "minimum": 0},
    "load.steady_seconds": {
        "type": "integer",
        "minimum": 1,
        "description": (
            f"{PROTOCOL_STEADY_SECONDS} is the NFR-05 protocol for a capacity claim; "
            "a shorter window is allowed and cannot make one."
        ),
    },
    "load.drain_seconds": {"type": "integer", "minimum": 0},
    "load.max_late_issue_ms": {"type": "integer", "minimum": 1},
    "query.enabled": {
        "type": "boolean",
        "description": "false means this lane proves no query percentile.",
    },
    "query.family": {"type": "string"},
    "query.concurrency": {"type": "integer", "minimum": 1},
    "query.min_complete_samples": {
        "type": "integer",
        "minimum": 1,
        "description": (
            f"{MIN_COMPLETED_SAMPLES_FOR_P95} is the NFR-08 guardrail; below it a p95 "
            "is discovery-grade."
        ),
    },
    "query.client_p95_budget_ms": {"type": "integer", "minimum": 1},
    "query.cache_profile": {"type": "string", "enum": [c.value for c in CacheProfile]},
    "query.deadline_ms": {"type": "integer", "minimum": 1},
    "evidence.require_ready_snapshot": {"type": "boolean"},
    "evidence.require_identity_diff": {"type": "boolean"},
    "evidence.require_semantic_match": {"type": "boolean"},
    "evidence.require_fault_receipt_if_chaos": {"type": "boolean"},
    "evidence.fail_on_missing_metrics": {"type": "boolean"},
    "safety.lab_only": {"type": "boolean"},
    "safety.dry_run_destructive_actions": {"type": "boolean"},
    "safety.max_test_volume_fill_percent": {"type": "integer", "minimum": 1, "maximum": 99},
    "safety.allow_host_root_path": {"type": "boolean"},
}

_SECTION_CLASSES = {
    "run": RunSection,
    "dataset": DatasetSection,
    "kafka": KafkaSection,
    "load": LoadSection,
    "query": QuerySection,
    "evidence": EvidenceSection,
    "safety": SafetySection,
}


def workload_schema() -> dict[str, Any]:
    sections: dict[str, Any] = {}
    missing: list[str] = []
    for name, cls in _SECTION_CLASSES.items():
        properties: dict[str, Any] = {}
        for key in sorted(cls.KEYS):
            spec = _WORKLOAD_PROPERTIES.get(f"{name}.{key}")
            if spec is None:
                missing.append(f"{name}.{key}")
                continue
            properties[key] = spec
        sections[name] = {
            "type": "object",
            "additionalProperties": False,
            "required": sorted(cls.KEYS),
            "properties": properties,
        }
    if missing:
        raise SystemExit(
            "no schema entry for these config keys; add them to "
            f"_WORKLOAD_PROPERTIES: {missing}"
        )
    return {
        "$schema": DRAFT,
        "$id": "https://example.invalid/p08/workload.schema.json",
        "title": "P08 workload manifest",
        "description": (
            "A complete, auditable statement of one benchmark run (section 3.1). "
            "Unknown and missing keys are both rejected: a config that can fall "
            "back to defaults cannot be pinned before a run, and A/B pairs that "
            "differ in one variable cannot be verified. Generated from "
            "p08_bench.config by scripts/build_schemas.py -- do not hand-edit."
        ),
        "type": "object",
        "additionalProperties": False,
        "required": sorted(WorkloadConfig.SECTIONS),
        "properties": sections,
    }


def metric_schema() -> dict[str, Any]:
    return {
        "$schema": DRAFT,
        "$id": "https://example.invalid/p08/metric.schema.json",
        "title": "P08 metric snapshot",
        "description": (
            "Counters, gauges and histograms from one run (sections 4.1, 5.2). "
            "Generated from p08_bench.metrics -- do not hand-edit."
        ),
        "type": "object",
        "additionalProperties": False,
        "required": ["stage_counters", "counters"],
        "properties": {
            "stage_counters": {
                "type": "object",
                "additionalProperties": False,
                "description": (
                    "The nine counters of section 4.1. Each is an integer or null; "
                    "null means not measured and must never be read as zero."
                ),
                "required": list(StageCounterName.ORDERED),
                "properties": {
                    name: {
                        "type": ["integer", "null"],
                        "minimum": 0,
                        "description": STAGE_COUNTER_SCOPES[name],
                    }
                    for name in StageCounterName.ORDERED
                },
            },
            "counters": {
                "type": "object",
                "description": (
                    "Monotonic run counters. A counter present at zero means "
                    "measured and never happened; an absent counter means not measured."
                ),
                "propertyNames": {"type": "string"},
                "additionalProperties": {"type": "integer", "minimum": 0},
                "examples": [{name: 0 for name in RUN_COUNTERS[:4]}],
            },
            "gauges": {"type": "object", "additionalProperties": {"type": "number"}},
            "peaks": {"type": "object", "additionalProperties": {"type": "number"}},
            "histograms": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "required": ["bounds", "counts", "n", "estimate"],
                    "properties": {
                        "bounds": {"type": "array", "items": {"type": "number"}},
                        "counts": {"type": "array", "items": {"type": "integer"}},
                        "n": {"type": "integer", "minimum": 0},
                        "overflow_count": {"type": "integer", "minimum": 0},
                        "bucket_max_reportable": {
                            "type": ["number", "null"],
                            "description": (
                                "null when the open top bucket is occupied: a maximum "
                                "cannot be derived from an unbounded bucket (section 4.3)."
                            ),
                        },
                        "estimate": {
                            "const": True,
                            "description": (
                                "A bucketed quantile is always an estimate; exact "
                                "quantiles come from retained samples."
                            ),
                        },
                    },
                },
            },
            "label_rules": {
                "type": "object",
                "description": "Cardinality control from section 5.2.",
                "properties": {
                    "allowed_label_keys": {
                        "type": "array",
                        "items": {"type": "string", "enum": sorted(ALLOWED_LABEL_KEYS)},
                    },
                    "forbidden_label_keys": {
                        "type": "array",
                        "items": {"type": "string", "enum": sorted(FORBIDDEN_LABEL_KEYS)},
                    },
                },
            },
        },
    }


def _report_properties(template: Any) -> dict[str, Any]:
    """Describe the section 25 template structurally.

    Every measurement is nullable, because the template ships as null and
    section 25 forbids replacing a null with a guess. What is *not* optional is
    the set of field names: the report has to stay diffable against the
    document.
    """
    if isinstance(template, dict):
        return {
            "type": "object",
            "additionalProperties": False,
            "required": sorted(template),
            "properties": {k: _report_properties(v) for k, v in template.items()},
        }
    if isinstance(template, bool):
        return {"type": "boolean"}
    if isinstance(template, list):
        return {"type": "array"}
    if isinstance(template, str):
        return {"type": "string"}
    if template is None:
        # Nullable and otherwise unconstrained: a measurement, a hash or a path.
        return {"type": ["string", "number", "integer", "boolean", "object", "array", "null"]}
    return {}


def report_schema() -> dict[str, Any]:
    template = blank_report("<run-id>", "<scenario-id>")
    schema = _report_properties(template)
    schema.update(
        {
            "$schema": DRAFT,
            "$id": "https://example.invalid/p08/report.schema.json",
            "title": f"P08 run report ({REPORT_VERSION})",
            "description": (
                "The section 25 report template. Every measurement is nullable and "
                "null means not measured -- it may not be replaced with an estimate. "
                "A dimension may only be PASS when its evidence fields are present; "
                "that rule is enforced by p08_bench.report.validate_report, which "
                "JSON Schema cannot express. Generated by scripts/build_schemas.py."
            ),
            "x-evidence-required-for-pass": {
                "capacity": [f"{s}.{f}" for s, f in CAPACITY_EVIDENCE_FIELDS],
                "latency": [f"{s}.{f}" for s, f in LATENCY_EVIDENCE_FIELDS],
                "correctness": [f"{s}.{f}" for s, f in CORRECTNESS_EVIDENCE_FIELDS],
            },
        }
    )
    return schema


SCHEMAS = {
    "workload.schema.json": workload_schema,
    "metric.schema.json": metric_schema,
    "report.schema.json": report_schema,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=HERE.parent / "specs")
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the files on disk differ from the generated ones",
    )
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)

    stale: list[str] = []
    for name, build in SCHEMAS.items():
        text = json.dumps(build(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        path = args.out / name
        if args.check:
            current = path.read_text(encoding="utf-8") if path.is_file() else ""
            if current != text:
                stale.append(name)
            continue
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")

    if args.check:
        if stale:
            raise SystemExit(
                "these schemas are out of date; run scripts/build_schemas.py: "
                + ", ".join(stale)
            )
        print("schemas are up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
