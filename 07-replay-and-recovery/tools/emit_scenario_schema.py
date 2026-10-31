"""Generate ``scenarios/manifest.schema.json`` from the loader's own key sets.

The design's source tree lists a scenario schema. Hand-writing one would create
a second description of the format that drifts from the first, so this derives it
from :mod:`p07.scenario_loader` instead: the loader stays the enforcement point
and the schema is a projection of it, for editors and for readers.

``tests/unit/test_yamlite_and_scenarios.py`` checks the checked-in file matches
what this emits, so a new key in the loader fails the suite until the schema is
regenerated::

    python -m tools.emit_scenario_schema
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from p07.injectors.base import DEFAULT_ALLOWLIST
from p07.scenario_loader import (
    FAULT_TYPES,
    SCENARIO_VERSION,
    _EXPECT_KEYS,
    _FAULT_KEYS,
    _LAB_KEYS,
    _LIMITS_KEYS,
    _REPLAY_KEYS,
    _SCENARIO_KEYS,
    _SOURCE_KEYS,
)

OUTPUT = Path("scenarios/manifest.schema.json")


def _object(keys: frozenset[str], required: list[str] | None = None) -> dict[str, Any]:
    """A closed object: ``additionalProperties: false`` mirrors the loader.

    The loader treats an unknown key as an error, so the schema has to as well --
    a permissive schema would tell an editor that a typo is fine.
    """
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {key: {} for key in sorted(keys)},
        **({"required": required} if required else {}),
    }


def build() -> dict[str, Any]:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://example.invalid/p07/scenario.schema.json",
        "title": "P07 scenario",
        "description": (
            "Generated from p07.scenario_loader by tools/emit_scenario_schema.py. "
            "The loader is the enforcement point; this file exists for editors and readers. "
            "Scenario files are written in the strict YAML subset p07.yamlite accepts."
        ),
        **_object(_SCENARIO_KEYS, required=["version", "scenario_id", "source"]),
    }
    properties: dict[str, Any] = schema["properties"]
    properties["version"] = {"const": SCENARIO_VERSION}
    properties["scenario_id"] = {"type": "string", "minLength": 1}
    properties["test_case_id"] = {"type": "string"}
    properties["description"] = {"type": "string"}
    properties["profile"] = {"type": "string"}

    properties["source"] = _object(_SOURCE_KEYS, required=["run_dir"])
    properties["source"]["properties"]["run_dir"] = {"type": "string", "minLength": 1}
    properties["source"]["properties"]["anchor_sha256"] = {
        "type": ["string", "null"],
        "pattern": "^[0-9a-f]{64}$",
        "description": "SHA-256 of events.jsonl, pinned OUTSIDE the run directory.",
    }
    properties["source"]["properties"]["require_fault_plan"] = {"type": "boolean"}

    properties["lab"] = _object(_LAB_KEYS)
    for key in ("partitions", "brokers", "replication_factor", "min_insync_replicas",
                "batch_max_records", "sequence_window"):
        properties["lab"]["properties"][key] = {"type": "integer", "minimum": 1}
    properties["lab"]["properties"]["event_time_stale_ns"] = {"type": "integer", "minimum": 0}
    properties["lab"]["properties"]["price_step_policy"] = {"enum": ["flag", "reject"]}
    for key in ("producer_idempotent", "kafka_idempotent_dedupe", "strict_poison"):
        properties["lab"]["properties"][key] = {"type": "boolean"}
    properties["lab"]["properties"]["group"] = {"type": "string"}

    properties["replay"] = _object(_REPLAY_KEYS)
    properties["replay"]["properties"]["chunk_sizes"] = {
        "type": "array",
        "minItems": 1,
        "items": {"type": "integer", "minimum": 1},
    }

    fault = _object(_FAULT_KEYS, required=["type", "target"])
    fault["properties"]["type"] = {"enum": sorted(FAULT_TYPES)}
    fault["properties"]["target"] = {"enum": sorted(DEFAULT_ALLOWLIST.targets)}
    fault["properties"]["partition"] = {
        "type": "integer",
        "minimum": -1,
        "description": "-1 means the busiest partition of the topic.",
    }
    fault["properties"]["confirm"] = {
        "type": "boolean",
        "description": "Required by destructive faults (retention expiry, TTL, volume loss).",
    }
    properties["faults"] = {"type": "array", "items": fault}

    properties["expect"] = _object(_EXPECT_KEYS)
    properties["expect"]["properties"]["verdict"] = {
        "enum": ["PASS", "FAIL", "INCONCLUSIVE", "INVALID", None]
    }
    for key in ("allow_physical_duplicates", "require_zero_conflicts", "require_fault_confirmed"):
        properties["expect"]["properties"][key] = {"type": "boolean"}
    properties["expect"]["properties"]["require_published"] = {"type": ["boolean", "null"]}

    properties["limits"] = _object(_LIMITS_KEYS)
    for key in sorted(_LIMITS_KEYS):
        properties["limits"]["properties"][key] = {"type": "integer", "minimum": 1}

    # The fault stages are informational: the runner, not the schema, decides
    # when each type is applied. Recorded so a reader can see the schedule.
    schema["x-fault-stages"] = dict(sorted(FAULT_TYPES.items()))
    return schema


def render() -> str:
    return json.dumps(build(), indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUTPUT}")
