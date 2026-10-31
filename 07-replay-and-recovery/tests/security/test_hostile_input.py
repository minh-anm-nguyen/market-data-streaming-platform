"""Hostile input and lab containment -- design group ``G28``.

The threat model is narrow and worth stating: P07 reads a *scenario file* and a
*dataset* that may both be untrusted, and it can be pointed at infrastructure.
So the questions are whether a config can make it run something, whether a
dataset can make it allocate without bound, and whether a fault can escape the
lab's allowlist.

What is deliberately **not** claimed: this is not a sandbox. A scenario file is
still input to code, the harness runs with the caller's privileges, and the
containment here is an allowlist plus a closed set of fault types -- not
isolation.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

import pytest

from p07.contracts import MAX_EVENT_VALUE_BYTES, EventKind, SemanticCore
from p07.dataplane.adapter import Adapter, SymbolLimits
from p07.dataplane.codec import DEFAULT_CODEC, MDP1_MAGIC, CodecError, frame_mdp1
from p07.dataset_sealer import SealError, seal_dataset
from p07.evidence.artifact_store import ArtifactStore
from p07.evidence.verdicts import Reason
from p07.injectors.base import DEFAULT_ALLOWLIST, TargetNotAllowed, inject
from p07.scenario_loader import ScenarioError, parse_scenario
from p07.yamlite import YamlLiteError, safe_load

pytestmark = pytest.mark.security

LIMITS = {
    "EURUSD": SymbolLimits(
        price_scale=100000,
        min_price=1,
        max_price=10**9,
        min_spread=0,
        max_spread=10**6,
        max_step=10**9,
    )
}


# -- untrusted config ------------------------------------------------------


@pytest.mark.whatif("P07-G28-02")
def test_a_config_cannot_name_a_python_object_to_construct() -> None:
    """The classic YAML deserialisation attack, refused by the subset loader."""
    for payload in (
        "cmd: !!python/object/apply:os.system ['echo pwned']",
        "cmd: !!python/name:os.system",
        "cmd: !!map {a: 1}",
    ):
        with pytest.raises(YamlLiteError):
            safe_load(payload)


@pytest.mark.whatif("P07-G28-02")
def test_a_scenario_cannot_smuggle_a_command_through_a_fault_entry() -> None:
    """Fault types are a closed enum, so no field takes a command."""
    document = {
        "version": "p07.scenario.v1",
        "scenario_id": "hostile",
        "source": {"run_dir": "fixtures/demo-fx-001"},
        "faults": [{"type": "kafka_broker_stop", "target": "kafka_broker", "command": "rm -rf /"}],
    }
    with pytest.raises(ScenarioError, match="unknown keys"):
        parse_scenario(document)


@pytest.mark.whatif("P07-G28-01")
def test_a_scenario_cannot_target_infrastructure_outside_the_lab() -> None:
    for target in ("prod-kafka-01", "/dev/sda", "localhost:9092", "../../etc"):
        document = {
            "version": "p07.scenario.v1",
            "scenario_id": "hostile",
            "source": {"run_dir": "fixtures/demo-fx-001"},
            "faults": [{"type": "kafka_broker_stop", "target": target}],
        }
        with pytest.raises(ScenarioError, match="TARGET_NOT_ALLOWLISTED"):
            parse_scenario(document)


def test_the_allowlist_is_enforced_at_the_injector_too() -> None:
    """Defence in depth: the loader checks, and so does the injection itself."""
    with pytest.raises(TargetNotAllowed):
        inject(
            fault_id="f",
            intended_fault="x",
            target="prod-kafka-01",
            allowlist=DEFAULT_ALLOWLIST,
            precondition=lambda: True,
            action=lambda: None,
            observe=lambda: (True, None),
        )


# -- untrusted artifact names ---------------------------------------------


@pytest.mark.whatif("P07-G28-03")
@pytest.mark.parametrize(
    "hostile",
    [
        "../outside.json",
        "../../outside.json",
        "nested/../../outside.json",
        "/etc/passwd",
        "C:/Windows/System32/x.json",
    ],
)
def test_an_artifact_name_cannot_escape_the_run_directory(tmp_path: Path, hostile: str) -> None:
    store = ArtifactStore(tmp_path / "runs", scenario_id="sc", run_id="r1", fsync=False)
    with pytest.raises((ValueError, OSError)):
        store.write_json(hostile, {"a": 1})
    assert list((tmp_path / "runs").glob("outside.json")) == []
    assert not (tmp_path / "outside.json").exists()


# -- hostile datasets -----------------------------------------------------


@pytest.mark.whatif("P07-G28-04")
def test_a_dataset_whose_report_disagrees_with_itself_is_refused(
    make_source: Callable[..., dict[str, Any]]
) -> None:
    """A source is quarantined, never repaired into something that passes."""
    info = make_source(corrupt_report_hash=True)
    with pytest.raises(SealError):
        seal_dataset(info["run_dir"])


@pytest.mark.whatif("P07-G03-13")
def test_a_declared_length_cannot_drive_an_allocation() -> None:
    """P03 rule 1: the length is validated *before* anything is reserved.

    A 4 GiB length prefix must be rejected on sight; the test asserts the framer
    closed the stream rather than buffering towards that number.
    """
    published: list[Any] = []
    adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
    adapter.feed(MDP1_MAGIC + (0xFFFFFFFF).to_bytes(4, "big"))
    assert adapter.counters.closed_reason == "INVALID_LENGTH"
    assert adapter.pending_bytes == 0
    assert published == []


@pytest.mark.whatif("P07-G03-13")
def test_an_oversize_payload_is_refused_by_the_codec() -> None:
    with pytest.raises(CodecError, match="PAYLOAD_TOO_LARGE"):
        DEFAULT_CODEC.decode(b"\x00" * (MAX_EVENT_VALUE_BYTES + 1))


@pytest.mark.whatif("P07-G28-05")
def test_a_hostile_byte_stream_never_produces_an_event() -> None:
    """Random and adversarial bytes: emit nothing, close, and do not hang."""
    for payload in (
        b"\x00" * 64,
        MDP1_MAGIC + b"\x00" * 4,  # a declared length of zero
        MDP1_MAGIC * 20,
        b'{"dataset_id": "ds"}\n' * 10,  # JSONL sent to the binary port
        os.urandom(512),
    ):
        published: list[Any] = []
        adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
        adapter.feed(payload)
        adapter.close()
        assert published == [], payload[:8]


@pytest.mark.whatif("P07-G28-06")
def test_symbol_cardinality_stays_bounded_under_a_flood() -> None:
    """An unknown-symbol flood must not grow adapter state.

    A hostile feed inventing symbols would otherwise be a memory-exhaustion
    vector through the tracker's key map (P03 case E-12).
    """
    published: list[Any] = []
    adapter = Adapter(dataset_id="ds", limits=LIMITS, sink=published.append)
    for index in range(500):
        core = SemanticCore(
            dataset_id="ds",
            symbol=f"SYM{index:05d}",
            sequence=1,
            event_time_ns=1000,
            price_scale=100000,
            event_kind=EventKind.TICK,
            source_schema_version="sim.v1",
            bid_ticks=None,
            ask_ticks=None,
            price_ticks=100,
        )
        adapter.feed(frame_mdp1(DEFAULT_CODEC.encode(core)))
    adapter.close()
    assert published == []
    # Only allowlisted symbols may ever get tracker state.
    assert set(adapter._keys) <= set(LIMITS)  # noqa: SLF001 - inspecting bounded state
    assert adapter.counters.business_rejects == 500


@pytest.mark.whatif("P07-G29-06")
def test_reason_codes_are_a_bounded_set_not_free_text() -> None:
    """Design 13.2: an unbounded label is a metrics-cardinality problem.

    Every reason the harness emits comes from :class:`Reason`, so a hostile
    dataset cannot inject a new label into the report's aggregation keys.
    """
    codes = {
        value
        for name, value in vars(Reason).items()
        if not name.startswith("_") and isinstance(value, str)
    }
    assert 30 < len(codes) < 200
    assert all(code.isupper() and " " not in code for code in codes)
