"""Schema evolution gate and the version matrix: what-if groups A and G.

The premise of this file is a finding from this project's own setup: of six
deliberately breaking schema variants, ``protoc`` compiled five without
complaint. Only the one using a field number in the 19000-19999
implementation-reserved range was refused. ``protoc`` checks that a schema is
well-formed, not that it is compatible with what already shipped -- so the gate
in :mod:`md_contract.schema_gate` is the thing standing between a renumbered
field and a release.

The version matrix runs in **subprocesses**, not because it is tidier but
because it is the only way: every generated module registers the same file name
and message names in the default descriptor pool, so importing two variants in
one interpreter raises ``duplicate file name``.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from md_contract.conformance import run_variant
from md_contract.domain import projection_line
from md_contract.errors import ContractError, Reason
from md_contract.schema_gate import (
    Severity,
    blocking,
    compare_descriptors,
    gate,
    index_messages,
    load_descriptor_set,
    require_gate,
)
from md_contract.strict_json import parse_sim_v1_line


# ---------------------------------------------------------------------------
# the gate
# ---------------------------------------------------------------------------


def test_the_baseline_passes_against_itself(descriptors) -> None:
    passed, findings = gate(descriptors["baseline"], descriptors["baseline"])
    assert passed
    assert findings == []


def test_the_baseline_records_the_frozen_tag_numbers(descriptors) -> None:
    messages = index_messages(load_descriptor_set(descriptors["baseline"]))
    event = messages["marketdata.v1.MarketDataEvent"]
    # These numbers are the release contract. A change here is a change to what
    # previously serialised bytes mean.
    assert {n: f.name for n, f in sorted(event.fields_by_number.items())} == {
        1: "dataset_id",
        2: "symbol",
        3: "sequence",
        4: "event_time_ns",
        5: "price_scale",
        6: "source_schema_version",
        10: "quote",
        11: "tick",
    }
    assert event.oneof_names == ("payload",), "only the author-declared oneof"
    quote = messages["marketdata.v1.Quote"]
    assert {n: f.name for n, f in sorted(quote.fields_by_number.items())} == {
        1: "bid_ticks",
        2: "ask_ticks",
    }
    # Every scalar carries explicit presence.
    for field in event.fields_by_number.values():
        if field.type_name in ("TYPE_INT64", "TYPE_STRING"):
            assert not field.in_real_oneof
            assert field.oneof_index is not None, "proto3 optional = synthetic oneof"


@pytest.mark.cases("A03")
def test_adding_an_optional_field_is_allowed_not_flagged_as_a_oneof_variant(
    descriptors,
) -> None:
    if "v1_1" not in descriptors:
        pytest.skip("v1.1 descriptor not generated")
    passed, findings = gate(descriptors["baseline"], descriptors["v1_1"])
    assert passed
    added = [f for f in findings if f.rule == "optional_field_added"]
    assert len(added) == 1
    assert "feed_label" in added[0].message
    assert added[0].severity is Severity.ALLOWED
    # The distinction that is easy to get wrong: proto3 puts every `optional`
    # field in its own synthetic oneof, so a gate reading `oneof_index` alone
    # would call this a new oneof variant and invert its own verdict.
    assert not [f for f in findings if f.rule == "oneof_variant_added"]


@pytest.mark.cases("A11")
def test_adding_a_oneof_variant_is_conditional_and_named_as_such(descriptors) -> None:
    if "v2_probe" not in descriptors:
        pytest.skip("v2_probe descriptor not generated")
    passed, findings = gate(descriptors["baseline"], descriptors["v2_probe"])
    # Not blocking: it is permitted *if* old readers fail closed, which the
    # version matrix below actually verifies.
    assert passed
    variants = [f for f in findings if f.rule == "oneof_variant_added"]
    assert len(variants) == 1
    assert "depth_update" in variants[0].message
    assert variants[0].severity is Severity.CONDITIONAL
    assert "UNSUPPORTED_EVENT_KIND" in variants[0].message


@pytest.mark.cases("A01")
def test_renumbering_a_field_is_blocked(descriptors) -> None:
    if "breaking_tag_reuse" not in descriptors:
        pytest.skip("variant descriptor not generated")
    passed, findings = gate(descriptors["baseline"], descriptors["breaking_tag_reuse"])
    assert not passed
    rules = {f.rule for f in blocking(findings)}
    assert "field_renumbered" in rules
    assert any("tag 3 to 7" in f.message for f in findings)


@pytest.mark.cases("A06", "A13")
def test_changing_an_integer_price_to_double_is_blocked(descriptors) -> None:
    if "breaking_type_change" not in descriptors:
        pytest.skip("variant descriptor not generated")
    passed, findings = gate(descriptors["baseline"], descriptors["breaking_type_change"])
    assert not passed
    offender = next(f for f in blocking(findings) if f.rule == "field_type_changed")
    assert "TYPE_INT64 -> TYPE_DOUBLE" in offender.message
    assert "2^53" in offender.message, "the reason is stated, not just the rule name"


@pytest.mark.cases("A07")
def test_changing_signed_sequence_to_unsigned_is_blocked(descriptors) -> None:
    if "breaking_unsigned_seq" not in descriptors:
        pytest.skip("variant descriptor not generated")
    passed, findings = gate(descriptors["baseline"], descriptors["breaking_unsigned_seq"])
    # Wire-compatible for non-negative values, and still forbidden: it would
    # reinterpret Project 01's deliberate negative fault values as huge
    # positives. Wire compatibility is not semantic compatibility.
    assert not passed
    offender = next(f for f in blocking(findings) if f.rule == "field_type_changed")
    assert "TYPE_UINT64" in offender.message


@pytest.mark.cases("A04", "A05")
def test_removing_a_business_critical_field_is_blocked(descriptors) -> None:
    if "breaking_removed_field" not in descriptors:
        pytest.skip("variant descriptor not generated")
    passed, findings = gate(descriptors["baseline"], descriptors["breaking_removed_field"])
    assert not passed
    offender = next(f for f in blocking(findings) if f.rule == "core_field_removed")
    assert "price_scale" in offender.message
    assert "reserved or not" in offender.message


@pytest.mark.cases("A14")
def test_protoc_itself_refuses_the_implementation_reserved_range(project_root) -> None:
    """The one breaking change protoc catches on its own.

    Recorded as a test so the boundary is explicit: this is what the compiler
    protects, and everything else in group A is what the gate has to protect.
    """
    import shutil

    protoc = shutil.which("protoc") or r"C:\msys64\ucrt64\bin\protoc.exe"
    if not Path(protoc).exists():
        pytest.skip("protoc not available")
    variant = project_root / "schemas" / "evolution" / "reserved_range"
    if not variant.exists():
        pytest.skip("reserved_range fixture not present")
    completed = subprocess.run(
        [protoc, "-I", str(variant), "-o", str(Path.cwd() / "nul_desc.tmp"),
         "marketdata/v1/market_data.proto"],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    assert "19000" in completed.stderr
    Path("nul_desc.tmp").unlink(missing_ok=True)


def test_require_gate_raises_with_the_blocking_findings_attached(descriptors) -> None:
    if "breaking_tag_reuse" not in descriptors:
        pytest.skip("variant descriptor not generated")
    with pytest.raises(ContractError) as excinfo:
        require_gate(descriptors["baseline"], descriptors["breaking_tag_reuse"])
    assert excinfo.value.reason is Reason.SCHEMA_GATE_REJECT
    assert excinfo.value.context["findings"]


# ---------------------------------------------------------------------------
# the version matrix, in separate processes
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reference_projection(small_jsonl, tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("versions") / "reference.jsonl"
    with out.open("wb") as handle, small_jsonl.open("rb") as src:
        for line in src:
            if line.strip():
                handle.write(projection_line(parse_sim_v1_line(line)))
    return out


def test_two_variants_cannot_be_imported_into_one_process() -> None:
    """The constraint that forces subprocess isolation (section 5.4).

    Asserted rather than assumed: if a future protobuf release made this work,
    the matrix could be simplified, and if it still fails this test explains why
    the matrix is built the way it is.
    """
    code = (
        "from md_contract.generated.v1.marketdata.v1 import market_data_pb2 as a;"
        "from md_contract.generated.v1_1.marketdata.v1 import market_data_pb2 as b"
    )
    import os

    env = dict(os.environ)
    root = Path(__file__).resolve().parents[2]
    env["PYTHONPATH"] = str(root / "python" / "src") + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env
    )
    assert completed.returncode != 0
    assert "duplicate file name" in completed.stderr


@pytest.mark.parametrize(
    ("writer", "reader"),
    [("v1", "v1"), ("v1_1", "v1"), ("v1", "v1_1"), ("v1_1", "v1_1")],
)
@pytest.mark.cases("G01", "G02", "A03")
def test_core_projection_is_unchanged_across_versions(
    writer: str, reader: str, small_jsonl, reference_projection, tmp_path
) -> None:
    stream = tmp_path / f"{writer}.pbstream"
    out = tmp_path / f"{writer}_by_{reader}.jsonl"
    extra = ["--feed-label", "prod-a"] if writer == "v1_1" else []

    encoded = run_variant(
        "--variant", writer, "--op", "encode",
        "--input", str(small_jsonl), "--output", str(stream), *extra,
    )
    assert encoded["exit_code"] == 0, encoded
    decoded = run_variant(
        "--variant", reader, "--op", "decode",
        "--input", str(stream), "--output", str(out),
    )
    assert decoded["exit_code"] == 0, decoded
    assert decoded["events_accepted"] == encoded["events_encoded"]
    assert decoded["unsupported_event_kind"] == 0
    # The v1 core projection is identical in every direction. Adding a field
    # changes nothing a v1 reader can see.
    assert out.read_bytes() == reference_projection.read_bytes()


@pytest.mark.cases("G01", "G03")
def test_a_v1_reader_retains_the_unknown_field_it_cannot_name(
    small_jsonl, tmp_path
) -> None:
    stream = tmp_path / "v1_1.pbstream"
    run_variant(
        "--variant", "v1_1", "--op", "encode", "--input", str(small_jsonl),
        "--output", str(stream), "--feed-label", "prod-a", "--limit", "20",
    )
    decoded = run_variant(
        "--variant", "v1", "--op", "decode", "--input", str(stream),
    )
    assert decoded["exit_code"] == 0
    # Field 20 is kept in the unknown set rather than discarded, which is what
    # makes lossless forwarding *possible* -- though core equality and lossless
    # forwarding remain separate guarantees (section 5.5).
    assert decoded["sample_extras"][0]["unknown_field_numbers"] == [20]


@pytest.mark.cases("G02")
def test_a_v1_1_reader_does_not_fabricate_the_new_field(small_jsonl, tmp_path) -> None:
    stream = tmp_path / "v1.pbstream"
    run_variant(
        "--variant", "v1", "--op", "encode", "--input", str(small_jsonl),
        "--output", str(stream), "--limit", "20",
    )
    decoded = run_variant("--variant", "v1_1", "--op", "decode", "--input", str(stream))
    extras = decoded["sample_extras"][0]
    # Absent, not an empty string presented as if it had been sent.
    assert extras["has_feed_label"] is False
    assert extras["feed_label"] is None


@pytest.mark.cases("A11", "D20", "G06", "E14")
def test_a_new_oneof_variant_fails_closed_at_an_old_reader(small_jsonl, tmp_path) -> None:
    stream = tmp_path / "v2_depth.pbstream"
    out = tmp_path / "v2_by_v1.jsonl"
    encoded = run_variant(
        "--variant", "v2_probe", "--op", "encode", "--input", str(small_jsonl),
        "--output", str(stream), "--payload", "depth_update", "--limit", "40",
    )
    assert encoded["events_encoded"] == 40

    decoded = run_variant(
        "--variant", "v1", "--op", "decode", "--input", str(stream), "--output", str(out),
    )
    # Every frame was read, every one was refused, and none was turned into a
    # quote or a tick. Nothing was emitted at all.
    assert decoded["frames_read"] == 40
    assert decoded["unsupported_event_kind"] == 40
    assert decoded["events_accepted"] == 0
    assert decoded["decode_errors"] == 0, "the bytes parse; it is the *kind* that is unknown"
    assert not out.exists() or out.read_bytes() == b""


@pytest.mark.cases("G05")
def test_protojson_is_not_used_as_a_lossless_bridge() -> None:
    """ProtoJSON cannot carry unknown fields, so it must not be a forwarding hop.

    Demonstrated rather than asserted by policy: the round trip through JSON
    drops field 20, which is why section 5.3 forbids a binary -> ProtoJSON ->
    binary path for lossless forwarding.
    """
    from google.protobuf import json_format

    from md_contract.codec import encode_event, parse_payload, unknown_field_numbers
    from tests.unit.test_codec import sample_quote

    with_unknown = encode_event(sample_quote()) + bytes([0xA2, 0x01, 3]) + b"abc"
    msg = parse_payload(with_unknown)
    assert unknown_field_numbers(msg) == (20,)

    # Binary -> binary keeps it.
    assert unknown_field_numbers(parse_payload(msg.SerializeToString())) == (20,)

    # Binary -> ProtoJSON -> binary loses it.
    as_json = json_format.MessageToJson(msg)
    assert "feed_label" not in as_json
    rebuilt = parse_payload(
        json_format.Parse(as_json, type(msg)()).SerializeToString()
    )
    assert unknown_field_numbers(rebuilt) == (), "ProtoJSON dropped the unknown field"


@pytest.mark.cases("E06", "K03")
def test_the_generated_code_enforces_its_runtime_version() -> None:
    """Python gencode carries a runtime version check of its own.

    Worth a test because it is a guarantee the project relies on rather than
    implements: if a future codegen dropped the check, the pinned-version
    discipline would lose one of its layers and the lockfile check in
    `toolchain-check` would be the only remaining guard.
    """
    source = (
        Path(__file__).resolve().parents[2]
        / "python" / "src" / "md_contract" / "generated" / "v1"
        / "marketdata" / "v1" / "market_data_pb2.py"
    ).read_text(encoding="utf-8")
    assert "ValidateProtobufRuntimeVersion" in source
    assert "runtime_version" in source


@pytest.mark.cases("A09")
def test_renaming_a_field_is_conditional_because_the_wire_survives_but_apis_do_not(
    descriptors, project_root
) -> None:
    """A rename keeps the tag, so old bytes still parse -- and breaks everything else.

    This is the case where "wire compatible" is most misleading. The binary
    format carries field *numbers*, not names, so previously serialised bytes
    decode unchanged. What breaks is the generated accessor
    (``source_schema_version()`` becomes ``feed_source()``), every caller of it,
    and the ProtoJSON field name. So the gate flags it for review rather than
    blocking it, and says why.
    """
    candidate = project_root / "build" / "descriptors" / "variants" / "rename_field.desc"
    if not candidate.exists():
        pytest.skip("rename_field descriptor not generated")

    passed, findings = gate(descriptors["baseline"], candidate)
    renamed = [f for f in findings if f.rule == "field_renamed"]
    assert len(renamed) == 1
    assert renamed[0].severity is Severity.CONDITIONAL
    assert "source_schema_version" in renamed[0].message
    assert "feed_source" in renamed[0].message
    assert "ProtoJSON" in renamed[0].message

    # The tag is unchanged, so this is NOT reported as a renumber or a removal.
    assert not [f for f in findings if f.rule == "field_renumbered"]
    assert not [f for f in findings if f.rule == "core_field_removed"]

    # Previously serialised bytes still decode under the renamed schema, because
    # tag 6 still means the same thing on the wire.
    messages = index_messages(load_descriptor_set(candidate))
    event = messages["marketdata.v1.MarketDataEvent"]
    assert event.fields_by_number[6].name == "feed_source"
    assert event.fields_by_number[6].type_name == "TYPE_STRING"


@pytest.mark.cases("G04")
def test_rebuilding_a_message_field_by_field_loses_unknown_fields() -> None:
    """Core equality is not lossless forwarding (section 5.5, ADR-005).

    A v1 reader that reconstructs a message from the fields it knows produces
    something with an identical core projection and *no* unknown fields. That is
    fine for forwarding core data and wrong for forwarding the original event,
    so the distinction has to be demonstrated rather than assumed.
    """
    from md_contract.codec import (
        domain_to_pb,
        encode_event,
        parse_payload,
        pb_to_domain,
        unknown_field_numbers,
    )
    from md_contract.domain import projection_json
    from tests.unit.test_codec import sample_quote

    # A v1.1 event: everything v1 knows, plus feed_label in field 20.
    original_bytes = encode_event(sample_quote()) + bytes([0xA2, 0x01, 3]) + b"abc"
    parsed = parse_payload(original_bytes)
    assert unknown_field_numbers(parsed) == (20,)

    # Path 1: reserialise the parsed message. The unknown field survives.
    forwarded = parse_payload(parsed.SerializeToString())
    assert unknown_field_numbers(forwarded) == (20,)

    # Path 2: rebuild from the typed domain event, which only holds v1 fields.
    rebuilt_bytes = encode_event(pb_to_domain(parsed))
    rebuilt = parse_payload(rebuilt_bytes)
    assert unknown_field_numbers(rebuilt) == (), "field 20 was dropped by the rebuild"

    # Both paths agree on the core projection -- which is exactly why core
    # equality cannot be used as evidence of lossless forwarding.
    assert projection_json(pb_to_domain(forwarded)) == projection_json(pb_to_domain(rebuilt))
    assert rebuilt_bytes != original_bytes


@pytest.mark.cases("G11")
def test_a_single_field_mutation_is_caught_by_the_core_projection() -> None:
    """Mutate one field at a time and confirm the oracle notices every one.

    This is the oracle's own test. If a field were missing from the projection,
    a change to it would pass unnoticed -- and that is the silent corruption the
    whole design is built to prevent. So every v1 field is mutated in turn and
    the projection must differ each time.
    """
    from md_contract.codec import decode_payload, encode_event
    from md_contract.domain import core_projection_v1, projection_json
    from tests.unit.test_codec import sample_quote

    base = sample_quote()
    baseline = projection_json(base)

    mutations = {
        "dataset_id": base._replace(dataset_id="ds-other"),
        "symbol": base._replace(symbol="GBPUSD"),
        "sequence": base._replace(sequence=base.sequence + 1),
        "event_time_ns": base._replace(event_time_ns=base.event_time_ns + 1),
        "price_scale": base._replace(price_scale=1000),
        "source_schema_version": base._replace(source_schema_version="sim.vX"),
        "bid_ticks": base._replace(bid_ticks=base.bid_ticks + 1),
        "ask_ticks": base._replace(ask_ticks=base.ask_ticks + 1),
    }
    for field_name, mutated in mutations.items():
        decoded = decode_payload(encode_event(mutated))
        assert projection_json(decoded) != baseline, (
            f"changing {field_name} did not change the projection, so the oracle "
            "would not detect a corruption of it"
        )
        differing = [
            key
            for key in set(core_projection_v1(base)) | set(core_projection_v1(decoded))
            if core_projection_v1(base).get(key) != core_projection_v1(decoded).get(key)
        ]
        assert field_name in differing

    # Dropping a field changes the projection too, via the `present` list.
    without_sequence = base._replace(sequence=None)
    assert projection_json(without_sequence) != baseline
    assert "sequence" not in without_sequence.present_fields()

    # And changing the payload case is visible.
    as_tick = base._replace(
        payload_case="tick", bid_ticks=None, ask_ticks=None, price_ticks=108346
    )
    assert projection_json(as_tick) != baseline
