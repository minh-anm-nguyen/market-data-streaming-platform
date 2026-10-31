"""C++ <-> Python conformance: what-if group E, plus E2E-01/02/03 and L01.

The oracle is the typed projection, produced independently by both languages
from the same source, and compared **byte for byte**. It is never the wire
bytes: Protobuf output is not canonical, so comparing hashes across runtimes
could fail on correct code and pass on two implementations sharing a bug
(ADR-005). One test below records whether the bytes happen to match, as an
observation, and deliberately does not assert it.

These tests skip -- not pass -- when the C++ tools are not built. A suite that
reported green because half of it never ran would be worse than one that said
so (section 11.2).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from md_contract.conformance import (
    Pipeline,
    cross_language_matrix,
    decode_pbstream,
    verify_pbstream,
)
from md_contract.config import Config
from md_contract.domain import projection_line
from md_contract.strict_json import parse_sim_v1_line
from md_contract.validation import ValidationMode

pytestmark = pytest.mark.cpp


@pytest.fixture
def python_stream(small_jsonl, tmp_path) -> tuple[Path, Path, int]:
    """Python encode -> .pbstream, plus the independent reference projection."""
    stream = tmp_path / "python.pbstream"
    reference = tmp_path / "reference_proj.jsonl"
    pipeline = Pipeline(config=Config(), mode=ValidationMode.FAULT_PASSTHROUGH)
    result = pipeline.run_file(small_jsonl, stream)
    with reference.open("wb") as handle, small_jsonl.open("rb") as src:
        for line in src:
            if line.strip():
                handle.write(projection_line(parse_sim_v1_line(line)))
    return stream, reference, result.encoded


@pytest.mark.cases("E01")
def test_python_encode_to_cpp_decode_is_byte_identical(
    cpp_tools, python_stream, tmp_path
) -> None:
    stream, reference, count = python_stream
    out = tmp_path / "cpp_proj.jsonl"
    report = cpp_tools.decode(stream, out)

    assert report["exit_code"] == 0, report
    assert report["events_accepted"] == count
    assert report["frames_read"] == count
    assert report["framing_errors"] == 0
    assert report["decode_errors"] == 0
    assert report["envelope_rejects"] == 0
    assert report["stream_end"] == "COMPLETED"
    # The projection the C++ decoder produced is identical, byte for byte, to
    # the one Python derived from the source independently.
    assert out.read_bytes() == reference.read_bytes()


@pytest.mark.cases("E02")
def test_cpp_encode_to_python_decode_is_byte_identical(
    cpp_tools, small_jsonl, python_stream, tmp_path
) -> None:
    _, reference, count = python_stream
    cpp_stream = tmp_path / "cpp.pbstream"
    cpp_projection = tmp_path / "cpp_enc_proj.jsonl"
    report = cpp_tools.encode(small_jsonl, cpp_stream, projection_out=cpp_projection)

    assert report["exit_code"] == 0, report
    assert report["events_encoded"] == count
    assert report["rejected"] == 0
    assert report["committed"] is True

    out = tmp_path / "py_proj.jsonl"
    result = decode_pbstream(cpp_stream, out, config=Config())
    assert result.accepted == count
    assert out.read_bytes() == reference.read_bytes()
    # The C++ strict JSON reader's own view of each event also matches, which
    # tests the two independent parsers against each other directly.
    assert cpp_projection.read_bytes() == reference.read_bytes()


@pytest.mark.cases("E04", "K07")
def test_wire_byte_equality_is_recorded_but_never_asserted(
    cpp_tools, small_jsonl, python_stream, tmp_path
) -> None:
    stream, _, _ = python_stream
    cpp_stream = tmp_path / "cpp.pbstream"
    cpp_tools.encode(small_jsonl, cpp_stream)

    identical = stream.read_bytes() == cpp_stream.read_bytes()
    # Both outcomes are acceptable. Recorded so it is clear the suite does not
    # depend on it: an upgrade that changed field ordering would break a
    # hash-based oracle while changing nothing about the events.
    assert isinstance(identical, bool)

    # What *is* asserted is that both streams decode to the same events.
    py_events = decode_pbstream(stream).events
    cpp_events = decode_pbstream(cpp_stream).events
    assert py_events == cpp_events


@pytest.mark.cases("E01", "E02", "E03")
def test_the_full_four_way_matrix_passes(small_jsonl, tmp_path) -> None:
    entries = cross_language_matrix(small_jsonl, tmp_path, limit=200)
    failed = [e for e in entries if e.verdict == "FAIL"]
    assert not failed, [e.as_dict() for e in failed]
    assert len(entries) >= 4
    assert all(e.verdict in ("PASS", "SKIPPED_WITH_REASON") for e in entries)


@pytest.mark.cases("E03")
def test_int64_extremes_cross_the_language_boundary_intact(
    cpp_tools, tmp_path
) -> None:
    """A hand-built corpus of boundary values, both directions.

    Generated rather than taken from Project 01 because a normal dataset never
    contains INT64_MIN, and those are exactly the values where a signedness or
    width mistake would show.
    """
    from md_contract.domain import INT64_MAX, INT64_MIN

    lines = []
    for index, (bid, ask) in enumerate(
        [
            (INT64_MIN, INT64_MAX),
            (-1, 1),
            (0, 0),
            (INT64_MAX, INT64_MAX),
            (INT64_MIN, INT64_MIN),
            (2**53 + 1, 2**53 + 3),
        ],
        start=1,
    ):
        lines.append(
            json.dumps(
                {
                    "schema_version": "sim.v1",
                    "dataset_id": "ds-boundary",
                    "event_type": "quote",
                    "symbol": "EURUSD",
                    "sequence": index,
                    "event_time_ns": INT64_MAX if index == 1 else index,
                    "price_scale": 100000,
                    "bid_ticks": bid,
                    "ask_ticks": ask,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    source = tmp_path / "boundary.jsonl"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    reference = tmp_path / "boundary_ref.jsonl"
    with reference.open("wb") as handle:
        for line in lines:
            handle.write(projection_line(parse_sim_v1_line(line.encode())))

    # Encoded at the CODEC level, deliberately bypassing business validation.
    #
    # This test is about integer fidelity across the language boundary, and most
    # of these values -- negative and zero prices -- are business violations the
    # policy layer correctly refuses without a fault manifest. That refusal has
    # its own tests in tests/unit/test_validation.py; routing the corpus through
    # the pipeline here would measure the validator instead of the codec, and
    # four of the six boundary events would never reach the wire.
    from md_contract.codec import encode_event
    from md_contract.framing import PbStreamWriter

    py_stream = tmp_path / "boundary_py.pbstream"
    with PbStreamWriter(py_stream) as writer:
        for line in lines:
            writer.write_frame(encode_event(parse_sim_v1_line(line.encode())))
    cpp_out = tmp_path / "boundary_cpp.jsonl"
    report = cpp_tools.decode(py_stream, cpp_out)
    assert report["exit_code"] == 0, report
    assert cpp_out.read_bytes() == reference.read_bytes()

    # C++ -> Python
    cpp_stream = tmp_path / "boundary_cpp.pbstream"
    assert cpp_tools.encode(source, cpp_stream)["exit_code"] == 0
    events = decode_pbstream(cpp_stream).events
    assert events[0].bid_ticks == INT64_MIN
    assert events[0].ask_ticks == INT64_MAX
    assert events[0].event_time_ns == INT64_MAX
    assert events[2].bid_ticks == 0 and "bid_ticks" in events[2].present_fields()
    assert events[5].bid_ticks == 2**53 + 1


@pytest.mark.cases("L01")
@pytest.mark.slow
def test_a_full_project_01_dataset_round_trips_end_to_end(
    cpp_tools, clean_run_dir, tmp_path
) -> None:
    """E2E-01: a committed Project 01 dataset, all 5000 events, both languages."""
    stream = tmp_path / "full.pbstream"
    pipeline = Pipeline(config=Config(), mode=ValidationMode.STRICT_NORMAL)
    result, dataset = pipeline.run_dataset(clean_run_dir, stream)

    assert dataset.completed
    assert result.metrics.get("json_rejected_total") == 0
    assert result.metrics.get("policy_rejected_total") == 0
    assert result.metrics.reconciles, result.metrics.unbalanced()
    assert result.encoded == dataset.events_lines == 5000

    out = tmp_path / "full_cpp.jsonl"
    report = cpp_tools.decode(stream, out)
    assert report["exit_code"] == 0, report
    assert report["events_accepted"] == 5000

    # And the stream verifies against the source it came from, in order.
    verified = verify_pbstream(stream, dataset.events_path)
    assert verified.passed, verified.as_dict()
    assert verified.source_events == verified.decoded_events == 5000
    assert verified.mismatches == []


@pytest.mark.cases("L04", "F01", "F02", "F03")
@pytest.mark.slow
def test_a_fault_injected_dataset_is_preserved_through_both_languages(
    cpp_tools, fault_run_dir, tmp_path
) -> None:
    """E2E-03: gaps, duplicates, reordering and bad values all survive intact."""
    stream = tmp_path / "faults.pbstream"
    pipeline = Pipeline(config=Config(), mode=ValidationMode.FAULT_PASSTHROUGH)
    result, dataset = pipeline.run_dataset(fault_run_dir, stream)

    assert result.metrics.get("policy_rejected_total") == 0
    assert result.metrics.get("protobuf_encoded_total") == 5000
    # The per-event anomalies were preserved under their manifest fault ids.
    assert result.faults_preserved, "no fault was recorded as preserved"

    # Count and order are unchanged: no gap was filled, no duplicate removed,
    # nothing re-sorted.
    verified = verify_pbstream(stream, dataset.events_path)
    assert verified.passed, verified.as_dict()

    report = cpp_tools.decode(stream, tmp_path / "faults_cpp.jsonl")
    assert report["exit_code"] == 0, report
    assert report["events_accepted"] == 5000

    # The anomalous values themselves, read back out of the C++-decoded stream.
    events = decode_pbstream(stream).events
    by_key = {(e.symbol, e.sequence): e for e in events}
    assert by_key[("EURUSD", 150)].bid_ticks == -1, "a negative price stayed negative"
    assert by_key[("EURUSD", 250)].bid_ticks == 0, "a zero price stayed zero and present"
    crossed = by_key[("GBPUSD", 180)]
    assert crossed.bid_ticks > crossed.ask_ticks, "the crossed quote stayed crossed"

    # The gap Project 01 injected at EURUSD 100 is still a gap.
    eur = sorted(seq for (symbol, seq) in by_key if symbol == "EURUSD")
    assert 100 not in eur
    assert 99 in eur and 101 in eur

    # The duplicate at GBPUSD 200 is still two events.
    gbp_200 = [e for e in events if e.symbol == "GBPUSD" and e.sequence == 200]
    assert len(gbp_200) == 2
    assert gbp_200[0] == gbp_200[1]


@pytest.mark.cases("E09", "K03")
def test_the_cpp_tool_reports_its_protobuf_runtime_version(cpp_tools, python_stream) -> None:
    stream, _, _ = python_stream
    report = cpp_tools.decode(stream)
    # Attribution: a result that cannot name its toolchain is not reportable
    # (FR-10). The C++ runtime is 7.35.1, the same release as the Python one.
    assert report["cpp_protobuf_runtime_version"] == "7.35.1"


@pytest.mark.cases("D13", "D15", "I13")
def test_the_cpp_decoder_rejects_a_corrupted_stream_at_the_right_offset(
    cpp_tools, python_stream, tmp_path
) -> None:
    stream, _, _ = python_stream
    data = bytearray(stream.read_bytes())
    # Corrupt a length header mid-stream into an illegal value.
    import struct

    from md_contract.framing import HEADER_LEN, MAGIC

    first_payload_len = struct.unpack(">I", data[4:8])[0]
    offset = len(MAGIC) + 3 * (HEADER_LEN + first_payload_len)
    data[offset : offset + HEADER_LEN] = struct.pack(">I", 0xFFFFFFFF)
    corrupted = tmp_path / "corrupted.pbstream"
    corrupted.write_bytes(bytes(data))

    out = tmp_path / "corrupted_out.jsonl"
    report = cpp_tools.decode(corrupted, out)
    assert report["exit_code"] != 0, "a corrupted stream must not exit 0"
    assert report["framing_errors"] == 1
    assert report["stream_end"] == "FAILED"
    assert "INVALID_LENGTH" in report["reasons"]
    # The three frames before the corruption were delivered; nothing after it
    # was, and the reader did not scan forward for a plausible next frame.
    assert report["events_accepted"] == 3
    assert len(out.read_text(encoding="utf-8").splitlines()) == 3


@pytest.mark.cases("D15")
def test_a_bit_flip_that_still_parses_is_caught_by_the_oracle_not_by_protobuf(
    cpp_tools, python_stream, tmp_path
) -> None:
    """E2E-06: the limit that cannot be engineered away.

    Protobuf has no checksum, so a flipped bit inside a varint can produce a
    message that parses perfectly and carries a different price. Nothing in the
    codec detects it. What catches it here is comparing against the source, and
    that is only available in test -- which is exactly why the design says a
    production path outside a trusted environment needs authenticated transport
    (ADR-009).
    """
    stream, reference, _ = python_stream
    data = bytearray(stream.read_bytes())

    # Flip a low bit inside the first frame's payload until the frame still
    # decodes but the projection differs.
    import struct

    from md_contract.framing import HEADER_LEN, MAGIC

    body_start = len(MAGIC) + HEADER_LEN
    body_len = struct.unpack(">I", data[4:8])[0]
    flipped = None
    for position in range(body_start, body_start + body_len):
        trial = bytearray(data)
        trial[position] ^= 0x01
        candidate = tmp_path / "flip.pbstream"
        candidate.write_bytes(bytes(trial))
        report = cpp_tools.decode(candidate, tmp_path / "flip.jsonl")
        if report["exit_code"] == 0 and report["events_accepted"] == report["frames_read"]:
            produced = (tmp_path / "flip.jsonl").read_bytes()
            if produced != reference.read_bytes():
                flipped = position
                break

    assert flipped is not None, "no single-bit flip produced a valid-but-different message"
    # The decoder was perfectly happy: no error, no warning, a different event.
    # Only the comparison against the source revealed it.
    assert report["decode_errors"] == 0
    assert report["framing_errors"] == 0
