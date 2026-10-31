"""MDP1 framing: what-if group D, plus I13-I14.

Two properties get the most attention here, because both are easy to implement
subtly wrong and neither shows up in a happy-path test:

* An advertised length is validated as an *integer*, before anything is
  allocated or read. A header claiming 0xffffffff must cost nothing.
* A clean EOF exactly on a frame boundary is COMPLETED; one byte into a header
  or body it is INCOMPLETE. Those are different outcomes and a reader that
  blurred them would report a truncated stream as a finished one.
"""

from __future__ import annotations

import struct

import pytest

from md_contract.codec import encode_event
from md_contract.domain import PAYLOAD_QUOTE, DomainEvent
from md_contract.errors import FramingError, Reason
from md_contract.framing import (
    HEADER_LEN,
    MAGIC,
    MAX_PAYLOAD_BYTES,
    BytesSource,
    FrameReader,
    FrameWriter,
    PbStreamWriter,
    StreamEnd,
    encode_stream,
    frame_bytes,
    iter_frames_from_file,
)


def sample_payload() -> bytes:
    return encode_event(
        DomainEvent(
            dataset_id="ds-test",
            symbol="EURUSD",
            sequence=7,
            event_time_ns=1793322000000000000,
            price_scale=100000,
            source_schema_version="sim.v1",
            payload_case=PAYLOAD_QUOTE,
            bid_ticks=108345,
            ask_ticks=108348,
        )
    )


def read_first(data: bytes, *, chunk: int | None = None, max_payload: int = MAX_PAYLOAD_BYTES):
    reader = FrameReader(
        BytesSource(data, chunk_size=chunk), max_payload_bytes=max_payload, deadline_s=None
    )
    return reader, reader.read_frame()


def framing_reason(data: bytes, *, chunk: int | None = None) -> tuple[Reason, StreamEnd | None]:
    reader = FrameReader(BytesSource(data, chunk_size=chunk), deadline_s=None)
    with pytest.raises(FramingError) as excinfo:
        reader.read_frame()
    return excinfo.value.reason, reader.end


@pytest.mark.cases("D01")
def test_bad_magic_is_rejected_before_any_payload_parse() -> None:
    reason, end = framing_reason(b"XXXX" + frame_bytes(sample_payload()))
    assert reason is Reason.BAD_MAGIC
    assert end is StreamEnd.FAILED


@pytest.mark.cases("D02", "I08")
def test_jsonl_on_the_protobuf_port_is_bad_magic_with_no_json_fallback() -> None:
    line = b'{"schema_version":"sim.v1","event_type":"quote"}\n'
    reason, end = framing_reason(line)
    assert reason is Reason.BAD_MAGIC
    assert end is StreamEnd.FAILED


@pytest.mark.cases("D03")
def test_truncated_prefix_sweep() -> None:
    # Every partial prefix length, not just one: an off-by-one in the magic read
    # would only show at a particular length.
    for n in range(1, 4):
        reason, end = framing_reason(MAGIC[:n])
        assert reason is Reason.TRUNCATED_PREFIX, n
        assert end is StreamEnd.INCOMPLETE


@pytest.mark.cases("D04", "D05", "D06")
def test_truncated_header_sweep() -> None:
    payload = sample_payload()
    for n in range(1, HEADER_LEN):
        reason, end = framing_reason(MAGIC + frame_bytes(payload)[:n])
        assert reason is Reason.TRUNCATED_HEADER, n
        assert end is StreamEnd.INCOMPLETE


@pytest.mark.cases("D07")
def test_zero_length_is_invalid_not_an_empty_event() -> None:
    reason, end = framing_reason(MAGIC + struct.pack(">I", 0))
    assert reason is Reason.INVALID_LENGTH
    assert end is StreamEnd.FAILED


@pytest.mark.cases("D08")
def test_length_boundary_4096_accepted_4097_refused() -> None:
    # 4096 is legal, so a reader that refused it would reject valid traffic.
    at_limit = MAGIC + struct.pack(">I", MAX_PAYLOAD_BYTES) + b"\x00" * MAX_PAYLOAD_BYTES
    reader, payload = read_first(at_limit)
    assert payload is not None and len(payload) == MAX_PAYLOAD_BYTES

    reason, _ = framing_reason(MAGIC + struct.pack(">I", MAX_PAYLOAD_BYTES + 1))
    assert reason is Reason.INVALID_LENGTH


@pytest.mark.cases("D09", "J03")
def test_huge_advertised_length_is_refused_without_allocating() -> None:
    # Only 8 bytes of input, claiming 4 GiB. If the reader allocated on the
    # advertised length, this would be a 4 GiB reserve from 8 bytes of traffic.
    data = MAGIC + b"\xff\xff\xff\xff"
    reason, end = framing_reason(data)
    assert reason is Reason.INVALID_LENGTH
    assert end is StreamEnd.FAILED


@pytest.mark.cases("D10")
def test_little_endian_header_fails_closed_rather_than_being_guessed() -> None:
    payload = sample_payload()
    swapped = MAGIC + struct.pack("<I", len(payload)) + payload
    reason, _ = framing_reason(swapped)
    # A ~60-byte length byte-swapped reads as an enormous number, so it is
    # refused. The reader never tries the other endianness to see if it fits.
    assert reason is Reason.INVALID_LENGTH


@pytest.mark.cases("D11")
def test_body_one_byte_short_is_truncated_and_emits_nothing() -> None:
    stream = encode_stream([sample_payload()])
    reason, end = framing_reason(stream[:-1])
    assert reason is Reason.TRUNCATED_BODY
    assert end is StreamEnd.INCOMPLETE


@pytest.mark.cases("D11")
def test_body_truncation_sweep_across_every_length() -> None:
    stream = encode_stream([sample_payload()])
    body_start = len(MAGIC) + HEADER_LEN
    for cut in range(body_start, len(stream)):
        reason, end = framing_reason(stream[:cut])
        assert reason is Reason.TRUNCATED_BODY, cut
        assert end is StreamEnd.INCOMPLETE


@pytest.mark.cases("D16")
def test_many_frames_coalesced_into_one_buffer() -> None:
    payloads = [sample_payload() for _ in range(5)]
    reader = FrameReader(BytesSource(encode_stream(payloads)), deadline_s=None)
    assert list(reader) == payloads
    assert reader.end is StreamEnd.COMPLETED
    assert reader.frames_read == 5


@pytest.mark.cases("D17")
def test_one_frame_split_across_single_byte_reads() -> None:
    payloads = [sample_payload() for _ in range(3)]
    # chunk_size=1 forces the reassembly path: every header and body arrives one
    # byte at a time, which is what a slow socket looks like.
    reader = FrameReader(BytesSource(encode_stream(payloads), chunk_size=1), deadline_s=None)
    assert list(reader) == payloads
    assert reader.end is StreamEnd.COMPLETED


@pytest.mark.parametrize("chunk", [1, 2, 3, 5, 7, 13, 64])
def test_reassembly_is_chunk_size_independent(chunk: int) -> None:
    payloads = [sample_payload(), sample_payload()]
    reader = FrameReader(
        BytesSource(encode_stream(payloads), chunk_size=chunk), deadline_s=None
    )
    assert list(reader) == payloads
    assert reader.end is StreamEnd.COMPLETED


def test_clean_eof_on_a_frame_boundary_is_completed() -> None:
    reader = FrameReader(BytesSource(encode_stream([sample_payload()])), deadline_s=None)
    assert reader.read_frame() is not None
    assert reader.read_frame() is None
    assert reader.end is StreamEnd.COMPLETED


def test_empty_stream_with_only_magic_is_completed() -> None:
    reader = FrameReader(BytesSource(MAGIC), deadline_s=None)
    assert reader.read_frame() is None
    assert reader.end is StreamEnd.COMPLETED


def test_writer_refuses_zero_length_and_oversize_frames() -> None:
    import io

    writer = FrameWriter(io.BytesIO())
    with pytest.raises(FramingError) as excinfo:
        writer.write_frame(b"")
    assert excinfo.value.reason is Reason.INVALID_LENGTH

    with pytest.raises(FramingError) as excinfo:
        writer.write_frame(b"x" * (MAX_PAYLOAD_BYTES + 1))
    assert excinfo.value.reason is Reason.INVALID_LENGTH


def test_writer_emits_the_magic_exactly_once() -> None:
    import io

    buffer = io.BytesIO()
    writer = FrameWriter(buffer)
    for _ in range(3):
        writer.write_frame(sample_payload())
    data = buffer.getvalue()
    assert data.startswith(MAGIC)
    assert data.count(MAGIC) == 1 or MAGIC not in data[len(MAGIC) :]
    assert writer.frames_written == 3


@pytest.mark.cases("I13")
def test_corruption_at_frame_n_stops_at_that_offset_without_resyncing() -> None:
    payloads = [sample_payload() for _ in range(6)]
    stream = bytearray(encode_stream(payloads))
    # Corrupt the length header of the 4th frame into something illegal.
    offset = len(MAGIC) + 3 * (HEADER_LEN + len(payloads[0]))
    stream[offset : offset + HEADER_LEN] = struct.pack(">I", 0xFFFFFFFF)

    reader = FrameReader(BytesSource(bytes(stream)), deadline_s=None)
    read = []
    with pytest.raises(FramingError) as excinfo:
        while (payload := reader.read_frame()) is not None:
            read.append(payload)

    assert len(read) == 3, "frames before the corruption are delivered"
    assert excinfo.value.reason is Reason.INVALID_LENGTH
    assert excinfo.value.offset == offset, "the reported offset is exact"
    assert reader.end is StreamEnd.FAILED
    # Crucially: it did not skip ahead and deliver the remaining good frames.
    # Doing so would silently hide however much data was lost (ADR-007).
    assert reader.frames_read == 3


@pytest.mark.cases("I10", "I12")
def test_pbstream_writer_commits_atomically_with_a_marker(tmp_path) -> None:
    target = tmp_path / "out.pbstream"
    payload = sample_payload()
    with PbStreamWriter(target) as writer:
        for _ in range(4):
            writer.write_frame(payload)
        # Mid-run: only the .part exists, and no marker claims completion.
        assert writer.part_path.exists()
        assert not writer.completed_path.exists()

    assert target.exists()
    assert not writer.part_path.exists()
    assert writer.committed
    import json

    marker = json.loads(writer.completed_path.read_text(encoding="utf-8"))
    assert marker["frames"] == 4
    assert marker["frame_protocol"] == "MDP1"
    assert len(marker["sha256"]) == 64
    assert list(iter_frames_from_file(target)) == [payload] * 4


@pytest.mark.cases("I10")
def test_pbstream_writer_leaves_no_marker_when_the_run_fails(tmp_path) -> None:
    target = tmp_path / "aborted.pbstream"
    with pytest.raises(RuntimeError):
        with PbStreamWriter(target) as writer:
            writer.write_frame(sample_payload())
            raise RuntimeError("simulated failure mid-stream")

    # A failed run leaves the .part behind and writes no COMPLETED marker, so a
    # later reader cannot mistake it for a committed artifact.
    assert writer.part_path.exists()
    assert not target.exists()
    assert not writer.completed_path.exists()
    assert not writer.committed


@pytest.mark.cases("D12", "I14")
def test_absolute_deadline_is_not_extended_by_a_trickle_of_bytes() -> None:
    """A slow sender must not be able to hold the reader open indefinitely.

    The source below always has one more byte available, so a per-read timeout
    that reset on every byte would never fire. The deadline is absolute, so the
    read is abandoned once the budget is spent regardless of progress.
    """
    import time

    class Trickle:
        def __init__(self) -> None:
            self.served = 0

        def read_some(self, size: int, deadline: float | None) -> bytes:
            # Always makes progress, but slowly -- the pathological client.
            time.sleep(0.01)
            self.served += 1
            return b"\x00"

    source = Trickle()
    # Claim a 4096-byte body; at 1 byte per 10 ms it would take 40 s to satisfy.
    header = MAGIC + struct.pack(">I", MAX_PAYLOAD_BYTES)
    primed = BytesSource(header)

    class Combined:
        def read_some(self, size: int, deadline: float | None) -> bytes:
            block = primed.read_some(size, deadline)
            return block if block else source.read_some(size, deadline)

    reader = FrameReader(Combined(), deadline_s=0.2)
    start = time.monotonic()
    with pytest.raises(FramingError) as excinfo:
        reader.read_frame()
    elapsed = time.monotonic() - start

    assert excinfo.value.reason is Reason.READ_DEADLINE_EXCEEDED
    assert elapsed < 2.0, f"the deadline did not bound the read ({elapsed:.2f}s)"
    assert source.served < MAX_PAYLOAD_BYTES, "gave up well before the body completed"
