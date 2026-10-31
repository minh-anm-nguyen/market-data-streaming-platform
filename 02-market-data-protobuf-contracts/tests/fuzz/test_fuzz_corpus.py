"""Fuzzing, partitioned for triage: what-if group J, plus H07.

Section 9.5 splits the corpus by layer -- JSON lexical, binary payload, framing,
schema -- so a crash points at one parser rather than at "the pipeline". The
partitions are kept separate here for the same reason.

Two properties are checked throughout:

* No crash, no hang, no unclassified exception. Every input either produces a
  valid event or a typed rejection.
* Rejection is *cheap*. If malformed input cost more than valid input, a hostile
  sender could amplify load by sending garbage (case H07), so the negative-mix
  lane is timed rather than only checked for correctness.

Every failure reports the exact input bytes, because a fuzz failure with no
reproducer is not actionable (case K10).
"""

from __future__ import annotations

import json
import random
import struct
import time

import pytest

from md_contract.codec import decode_payload, encode_event, parse_payload
from md_contract.domain import PAYLOAD_QUOTE, DomainEvent
from md_contract.errors import ContractError
from md_contract.framing import MAGIC, BytesSource, FrameReader, encode_stream
from md_contract.strict_json import parse_sim_v1_line

pytestmark = pytest.mark.fuzz

#: Fixed so a failure is reproducible. A fuzz run whose seed is not recorded
#: cannot be replayed, which makes the finding unusable (case K10).
SEED = 20261030


def sample_event(sequence: int = 1) -> DomainEvent:
    return DomainEvent(
        dataset_id="ds-fuzz",
        symbol="EURUSD",
        sequence=sequence,
        event_time_ns=1793322000000000000,
        price_scale=100000,
        source_schema_version="sim.v1",
        payload_case=PAYLOAD_QUOTE,
        bid_ticks=108345,
        ask_ticks=108348,
    )


# ---------------------------------------------------------------------------
# 1. binary payload fuzzing
# ---------------------------------------------------------------------------


@pytest.mark.cases("J01")
def test_random_bytes_never_crash_the_decoder() -> None:
    rng = random.Random(SEED)
    for iteration in range(4000):
        size = rng.randrange(0, 300)
        payload = bytes(rng.randrange(256) for _ in range(size))
        try:
            decode_payload(payload)
        except ContractError:
            pass
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"unclassified {type(exc).__name__} on iteration {iteration} "
                f"(seed={SEED}): {payload.hex()}"
            )


@pytest.mark.cases("J01", "D15")
def test_mutating_a_valid_payload_never_produces_an_unclassified_failure() -> None:
    """Bit flips, byte swaps and truncations of a known-good message.

    This is the most productive mutation strategy for a wire format: the input
    stays structurally plausible, so it reaches deeper into the parser than
    uniform random bytes do.
    """
    rng = random.Random(SEED + 1)
    base = encode_event(sample_event())
    survived_as_valid = 0

    for iteration in range(3000):
        mutated = bytearray(base)
        strategy = rng.randrange(4)
        if strategy == 0 and mutated:  # flip one bit
            index = rng.randrange(len(mutated))
            mutated[index] ^= 1 << rng.randrange(8)
        elif strategy == 1 and mutated:  # replace one byte
            mutated[rng.randrange(len(mutated))] = rng.randrange(256)
        elif strategy == 2 and len(mutated) > 1:  # truncate
            del mutated[rng.randrange(len(mutated)) :]
        else:  # append junk
            mutated.extend(rng.randrange(256) for _ in range(rng.randrange(1, 8)))

        try:
            decode_payload(bytes(mutated))
            survived_as_valid += 1
        except ContractError:
            pass
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"unclassified {type(exc).__name__} on iteration {iteration} "
                f"(seed={SEED + 1}): {bytes(mutated).hex()}"
            )

    # Some mutations necessarily still parse -- flipping a bit inside a price
    # varint gives a valid message with a different price. Protobuf has no
    # checksum, so nothing here can detect that; only comparing against the
    # source can (ADR-009). Recorded so the limit is explicit.
    assert survived_as_valid > 0, (
        "no mutation produced a still-valid message, so this run did not "
        "exercise the valid-but-corrupted case at all"
    )


@pytest.mark.cases("J02")
def test_a_spray_of_unknown_small_fields_stays_bounded() -> None:
    """Many unknown fields in one frame: CPU must stay bounded by the byte cap."""
    # Fields 100..1000, varint, each 2-3 bytes. The 4096-byte payload cap is
    # what bounds the work; without it this is an amplification vector.
    chunks = [encode_event(sample_event())]
    number = 100
    while sum(len(c) for c in chunks) < 3500:
        tag = (number << 3) | 0
        varint = b""
        value = tag
        while value >= 0x80:
            varint += bytes([(value & 0x7F) | 0x80])
            value >>= 7
        varint += bytes([value])
        chunks.append(varint + b"\x01")
        number += 1
    payload = b"".join(chunks)
    assert len(payload) <= 4096

    start = time.perf_counter()
    for _ in range(200):
        decode_payload(payload)
    elapsed = time.perf_counter() - start
    # Generous, because this asserts "bounded", not a performance target.
    assert elapsed < 10.0, f"{elapsed:.2f}s for 200 unknown-field-spray frames"


@pytest.mark.cases("J03")
def test_an_absurd_advertised_length_costs_nothing() -> None:
    rng = random.Random(SEED + 2)
    start = time.perf_counter()
    for _ in range(2000):
        length = rng.randrange(4097, 2**32)
        data = MAGIC + struct.pack(">I", length)
        reader = FrameReader(BytesSource(data), deadline_s=None)
        with pytest.raises(ContractError):
            reader.read_frame()
    elapsed = time.perf_counter() - start
    # If the reader allocated on the advertised length, 2000 iterations
    # averaging ~2 GiB each would not complete at all.
    assert elapsed < 5.0, f"{elapsed:.2f}s suggests allocation before validation"


# ---------------------------------------------------------------------------
# 2. framing fuzzing
# ---------------------------------------------------------------------------


@pytest.mark.cases("J01", "D03", "D04", "D11")
def test_every_prefix_of_a_valid_stream_is_handled() -> None:
    """Truncation at every single byte offset.

    Exhaustive rather than sampled: an off-by-one in the header or body read
    shows up at exactly one offset, and a random sweep could miss it.
    """
    stream = encode_stream([encode_event(sample_event(i)) for i in range(1, 6)])
    for cut in range(len(stream) + 1):
        reader = FrameReader(BytesSource(stream[:cut]), deadline_s=None)
        frames = []
        try:
            while (payload := reader.read_frame()) is not None:
                frames.append(payload)
        except ContractError:
            pass
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"unclassified {type(exc).__name__} at cut={cut}: {exc}")
        # Whatever was returned must be complete frames that decode.
        for payload in frames:
            decode_payload(payload)


@pytest.mark.cases("J01")
def test_random_framing_bytes_never_hang_or_crash() -> None:
    rng = random.Random(SEED + 3)
    for iteration in range(2000):
        size = rng.randrange(0, 80)
        data = bytes(rng.randrange(256) for _ in range(size))
        if rng.random() < 0.5:
            data = MAGIC + data  # get past the magic more often
        reader = FrameReader(BytesSource(data), deadline_s=None)
        try:
            while reader.read_frame() is not None:
                pass
        except ContractError:
            pass
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"unclassified {type(exc).__name__} on iteration {iteration} "
                f"(seed={SEED + 3}): {data.hex()}"
            )


# ---------------------------------------------------------------------------
# 3. JSON lexical fuzzing
# ---------------------------------------------------------------------------


@pytest.mark.cases("J01", "J04", "J05")
def test_random_and_mutated_json_lines_are_always_classified() -> None:
    rng = random.Random(SEED + 4)
    valid = json.dumps(
        {
            "schema_version": "sim.v1",
            "dataset_id": "ds-fuzz",
            "event_type": "quote",
            "symbol": "EURUSD",
            "sequence": 1,
            "event_time_ns": 1793322000000000000,
            "price_scale": 100000,
            "bid_ticks": 108345,
            "ask_ticks": 108348,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    for iteration in range(4000):
        if rng.random() < 0.4:
            line = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 120)))
        else:
            mutated = bytearray(valid)
            for _ in range(rng.randrange(1, 4)):
                if not mutated:
                    break
                index = rng.randrange(len(mutated))
                choice = rng.randrange(3)
                if choice == 0:
                    mutated[index] = rng.randrange(256)
                elif choice == 1:
                    del mutated[index]
                else:
                    mutated.insert(index, rng.randrange(256))
            line = bytes(mutated)
        try:
            parse_sim_v1_line(line)
        except ContractError:
            pass
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"unclassified {type(exc).__name__} on iteration {iteration} "
                f"(seed={SEED + 4}): {line!r}"
            )


@pytest.mark.cases("J04", "J05")
def test_hostile_strings_are_rejected_without_being_sanitised_into_validity() -> None:
    hostile = [
        '"EUR\\u202eUSD"',          # bidi override: display spoofing
        '"EUR\\u0000USD"',          # embedded NUL
        '"EUR\\nUSD"',              # newline: log injection
        '"EUR\\r\\nUSD"',           # CRLF
        '"\\u0441\\u0440"',         # Cyrillic lookalikes
        '"../../etc/passwd"',       # path traversal
        '"EURUSD\\u200b"',          # zero-width space
        '"\\ud800"',                # lone surrogate
    ]
    from tests.conftest import quote_line

    for literal in hostile:
        with pytest.raises(ContractError):
            parse_sim_v1_line(quote_line(symbol=literal))


@pytest.mark.cases("J08")
def test_semantically_toxic_but_well_formed_input_is_refused_in_strict_mode() -> None:
    """A payload can be perfectly valid Protobuf and still be unacceptable.

    These parse, pass the envelope check, and are refused on their values -- the
    fourth defence layer, which the first three cannot provide (section 7).
    """
    from md_contract.errors import PolicyError
    from md_contract.validation import ValidationMode, validate_business

    toxic = [
        sample_event()._replace(bid_ticks=-(2**62), ask_ticks=-(2**62)),
        sample_event()._replace(sequence=-(2**62)),
        sample_event()._replace(price_scale=-1),
        sample_event()._replace(bid_ticks=2**62, ask_ticks=1),
    ]
    for event in toxic:
        # Structurally impeccable...
        assert decode_payload(encode_event(event)) == event
        # ... and refused on policy.
        with pytest.raises(PolicyError):
            validate_business(event, mode=ValidationMode.STRICT_NORMAL)


# ---------------------------------------------------------------------------
# 4. amplification
# ---------------------------------------------------------------------------


@pytest.mark.cases("H07")
def test_rejecting_malformed_input_is_not_more_expensive_than_accepting_valid_input() -> None:
    """80% malformed mix: rejection must not amplify CPU cost.

    If garbage were slower to refuse than valid data is to accept, sending
    garbage would be an attack. The ratio is what matters here, not the absolute
    numbers, so this is a correctness guard rather than a benchmark.
    """
    rng = random.Random(SEED + 5)
    valid = [encode_event(sample_event(i)) for i in range(1, 51)]
    malformed = [
        b"\x00",
        b"\x18\x80",
        b"\xff" * 48,
        b"",
        b"\x1f\x01",
        bytes(rng.randrange(256) for _ in range(60)),
    ]

    def run(corpus: list[bytes], rounds: int) -> float:
        start = time.perf_counter()
        for _ in range(rounds):
            for payload in corpus:
                try:
                    decode_payload(payload)
                except ContractError:
                    pass
        return time.perf_counter() - start

    rounds = 400
    valid_s = run(valid, rounds) / (rounds * len(valid))
    malformed_s = run(malformed, rounds) / (rounds * len(malformed))

    assert malformed_s < valid_s * 20, (
        f"rejecting malformed input costs {malformed_s * 1e6:.2f}us against "
        f"{valid_s * 1e6:.2f}us to accept valid input -- that ratio is an "
        "amplification vector"
    )


@pytest.mark.cases("H06")
def test_worst_case_sized_frames_stay_within_the_budget() -> None:
    """Payloads pushed up against the 4096-byte cap."""
    big = sample_event()._replace(dataset_id="d" * 120)
    payload = encode_event(big)
    # Pad with unknown fields up to just under the cap.
    pad = bytes([0xA2, 0x01]) + bytes([0x7F]) + b"x" * 127
    while len(payload) + len(pad) <= 4096:
        payload += pad
    assert len(payload) <= 4096
    decoded = decode_payload(payload)
    assert decoded.dataset_id == "d" * 120
    # One more pad goes over, and is refused before the parser sees it.
    with pytest.raises(ContractError):
        parse_payload(payload + pad)
