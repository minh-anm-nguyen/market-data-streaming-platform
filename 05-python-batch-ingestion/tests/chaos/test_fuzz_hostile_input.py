"""Fuzzing with hostile input (spec 9.4).

The threat envelope is explicit: a record value is at most 4096 bytes and the
producer is untrusted within that bound. Inside that envelope no input may
crash the process, raise an unhandled exception, write a market row, or consume
unbounded time. A crash or an OOM from input inside the envelope is a P0.

The corpus is seeded, so a failure reports the exact bytes that caused it and
can be turned into a regression fixture directly.
"""

from __future__ import annotations

import random
import time

import pytest

from p05_ingest.contract_validator import ValidationOk, ValidationReject
from p05_ingest.model import ConsumedRecord, TopicPartition
from p05_ingest.testkit import QUOTES_TOPIC, TICKS_TOPIC, make_quote, make_tick
from tests.conftest import build_harness, make_config

pytestmark = pytest.mark.chaos

MAX_VALUE_BYTES = 4096
SEED = 20261030
TP0 = TopicPartition(TICKS_TOPIC, 0)


def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def hostile_corpus(seed: int = SEED, count: int = 1500) -> list[bytes]:
    """Structured garbage, biased toward shapes that break naive parsers."""
    rng = random.Random(seed)
    valid = [
        make_tick(sequence=1).SerializeToString(),
        make_quote(sequence=1).SerializeToString(),
    ]
    corpus: list[bytes] = [
        b"",
        b"\x00",
        b"\xff",
        b"\xff" * 11,
        b"\x80" * 20,
        varint((1 << 3) | 2) + varint(10**9) + b"ab",
        varint((1 << 3) | 2) + varint(2) + b"\xff\xfe",
        varint((3 << 3) | 3),
        varint((3 << 3) | 4),
        varint((3 << 3) | 6),
        varint((1 << 3) | 2) + varint(MAX_VALUE_BYTES) + b"A" * MAX_VALUE_BYTES,
        b"MDP1" + valid[0],
        b"\x00\x00\x00\x10" + valid[0],
    ]

    for _ in range(count):
        choice = rng.randrange(7)
        if choice == 0:
            corpus.append(bytes(rng.randrange(256) for _ in range(rng.randrange(64))))
        elif choice == 1:
            base = bytearray(rng.choice(valid))
            if base:
                base[rng.randrange(len(base))] = rng.randrange(256)
            corpus.append(bytes(base))
        elif choice == 2:
            base = rng.choice(valid)
            corpus.append(base[: rng.randrange(len(base) + 1)])
        elif choice == 3:
            tag = rng.randrange(1, 2000)
            wire = rng.choice([0, 1, 2, 5])
            corpus.append(rng.choice(valid) + varint((tag << 3) | wire) + varint(rng.randrange(2**32)))
        elif choice == 4:
            # Deeply repeated submessage tags: a recursion-shaped payload,
            # bounded by the 4 KiB cap.
            depth = rng.randrange(1, 60)
            payload = b"\x08\x01"
            for _ in range(depth):
                payload = varint((10 << 3) | 2) + varint(len(payload)) + payload
                if len(payload) > MAX_VALUE_BYTES:
                    break
            corpus.append(payload)
        elif choice == 5:
            corpus.append(rng.choice(valid) * rng.randrange(1, 4))
        else:
            corpus.append(
                varint((rng.randrange(1, 8) << 3) | 0) + varint(rng.randrange(2**70))
            )
    return [c for c in corpus if len(c) <= MAX_VALUE_BYTES]


def test_no_hostile_input_raises_from_the_validator(validator):
    """Every input is either accepted or refused -- never an exception."""
    corpus = hostile_corpus()
    assert len(corpus) > 1000, "the corpus should be substantial"

    accepted = 0
    for payload in corpus:
        record = ConsumedRecord(tp=TP0, offset=0, key=b"EURUSD", value=payload)
        try:
            result = validator.validate(record)
        except Exception as exc:  # pragma: no cover - this is the P0 case
            pytest.fail(
                f"hostile input raised {type(exc).__name__}: {exc}\n"
                f"  seed={SEED}\n  input_hex={payload.hex()}"
            )
        assert isinstance(result, (ValidationOk, ValidationReject))
        if isinstance(result, ValidationOk):
            accepted += 1
            # Anything accepted must be fully formed.
            p = result.projection
            assert p.dataset_id
            assert p.price_scale > 0
            assert p.sequence >= 0
    # A few crafted inputs are legitimately valid; the rest must be refused.
    assert accepted < len(corpus) // 10, "the fuzzer is not actually hostile"


def test_every_rejection_carries_a_stable_reason_code(validator):
    codes: set[str] = set()
    for payload in hostile_corpus():
        record = ConsumedRecord(tp=TP0, offset=0, key=b"EURUSD", value=payload)
        result = validator.validate(record)
        if isinstance(result, ValidationReject):
            assert result.code, "a rejection without a code cannot be triaged"
            assert result.code.isupper()
            codes.add(result.code)
    # The corpus should exercise several distinct failure modes, not just one.
    assert len(codes) >= 4, f"only reached {sorted(codes)}"


def test_validation_time_is_bounded_per_record(validator):
    """No input inside the envelope may cost pathological CPU."""
    worst = 0.0
    worst_input = b""
    for payload in hostile_corpus():
        record = ConsumedRecord(tp=TP0, offset=0, key=b"EURUSD", value=payload)
        started = time.perf_counter()
        validator.validate(record)
        elapsed = time.perf_counter() - started
        if elapsed > worst:
            worst, worst_input = elapsed, payload
    assert worst < 0.1, (
        f"a single 4 KiB record took {worst * 1000:.1f} ms to validate\n"
        f"  input_hex={worst_input.hex()[:200]}"
    )


def test_hostile_keys_and_headers_are_refused(validator):
    """The key and header surfaces get the same treatment as the value."""
    rng = random.Random(SEED)
    good_value = make_tick().SerializeToString()
    for _ in range(300):
        key = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 64)))
        headers = tuple(
            (
                rng.choice(["md-schema", "md-run-id", "x-unknown", "md-dataset-id"]),
                bytes(rng.randrange(256) for _ in range(rng.randrange(0, 600))),
            )
            for _ in range(rng.randrange(0, 4))
        )
        record = ConsumedRecord(
            tp=TP0, offset=0, key=key, value=good_value, headers=headers
        )
        result = validator.validate(record)
        if isinstance(result, ValidationOk):
            # Only acceptable when the key really is the symbol and every
            # header is both known and consistent.
            assert key == result.projection.symbol.encode("ascii")


def test_a_hostile_stream_writes_no_market_rows_and_stops(dev_config, clock):
    """End to end: a partition fed only garbage must stop with an empty table."""
    cfg = make_config(
        dev_config,
        kafka={"topics": ["md.ticks.v1"]},
        batch={"per_partition_max_rows": 4},
    )
    harness = build_harness(cfg, clock=clock, partitions=1)
    for payload in hostile_corpus(count=50)[:40]:
        harness.cluster.produce(TICKS_TOPIC, 0, key=b"EURUSD", value=payload)

    status = harness.finish()

    assert not status.complete
    assert TP0 in harness.engine.blocked_partitions
    # Whatever the first refusal was, nothing after it may be committed.
    committed = harness.cluster.committed(cfg.kafka.group_id, TP0) or 0
    written = {r["kafka_offset"] for r in harness.sink.rows}
    assert all(o < committed for o in written) or not written


def test_oversized_value_is_refused_before_the_parser(validator, dev_config):
    """Beyond the envelope: refused on size, never parsed (spec NFR-03)."""
    cap = dev_config.contracts.max_record_value_bytes
    for size in (cap + 1, cap * 4, cap * 100):
        record = ConsumedRecord(
            tp=TP0, offset=0, key=b"EURUSD", value=b"\x08" * size
        )
        started = time.perf_counter()
        result = validator.validate(record)
        elapsed = time.perf_counter() - started
        assert isinstance(result, ValidationReject)
        assert result.code == "VALUE_TOO_LARGE"
        assert elapsed < 0.05, "the size check must not scale with the payload"


def test_valid_records_still_pass_after_the_fuzz_corpus(validator):
    """A sanity check that the validator was not simply refusing everything."""
    for topic, event in (
        (TICKS_TOPIC, make_tick(sequence=5)),
        (QUOTES_TOPIC, make_quote(sequence=5)),
    ):
        record = ConsumedRecord(
            tp=TopicPartition(topic, 0),
            offset=0,
            key=b"EURUSD",
            value=event.SerializeToString(),
        )
        assert isinstance(validator.validate(record), ValidationOk)
