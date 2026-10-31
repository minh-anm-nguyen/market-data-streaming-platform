#!/usr/bin/env python3
"""Generate the golden key-to-partition mapping that the C++ startup audit checks.

This is the independent oracle case B-07 asks for. The murmur2 below is written
from the Kafka specification of ``org.apache.kafka.common.utils.Utils.murmur2``,
in Python, by hand -- deliberately *not* by calling into librdkafka or by
importing kafka-python's partitioner. If both sides of the comparison came from
the same implementation the audit would only prove that the implementation
agrees with itself.

The partition is ``(murmur2(key) & 0x7fffffff) % partition_count``, which is
what librdkafka's ``murmur2_random`` partitioner computes for a non-null key.

Run::

    python3 tools/gen_partition_golden.py --partitions 8 \\
        --out config/topic-partition-golden.json

and commit the result. A change to the hash, the key encoding or the partition
count then fails the audit at startup instead of silently repartitioning a topic
that something claims per-key ordering for.
"""

from __future__ import annotations

import argparse
import json
import sys

# Kafka murmur2 constants.
SEED = 0x9747B28C
M = 0x5BD1E995
R = 24
MASK32 = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """Kafka-compatible murmur2 over raw bytes, returned as an unsigned 32-bit int.

    The Java original works on a signed ``int`` with ``>>>`` (logical) shifts.
    Doing everything unsigned and masking to 32 bits after each multiply
    reproduces it exactly, and avoids the trap of Python integers being
    arbitrary precision: without the mask, ``h *= M`` would grow without bound
    and the tail mixing would diverge from Java after the first block.
    """
    length = len(data)
    h = (SEED ^ length) & MASK32
    blocks = length // 4

    for i in range(blocks):
        i4 = i * 4
        k = (
            data[i4]
            | (data[i4 + 1] << 8)
            | (data[i4 + 2] << 16)
            | (data[i4 + 3] << 24)
        ) & MASK32
        k = (k * M) & MASK32
        k ^= k >> R
        k = (k * M) & MASK32
        h = (h * M) & MASK32
        h ^= k

    # The Java switch falls through; only the length-1 arm multiplies by m.
    tail = length & ~3
    remainder = length % 4
    if remainder == 3:
        h ^= (data[tail + 2] << 16) & MASK32
        h ^= (data[tail + 1] << 8) & MASK32
        h ^= data[tail]
        h = (h * M) & MASK32
    elif remainder == 2:
        h ^= (data[tail + 1] << 8) & MASK32
        h ^= data[tail]
        h = (h * M) & MASK32
    elif remainder == 1:
        h ^= data[tail]
        h = (h * M) & MASK32

    h ^= h >> 13
    h = (h * M) & MASK32
    h ^= h >> 15
    return h & MASK32


def partition_for(key: str, partition_count: int) -> int:
    """Expected partition for a key, matching librdkafka murmur2_random."""
    if not key:
        raise ValueError("an empty key must be rejected before the producer, not hashed")
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    # A mask, not abs(): abs(INT_MIN) overflows in Java and the mask is what the
    # reference implementation actually uses.
    return (murmur2(key.encode("ascii")) & 0x7FFFFFFF) % partition_count


#: The three symbols E2E-02 names, plus a spread of generated keys so the golden
#: file covers short, 4-byte-aligned and long keys, and every tail length of the
#: murmur2 switch (0, 1, 2 and 3 bytes).
DEFAULT_KEYS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "AUDUSD",
    "USDCHF",
    "USDCAD",
    "NZDUSD",
    "EURGBP",
    "EURJPY",
    "GBPJPY",
    "XAUUSD",
    "XAGUSD",
    "ABC",
    "ABCD",
    "ABCDE",
    "ABCDEF",
    "A1.B2_C3",
    "US500",
    "GER40",
    "UK100",
    "SPX",
    "NDX",
    "WTI",
    "BRENT",
    "BTCUSD",
    "ETHUSD",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--out", default="config/topic-partition-golden.json")
    parser.add_argument(
        "--keys",
        help="comma-separated keys; defaults to the built-in coverage set",
    )
    parser.add_argument(
        "--extra-generated",
        type=int,
        default=24,
        help="additional SYMnnn keys, so the set is wider than hand-picked names",
    )
    args = parser.parse_args()

    keys = (
        [k for k in (args.keys.split(",") if args.keys else []) if k]
        or list(DEFAULT_KEYS)
    )
    for i in range(args.extra_generated):
        keys.append(f"SYM{i:03d}")

    mapping = {key: partition_for(key, args.partitions) for key in sorted(set(keys))}

    # A mapping that lands every key on one partition would pass an audit while
    # proving nothing about the hash, so refuse to emit one.
    distinct = len(set(mapping.values()))
    if args.partitions > 1 and distinct < 2:
        print(
            f"refusing to write a golden file where all {len(mapping)} keys map to "
            f"one partition; that would not detect a hash change",
            file=sys.stderr,
        )
        return 2

    document = {
        "generated_by": "tools/gen_partition_golden.py",
        "algorithm": "kafka murmur2, (hash & 0x7fffffff) % partition_count",
        "partitioner": "murmur2_random",
        "partition_count": args.partitions,
        "key_encoding": "ascii",
        "mapping": mapping,
    }
    with open(args.out, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(document, handle, indent=2, sort_keys=False)
        handle.write("\n")

    print(
        f"wrote {args.out}: {len(mapping)} keys over {args.partitions} partitions, "
        f"{distinct} distinct partitions used"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
