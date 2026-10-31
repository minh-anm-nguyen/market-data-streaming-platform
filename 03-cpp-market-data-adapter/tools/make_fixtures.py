#!/usr/bin/env python3
"""Generates every .pbstream fixture the end-to-end matrix needs.

Fixtures are built with Google's Python protobuf runtime, deliberately not
with the C++ code under test, so a shared encoding bug cannot make a golden
file agree with a broken decoder.

Usage:
    python3 tools/make_fixtures.py --out fixtures [--protoc /path/to/protoc]
"""

from __future__ import annotations

import argparse
import json
import pathlib
import struct
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import mdp1  # noqa: E402
from mdp1 import Event  # noqa: E402

DATASET = "ds-golden"
SYMBOLS = [("EURUSD", 100000, 109000), ("GBPUSD", 100000, 125000),
           ("USDJPY", 1000, 15000)]


def quote(symbol: str, scale: int, base: int, seq: int, t: int,
          spread: int = 10) -> Event:
    return Event(dataset_id=DATASET, symbol=symbol, sequence=seq,
                 event_time_ns=t, price_scale=scale, kind="quote",
                 bid_ticks=base + (seq % 100),
                 ask_ticks=base + (seq % 100) + spread)


def tick(symbol: str, scale: int, base: int, seq: int, t: int) -> Event:
    return Event(dataset_id=DATASET, symbol=symbol, sequence=seq,
                 event_time_ns=t, price_scale=scale, kind="tick",
                 price_ticks=base + (seq % 100))


def golden_events(count: int) -> list[Event]:
    """Three symbols, 80/20 quote/tick, strictly increasing per-symbol
    sequences and event times. No anomalies at all."""
    out: list[Event] = []
    seqs = {s[0]: 0 for s in SYMBOLS}
    for i in range(count):
        name, scale, base = SYMBOLS[i % len(SYMBOLS)]
        seqs[name] += 1
        t = (i + 1) * 1_000_000
        out.append(quote(name, scale, base, seqs[name], t)
                   if i % 5 else tick(name, scale, base, seqs[name], t))
    return out


def build(out_dir: pathlib.Path, protoc: str) -> None:
    def enc(e: Event) -> bytes:
        return e.encode(protoc)

    # --- normal -------------------------------------------------------------
    normal = out_dir / "normal"
    events = golden_events(3000)
    mdp1.write_pbstream(normal / "golden.pbstream", [enc(e) for e in events],
                        dataset_id=DATASET)

    # The expected projection, derived from the inputs rather than from any
    # adapter output.
    expected = []
    for e in events:
        rec = {"dataset_id": e.dataset_id, "symbol": e.symbol,
               "sequence": e.sequence, "event_time_ns": e.event_time_ns,
               "price_scale": e.price_scale, "kind": e.kind}
        if e.kind == "quote":
            rec["bid_ticks"] = e.bid_ticks
            rec["ask_ticks"] = e.ask_ticks
        else:
            rec["price_ticks"] = e.price_ticks
        expected.append(rec)
    (out_dir / "expected").mkdir(parents=True, exist_ok=True)
    (out_dir / "expected" / "golden.expected.json").write_text(
        json.dumps(expected, indent=1) + "\n")

    # A small stream for the chunking and TCP tests.
    small = golden_events(100)
    mdp1.write_pbstream(normal / "small.pbstream", [enc(e) for e in small],
                        dataset_id=DATASET)

    # --- sequence oracles ---------------------------------------------------
    seqdir = out_dir / "sequence_oracles"
    # E2E-03: 1, 2, 4, 3(late), 2(exact dup), 2*(conflict), 5
    trace = [
        quote("EURUSD", 100000, 109000, 1, 1_000_000),
        quote("EURUSD", 100000, 109000, 2, 2_000_000),
        quote("EURUSD", 100000, 109000, 4, 4_000_000),
        quote("EURUSD", 100000, 109000, 3, 3_000_000),
        quote("EURUSD", 100000, 109000, 2, 2_000_000),
        quote("EURUSD", 100000, 109000, 2, 2_000_000, spread=40),  # conflict
        quote("EURUSD", 100000, 109000, 5, 5_000_000),
    ]
    mdp1.write_pbstream(seqdir / "e2e03.pbstream", [enc(e) for e in trace],
                        dataset_id=DATASET)
    (out_dir / "expected" / "e2e03.expected.json").write_text(json.dumps({
        "classification": ["OK", "OK", "GAP", "REORDER_LATE",
                           "DUPLICATE_EXACT", "DUPLICATE_CONFLICT", "OK"],
        "published_sequences": [1, 2, 4, 5],
        "contiguous_observed_valid": 5,
        "contiguous_published": 2,
    }, indent=1) + "\n")

    # A gap wider than the window, to exercise HUGE_GAP.
    huge = [quote("EURUSD", 100000, 109000, 1, 1_000_000),
            quote("EURUSD", 100000, 109000, 2**62, 2_000_000)]
    mdp1.write_pbstream(seqdir / "huge_gap.pbstream", [enc(e) for e in huge],
                        dataset_id=DATASET)

    # Business-invalid events interleaved with valid ones.
    biz = [
        quote("EURUSD", 100000, 109000, 1, 1_000_000),
        Event(dataset_id=DATASET, symbol="EURUSD", sequence=2,
              event_time_ns=2_000_000, price_scale=100000, kind="quote",
              bid_ticks=-5, ask_ticks=10),                       # negative bid
        Event(dataset_id=DATASET, symbol="EURUSD", sequence=3,
              event_time_ns=3_000_000, price_scale=999, kind="quote",
              bid_ticks=109000, ask_ticks=109010),               # wrong scale
        Event(dataset_id=DATASET, symbol="EURUSD", sequence=0,
              event_time_ns=4_000_000, price_scale=100000, kind="quote",
              bid_ticks=109000, ask_ticks=109010),               # sequence 0
        quote("EURUSD", 100000, 109000, 4, 5_000_000),
    ]
    mdp1.write_pbstream(seqdir / "business_invalid.pbstream",
                        [enc(e) for e in biz], dataset_id=DATASET)

    # --- freshness oracles --------------------------------------------------
    fresh = out_dir / "freshness_oracles"
    # E2E-04: two good quotes, then one 9 s behind the watermark.
    stale = [
        quote("EURUSD", 100000, 109000, 1, 10_000_000_000),
        quote("EURUSD", 100000, 109000, 2, 10_100_000_000),
        quote("EURUSD", 100000, 109000, 3, 1_000_000_000),  # EVENT_TIME_OLD
        quote("EURUSD", 100000, 109000, 4, 10_200_000_000),
    ]
    mdp1.write_pbstream(fresh / "event_time_old.pbstream",
                        [enc(e) for e in stale], dataset_id=DATASET)

    # A historical replay: timestamps from a year ago, increasing normally.
    base_t = 1_700_000_000 * 1_000_000_000
    hist = [quote("EURUSD", 100000, 109000, i, base_t + i * 1_000_000_000)
            for i in range(1, 51)]
    mdp1.write_pbstream(fresh / "historical_replay.pbstream",
                        [enc(e) for e in hist], dataset_id=DATASET)

    # One quote then ticks only: quote inactivity without a sequence gap.
    tick_only = [quote("EURUSD", 100000, 109000, 1, 1_000_000)]
    tick_only += [tick("EURUSD", 100000, 109000, i, i * 1_000_000)
                  for i in range(2, 30)]
    mdp1.write_pbstream(fresh / "tick_only.pbstream",
                        [enc(e) for e in tick_only], dataset_id=DATASET)

    # --- malformed MDP1 framing ---------------------------------------------
    bad = out_dir / "malformed_mdp1"
    one = enc(Event(dataset_id=DATASET))
    cases = {
        "bad_magic.pbstream": b"JSON" + mdp1.frame(one),
        "wrong_protocol.pbstream": b"MDP2" + mdp1.frame(one),
        "no_magic.pbstream": mdp1.frame(one),
        "length_zero.pbstream": mdp1.MAGIC + struct.pack(">I", 0) + one,
        "length_too_big.pbstream": mdp1.MAGIC + struct.pack(">I", 4097) + one,
        "length_uint32_max.pbstream":
            mdp1.MAGIC + struct.pack(">I", 0xFFFFFFFF) + one,
        "truncated_prefix.pbstream": b"MD",
        "truncated_header.pbstream": mdp1.MAGIC + b"\x00\x00",
        "truncated_body.pbstream":
            mdp1.MAGIC + struct.pack(">I", len(one)) + one[:-3],
        "second_magic_midstream.pbstream":
            mdp1.MAGIC + mdp1.frame(one) + mdp1.MAGIC + mdp1.frame(one),
        "trailing_garbage.pbstream":
            mdp1.MAGIC + mdp1.frame(one) + b"\x00\x01\x02",
    }
    for name, data in cases.items():
        mdp1.write_raw(bad / name, data)

    # --- malformed protobuf --------------------------------------------------
    badpb = out_dir / "malformed_protobuf"
    pb_cases = {
        "runaway_varint.pbstream": b"\xff" * 12,
        "truncated_field.pbstream": b"\x0a\x7f",
        "reserved_wire_type.pbstream": b"\x0f\x01",
        "tag_without_value.pbstream": b"\x08",
    }
    for name, payload in pb_cases.items():
        mdp1.write_raw(badpb / name, mdp1.stream([payload]))

    # Wire-valid but structurally incomplete messages.
    struct_cases = {
        "missing_symbol.pbstream": Event(symbol=None),
        "missing_sequence.pbstream": Event(sequence=None),
        "missing_event_time.pbstream": Event(event_time_ns=None),
        "missing_price_scale.pbstream": Event(price_scale=None),
        "missing_version.pbstream": Event(source_schema_version=None),
        "no_oneof.pbstream": Event(kind=None),
        "quote_without_bid.pbstream": Event(kind="quote", bid_ticks=None),
        "tick_without_price.pbstream": Event(kind="tick", price_ticks=None),
        "wrong_schema_version.pbstream": Event(source_schema_version="sim.v2"),
        "symbol_charset.pbstream": Event(symbol="eur usd"),
        "symbol_too_long.pbstream": Event(symbol="A" * 33),
        "empty_dataset_id.pbstream": Event(dataset_id=""),
    }
    for name, e in struct_cases.items():
        mdp1.write_raw(badpb / name, mdp1.stream([enc(e)]))

    # --- fault_observe corpus and its plan -----------------------------------
    # The plan is the oracle: tools/fault_sink.py scores a fault_observe run
    # against it and fails on a miss OR on a flag the plan does not contain.
    faults = out_dir / "fault_observe"
    def q(seq, t_sec, bid=109000):
        return Event(dataset_id=DATASET, symbol="EURUSD", sequence=seq,
                     event_time_ns=t_sec * 1_000_000_000, price_scale=100000,
                     kind="quote", bid_ticks=bid, ask_ticks=bid + 10)

    injected = [
        q(1, 1), q(2, 2), q(3, 3),
        q(5, 5),              # GAP over 4
        q(4, 4),              # REORDER_LATE fills it
        q(6, 6),
        q(6, 6),              # DUPLICATE_EXACT
        q(7, 7),
        q(7, 7, 109500),      # DUPLICATE_CONFLICT (same id, different price)
        q(8, 1),              # EVENT_TIME_OLD: 6 s behind the watermark
        q(9, 9),
    ]
    mdp1.write_pbstream(faults / "injected.pbstream",
                        [enc(e) for e in injected], dataset_id=DATASET)
    (faults / "plan.json").write_text(json.dumps({
        "dataset_id": DATASET,
        "faults": [
            {"symbol": "EURUSD", "sequence": 5, "kind": "GAP"},
            {"symbol": "EURUSD", "sequence": 4, "kind": "REORDER_LATE"},
            {"symbol": "EURUSD", "sequence": 6, "kind": "DUPLICATE_EXACT"},
            {"symbol": "EURUSD", "sequence": 7, "kind": "DUPLICATE_CONFLICT"},
            {"symbol": "EURUSD", "sequence": 8, "kind": "EVENT_TIME_OLD"},
        ],
    }, indent=1) + "\n")

    # --- provenance variants -------------------------------------------------
    prov = out_dir / "provenance"
    data = [enc(e) for e in golden_events(20)]
    mdp1.write_pbstream(prov / "no_manifest.pbstream", data, manifest=False)
    mdp1.write_pbstream(prov / "in_progress.pbstream", data,
                        status="IN_PROGRESS", dataset_id=DATASET)
    full = mdp1.write_pbstream(prov / "digest_mismatch.pbstream", data,
                               dataset_id=DATASET)
    # Flip a byte after the manifest was written.
    corrupted = bytearray(full)
    corrupted[-1] ^= 0x01
    (prov / "digest_mismatch.pbstream").write_bytes(bytes(corrupted))

    print(f"fixtures written to {out_dir}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="fixtures", type=pathlib.Path)
    ap.add_argument("--protoc", default="protoc")
    args = ap.parse_args()
    build(args.out, args.protoc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
