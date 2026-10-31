"""A deliberately badly behaved TCP client, for the slow-consumer scenarios.

This is the ``throttle_client`` fault from section 7.1: it is driven from the
consumer side, which is why the simulator records it as not-applied rather than
claiming it injected something.

    python scripts/slow_client.py --port 9009 --mode stall
    python scripts/slow_client.py --port 9009 --mode byte --bytes-per-read 1
    python scripts/slow_client.py --port 9009 --mode throttle --read-delay-ms 50

Modes:
    fast      read as fast as possible and report framing statistics
    byte      read N bytes per call, to exercise record reassembly (case N06)
    throttle  read slowly, so the server's queue builds up (case N03)
    stall     connect and never read at all (slowloris-like)

It reports what it *observed*, never what was "sent": the only thing a client
can honestly report is what arrived at its socket.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time


class FramingStats:
    """Counts what a correct delimiter-based consumer would see."""

    def __init__(self) -> None:
        self.reads = 0
        self.bytes_read = 0
        self.records = 0
        self.max_records_in_one_read = 0
        self.reads_with_partial_record = 0
        self.unparseable = 0
        self.leftover_bytes = 0
        self.first_record = None
        self.last_record = None

    def as_dict(self) -> dict:
        return {
            "reads": self.reads,
            "bytes_read": self.bytes_read,
            "records_observed": self.records,
            "max_records_in_one_read": self.max_records_in_one_read,
            "reads_ending_mid_record": self.reads_with_partial_record,
            "unparseable_records": self.unparseable,
            "leftover_bytes_at_eof": self.leftover_bytes,
            "first_record": self.first_record,
            "last_record": self.last_record,
            "note": (
                "Counts what arrived at this socket. A record the server queued "
                "or wrote but that never arrived is invisible here, which is "
                "exactly why the server does not claim delivery."
            ),
        }


async def consume(reader, *, mode: str, chunk: int, delay_s: float, limit: int | None):
    stats = FramingStats()
    buffer = bytearray()
    while True:
        if limit is not None and stats.records >= limit:
            break
        if delay_s:
            await asyncio.sleep(delay_s)
        data = await reader.read(chunk)
        if not data:
            break
        stats.reads += 1
        stats.bytes_read += len(data)
        buffer += data

        in_this_read = 0
        while True:
            index = buffer.find(b"\n")
            if index == -1:
                break
            line = bytes(buffer[:index])
            del buffer[: index + 1]
            stats.records += 1
            in_this_read += 1
            try:
                record = json.loads(line)
            except ValueError:
                stats.unparseable += 1
                continue
            if stats.first_record is None:
                stats.first_record = record
            stats.last_record = record
        stats.max_records_in_one_read = max(stats.max_records_in_one_read, in_this_read)
        if buffer:
            stats.reads_with_partial_record += 1

    stats.leftover_bytes = len(buffer)
    return stats


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--mode", choices=("fast", "byte", "throttle", "stall"), default="fast"
    )
    parser.add_argument("--bytes-per-read", type=int, default=65536)
    parser.add_argument("--read-delay-ms", type=float, default=0.0)
    parser.add_argument(
        "--max-records", type=int, help="stop after this many records (default: EOF)"
    )
    parser.add_argument(
        "--stall-seconds", type=float, default=30.0, help="how long to hold a stalled connection"
    )
    args = parser.parse_args()

    reader, writer = await asyncio.open_connection(args.host, args.port)
    started = time.monotonic()
    try:
        if args.mode == "stall":
            # Connect and never read. The server must disconnect us on its own
            # timeout rather than grow its queue (case N03).
            await asyncio.sleep(args.stall_seconds)
            stats = FramingStats()
        else:
            chunk = 1 if args.mode == "byte" else args.bytes_per_read
            if args.mode == "byte":
                chunk = max(1, args.bytes_per_read if args.bytes_per_read < 16 else 1)
            delay = args.read_delay_ms / 1000.0 if args.mode == "throttle" else 0.0
            stats = await consume(
                reader, mode=args.mode, chunk=chunk, delay_s=delay, limit=args.max_records
            )
    finally:
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), 5.0)
        except (asyncio.TimeoutError, OSError):
            pass

    elapsed = time.monotonic() - started
    print(
        json.dumps(
            {
                "mode": args.mode,
                "elapsed_s": round(elapsed, 3),
                "observed_records_per_sec": (
                    round(stats.records / elapsed, 1) if elapsed > 0 else None
                ),
                **stats.as_dict(),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
