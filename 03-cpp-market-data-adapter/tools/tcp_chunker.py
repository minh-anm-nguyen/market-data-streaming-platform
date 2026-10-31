#!/usr/bin/env python3
"""A fault-injecting MDP1 TCP server for end-to-end tests.

Serves a .pbstream over loopback with controllable pathologies: arbitrary
chunk sizes, per-chunk delays, silence before the prefix, mid-frame close,
and a corrupted magic. It is a test driver, not a Project 02 bridge, and any
report that used it must say so.

Usage:
    python3 tools/tcp_chunker.py --file f.pbstream --port 9010 \
        [--chunk 1] [--delay-us 0] [--silence-ms 0] [--sessions 1]
        [--truncate-at N] [--corrupt-magic] [--split-at N]
"""

from __future__ import annotations

import argparse
import pathlib
import socket
import sys
import time


def serve(args: argparse.Namespace) -> int:
    data = pathlib.Path(args.file).read_bytes() if args.file else b""
    if args.corrupt_magic:
        data = b"MDP2" + data[4:]
    if args.truncate_at >= 0:
        data = data[: args.truncate_at]

    # Each session gets its own payload, so a reconnect test sees two
    # separate MDP1 streams rather than one stream with a second magic
    # embedded in the middle of it.
    if args.split_at > 0:
        sessions = [data[: args.split_at], b"MDP1" + data[args.split_at:]]
    else:
        sessions = [data] * max(1, args.sessions)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(4)
    actual_port = srv.getsockname()[1]
    # Printed on its own line so a harness can read the port back when it
    # asked for an ephemeral one.
    print(f"LISTENING {actual_port}", flush=True)

    for payload in sessions:
        conn, _ = srv.accept()
        try:
            if args.silence_ms > 0:
                time.sleep(args.silence_ms / 1000.0)
            step = args.chunk if args.chunk > 0 else (len(payload) or 1)
            for off in range(0, len(payload), step):
                conn.sendall(payload[off: off + step])
                if args.delay_us > 0:
                    time.sleep(args.delay_us / 1_000_000.0)
            if args.hold_ms > 0:
                time.sleep(args.hold_ms / 1000.0)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            conn.close()

    srv.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--chunk", type=int, default=0,
                    help="bytes per send; 0 sends everything at once")
    ap.add_argument("--delay-us", type=int, default=0)
    ap.add_argument("--silence-ms", type=int, default=0,
                    help="delay before the first byte (prefix deadline test)")
    ap.add_argument("--hold-ms", type=int, default=0,
                    help="keep the connection open after the last byte")
    ap.add_argument("--sessions", type=int, default=1)
    ap.add_argument("--split-at", type=int, default=0,
                    help="serve bytes [0,N) then a fresh stream from N")
    ap.add_argument("--truncate-at", type=int, default=-1)
    ap.add_argument("--corrupt-magic", action="store_true")
    return serve(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
