# Benchmarks — what has actually been measured

**Capacity status: `NOT_ESTABLISHED`.**

Numbers were measured. A sustainable throughput was not, and this document
does not claim one. That distinction is the point of the page.

## Environment

| | |
|---|---|
| CPU | Intel Core i5-10300H @ 2.50 GHz, 8 logical cores |
| RAM | 15.9 GiB |
| Kernel | Linux 6.6.87.2-microsoft-standard-WSL2 |
| Host | **WSL2 on Windows**, shared with a desktop workload |
| Compiler | g++ 11.4.0, `-O3 -DNDEBUG` (the `perf` flavour) |
| protobuf | 3.12.4 (protoc and libprotobuf from one pinned prefix) |
| CPU pinning | none |
| Frequency governor | not controlled |
| Workload | in-memory corpus, 300 000 events, 3 symbols, 80/20 quote/tick, mean payload 49 B |

This environment is **not** suitable for a capacity verdict: a shared laptop
under a hypervisor, with no CPU pinning and no governor control. The numbers
below are useful for catching a regression of the right order of magnitude.
They are not a figure to quote.

## Results — 5 runs per lane, 300 000 events each

| Lane | Median events/s | Range | What it does **not** establish |
|---|---:|---|---|
| A — decode-only | 1 408 951 | 1 275 190 – 1 606 433 | TCP or P04 capacity; no validation or state work included |
| B — classify-only | 1 182 532 | 778 189 – 1 418 953 | parser or network latency |
| C — offline-full | 128 294 | 100 456 – 232 610 | durable downstream throughput; `metrics_only` writes nothing |

Classification latency, lane B (300 000 samples, single run):

| p50 | p95 | p99 | p99.9 | max |
|---:|---:|---:|---:|---:|
| 288 ns | 400 ns | 496 ns | 1 600 ns | 9.9 ms |

Processing latency, lane C (framer-complete to classifier-result):

| p50 | p95 | p99 | p99.9 | max |
|---:|---:|---:|---:|---:|
| 992 ns | 3 072 ns | 3 968 ns | 57 344 ns | 10.6 ms |

Resources during lane C: peak RSS 61 MB, peak queue 4 096 events / 1.2 MB,
per-key sequence state 590 688 B for 3 keys. Conservation equations held.

## Reading these numbers

**The 120 000 events/s milestone is `INCONCLUSIVE` here.** Lane C's median is
above it; its slowest run is well below it. A 2.3× spread across identical
runs means the environment, not the adapter, dominates the measurement.
Reporting "120k achieved" from this data would be exactly the false claim
the design document warns against.

**Lane C is limited by the hand-off, not by parsing.** Decoding is about 11×
faster than the full pipeline, and the queue sat at its 4 096-event cap
throughout, so the sink writer thread — not the framer, the validators or
the state engines — sets the pace. That is where the next optimisation
belongs, and only after it can be measured on a machine that holds still.

**The ~10 ms maxima are environment artefacts**, almost certainly scheduler
preemption or first-touch page faults rather than a tail in the adapter:
p99.9 is three orders of magnitude lower.

## One change made from these measurements

`BoundedQueue::try_push` originally signalled the condition variable on
every push. Above a million events per second that futex wake dominated the
hand-off. It now signals only on the empty → non-empty edge, the only
transition that can have a waiter: the writer waits solely when it found the
queue empty, and its wait is bounded, so a missed wake costs one timeout
rather than an event.

Lane C median before: **104 376/s**. After: **128 294/s** (+23 %). The full
suite, both sanitizers and the e2e matrix were re-run afterwards.

## What must still happen before any capacity claim

1. A machine that holds still: bare metal or a dedicated VM, CPU pinning, a
   fixed frequency governor, no desktop workload.
2. The load progression from design section 12.2 — 1k, 10k, 50k, 120k
   events/s — each held for ten minutes against a **sustained** criterion:
   correctness holds, queues stay bounded, no backlog grows.
3. Lanes D (live TCP) and E (P04 hand-off), which have not been measured at
   all, and lane F (adversarial input) likewise.
4. A 6–24 hour soak (`scripts/run_soak.sh`) read for RSS and
   file-descriptor drift, not merely for the absence of a crash.

Until all four are done, every report this binary writes carries
`sustainable_throughput_verified: false`, and that is not a placeholder.

## Reproducing

```bash
./scripts/build.sh perf
~/.cache/p03-build/perf/md_adapter_bench --lane decode-only   --events 300000
~/.cache/p03-build/perf/md_adapter_bench --lane classify-only --events 300000
~/.cache/p03-build/perf/md_adapter_bench --lane offline-full  --events 300000
```

Each lane prints its own `does_not_establish` line. Quote it with the number.
