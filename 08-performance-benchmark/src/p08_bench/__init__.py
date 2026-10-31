"""P08 benchmark harness, metrology kernel and verdict analyzer.

Scope, stated here because the package is easy to misread as a performance
claim: this code **measures**, it does not assert that any throughput or latency
target was met. Every number it produces is tied to a run manifest, a host
profile and an evidence bundle, and a run without those comes out
``INVALID`` or ``INCONCLUSIVE`` rather than fast.

The pipeline it measures (P01-P06) is only partly implemented in this
repository. Lanes that need Kafka, ClickHouse or the C++ adapter report
``BLOCKED_BY_UPSTREAM_IMPLEMENTATION`` through
:class:`p08_bench.errors.UpstreamUnavailable`; they never silently pass.

Workload data is synthetic. Not market data, not for trading.
"""

from __future__ import annotations

__version__ = "0.1.0"

#: Report schema version, written into every emitted report (section 25).
REPORT_VERSION = "p08.v1"

#: Manifest schema version (section 21.1).
MANIFEST_VERSION = "p08.v1"

__all__ = ["__version__", "REPORT_VERSION", "MANIFEST_VERSION"]
