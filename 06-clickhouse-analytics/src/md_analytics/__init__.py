"""Project 06 -- ClickHouse time-series analytics and materialized 1m OHLC bars.

Correctness order, from the system design: correctness, then never publishing
something wrong, then being able to reconcile, then bounded resources, then
recovery, and only then performance.

Two things to know before using this package:

* **A tick is a price update, not a trade.** There is no volume anywhere, and
  ``tick_count`` must not be read as one (section 3.2).
* **Nothing is READY without proof.** A successful INSERT is not evidence; a
  matching ``COUNT(*)`` is not evidence. Only an independent reconciliation
  against the sealed source is (section 0.2).
"""

from __future__ import annotations

from .contracts import CONTRACT_VERSION, SourceIdentity, SourceOrder
from .errors import EvidenceClass, Verdict
from .oracle import Bar, BarKey
from .timebars import BAR_INTERVAL_NS, bar_start_ns

__version__ = "0.1.0"

__all__ = [
    "BAR_INTERVAL_NS",
    "Bar",
    "BarKey",
    "CONTRACT_VERSION",
    "EvidenceClass",
    "SourceIdentity",
    "SourceOrder",
    "Verdict",
    "__version__",
    "bar_start_ns",
]
