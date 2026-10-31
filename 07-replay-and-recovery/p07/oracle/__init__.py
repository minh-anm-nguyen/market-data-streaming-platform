"""Independent oracles: what the pipeline *should* have produced.

Everything in this subpackage derives its answer from the sealed P01 source and
a pinned policy, never from a counter, log line or digest produced by the
component under test. That independence is the whole point: an oracle that
reads P03's own accepted count cannot detect P03 miscounting (design 0.1, 3.2).
"""
