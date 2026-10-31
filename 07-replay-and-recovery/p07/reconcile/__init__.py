"""Exact reconciliation between the expected sets and what was observed.

Every function here compares *whole* sets, never samples. The lab config says
``allow_sample_only_pass: false`` and design 13.3 repeats it for the large
fixtures: "RPO/CORRECTNESS phải xét toàn bộ finite dataset, không lấy 1%
sample". A count that happens to match is not a match.
"""
