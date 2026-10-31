"""Rendering: machine-readable results and the human summary beside them.

Design 2.1's rule for the reporter is the shortest in the table and the easiest
to break: it must not invent a ``PASS`` or a throughput number that was not
measured. So every renderer here only formats what the run recorded, and prints
``not measured`` where nothing was.
"""
