"""Recovery: fencing, capturing a cut, and deciding whether replay is even possible.

The hard part of recovery is not restarting things; it is refusing to claim
anything once the evidence has gone. Both modules here are built around that
refusal.
"""
