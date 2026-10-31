"""FX/CFD market data simulator (Project 01).

Generates SYNTHETIC tick/quote data for testing downstream market-data systems.
This is not market data and must not be used for trading.
"""

from .errors import (
    ConfigError,
    FaultPlanError,
    OutputError,
    ReplayError,
    RuntimeFailure,
    SimError,
    VerifyError,
)
from .model import EVENT_QUOTE, EVENT_TICK, SCHEMA_VERSION, Event

__all__ = [
    "SCHEMA_VERSION",
    "EVENT_QUOTE",
    "EVENT_TICK",
    "Event",
    "SimError",
    "ConfigError",
    "FaultPlanError",
    "OutputError",
    "ReplayError",
    "RuntimeFailure",
    "VerifyError",
]

__version__ = "0.1.0"
