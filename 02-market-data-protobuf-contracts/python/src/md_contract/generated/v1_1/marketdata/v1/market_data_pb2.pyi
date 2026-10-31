from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Quote(_message.Message):
    __slots__ = ("bid_ticks", "ask_ticks")
    BID_TICKS_FIELD_NUMBER: _ClassVar[int]
    ASK_TICKS_FIELD_NUMBER: _ClassVar[int]
    bid_ticks: int
    ask_ticks: int
    def __init__(self, bid_ticks: _Optional[int] = ..., ask_ticks: _Optional[int] = ...) -> None: ...

class Tick(_message.Message):
    __slots__ = ("price_ticks",)
    PRICE_TICKS_FIELD_NUMBER: _ClassVar[int]
    price_ticks: int
    def __init__(self, price_ticks: _Optional[int] = ...) -> None: ...

class MarketDataEvent(_message.Message):
    __slots__ = ("dataset_id", "symbol", "sequence", "event_time_ns", "price_scale", "source_schema_version", "feed_label", "quote", "tick")
    DATASET_ID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_NS_FIELD_NUMBER: _ClassVar[int]
    PRICE_SCALE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    FEED_LABEL_FIELD_NUMBER: _ClassVar[int]
    QUOTE_FIELD_NUMBER: _ClassVar[int]
    TICK_FIELD_NUMBER: _ClassVar[int]
    dataset_id: str
    symbol: str
    sequence: int
    event_time_ns: int
    price_scale: int
    source_schema_version: str
    feed_label: str
    quote: Quote
    tick: Tick
    def __init__(self, dataset_id: _Optional[str] = ..., symbol: _Optional[str] = ..., sequence: _Optional[int] = ..., event_time_ns: _Optional[int] = ..., price_scale: _Optional[int] = ..., source_schema_version: _Optional[str] = ..., feed_label: _Optional[str] = ..., quote: _Optional[_Union[Quote, _Mapping]] = ..., tick: _Optional[_Union[Tick, _Mapping]] = ...) -> None: ...
