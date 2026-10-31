// Typed domain model. Decoding produces these; nothing downstream touches the
// protobuf message again except the raw-bytes passthrough used by the P04 sink.
#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "md_adapter/status.hpp"

namespace md {

using PriceTicks = std::int64_t;
using EventTimeNs = std::int64_t;
using Sequence = std::int64_t;

struct QuotePrice {
  PriceTicks bid{0};
  PriceTicks ask{0};
};

struct TickPrice {
  PriceTicks price{0};
};

using Payload = std::variant<QuotePrice, TickPrice>;

enum class PayloadKind : std::uint8_t { kQuote = 0, kTick = 1 };

inline PayloadKind kind_of(const Payload& p) noexcept {
  return std::holds_alternative<QuotePrice>(p) ? PayloadKind::kQuote
                                               : PayloadKind::kTick;
}
std::string_view to_string(PayloadKind k) noexcept;

// Exact projection of marketdata.v1.MarketDataEvent after structural
// validation. Integers are carried verbatim -- no clamping, no float.
struct DomainEvent {
  std::string dataset_id;
  std::string symbol;
  Sequence sequence{0};
  EventTimeNs event_time_ns{0};
  std::int64_t price_scale{0};
  std::string source_schema_version;
  Payload payload{QuotePrice{}};

  // True when the wire message carried fields this build does not know.
  bool unknown_fields{false};
};

// The compact typed core stored per sequence slot and used for exact-duplicate
// comparison. Identity fields (dataset/symbol) are fixed by the tracker key and
// `source_schema_version` is pinned by the structural validator, so they are
// not repeated here.
struct TypedCore {
  EventTimeNs event_time_ns{0};
  std::int64_t price_scale{0};
  PriceTicks a{0};  // bid (quote) or price (tick)
  PriceTicks b{0};  // ask (quote) or 0 (tick)
  PayloadKind kind{PayloadKind::kQuote};

  friend bool operator==(const TypedCore&, const TypedCore&) = default;
};

TypedCore core_of(const DomainEvent& e) noexcept;

// Out-of-band annotation. Serializing this is NOT serializing a P02 message.
struct ClassifiedEvent {
  DomainEvent source;
  std::uint32_t quality_flags{quality::kNone};
  std::uint64_t arrival_index{0};
  FeedState feed_state{FeedState::kInitial};
  Reason primary_reason{Reason::kNone};
  // Raw wire bytes of the original MarketDataEvent, kept only when a sink needs
  // byte-exact passthrough (P04). Empty otherwise.
  std::string raw_wire;
};

}  // namespace md
