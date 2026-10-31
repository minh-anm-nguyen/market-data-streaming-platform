#include "md_adapter/validators.hpp"

namespace md {

StructuralLimits structural_limits_from(const Config& c) {
  StructuralLimits l;
  l.max_dataset_id_bytes = static_cast<std::size_t>(c.validation.max_dataset_id_bytes);
  l.max_symbol_bytes = static_cast<std::size_t>(c.validation.max_symbol_bytes);
  l.max_source_version_bytes =
      static_cast<std::size_t>(c.validation.max_source_version_bytes);
  l.expected_source_schema_version = c.identity.source_schema_version;
  l.reject_unknown_fields = c.validation.reject_unknown_fields;
  return l;
}

bool is_valid_utf8(std::string_view s) {
  std::size_t i = 0;
  const std::size_t n = s.size();
  while (i < n) {
    const auto c0 = static_cast<unsigned char>(s[i]);
    std::size_t need = 0;
    std::uint32_t cp = 0;
    if (c0 < 0x80u) {
      ++i;
      continue;
    } else if ((c0 & 0xE0u) == 0xC0u) {
      need = 1;
      cp = c0 & 0x1Fu;
    } else if ((c0 & 0xF0u) == 0xE0u) {
      need = 2;
      cp = c0 & 0x0Fu;
    } else if ((c0 & 0xF8u) == 0xF0u) {
      need = 3;
      cp = c0 & 0x07u;
    } else {
      return false;  // continuation byte or 5/6-byte lead
    }
    if (i + need >= n) return false;  // truncated multi-byte sequence
    for (std::size_t k = 1; k <= need; ++k) {
      const auto cc = static_cast<unsigned char>(s[i + k]);
      if ((cc & 0xC0u) != 0x80u) return false;
      cp = (cp << 6) | (cc & 0x3Fu);
    }
    // Reject overlong encodings, UTF-16 surrogates and out-of-range points.
    if (need == 1 && cp < 0x80u) return false;
    if (need == 2 && cp < 0x800u) return false;
    if (need == 3 && cp < 0x10000u) return false;
    if (cp >= 0xD800u && cp <= 0xDFFFu) return false;
    if (cp > 0x10FFFFu) return false;
    i += need + 1;
  }
  return true;
}

bool is_allowed_symbol_charset(std::string_view s) {
  if (s.empty()) return false;
  for (char ch : s) {
    const bool ok = (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
                    ch == '.' || ch == '_' || ch == '-';
    if (!ok) return false;
  }
  return true;
}

Reason validate_structural(const marketdata::v1::MarketDataEvent& m,
                           const StructuralLimits& limits, bool unknown_fields,
                           DomainEvent& out) {
  // 1. Explicit field presence. proto3 `optional` gives real has_ bits, so a
  //    default-valued field and an absent field are distinguishable -- the
  //    adapter must never treat 0/"" as "present with that value".
  if (!m.has_dataset_id()) return Reason::kMissingDatasetId;
  if (!m.has_symbol()) return Reason::kMissingSymbol;
  if (!m.has_sequence()) return Reason::kMissingSequence;
  if (!m.has_event_time_ns()) return Reason::kMissingEventTime;
  if (!m.has_price_scale()) return Reason::kMissingPriceScale;
  if (!m.has_source_schema_version()) return Reason::kMissingSourceVersion;

  // 2. Payload oneof must be set to a kind this build supports.
  if (m.payload_case() == marketdata::v1::MarketDataEvent::PAYLOAD_NOT_SET)
    return Reason::kMissingPayloadOneof;

  // 3. Byte caps before anything is copied out of the message.
  const std::string& dataset_id = m.dataset_id();
  const std::string& symbol = m.symbol();
  const std::string& version = m.source_schema_version();
  if (dataset_id.empty() || dataset_id.size() > limits.max_dataset_id_bytes)
    return Reason::kFieldTooLong;
  if (symbol.empty() || symbol.size() > limits.max_symbol_bytes)
    return Reason::kFieldTooLong;
  if (version.empty() || version.size() > limits.max_source_version_bytes)
    return Reason::kFieldTooLong;

  // 4. Charset. protobuf already rejects invalid UTF-8 in proto3 string
  //    fields, but the check is repeated here so the rule holds for any
  //    future non-protobuf input path and is directly unit-testable.
  if (!is_valid_utf8(dataset_id) || !is_valid_utf8(version))
    return Reason::kInvalidUtf8;
  if (!is_allowed_symbol_charset(symbol)) return Reason::kSymbolCharset;

  // 5. Schema version is a semantic contract, not a free-form tag.
  if (version != limits.expected_source_schema_version)
    return Reason::kUnsupportedSchemaVersion;

  if (unknown_fields && limits.reject_unknown_fields)
    return Reason::kUnknownFieldPresent;

  // 6. Payload field presence inside the chosen oneof arm. A `Quote` with no
  //    bid is structurally incomplete even though the oneof is set.
  switch (m.payload_case()) {
    case marketdata::v1::MarketDataEvent::kQuote: {
      const auto& q = m.quote();
      if (!q.has_bid_ticks() || !q.has_ask_ticks())
        return Reason::kMissingQuoteField;
      out.payload = QuotePrice{q.bid_ticks(), q.ask_ticks()};
      break;
    }
    case marketdata::v1::MarketDataEvent::kTick: {
      const auto& t = m.tick();
      if (!t.has_price_ticks()) return Reason::kMissingTickField;
      out.payload = TickPrice{t.price_ticks()};
      break;
    }
    default:
      return Reason::kMissingPayloadOneof;
  }

  // 7. Copy out verbatim. Signed values, including negative ones, survive this
  //    stage untouched; judging them is the business validator's job.
  out.dataset_id = dataset_id;
  out.symbol = symbol;
  out.sequence = m.sequence();
  out.event_time_ns = m.event_time_ns();
  out.price_scale = m.price_scale();
  out.source_schema_version = version;
  out.unknown_fields = unknown_fields;
  return Reason::kNone;
}

}  // namespace md
