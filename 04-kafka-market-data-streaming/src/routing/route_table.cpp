#include "routing/route_table.h"

#include "routing/partition_policy.h"

namespace p04 {
namespace {

// The symbol alphabet is the one Project 01 enforces on its own records
// (src/market_sim/model.py: uppercase letters, digits, underscore and dot).
// Matching it exactly matters: a wider alphabet here would accept a key that
// Project 01 can never produce, and a narrower one would reject a legitimate
// dataset. Either way the two projects would disagree about what a symbol is.
bool IsAllowedSymbolByte(unsigned char c) {
  return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.';
}

bool HasLowercaseAscii(const std::string& s) {
  for (const char c : s) {
    const auto u = static_cast<unsigned char>(c);
    if (u >= 'a' && u <= 'z') return true;
  }
  return false;
}

}  // namespace

const char* ToString(RouteReject reason) {
  switch (reason) {
    case RouteReject::kNone:                     return "NONE";
    case RouteReject::kPayloadKindUnset:         return "MISSING_PAYLOAD";
    case RouteReject::kDatasetIdAbsent:          return "DATASET_ID_ABSENT";
    case RouteReject::kSymbolAbsent:             return "SYMBOL_ABSENT";
    case RouteReject::kSequenceAbsent:           return "SEQUENCE_ABSENT";
    case RouteReject::kEventTimeAbsent:          return "EVENT_TIME_ABSENT";
    case RouteReject::kPriceScaleAbsent:         return "PRICE_SCALE_ABSENT";
    case RouteReject::kSchemaVersionAbsent:      return "SCHEMA_VERSION_ABSENT";
    case RouteReject::kSchemaVersionUnexpected:  return "SCHEMA_VERSION_UNEXPECTED";
    case RouteReject::kSymbolEmpty:              return "KEY_INVALID_EMPTY";
    case RouteReject::kSymbolTooShort:           return "KEY_INVALID_TOO_SHORT";
    case RouteReject::kSymbolTooLong:            return "KEY_INVALID_TOO_LONG";
    case RouteReject::kSymbolNotCanonicalCase:   return "KEY_NOT_CANONICAL_UPPERCASE";
    case RouteReject::kSymbolCharsetInvalid:     return "KEY_CHARSET_INVALID";
    case RouteReject::kQuoteBodyIncomplete:      return "QUOTE_BODY_INCOMPLETE";
    case RouteReject::kTickBodyIncomplete:       return "TICK_BODY_INCOMPLETE";
    case RouteReject::kSequenceNegative:         return "SEQUENCE_NEGATIVE";
    case RouteReject::kPriceScaleNotPositive:    return "PRICE_SCALE_NOT_POSITIVE";
    case RouteReject::kValueTooLarge:            return "VALUE_TOO_LARGE";
    case RouteReject::kHeaderTooLarge:           return "HEADERS_TOO_LARGE";
    case RouteReject::kHeaderValueMismatch:      return "HEADER_VALUE_MISMATCH";
    case RouteReject::kTopicNotConfigured:       return "TOPIC_NOT_CONFIGURED";
  }
  return "UNMAPPED";
}

RouteTable::RouteTable(RouteConfig config) : config_(std::move(config)) {}

const std::string& RouteTable::TopicFor(EventKind kind) const {
  switch (kind) {
    case EventKind::kTick:  return config_.ticks_topic;
    case EventKind::kQuote: return config_.quotes_topic;
  }
  return empty_;
}

bool RouteTable::IsMarketTopic(const std::string& topic) const {
  return topic == config_.ticks_topic || topic == config_.quotes_topic;
}

RouteReject RouteTable::ValidateSymbol(const std::string& symbol) const {
  if (symbol.empty()) return RouteReject::kSymbolEmpty;
  if (symbol.size() > config_.max_key_bytes) return RouteReject::kSymbolTooLong;
  // Case ordering matters for diagnosis: a lowercase symbol is reported as a
  // canonicalisation failure rather than a charset failure, because the two
  // call for different fixes upstream. Checking case first is what makes
  // case B-03 distinguishable from case B-04.
  if (HasLowercaseAscii(symbol)) return RouteReject::kSymbolNotCanonicalCase;
  for (const char c : symbol) {
    if (!IsAllowedSymbolByte(static_cast<unsigned char>(c))) {
      return RouteReject::kSymbolCharsetInvalid;
    }
  }
  if (symbol.size() < config_.min_symbol_bytes) return RouteReject::kSymbolTooShort;
  return RouteReject::kNone;
}

std::optional<Route> RouteTable::Build(const OutputEvent& event,
                                      const std::string& encoded_value,
                                      RouteReject* reason,
                                      std::string* detail) const {
  const auto fail = [&](RouteReject r, std::string why) -> std::optional<Route> {
    if (reason != nullptr) *reason = r;
    if (detail != nullptr) *detail = std::move(why);
    return std::nullopt;
  };
  if (reason != nullptr) *reason = RouteReject::kNone;
  if (detail != nullptr) detail->clear();

  const TypedEvent& e = event.event;

  // The oneof decides the topic. Checked first because an event with no payload
  // has no route at all, and must not fall back to a default topic (D-04).
  if (!e.kind.has_value()) {
    return fail(RouteReject::kPayloadKindUnset, "oneof payload is unset");
  }
  const EventKind kind = *e.kind;
  const std::string& topic = TopicFor(kind);
  if (topic.empty()) {
    return fail(RouteReject::kTopicNotConfigured,
                std::string("no topic configured for kind ") + ToString(kind));
  }

  // Every core identity field must be present. These are the fields the
  // reconciliation oracle joins on; defaulting any of them would make a record
  // unattributable, which section 5.2 treats as worse than dropping it.
  if (!e.dataset_id.has_value()) return fail(RouteReject::kDatasetIdAbsent, "dataset_id absent");
  if (!e.symbol.has_value()) return fail(RouteReject::kSymbolAbsent, "symbol absent");
  if (!e.sequence.has_value()) return fail(RouteReject::kSequenceAbsent, "sequence absent");
  if (!e.event_time_ns.has_value()) return fail(RouteReject::kEventTimeAbsent, "event_time_ns absent");
  if (!e.price_scale.has_value()) return fail(RouteReject::kPriceScaleAbsent, "price_scale absent");
  if (!e.source_schema_version.has_value()) {
    return fail(RouteReject::kSchemaVersionAbsent, "source_schema_version absent");
  }
  if (*e.source_schema_version != config_.expected_source_schema) {
    return fail(RouteReject::kSchemaVersionUnexpected,
                "source_schema_version=" + *e.source_schema_version + " expected=" +
                    config_.expected_source_schema);
  }

  if (*e.sequence < 0) {
    return fail(RouteReject::kSequenceNegative,
                "sequence=" + std::to_string(*e.sequence));
  }
  if (*e.price_scale <= 0) {
    return fail(RouteReject::kPriceScaleNotPositive,
                "price_scale=" + std::to_string(*e.price_scale));
  }

  const RouteReject symbol_verdict = ValidateSymbol(*e.symbol);
  if (symbol_verdict != RouteReject::kNone) {
    return fail(symbol_verdict, "symbol=" + *e.symbol);
  }

  // The payload arm must actually carry its prices. A quote with an absent ask
  // is not a quote; publishing it would put a zero ask on a market topic.
  if (kind == EventKind::kQuote) {
    if (!e.quote.bid_ticks.has_value() || !e.quote.ask_ticks.has_value()) {
      return fail(RouteReject::kQuoteBodyIncomplete,
                  std::string("bid present=") + (e.quote.bid_ticks ? "1" : "0") +
                      " ask present=" + (e.quote.ask_ticks ? "1" : "0"));
    }
  } else {
    if (!e.tick.price_ticks.has_value()) {
      return fail(RouteReject::kTickBodyIncomplete, "price_ticks absent");
    }
  }

  if (encoded_value.size() > config_.max_value_bytes) {
    return fail(RouteReject::kValueTooLarge,
                std::to_string(encoded_value.size()) + " > " +
                    std::to_string(config_.max_value_bytes));
  }

  Route route;
  route.topic = topic;
  route.key = *e.symbol;
  route.value = encoded_value;
  route.kind = kind;
  route.expected_partition = SelectPartition(route.key, config_.partition_count);
  if (route.expected_partition < 0) {
    // Unreachable unless the symbol validation above was weakened, but a
    // negative partition must never be handed to the producer, so it is a hard
    // failure rather than an assert that a release build would drop.
    return fail(RouteReject::kSymbolEmpty, "partition selection refused the key");
  }

  if (config_.emit_headers) {
    // Header values are bounded and all derived from already-validated fields.
    // Nothing unbounded (no quality_flags JSON, no provenance blob) goes here:
    // section 3.4 forbids it, and D-11 tests that the cap is enforced before
    // anything is queued.
    route.headers.push_back({"md-schema", config_.schema_header_value});
    if (!config_.descriptor_sha256.empty()) {
      route.headers.push_back({"md-descriptor-sha256", config_.descriptor_sha256});
    }
    route.headers.push_back({"md-publish-policy", "market_valid"});
    route.headers.push_back({"md-dataset-id", *e.dataset_id});
    if (!config_.run_id.empty()) {
      route.headers.push_back({"md-run-id", config_.run_id});
    }

    std::size_t header_bytes = 0;
    for (const auto& h : route.headers) header_bytes += h.key.size() + h.value.size();
    if (header_bytes > config_.max_headers_total_bytes) {
      return fail(RouteReject::kHeaderTooLarge,
                  std::to_string(header_bytes) + " > " +
                      std::to_string(config_.max_headers_total_bytes));
    }

    // md-dataset-id mirrors a field inside the value. Case D-05 requires the
    // mismatch to be caught rather than published, so the invariant is checked
    // here at construction, where it cannot drift, instead of only in the
    // verifier where it would already be too late.
    for (const auto& h : route.headers) {
      if (h.key == "md-dataset-id" && h.value != *e.dataset_id) {
        return fail(RouteReject::kHeaderValueMismatch,
                    "md-dataset-id=" + h.value + " value.dataset_id=" + *e.dataset_id);
      }
    }
  }

  route.estimated_bytes = route.key.size() + route.value.size();
  for (const auto& h : route.headers) {
    route.estimated_bytes += h.key.size() + h.value.size();
  }
  return route;
}

}  // namespace p04
