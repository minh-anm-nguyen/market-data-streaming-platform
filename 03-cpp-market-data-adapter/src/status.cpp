#include "md_adapter/status.hpp"

#include <string>

#include "md_adapter/domain_event.hpp"

namespace md {

std::string_view to_string(Reason r) noexcept {
  switch (r) {
    case Reason::kNone: return "none";
    case Reason::kBadMagic: return "BAD_MAGIC";
    case Reason::kInvalidLength: return "INVALID_LENGTH";
    case Reason::kTruncatedPrefix: return "TRUNCATED_PREFIX";
    case Reason::kTruncatedHeader: return "TRUNCATED_HEADER";
    case Reason::kTruncatedBody: return "TRUNCATED_BODY";
    case Reason::kFrameTimeout: return "FRAME_TIMEOUT";
    case Reason::kConnectTimeout: return "CONNECT_TIMEOUT";
    case Reason::kConnectRefused: return "CONNECT_REFUSED";
    case Reason::kIoError: return "IO_ERROR";
    case Reason::kPeerClosed: return "PEER_CLOSED";
    case Reason::kProtoDecodeError: return "PROTO_DECODE_ERROR";
    case Reason::kMissingDatasetId: return "MISSING_DATASET_ID";
    case Reason::kMissingSymbol: return "MISSING_SYMBOL";
    case Reason::kMissingSequence: return "MISSING_SEQUENCE";
    case Reason::kMissingEventTime: return "MISSING_EVENT_TIME";
    case Reason::kMissingPriceScale: return "MISSING_PRICE_SCALE";
    case Reason::kMissingSourceVersion: return "MISSING_SOURCE_VERSION";
    case Reason::kMissingPayloadOneof: return "MISSING_PAYLOAD_ONEOF";
    case Reason::kMissingQuoteField: return "MISSING_QUOTE_FIELD";
    case Reason::kMissingTickField: return "MISSING_TICK_FIELD";
    case Reason::kFieldTooLong: return "FIELD_TOO_LONG";
    case Reason::kInvalidUtf8: return "INVALID_UTF8";
    case Reason::kSymbolCharset: return "SYMBOL_CHARSET";
    case Reason::kUnsupportedSchemaVersion: return "UNSUPPORTED_SCHEMA_VERSION";
    case Reason::kUnknownFieldPresent: return "UNKNOWN_FIELD_PRESENT";
    case Reason::kInvalidSequence: return "INVALID_SEQUENCE";
    case Reason::kNegativeEventTime: return "NEGATIVE_EVENT_TIME";
    case Reason::kInvalidPriceScale: return "INVALID_PRICE_SCALE";
    case Reason::kScaleMismatch: return "SCALE_MISMATCH";
    case Reason::kQuoteBidNonPositive: return "QUOTE_BID_NON_POSITIVE";
    case Reason::kQuoteCrossed: return "QUOTE_CROSSED";
    case Reason::kSpreadOutOfRange: return "SPREAD_OUT_OF_RANGE";
    case Reason::kPriceOutOfRange: return "PRICE_OUT_OF_RANGE";
    case Reason::kTickNonPositive: return "TICK_NON_POSITIVE";
    case Reason::kSymbolNotAllowed: return "SYMBOL_NOT_ALLOWED";
    case Reason::kArithmeticOverflow: return "ARITHMETIC_OVERFLOW";
    case Reason::kSequenceGap: return "SEQUENCE_GAP";
    case Reason::kDuplicateExact: return "DUPLICATE_EXACT";
    case Reason::kDuplicateConflict: return "DUPLICATE_CONFLICT";
    case Reason::kReorderLate: return "REORDER_LATE";
    case Reason::kTooOldUnverifiable: return "TOO_OLD_UNVERIFIABLE";
    case Reason::kSequenceRollback: return "SEQUENCE_ROLLBACK";
    case Reason::kInitialGap: return "INITIAL_GAP";
    case Reason::kStartUnverified: return "START_UNVERIFIED";
    case Reason::kHugeGap: return "HUGE_GAP";
    case Reason::kEventTimeOld: return "EVENT_TIME_OLD";
    case Reason::kFeedInactive: return "FEED_INACTIVE";
    case Reason::kContinuityLost: return "CONTINUITY_LOST";
    case Reason::kQueueBudgetExceeded: return "QUEUE_BUDGET_EXCEEDED";
    case Reason::kBackpressureTimeout: return "BACKPRESSURE_TIMEOUT";
    case Reason::kSinkIoError: return "SINK_IO_ERROR";
    case Reason::kStateCapExceeded: return "STATE_CAP_EXCEEDED";
    case Reason::kResourceExhausted: return "RESOURCE_EXHAUSTED";
    case Reason::kSourceUnverified: return "SOURCE_UNVERIFIED";
    case Reason::kConfigInvalid: return "CONFIG_INVALID";
    case Reason::kDescriptorMismatch: return "DESCRIPTOR_MISMATCH";
    case Reason::kUnexpectedFault: return "UNEXPECTED_FAULT";
    case Reason::kReasonCount: return "INVALID";
  }
  return "INVALID";
}

void append_quality_flags(std::uint32_t flags, std::string& out) {
  struct Entry { std::uint32_t bit; const char* name; };
  static constexpr Entry kTable[] = {
      {quality::kGap, "GAP"},
      {quality::kDegraded, "DEGRADED"},
      {quality::kDuplicateExact, "DUPLICATE_EXACT"},
      {quality::kDuplicateConflict, "DUPLICATE_CONFLICT"},
      {quality::kReorderLate, "REORDER_LATE"},
      {quality::kTooOldUnverifiable, "TOO_OLD_UNVERIFIABLE"},
      {quality::kEventTimeOld, "EVENT_TIME_OLD"},
      {quality::kQuoteInactive, "QUOTE_INACTIVE"},
      {quality::kRecovered, "RECOVERED"},
      {quality::kStartUnverified, "START_UNVERIFIED"},
      {quality::kInitialGap, "INITIAL_GAP"},
      {quality::kSequenceRollback, "SEQUENCE_ROLLBACK"},
      {quality::kHugeGap, "HUGE_GAP"},
      {quality::kContinuityLost, "CONTINUITY_LOST"},
      {quality::kUnknownFields, "UNKNOWN_FIELDS"},
      {quality::kFaultTestOnly, "FAULT_TEST_ONLY"},
  };
  bool first = true;
  for (const auto& e : kTable) {
    if ((flags & e.bit) == 0) continue;
    if (!first) out += ',';
    out += e.name;
    first = false;
  }
}

std::string_view to_string(FeedState s) noexcept {
  switch (s) {
    case FeedState::kInitial: return "INITIAL";
    case FeedState::kStartUnverified: return "START_UNVERIFIED";
    case FeedState::kHealthy: return "HEALTHY";
    case FeedState::kDegraded: return "DEGRADED";
    case FeedState::kInactive: return "INACTIVE";
    case FeedState::kRecovering: return "RECOVERING";
  }
  return "INVALID";
}

std::string_view to_string(Provenance p) noexcept {
  switch (p) {
    case Provenance::kVerifiedCompleteFile: return "VERIFIED_COMPLETE_FILE";
    case Provenance::kVerifiedPartialFile: return "VERIFIED_PARTIAL_FILE";
    case Provenance::kLiveUnverified: return "LIVE_UNVERIFIED";
    case Provenance::kLiveWithSourceEvidence: return "LIVE_WITH_SOURCE_EVIDENCE";
  }
  return "INVALID";
}

std::string_view to_string(RunStatus s) noexcept {
  switch (s) {
    case RunStatus::kCompleteOffline: return "COMPLETE_OFFLINE";
    case RunStatus::kPartialLive: return "PARTIAL_LIVE";
    case RunStatus::kIncomplete: return "INCOMPLETE";
    case RunStatus::kFailed: return "FAILED";
  }
  return "INVALID";
}

std::string_view to_string(PayloadKind k) noexcept {
  return k == PayloadKind::kQuote ? "quote" : "tick";
}

TypedCore core_of(const DomainEvent& e) noexcept {
  TypedCore c;
  c.event_time_ns = e.event_time_ns;
  c.price_scale = e.price_scale;
  c.kind = kind_of(e.payload);
  if (c.kind == PayloadKind::kQuote) {
    const auto& q = std::get<QuotePrice>(e.payload);
    c.a = q.bid;
    c.b = q.ask;
  } else {
    c.a = std::get<TickPrice>(e.payload).price;
    c.b = 0;
  }
  return c;
}

}  // namespace md
