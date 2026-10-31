// Topic selection, key construction and the record-shape contract
// (FR-02, FR-03, FR-05, NFR-05, section 3.3, section 3.4).
//
// Everything here is a pure function of the event plus the pinned config. There
// is no clock, no socket and no allocation of anything that outlives the call,
// which is what makes cases B-01 to B-04 and D-03 to D-11 testable as unit
// tests with no broker at all.
//
// Three rules shape the design:
//
//   * The topic comes from the oneof arm, never from a string in the input.
//     Section 2.3 states it plainly: no topic may be chosen from a symbol or
//     from user input. Build returns a topic drawn from the configured
//     allowlist or it rejects the event.
//   * Uppercase canonicalisation of the key is an assertion, not a repair
//     (section 3.3). A lowercase symbol is rejected with its own reason code so
//     case B-03 can tell "we normalised it" apart from "we refused it".
//   * Missing fields are rejected, not defaulted. proto3 presence is carried
//     all the way here precisely so that an absent sequence fails instead of
//     publishing sequence 0 (case D-03).
#ifndef P04_ROUTING_ROUTE_TABLE_H_
#define P04_ROUTING_ROUTE_TABLE_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "handoff/output_event.h"

namespace p04 {

// Reasons an event cannot become a Kafka record. One reason per failure mode,
// because the test matrix asserts on specific reasons: a single generic
// INVALID would make A-04, B-02, B-03, B-04, D-03 and D-10 indistinguishable.
enum class RouteReject {
  kNone,
  kPayloadKindUnset,       // D-04: oneof not set
  kDatasetIdAbsent,
  kSymbolAbsent,
  kSequenceAbsent,         // D-03
  kEventTimeAbsent,
  kPriceScaleAbsent,
  kSchemaVersionAbsent,
  kSchemaVersionUnexpected,
  kSymbolEmpty,            // B-02
  kSymbolTooShort,
  kSymbolTooLong,          // NFR-05: key cap
  kSymbolNotCanonicalCase, // B-03: lowercase arrives, must not be normalised
  kSymbolCharsetInvalid,   // B-04: NUL, Unicode confusables, control bytes
  kQuoteBodyIncomplete,    // bid or ask absent
  kTickBodyIncomplete,     // price absent
  kSequenceNegative,
  kPriceScaleNotPositive,
  kValueTooLarge,          // D-10
  kHeaderTooLarge,         // D-11
  kHeaderValueMismatch,    // D-05: header disagrees with the value it mirrors
  kTopicNotConfigured,
};

const char* ToString(RouteReject reason);

// A Kafka record header (section 3.4). Bounded by construction: the builder
// refuses to emit a header set whose total size exceeds the configured cap.
struct RecordHeader {
  std::string key;
  std::string value;
};

// The configured, pinned record contract. Loaded from the TOML config; nothing
// here is discovered at runtime, because a discovered topic name is a routing
// error waiting to happen (section 8.2, ROUTING_ERROR).
struct RouteConfig {
  std::string ticks_topic = "md.ticks.v1";
  std::string quotes_topic = "md.quotes.v1";
  std::string expected_source_schema = "sim.v1";
  std::size_t max_value_bytes = 4096;
  std::size_t max_key_bytes = 32;
  std::size_t max_headers_total_bytes = 512;
  std::size_t min_symbol_bytes = 3;
  std::int32_t partition_count = 8;
  // Headers are optional per section 3.4 but on by default here, because the
  // snapshot verifier uses md-dataset-id to cross-check the value (case D-05).
  bool emit_headers = true;
  std::string schema_header_value = "marketdata.v1";
  std::string descriptor_sha256;
  std::string run_id;
};

// The result: everything needed to call produce, with the expected partition
// precomputed so the delivery report can be audited against it.
struct Route {
  std::string topic;
  std::string key;
  std::string value;  // raw serialized MarketDataEvent; no MDP1, no length prefix
  std::vector<RecordHeader> headers;
  std::int32_t expected_partition = -1;
  EventKind kind = EventKind::kTick;
  std::size_t estimated_bytes = 0;  // key + value + headers, for the byte budget
};

class RouteTable {
 public:
  explicit RouteTable(RouteConfig config);

  // Builds the record for an event whose value has already been serialized.
  //
  // The encoded value is passed in rather than produced here so that the codec
  // stays the only place that touches protobuf, and so a test can hand in a
  // deliberately wrong payload (an MDP1-framed buffer, an oversized blob) and
  // observe the size and shape guards firing independently of the encoder.
  std::optional<Route> Build(const OutputEvent& event,
                            const std::string& encoded_value,
                            RouteReject* reason,
                            std::string* detail) const;

  // Validates the symbol on its own. Exposed because the publish gate and the
  // verifier both need it, and because a second copy of these rules would
  // eventually disagree with this one.
  RouteReject ValidateSymbol(const std::string& symbol) const;

  const RouteConfig& config() const { return config_; }

  // The topic for a kind, or an empty string if it is not configured.
  const std::string& TopicFor(EventKind kind) const;

  // Whether a topic name is one of the two configured market topics. Used by
  // the verifier to reject a record that arrived on an unexpected topic.
  bool IsMarketTopic(const std::string& topic) const;

 private:
  RouteConfig config_;
  std::string empty_;
};

}  // namespace p04

#endif  // P04_ROUTING_ROUTE_TABLE_H_
