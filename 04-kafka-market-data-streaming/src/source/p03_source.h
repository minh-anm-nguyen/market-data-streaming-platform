// The real Project 03 upstream: read the adapter's p04_stub handoff stream.
//
// This replaces MockP03Classifier for runs that have a real adapter upstream.
// The mock stays, because the unit and property suites are built on it and a
// deterministic stand-in is worth keeping, but it is no longer the only option
// and a run report now says which upstream produced its events.
//
// What changes, and why it matters. The mock derives a classification from P01
// JSONL by re-implementing a policy that belongs to Project 03. Reading the
// adapter's own output instead means the classification, the sequence-gap
// detection, the staleness decision and the suppression set are all Project
// 03's, which is the only way the P01 -> P02 -> P03 -> P04 chain proves anything
// about the adapter (P09 finding P09-M0-05).
//
// Record shape, written by P04StubSink in 03-*/src/sink.cpp, one per line:
//
//   {"key":"EURUSD","value_hex":"0a08...","value_bytes":46,
//    "quality":{"flags":["GAP","DEGRADED"],"feed_state":"DEGRADED",
//               "primary_reason":"SEQUENCE_GAP","arrival_index":17,
//               "event_id":{...}}}
//
// Three rules this reader holds to:
//
//   1. **The bytes are Project 03's.** `value_hex` is the untouched
//      marketdata.v1 wire message. It is decoded to fill a TypedEvent, then
//      re-encoded and compared against the original bytes; a mismatch is a
//      parse error and stops the run. P04 publishes a value it serialized
//      itself, so without that check "these are P03's bytes" would be an
//      assumption rather than a fact (ADR-14).
//   2. **A framing prefix is a violation, not something to strip.** A Kafka
//      record value carries its own boundary, so an `MDP1` magic or a 4-byte
//      length at the start of the payload means something upstream packaged it
//      wrongly. Stripping it would hide that bug (case L07).
//   3. **An unrecognised reason is kUnclassified, never kMarketValid.** P04
//      refuses to publish an unclassified event, so a reason string this
//      mapping has not seen fails the run instead of silently entering the
//      market topics.

#ifndef P04_SOURCE_P03_SOURCE_H_
#define P04_SOURCE_P03_SOURCE_H_

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "handoff/output_event.h"
#include "source/p01_source.h"

namespace p04 {

// Everything the reader needs that is not in the file itself.
struct P03HandoffConfig {
  std::string run_id;
  std::string descriptor_sha256;        // the pin this binary carries
  std::string expected_dataset_id;      // from the P01 report; "" accepts any
  std::string expected_schema_version;  // "sim.v1"; "" skips the check
  // A record whose decoded symbol differs from record.key is a routing
  // violation: the key decides the partition and the payload decides the
  // meaning, so they must agree (case L08).
  bool require_key_matches_symbol = true;
};

// Counts kept apart because they are different claims. `records_read` is lines
// consumed; `offered` is what reached the callback; the classification tallies
// are what Project 03 decided, not what Project 04 re-derived.
struct P03HandoffStats {
  std::uint64_t records_read = 0;
  std::uint64_t offered = 0;
  std::uint64_t market_valid = 0;
  std::uint64_t degraded_forwarded = 0;
  std::uint64_t fault_test_only = 0;
  std::uint64_t other_classified = 0;
  std::uint64_t reencode_mismatches = 0;
  std::uint64_t framing_violations = 0;
  std::uint64_t key_symbol_mismatches = 0;
};

// Reads `path` line by line and calls `on_event` for each record. The callback
// returns false to stop early, exactly as ReadEvents does, so the caller keeps
// control of pacing, backpressure and the stop signal.
//
// Returns false when the file could not be read at all, or when a record failed
// to parse: this reader is fail-closed, because a half-read handoff stream is
// not a smaller dataset, it is an unknown one. `errors` carries the per-line
// reasons either way.
bool ReadP03Handoff(const std::string& path, const P03HandoffConfig& config,
                    const std::function<bool(const OutputEvent&)>& on_event,
                    P03HandoffStats* stats,
                    std::vector<SourceParseError>* errors, std::string* error);

// Exposed for the unit tests: the reason-to-classification mapping, the part
// most likely to need extending when P03 adds a reason.
bool ClassifyFromP03Reason(const std::string& primary_reason,
                           const std::vector<std::string>& flags,
                           Classification* classification,
                           std::uint32_t* quality_flags);

// Exposed for the unit tests: P03's feed-state vocabulary is wider than P04's.
FeedState FeedStateFromP03(const std::string& feed_state);

}  // namespace p04

#endif  // P04_SOURCE_P03_SOURCE_H_
