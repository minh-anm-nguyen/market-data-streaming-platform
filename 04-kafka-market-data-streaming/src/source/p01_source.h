// The upstream stand-in: a Project 01 dataset reader plus a Project 03
// classifier substitute.
//
// Project 03 does not exist yet (section 0.1 lists it as design-only), so
// something has to produce OutputEvent values for Project 04 to publish. That
// something is named MockP03Classifier, it lives here rather than in src/policy
// so it cannot be mistaken for production logic, and every report it feeds
// records that the upstream was a mock.
//
// What is NOT mocked is the data. Project 01 is implemented and produces real
// run directories with events.jsonl, report.json, a COMPLETED marker and a
// SHA-256 over the event file. That is the independent source manifest section
// 10.1 requires: the expected set of events comes from a tool outside Project
// 04, and the digest lets the harness prove it read the same bytes.
//
// The reader is strict in the same way Project 01's own parser is strict
// (src/market_sim/model.py): a float where an integer belongs, an unknown
// field, a missing field or a bad symbol is a parse error with a reason, not a
// silently coerced value. Case A-05 depends on this: JSONL must never be
// forwarded to Kafka as-is, and a lenient reader is the first step down that
// road.
#ifndef P04_SOURCE_P01_SOURCE_H_
#define P04_SOURCE_P01_SOURCE_H_

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "handoff/output_event.h"

namespace p04 {

// What Project 01 recorded about a run, read from report.json and the
// COMPLETED marker. This is the left-hand side of every conservation equation.
struct SourceManifest {
  std::string dataset_id;
  std::string schema_version;
  std::string events_path;
  std::string events_sha256;       // as recorded by P01
  std::string computed_sha256;     // as recomputed by this reader
  std::uint64_t recorded_events = 0;
  std::uint64_t emitted_events = 0;
  std::uint64_t intentional_duplicates = 0;
  std::uint64_t intentional_omissions = 0;
  std::uint64_t intentional_mutations = 0;
  bool completed_marker = false;
  bool faults_enabled = false;
  bool digest_matches = false;

  // A run is only usable as a completeness oracle when the marker is present,
  // the digest matches and the counters reconcile. Anything else is a
  // diagnostic run whose verdict can only be INCOMPLETE (fixture F13).
  bool UsableAsCompletenessOracle() const {
    return completed_marker && digest_matches && !events_sha256.empty();
  }
  std::string Explain() const;
};

struct SourceParseError {
  std::uint64_t line_number = 0;
  std::string reason;
  std::string detail;
};

// Loads report.json, the COMPLETED marker and recomputes the event digest.
// Returns false and fills error when the directory is not a P01 run at all.
bool LoadSourceManifest(const std::string& run_dir, SourceManifest* out,
                        std::string* error);

// Streams events.jsonl, calling the visitor once per record in file order.
//
// Streaming rather than loading: a 120k events/s dataset is hundreds of
// megabytes, and section 7 budgets bounded memory for the publisher. The
// visitor returns false to stop early, which the rate-limited and
// crash-injection harnesses use.
bool ReadEvents(const std::string& events_path,
                const std::function<bool(const TypedEvent&, const std::string& raw_line)>& visitor,
                std::vector<SourceParseError>* errors, std::string* error);

// The Project 03 substitute.
//
// It applies the classification rules the Project 03 design describes, and
// nothing more:
//
//   * structural: required fields present, symbol canonical, scale positive;
//   * business: a quote with ask < bid, or a non-positive price, is rejected
//     rather than repaired (section 2.3);
//   * duplicate: an identity already seen in this run is suppressed;
//   * sequence: a gap sets a quality flag but does not reject, because a gap
//     may be a deliberate P01 omission and case B-10 forbids treating a
//     missing sequence as loss;
//   * freshness: an event whose event_time_ns goes backwards for a symbol is
//     flagged, and marked stale only past the configured tolerance.
//
// The classifier never mutates prices, sequences or timestamps.
struct MockP03Config {
  std::string run_id;
  std::string descriptor_sha256;
  std::string expected_schema_version = "sim.v1";
  // Events whose identity appears in this set are forced to FAULT_TEST_ONLY, so
  // the fault-lane tests can drive case A-03 from a fixture rather than from a
  // code change.
  std::unordered_set<std::string> force_fault_test_identities;
  // Tolerance before a backwards timestamp is called stale rather than just
  // flagged. Zero disables staleness classification.
  std::int64_t stale_tolerance_ns = 0;
  bool emit_unclassified_for_unknown_feed = true;
};

class MockP03Classifier {
 public:
  explicit MockP03Classifier(MockP03Config config);

  // Classifies one parsed source event into an OutputEvent. arrival_index is
  // assigned here and is monotonic across the run, which is what lets the
  // ledger and the verifier agree on the order events were offered in.
  OutputEvent Classify(const TypedEvent& event, const std::string& raw_line);

  struct Counts {
    std::uint64_t total = 0;
    std::uint64_t market_valid = 0;
    std::uint64_t structural_invalid = 0;
    std::uint64_t business_reject = 0;
    std::uint64_t duplicate_suppressed = 0;
    std::uint64_t stale_suppressed = 0;
    std::uint64_t fault_test_only = 0;
    std::uint64_t unclassified = 0;
    std::uint64_t sequence_gaps_flagged = 0;
  };
  const Counts& counts() const { return counts_; }

 private:
  MockP03Config config_;
  std::uint64_t arrival_index_ = 0;
  Counts counts_;
  std::unordered_set<EventIdentity, EventIdentityHash> seen_;
  std::unordered_map<std::string, std::int64_t> last_sequence_;
  std::unordered_map<std::string, std::int64_t> last_event_time_;
};

}  // namespace p04

#endif  // P04_SOURCE_P01_SOURCE_H_
