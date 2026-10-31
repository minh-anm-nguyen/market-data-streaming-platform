// The market-versus-fault decision that happens before anything touches Kafka
// (section 3.5, FR-01, cases A-01 to A-03, A-06, A-09, A-10).
//
// This gate is one-way. It can only suppress. Section 2.3 states what it must
// never do: auto-correct a price or a sequence. Section 3.5 states what it must
// never allow: a structurally invalid, business-rejected, duplicate-suppressed
// or deliberately faulty event reaching a market topic that someone will later
// read as market-valid data.
//
// The subtle requirement is case A-03. An event classified FAULT_TEST_ONLY is
// not merely rerouted; without a verified fault manifest it is refused
// outright, including from the test lane, because an unverified anomaly is
// indistinguishable from a bug in the generator. Fail closed is the default and
// the config cannot turn that into fail open for market topics.
#ifndef P04_POLICY_PUBLISH_GATE_H_
#define P04_POLICY_PUBLISH_GATE_H_

#include <cstddef>
#include <string>
#include <unordered_set>

#include "handoff/output_event.h"

namespace p04 {

// Where an event is allowed to go. kAuditLane is only reachable when a
// separate, explicitly configured anomaly topic exists; section 3.5 warns that
// writing to two topics is not atomic, so reaching the audit lane never implies
// the market record succeeded or vice versa.
enum class PublishLane { kMarketTopic, kAuditLane, kDropped };

const char* ToString(PublishLane lane);

// Why the gate decided what it decided. Every suppression has a reason that
// reaches the run report; section 16 point 3 requires unknowns to be accounted
// for, and an unexplained drop is an unknown.
enum class SuppressReason {
  kNone,
  kStructuralInvalid,
  kBusinessReject,
  kDuplicateSuppressed,
  kStaleSuppressed,
  kUnclassified,
  kFeedStateNotLive,          // A-06
  kFaultManifestRequired,     // A-03
  kFaultTestNotAllowedInMarket,
  kDatasetMixRejected,        // A-10
  kDatasetNotAllowlisted,
  kSymbolBudgetExceeded,      // NFR-13 / F11: bounded active symbol set
  kDatasetBudgetExceeded,
};

const char* ToString(SuppressReason reason);

struct PublishGateConfig {
  // Strict default from section 3.5: deliberately faulty events never reach a
  // market topic. A test that wants them on Kafka must set up an isolated test
  // topic and flip this, and the run report records that it was flipped.
  bool allow_fault_test_into_market_topics = false;

  // A verified fault manifest from Project 01 (its fault-manifest.jsonl plus a
  // COMPLETED marker) is required before any fault-test event may be published
  // anywhere. Without it the gate refuses (A-03).
  bool fault_manifest_verified = false;

  // An isolated anomaly topic, if one is configured. Empty means the audit lane
  // does not exist and audit-eligible events are dropped with a reason.
  std::string anomaly_topic;

  // Section 11.2 contract: one active dataset per run unless explicitly
  // relaxed. A-10 requires the second dataset to be refused rather than mixed.
  bool single_active_dataset = true;

  // Bounded identity space, so a hostile or buggy source cannot grow unbounded
  // per-symbol state (NFR-13, fixture F11).
  std::size_t max_active_symbols = 256;
  std::size_t max_active_datasets = 4;

  // Optional allowlist. When non-empty, a dataset_id outside it is refused;
  // this is what keeps a stray replay of an old dataset out of a clean run.
  std::unordered_set<std::string> dataset_allowlist;

  // Feed states considered publishable. Live only, by default: warmup data is
  // not yet trustworthy and stale data is explicitly suppressed upstream.
  bool require_live_feed_state = true;
};

struct GateDecision {
  PublishLane lane = PublishLane::kDropped;
  SuppressReason reason = SuppressReason::kNone;
  std::string detail;

  bool market_valid() const { return lane == PublishLane::kMarketTopic; }
};

// Stateful because A-09, A-10 and the symbol budget need memory across events:
// a dataset reset is only recognisable by remembering which datasets have been
// seen, and a symbol budget is only enforceable by counting distinct symbols.
//
// Not thread-safe by design. Section 2.4 pins a single-owner input path, so a
// mutex here would hide a violation of that ownership model rather than fix it.
class PublishGate {
 public:
  explicit PublishGate(PublishGateConfig config);

  GateDecision Decide(const OutputEvent& event);

  // Counters for the run report, keyed by reason so the report can show where
  // every suppressed event went.
  const std::size_t* suppressed_counts() const { return suppressed_counts_; }
  std::size_t suppressed_count(SuppressReason reason) const;
  std::size_t market_valid_count() const { return market_valid_count_; }
  std::size_t audit_lane_count() const { return audit_lane_count_; }

  const std::unordered_set<std::string>& observed_datasets() const {
    return observed_datasets_;
  }
  const std::unordered_set<std::string>& observed_symbols() const {
    return observed_symbols_;
  }

  const PublishGateConfig& config() const { return config_; }

 private:
  GateDecision Suppress(SuppressReason reason, std::string detail);

  PublishGateConfig config_;
  std::size_t market_valid_count_ = 0;
  std::size_t audit_lane_count_ = 0;
  static constexpr std::size_t kReasonCount = 13;
  std::size_t suppressed_counts_[kReasonCount] = {};
  std::unordered_set<std::string> observed_datasets_;
  std::unordered_set<std::string> observed_symbols_;
  std::string first_dataset_;
};

}  // namespace p04

#endif  // P04_POLICY_PUBLISH_GATE_H_
