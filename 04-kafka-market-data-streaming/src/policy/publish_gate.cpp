#include "policy/publish_gate.h"

namespace p04 {

const char* ToString(PublishLane lane) {
  switch (lane) {
    case PublishLane::kMarketTopic: return "market_topic";
    case PublishLane::kAuditLane:   return "audit_lane";
    case PublishLane::kDropped:     return "dropped";
  }
  return "unknown";
}

const char* ToString(SuppressReason reason) {
  switch (reason) {
    case SuppressReason::kNone:                       return "NONE";
    case SuppressReason::kStructuralInvalid:          return "STRUCTURAL_INVALID";
    case SuppressReason::kBusinessReject:             return "BUSINESS_REJECT";
    case SuppressReason::kDuplicateSuppressed:        return "DUPLICATE_SUPPRESSED";
    case SuppressReason::kStaleSuppressed:            return "STALE_SUPPRESSED";
    case SuppressReason::kUnclassified:               return "UNCLASSIFIED";
    case SuppressReason::kFeedStateNotLive:           return "FEED_STATE_NOT_LIVE";
    case SuppressReason::kFaultManifestRequired:      return "FAULT_MANIFEST_REQUIRED";
    case SuppressReason::kFaultTestNotAllowedInMarket:return "FAULT_TEST_NOT_ALLOWED_IN_MARKET";
    case SuppressReason::kDatasetMixRejected:         return "DATASET_MIX_REJECTED";
    case SuppressReason::kDatasetNotAllowlisted:      return "DATASET_NOT_ALLOWLISTED";
    case SuppressReason::kSymbolBudgetExceeded:       return "SYMBOL_BUDGET_EXCEEDED";
    case SuppressReason::kDatasetBudgetExceeded:      return "DATASET_BUDGET_EXCEEDED";
  }
  return "UNMAPPED";
}

PublishGate::PublishGate(PublishGateConfig config) : config_(std::move(config)) {}

std::size_t PublishGate::suppressed_count(SuppressReason reason) const {
  const auto index = static_cast<std::size_t>(reason);
  return index < kReasonCount ? suppressed_counts_[index] : 0;
}

GateDecision PublishGate::Suppress(SuppressReason reason, std::string detail) {
  const auto index = static_cast<std::size_t>(reason);
  if (index < kReasonCount) ++suppressed_counts_[index];
  GateDecision decision;
  decision.lane = PublishLane::kDropped;
  decision.reason = reason;
  decision.detail = std::move(detail);
  return decision;
}

GateDecision PublishGate::Decide(const OutputEvent& event) {
  const std::string dataset_id = event.event.dataset_id.value_or(std::string());
  const std::string symbol = event.event.symbol.value_or(std::string());

  // Identity budgets are checked before anything else that could allocate
  // per-identity state. The order matters under fixture F11 (many synthetic
  // symbols): if the dataset or symbol set were grown first and the budget
  // checked second, the budget would be enforced one event too late and the
  // exporter cardinality cap in section 8.1 would already be breached.
  const bool new_dataset =
      !dataset_id.empty() && observed_datasets_.find(dataset_id) == observed_datasets_.end();
  const bool new_symbol =
      !symbol.empty() && observed_symbols_.find(symbol) == observed_symbols_.end();

  if (!config_.dataset_allowlist.empty() && !dataset_id.empty() &&
      config_.dataset_allowlist.find(dataset_id) == config_.dataset_allowlist.end()) {
    return Suppress(SuppressReason::kDatasetNotAllowlisted, "dataset_id=" + dataset_id);
  }
  if (new_dataset && observed_datasets_.size() >= config_.max_active_datasets) {
    return Suppress(SuppressReason::kDatasetBudgetExceeded,
                    "active_datasets=" + std::to_string(observed_datasets_.size()));
  }
  if (new_symbol && observed_symbols_.size() >= config_.max_active_symbols) {
    return Suppress(SuppressReason::kSymbolBudgetExceeded,
                    "active_symbols=" + std::to_string(observed_symbols_.size()));
  }

  // Single-active-dataset policy. Note this compares against the first dataset
  // actually admitted, not against whatever arrived last, so an interleaved
  // A/B/A/B stream is rejected consistently rather than flapping (A-10).
  if (config_.single_active_dataset && !dataset_id.empty()) {
    if (first_dataset_.empty()) {
      first_dataset_ = dataset_id;
    } else if (dataset_id != first_dataset_) {
      return Suppress(SuppressReason::kDatasetMixRejected,
                      "active=" + first_dataset_ + " offered=" + dataset_id);
    }
  }

  // The upstream classification is authoritative. Nothing below may upgrade it.
  switch (event.classification) {
    case Classification::kStructuralInvalid:
      return Suppress(SuppressReason::kStructuralInvalid, event.Identity().ToString());
    case Classification::kBusinessReject:
      return Suppress(SuppressReason::kBusinessReject, event.Identity().ToString());
    case Classification::kDuplicateSuppressed:
      return Suppress(SuppressReason::kDuplicateSuppressed, event.Identity().ToString());
    case Classification::kStaleSuppressed:
      return Suppress(SuppressReason::kStaleSuppressed, event.Identity().ToString());
    case Classification::kUnclassified:
      return Suppress(SuppressReason::kUnclassified, event.Identity().ToString());

    case Classification::kFaultTestOnly: {
      // Fail closed in two independent ways. Both have to pass, and the
      // manifest requirement is checked first so that a config with
      // allow_fault_test_into_market_topics set but no verified manifest still
      // refuses: an unverified anomaly has no provenance to justify it (A-03).
      if (!config_.fault_manifest_verified) {
        return Suppress(SuppressReason::kFaultManifestRequired,
                        event.Identity().ToString());
      }
      if (config_.allow_fault_test_into_market_topics) {
        // Reachable only when an operator deliberately configured a test run
        // against an isolated topic set. The run report records the flag, so a
        // reader can tell this run apart from a clean one.
        ++market_valid_count_;
        if (new_dataset) observed_datasets_.insert(dataset_id);
        if (new_symbol) observed_symbols_.insert(symbol);
        GateDecision decision;
        decision.lane = PublishLane::kMarketTopic;
        decision.reason = SuppressReason::kNone;
        decision.detail = "fault_test_allowed_by_config";
        return decision;
      }
      if (!config_.anomaly_topic.empty()) {
        ++audit_lane_count_;
        if (new_dataset) observed_datasets_.insert(dataset_id);
        if (new_symbol) observed_symbols_.insert(symbol);
        GateDecision decision;
        decision.lane = PublishLane::kAuditLane;
        decision.reason = SuppressReason::kNone;
        decision.detail = "routed to isolated anomaly topic";
        return decision;
      }
      return Suppress(SuppressReason::kFaultTestNotAllowedInMarket,
                      event.Identity().ToString());
    }

    case Classification::kMarketValid:
      break;
  }

  // Feed state is a separate axis from classification: an event can be
  // individually valid while the adapter could not vouch for the feed it came
  // from. Case A-06 requires that combination to be suppressed with its own
  // reason rather than merged into "unclassified".
  if (config_.require_live_feed_state && event.feed_state != FeedState::kLive) {
    return Suppress(SuppressReason::kFeedStateNotLive,
                    std::string("feed_state=") + ToString(event.feed_state));
  }

  if (new_dataset) observed_datasets_.insert(dataset_id);
  if (new_symbol) observed_symbols_.insert(symbol);
  ++market_valid_count_;

  GateDecision decision;
  decision.lane = PublishLane::kMarketTopic;
  decision.reason = SuppressReason::kNone;
  return decision;
}

}  // namespace p04
