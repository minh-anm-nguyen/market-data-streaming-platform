#include "handoff/output_event.h"

#include <functional>

namespace p04 {

const char* ToString(EventKind kind) {
  switch (kind) {
    case EventKind::kQuote: return "quote";
    case EventKind::kTick:  return "tick";
  }
  return "unknown";
}

const char* ToString(Classification classification) {
  switch (classification) {
    case Classification::kMarketValid:         return "market_valid";
    case Classification::kStructuralInvalid:   return "structural_invalid";
    case Classification::kBusinessReject:      return "business_reject";
    case Classification::kDuplicateSuppressed: return "duplicate_suppressed";
    case Classification::kStaleSuppressed:     return "stale_suppressed";
    case Classification::kFaultTestOnly:       return "fault_test_only";
    case Classification::kUnclassified:        return "unclassified";
  }
  return "unknown";
}

const char* ToString(FeedState state) {
  switch (state) {
    case FeedState::kUnknown:      return "unknown";
    case FeedState::kWarmup:       return "warmup";
    case FeedState::kLive:         return "live";
    case FeedState::kStale:        return "stale";
    case FeedState::kDisconnected: return "disconnected";
  }
  return "unknown";
}

std::string EventIdentity::ToString() const {
  return dataset_id + "/" + symbol + "/" + std::to_string(sequence);
}

std::size_t EventIdentityHash::operator()(const EventIdentity& id) const {
  // Order-dependent mixing, so swapping dataset_id and symbol gives a different
  // hash. This is deliberately NOT a partitioning hash: the Kafka partitioner is
  // pinned separately in routing/partition_policy.h, because a std::hash that
  // varies by standard library version would silently repartition the topic.
  std::size_t h = std::hash<std::string>{}(id.dataset_id);
  h ^= std::hash<std::string>{}(id.symbol) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  h ^= std::hash<std::int64_t>{}(id.sequence) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  return h;
}

EventIdentity OutputEvent::Identity() const {
  EventIdentity id;
  if (event.dataset_id) id.dataset_id = *event.dataset_id;
  if (event.symbol) id.symbol = *event.symbol;
  if (event.sequence) id.sequence = *event.sequence;
  return id;
}

}  // namespace p04
