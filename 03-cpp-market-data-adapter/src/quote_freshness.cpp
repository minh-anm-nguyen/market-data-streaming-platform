#include "md_adapter/quote_freshness.hpp"

#include <algorithm>
#include <limits>

namespace md {
namespace {

constexpr char kKeySep = '\x1f';

std::string make_key(const std::string& dataset_id, const std::string& symbol) {
  std::string k;
  k.reserve(dataset_id.size() + symbol.size() + 1);
  k += dataset_id;
  k += kKeySep;
  k += symbol;
  return k;
}

// Checked age: watermark - event_time. Returns false when the subtraction
// would overflow, which the caller treats as "cannot judge", not as fresh.
bool checked_age(std::int64_t watermark, std::int64_t event_time,
                 std::int64_t& age) {
  return !__builtin_sub_overflow(watermark, event_time, &age);
}

}  // namespace

FeedState FreshnessEngine::derive_state(const KeyState& k) {
  if (k.events == 0) return FeedState::kInitial;
  if (k.liveness == Liveness::kInactive) return FeedState::kInactive;
  if (k.liveness == Liveness::kRecovering) return FeedState::kRecovering;
  if (k.degraded_sticky) return FeedState::kDegraded;
  if (k.start_unverified && k.events <= 1) return FeedState::kStartUnverified;
  return FeedState::kHealthy;
}

FreshnessEngine::KeyState& FreshnessEngine::key_for(const DomainEvent& e,
                                                    std::int64_t now_mono_ns) {
  const std::string key = make_key(e.dataset_id, e.symbol);
  const auto it = keys_.find(key);
  if (it != keys_.end()) return it->second;
  KeyState& k = keys_[key];
  k.dataset_id = e.dataset_id;
  k.symbol = e.symbol;
  // Anchor the inactivity timer at first observation so a feed that never
  // sends a valid quote still reports inactivity instead of staying silent.
  k.last_good_quote_mono_ns = now_mono_ns;
  k.start_unverified = true;
  return k;
}

FreshnessEngine::Result FreshnessEngine::observe(const DomainEvent& e,
                                                 std::int64_t now_mono_ns) {
  Result r;
  KeyState& k = key_for(e, now_mono_ns);
  ++k.events;

  // --- event-time regression ----------------------------------------------
  bool stale = false;
  if (k.has_watermark) {
    std::int64_t age = 0;
    if (!checked_age(k.watermark_ns, e.event_time_ns, age)) {
      // An event time far enough from the watermark to overflow int64 is not
      // a timestamp this feed can be reasoned about.
      stale = true;
    } else if (age > limits_.event_time_stale_threshold_ns) {
      stale = true;
    }
  }
  // The very first event has no watermark to compare against; it is UNKNOWN,
  // never "stale because it is older than today".

  if (stale) {
    r.event_time_old = true;
    r.reason = Reason::kEventTimeOld;
    r.quality_flags |= quality::kEventTimeOld | quality::kDegraded;
    k.degraded_sticky = true;
    ++event_time_old_total_;
    // A stale quote does not refresh the watermark and does not refresh the
    // liveness clock: it is not evidence that the feed is current.
    return r;
  }

  if (!k.has_watermark || e.event_time_ns > k.watermark_ns) {
    k.watermark_ns = e.event_time_ns;
    k.has_watermark = true;
  }

  // --- liveness ------------------------------------------------------------
  // Only a quote refreshes the quote-inactivity clock. A tick is a price
  // update, not evidence that quoting is alive.
  if (kind_of(e.payload) == PayloadKind::kQuote) {
    k.last_good_quote_mono_ns = now_mono_ns;
    if (k.liveness == Liveness::kInactive) {
      k.liveness = Liveness::kRecovering;
      r.recovered = true;
      r.quality_flags |= quality::kRecovered;
    } else if (k.liveness == Liveness::kRecovering) {
      k.liveness = Liveness::kActive;
    } else {
      k.liveness = Liveness::kActive;
    }
  }

  return r;
}

void FreshnessEngine::tick(std::int64_t now_mono_ns,
                           std::vector<Transition>& out) {
  for (auto& kv : keys_) {
    KeyState& k = kv.second;
    if (k.events == 0) continue;
    if (k.liveness == Liveness::kInactive) continue;

    std::int64_t idle = 0;
    if (__builtin_sub_overflow(now_mono_ns, k.last_good_quote_mono_ns, &idle))
      continue;
    if (idle <= limits_.quote_inactivity_threshold_ns) continue;

    const FeedState from = derive_state(k);
    k.liveness = Liveness::kInactive;
    ++k.inactive_transitions;
    ++inactive_transitions_;
    // One transition per active->inactive edge, not one per timer tick.
    out.push_back(Transition{k.dataset_id, k.symbol, from, derive_state(k),
                             now_mono_ns});
  }
}

void FreshnessEngine::mark_degraded(const std::string& dataset_id,
                                    const std::string& symbol) {
  const auto it = keys_.find(make_key(dataset_id, symbol));
  if (it != keys_.end()) it->second.degraded_sticky = true;
}

FeedState FreshnessEngine::state_of(const std::string& dataset_id,
                                    const std::string& symbol) const {
  const auto it = keys_.find(make_key(dataset_id, symbol));
  if (it == keys_.end()) return FeedState::kInitial;
  return derive_state(it->second);
}

std::vector<FreshnessEngine::KeyView> FreshnessEngine::snapshot() const {
  std::vector<KeyView> out;
  out.reserve(keys_.size());
  for (const auto& kv : keys_) {
    std::string pretty = kv.first;
    std::replace(pretty.begin(), pretty.end(), kKeySep, '/');
    out.push_back(KeyView{std::move(pretty), derive_state(kv.second),
                          kv.second.watermark_ns, kv.second.has_watermark,
                          kv.second.inactive_transitions});
  }
  return out;
}

}  // namespace md
