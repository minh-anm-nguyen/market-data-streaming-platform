// Quote freshness.
//
// Three different notions of "old" are kept apart on purpose (design 6):
//
//   1. event-time regression -- compares an event's own timestamp against the
//      highest timestamp already accepted for that key. Replaying a 2025
//      dataset in 2026 is NOT stale.
//   2. feed inactivity       -- measured on a monotonic clock from the last
//      *valid quote*. A stream of ticks does not refresh it.
//   3. end-to-end age        -- receive wall time minus event time. Only
//      meaningful live with synchronised clocks; never used as a rule here.
//
// Only (1) and (2) drive classification.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "md_adapter/domain_event.hpp"
#include "md_adapter/status.hpp"

namespace md {

struct FreshnessLimits {
  std::int64_t event_time_stale_threshold_ns = 2'000'000'000;
  std::int64_t quote_inactivity_threshold_ns = 3'000'000'000;
};

class FreshnessEngine {
 public:
  struct Result {
    std::uint32_t quality_flags = quality::kNone;
    Reason reason = Reason::kNone;
    bool event_time_old = false;
    bool recovered = false;
  };

  struct Transition {
    std::string dataset_id;
    std::string symbol;
    FeedState from = FeedState::kInitial;
    FeedState to = FeedState::kInitial;
    std::int64_t at_mono_ns = 0;
  };

  explicit FreshnessEngine(FreshnessLimits limits) : limits_(limits) {}

  // Call with business-valid events only, in arrival order.
  Result observe(const DomainEvent& e, std::int64_t now_mono_ns);

  // Drive from the event loop, including while the transport is silent.
  // Appends every state change that happened at this tick.
  void tick(std::int64_t now_mono_ns, std::vector<Transition>& out);

  // The classifier reports stream-quality problems here so that feed state has
  // a single owner. DEGRADED is sticky: it is not cleared by a timer.
  void mark_degraded(const std::string& dataset_id, const std::string& symbol);

  FeedState state_of(const std::string& dataset_id,
                     const std::string& symbol) const;

  std::uint64_t inactive_transitions() const noexcept {
    return inactive_transitions_;
  }
  std::uint64_t event_time_old_total() const noexcept {
    return event_time_old_total_;
  }

  struct KeyView {
    std::string key;
    FeedState state = FeedState::kInitial;
    std::int64_t watermark_ns = 0;
    bool has_watermark = false;
    std::uint64_t inactive_transitions = 0;
  };
  std::vector<KeyView> snapshot() const;

 private:
  enum class Liveness : std::uint8_t { kUnknown, kActive, kInactive, kRecovering };

  struct KeyState {
    std::string dataset_id;
    std::string symbol;
    std::int64_t watermark_ns = 0;
    bool has_watermark = false;
    // Monotonic receive time of the last valid, non-stale quote. Falls back to
    // the time the key was first observed so a tick-only feed still goes
    // inactive.
    std::int64_t last_good_quote_mono_ns = 0;
    bool degraded_sticky = false;
    bool start_unverified = false;
    Liveness liveness = Liveness::kUnknown;
    std::uint64_t events = 0;
    std::uint64_t inactive_transitions = 0;
  };

  static FeedState derive_state(const KeyState& k);
  KeyState& key_for(const DomainEvent& e, std::int64_t now_mono_ns);

  FreshnessLimits limits_;
  std::map<std::string, KeyState> keys_;
  std::uint64_t inactive_transitions_ = 0;
  std::uint64_t event_time_old_total_ = 0;
};

}  // namespace md
