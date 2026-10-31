// Output policy.
//
// The sequence tracker and the freshness engine only *describe* what they saw.
// This is where the adapter decides whether a described event is forwarded,
// and that decision differs by mode:
//
//   strict_monitor / offline_verify
//       A new, in-order or gapped event is forwarded carrying DEGRADED, so
//       analytics still sees fresh data and still knows continuity is broken
//       (ADR-07). Duplicates, late arrivals, unverifiable old events and
//       stale quotes are NOT forwarded.
//
//   fault_observe
//       Everything business-valid is forwarded in arrival order with its
//       flags, into a test-only sink, so a fault-injection run can be scored.
//       This mode requires a verified fault manifest and may never feed the
//       production-like P04 sink.
#pragma once

#include "md_adapter/config.hpp"
#include "md_adapter/domain_event.hpp"
#include "md_adapter/quote_freshness.hpp"
#include "md_adapter/sequence_tracker.hpp"

namespace md {

class Classifier {
 public:
  struct Decision {
    bool publish = false;
    std::uint32_t quality_flags = quality::kNone;
    // The single reason chosen for the partitioned counters. Diagnostics may
    // overlap; this one does not.
    Reason primary_reason = Reason::kNone;
    Reason suppressed_reason = Reason::kNone;
    bool integrity_alert = false;
  };

  explicit Classifier(ValidationMode mode) : mode_(mode) {}

  Decision classify(const DomainEvent& e, const SeqResult& seq,
                    const FreshnessEngine::Result& fresh) const;

  ValidationMode mode() const noexcept { return mode_; }

 private:
  ValidationMode mode_;
};

}  // namespace md
