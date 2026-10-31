// Counters, latency histogram and the conservation equations.
//
// The partition counters (design 7.3) must add up exactly. Diagnostic/tag
// counters (gaps, duplicates, stale) deliberately overlap and are never summed
// into a partition -- an event can carry several quality flags at once.
#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "md_adapter/status.hpp"

namespace md {

// Compact HDR-style histogram: 16 linear sub-buckets per power of two, so the
// reported quantile is within ~6% of the true value with 1 KiB of state.
class LatencyHistogram {
 public:
  void record(std::int64_t ns);
  // Returns 0 when no samples were recorded.
  std::int64_t quantile(double q) const;
  std::uint64_t count() const noexcept { return count_; }
  std::int64_t max() const noexcept { return max_; }
  std::int64_t min() const noexcept { return count_ == 0 ? 0 : min_; }
  double mean() const noexcept;
  void reset();

 private:
  static constexpr unsigned kSubBits = 4;
  static constexpr unsigned kSub = 1u << kSubBits;
  static constexpr unsigned kBuckets = 64 * kSub;
  std::array<std::uint64_t, kBuckets> b_{};
  std::uint64_t count_ = 0;
  std::int64_t max_ = 0;
  std::int64_t min_ = 0;
  double sum_ = 0.0;
};

struct Metrics {
  // --- partition chain (must balance) --------------------------------------
  std::uint64_t transport_bytes_read = 0;
  std::uint64_t frames_complete = 0;
  std::uint64_t frames_decode_error = 0;
  std::uint64_t frames_decoded = 0;
  std::uint64_t structural_reject = 0;
  std::uint64_t structural_ok = 0;
  std::uint64_t business_reject = 0;
  std::uint64_t business_ok = 0;
  std::uint64_t policy_suppressed = 0;
  std::uint64_t classified_output_attempted = 0;
  std::uint64_t output_success = 0;
  std::uint64_t output_failed_or_pending = 0;

  // --- bounded per-reason breakdowns ---------------------------------------
  std::array<std::uint64_t, static_cast<std::size_t>(Reason::kReasonCount)>
      structural_reject_by_reason{};
  std::array<std::uint64_t, static_cast<std::size_t>(Reason::kReasonCount)>
      business_reject_by_reason{};
  std::array<std::uint64_t, static_cast<std::size_t>(Reason::kReasonCount)>
      policy_suppressed_by_reason{};

  // --- diagnostics (overlapping tags; never summed into the partition) -----
  std::uint64_t gap_detected = 0;
  std::uint64_t sequence_missing_slots_observed = 0;
  std::uint64_t duplicate_exact = 0;
  std::uint64_t duplicate_conflict = 0;
  std::uint64_t reorder_late = 0;
  std::uint64_t too_old_unverifiable = 0;
  std::uint64_t sequence_rollback = 0;
  std::uint64_t stale_event_time = 0;
  std::uint64_t quote_inactive_transitions = 0;
  std::uint64_t recovered_transitions = 0;
  std::uint64_t start_unverified = 0;
  std::uint64_t state_cap_exceeded = 0;
  std::uint64_t unknown_fields_seen = 0;
  std::uint64_t raw_sequence_observed_invalid = 0;
  std::uint64_t unexpected_faults = 0;

  // --- transport / resources ------------------------------------------------
  std::uint64_t connection_epochs = 0;
  std::uint64_t reconnects = 0;
  std::uint64_t read_paused_ns = 0;
  std::uint64_t read_pause_events = 0;
  std::uint64_t peak_queue_events = 0;
  std::uint64_t peak_queue_bytes = 0;
  std::uint64_t output_pending = 0;

  LatencyHistogram processing_latency;

  void count_structural_reject(Reason r);
  void count_business_reject(Reason r);
  void count_policy_suppressed(Reason r);

  struct Conservation {
    bool ok = true;
    std::vector<std::string> violations;
  };
  // Checks the equations from design 7.3.
  Conservation check_conservation() const;
};

// Peak RSS in bytes from /proc/self/status, or 0 when unavailable.
std::uint64_t current_rss_bytes();
std::uint64_t peak_rss_bytes();
// Open file descriptors, or 0 when unavailable.
std::uint64_t open_fd_count();

}  // namespace md
