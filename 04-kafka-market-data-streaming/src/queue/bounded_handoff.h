// The two-tier bounded handoff budget (section 4.4, NFR-03, ADR-07,
// cases A-07, A-08, A-14, I-*).
//
// This is a credit allocator, not a container. Section 4.3 shows the shape:
// reserve a budget before calling produce, release it when the delivery report
// arrives, and release it immediately if produce itself refuses. So the depth
// of this structure is exactly "attempts that P04 has accepted responsibility
// for but has no terminal outcome for yet", which is the number that must stay
// bounded whatever the broker does.
//
// Both caps are enforced, records and bytes, and whichever is hit first wins
// (NFR-03). A records-only cap would let 4096 maximum-size events hold 16 MiB,
// and a bytes-only cap would let a flood of tiny events hold unbounded
// per-attempt bookkeeping.
//
// Nothing is ever dropped to make room. Section 2.3 forbids dropping the oldest
// and forbids unbounded allocation, which leaves exactly one honest option:
// refuse the new reservation and tell the caller to pause or fail.
//
// Hysteresis is why there are two watermarks. Pausing at 80% and resuming only
// at 40% stops the upstream from oscillating between paused and running once
// per event, which case A-07 asks to be demonstrated rather than assumed.
#ifndef P04_QUEUE_BOUNDED_HANDOFF_H_
#define P04_QUEUE_BOUNDED_HANDOFF_H_

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>

namespace p04 {

struct HandoffConfig {
  std::size_t max_records = 4096;
  std::size_t max_bytes = 8u * 1024u * 1024u;
  int high_watermark_percent = 80;
  int low_watermark_percent = 40;
  // How long a blocking reservation may wait for the queue to drain before the
  // run is declared INCOMPLETE rather than waiting forever (section 4.4).
  int stall_timeout_ms = 5000;
};

enum class ReserveResult {
  kAccepted,
  kRejectedFull,      // non-blocking caller must apply backpressure upstream
  kRejectedTooLarge,  // single item larger than the whole budget; a contract bug
  kStallTimeout,      // blocking caller waited past stall_timeout_ms
  kClosed,            // shutdown began; no new work is accepted
};

const char* ToString(ReserveResult result);

struct HandoffStats {
  std::size_t records = 0;
  std::size_t bytes = 0;
  std::size_t max_records_observed = 0;
  std::size_t max_bytes_observed = 0;
  double oldest_age_seconds = 0.0;
  std::uint64_t reserve_accepted = 0;
  std::uint64_t reserve_rejected_full = 0;
  std::uint64_t reserve_rejected_too_large = 0;
  std::uint64_t reserve_stall_timeout = 0;
  std::uint64_t pause_transitions = 0;
  std::uint64_t resume_transitions = 0;
  double backpressure_seconds_total = 0.0;
  bool paused = false;
};

class BoundedHandoff {
 public:
  explicit BoundedHandoff(HandoffConfig config);

  // Non-blocking. Returns kAccepted only if both budgets allow the item.
  //
  // attempt_id must be unique and monotonically increasing; it is the key used
  // to track the age of the oldest outstanding reservation, and reusing one
  // would make the oldest-age metric lie.
  ReserveResult TryReserve(std::uint64_t attempt_id, std::size_t bytes);

  // Blocks until the budget allows the item or stall_timeout_ms elapses. Used
  // by the single-owner input path, which is allowed to stall upstream but is
  // never allowed to buffer without limit.
  ReserveResult ReserveOrWait(std::uint64_t attempt_id, std::size_t bytes);

  // Releases a reservation. The byte count is the one recorded at reserve time
  // rather than one the caller supplies again: a caller that computed it twice
  // could pass a different number and silently corrupt the budget, and the
  // delivery-report path is far away from the reserve path.
  //
  // Safe to call from a delivery-report callback: it takes the lock, updates
  // counters, notifies waiters and returns. Section 4.3 warns against doing
  // anything slow in a callback, and this is the whole reason the callback does
  // not also do disk IO.
  //
  // Returns false if the attempt id was not outstanding, which means the ledger
  // and the budget have diverged and the caller should report it rather than
  // ignore it.
  bool Release(std::uint64_t attempt_id);

  // Whether the upstream should currently stop handing over events. Latches at
  // the high watermark and only clears at the low watermark.
  bool ShouldPauseUpstream() const;

  // Stops accepting new reservations. Outstanding ones can still be released,
  // which is what lets a drain finish after SIGTERM.
  void Close();
  bool closed() const;

  HandoffStats Stats() const;

  const HandoffConfig& config() const { return config_; }

 private:
  bool FitsLocked(std::size_t bytes) const;
  void UpdatePauseStateLocked();

  HandoffConfig config_;
  std::size_t high_records_;
  std::size_t high_bytes_;
  std::size_t low_records_;
  std::size_t low_bytes_;

  mutable std::mutex mutex_;
  std::condition_variable drained_;

  std::size_t records_ = 0;
  std::size_t bytes_ = 0;
  bool paused_ = false;
  bool closed_ = false;

  // Ordered by attempt id, which is monotonic, so begin() is the oldest
  // outstanding reservation. Bounded by max_records, so this map can never grow
  // past the configured cap.
  struct Reservation {
    std::chrono::steady_clock::time_point reserved_at;
    std::size_t bytes = 0;
  };
  std::map<std::uint64_t, Reservation> outstanding_;

  std::chrono::steady_clock::time_point paused_since_{};
  HandoffStats stats_{};
};

}  // namespace p04

#endif  // P04_QUEUE_BOUNDED_HANDOFF_H_
