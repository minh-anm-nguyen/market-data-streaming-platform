#include "queue/bounded_handoff.h"

#include <algorithm>

namespace p04 {
namespace {

std::size_t PercentOf(std::size_t value, int percent) {
  if (percent <= 0) return 0;
  if (percent >= 100) return value;
  return (value * static_cast<std::size_t>(percent)) / 100u;
}

}  // namespace

const char* ToString(ReserveResult result) {
  switch (result) {
    case ReserveResult::kAccepted:         return "ACCEPTED";
    case ReserveResult::kRejectedFull:     return "REJECTED_FULL";
    case ReserveResult::kRejectedTooLarge: return "REJECTED_TOO_LARGE";
    case ReserveResult::kStallTimeout:     return "STALL_TIMEOUT";
    case ReserveResult::kClosed:           return "CLOSED";
  }
  return "UNMAPPED";
}

BoundedHandoff::BoundedHandoff(HandoffConfig config) : config_(config) {
  // Watermarks are precomputed so the hot path does no division. They are also
  // clamped into a sane order: a config with low >= high would make the
  // hysteresis meaningless, and silently accepting it would turn case A-07 into
  // a test of nothing.
  high_records_ = std::max<std::size_t>(1, PercentOf(config_.max_records, config_.high_watermark_percent));
  high_bytes_ = std::max<std::size_t>(1, PercentOf(config_.max_bytes, config_.high_watermark_percent));
  low_records_ = PercentOf(config_.max_records, config_.low_watermark_percent);
  low_bytes_ = PercentOf(config_.max_bytes, config_.low_watermark_percent);
  if (low_records_ >= high_records_) low_records_ = high_records_ > 0 ? high_records_ - 1 : 0;
  if (low_bytes_ >= high_bytes_) low_bytes_ = high_bytes_ > 0 ? high_bytes_ - 1 : 0;
}

bool BoundedHandoff::FitsLocked(std::size_t bytes) const {
  return records_ + 1 <= config_.max_records && bytes_ + bytes <= config_.max_bytes;
}

void BoundedHandoff::UpdatePauseStateLocked() {
  const auto now = std::chrono::steady_clock::now();
  if (!paused_) {
    if (records_ >= high_records_ || bytes_ >= high_bytes_) {
      paused_ = true;
      paused_since_ = now;
      ++stats_.pause_transitions;
    }
    return;
  }
  // Only the low watermark clears the pause. Using the same threshold for both
  // directions is the classic flapping bug this avoids.
  if (records_ <= low_records_ && bytes_ <= low_bytes_) {
    paused_ = false;
    ++stats_.resume_transitions;
    stats_.backpressure_seconds_total +=
        std::chrono::duration<double>(now - paused_since_).count();
  }
}

ReserveResult BoundedHandoff::TryReserve(std::uint64_t attempt_id, std::size_t bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) return ReserveResult::kClosed;
  // An item that cannot fit even in an empty queue is a contract violation, not
  // backpressure. Reporting it as full would send the upstream into a pause
  // loop waiting for space that will never be enough.
  if (bytes > config_.max_bytes) {
    ++stats_.reserve_rejected_too_large;
    return ReserveResult::kRejectedTooLarge;
  }
  if (!FitsLocked(bytes)) {
    ++stats_.reserve_rejected_full;
    UpdatePauseStateLocked();
    return ReserveResult::kRejectedFull;
  }
  records_ += 1;
  bytes_ += bytes;
  outstanding_.emplace(attempt_id, Reservation{std::chrono::steady_clock::now(), bytes});
  stats_.max_records_observed = std::max(stats_.max_records_observed, records_);
  stats_.max_bytes_observed = std::max(stats_.max_bytes_observed, bytes_);
  ++stats_.reserve_accepted;
  UpdatePauseStateLocked();
  return ReserveResult::kAccepted;
}

ReserveResult BoundedHandoff::ReserveOrWait(std::uint64_t attempt_id, std::size_t bytes) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (closed_) return ReserveResult::kClosed;
  if (bytes > config_.max_bytes) {
    ++stats_.reserve_rejected_too_large;
    return ReserveResult::kRejectedTooLarge;
  }

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(config_.stall_timeout_ms);
  const auto wait_start = std::chrono::steady_clock::now();
  while (!FitsLocked(bytes)) {
    if (closed_) return ReserveResult::kClosed;
    // wait_until rather than wait_for in a loop: a spurious wakeup must not
    // restart the deadline, or a busy broker could keep the caller here
    // indefinitely while still reporting that it honoured a 5 s budget.
    if (drained_.wait_until(lock, deadline) == std::cv_status::timeout &&
        !FitsLocked(bytes)) {
      ++stats_.reserve_stall_timeout;
      stats_.backpressure_seconds_total +=
          std::chrono::duration<double>(std::chrono::steady_clock::now() - wait_start).count();
      return ReserveResult::kStallTimeout;
    }
  }
  records_ += 1;
  bytes_ += bytes;
  outstanding_.emplace(attempt_id, Reservation{std::chrono::steady_clock::now(), bytes});
  stats_.max_records_observed = std::max(stats_.max_records_observed, records_);
  stats_.max_bytes_observed = std::max(stats_.max_bytes_observed, bytes_);
  ++stats_.reserve_accepted;
  UpdatePauseStateLocked();
  return ReserveResult::kAccepted;
}

bool BoundedHandoff::Release(std::uint64_t attempt_id) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = outstanding_.find(attempt_id);
    if (it == outstanding_.end()) {
      // Releasing an unknown attempt means the ledger and the budget have
      // diverged. Silently subtracting here would corrupt the budget and could
      // underflow it, so the release is refused and the caller reports it.
      return false;
    }
    const std::size_t bytes = it->second.bytes;
    outstanding_.erase(it);
    records_ -= 1;
    bytes_ = bytes >= bytes_ ? 0 : bytes_ - bytes;
    UpdatePauseStateLocked();
  }
  drained_.notify_all();
  return true;
}

bool BoundedHandoff::ShouldPauseUpstream() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return paused_;
}

void BoundedHandoff::Close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }
  drained_.notify_all();
}

bool BoundedHandoff::closed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_;
}

HandoffStats BoundedHandoff::Stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  HandoffStats out = stats_;
  out.records = records_;
  out.bytes = bytes_;
  out.paused = paused_;
  if (!outstanding_.empty()) {
    out.oldest_age_seconds = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() -
                                 outstanding_.begin()->second.reserved_at)
                                 .count();
  }
  // A run that is currently paused has not yet closed its backpressure
  // interval, so the accumulated total would under-report. Adding the open
  // interval here keeps the metric honest at the moment it is read.
  if (paused_) {
    out.backpressure_seconds_total +=
        std::chrono::duration<double>(std::chrono::steady_clock::now() - paused_since_).count();
  }
  return out;
}

}  // namespace p04
