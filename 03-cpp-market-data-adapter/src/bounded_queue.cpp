#include "md_adapter/bounded_queue.hpp"

#include <chrono>

namespace md {

std::size_t estimate_event_bytes(const ClassifiedEvent& e) {
  // sizeof covers the fixed part; the strings own separate heap blocks, and
  // the deque node plus allocator rounding is charged a flat allowance so the
  // byte cap reflects real memory rather than just payload size.
  constexpr std::size_t kNodeOverhead = 64;
  return sizeof(ClassifiedEvent) + e.source.dataset_id.size() +
         e.source.symbol.size() + e.source.source_schema_version.size() +
         e.raw_wire.size() + kNodeOverhead;
}

BoundedQueue::PushResult BoundedQueue::try_push(ClassifiedEvent&& e) {
  const std::size_t cost = estimate_event_bytes(e);
  bool was_empty = false;
  {
    std::lock_guard<std::mutex> g(m_);
    if (closed_) return PushResult::kClosed;
    // In-flight work counts against the caps: an event handed to the writer
    // but not yet written is still memory this process is holding.
    if (q_.size() + inflight_events_ + 1 > limits_.max_events)
      return PushResult::kRejectedFull;
    if (bytes_ + inflight_bytes_ + cost > limits_.max_bytes)
      return PushResult::kRejectedFull;
    was_empty = q_.empty();
    q_.push_back(std::move(e));
    bytes_ += cost;
    const std::size_t outstanding_events = q_.size() + inflight_events_;
    const std::size_t outstanding_bytes = bytes_ + inflight_bytes_;
    if (outstanding_events > peak_events_) peak_events_ = outstanding_events;
    if (outstanding_bytes > peak_bytes_) peak_bytes_ = outstanding_bytes;
  }
  // Only the empty -> non-empty edge can have a waiter to wake: the writer
  // waits solely when it found the queue empty, and its wait is bounded, so
  // a missed wake costs at most one timeout rather than a lost event. At a
  // million events a second the unconditional wake was the dominant cost of
  // the hand-off.
  if (was_empty) cv_.notify_one();
  return PushResult::kAccepted;
}

std::optional<ClassifiedEvent> BoundedQueue::pop(std::int64_t timeout_ns) {
  std::unique_lock<std::mutex> lk(m_);
  if (q_.empty()) {
    if (closed_) return std::nullopt;
    cv_.wait_for(lk, std::chrono::nanoseconds(timeout_ns),
                 [this] { return !q_.empty() || closed_; });
  }
  if (q_.empty()) return std::nullopt;
  ClassifiedEvent e = std::move(q_.front());
  q_.pop_front();
  const std::size_t cost = estimate_event_bytes(e);
  bytes_ = (bytes_ >= cost) ? bytes_ - cost : 0;
  lk.unlock();
  cv_.notify_all();
  return e;
}

std::size_t BoundedQueue::pop_batch(std::size_t max_items,
                                    std::vector<ClassifiedEvent>& out) {
  std::size_t n = 0;
  {
    std::lock_guard<std::mutex> g(m_);
    while (n < max_items && !q_.empty()) {
      ClassifiedEvent e = std::move(q_.front());
      q_.pop_front();
      const std::size_t cost = estimate_event_bytes(e);
      bytes_ = (bytes_ >= cost) ? bytes_ - cost : 0;
      inflight_bytes_ += cost;
      ++inflight_events_;
      out.push_back(std::move(e));
      ++n;
    }
  }
  return n;  // nothing was freed yet, so no waiter can make progress
}

void BoundedQueue::release(const std::vector<ClassifiedEvent>& batch) {
  {
    std::lock_guard<std::mutex> g(m_);
    for (const auto& e : batch) {
      const std::size_t cost = estimate_event_bytes(e);
      inflight_bytes_ = (inflight_bytes_ >= cost) ? inflight_bytes_ - cost : 0;
      if (inflight_events_ > 0) --inflight_events_;
    }
  }
  cv_.notify_all();
}

std::size_t BoundedQueue::outstanding() const {
  std::lock_guard<std::mutex> g(m_);
  return q_.size() + inflight_events_;
}

void BoundedQueue::close() {
  {
    std::lock_guard<std::mutex> g(m_);
    closed_ = true;
  }
  cv_.notify_all();
}

bool BoundedQueue::closed() const {
  std::lock_guard<std::mutex> g(m_);
  return closed_;
}

bool BoundedQueue::should_pause() const {
  std::lock_guard<std::mutex> g(m_);
  const auto pct_events = (q_.size() + inflight_events_) * 100 /
                          (limits_.max_events == 0 ? 1 : limits_.max_events);
  const auto pct_bytes = (bytes_ + inflight_bytes_) * 100 /
                         (limits_.max_bytes == 0 ? 1 : limits_.max_bytes);
  const auto hi = static_cast<std::size_t>(limits_.pause_high_watermark_pct);
  return pct_events >= hi || pct_bytes >= hi;
}

bool BoundedQueue::should_resume() const {
  std::lock_guard<std::mutex> g(m_);
  const auto pct_events = (q_.size() + inflight_events_) * 100 /
                          (limits_.max_events == 0 ? 1 : limits_.max_events);
  const auto pct_bytes = (bytes_ + inflight_bytes_) * 100 /
                         (limits_.max_bytes == 0 ? 1 : limits_.max_bytes);
  const auto lo = static_cast<std::size_t>(limits_.resume_low_watermark_pct);
  return pct_events <= lo && pct_bytes <= lo;
}

std::size_t BoundedQueue::size() const {
  std::lock_guard<std::mutex> g(m_);
  return q_.size();
}
std::size_t BoundedQueue::bytes() const {
  std::lock_guard<std::mutex> g(m_);
  return bytes_;
}
std::size_t BoundedQueue::peak_events() const {
  std::lock_guard<std::mutex> g(m_);
  return peak_events_;
}
std::size_t BoundedQueue::peak_bytes() const {
  std::lock_guard<std::mutex> g(m_);
  return peak_bytes_;
}

}  // namespace md
