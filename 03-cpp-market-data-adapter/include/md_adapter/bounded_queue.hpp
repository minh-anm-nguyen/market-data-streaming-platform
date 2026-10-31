// Bounded hand-off queue between the single owner thread (io + decode +
// classification) and the sink writer thread.
//
// Both caps are enforced: an event count and a byte budget. Counting only
// entries lets many small events blow the memory budget, and counting only
// bytes lets many tiny events blow the per-object allocation overhead
// (design 7.2).
//
// There is no "drop oldest" and no silent discard. When the queue is full the
// producer is told to pause reading; if the sink never drains, the run fails
// with BACKPRESSURE_TIMEOUT and reports what was still pending.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

#include "md_adapter/domain_event.hpp"
#include "md_adapter/status.hpp"

namespace md {

struct QueueLimits {
  std::size_t max_events = 4096;
  std::size_t max_bytes = 8 * 1024 * 1024;
  int pause_high_watermark_pct = 80;
  int resume_low_watermark_pct = 40;
};

// Conservative per-event accounting: the payload plus the heap the strings
// own plus a fixed allowance for the node and allocator overhead.
std::size_t estimate_event_bytes(const ClassifiedEvent& e);

class BoundedQueue {
 public:
  explicit BoundedQueue(QueueLimits limits) : limits_(limits) {}

  enum class PushResult { kAccepted, kRejectedFull, kClosed };

  // Non-blocking. Returns kRejectedFull when the event does not fit; the
  // caller must then pause reading rather than discard anything.
  PushResult try_push(ClassifiedEvent&& e);

  // Blocks until an event is available, the queue is closed and drained, or
  // `timeout_ns` elapses. Returns nullopt on timeout or on closed+empty.
  std::optional<ClassifiedEvent> pop(std::int64_t timeout_ns);

  // Pops up to `max_items` without blocking, charging them to *in flight*
  // rather than discharging them. Batching keeps the writer off the lock on
  // every event, but an event that has left the deque and not yet reached
  // the sink is still unwritten work: if it stopped counting here, the caps
  // would stop describing anything and backpressure would never engage.
  //
  // The caller MUST call release() once the batch has been written.
  std::size_t pop_batch(std::size_t max_items, std::vector<ClassifiedEvent>& out);

  // Discharges a batch previously obtained from pop_batch().
  void release(const std::vector<ClassifiedEvent>& batch);

  // Queued plus in-flight: the real amount of work that has been accepted
  // and not yet written.
  std::size_t outstanding() const;

  // After close() the writer drains what is left and then sees kClosed.
  void close();
  bool closed() const;

  bool should_pause() const;   // at/above the high watermark
  bool should_resume() const;  // at/below the low watermark

  std::size_t size() const;
  std::size_t bytes() const;
  std::size_t peak_events() const;
  std::size_t peak_bytes() const;
  const QueueLimits& limits() const noexcept { return limits_; }

 private:
  mutable std::mutex m_;
  std::condition_variable cv_;
  std::deque<ClassifiedEvent> q_;
  std::size_t bytes_ = 0;
  std::size_t inflight_events_ = 0;
  std::size_t inflight_bytes_ = 0;
  std::size_t peak_events_ = 0;
  std::size_t peak_bytes_ = 0;
  bool closed_ = false;
  QueueLimits limits_;
};

}  // namespace md
