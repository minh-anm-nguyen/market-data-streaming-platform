// Injected clocks. Freshness and deadlines must be drivable from tests without
// sleeping, and offline replay must never consult wall-clock time (FR-06/FR-10).
#pragma once

#include <cstdint>
#include <mutex>

namespace md {

class Clock {
 public:
  virtual ~Clock() = default;
  // Monotonic nanoseconds. Guaranteed non-decreasing within one Clock object.
  virtual std::int64_t now_mono_ns() const = 0;
};

// CLOCK_MONOTONIC. Note: on Linux this does not advance across system suspend;
// detecting hibernation would need CLOCK_BOOTTIME and is out of MVP scope
// (design section 6).
class SteadyClock final : public Clock {
 public:
  std::int64_t now_mono_ns() const override;
};

// Scriptable clock for replay and tests. Time only moves when advanced.
class VirtualClock final : public Clock {
 public:
  explicit VirtualClock(std::int64_t start_ns = 0) : now_(start_ns) {}

  std::int64_t now_mono_ns() const override {
    std::lock_guard<std::mutex> g(m_);
    return now_;
  }
  void advance_ns(std::int64_t delta) {
    std::lock_guard<std::mutex> g(m_);
    if (delta > 0) now_ += delta;
  }
  void advance_ms(std::int64_t delta_ms) { advance_ns(delta_ms * 1'000'000); }
  void set_ns(std::int64_t v) {
    std::lock_guard<std::mutex> g(m_);
    if (v > now_) now_ = v;  // never goes backwards
  }

 private:
  mutable std::mutex m_;
  std::int64_t now_{0};
};

}  // namespace md
