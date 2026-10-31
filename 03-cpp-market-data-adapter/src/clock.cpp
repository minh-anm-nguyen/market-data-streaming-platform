#include "md_adapter/clock.hpp"

#include <time.h>

namespace md {

std::int64_t SteadyClock::now_mono_ns() const {
  // CLOCK_MONOTONIC, read directly rather than through std::chrono so the
  // choice of clock is explicit and auditable. Note it does not advance
  // across system suspend on Linux; detecting that would need
  // CLOCK_BOOTTIME and is out of MVP scope (design section 6).
  struct ::timespec ts {};
  if (::clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
  return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000 +
         static_cast<std::int64_t>(ts.tv_nsec);
}

}  // namespace md
