#include "md_adapter/metrics.hpp"

#include <dirent.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>

namespace md {
namespace {

unsigned bucket_of(std::uint64_t v) {
  if (v == 0) return 0;
  const unsigned e = 63u - static_cast<unsigned>(__builtin_clzll(v));
  if (e < 4) return static_cast<unsigned>(v);  // exact for the first 16 values
  const unsigned sub =
      static_cast<unsigned>((v >> (e - 4)) & 0xFu);  // 4 mantissa bits
  return (e << 4) | sub;
}

std::uint64_t bucket_low(unsigned idx) {
  const unsigned e = idx >> 4;
  if (e < 4) return idx;
  const std::uint64_t mant = 16u + (idx & 0xFu);
  return mant << (e - 4);
}

}  // namespace

void LatencyHistogram::record(std::int64_t ns) {
  if (ns < 0) ns = 0;
  const unsigned idx = bucket_of(static_cast<std::uint64_t>(ns));
  if (idx < kBuckets) ++b_[idx];
  if (count_ == 0 || ns < min_) min_ = ns;
  if (ns > max_) max_ = ns;
  sum_ += static_cast<double>(ns);
  ++count_;
}

std::int64_t LatencyHistogram::quantile(double q) const {
  if (count_ == 0) return 0;
  if (q < 0.0) q = 0.0;
  if (q > 1.0) q = 1.0;
  const auto target = static_cast<std::uint64_t>(
      static_cast<double>(count_) * q + 0.5);
  std::uint64_t seen = 0;
  for (unsigned i = 0; i < kBuckets; ++i) {
    seen += b_[i];
    if (seen >= target && b_[i] != 0)
      return static_cast<std::int64_t>(bucket_low(i));
  }
  return max_;
}

double LatencyHistogram::mean() const noexcept {
  return count_ == 0 ? 0.0 : sum_ / static_cast<double>(count_);
}

void LatencyHistogram::reset() {
  b_.fill(0);
  count_ = 0;
  max_ = 0;
  min_ = 0;
  sum_ = 0.0;
}

void Metrics::count_structural_reject(Reason r) {
  ++structural_reject;
  structural_reject_by_reason[static_cast<std::size_t>(r)] += 1;
}
void Metrics::count_business_reject(Reason r) {
  ++business_reject;
  business_reject_by_reason[static_cast<std::size_t>(r)] += 1;
}
void Metrics::count_policy_suppressed(Reason r) {
  ++policy_suppressed;
  policy_suppressed_by_reason[static_cast<std::size_t>(r)] += 1;
}

Metrics::Conservation Metrics::check_conservation() const {
  Conservation c;
  const auto eq = [&c](const char* name, std::uint64_t lhs, std::uint64_t rhs) {
    if (lhs != rhs) {
      c.ok = false;
      c.violations.push_back(std::string(name) + ": " + std::to_string(lhs) +
                             " != " + std::to_string(rhs));
    }
  };
  eq("frames_complete == frames_decode_error + frames_decoded", frames_complete,
     frames_decode_error + frames_decoded);
  eq("frames_decoded == structural_reject + structural_ok", frames_decoded,
     structural_reject + structural_ok);
  eq("structural_ok == business_reject + business_ok", structural_ok,
     business_reject + business_ok);
  eq("business_ok == policy_suppressed + classified_output_attempted",
     business_ok, policy_suppressed + classified_output_attempted);
  eq("classified_output_attempted == output_success + output_failed_or_pending",
     classified_output_attempted, output_success + output_failed_or_pending);

  std::uint64_t sr = 0;
  for (auto v : structural_reject_by_reason) sr += v;
  eq("sum(structural_reject_by_reason) == structural_reject", sr,
     structural_reject);
  std::uint64_t br = 0;
  for (auto v : business_reject_by_reason) br += v;
  eq("sum(business_reject_by_reason) == business_reject", br, business_reject);
  std::uint64_t ps = 0;
  for (auto v : policy_suppressed_by_reason) ps += v;
  eq("sum(policy_suppressed_by_reason) == policy_suppressed", ps,
     policy_suppressed);
  return c;
}

namespace {
std::uint64_t read_status_kib(const char* field) {
  std::ifstream f("/proc/self/status");
  if (!f) return 0;
  std::string line;
  const std::size_t flen = std::strlen(field);
  while (std::getline(f, line)) {
    if (line.compare(0, flen, field) != 0) continue;
    const char* p = line.c_str() + flen;
    while (*p == ' ' || *p == '\t' || *p == ':') ++p;
    return static_cast<std::uint64_t>(std::strtoull(p, nullptr, 10)) * 1024u;
  }
  return 0;
}
}  // namespace

std::uint64_t current_rss_bytes() { return read_status_kib("VmRSS"); }
std::uint64_t peak_rss_bytes() { return read_status_kib("VmHWM"); }

std::uint64_t open_fd_count() {
  DIR* d = ::opendir("/proc/self/fd");
  if (d == nullptr) return 0;
  std::uint64_t n = 0;
  while (::readdir(d) != nullptr) ++n;
  ::closedir(d);
  return n >= 3 ? n - 3 : 0;  // '.', '..' and the opendir fd itself
}

}  // namespace md
