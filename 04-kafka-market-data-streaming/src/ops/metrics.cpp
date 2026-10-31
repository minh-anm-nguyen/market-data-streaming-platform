#include "ops/metrics.h"

#include <algorithm>
#include <cstdio>
#include <dirent.h>
#include <unistd.h>
#include <cstdlib>
#include <fstream>
#include <sstream>

#include "policy/publish_gate.h"
#include "routing/route_table.h"

namespace p04 {

static_assert(kErrorClassCount == static_cast<std::size_t>(ErrorClass::kPolicySuppressed) + 1,
              "kErrorClassCount must match the ErrorClass enum; a new class "
              "without a bigger array would write past the counters");

void LatencyHistogram::Observe(double seconds) {
  ++count_;
  sum_ += seconds;
  max_ = std::max(max_, seconds);
  for (std::size_t i = 0; i < kLatencyBucketsSeconds.size(); ++i) {
    if (seconds <= kLatencyBucketsSeconds[i]) {
      ++buckets_[i];
      return;
    }
  }
  ++buckets_[kLatencyBucketsSeconds.size()];  // overflow
}

double LatencyHistogram::Quantile(double q) const {
  if (count_ == 0) return -1.0;
  const auto target = static_cast<std::uint64_t>(
      static_cast<double>(count_) * std::min(std::max(q, 0.0), 1.0));
  std::uint64_t cumulative = 0;
  for (std::size_t i = 0; i < kLatencyBucketsSeconds.size(); ++i) {
    cumulative += buckets_[i];
    if (cumulative >= target && cumulative > 0) return kLatencyBucketsSeconds[i];
  }
  // Everything above the last bound: report the observed maximum rather than
  // the bound, since claiming 30 s when the real value was 90 s would understate
  // the problem.
  return max_;
}

Metrics::Metrics(std::string ticks_topic, std::string quotes_topic,
                 std::int32_t partition_count)
    : ticks_topic_(std::move(ticks_topic)),
      quotes_topic_(std::move(quotes_topic)),
      partition_count_(partition_count > 0 ? partition_count : 1) {
  // Sized from the closed enums, so the arrays cannot be outgrown by data.
  policy_suppressed_.assign(16, 0);
  route_invalid_.assign(32, 0);
  acked_by_partition_.assign(static_cast<std::size_t>(partition_count_), 0);
}

TopicSlot Metrics::SlotFor(const std::string& topic) const {
  if (topic == ticks_topic_) return TopicSlot::kTicks;
  if (topic == quotes_topic_) return TopicSlot::kQuotes;
  return TopicSlot::kOther;
}

void Metrics::UpstreamAccepted(bool is_quote) {
  ++upstream_accepted_[is_quote ? 0 : 1];
}

void Metrics::PolicySuppressed(std::size_t reason_index) {
  if (reason_index < policy_suppressed_.size()) ++policy_suppressed_[reason_index];
}

void Metrics::RouteInvalid(std::size_t reason_index) {
  if (reason_index < route_invalid_.size()) ++route_invalid_[reason_index];
}

void Metrics::ProduceEnqueued(const std::string& topic) {
  ++enqueued_[static_cast<std::size_t>(SlotFor(topic))];
}

void Metrics::ProduceEnqueueFailed(ErrorClass error_class) {
  ++enqueue_failed_[static_cast<std::size_t>(error_class)];
}

void Metrics::DeliveryAcked(const std::string& topic, std::int32_t partition,
                            double ack_latency_seconds) {
  ++acked_by_topic_[static_cast<std::size_t>(SlotFor(topic))];
  if (partition >= 0 && partition < partition_count_) {
    ++acked_by_partition_[static_cast<std::size_t>(partition)];
  } else {
    // A partition outside the configured range is itself the finding: the topic
    // has more partitions than the pinned contract says (case B-05).
    ++partition_overflow_;
  }
  if (ack_latency_seconds >= 0.0) ack_latency_.Observe(ack_latency_seconds);
}

void Metrics::DeliveryFailed(ErrorClass error_class) {
  ++delivery_failed_[static_cast<std::size_t>(error_class)];
}

void Metrics::DeliveryUnknown(ErrorClass error_class) {
  ++delivery_unknown_[static_cast<std::size_t>(error_class)];
}

void Metrics::PartitionMappingMismatch() { ++partition_mismatch_; }

void Metrics::CaughtFatalError(ErrorClass error_class) {
  ++fatal_errors_[static_cast<std::size_t>(error_class)];
}

void Metrics::SetAppQueue(std::size_t records, std::size_t bytes,
                          double oldest_age_seconds) {
  app_queue_records_ = records;
  app_queue_bytes_ = bytes;
  app_queue_oldest_age_ = oldest_age_seconds;
}

void Metrics::SetLibQueue(std::int64_t records, std::int64_t bytes) {
  lib_queue_records_ = records;
  lib_queue_bytes_ = bytes;
}

void Metrics::AddBackpressureSeconds(double seconds) {
  backpressure_seconds_ = seconds;
}

void Metrics::BackpressureTimeout() { ++backpressure_timeouts_; }

void Metrics::SetProcessStats(std::uint64_t rss_bytes, std::uint64_t fd_count,
                              double cpu_seconds) {
  rss_bytes_ = rss_bytes;
  fd_count_ = fd_count;
  cpu_seconds_ = cpu_seconds;
}

std::uint64_t Metrics::acked_total() const {
  std::uint64_t total = 0;
  for (const auto value : acked_by_topic_) total += value;
  return total;
}

std::uint64_t Metrics::enqueued_total() const {
  std::uint64_t total = 0;
  for (const auto value : enqueued_) total += value;
  return total;
}

std::string Metrics::RenderPrometheus() const {
  std::ostringstream out;
  const auto topic_name = [&](std::size_t slot) -> std::string {
    switch (static_cast<TopicSlot>(slot)) {
      case TopicSlot::kTicks:  return ticks_topic_;
      case TopicSlot::kQuotes: return quotes_topic_;
      case TopicSlot::kOther:  return "other";
    }
    return "other";
  };

  out << "# HELP p04_upstream_accepted_total Events offered by the adapter.\n";
  out << "# TYPE p04_upstream_accepted_total counter\n";
  out << "p04_upstream_accepted_total{kind=\"quote\"} " << upstream_accepted_[0] << "\n";
  out << "p04_upstream_accepted_total{kind=\"tick\"} " << upstream_accepted_[1] << "\n";

  out << "# TYPE p04_policy_suppressed_total counter\n";
  for (std::size_t i = 0; i < policy_suppressed_.size(); ++i) {
    if (policy_suppressed_[i] == 0) continue;
    out << "p04_policy_suppressed_total{reason=\""
        << ToString(static_cast<SuppressReason>(i)) << "\"} " << policy_suppressed_[i] << "\n";
  }

  out << "# TYPE p04_route_invalid_total counter\n";
  for (std::size_t i = 0; i < route_invalid_.size(); ++i) {
    if (route_invalid_[i] == 0) continue;
    out << "p04_route_invalid_total{reason=\""
        << ToString(static_cast<RouteReject>(i)) << "\"} " << route_invalid_[i] << "\n";
  }

  out << "# TYPE p04_produce_enqueued_total counter\n";
  for (std::size_t i = 0; i < kTopicSlotCount; ++i) {
    out << "p04_produce_enqueued_total{topic=\"" << topic_name(i) << "\"} " << enqueued_[i] << "\n";
  }

  out << "# TYPE p04_delivery_acked_total counter\n";
  for (std::size_t i = 0; i < kTopicSlotCount; ++i) {
    out << "p04_delivery_acked_total{topic=\"" << topic_name(i) << "\"} "
        << acked_by_topic_[i] << "\n";
  }
  for (std::size_t p = 0; p < acked_by_partition_.size(); ++p) {
    out << "p04_delivery_acked_partition_total{partition=\"" << p << "\"} "
        << acked_by_partition_[p] << "\n";
  }
  out << "p04_delivery_acked_partition_overflow_total " << partition_overflow_ << "\n";

  const auto dump_error_array =
      [&](const char* name, const std::array<std::uint64_t, kErrorClassCount>& values) {
        out << "# TYPE " << name << " counter\n";
        for (std::size_t i = 0; i < kErrorClassCount; ++i) {
          if (values[i] == 0) continue;
          out << name << "{error_class=\"" << ToString(static_cast<ErrorClass>(i))
              << "\"} " << values[i] << "\n";
        }
      };
  dump_error_array("p04_produce_enqueue_failed_total", enqueue_failed_);
  dump_error_array("p04_delivery_failed_total", delivery_failed_);
  dump_error_array("p04_delivery_unknown_total", delivery_unknown_);
  dump_error_array("p04_caught_fatal_errors_total", fatal_errors_);

  out << "p04_partition_mapping_mismatch_total " << partition_mismatch_ << "\n";
  out << "p04_app_queue_records " << app_queue_records_ << "\n";
  out << "p04_app_queue_bytes " << app_queue_bytes_ << "\n";
  out << "p04_app_queue_oldest_age_seconds " << app_queue_oldest_age_ << "\n";
  out << "p04_lib_queue_records " << lib_queue_records_ << "\n";
  out << "p04_lib_queue_bytes " << lib_queue_bytes_ << "\n";
  out << "p04_backpressure_seconds_total " << backpressure_seconds_ << "\n";
  out << "p04_backpressure_timeout_total " << backpressure_timeouts_ << "\n";
  out << "p04_process_rss_bytes " << rss_bytes_ << "\n";
  out << "p04_process_fd_count " << fd_count_ << "\n";
  out << "p04_process_cpu_seconds " << cpu_seconds_ << "\n";

  out << "# TYPE p04_broker_ack_latency_seconds histogram\n";
  std::uint64_t cumulative = 0;
  for (std::size_t i = 0; i < kLatencyBucketsSeconds.size(); ++i) {
    cumulative += ack_latency_.buckets()[i];
    out << "p04_broker_ack_latency_seconds_bucket{le=\"" << kLatencyBucketsSeconds[i]
        << "\"} " << cumulative << "\n";
  }
  cumulative += ack_latency_.buckets()[kLatencyBucketsSeconds.size()];
  out << "p04_broker_ack_latency_seconds_bucket{le=\"+Inf\"} " << cumulative << "\n";
  out << "p04_broker_ack_latency_seconds_sum " << ack_latency_.sum() << "\n";
  out << "p04_broker_ack_latency_seconds_count " << ack_latency_.count() << "\n";
  return out.str();
}

bool ReadProcessStats(std::uint64_t* rss_bytes, std::uint64_t* fd_count,
                      double* cpu_seconds) {
  bool any = false;
#if defined(__linux__)
  // /proc/self/statm reports RSS in pages; the page size is queried rather than
  // assumed to be 4096, because a huge-page kernel would make the assumption
  // wrong by a factor of 512.
  std::ifstream statm("/proc/self/statm");
  if (statm) {
    long total_pages = 0;
    long resident_pages = 0;
    statm >> total_pages >> resident_pages;
    if (statm && rss_bytes != nullptr) {
      *rss_bytes = static_cast<std::uint64_t>(resident_pages) *
                   static_cast<std::uint64_t>(sysconf(_SC_PAGESIZE));
      any = true;
    }
  }
  if (fd_count != nullptr) {
    DIR* dir = opendir("/proc/self/fd");
    if (dir != nullptr) {
      std::uint64_t count = 0;
      while (readdir(dir) != nullptr) ++count;
      closedir(dir);
      // Subtract . and .. plus the descriptor opendir itself holds.
      *fd_count = count > 3 ? count - 3 : 0;
      any = true;
    }
  }
  std::ifstream stat("/proc/self/stat");
  if (stat && cpu_seconds != nullptr) {
    std::string field;
    long utime = 0;
    long stime = 0;
    for (int i = 1; i <= 15 && stat >> field; ++i) {
      if (i == 14) utime = std::strtol(field.c_str(), nullptr, 10);
      if (i == 15) stime = std::strtol(field.c_str(), nullptr, 10);
    }
    const long ticks_per_second = sysconf(_SC_CLK_TCK);
    if (ticks_per_second > 0) {
      *cpu_seconds = static_cast<double>(utime + stime) / static_cast<double>(ticks_per_second);
      any = true;
    }
  }
#else
  (void)rss_bytes;
  (void)fd_count;
  (void)cpu_seconds;
#endif
  return any;
}

}  // namespace p04
