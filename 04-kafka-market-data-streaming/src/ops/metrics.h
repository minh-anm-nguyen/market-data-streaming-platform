// The metric set from section 8.1, with the cardinality cap built into the
// types rather than left to discipline (NFR-13).
//
// Section 8.1 forbids labelling metrics by dataset_id, by event id or by an
// arbitrary symbol, and case N-* plus fixture F11 inject 100k fake symbols to
// prove the exporter does not grow with them. The structural answer used here
// is that there are no dynamic label maps at all:
//
//   * per-topic counters are a fixed array of two, the two configured market
//     topics, plus one "other" bucket;
//   * per-partition counters are a fixed array sized by the configured
//     partition count, with anything out of range landing in an overflow slot;
//   * per-error-class counters are a fixed array sized by the ErrorClass enum.
//
// Adding a 100001st symbol therefore allocates nothing. That is a property of
// the data structure, which is a stronger guarantee than a code review.
//
// The histogram is a fixed-bucket latency histogram over the handoff-to-ACK
// interval, measured on the steady clock (NFR-08). It stores counts only; no
// per-sample retention, so its memory is constant regardless of throughput.
#ifndef P04_OPS_METRICS_H_
#define P04_OPS_METRICS_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ops/error_class.h"

namespace p04 {

// Fixed bucket upper bounds in seconds. Chosen to straddle the latencies that
// matter here: sub-millisecond for a local broker, the 30 s message timeout at
// the top, and enough resolution in between to see a p99 move.
inline constexpr std::array<double, 14> kLatencyBucketsSeconds = {
    0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1,
    0.25,   0.5,   1.0,   5.0,   15.0, 30.0};

class LatencyHistogram {
 public:
  void Observe(double seconds);
  // Interpolation-free quantile: returns the upper bound of the bucket that
  // contains the quantile, or the recorded maximum when it falls in the
  // overflow bucket. Deliberately coarse and honest rather than a smooth curve
  // invented from bucket boundaries.
  double Quantile(double q) const;
  std::uint64_t count() const { return count_; }
  double sum() const { return sum_; }
  double max() const { return max_; }
  const std::array<std::uint64_t, kLatencyBucketsSeconds.size() + 1>& buckets() const {
    return buckets_;
  }

 private:
  std::array<std::uint64_t, kLatencyBucketsSeconds.size() + 1> buckets_{};
  std::uint64_t count_ = 0;
  double sum_ = 0.0;
  double max_ = 0.0;
};

// Which topic a counter belongs to. Three slots, fixed forever: the two market
// topics and everything else.
enum class TopicSlot { kTicks = 0, kQuotes = 1, kOther = 2 };
constexpr std::size_t kTopicSlotCount = 3;

class Metrics {
 public:
  // partition_count sizes the per-partition arrays. One extra slot absorbs any
  // partition id outside the configured range, which is itself a signal: a
  // non-zero overflow count means the topic was repartitioned under us.
  Metrics(std::string ticks_topic, std::string quotes_topic,
          std::int32_t partition_count);

  TopicSlot SlotFor(const std::string& topic) const;

  // Upstream and policy.
  void UpstreamAccepted(bool is_quote);
  void PolicySuppressed(std::size_t reason_index);
  void RouteInvalid(std::size_t reason_index);

  // Producer path.
  void ProduceEnqueued(const std::string& topic);
  void ProduceEnqueueFailed(ErrorClass error_class);
  void DeliveryAcked(const std::string& topic, std::int32_t partition,
                     double ack_latency_seconds);
  void DeliveryFailed(ErrorClass error_class);
  void DeliveryUnknown(ErrorClass error_class);
  void PartitionMappingMismatch();
  void CaughtFatalError(ErrorClass error_class);

  // Queue and backpressure gauges, refreshed from the owners rather than
  // tracked twice.
  void SetAppQueue(std::size_t records, std::size_t bytes, double oldest_age_seconds);
  void SetLibQueue(std::int64_t records, std::int64_t bytes);
  void AddBackpressureSeconds(double seconds);
  void BackpressureTimeout();

  // Process resources, sampled by the caller (section 8.1 asks for RSS, FD
  // count and CPU seconds; reading them is platform-specific and lives in
  // process_stats).
  void SetProcessStats(std::uint64_t rss_bytes, std::uint64_t fd_count,
                       double cpu_seconds);

  // Renders the Prometheus text exposition format. Written to a file in the run
  // directory rather than served over HTTP: this is a lab harness, and an open
  // port would be one more thing to secure for no test value.
  std::string RenderPrometheus() const;

  const LatencyHistogram& ack_latency() const { return ack_latency_; }

  std::uint64_t acked_total() const;
  std::uint64_t enqueued_total() const;
  std::uint64_t partition_overflow_count() const { return partition_overflow_; }

 private:
  std::string ticks_topic_;
  std::string quotes_topic_;
  std::int32_t partition_count_;

  std::array<std::uint64_t, 2> upstream_accepted_{};  // [quote, tick]
  std::vector<std::uint64_t> policy_suppressed_;
  std::vector<std::uint64_t> route_invalid_;

  std::array<std::uint64_t, kTopicSlotCount> enqueued_{};
  std::array<std::uint64_t, kTopicSlotCount> acked_by_topic_{};
  std::vector<std::uint64_t> acked_by_partition_;
  std::uint64_t partition_overflow_ = 0;

  std::array<std::uint64_t, kErrorClassCount> enqueue_failed_{};
  std::array<std::uint64_t, kErrorClassCount> delivery_failed_{};
  std::array<std::uint64_t, kErrorClassCount> delivery_unknown_{};
  std::array<std::uint64_t, kErrorClassCount> fatal_errors_{};

  std::uint64_t partition_mismatch_ = 0;
  std::uint64_t backpressure_timeouts_ = 0;
  double backpressure_seconds_ = 0.0;

  std::size_t app_queue_records_ = 0;
  std::size_t app_queue_bytes_ = 0;
  double app_queue_oldest_age_ = 0.0;
  std::int64_t lib_queue_records_ = 0;
  std::int64_t lib_queue_bytes_ = 0;

  std::uint64_t rss_bytes_ = 0;
  std::uint64_t fd_count_ = 0;
  double cpu_seconds_ = 0.0;

  LatencyHistogram ack_latency_;
};

// Reads resident set size, open file descriptor count and CPU seconds for this
// process. Returns false on a platform where a value is unavailable, leaving
// the output untouched, so the report records "not measured" rather than zero
// (section 14: null is not 0).
bool ReadProcessStats(std::uint64_t* rss_bytes, std::uint64_t* fd_count,
                      double* cpu_seconds);

}  // namespace p04

#endif  // P04_OPS_METRICS_H_
