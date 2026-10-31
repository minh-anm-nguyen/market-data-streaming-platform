// The run report and evidence contract from section 14.
//
// The rule that shapes this file: null is not zero. Section 14 is explicit that
// an unmeasured value must stay null, because 0 is an observation and null is
// the absence of one. So every numeric field here is optional, and the writer
// emits null for anything the run did not actually measure. A report full of
// zeros would read as "we measured nothing wrong" when it means "we measured
// nothing".
//
// The second rule: three verdicts, not one. A run that refused to publish under
// an ISR failure is safe and not performant, and section 13 ADR-12 insists the
// two be separable. So there is a safety verdict, a performance verdict and an
// overall verdict, and the overall one is never better than the worst of them.
#ifndef P04_OPS_RUN_MANIFEST_H_
#define P04_OPS_RUN_MANIFEST_H_

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace p04 {

enum class Verdict { kNotRun, kPass, kFail, kIncomplete, kInconclusive, kNoGo };

const char* ToString(Verdict verdict);

// The worse of two verdicts, used to fold the safety and performance verdicts
// into the overall one. Ordering is by how much it should stop a release:
// FAIL and NO_GO outrank INCOMPLETE, which outranks INCONCLUSIVE, which
// outranks PASS.
Verdict Worse(Verdict a, Verdict b);

struct TopicMetadataReport {
  std::optional<std::int32_t> partitions;
  std::optional<std::int32_t> replication_factor;
  std::optional<std::int32_t> min_insync_replicas;
  std::string cleanup_policy;
  std::optional<bool> unclean_leader_election;
};

struct RunManifest {
  std::string project = "P04-Kafka-Market-Data-Streaming";
  std::string run_id;
  std::string mode;
  std::string profile;
  std::string started_at_utc;
  std::string finished_at_utc;

  Verdict verdict = Verdict::kNotRun;
  Verdict safety_verdict = Verdict::kNotRun;
  Verdict performance_verdict = Verdict::kNotRun;
  std::vector<std::string> verdict_reasons;

  std::vector<std::string> tested_case_ids;
  std::vector<std::string> fault_activation_evidence;

  // Provenance digests (NFR-14).
  std::string dataset_sha256;
  std::string dataset_sha256_recorded;
  std::string proto_descriptor_sha256;
  std::string proto_message_full_name;
  std::string config_sha256;
  std::string binary_sha256;
  std::string kafka_version;
  std::string librdkafka_version;
  std::string protobuf_version;
  std::string compiler;
  std::string hardware;
  std::string source_manifest_summary;
  bool source_usable_as_oracle = false;
  bool mock_upstream_p03 = true;

  std::map<std::string, TopicMetadataReport> topic_metadata;

  // Counters. Optional so an unmeasured one stays null in the JSON.
  std::optional<std::uint64_t> source_expected_market_valid;
  std::optional<std::uint64_t> upstream_accepted;
  std::optional<std::uint64_t> policy_suppressed;
  std::optional<std::uint64_t> route_rejected;
  std::optional<std::uint64_t> p04_enqueued_in_library;
  std::optional<std::uint64_t> p04_broker_acked;
  std::optional<std::uint64_t> p04_delivery_failed_definite;
  std::optional<std::uint64_t> p04_delivery_unknown;
  std::optional<std::uint64_t> p04_pending_at_shutdown;
  std::optional<std::uint64_t> p04_app_level_resends;
  std::optional<std::uint64_t> p04_partition_mismatch;
  std::optional<std::uint64_t> p04_double_finalize_refused;

  // Verifier results are filled in by the Python snapshot verifier, which runs
  // after this binary exits. They stay null here, and the harness merges the
  // two documents; this binary never guesses what the verifier will find.
  std::optional<std::uint64_t> verifier_observed_unique;
  std::optional<std::uint64_t> verifier_missing;
  std::optional<std::uint64_t> verifier_duplicates;
  std::optional<std::uint64_t> verifier_conflicts;

  std::optional<std::uint64_t> requested_events_per_second;
  std::optional<double> actual_sustained_acked_events_per_second;
  std::optional<double> p50_ack_ms;
  std::optional<double> p95_ack_ms;
  std::optional<double> p99_ack_ms;
  std::optional<double> p999_ack_ms;
  std::optional<double> max_ack_ms;
  std::optional<std::uint64_t> max_app_queue_records;
  std::optional<std::uint64_t> max_app_queue_bytes;
  std::optional<double> max_app_queue_oldest_age_seconds;
  std::optional<std::uint64_t> max_process_rss_bytes;
  std::optional<std::uint64_t> process_fd_count;
  std::optional<double> process_cpu_seconds;
  std::optional<double> wall_seconds;

  std::string conservation_check;  // "ok" or the failing equation
  std::string contract_error;
  std::optional<int> exit_code;
  std::string notes;

  // Serialises to the shape in section 14, with null for every unset optional.
  std::string ToJson() const;
  bool WriteJson(const std::string& path) const;

  // A short Markdown summary, which section 14 also requires. Kept to the facts
  // a reader needs to decide whether to trust the run.
  std::string ToMarkdown() const;
  bool WriteMarkdown(const std::string& path) const;
};

// UTC timestamp in RFC 3339, for the report timestamps.
std::string UtcNow();

// Creates the run directory, returning false if it exists and is not empty, so
// a run can never silently overwrite another run evidence.
bool PrepareRunDirectory(const std::string& path, std::string* error);

}  // namespace p04

#endif  // P04_OPS_RUN_MANIFEST_H_
