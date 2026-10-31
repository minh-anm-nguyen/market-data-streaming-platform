#include "ops/run_manifest.h"

#include <sys/stat.h>
#include <sys/types.h>

#include <cerrno>
#include <cstdio>
#include <ctime>
#include <dirent.h>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace p04 {
namespace {

std::string Escape(const std::string& in) {
  std::string out;
  out.reserve(in.size() + 8);
  for (const char c : in) {
    switch (c) {
      case '"':  out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:   out += c;
    }
  }
  return out;
}

std::string Quote(const std::string& in) { return "\"" + Escape(in) + "\""; }

// The null-not-zero rule in one helper. Every optional field goes through this.
template <typename T>
std::string OrNull(const std::optional<T>& value) {
  if (!value.has_value()) return "null";
  std::ostringstream out;
  out << std::setprecision(10) << *value;
  return out.str();
}

std::string OrNullBool(const std::optional<bool>& value) {
  if (!value.has_value()) return "null";
  return *value ? "true" : "false";
}

std::string JsonArray(const std::vector<std::string>& items) {
  std::ostringstream out;
  out << "[";
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) out << ", ";
    out << Quote(items[i]);
  }
  out << "]";
  return out.str();
}

int Rank(Verdict verdict) {
  // Higher is worse.
  switch (verdict) {
    case Verdict::kPass:         return 0;
    case Verdict::kNotRun:       return 1;
    case Verdict::kInconclusive: return 2;
    case Verdict::kIncomplete:   return 3;
    case Verdict::kNoGo:         return 4;
    case Verdict::kFail:         return 5;
  }
  return 5;
}

}  // namespace

const char* ToString(Verdict verdict) {
  switch (verdict) {
    case Verdict::kNotRun:       return "NOT_RUN";
    case Verdict::kPass:         return "PASS";
    case Verdict::kFail:         return "FAIL";
    case Verdict::kIncomplete:   return "INCOMPLETE";
    case Verdict::kInconclusive: return "INCONCLUSIVE";
    case Verdict::kNoGo:         return "NO_GO";
  }
  return "UNMAPPED";
}

Verdict Worse(Verdict a, Verdict b) { return Rank(a) >= Rank(b) ? a : b; }

std::string UtcNow() {
  const std::time_t now = std::time(nullptr);
  std::tm tm_utc{};
#if defined(_WIN32)
  gmtime_s(&tm_utc, &now);
#else
  gmtime_r(&now, &tm_utc);
#endif
  char buffer[32];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
  return buffer;
}

bool PrepareRunDirectory(const std::string& path, std::string* error) {
  // Create every missing component, so output_dir/run_id works without the
  // caller having to make the parent first.
  std::string partial;
  for (std::size_t i = 0; i <= path.size(); ++i) {
    if (i == path.size() || path[i] == '/') {
      if (!partial.empty()) {
        if (mkdir(partial.c_str(), 0755) != 0 && errno != EEXIST) {
          *error = "cannot create directory " + partial;
          return false;
        }
      }
    }
    if (i < path.size()) partial.push_back(path[i]);
  }

  // Refuse a directory that already holds evidence. Overwriting a previous run
  // report is how a failing run disappears, and section 14 treats the artifact
  // set as the proof, so it may not be clobbered by accident.
  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    *error = "cannot open run directory " + path;
    return false;
  }
  int entries = 0;
  while (const dirent* entry = readdir(dir)) {
    const std::string name = entry->d_name;
    if (name == "." || name == "..") continue;
    ++entries;
  }
  closedir(dir);
  if (entries > 0) {
    *error = "run directory " + path +
             " is not empty; pick a new run_id rather than overwriting evidence";
    return false;
  }
  if (error != nullptr) error->clear();
  return true;
}

std::string RunManifest::ToJson() const {
  std::ostringstream out;
  out << "{\n";
  out << "  \"project\": " << Quote(project) << ",\n";
  out << "  \"run_id\": " << Quote(run_id) << ",\n";
  out << "  \"mode\": " << Quote(mode) << ",\n";
  out << "  \"broker_profile\": " << Quote(profile) << ",\n";
  out << "  \"started_at_utc\": " << Quote(started_at_utc) << ",\n";
  out << "  \"finished_at_utc\": " << Quote(finished_at_utc) << ",\n";
  out << "  \"verdict\": " << Quote(ToString(verdict)) << ",\n";
  out << "  \"safety_verdict\": " << Quote(ToString(safety_verdict)) << ",\n";
  out << "  \"performance_verdict\": " << Quote(ToString(performance_verdict)) << ",\n";
  out << "  \"verdict_reasons\": " << JsonArray(verdict_reasons) << ",\n";
  out << "  \"tested_case_ids\": " << JsonArray(tested_case_ids) << ",\n";
  out << "  \"fault_activation_evidence\": " << JsonArray(fault_activation_evidence) << ",\n";
  out << "  \"mock_upstream_p03\": " << (mock_upstream_p03 ? "true" : "false") << ",\n";
  out << "  \"source_usable_as_oracle\": " << (source_usable_as_oracle ? "true" : "false") << ",\n";
  out << "  \"source_manifest_summary\": " << Quote(source_manifest_summary) << ",\n";
  out << "  \"dataset_sha256\": " << Quote(dataset_sha256) << ",\n";
  out << "  \"dataset_sha256_recorded\": " << Quote(dataset_sha256_recorded) << ",\n";
  out << "  \"proto_descriptor_sha256\": " << Quote(proto_descriptor_sha256) << ",\n";
  out << "  \"proto_message_full_name\": " << Quote(proto_message_full_name) << ",\n";
  out << "  \"config_sha256\": " << Quote(config_sha256) << ",\n";
  out << "  \"binary_sha256\": " << Quote(binary_sha256) << ",\n";
  out << "  \"kafka_version\": " << Quote(kafka_version) << ",\n";
  out << "  \"librdkafka_version\": " << Quote(librdkafka_version) << ",\n";
  out << "  \"protobuf_version\": " << Quote(protobuf_version) << ",\n";
  out << "  \"compiler\": " << Quote(compiler) << ",\n";
  out << "  \"hardware\": " << Quote(hardware) << ",\n";

  out << "  \"topic_metadata\": {";
  bool first_topic = true;
  for (const auto& [name, meta] : topic_metadata) {
    if (!first_topic) out << ",";
    first_topic = false;
    out << "\n    " << Quote(name) << ": {"
        << "\"partitions\": " << OrNull(meta.partitions)
        << ", \"replication_factor\": " << OrNull(meta.replication_factor)
        << ", \"min_isr\": " << OrNull(meta.min_insync_replicas)
        << ", \"cleanup_policy\": " << Quote(meta.cleanup_policy)
        << ", \"unclean_leader_election\": " << OrNullBool(meta.unclean_leader_election)
        << "}";
  }
  out << (first_topic ? "" : "\n  ") << "},\n";

  out << "  \"source_expected_market_valid\": " << OrNull(source_expected_market_valid) << ",\n";
  out << "  \"upstream_accepted\": " << OrNull(upstream_accepted) << ",\n";
  out << "  \"policy_suppressed\": " << OrNull(policy_suppressed) << ",\n";
  out << "  \"route_rejected\": " << OrNull(route_rejected) << ",\n";
  out << "  \"p04_enqueued_in_library\": " << OrNull(p04_enqueued_in_library) << ",\n";
  out << "  \"p04_broker_acked\": " << OrNull(p04_broker_acked) << ",\n";
  out << "  \"p04_delivery_failed_definite\": " << OrNull(p04_delivery_failed_definite) << ",\n";
  out << "  \"p04_delivery_unknown\": " << OrNull(p04_delivery_unknown) << ",\n";
  out << "  \"p04_pending_at_shutdown\": " << OrNull(p04_pending_at_shutdown) << ",\n";
  out << "  \"p04_app_level_resends\": " << OrNull(p04_app_level_resends) << ",\n";
  out << "  \"p04_partition_mismatch\": " << OrNull(p04_partition_mismatch) << ",\n";
  out << "  \"p04_double_finalize_refused\": " << OrNull(p04_double_finalize_refused) << ",\n";
  out << "  \"verifier_observed_unique\": " << OrNull(verifier_observed_unique) << ",\n";
  out << "  \"verifier_missing\": " << OrNull(verifier_missing) << ",\n";
  out << "  \"verifier_duplicates\": " << OrNull(verifier_duplicates) << ",\n";
  out << "  \"verifier_conflicts\": " << OrNull(verifier_conflicts) << ",\n";
  out << "  \"requested_events_per_second\": " << OrNull(requested_events_per_second) << ",\n";
  out << "  \"actual_sustained_acked_events_per_second\": "
      << OrNull(actual_sustained_acked_events_per_second) << ",\n";
  out << "  \"p50_ack_ms\": " << OrNull(p50_ack_ms) << ",\n";
  out << "  \"p95_ack_ms\": " << OrNull(p95_ack_ms) << ",\n";
  out << "  \"p99_ack_ms\": " << OrNull(p99_ack_ms) << ",\n";
  out << "  \"p999_ack_ms\": " << OrNull(p999_ack_ms) << ",\n";
  out << "  \"max_ack_ms\": " << OrNull(max_ack_ms) << ",\n";
  out << "  \"max_app_queue_records\": " << OrNull(max_app_queue_records) << ",\n";
  out << "  \"max_app_queue_bytes\": " << OrNull(max_app_queue_bytes) << ",\n";
  out << "  \"max_app_queue_oldest_age_seconds\": "
      << OrNull(max_app_queue_oldest_age_seconds) << ",\n";
  out << "  \"max_process_rss_bytes\": " << OrNull(max_process_rss_bytes) << ",\n";
  out << "  \"process_fd_count\": " << OrNull(process_fd_count) << ",\n";
  out << "  \"process_cpu_seconds\": " << OrNull(process_cpu_seconds) << ",\n";
  out << "  \"wall_seconds\": " << OrNull(wall_seconds) << ",\n";
  out << "  \"conservation_check\": " << Quote(conservation_check) << ",\n";
  out << "  \"contract_error\": " << Quote(contract_error) << ",\n";
  out << "  \"exit_code\": " << OrNull(exit_code) << ",\n";
  out << "  \"notes\": " << Quote(notes) << "\n";
  out << "}\n";
  return out.str();
}

bool RunManifest::WriteJson(const std::string& path) const {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return false;
  out << ToJson();
  out.flush();
  return static_cast<bool>(out);
}

std::string RunManifest::ToMarkdown() const {
  std::ostringstream out;
  out << "# Project 04 run report: " << run_id << "\n\n";
  out << "| field | value |\n|---|---|\n";
  out << "| verdict | **" << ToString(verdict) << "** |\n";
  out << "| safety verdict | " << ToString(safety_verdict) << " |\n";
  out << "| performance verdict | " << ToString(performance_verdict) << " |\n";
  out << "| mode | " << mode << " |\n";
  out << "| profile | " << profile << " |\n";
  out << "| upstream | " << (mock_upstream_p03 ? "MOCK P03 classifier (P03 not implemented)"
                                               : "real P03 adapter") << " |\n";
  out << "| source usable as completeness oracle | "
      << (source_usable_as_oracle ? "yes" : "no") << " |\n";
  out << "| kafka | " << (kafka_version.empty() ? "not recorded" : kafka_version) << " |\n";
  out << "| librdkafka | " << librdkafka_version << " |\n";
  out << "| protobuf | " << protobuf_version << " |\n";
  out << "| descriptor sha256 | `" << proto_descriptor_sha256 << "` |\n";
  out << "| config sha256 | `" << config_sha256 << "` |\n";
  out << "| dataset sha256 | `" << dataset_sha256 << "` |\n";
  out << "| conservation | " << conservation_check << " |\n";
  if (!contract_error.empty()) out << "| contract error | " << contract_error << " |\n";
  out << "\n## Attempt accounting\n\n";
  out << "| counter | value |\n|---|---|\n";
  out << "| upstream_accepted | " << OrNull(upstream_accepted) << " |\n";
  out << "| policy_suppressed | " << OrNull(policy_suppressed) << " |\n";
  out << "| route_rejected | " << OrNull(route_rejected) << " |\n";
  out << "| enqueued_in_library | " << OrNull(p04_enqueued_in_library) << " |\n";
  out << "| broker_acked | " << OrNull(p04_broker_acked) << " |\n";
  out << "| delivery_failed_definite | " << OrNull(p04_delivery_failed_definite) << " |\n";
  out << "| delivery_unknown | " << OrNull(p04_delivery_unknown) << " |\n";
  out << "| pending_at_shutdown | " << OrNull(p04_pending_at_shutdown) << " |\n";
  out << "| app_level_resends | " << OrNull(p04_app_level_resends) << " |\n";
  out << "\n";
  if (!verdict_reasons.empty()) {
    out << "## Why this verdict\n\n";
    for (const auto& reason : verdict_reasons) out << "- " << reason << "\n";
    out << "\n";
  }
  out << "## Not proven by this run\n\n";
  out << "- A broker acknowledgement is not consumer visibility and not a "
         "ClickHouse commit (section 4.1).\n";
  out << "- Records counted as UNKNOWN may or may not be on the broker; only the "
         "independent snapshot verifier can tell (section 4.2).\n";
  out << "- Verifier columns are null in this document: the Python snapshot "
         "verifier writes its own report and the harness merges them.\n";
  if (mock_upstream_p03) {
    out << "- The upstream was the mock P03 classifier, so nothing here is "
           "evidence about the real adapter.\n";
  }
  return out.str();
}

bool RunManifest::WriteMarkdown(const std::string& path) const {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return false;
  out << ToMarkdown();
  out.flush();
  return static_cast<bool>(out);
}

}  // namespace p04
