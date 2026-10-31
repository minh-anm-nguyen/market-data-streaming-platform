#include "md_adapter/report.hpp"

#include <sys/utsname.h>

#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace md {
namespace {

void kv(std::ostringstream& o, const char* indent, const char* key,
        const std::string& v) {
  o << indent << key << ": \"" << v << "\"\n";
}
void kv_raw(std::ostringstream& o, const char* indent, const char* key,
            const std::string& v) {
  o << indent << key << ": " << v << "\n";
}
void kv_u(std::ostringstream& o, const char* indent, const char* key,
          std::uint64_t v) {
  o << indent << key << ": " << v << "\n";
}
// A value that was not measured is null. Reporting 0 for "not measured" is
// how a report ends up claiming zero loss it never observed.
void kv_opt_i(std::ostringstream& o, const char* indent, const char* key,
              std::int64_t v, bool known) {
  o << indent << key << ": ";
  if (known) {
    o << v;
  } else {
    o << "null";
  }
  o << "\n";
}

std::string reason_breakdown(
    const std::array<std::uint64_t,
                     static_cast<std::size_t>(Reason::kReasonCount)>& a) {
  std::ostringstream o;
  bool any = false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    if (a[i] == 0) continue;
    if (any) o << ", ";
    o << to_string(static_cast<Reason>(i)) << ": " << a[i];
    any = true;
  }
  return any ? ("{" + o.str() + "}") : "{}";
}

std::string kernel_release() {
  struct ::utsname u {};
  if (::uname(&u) != 0) return "UNKNOWN";
  return std::string(u.sysname) + " " + u.release;
}

std::string cpu_model() {
  std::ifstream f("/proc/cpuinfo");
  std::string line;
  while (std::getline(f, line)) {
    if (line.rfind("model name", 0) != 0) continue;
    const std::size_t c = line.find(':');
    if (c == std::string::npos) continue;
    std::string v = line.substr(c + 1);
    while (!v.empty() && v.front() == ' ') v.erase(v.begin());
    return v;
  }
  return "UNKNOWN";
}

std::string compiler_id() {
#if defined(__clang__)
  return std::string("clang++ ") + __clang_version__;
#elif defined(__GNUC__)
  return "g++ " + std::to_string(__GNUC__) + "." +
         std::to_string(__GNUC_MINOR__) + "." +
         std::to_string(__GNUC_PATCHLEVEL__);
#else
  return "UNKNOWN";
#endif
}

}  // namespace

std::string utc_now_iso8601() {
  const auto now = std::chrono::system_clock::now();
  const std::time_t t = std::chrono::system_clock::to_time_t(now);
  struct ::tm tmv {};
  ::gmtime_r(&t, &tmv);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tmv);
  return buf;
}

std::string make_run_id() {
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
  char buf[48];
  std::snprintf(buf, sizeof(buf), "p03-%lld", static_cast<long long>(ns));
  return buf;
}

std::string render_report_yaml(const ReportInputs& in) {
  std::ostringstream o;
  const Config& c = *in.cfg;
  const Metrics& m = *in.metrics;
  const RunOutcome& r = *in.outcome;

  o << "project: \"P03-C++-FXCFD-Market-Data-Adapter\"\n";
  o << "document_version: \"1.0\"\n";
  kv(o, "", "run_id", in.run_id);
  kv(o, "", "timestamp_utc", in.timestamp_utc);
  kv_raw(o, "", "status", std::string(to_string(r.status)));
  kv(o, "", "mode", std::string(to_string(c.input.mode)));
  kv(o, "", "validation_mode", std::string(to_string(c.validation.mode)));
  kv(o, "", "test_lane",
     c.input.mode == InputMode::kPbstream ? "offline-full" : "live-full");
  kv(o, "", "terminal_reason", std::string(to_string(r.terminal_reason)));
  kv(o, "", "summary", r.summary);

  o << "input:\n";
  kv(o, "  ", "path",
     c.input.mode == InputMode::kPbstream
         ? c.input.file
         : (c.input.host + ":" + std::to_string(c.input.port)));
  kv(o, "  ", "p02_pbstream_sha256",
     in.input_sha256.empty() ? "UNKNOWN" : in.input_sha256);
  kv(o, "  ", "p02_descriptor_sha256_runtime", in.descriptor_sha256_runtime);
  kv(o, "  ", "p02_descriptor_sha256_expected",
     in.descriptor_sha256_expected.empty() ? "UNPINNED"
                                           : in.descriptor_sha256_expected);
  kv_raw(o, "  ", "descriptor_pinned", in.descriptor_pinned ? "true" : "false");
  kv_raw(o, "  ", "descriptor_matches",
         in.descriptor_matches ? "true" : "false");
  kv(o, "  ", "source_completeness", std::string(to_string(r.provenance)));
  kv(o, "  ", "manifest_status",
     in.input_manifest_status.empty() ? "ABSENT" : in.input_manifest_status);
  kv_opt_i(o, "  ", "expected_frames", in.input_manifest_frames,
           in.input_manifest_frames >= 0);
  kv_u(o, "  ", "observed_frames_complete", r.frames_from_source);
  o << "  provenance_notes:\n";
  for (const auto& n : r.notes) o << "    - \"" << n << "\"\n";

  o << "runtime:\n";
  kv(o, "  ", "adapter_version", MD_ADAPTER_VERSION);
  kv(o, "  ", "compiler", compiler_id());
  kv(o, "  ", "build_descriptor_sha256", MD_PROTO_DESCRIPTOR_SHA256);
  kv(o, "  ", "linux_kernel", kernel_release());
  kv(o, "  ", "cpu_model", cpu_model());
  kv(o, "  ", "clock_source",
     c.freshness.clock_mode == ClockMode::kVirtualForReplay
         ? "VIRTUAL (injected; offline replay)"
         : "CLOCK_MONOTONIC");
  kv(o, "  ", "command_line", in.command_line);

  o << "config:\n";
  kv(o, "  ", "path", c.source_path);
  kv(o, "  ", "sha256", c.source_sha256);
  kv_u(o, "  ", "max_payload_bytes",
       static_cast<std::uint64_t>(c.input.max_payload_bytes));
  kv_u(o, "  ", "sequence_window",
       static_cast<std::uint64_t>(c.validation.sequence_window));
  kv_u(o, "  ", "max_state_bytes",
       static_cast<std::uint64_t>(c.identity.max_state_bytes));
  kv_u(o, "  ", "output_queue_bytes_cap",
       static_cast<std::uint64_t>(c.output.max_queue_bytes));
  kv_u(o, "  ", "output_queue_events_cap",
       static_cast<std::uint64_t>(c.output.max_queue_events));
  kv_opt_i(o, "  ", "sink_stub_delay_us", c.output.stub_delay_us, true);

  o << "reconciliation:\n";
  kv_u(o, "  ", "transport_bytes_read", m.transport_bytes_read);
  kv_u(o, "  ", "frames_complete", m.frames_complete);
  kv_u(o, "  ", "proto_decode_error", m.frames_decode_error);
  kv_u(o, "  ", "frames_decoded", m.frames_decoded);
  kv_u(o, "  ", "structural_reject", m.structural_reject);
  kv_u(o, "  ", "structural_ok", m.structural_ok);
  kv_u(o, "  ", "business_reject", m.business_reject);
  kv_u(o, "  ", "business_ok", m.business_ok);
  kv_u(o, "  ", "policy_suppressed", m.policy_suppressed);
  kv_u(o, "  ", "classified_output_attempted", m.classified_output_attempted);
  kv_u(o, "  ", "output_success", m.output_success);
  kv_u(o, "  ", "output_failed_or_pending", m.output_failed_or_pending);
  // The adapter cannot know what the source intended to send without a
  // source-side manifest. null means unknown, not zero.
  kv_opt_i(o, "  ", "source_manifest_missing_or_unknown_events",
           in.input_manifest_frames >= 0
               ? in.input_manifest_frames -
                     static_cast<std::int64_t>(m.frames_complete)
               : 0,
           in.input_manifest_frames >= 0);
  kv_raw(o, "  ", "structural_reject_by_reason",
         reason_breakdown(m.structural_reject_by_reason));
  kv_raw(o, "  ", "business_reject_by_reason",
         reason_breakdown(m.business_reject_by_reason));
  kv_raw(o, "  ", "policy_suppressed_by_reason",
         reason_breakdown(m.policy_suppressed_by_reason));

  o << "anomalies:\n";
  kv_u(o, "  ", "gap_incidents", m.gap_detected);
  kv_u(o, "  ", "missing_slots_observed", m.sequence_missing_slots_observed);
  kv_u(o, "  ", "duplicate_exact", m.duplicate_exact);
  kv_u(o, "  ", "duplicate_conflict", m.duplicate_conflict);
  kv_u(o, "  ", "reorder_late", m.reorder_late);
  kv_u(o, "  ", "too_old_unverifiable", m.too_old_unverifiable);
  kv_u(o, "  ", "sequence_rollback", m.sequence_rollback);
  kv_u(o, "  ", "event_time_old", m.stale_event_time);
  kv_u(o, "  ", "quote_inactive_transitions", m.quote_inactive_transitions);
  kv_u(o, "  ", "recovered_transitions", m.recovered_transitions);
  kv_u(o, "  ", "start_unverified", m.start_unverified);
  kv_u(o, "  ", "state_cap_exceeded", m.state_cap_exceeded);
  kv_u(o, "  ", "unknown_fields_seen", m.unknown_fields_seen);
  kv_u(o, "  ", "raw_sequence_observed_invalid",
       m.raw_sequence_observed_invalid);
  kv_u(o, "  ", "unexpected_faults", m.unexpected_faults);

  o << "resources:\n";
  kv_u(o, "  ", "peak_rss_bytes", peak_rss_bytes());
  kv_u(o, "  ", "current_rss_bytes", current_rss_bytes());
  kv_u(o, "  ", "peak_queue_bytes", m.peak_queue_bytes);
  kv_u(o, "  ", "peak_queue_events", m.peak_queue_events);
  kv_u(o, "  ", "max_fd_count", open_fd_count());
  kv_u(o, "  ", "backpressure_pause_ns", m.read_paused_ns);
  kv_u(o, "  ", "backpressure_pause_events", m.read_pause_events);
  if (in.tracker != nullptr) {
    kv_u(o, "  ", "sequence_state_bytes", in.tracker->state_bytes());
    kv_u(o, "  ", "sequence_keys", in.tracker->key_count());
    kv_u(o, "  ", "sequence_state_cap_rejections",
         in.tracker->state_cap_rejections());
  }

  o << "performance:\n";
  const double secs = static_cast<double>(r.wall_ns) / 1e9;
  kv_opt_i(o, "  ", "wall_ns", r.wall_ns, r.wall_ns > 0);
  kv_opt_i(o, "  ", "input_events_per_second",
           secs > 0.0 ? static_cast<std::int64_t>(
                            static_cast<double>(m.business_ok) / secs)
                      : 0,
           secs > 0.0);
  kv_opt_i(o, "  ", "output_events_per_second",
           secs > 0.0 ? static_cast<std::int64_t>(
                            static_cast<double>(m.output_success) / secs)
                      : 0,
           secs > 0.0);
  const LatencyHistogram& h = m.processing_latency;
  const bool has_lat = h.count() > 0;
  kv_opt_i(o, "  ", "processing_latency_samples",
           static_cast<std::int64_t>(h.count()), true);
  kv_opt_i(o, "  ", "processing_latency_p50_ns", h.quantile(0.50), has_lat);
  kv_opt_i(o, "  ", "processing_latency_p95_ns", h.quantile(0.95), has_lat);
  kv_opt_i(o, "  ", "processing_latency_p99_ns", h.quantile(0.99), has_lat);
  kv_opt_i(o, "  ", "processing_latency_p999_ns", h.quantile(0.999), has_lat);
  kv_opt_i(o, "  ", "processing_latency_max_ns", h.max(), has_lat);
  // This binary measures one run. Declaring a sustainable rate needs a
  // benchmark lane with a steady-state criterion, so it is never asserted
  // here.
  kv_raw(o, "  ", "sustainable_throughput_verified", "false");

  o << "feed_state:\n";
  if (in.freshness != nullptr) {
    for (const auto& k : in.freshness->snapshot()) {
      o << "  - key: \"" << k.key << "\"\n";
      o << "    state: " << to_string(k.state) << "\n";
      kv_opt_i(o, "    ", "event_time_watermark_ns", k.watermark_ns,
               k.has_watermark);
      kv_u(o, "    ", "inactive_transitions", k.inactive_transitions);
    }
  }

  o << "sequence_keys:\n";
  if (in.tracker != nullptr) {
    for (const auto& entry : in.tracker->all_stats()) {
      const auto& s = entry.second;
      o << "  - key: \"" << entry.first << "\"\n";
      kv_u(o, "    ", "events_observed", s.events_observed);
      kv_opt_i(o, "    ", "first_seen", s.first_seen, s.first_seen != 0);
      kv_opt_i(o, "    ", "highest_seen_valid", s.highest_seen_valid, true);
      kv_opt_i(o, "    ", "contiguous_observed_valid",
               s.contiguous_observed_valid, true);
      kv_opt_i(o, "    ", "contiguous_published", s.contiguous_published, true);
      kv_u(o, "    ", "gap_incidents", s.gap_incidents);
      kv_u(o, "    ", "missing_slots_observed", s.missing_slots_observed);
      kv_u(o, "    ", "duplicate_exact", s.duplicate_exact);
      kv_u(o, "    ", "duplicate_conflict", s.duplicate_conflict);
      kv_u(o, "    ", "reorder_late", s.reorder_late);
      kv_u(o, "    ", "too_old_unverifiable", s.too_old);
      kv_raw(o, "    ", "continuity_uncertain",
             s.continuity_uncertain ? "true" : "false");
    }
  }

  o << "sink:\n";
  kv(o, "  ", "kind", std::string(to_string(c.output.sink)));
  kv(o, "  ", "artifact", in.sink_artifact);
  kv(o, "  ", "artifact_sha256",
     in.sink_artifact_sha256.empty() ? "NONE" : in.sink_artifact_sha256);
  kv_u(o, "  ", "events_written", in.sink_events);

  const Metrics::Conservation cons = m.check_conservation();
  o << "checks:\n";
  kv_raw(o, "  ", "all_conservation_equations_hold", cons.ok ? "true" : "false");
  o << "  conservation_violations:\n";
  for (const auto& v : cons.violations) o << "    - \"" << v << "\"\n";
  // Only a fully verified source permits a no-silent-loss claim; otherwise it
  // is unknown, not true.
  kv_raw(o, "  ", "no_silent_loss_when_full_source_verified",
         r.provenance == Provenance::kVerifiedCompleteFile
             ? (cons.ok ? "true" : "false")
             : "null");
  kv_raw(o, "  ", "bounded_memory_under_stress", "null");
  kv_raw(o, "  ", "sanitizer_clean", "null");
  kv_raw(o, "  ", "fuzz_budgets_completed", "null");
  kv_raw(o, "  ", "source_reference_verified",
         r.provenance == Provenance::kVerifiedCompleteFile ? "true" : "false");

  o << "known_limitations:\n";
  o << "  - \"No app-level ACK or replay cursor in this MVP\"\n";
  o << "  - \"Live stream completeness is not provable without external "
       "source-side evidence\"\n";
  o << "  - \"A SEQUENCE_ROLLBACK inside one dataset_id leaves continuity "
       "lost until a new verified dataset epoch is started\"\n";
  kv_raw(o, "", "release_verdict", "NOT_EVALUATED");
  return o.str();
}

bool write_report(const std::string& path, const std::string& yaml) {
  const std::filesystem::path p(path);
  if (p.has_parent_path()) {
    std::error_code ec;
    std::filesystem::create_directories(p.parent_path(), ec);
    if (ec) return false;
  }
  std::ofstream f(path, std::ios::binary | std::ios::trunc);
  if (!f) return false;
  f << yaml;
  f.flush();
  return static_cast<bool>(f);
}

}  // namespace md
