// Adapter configuration: the TOML contract from design section 14, plus the
// validator that refuses to start on a configuration that cannot be honoured.
//
// The validator is deliberately pedantic. Every rule here exists because the
// what-if matrix has a case for it (wrong port, non-loopback host, watermark
// inversion, fault_observe without a manifest, caps that cannot hold one
// frame, ...). Starting with a bad config and failing later is a worse
// outcome than refusing to start.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace md {

enum class InputMode { kPbstream, kTcp };
enum class ValidationMode { kStrictMonitor, kFaultObserve, kOfflineVerify };
enum class SinkKind { kMetricsOnly, kNdjsonTest, kP04Stub };
enum class ClockMode { kVirtualForReplay, kMonotonicLive };

std::string_view to_string(InputMode m) noexcept;
std::string_view to_string(ValidationMode m) noexcept;
std::string_view to_string(SinkKind s) noexcept;
std::string_view to_string(ClockMode c) noexcept;

struct IdentityConfig {
  std::string protocol = "MDP1";
  std::string proto_package = "marketdata.v1";
  std::string source_schema_version = "sim.v1";
  // Digest of the protobuf FileDescriptorSet this build was generated from.
  // "UNPINNED" means the operator accepted an unverified contract.
  std::string expected_descriptor_sha256 = "UNPINNED";
  std::vector<std::string> allowed_symbols;
  std::int64_t max_symbols = 256;
  std::int64_t max_datasets = 4;
  std::int64_t max_state_bytes = 67108864;
};

struct InputConfig {
  InputMode mode = InputMode::kPbstream;
  std::string file;
  bool require_completed_manifest = true;
  std::string host = "127.0.0.1";
  std::int64_t port = 9010;
  std::int64_t max_payload_bytes = 4096;
  std::int64_t read_chunk_bytes = 65536;
  std::int64_t max_frames_per_iteration = 256;
  std::int64_t connect_deadline_ms = 3000;
  std::int64_t prefix_deadline_ms = 2000;
  std::int64_t header_partial_deadline_ms = 2000;
  std::int64_t body_deadline_ms = 2000;
  std::int64_t idle_timeout_ms = 0;  // 0 = idle transport wait is allowed
  // Escape hatch for deliberately testing a non-loopback endpoint. Off by
  // default; the security gate (M-group cases) depends on it.
  bool allow_non_loopback = false;
};

struct ValidationConfig {
  ValidationMode mode = ValidationMode::kStrictMonitor;
  std::string fault_manifest;
  std::int64_t max_dataset_id_bytes = 128;
  std::int64_t max_symbol_bytes = 32;
  std::int64_t max_source_version_bytes = 32;
  std::int64_t sequence_window = 4096;
  std::string on_business_reject = "skip_with_reason";
  std::string on_malformed_frame = "fail_closed";
  // proto3 keeps unknown fields; the adapter records them and may reject.
  bool reject_unknown_fields = false;
};

struct PriceRules {
  std::int64_t price_scale = 0;
  std::int64_t min_price_ticks = 1;
  std::int64_t max_price_ticks = 0;
  std::int64_t min_spread_ticks = 0;
  std::int64_t max_spread_ticks = 0;
};

struct FreshnessConfig {
  std::int64_t event_time_stale_threshold_ns = 2'000'000'000;
  std::int64_t quote_inactivity_threshold_ms = 3000;
  std::int64_t timer_tick_ms = 100;
  ClockMode clock_mode = ClockMode::kVirtualForReplay;
};

struct OutputConfig {
  SinkKind sink = SinkKind::kNdjsonTest;
  std::int64_t max_queue_events = 4096;
  std::int64_t max_queue_bytes = 8388608;
  std::int64_t pause_read_high_watermark_pct = 80;
  std::int64_t resume_read_low_watermark_pct = 40;
  std::int64_t output_stall_deadline_ms = 5000;
  std::int64_t shutdown_drain_deadline_ms = 5000;
  std::string file = "out/adapter_events.ndjson";
  // Test-only: an artificial per-event delay in the P04 stub so backpressure
  // can be exercised deterministically. Always echoed into the run report, so
  // a throttled run can never be mistaken for an unthrottled one.
  std::int64_t stub_delay_us = 0;
};

struct ReconnectConfig {
  std::int64_t initial_backoff_ms = 100;
  std::int64_t max_backoff_ms = 10000;
  std::int64_t max_attempts = 50;
  bool continue_dataset_within_process = true;
  // Deterministic jitter: a fixed seed keeps reconnect timing reproducible in
  // tests while still spreading retries.
  std::int64_t jitter_seed = 0;
  std::int64_t jitter_pct = 20;
};

struct BenchmarkConfig {
  std::vector<std::int64_t> target_events_per_sec{1000, 10000, 50000, 120000};
  std::int64_t report_interval_ms = 1000;
  bool sample_processing_latency = true;
};

struct Config {
  IdentityConfig identity;
  InputConfig input;
  ValidationConfig validation;
  std::map<std::string, PriceRules> prices;
  FreshnessConfig freshness;
  OutputConfig output;
  ReconnectConfig reconnect;
  BenchmarkConfig benchmark;

  // SHA-256 of the exact bytes that produced this config, for the report.
  std::string source_sha256;
  std::string source_path;

  // Parses and validates. Returns false and fills `errors` on any problem;
  // a partially-filled Config is never returned as usable.
  static bool load_from_string(std::string_view text, std::string_view origin,
                               Config& out, std::vector<std::string>& errors);
  static bool load_from_file(const std::string& path, Config& out,
                             std::vector<std::string>& errors);

  // Semantic checks that do not depend on the TOML layer.
  void validate(std::vector<std::string>& errors) const;

  const PriceRules* rules_for(const std::string& symbol) const;
  bool symbol_allowed(const std::string& symbol) const;
};

}  // namespace md
