#include "md_adapter/config.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>

#include "md_adapter/sha256.hpp"
#include "md_adapter/toml_lite.hpp"

namespace md {
namespace {

template <class T>
void assign_if(std::optional<T> v, T& dst) {
  if (v.has_value()) dst = *v;
}

bool enum_from(const std::optional<std::string>& v, std::string_view key,
               std::initializer_list<std::pair<const char*, int>> table,
               int& dst, std::vector<std::string>& errors) {
  if (!v.has_value()) return true;
  for (const auto& e : table) {
    if (*v == e.first) {
      dst = e.second;
      return true;
    }
  }
  std::string allowed;
  for (const auto& e : table) {
    if (!allowed.empty()) allowed += "|";
    allowed += e.first;
  }
  errors.push_back(std::string(key) + ": unknown value '" + *v +
                   "' (expected one of " + allowed + ")");
  return false;
}

bool is_loopback_host(const std::string& h) {
  return h == "127.0.0.1" || h == "::1" || h == "localhost" || h == "[::1]";
}

// A path is rejected when it could escape the run directory or hide behind a
// shell metacharacter. This is a guard against config mistakes, not a
// substitute for filesystem permissions.
bool unsafe_path(const std::string& p) {
  if (p.empty()) return true;
  if (p.find("..") != std::string::npos) return true;
  return p.find_first_of("\n\r\t*?$`;|&<>") != std::string::npos;
}

}  // namespace

std::string_view to_string(InputMode m) noexcept {
  return m == InputMode::kPbstream ? "pbstream" : "tcp";
}
std::string_view to_string(ValidationMode m) noexcept {
  switch (m) {
    case ValidationMode::kStrictMonitor: return "strict_monitor";
    case ValidationMode::kFaultObserve: return "fault_observe";
    case ValidationMode::kOfflineVerify: return "offline_verify";
  }
  return "invalid";
}
std::string_view to_string(SinkKind s) noexcept {
  switch (s) {
    case SinkKind::kMetricsOnly: return "metrics_only";
    case SinkKind::kNdjsonTest: return "ndjson_test";
    case SinkKind::kP04Stub: return "p04_stub";
  }
  return "invalid";
}
std::string_view to_string(ClockMode c) noexcept {
  return c == ClockMode::kVirtualForReplay ? "virtual_for_replay"
                                           : "monotonic_live";
}

const PriceRules* Config::rules_for(const std::string& symbol) const {
  const auto it = prices.find(symbol);
  return it == prices.end() ? nullptr : &it->second;
}

bool Config::symbol_allowed(const std::string& symbol) const {
  const auto& a = identity.allowed_symbols;
  return std::find(a.begin(), a.end(), symbol) != a.end();
}

bool Config::load_from_file(const std::string& path, Config& out,
                            std::vector<std::string>& errors) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    errors.push_back("cannot open config file: " + path);
    return false;
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  return load_from_string(ss.str(), path, out, errors);
}

bool Config::load_from_string(std::string_view text, std::string_view origin,
                              Config& out, std::vector<std::string>& errors) {
  const std::size_t before = errors.size();
  Config c;
  c.source_path = std::string(origin);
  c.source_sha256 = sha256_hex(text);

  TomlLite t;
  t.parse(text, errors);

  // [identity]
  assign_if(t.get_string("identity.protocol", errors), c.identity.protocol);
  assign_if(t.get_string("identity.proto_package", errors),
            c.identity.proto_package);
  assign_if(t.get_string("identity.source_schema_version", errors),
            c.identity.source_schema_version);
  assign_if(t.get_string("identity.expected_descriptor_sha256", errors),
            c.identity.expected_descriptor_sha256);
  assign_if(t.get_string_array("identity.allowed_symbols", errors),
            c.identity.allowed_symbols);
  assign_if(t.get_int("identity.max_symbols", errors), c.identity.max_symbols);
  assign_if(t.get_int("identity.max_datasets", errors), c.identity.max_datasets);
  assign_if(t.get_int("identity.max_state_bytes", errors),
            c.identity.max_state_bytes);

  // [input]
  {
    int mode = static_cast<int>(c.input.mode);
    enum_from(t.get_string("input.mode", errors), "input.mode",
              {{"pbstream", 0}, {"tcp", 1}}, mode, errors);
    c.input.mode = static_cast<InputMode>(mode);
  }
  assign_if(t.get_string("input.file", errors), c.input.file);
  assign_if(t.get_bool("input.require_completed_manifest", errors),
            c.input.require_completed_manifest);
  assign_if(t.get_string("input.host", errors), c.input.host);
  assign_if(t.get_int("input.port", errors), c.input.port);
  assign_if(t.get_int("input.max_payload_bytes", errors),
            c.input.max_payload_bytes);
  assign_if(t.get_int("input.read_chunk_bytes", errors), c.input.read_chunk_bytes);
  assign_if(t.get_int("input.max_frames_per_iteration", errors),
            c.input.max_frames_per_iteration);
  assign_if(t.get_int("input.connect_deadline_ms", errors),
            c.input.connect_deadline_ms);
  assign_if(t.get_int("input.prefix_deadline_ms", errors),
            c.input.prefix_deadline_ms);
  assign_if(t.get_int("input.header_partial_deadline_ms", errors),
            c.input.header_partial_deadline_ms);
  assign_if(t.get_int("input.body_deadline_ms", errors), c.input.body_deadline_ms);
  assign_if(t.get_int("input.idle_timeout_ms", errors), c.input.idle_timeout_ms);
  assign_if(t.get_bool("input.allow_non_loopback", errors),
            c.input.allow_non_loopback);

  // [validation]
  {
    int mode = static_cast<int>(c.validation.mode);
    enum_from(t.get_string("validation.mode", errors), "validation.mode",
              {{"strict_monitor", 0}, {"fault_observe", 1}, {"offline_verify", 2}},
              mode, errors);
    c.validation.mode = static_cast<ValidationMode>(mode);
  }
  assign_if(t.get_string("validation.fault_manifest", errors),
            c.validation.fault_manifest);
  assign_if(t.get_int("validation.max_dataset_id_bytes", errors),
            c.validation.max_dataset_id_bytes);
  assign_if(t.get_int("validation.max_symbol_bytes", errors),
            c.validation.max_symbol_bytes);
  assign_if(t.get_int("validation.max_source_version_bytes", errors),
            c.validation.max_source_version_bytes);
  assign_if(t.get_int("validation.sequence_window", errors),
            c.validation.sequence_window);
  assign_if(t.get_string("validation.on_business_reject", errors),
            c.validation.on_business_reject);
  assign_if(t.get_string("validation.on_malformed_frame", errors),
            c.validation.on_malformed_frame);
  assign_if(t.get_bool("validation.reject_unknown_fields", errors),
            c.validation.reject_unknown_fields);

  // [prices.<SYMBOL>]
  for (const auto& sym : t.subtables_of("prices")) {
    PriceRules r;
    const std::string base = "prices." + sym + ".";
    assign_if(t.get_int(base + "price_scale", errors), r.price_scale);
    assign_if(t.get_int(base + "min_price_ticks", errors), r.min_price_ticks);
    assign_if(t.get_int(base + "max_price_ticks", errors), r.max_price_ticks);
    assign_if(t.get_int(base + "min_spread_ticks", errors), r.min_spread_ticks);
    assign_if(t.get_int(base + "max_spread_ticks", errors), r.max_spread_ticks);
    c.prices.emplace(sym, r);
  }

  // [freshness]
  assign_if(t.get_int("freshness.event_time_stale_threshold_ns", errors),
            c.freshness.event_time_stale_threshold_ns);
  assign_if(t.get_int("freshness.quote_inactivity_threshold_ms", errors),
            c.freshness.quote_inactivity_threshold_ms);
  assign_if(t.get_int("freshness.timer_tick_ms", errors),
            c.freshness.timer_tick_ms);
  {
    int mode = static_cast<int>(c.freshness.clock_mode);
    enum_from(t.get_string("freshness.clock_mode", errors), "freshness.clock_mode",
              {{"virtual_for_replay", 0}, {"monotonic_live", 1}}, mode, errors);
    c.freshness.clock_mode = static_cast<ClockMode>(mode);
  }

  // [output]
  {
    int sink = static_cast<int>(c.output.sink);
    enum_from(t.get_string("output.sink", errors), "output.sink",
              {{"metrics_only", 0}, {"ndjson_test", 1}, {"p04_stub", 2}}, sink,
              errors);
    c.output.sink = static_cast<SinkKind>(sink);
  }
  assign_if(t.get_int("output.max_queue_events", errors),
            c.output.max_queue_events);
  assign_if(t.get_int("output.max_queue_bytes", errors), c.output.max_queue_bytes);
  assign_if(t.get_int("output.pause_read_high_watermark_pct", errors),
            c.output.pause_read_high_watermark_pct);
  assign_if(t.get_int("output.resume_read_low_watermark_pct", errors),
            c.output.resume_read_low_watermark_pct);
  assign_if(t.get_int("output.output_stall_deadline_ms", errors),
            c.output.output_stall_deadline_ms);
  assign_if(t.get_int("output.shutdown_drain_deadline_ms", errors),
            c.output.shutdown_drain_deadline_ms);
  assign_if(t.get_string("output.file", errors), c.output.file);
  assign_if(t.get_int("output.stub_delay_us", errors), c.output.stub_delay_us);

  // [reconnect]
  assign_if(t.get_int("reconnect.initial_backoff_ms", errors),
            c.reconnect.initial_backoff_ms);
  assign_if(t.get_int("reconnect.max_backoff_ms", errors),
            c.reconnect.max_backoff_ms);
  assign_if(t.get_int("reconnect.max_attempts", errors),
            c.reconnect.max_attempts);
  assign_if(t.get_bool("reconnect.continue_dataset_within_process", errors),
            c.reconnect.continue_dataset_within_process);
  assign_if(t.get_int("reconnect.jitter_seed", errors), c.reconnect.jitter_seed);
  assign_if(t.get_int("reconnect.jitter_pct", errors), c.reconnect.jitter_pct);

  // [benchmark]
  assign_if(t.get_int_array("benchmark.target_events_per_sec", errors),
            c.benchmark.target_events_per_sec);
  assign_if(t.get_int("benchmark.report_interval_ms", errors),
            c.benchmark.report_interval_ms);
  assign_if(t.get_bool("benchmark.sample_processing_latency", errors),
            c.benchmark.sample_processing_latency);

  for (const auto& k : t.unconsumed_keys())
    errors.push_back("unknown configuration key: " + k);

  c.validate(errors);

  if (errors.size() != before) return false;
  out = std::move(c);
  return true;
}

void Config::validate(std::vector<std::string>& e) const {
  const auto positive = [&e](std::int64_t v, const char* name) {
    if (v <= 0) e.push_back(std::string(name) + " must be > 0");
  };
  const auto non_negative = [&e](std::int64_t v, const char* name) {
    if (v < 0) e.push_back(std::string(name) + " must be >= 0");
  };

  // identity ---------------------------------------------------------------
  if (identity.protocol != "MDP1")
    e.push_back("identity.protocol must be MDP1 (no protocol auto-negotiation)");
  if (identity.proto_package != "marketdata.v1")
    e.push_back("identity.proto_package must be marketdata.v1");
  if (identity.source_schema_version.empty())
    e.push_back("identity.source_schema_version must not be empty");
  if (identity.allowed_symbols.empty())
    e.push_back("identity.allowed_symbols must list at least one symbol");
  positive(identity.max_symbols, "identity.max_symbols");
  positive(identity.max_datasets, "identity.max_datasets");
  positive(identity.max_state_bytes, "identity.max_state_bytes");
  if (static_cast<std::int64_t>(identity.allowed_symbols.size()) >
      identity.max_symbols)
    e.push_back("identity.max_symbols is smaller than allowed_symbols");
  for (const auto& s : identity.allowed_symbols) {
    if (s.empty()) {
      e.push_back("identity.allowed_symbols contains an empty symbol");
      continue;
    }
    if (static_cast<std::int64_t>(s.size()) > validation.max_symbol_bytes)
      e.push_back("allowed symbol longer than validation.max_symbol_bytes: " + s);
    for (char ch : s) {
      const bool ok = (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
                      ch == '.' || ch == '_' || ch == '-';
      if (!ok) {
        e.push_back("allowed symbol violates the ASCII allowlist: " + s);
        break;
      }
    }
  }

  // input ------------------------------------------------------------------
  positive(input.max_payload_bytes, "input.max_payload_bytes");
  if (input.max_payload_bytes > 4096)
    e.push_back("input.max_payload_bytes exceeds the MDP1 cap of 4096");
  positive(input.read_chunk_bytes, "input.read_chunk_bytes");
  positive(input.max_frames_per_iteration, "input.max_frames_per_iteration");
  positive(input.connect_deadline_ms, "input.connect_deadline_ms");
  // A zero/absent partial-frame deadline would let a slowloris sender hold the
  // connection open indefinitely.
  positive(input.prefix_deadline_ms, "input.prefix_deadline_ms");
  positive(input.header_partial_deadline_ms, "input.header_partial_deadline_ms");
  positive(input.body_deadline_ms, "input.body_deadline_ms");
  non_negative(input.idle_timeout_ms, "input.idle_timeout_ms");

  if (input.mode == InputMode::kTcp) {
    if (input.port < 1 || input.port > 65535)
      e.push_back("input.port is outside 1..65535");
    if (input.port == 9009)
      e.push_back(
          "input.port 9009 is the Project 01 JSONL port; MDP1 protobuf is "
          "served on 9010 and the adapter never sniffs the protocol");
    if (input.host.empty()) e.push_back("input.host must not be empty");
    if (!is_loopback_host(input.host) && !input.allow_non_loopback)
      e.push_back(
          "input.host is not loopback; set input.allow_non_loopback=true to "
          "accept an untrusted endpoint explicitly");
  } else {
    if (input.file.empty())
      e.push_back("input.file is required for input.mode=pbstream");
    else if (unsafe_path(input.file))
      e.push_back("input.file contains an unsafe path: " + input.file);
  }

  // validation -------------------------------------------------------------
  positive(validation.max_dataset_id_bytes, "validation.max_dataset_id_bytes");
  positive(validation.max_symbol_bytes, "validation.max_symbol_bytes");
  positive(validation.max_source_version_bytes,
           "validation.max_source_version_bytes");
  positive(validation.sequence_window, "validation.sequence_window");
  if (validation.sequence_window > 1 << 20)
    e.push_back("validation.sequence_window is unreasonably large (> 1048576)");
  if (validation.on_business_reject != "skip_with_reason" &&
      validation.on_business_reject != "fail_closed")
    e.push_back(
        "validation.on_business_reject must be skip_with_reason|fail_closed");
  if (validation.on_malformed_frame != "fail_closed")
    e.push_back(
        "validation.on_malformed_frame must be fail_closed in this build "
        "(no resynchronisation scan exists)");
  if (validation.mode == ValidationMode::kFaultObserve &&
      validation.fault_manifest.empty())
    e.push_back(
        "validation.mode=fault_observe requires validation.fault_manifest "
        "(a verified fault plan)");
  if (validation.mode == ValidationMode::kFaultObserve &&
      output.sink == SinkKind::kP04Stub)
    e.push_back(
        "fault_observe must not feed the production-like P04 sink; use "
        "ndjson_test or metrics_only");
  if (!validation.fault_manifest.empty() &&
      unsafe_path(validation.fault_manifest))
    e.push_back("validation.fault_manifest contains an unsafe path");

  // prices -----------------------------------------------------------------
  for (const auto& sym : identity.allowed_symbols) {
    if (prices.find(sym) == prices.end())
      e.push_back("no [prices." + sym + "] rules for an allowed symbol");
  }
  for (const auto& kv : prices) {
    const std::string& sym = kv.first;
    const PriceRules& r = kv.second;
    const std::string p = "prices." + sym + ".";
    if (std::find(identity.allowed_symbols.begin(),
                  identity.allowed_symbols.end(),
                  sym) == identity.allowed_symbols.end())
      e.push_back(p + " defines rules for a symbol missing from allowed_symbols");
    if (r.price_scale <= 0) e.push_back(p + "price_scale must be > 0");
    if (r.min_price_ticks <= 0) e.push_back(p + "min_price_ticks must be > 0");
    if (r.max_price_ticks < r.min_price_ticks)
      e.push_back(p + "max_price_ticks < min_price_ticks");
    if (r.min_spread_ticks < 0) e.push_back(p + "min_spread_ticks must be >= 0");
    if (r.max_spread_ticks < r.min_spread_ticks)
      e.push_back(p + "max_spread_ticks < min_spread_ticks");
    // bid + spread must stay inside int64 so the validator can never overflow.
    std::int64_t sum = 0;
    if (__builtin_add_overflow(r.max_price_ticks, r.max_spread_ticks, &sum))
      e.push_back(p + "max_price_ticks + max_spread_ticks overflows int64");
  }

  // freshness --------------------------------------------------------------
  positive(freshness.event_time_stale_threshold_ns,
           "freshness.event_time_stale_threshold_ns");
  positive(freshness.quote_inactivity_threshold_ms,
           "freshness.quote_inactivity_threshold_ms");
  positive(freshness.timer_tick_ms, "freshness.timer_tick_ms");
  if (freshness.timer_tick_ms > freshness.quote_inactivity_threshold_ms)
    e.push_back(
        "freshness.timer_tick_ms is coarser than the inactivity threshold; "
        "the transition could not be detected within one tick");
  if (freshness.clock_mode == ClockMode::kVirtualForReplay &&
      input.mode == InputMode::kTcp)
    e.push_back(
        "freshness.clock_mode=virtual_for_replay is offline-only; a live TCP "
        "run must use monotonic_live");

  // output -----------------------------------------------------------------
  positive(output.max_queue_events, "output.max_queue_events");
  positive(output.max_queue_bytes, "output.max_queue_bytes");
  if (output.max_queue_bytes < input.max_payload_bytes)
    e.push_back(
        "output.max_queue_bytes cannot hold a single maximum-size frame");
  if (output.pause_read_high_watermark_pct <= 0 ||
      output.pause_read_high_watermark_pct > 100)
    e.push_back("output.pause_read_high_watermark_pct must be in 1..100");
  if (output.resume_read_low_watermark_pct < 0 ||
      output.resume_read_low_watermark_pct > 100)
    e.push_back("output.resume_read_low_watermark_pct must be in 0..100");
  if (output.resume_read_low_watermark_pct >=
      output.pause_read_high_watermark_pct)
    e.push_back(
        "output.resume_read_low_watermark_pct must be strictly below the high "
        "watermark (hysteresis)");
  positive(output.output_stall_deadline_ms, "output.output_stall_deadline_ms");
  non_negative(output.stub_delay_us, "output.stub_delay_us");
  if (output.stub_delay_us != 0 && output.sink != SinkKind::kP04Stub)
    e.push_back("output.stub_delay_us only applies to the p04_stub sink");
  non_negative(output.shutdown_drain_deadline_ms,
               "output.shutdown_drain_deadline_ms");
  if (output.sink != SinkKind::kMetricsOnly) {
    if (output.file.empty())
      e.push_back("output.file is required for a file-backed sink");
    else if (unsafe_path(output.file))
      e.push_back("output.file contains an unsafe path: " + output.file);
  }

  // reconnect --------------------------------------------------------------
  positive(reconnect.initial_backoff_ms, "reconnect.initial_backoff_ms");
  positive(reconnect.max_backoff_ms, "reconnect.max_backoff_ms");
  if (reconnect.max_backoff_ms < reconnect.initial_backoff_ms)
    e.push_back("reconnect.max_backoff_ms < reconnect.initial_backoff_ms");
  non_negative(reconnect.max_attempts, "reconnect.max_attempts");
  if (reconnect.jitter_pct < 0 || reconnect.jitter_pct > 100)
    e.push_back("reconnect.jitter_pct must be in 0..100");

  // benchmark --------------------------------------------------------------
  positive(benchmark.report_interval_ms, "benchmark.report_interval_ms");
  for (auto v : benchmark.target_events_per_sec)
    if (v <= 0) e.push_back("benchmark.target_events_per_sec must all be > 0");
}

}  // namespace md
