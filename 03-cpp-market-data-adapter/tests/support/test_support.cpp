#include "test_support.hpp"

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include "md_adapter/sha256.hpp"

namespace mdtest {

std::string be32(std::uint32_t v) {
  std::string s(4, '\0');
  s[0] = static_cast<char>((v >> 24) & 0xff);
  s[1] = static_cast<char>((v >> 16) & 0xff);
  s[2] = static_cast<char>((v >> 8) & 0xff);
  s[3] = static_cast<char>(v & 0xff);
  return s;
}

std::string frame(const std::string& payload) {
  return be32(static_cast<std::uint32_t>(payload.size())) + payload;
}

std::string stream(const std::vector<std::string>& payloads) {
  std::string s = "MDP1";
  for (const auto& p : payloads) s += frame(p);
  return s;
}

std::span<const std::byte> as_bytes(const std::string& s) {
  return std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(s.data()), s.size());
}

marketdata::v1::MarketDataEvent make_message(const EventSpec& s) {
  marketdata::v1::MarketDataEvent m;
  if (s.has_dataset_id) m.set_dataset_id(s.dataset_id);
  if (s.has_symbol) m.set_symbol(s.symbol);
  if (s.has_sequence) m.set_sequence(s.sequence);
  if (s.has_event_time) m.set_event_time_ns(s.event_time_ns);
  if (s.has_price_scale) m.set_price_scale(s.price_scale);
  if (s.has_version) m.set_source_schema_version(s.version);
  if (s.kind == "quote") {
    auto* q = m.mutable_quote();
    if (s.has_bid) q->set_bid_ticks(s.bid);
    if (s.has_ask) q->set_ask_ticks(s.ask);
  } else if (s.kind == "tick") {
    auto* t = m.mutable_tick();
    if (s.has_price) t->set_price_ticks(s.price);
  }
  return m;
}

std::string encode(const EventSpec& s) {
  return make_message(s).SerializeAsString();
}

md::DomainEvent quote_event(std::int64_t seq, std::int64_t event_time_ns,
                            std::int64_t bid, std::int64_t ask,
                            const std::string& symbol,
                            const std::string& dataset) {
  md::DomainEvent e;
  e.dataset_id = dataset;
  e.symbol = symbol;
  e.sequence = seq;
  e.event_time_ns = event_time_ns;
  e.price_scale = (symbol == "USDJPY") ? 1000 : 100000;
  e.source_schema_version = "sim.v1";
  e.payload = md::QuotePrice{bid, ask};
  return e;
}

md::DomainEvent tick_event(std::int64_t seq, std::int64_t event_time_ns,
                           std::int64_t price, const std::string& symbol,
                           const std::string& dataset) {
  md::DomainEvent e;
  e.dataset_id = dataset;
  e.symbol = symbol;
  e.sequence = seq;
  e.event_time_ns = event_time_ns;
  e.price_scale = (symbol == "USDJPY") ? 1000 : 100000;
  e.source_schema_version = "sim.v1";
  e.payload = md::TickPrice{price};
  return e;
}

std::string base_config_toml() {
  return R"([identity]
protocol = "MDP1"
proto_package = "marketdata.v1"
source_schema_version = "sim.v1"
expected_descriptor_sha256 = "UNPINNED"
allowed_symbols = ["EURUSD", "GBPUSD", "USDJPY"]
max_symbols = 256
max_datasets = 4
max_state_bytes = 67108864

[input]
mode = "pbstream"
file = "in.pbstream"
require_completed_manifest = false
host = "127.0.0.1"
port = 9010
max_payload_bytes = 4096
read_chunk_bytes = 65536
max_frames_per_iteration = 256
connect_deadline_ms = 3000
prefix_deadline_ms = 2000
header_partial_deadline_ms = 2000
body_deadline_ms = 2000
idle_timeout_ms = 0

[validation]
mode = "strict_monitor"
max_dataset_id_bytes = 128
max_symbol_bytes = 32
max_source_version_bytes = 32
sequence_window = 4096
on_business_reject = "skip_with_reason"
on_malformed_frame = "fail_closed"

[prices.EURUSD]
price_scale = 100000
min_price_ticks = 1
max_price_ticks = 500000
min_spread_ticks = 0
max_spread_ticks = 200

[prices.GBPUSD]
price_scale = 100000
min_price_ticks = 1
max_price_ticks = 500000
min_spread_ticks = 0
max_spread_ticks = 200

[prices.USDJPY]
price_scale = 1000
min_price_ticks = 1
max_price_ticks = 500000
min_spread_ticks = 0
max_spread_ticks = 200

[freshness]
event_time_stale_threshold_ns = 2000000000
quote_inactivity_threshold_ms = 3000
timer_tick_ms = 100
clock_mode = "virtual_for_replay"

[output]
sink = "metrics_only"
max_queue_events = 4096
max_queue_bytes = 8388608
pause_read_high_watermark_pct = 80
resume_read_low_watermark_pct = 40
output_stall_deadline_ms = 5000
shutdown_drain_deadline_ms = 5000
file = "out.ndjson"

[reconnect]
initial_backoff_ms = 100
max_backoff_ms = 10000
max_attempts = 50
continue_dataset_within_process = true

[benchmark]
target_events_per_sec = [1000]
report_interval_ms = 1000
sample_processing_latency = true
)";
}

md::Config base_config() {
  md::Config c;
  std::vector<std::string> errors;
  const bool ok =
      md::Config::load_from_string(base_config_toml(), "<test>", c, errors);
  if (!ok) {
    // Fail loudly: a broken baseline would silently weaken every test.
    std::string msg = "base_config() is invalid:";
    for (const auto& e : errors) msg += "\n  " + e;
    throw std::runtime_error(msg);
  }
  return c;
}

md::FrameAction CollectingHandler::on_frame(std::span<const std::byte> payload,
                                            md::Reason& fatal_reason) {
  frames.emplace_back(reinterpret_cast<const char*>(payload.data()),
                      payload.size());
  if (next_action == md::FrameAction::kFatal) fatal_reason = fatal_with;
  return next_action;
}

md::Mdp1Framer::FeedResult feed_chunked(md::Mdp1Framer& framer,
                                        const std::string& data,
                                        std::size_t chunk,
                                        md::FrameHandler& handler,
                                        std::int64_t now_ns) {
  // Mirrors the real read loop: whatever the framer does not consume (because
  // it hit its per-iteration frame budget) is retained and re-offered, so a
  // chunked feed cannot lose bytes that a single feed would have kept.
  md::Mdp1Framer::FeedResult last;
  std::size_t consumed_total = 0;
  std::size_t next = 0;
  std::string buffered;
  while (next < data.size() || !buffered.empty()) {
    if (next < data.size()) {
      const std::size_t n = std::min(chunk, data.size() - next);
      buffered.append(data, next, n);
      next += n;
    }
    last = framer.feed(as_bytes(buffered), now_ns, handler);
    consumed_total += last.bytes_consumed;
    buffered.erase(0, last.bytes_consumed);
    if (last.fatal || last.paused) break;
    if (last.bytes_consumed == 0 && next >= data.size()) break;
  }
  last.bytes_consumed = consumed_total;
  return last;
}

TempDir::TempDir() {
  const auto base = std::filesystem::temp_directory_path() /
                    ("p03-test-" + std::to_string(::getpid()) + "-" +
                     std::to_string(reinterpret_cast<std::uintptr_t>(this)));
  std::filesystem::create_directories(base);
  path_ = base.string();
}

TempDir::~TempDir() {
  std::error_code ec;
  std::filesystem::remove_all(path_, ec);
}

std::string TempDir::file(const std::string& name) const {
  return (std::filesystem::path(path_) / name).string();
}

void write_file(const std::string& path, const std::string& data) {
  std::ofstream f(path, std::ios::binary | std::ios::trunc);
  f.write(data.data(), static_cast<std::streamsize>(data.size()));
}

std::string read_file(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  std::ostringstream ss;
  ss << f.rdbuf();
  return ss.str();
}

void write_manifest(const std::string& pbstream_path, const std::string& data,
                    std::int64_t frames, const std::string& status) {
  std::ostringstream o;
  o << "{\"dataset_id\": \"ds-test\", \"sha256\": \"" << md::sha256_hex(data)
    << "\", \"status\": \"" << status << "\", \"frames\": " << frames
    << ", \"bytes\": " << data.size() << "}\n";
  write_file(pbstream_path + ".manifest.json", o.str());
}

}  // namespace mdtest
