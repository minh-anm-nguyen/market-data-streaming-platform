// md_adapter_bench -- isolated benchmark lanes (design section 12.1).
//
// The lanes exist so that numbers are never mixed. A decode-only figure says
// nothing about live TCP capacity, and a metrics_only figure says nothing
// about sink throughput. Each lane therefore prints what it measured AND what
// it explicitly does not establish.
//
// Nothing here declares a sustainable rate. Sustainability needs a steady
// state over time with bounded queues and a real source, which is the job of
// scripts/run_bench.sh driving the full adapter.
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "md_adapter/bounded_queue.hpp"
#include "md_adapter/classifier.hpp"
#include "md_adapter/clock.hpp"
#include "md_adapter/config.hpp"
#include "md_adapter/decoder.hpp"
#include "md_adapter/framer.hpp"
#include "md_adapter/metrics.hpp"
#include "md_adapter/pipeline.hpp"
#include "md_adapter/quote_freshness.hpp"
#include "md_adapter/sequence_tracker.hpp"
#include "md_adapter/validators.hpp"

namespace {

constexpr const char* kDefaultConfig = R"(
[identity]
protocol = "MDP1"
proto_package = "marketdata.v1"
source_schema_version = "sim.v1"
allowed_symbols = ["EURUSD", "GBPUSD", "USDJPY"]
max_symbols = 256
max_datasets = 4
max_state_bytes = 268435456

[input]
mode = "pbstream"
file = "bench.pbstream"
require_completed_manifest = false

[validation]
mode = "strict_monitor"
sequence_window = 4096

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
clock_mode = "virtual_for_replay"

[output]
sink = "metrics_only"
)";

// Builds an in-memory corpus of encoded MarketDataEvents. Generated with the
// real protobuf encoder so payload sizes match the wire, not an estimate.
std::vector<std::string> build_corpus(std::size_t events, int symbols,
                                      int quote_pct) {
  static const char* kSyms[] = {"EURUSD", "GBPUSD", "USDJPY"};
  static const std::int64_t kScales[] = {100000, 100000, 1000};
  std::vector<std::string> out;
  out.reserve(events);
  marketdata::v1::MarketDataEvent m;
  std::mt19937_64 rng(12345);
  for (std::size_t i = 0; i < events; ++i) {
    const int s = static_cast<int>(i % static_cast<std::size_t>(symbols));
    m.Clear();
    m.set_dataset_id("bench-ds");
    m.set_symbol(kSyms[s]);
    m.set_sequence(static_cast<std::int64_t>(i / static_cast<std::size_t>(symbols)) + 1);
    m.set_event_time_ns(static_cast<std::int64_t>(i) * 1'000'000);
    m.set_price_scale(kScales[s]);
    m.set_source_schema_version("sim.v1");
    const int roll = static_cast<int>(rng() % 100);
    if (roll < quote_pct) {
      const std::int64_t bid = 100000 + static_cast<std::int64_t>(rng() % 1000);
      m.mutable_quote()->set_bid_ticks(bid);
      m.mutable_quote()->set_ask_ticks(bid + 10);
    } else {
      m.mutable_tick()->set_price_ticks(
          100000 + static_cast<std::int64_t>(rng() % 1000));
    }
    out.push_back(m.SerializeAsString());
  }
  return out;
}

std::string frame_corpus(const std::vector<std::string>& corpus) {
  std::string bytes = "MDP1";
  for (const auto& p : corpus) {
    const auto n = static_cast<std::uint32_t>(p.size());
    const char hdr[4] = {static_cast<char>((n >> 24) & 0xff),
                         static_cast<char>((n >> 16) & 0xff),
                         static_cast<char>((n >> 8) & 0xff),
                         static_cast<char>(n & 0xff)};
    bytes.append(hdr, 4);
    bytes.append(p);
  }
  return bytes;
}

struct LaneResult {
  double seconds = 0.0;
  std::uint64_t units = 0;
  std::uint64_t bytes = 0;
};

void print_lane(const char* lane, const LaneResult& r, const char* unit,
                const char* does_not_establish, const md::LatencyHistogram* h) {
  const double rate =
      r.seconds > 0.0 ? static_cast<double>(r.units) / r.seconds : 0.0;
  std::cout << "lane: " << lane << "\n";
  std::cout << "  " << unit << ": " << r.units << "\n";
  std::cout << "  seconds: " << r.seconds << "\n";
  std::cout << "  rate_per_sec: " << static_cast<std::uint64_t>(rate) << "\n";
  if (r.bytes != 0) {
    std::cout << "  bytes: " << r.bytes << "\n";
    std::cout << "  mib_per_sec: "
              << (r.seconds > 0.0
                      ? static_cast<double>(r.bytes) / r.seconds / 1048576.0
                      : 0.0)
              << "\n";
  }
  if (h != nullptr && h->count() > 0) {
    std::cout << "  latency_p50_ns: " << h->quantile(0.50) << "\n";
    std::cout << "  latency_p95_ns: " << h->quantile(0.95) << "\n";
    std::cout << "  latency_p99_ns: " << h->quantile(0.99) << "\n";
    std::cout << "  latency_p999_ns: " << h->quantile(0.999) << "\n";
    std::cout << "  latency_max_ns: " << h->max() << "\n";
    std::cout << "  latency_samples: " << h->count() << "\n";
  }
  std::cout << "  peak_rss_bytes: " << md::peak_rss_bytes() << "\n";
  std::cout << "  does_not_establish: \"" << does_not_establish << "\"\n";
  std::cout << "  sustainable_throughput_verified: false\n";
}

double now_seconds() {
  return std::chrono::duration<double>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Collects frames without doing anything else, so lane A measures the framer
// and decoder only.
class CountingHandler final : public md::FrameHandler {
 public:
  explicit CountingHandler(md::ProtobufDecoder& d) : dec_(d) {}
  md::FrameAction on_frame(std::span<const std::byte> payload,
                           md::Reason& fatal) override {
    const auto r = dec_.decode(payload);
    if (!r.ok) {
      fatal = md::Reason::kProtoDecodeError;
      return md::FrameAction::kFatal;
    }
    ++frames;
    return md::FrameAction::kContinue;
  }
  std::uint64_t frames = 0;

 private:
  md::ProtobufDecoder& dec_;
};

void usage() {
  std::cerr << "md_adapter_bench --lane <decode-only|classify-only|"
               "offline-full> [--events N] [--symbols N] [--quote-pct N]\n";
}

}  // namespace

int main(int argc, char** argv) {
  md::silence_protobuf_logging();

  std::string lane = "decode-only";
  std::size_t events = 200000;
  int symbols = 3;
  int quote_pct = 80;

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    const auto val = [&]() -> std::string {
      return (i + 1 < argc) ? argv[++i] : std::string();
    };
    if (a == "--lane") lane = val();
    else if (a == "--events") events = std::stoull(val());
    else if (a == "--symbols") symbols = std::stoi(val());
    else if (a == "--quote-pct") quote_pct = std::stoi(val());
    else if (a == "--help") { usage(); return 0; }
    else { usage(); return 2; }
  }
  if (symbols < 1 || symbols > 3 || quote_pct < 0 || quote_pct > 100) {
    usage();
    return 2;
  }

  md::Config cfg;
  std::vector<std::string> errors;
  if (!md::Config::load_from_string(kDefaultConfig, "<bench-builtin>", cfg,
                                    errors)) {
    for (const auto& e : errors) std::cerr << "config: " << e << "\n";
    return 2;
  }

  const auto corpus = build_corpus(events, symbols, quote_pct);
  std::uint64_t payload_bytes = 0;
  for (const auto& p : corpus) payload_bytes += p.size();
  std::cout << "workload:\n";
  std::cout << "  events: " << events << "\n";
  std::cout << "  symbols: " << symbols << "\n";
  std::cout << "  quote_pct: " << quote_pct << "\n";
  std::cout << "  payload_bytes_total: " << payload_bytes << "\n";
  std::cout << "  payload_bytes_mean: "
            << (events ? payload_bytes / events : 0) << "\n";

  if (lane == "decode-only") {
    const std::string framed = frame_corpus(corpus);
    md::ProtobufDecoder dec;
    CountingHandler h(dec);
    md::FramerLimits fl;
    fl.prefix_deadline_ns = 0;
    fl.header_partial_deadline_ns = 0;
    fl.body_deadline_ns = 0;
    fl.max_frames_per_iteration = SIZE_MAX;
    md::Mdp1Framer framer(fl);
    framer.reset(0);

    // Warm the page cache and the allocator before measuring.
    {
      md::ProtobufDecoder wd;
      CountingHandler wh(wd);
      md::Mdp1Framer wf(fl);
      wf.reset(0);
      wf.feed(std::span<const std::byte>(
                  reinterpret_cast<const std::byte*>(framed.data()),
                  std::min<std::size_t>(framed.size(), 1 << 20)),
              0, wh);
    }

    const double t0 = now_seconds();
    const auto fr = framer.feed(
        std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(framed.data()), framed.size()),
        0, h);
    const double t1 = now_seconds();
    if (fr.fatal) {
      std::cerr << "bench: framing failed: " << md::to_string(fr.reason) << "\n";
      return 4;
    }
    print_lane("A-decode-only", {t1 - t0, h.frames, framed.size()}, "frames",
               "TCP or P04 capacity; no validation or state work is included",
               nullptr);
    return 0;
  }

  if (lane == "classify-only") {
    // Pre-decode everything, then measure validation + sequence + freshness
    // + policy with the transport entirely out of the picture.
    md::ProtobufDecoder dec;
    std::vector<md::DomainEvent> decoded;
    decoded.reserve(corpus.size());
    const auto limits = md::structural_limits_from(cfg);
    for (const auto& p : corpus) {
      const auto r = dec.decode(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(p.data()), p.size()));
      if (!r.ok) continue;
      md::DomainEvent e;
      if (md::validate_structural(dec.message(), limits, r.unknown_fields, e) ==
          md::Reason::kNone)
        decoded.push_back(std::move(e));
    }

    md::BusinessValidator bv(cfg);
    md::SequenceLimits sl;
    sl.window = 4096;
    sl.max_state_bytes = static_cast<std::uint64_t>(cfg.identity.max_state_bytes);
    sl.live_baseline_unverified = false;
    md::SequenceTracker tracker(sl);
    md::FreshnessEngine fresh({2'000'000'000, 3'000'000'000});
    md::Classifier cls(md::ValidationMode::kStrictMonitor);
    md::LatencyHistogram hist;
    md::VirtualClock clk(0);     // drives freshness, deterministically
    md::SteadyClock perf;        // measures time, which a virtual clock cannot

    std::uint64_t ok = 0;
    const double t0 = now_seconds();
    for (const auto& e : decoded) {
      const std::int64_t s0 = perf.now_mono_ns();
      if (bv.validate(e) != md::Reason::kNone) continue;
      const auto sr = tracker.observe(e);
      const auto fr2 = fresh.observe(e, clk.now_mono_ns());
      const auto d = cls.classify(e, sr, fr2);
      if (d.publish) ++ok;
      hist.record(perf.now_mono_ns() - s0);
      clk.advance_ns(1000);
    }
    const double t1 = now_seconds();
    std::cout << "  published: " << ok << "\n";
    std::cout << "  sequence_state_bytes: " << tracker.state_bytes() << "\n";
    print_lane("B-classify-only", {t1 - t0, decoded.size(), 0}, "events",
               "parser or network latency; the freshness clock is virtual, "
               "so this lane says nothing about inactivity timing",
               &hist);
    return 0;
  }

  if (lane == "offline-full") {
    // Framer + decoder + validators + state + policy into metrics_only. This
    // is the closest in-process figure to offline replay, and it still says
    // nothing about durable downstream throughput.
    const std::string framed = frame_corpus(corpus);
    md::MetricsOnlySink sink;
    md::BoundedQueue queue({static_cast<std::size_t>(cfg.output.max_queue_events),
                            static_cast<std::size_t>(cfg.output.max_queue_bytes),
                            80, 40});
    md::SteadyClock clk;
    md::Pipeline pipeline(cfg, clk, queue, false, false);
    md::SinkWriter writer(queue, sink);
    writer.start();

    md::FramerLimits fl;
    fl.prefix_deadline_ns = 0;
    fl.header_partial_deadline_ns = 0;
    fl.body_deadline_ns = 0;
    fl.max_frames_per_iteration = 256;
    md::Mdp1Framer framer(fl);
    framer.reset(clk.now_mono_ns());

    const double t0 = now_seconds();
    std::size_t off = 0;
    bool failed = false;
    while (off < framed.size()) {
      const auto fr = framer.feed(
          std::span<const std::byte>(
              reinterpret_cast<const std::byte*>(framed.data()) + off,
              framed.size() - off),
          clk.now_mono_ns(), pipeline);
      off += fr.bytes_consumed;
      pipeline.confirm_delivered(writer.written());
      if (fr.fatal) {
        std::cerr << "bench: stream failed: " << md::to_string(fr.reason) << "\n";
        failed = true;
        break;
      }
      if (fr.paused) {
        while (!pipeline.flush_pending())
          std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      if (fr.bytes_consumed == 0 && !fr.paused) break;
    }
    writer.stop_and_drain(5'000'000'000);
    const double t1 = now_seconds();
    if (failed) return 4;

    // The runner normally closes these out; this lane drives the pipeline
    // directly, so it has to do the same reconciliation or the conservation
    // check below would compare against zeros.
    auto& mm = pipeline.metrics();
    mm.output_success = writer.written();
    mm.output_failed_or_pending =
        mm.classified_output_attempted >= mm.output_success
            ? mm.classified_output_attempted - mm.output_success
            : 0;
    const auto& m = pipeline.metrics();
    std::cout << "  frames_complete: " << m.frames_complete << "\n";
    std::cout << "  business_ok: " << m.business_ok << "\n";
    std::cout << "  output_success: " << writer.written() << "\n";
    std::cout << "  peak_queue_events: " << queue.peak_events() << "\n";
    std::cout << "  peak_queue_bytes: " << queue.peak_bytes() << "\n";
    std::cout << "  sequence_state_bytes: "
              << pipeline.tracker().state_bytes() << "\n";
    const auto cons = m.check_conservation();
    std::cout << "  conservation_ok: " << (cons.ok ? "true" : "false") << "\n";
    print_lane("C-offline-full", {t1 - t0, m.frames_complete, framed.size()},
               "frames",
               "durable downstream throughput; metrics_only writes nothing",
               &m.processing_latency);
    return cons.ok ? 0 : 4;
  }

  usage();
  return 2;
}
