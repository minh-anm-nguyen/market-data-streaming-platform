// Fuzzes the whole pipeline: framer -> decode -> validate -> state -> policy.
//
// This is where state-machine bugs show up that neither the framer nor the
// decoder fuzzer can reach: window arithmetic, duplicate comparison, and the
// bounded-state caps under adversarial sequences.
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "md_adapter/pipeline.hpp"

namespace {

const md::Config& fuzz_config() {
  static const md::Config* cfg = [] {
    auto* c = new md::Config();
    std::vector<std::string> errs;
    const char* kToml = R"(
[identity]
allowed_symbols = ["EURUSD", "GBPUSD", "USDJPY"]
max_symbols = 8
max_datasets = 2
max_state_bytes = 4194304
[input]
file = "x.pbstream"
require_completed_manifest = false
[validation]
sequence_window = 128
[output]
sink = "metrics_only"
max_queue_events = 64
max_queue_bytes = 1048576
[prices.EURUSD]
price_scale = 100000
min_price_ticks = 1
max_price_ticks = 500000
max_spread_ticks = 200
[prices.GBPUSD]
price_scale = 100000
min_price_ticks = 1
max_price_ticks = 500000
max_spread_ticks = 200
[prices.USDJPY]
price_scale = 1000
min_price_ticks = 1
max_price_ticks = 500000
max_spread_ticks = 200
)";
    if (!md::Config::load_from_string(kToml, "<fuzz>", *c, errs))
      __builtin_trap();
    return c;
  }();
  return *cfg;
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
  if (size > (1u << 18)) return 0;
  md::silence_protobuf_logging();

  const md::Config& cfg = fuzz_config();
  md::VirtualClock clock(0);
  md::BoundedQueue queue({64, 1u << 20, 80, 40});
  md::Pipeline pipeline(cfg, clock, queue, false, false);

  md::FramerLimits fl;
  fl.max_payload_bytes = 4096;
  fl.prefix_deadline_ns = 0;
  fl.header_partial_deadline_ns = 0;
  fl.body_deadline_ns = 0;
  fl.max_frames_per_iteration = 32;
  md::Mdp1Framer framer(fl);
  framer.reset(0);

  const auto span = std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(data), size);
  std::size_t off = 0;
  std::vector<md::ClassifiedEvent> drained;
  while (off < size) {
    const auto r = framer.feed(span.subspan(off), clock.now_mono_ns(), pipeline);
    off += r.bytes_consumed;
    clock.advance_ns(1'000'000);
    pipeline.tick(clock.now_mono_ns());

    drained.clear();
    queue.pop_batch(4096, drained);
    queue.release(drained);
    if (r.fatal) break;
    if (r.paused && !pipeline.flush_pending()) break;
    if (r.bytes_consumed == 0 && !r.paused) break;
  }

  // Caps must hold no matter what the input claimed.
  if (pipeline.tracker().state_bytes() >
      static_cast<std::uint64_t>(cfg.identity.max_state_bytes))
    __builtin_trap();
  if (pipeline.tracker().key_count() >
      static_cast<std::size_t>(cfg.identity.max_symbols) *
          static_cast<std::size_t>(cfg.identity.max_datasets))
    __builtin_trap();
  if (queue.peak_events() > 64) __builtin_trap();

  // Counters must still balance after an arbitrary byte stream.
  const auto& m = pipeline.metrics();
  if (m.frames_complete != m.frames_decode_error + m.frames_decoded)
    __builtin_trap();
  if (m.frames_decoded != m.structural_reject + m.structural_ok)
    __builtin_trap();
  if (m.structural_ok != m.business_reject + m.business_ok) __builtin_trap();
  if (m.business_ok != m.policy_suppressed + m.classified_output_attempted)
    __builtin_trap();
  return 0;
}
