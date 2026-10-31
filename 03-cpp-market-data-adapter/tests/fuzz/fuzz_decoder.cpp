// Fuzzes protobuf decoding plus structural and business validation.
//
// The property: for any bytes at all, either the event is rejected with a
// reason, or it is accepted AND every field it produced satisfies the
// contract. There is no third outcome.
#include <cstddef>
#include <cstdint>
#include <span>

#include "md_adapter/config.hpp"
#include "md_adapter/decoder.hpp"
#include "md_adapter/validators.hpp"

namespace {

const md::Config& fuzz_config() {
  static const md::Config* cfg = [] {
    auto* c = new md::Config();
    std::vector<std::string> errs;
    const char* kToml = R"(
[identity]
allowed_symbols = ["EURUSD", "GBPUSD", "USDJPY"]
[input]
file = "x.pbstream"
require_completed_manifest = false
[output]
sink = "metrics_only"
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
  if (size > 4096) return 0;  // the framer would reject anything larger
  md::silence_protobuf_logging();

  static md::ProtobufDecoder decoder;
  static const md::StructuralLimits limits =
      md::structural_limits_from(fuzz_config());
  static const md::BusinessValidator business(fuzz_config());

  const auto r = decoder.decode(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(data), size));
  if (!r.ok) return 0;

  md::DomainEvent e;
  if (md::validate_structural(decoder.message(), limits, r.unknown_fields, e) !=
      md::Reason::kNone)
    return 0;

  // Structural acceptance implies hard bounds on what came out.
  if (e.symbol.empty() || e.symbol.size() > 32) __builtin_trap();
  if (e.dataset_id.empty() || e.dataset_id.size() > 128) __builtin_trap();
  if (e.source_schema_version != "sim.v1") __builtin_trap();

  if (business.validate(e) != md::Reason::kNone) return 0;

  // Business acceptance implies market-plausible values.
  if (e.sequence <= 0 || e.event_time_ns < 0 || e.price_scale <= 0)
    __builtin_trap();
  if (md::kind_of(e.payload) == md::PayloadKind::kQuote) {
    const auto& q = std::get<md::QuotePrice>(e.payload);
    if (q.bid <= 0 || q.ask < q.bid) __builtin_trap();
  } else {
    if (std::get<md::TickPrice>(e.payload).price <= 0) __builtin_trap();
  }
  return 0;
}
