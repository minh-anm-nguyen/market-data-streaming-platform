// Fuzzes the TOML parser and the configuration validator.
//
// Configuration is operator-supplied rather than peer-supplied, but a parser
// that crashes on a typo is still a defect, and the validator is the gate
// that stops an impossible configuration from reaching the hot path.
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "md_adapter/config.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
  if (size > (1u << 16)) return 0;
  const std::string_view text(reinterpret_cast<const char*>(data), size);

  md::Config cfg;
  std::vector<std::string> errors;
  const bool ok = md::Config::load_from_string(text, "<fuzz>", cfg, errors);

  if (!ok) {
    // A rejection must always say why; a silent refusal is unusable.
    if (errors.empty()) __builtin_trap();
    return 0;
  }
  // Acceptance means the invariants the rest of the code relies on hold.
  if (errors.size() != 0) __builtin_trap();
  if (cfg.identity.allowed_symbols.empty()) __builtin_trap();
  if (cfg.input.max_payload_bytes <= 0 || cfg.input.max_payload_bytes > 4096)
    __builtin_trap();
  if (cfg.validation.sequence_window <= 0) __builtin_trap();
  if (cfg.output.resume_read_low_watermark_pct >=
      cfg.output.pause_read_high_watermark_pct)
    __builtin_trap();
  if (cfg.output.max_queue_bytes < cfg.input.max_payload_bytes)
    __builtin_trap();
  for (const auto& s : cfg.identity.allowed_symbols)
    if (cfg.prices.find(s) == cfg.prices.end()) __builtin_trap();
  return 0;
}
