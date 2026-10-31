// Structural and business validation.
//
// The split matters. Structural validation answers "is this a message this
// build understands at all?" and its failures are fatal to the stream in
// fail-closed mode. Business validation answers "is this a plausible market
// event?" and its failures reject one event while the stream continues --
// a frame boundary is still intact.
//
// Neither validator ever repairs anything. No clamping, no rounding, no
// trimming, no case folding (design 3.4/3.5).
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "market_data.pb.h"
#include "md_adapter/config.hpp"
#include "md_adapter/domain_event.hpp"
#include "md_adapter/status.hpp"

namespace md {

struct StructuralLimits {
  std::size_t max_dataset_id_bytes = 128;
  std::size_t max_symbol_bytes = 32;
  std::size_t max_source_version_bytes = 32;
  std::string expected_source_schema_version = "sim.v1";
  bool reject_unknown_fields = false;
};

StructuralLimits structural_limits_from(const Config& c);

// Returns kNone on success and fills `out`. On failure `out` is left in an
// unspecified state and must not be used.
Reason validate_structural(const marketdata::v1::MarketDataEvent& msg,
                           const StructuralLimits& limits, bool unknown_fields,
                           DomainEvent& out);

// Strict UTF-8 validation (no overlong forms, no surrogates, no > U+10FFFF).
bool is_valid_utf8(std::string_view s);
// Symbol allowlist: [A-Z0-9._-]+
bool is_allowed_symbol_charset(std::string_view s);

// ---------------------------------------------------------------------------

class BusinessValidator {
 public:
  explicit BusinessValidator(const Config& c);

  // Returns kNone when the event is market-plausible under the configured
  // rules for its symbol.
  Reason validate(const DomainEvent& e) const;

 private:
  std::map<std::string, PriceRules> rules_;
  std::vector<std::string> allowed_symbols_;
};

}  // namespace md
