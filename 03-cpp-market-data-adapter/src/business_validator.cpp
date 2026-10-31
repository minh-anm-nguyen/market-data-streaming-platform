#include "md_adapter/validators.hpp"

#include <algorithm>

namespace md {
namespace {

// Checked subtraction. The validator must survive adversarial int64 values
// (INT64_MIN bid against INT64_MAX ask) without undefined behaviour.
bool checked_sub(std::int64_t a, std::int64_t b, std::int64_t& out) {
  return !__builtin_sub_overflow(a, b, &out);
}

}  // namespace

BusinessValidator::BusinessValidator(const Config& c)
    : rules_(c.prices), allowed_symbols_(c.identity.allowed_symbols) {}

Reason BusinessValidator::validate(const DomainEvent& e) const {
  // Identity ---------------------------------------------------------------
  if (std::find(allowed_symbols_.begin(), allowed_symbols_.end(), e.symbol) ==
      allowed_symbols_.end())
    return Reason::kSymbolNotAllowed;

  const auto it = rules_.find(e.symbol);
  if (it == rules_.end()) return Reason::kSymbolNotAllowed;
  const PriceRules& r = it->second;

  // Sequence and time ------------------------------------------------------
  // Sequence 0 is not a valid baseline: the simulator's contract starts at 1
  // and a 0 would be indistinguishable from an unset field on a lossy path.
  if (e.sequence <= 0) return Reason::kInvalidSequence;
  if (e.event_time_ns < 0) return Reason::kNegativeEventTime;

  // Scale ------------------------------------------------------------------
  if (e.price_scale <= 0) return Reason::kInvalidPriceScale;
  // A mismatched scale silently rescales every price downstream, so it is a
  // rejection and never a conversion.
  if (e.price_scale != r.price_scale) return Reason::kScaleMismatch;

  // Prices -----------------------------------------------------------------
  if (kind_of(e.payload) == PayloadKind::kQuote) {
    const auto& q = std::get<QuotePrice>(e.payload);
    if (q.bid <= 0) return Reason::kQuoteBidNonPositive;
    if (q.ask < q.bid) return Reason::kQuoteCrossed;
    if (q.bid < r.min_price_ticks || q.bid > r.max_price_ticks)
      return Reason::kPriceOutOfRange;
    if (q.ask < r.min_price_ticks || q.ask > r.max_price_ticks)
      return Reason::kPriceOutOfRange;
    std::int64_t spread = 0;
    if (!checked_sub(q.ask, q.bid, spread)) return Reason::kArithmeticOverflow;
    if (spread < r.min_spread_ticks || spread > r.max_spread_ticks)
      return Reason::kSpreadOutOfRange;
  } else {
    const auto& t = std::get<TickPrice>(e.payload);
    if (t.price <= 0) return Reason::kTickNonPositive;
    if (t.price < r.min_price_ticks || t.price > r.max_price_ticks)
      return Reason::kPriceOutOfRange;
  }

  return Reason::kNone;
}

}  // namespace md
