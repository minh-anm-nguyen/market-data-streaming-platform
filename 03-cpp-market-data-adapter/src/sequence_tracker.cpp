#include "md_adapter/sequence_tracker.hpp"

#include <algorithm>
#include <limits>

namespace md {
namespace {

constexpr char kKeySep = '\x1f';  // ASCII unit separator: not legal in a symbol

std::string make_key(const std::string& dataset_id, const std::string& symbol) {
  std::string k;
  k.reserve(dataset_id.size() + symbol.size() + 1);
  k += dataset_id;
  k += kKeySep;
  k += symbol;
  return k;
}

// Saturating signed subtraction. Gap widths are reported, never used to size
// work, so saturating at INT64_MAX is the honest answer for an absurd jump.
std::int64_t saturating_sub(std::int64_t a, std::int64_t b) {
  std::int64_t out = 0;
  if (!__builtin_sub_overflow(a, b, &out)) return out;
  return a > b ? std::numeric_limits<std::int64_t>::max()
               : std::numeric_limits<std::int64_t>::min();
}

std::size_t ring_index(Sequence s, std::uint32_t window) {
  // s > 0 is guaranteed by business validation.
  return static_cast<std::size_t>(static_cast<std::uint64_t>(s) %
                                  static_cast<std::uint64_t>(window));
}

}  // namespace

std::string_view to_string(SeqClass c) noexcept {
  switch (c) {
    case SeqClass::kOk: return "OK";
    case SeqClass::kGap: return "GAP";
    case SeqClass::kDuplicateExact: return "DUPLICATE_EXACT";
    case SeqClass::kDuplicateConflict: return "DUPLICATE_CONFLICT";
    case SeqClass::kReorderLate: return "REORDER_LATE";
    case SeqClass::kTooOldUnverifiable: return "TOO_OLD_UNVERIFIABLE";
    case SeqClass::kStateCapExceeded: return "STATE_CAP_EXCEEDED";
  }
  return "INVALID";
}

SequenceTracker::SequenceTracker(SequenceLimits limits) : limits_(limits) {
  if (limits_.window == 0) limits_.window = 1;
}

std::uint64_t SequenceTracker::bytes_per_key() const noexcept {
  // The ring dominates; the rest is the KeyState header plus a conservative
  // allowance for the map node and the key string.
  return static_cast<std::uint64_t>(limits_.window) * sizeof(Slot) +
         sizeof(KeyState) + 128;
}

bool SequenceTracker::within_window(const KeyState& k, Sequence s) const noexcept {
  if (!k.initialized) return false;
  if (s > k.highest) return false;
  // s > highest - window, computed without overflow.
  const std::int64_t lowest =
      saturating_sub(k.highest, static_cast<std::int64_t>(limits_.window) - 1);
  return s >= lowest;
}

const SequenceTracker::Slot* SequenceTracker::seen_slot(
    const KeyState& k, Sequence s) const noexcept {
  const Slot& slot = k.ring[ring_index(s, limits_.window)];
  // Two sequences sharing a ring index differ by a multiple of the window, so
  // at most one of them can be inside the window at any time; comparing the
  // stored sequence is therefore sufficient and no clearing pass is needed
  // when the window advances. That is what keeps a huge jump O(1).
  return (slot.seq == s) ? &slot : nullptr;
}

void SequenceTracker::mark_seen(KeyState& k, const DomainEvent& e) {
  Slot& slot = k.ring[ring_index(e.sequence, limits_.window)];
  slot.seq = e.sequence;
  slot.core = core_of(e);
}

void SequenceTracker::advance_contiguous(KeyState& k) {
  // Bounded by the window: once the next slot is outside it we can no longer
  // prove anything about continuity.
  while (k.contiguous_observed < k.highest) {
    const Sequence next = k.contiguous_observed + 1;
    if (!within_window(k, next) || seen_slot(k, next) == nullptr) break;
    k.contiguous_observed = next;
  }
  const std::int64_t lowest =
      saturating_sub(k.highest, static_cast<std::int64_t>(limits_.window) - 1);
  if (k.contiguous_observed < lowest - 1) k.stats.continuity_uncertain = true;
}

SequenceTracker::KeyState* SequenceTracker::find_or_create(
    const std::string& dataset_id, const std::string& symbol,
    bool& cap_exceeded) {
  cap_exceeded = false;
  const std::string key = make_key(dataset_id, symbol);
  const auto it = keys_.find(key);
  if (it != keys_.end()) return &it->second;

  // Caps are checked before any allocation. An active key is never evicted to
  // make room: silently dropping live state would turn a resource problem
  // into a data-integrity problem (design 5.1).
  const auto ds = datasets_.find(dataset_id);
  if (ds == datasets_.end() && datasets_.size() >= limits_.max_datasets) {
    ++cap_rejections_;
    cap_exceeded = true;
    return nullptr;
  }
  if (ds != datasets_.end() && ds->second >= limits_.max_symbols_per_dataset) {
    ++cap_rejections_;
    cap_exceeded = true;
    return nullptr;
  }
  if (state_bytes_ + bytes_per_key() > limits_.max_state_bytes) {
    ++cap_rejections_;
    cap_exceeded = true;
    return nullptr;
  }

  KeyState& k = keys_[key];
  k.ring.assign(limits_.window, Slot{});
  state_bytes_ += bytes_per_key();
  datasets_[dataset_id] += 1;
  return &k;
}

SeqResult SequenceTracker::observe(const DomainEvent& e) {
  SeqResult r;
  bool cap_exceeded = false;
  KeyState* kp = find_or_create(e.dataset_id, e.symbol, cap_exceeded);
  if (kp == nullptr) {
    r.klass = SeqClass::kStateCapExceeded;
    r.reason = Reason::kStateCapExceeded;
    return r;
  }
  KeyState& k = *kp;
  r.handle.p_ = &k;
  ++k.stats.events_observed;

  // ---- first event for this key ------------------------------------------
  if (!k.initialized) {
    k.initialized = true;
    k.highest = e.sequence;
    k.first_seen = e.sequence;
    k.stats.first_seen = e.sequence;
    k.contiguous_observed = (e.sequence == 1) ? 1 : 0;
    mark_seen(k, e);
    r.first_event_for_key = true;
    r.klass = SeqClass::kOk;
    if (limits_.expect_start_at_one && e.sequence != 1) {
      r.quality_flags |= quality::kInitialGap | quality::kGap | quality::kDegraded;
      r.reason = Reason::kInitialGap;
      r.missing_slots_observed = saturating_sub(e.sequence, 1);
      k.stats.missing_slots_observed += static_cast<std::uint64_t>(
          std::max<std::int64_t>(0, r.missing_slots_observed));
      ++k.stats.gap_incidents;
      r.klass = SeqClass::kGap;
    } else if (limits_.live_baseline_unverified) {
      // Starting mid-stream is normal on a live feed; what is not acceptable
      // is calling it verified continuity.
      r.quality_flags |= quality::kStartUnverified;
      r.reason = Reason::kStartUnverified;
    }
    k.stats.highest_seen_valid = k.highest;
    k.stats.contiguous_observed_valid = k.contiguous_observed;
    r.continuity_uncertain = k.stats.continuity_uncertain;
    return r;
  }

  // ---- forward progress ---------------------------------------------------
  if (e.sequence > k.highest) {
    const std::int64_t delta = saturating_sub(e.sequence, k.highest);
    if (delta > 1) {
      r.klass = SeqClass::kGap;
      r.reason = Reason::kSequenceGap;
      r.quality_flags |= quality::kGap | quality::kDegraded;
      r.missing_slots_observed = delta - 1;
      ++k.stats.gap_incidents;
      k.stats.missing_slots_observed +=
          static_cast<std::uint64_t>(r.missing_slots_observed);
      if (delta >= static_cast<std::int64_t>(limits_.window)) {
        // Everything previously tracked has fallen out of the window.
        r.quality_flags |= quality::kHugeGap;
        k.stats.continuity_uncertain = true;
      }
    } else {
      r.klass = SeqClass::kOk;
    }
    k.highest = e.sequence;
    mark_seen(k, e);
    advance_contiguous(k);
    k.stats.highest_seen_valid = k.highest;
    k.stats.contiguous_observed_valid = k.contiguous_observed;
    r.continuity_uncertain = k.stats.continuity_uncertain;
    return r;
  }

  // ---- at or below the current high --------------------------------------
  if (within_window(k, e.sequence)) {
    if (const Slot* prev = seen_slot(k, e.sequence); prev != nullptr) {
      // Typed comparison, not a hash: a 64/128-bit digest can collide and the
      // difference between an exact and a conflicting duplicate is an
      // integrity decision (design section 5).
      if (prev->core == core_of(e)) {
        r.klass = SeqClass::kDuplicateExact;
        r.reason = Reason::kDuplicateExact;
        r.quality_flags |= quality::kDuplicateExact;
        ++k.stats.duplicate_exact;
      } else {
        r.klass = SeqClass::kDuplicateConflict;
        r.reason = Reason::kDuplicateConflict;
        r.quality_flags |= quality::kDuplicateConflict | quality::kDegraded;
        ++k.stats.duplicate_conflict;
        // The first value observed at this sequence is kept. Overwriting it
        // would let a later sender silently rewrite history.
      }
    } else {
      r.klass = SeqClass::kReorderLate;
      r.reason = Reason::kReorderLate;
      r.quality_flags |= quality::kReorderLate;
      ++k.stats.reorder_late;
      // Observed for continuity accounting only; strict mode will not publish
      // it, so published continuity stays holed.
      mark_seen(k, e);
      advance_contiguous(k);
    }
  } else {
    // Below the window. We saw this slot once, but we can no longer prove
    // what was in it, so calling it a duplicate would be a guess.
    r.klass = SeqClass::kTooOldUnverifiable;
    r.reason = Reason::kTooOldUnverifiable;
    r.quality_flags |= quality::kTooOldUnverifiable;
    ++k.stats.too_old;

    if (e.sequence == k.first_seen) {
      // The baseline arriving again inside the same dataset_id looks like a
      // sender restart. That is a suspicion, not a verified fact -- the
      // adapter must not promote it to "new trading session" on its own
      // authority (design 4.3) -- so it travels as a flag on top of the
      // honest classification, and continuity is marked uncertain.
      r.reason = Reason::kSequenceRollback;
      r.quality_flags |= quality::kSequenceRollback | quality::kDegraded;
      k.stats.continuity_uncertain = true;
    }
  }

  k.stats.highest_seen_valid = k.highest;
  k.stats.contiguous_observed_valid = k.contiguous_observed;
  r.continuity_uncertain = k.stats.continuity_uncertain;
  return r;
}

void SequenceTracker::note_published(KeyHandle h, Sequence seq) {
  if (!h.valid()) return;
  KeyState& k = *static_cast<KeyState*>(h.p_);
  if (seq == k.contiguous_published + 1) {
    k.contiguous_published = seq;
    k.stats.contiguous_published = seq;
  }
}

void SequenceTracker::note_published(const std::string& dataset_id,
                                     const std::string& symbol, Sequence seq) {
  const auto it = keys_.find(make_key(dataset_id, symbol));
  if (it == keys_.end()) return;
  KeyState& k = it->second;
  if (seq == k.contiguous_published + 1) {
    k.contiguous_published = seq;
    k.stats.contiguous_published = seq;
  }
  // Out-of-order publication never advances published continuity. It can only
  // be healed by an explicit new verified epoch, not by a late arrival.
}

const SequenceTracker::KeyStats* SequenceTracker::stats(
    const std::string& dataset_id, const std::string& symbol) const {
  const auto it = keys_.find(make_key(dataset_id, symbol));
  return it == keys_.end() ? nullptr : &it->second.stats;
}

std::vector<std::pair<std::string, SequenceTracker::KeyStats>>
SequenceTracker::all_stats() const {
  std::vector<std::pair<std::string, KeyStats>> out;
  out.reserve(keys_.size());
  for (const auto& kv : keys_) {
    std::string pretty = kv.first;
    std::replace(pretty.begin(), pretty.end(), kKeySep, '/');
    out.emplace_back(std::move(pretty), kv.second.stats);
  }
  return out;
}

}  // namespace md
