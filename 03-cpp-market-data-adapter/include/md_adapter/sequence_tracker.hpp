// Per-(dataset, symbol) sequence state.
//
// Everything here is bounded and data-independent. A jump from sequence 1 to
// INT64_MAX must cost the same as a jump from 1 to 2: no loop ever runs over a
// gap width, and no allocation is ever sized from a wire value.
//
// Two continuity notions are tracked separately and must not be conflated
// (ADR-06):
//   * observed_valid  -- the highest N such that 1..N were all seen as
//                        business-valid events, in any arrival order;
//   * published       -- the highest N such that 1..N were all actually
//                        forwarded to the sink. Strict mode suppresses late
//                        arrivals, so a filled observation gap does not heal
//                        published continuity.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "md_adapter/domain_event.hpp"
#include "md_adapter/status.hpp"

namespace md {

enum class SeqClass : std::uint8_t {
  kOk,                   // in order, exactly one past the previous high
  kGap,                  // in order but skipped slots
  kDuplicateExact,       // same sequence, identical typed core
  kDuplicateConflict,    // same sequence, different typed core
  kReorderLate,          // fills an observed hole inside the window
  kTooOldUnverifiable,   // below the window; cannot be judged
  kStateCapExceeded,     // no state could be allocated for this key
};
std::string_view to_string(SeqClass c) noexcept;

// Opaque, stable reference to one key's state. std::map nodes are not
// invalidated by later insertions, so a handle stays valid for the life of
// the tracker.
class KeyHandle {
 public:
  bool valid() const noexcept { return p_ != nullptr; }

 private:
  friend class SequenceTracker;
  void* p_ = nullptr;
};

struct SeqResult {
  KeyHandle handle;
  SeqClass klass = SeqClass::kOk;
  std::uint32_t quality_flags = quality::kNone;
  Reason reason = Reason::kNone;
  // Newly observed missing slots, saturating. This is a count of logical
  // slots never seen -- not a count of lost TCP bytes.
  std::int64_t missing_slots_observed = 0;
  bool first_event_for_key = false;
  bool continuity_uncertain = false;
};

struct SequenceLimits {
  std::uint32_t window = 4096;
  std::size_t max_symbols_per_dataset = 256;
  std::size_t max_datasets = 4;
  std::uint64_t max_state_bytes = 64ull * 1024 * 1024;
  // Offline verified fixtures are known to start at sequence 1; a live feed
  // has no such guarantee and gets START_UNVERIFIED instead.
  bool expect_start_at_one = false;
  bool live_baseline_unverified = true;
};

class SequenceTracker {
 public:
  explicit SequenceTracker(SequenceLimits limits);

  // Call only with structurally AND business valid events (design section 5:
  // an invalid event must never update seen-valid state).
  SeqResult observe(const DomainEvent& e);

  // Records that an event was actually forwarded, advancing published
  // continuity only while it stays in order.
  void note_published(const std::string& dataset_id, const std::string& symbol,
                      Sequence seq);
  // Handle form: used on the hot path so confirming a delivery costs no map
  // lookup and no string copy.
  void note_published(KeyHandle h, Sequence seq);

  struct KeyStats {
    Sequence highest_seen_valid = 0;
    Sequence contiguous_observed_valid = 0;
    Sequence contiguous_published = 0;
    Sequence first_seen = 0;
    std::uint64_t events_observed = 0;
    std::uint64_t gap_incidents = 0;
    std::uint64_t missing_slots_observed = 0;
    std::uint64_t duplicate_exact = 0;
    std::uint64_t duplicate_conflict = 0;
    std::uint64_t reorder_late = 0;
    std::uint64_t too_old = 0;
    bool continuity_uncertain = false;
  };

  const KeyStats* stats(const std::string& dataset_id,
                        const std::string& symbol) const;
  std::vector<std::pair<std::string, KeyStats>> all_stats() const;

  std::uint64_t state_bytes() const noexcept { return state_bytes_; }
  std::size_t key_count() const noexcept { return keys_.size(); }
  std::size_t dataset_count() const noexcept { return datasets_.size(); }
  std::uint64_t state_cap_rejections() const noexcept { return cap_rejections_; }

  // Bytes one key costs. Exposed so capacity planning uses the real number
  // rather than the per-symbol bitmap figure, which omits the typed cores.
  std::uint64_t bytes_per_key() const noexcept;

 private:
  // seq == 0 means "slot empty"; business validation guarantees seq > 0.
  struct Slot {
    Sequence seq = 0;
    TypedCore core;
  };

  struct KeyState {
    Sequence highest = 0;
    Sequence contiguous_observed = 0;
    Sequence contiguous_published = 0;
    Sequence first_seen = 0;
    bool initialized = false;
    std::vector<Slot> ring;
    KeyStats stats;
  };

  KeyState* find_or_create(const std::string& dataset_id,
                           const std::string& symbol, bool& cap_exceeded);

  bool within_window(const KeyState& k, Sequence s) const noexcept;
  const Slot* seen_slot(const KeyState& k, Sequence s) const noexcept;
  void mark_seen(KeyState& k, const DomainEvent& e);
  void advance_contiguous(KeyState& k);

  SequenceLimits limits_;
  std::map<std::string, KeyState> keys_;  // key = dataset_id + '\x1f' + symbol
  std::map<std::string, std::size_t> datasets_;  // dataset_id -> symbol count
  std::uint64_t state_bytes_ = 0;
  std::uint64_t cap_rejections_ = 0;
};

}  // namespace md
