// The Project 03 handoff contract as Project 04 consumes it.
//
// Project 03 is still design-only, so this header is Project 04's half of the
// agreed boundary, written from
// PROJECT_03_CPP_FX_CFD_Market_Data_Adapter_System_Design.md as summarised in
// section 0.1 of the Project 04 design: an OutputEvent is a *typed*
// MarketDataEvent plus arrival_index, quality_flags, feed_state and provenance.
// The stand-in producer of these values lives in src/source/, is named
// mock_p03_classifier, and is never described as the real adapter.
//
// Two decisions in here carry most of the correctness weight:
//
//   * Every schema field is std::optional, mirroring proto3 optional. Collapsing
//     presence into a sentinel (0, "", -1) would make case D-03 untestable:
//     "sequence absent" and "sequence == 0" must stay different facts all the
//     way to the publish gate.
//   * The classification arrives from upstream and is never recomputed here to
//     something more permissive. Section 3.5 makes the publish gate a one-way
//     filter: it may suppress, it may never promote an event to market_valid,
//     and it may never repair a price or a sequence.
#ifndef P04_HANDOFF_OUTPUT_EVENT_H_
#define P04_HANDOFF_OUTPUT_EVENT_H_

#include <cstdint>
#include <optional>
#include <string>

namespace p04 {

// Which arm of the protobuf oneof payload is populated. There is no "unset"
// member: an event whose oneof is unset carries std::nullopt instead, because
// case D-04 requires an unset oneof to be rejected rather than routed to a
// fallback topic.
enum class EventKind { kQuote, kTick };

const char* ToString(EventKind kind);

// The upstream verdict. Only kMarketValid is eligible for a market topic
// (section 3.5); everything else is suppressed with a reason that survives into
// the run report, because "we dropped it" without a reason is indistinguishable
// from silent loss.
enum class Classification {
  kMarketValid,
  kStructuralInvalid,    // wire/structural failure upstream: no trusted event
  kBusinessReject,       // e.g. inverted spread, impossible price
  kDuplicateSuppressed,  // identity already seen upstream
  kStaleSuppressed,      // freshness policy rejected it
  kFaultTestOnly,        // deliberately injected anomaly, needs verified manifest
  kUnclassified,         // upstream has not decided yet; must not be published
};

const char* ToString(Classification classification);

// Feed liveness as the adapter saw it. kUnknown is explicitly not publishable
// (case A-06): an event that arrived while the adapter could not vouch for feed
// state is not market-valid, it is unclassified in disguise.
enum class FeedState { kUnknown, kWarmup, kLive, kStale, kDisconnected };

const char* ToString(FeedState state);

// Quality flags are a bitset owned by Project 03. Project 04 forwards them into
// the audit lane and the ledger but never into the Kafka value: section 3.4
// forbids widening the pinned schema or the headers to carry unbounded
// provenance.
enum QualityFlag : std::uint32_t {
  kQualityNone = 0,
  kQualitySequenceGapBefore = 1u << 0,
  kQualityTimestampNonMonotonic = 1u << 1,
  kQualityPriceBoundClamped = 1u << 2,
  kQualityFaultInjected = 1u << 3,
  kQualityReplayed = 1u << 4,
};

// The payload arms, with presence per field so a quote missing ask_ticks is
// rejected rather than silently published as zero.
struct QuoteBody {
  std::optional<std::int64_t> bid_ticks;
  std::optional<std::int64_t> ask_ticks;
};

struct TickBody {
  std::optional<std::int64_t> price_ticks;
};

// The typed MarketDataEvent. Field names and semantics come from the pinned
// schema in schemas/market_data.proto and must not drift from it.
struct TypedEvent {
  std::optional<std::string> dataset_id;
  std::optional<std::string> symbol;
  std::optional<std::int64_t> sequence;
  std::optional<std::int64_t> event_time_ns;
  std::optional<std::int64_t> price_scale;
  std::optional<std::string> source_schema_version;

  // std::nullopt means the oneof is unset on the wire.
  std::optional<EventKind> kind;
  QuoteBody quote;
  TickBody tick;
};

// The logical source identity, (dataset_id, symbol, sequence), as section 3.3
// defines it. kind is carried alongside rather than inside, because the same
// logical identity is what reconciliation joins on while kind only says which
// topic it should have gone to.
//
// This is NOT an attempt id. Section 10.1 oracle rule 3 is explicit: an attempt
// id must be unique per enqueue attempt, so one identity can own several
// attempts after an application-level resend. Conflating the two is how a
// duplicate becomes invisible.
struct EventIdentity {
  std::string dataset_id;
  std::string symbol;
  std::int64_t sequence = 0;

  bool operator==(const EventIdentity& other) const {
    return sequence == other.sequence && symbol == other.symbol &&
           dataset_id == other.dataset_id;
  }
  bool operator<(const EventIdentity& other) const {
    if (dataset_id != other.dataset_id) return dataset_id < other.dataset_id;
    if (symbol != other.symbol) return symbol < other.symbol;
    return sequence < other.sequence;
  }
  std::string ToString() const;
};

struct EventIdentityHash {
  std::size_t operator()(const EventIdentity& id) const;
};

// Where the event came from, so a record on Kafka can be traced back to a
// dataset and a run without putting any of this in the protobuf value.
struct Provenance {
  std::string source_dataset_id;      // from the P01 run report
  std::string source_schema_version;  // "sim.v1"
  std::string run_id;                 // this P04 run
  std::string descriptor_sha256;      // pinned P02 descriptor digest
  std::uint64_t arrival_index = 0;    // monotonic per P04 run, assigned upstream
};

// One unit of work crossing the P03 to P04 boundary.
//
// raw_source_payload is optional and only populated when the harness wants
// byte-level reconciliation against the source file. Section 3.1 warns that
// byte-identity across runtimes is not an oracle; semantic field equality is.
// Keeping the raw bytes is for diagnosis, not for asserting equality.
struct OutputEvent {
  TypedEvent event;
  Classification classification = Classification::kUnclassified;
  FeedState feed_state = FeedState::kUnknown;
  std::uint32_t quality_flags = kQualityNone;
  Provenance provenance;
  std::string raw_source_payload;

  // Best-effort identity for logging and reconciliation. Fields that are absent
  // become empty/zero here, which is safe because an event with an absent
  // identity field can never reach a market topic: the route validator rejects
  // it first (cases D-03, B-02).
  EventIdentity Identity() const;
};

}  // namespace p04

#endif  // P04_HANDOFF_OUTPUT_EVENT_H_
