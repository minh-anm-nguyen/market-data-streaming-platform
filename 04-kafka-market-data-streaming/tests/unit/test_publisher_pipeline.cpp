// The publish pipeline end to end against the deterministic fake transport
// (section 4.1 state machine, section 10.4 invariants, cases A-01, A-07, D-10,
// E-01 to E-04, E-06, E-07, F-01 to F-04, I-*, Q-*).
//
// Everything here runs with no broker, no Docker and no sleeping. That is the
// point of the transport seam: the state machine, the ack accounting and the
// bounded queues are exercised deterministically, and the same pipeline code is
// then run against a real cluster by tests/integration. A pass here is reported
// as a pass against the fake, never as a pass for a case whose oracle requires
// a broker.
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "app/publisher.h"
#include "ops/metrics.h"
#include "producer/fake_transport.h"
#include "routing/partition_policy.h"
#include "serialization/marketdata_codec.h"

namespace p04 {
namespace {

// One assembled pipeline, so each test states only what it varies.
struct Harness {
  explicit Harness(FakeTransportConfig transport_config = {},
                   HandoffConfig handoff_config = {},
                   PublisherConfig publisher_config = {},
                   PublishGateConfig gate_config = {})
      : transport(std::make_unique<FakeTransport>(transport_config)),
        routes(MakeRouteConfig()),
        gate(gate_config),
        handoff(handoff_config),
        metrics("md.ticks.v1", "md.quotes.v1", 8),
        publisher(transport.get(), &routes, &gate, &handoff, &ledger, &metrics,
                  publisher_config) {
    publisher.Install();
  }

  static RouteConfig MakeRouteConfig() {
    RouteConfig config;
    config.ticks_topic = "md.ticks.v1";
    config.quotes_topic = "md.quotes.v1";
    config.expected_source_schema = "sim.v1";
    config.partition_count = 8;
    config.run_id = "pipeline-test";
    return config;
  }

  std::unique_ptr<FakeTransport> transport;
  RouteTable routes;
  PublishGate gate;
  BoundedHandoff handoff;
  PublishLedger ledger;
  Metrics metrics;
  Publisher publisher;
};

OutputEvent MakeEvent(EventKind kind, const std::string& symbol, std::int64_t sequence,
                      Classification classification = Classification::kMarketValid) {
  OutputEvent out;
  out.classification = classification;
  out.feed_state = FeedState::kLive;
  out.event.dataset_id = "ds-pipeline";
  out.event.symbol = symbol;
  out.event.sequence = sequence;
  out.event.event_time_ns = 1793322000000000000LL + sequence;
  out.event.price_scale = 100000;
  out.event.source_schema_version = "sim.v1";
  out.event.kind = kind;
  if (kind == EventKind::kQuote) {
    out.event.quote.bid_ticks = 108345 + sequence;
    out.event.quote.ask_ticks = 108348 + sequence;
  } else {
    out.event.tick.price_ticks = 108347 + sequence;
  }
  return out;
}

HandoffConfig SmallHandoff(std::size_t records, std::size_t bytes = 1u << 20) {
  HandoffConfig config;
  config.max_records = records;
  config.max_bytes = bytes;
  config.high_watermark_percent = 80;
  config.low_watermark_percent = 40;
  config.stall_timeout_ms = 40;
  return config;
}

// The happy path, and the one thing it must not claim: after Offer the record
// is queued, not delivered.
TEST(PublisherPipeline, OfferQueuesAndOnlyPollDelivers) {
  Harness h;
  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1)),
            OfferResult::kAcceptedForAsyncDelivery);

  LedgerCounters counters = h.ledger.Counters();
  EXPECT_EQ(counters.queued_lib, 1u);
  EXPECT_EQ(counters.acked, 0u) << "produce success was treated as an ACK";
  EXPECT_EQ(counters.pending_in_library, 1u);
  EXPECT_EQ(h.handoff.Stats().records, 1u) << "the budget must still be held";

  EXPECT_EQ(h.publisher.Poll(0), 1);
  counters = h.ledger.Counters();
  EXPECT_EQ(counters.acked, 1u);
  EXPECT_EQ(counters.pending_in_library, 0u);
  EXPECT_EQ(h.handoff.Stats().records, 0u) << "the budget must be released on report";
  EXPECT_FALSE(h.publisher.contract_violated());
}

TEST(PublisherPipeline, RoutesQuotesAndTicksToTheirTopicsWithSymbolKeys) {
  Harness h;
  for (int i = 1; i <= 5; ++i) {
    ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", i)),
              OfferResult::kAcceptedForAsyncDelivery);
    ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kTick, "GBPUSD", i)),
              OfferResult::kAcceptedForAsyncDelivery);
  }
  h.publisher.Poll(0);

  int quotes = 0;
  int ticks = 0;
  for (const auto& record : h.transport->log()) {
    TypedEvent decoded;
    CodecError error = CodecError::kNone;
    ASSERT_TRUE(Decode(record.value, &decoded, &error)) << ToString(error);
    ASSERT_TRUE(decoded.kind.has_value());
    if (record.topic == "md.quotes.v1") {
      ++quotes;
      EXPECT_EQ(*decoded.kind, EventKind::kQuote);
      EXPECT_EQ(record.key, "EURUSD");
    } else if (record.topic == "md.ticks.v1") {
      ++ticks;
      EXPECT_EQ(*decoded.kind, EventKind::kTick);
      EXPECT_EQ(record.key, "GBPUSD");
    } else {
      FAIL() << "record landed on an unexpected topic: " << record.topic;
    }
    ASSERT_TRUE(decoded.symbol.has_value());
    EXPECT_EQ(*decoded.symbol, record.key);
  }
  EXPECT_EQ(quotes, 5);
  EXPECT_EQ(ticks, 5);
}

// Per topic-partition ordering for one key, which is the only ordering this
// project claims (section 3.3).
TEST(PublisherPipeline, OffsetsAndSequencesIncreaseWithinOneTopicPartition) {
  Harness h;
  for (int i = 1; i <= 50; ++i) {
    ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kTick, "EURUSD", i)),
              OfferResult::kAcceptedForAsyncDelivery);
    h.publisher.Poll(0);
  }
  const std::int32_t partition = SelectPartition("EURUSD", 8);
  const auto records = h.transport->RecordsFor("md.ticks.v1", partition);
  ASSERT_EQ(records.size(), 50u);

  std::int64_t previous_offset = -1;
  std::int64_t previous_sequence = -1;
  for (const auto& record : records) {
    EXPECT_GT(record.offset, previous_offset);
    previous_offset = record.offset;
    TypedEvent decoded;
    CodecError error = CodecError::kNone;
    ASSERT_TRUE(Decode(record.value, &decoded, &error));
    ASSERT_TRUE(decoded.sequence.has_value());
    EXPECT_GT(*decoded.sequence, previous_sequence)
        << "sequences must be strictly increasing within one topic-partition";
    previous_sequence = *decoded.sequence;
  }
}

// Case A-01: a suppressed event is counted and never reaches the log.
TEST(PublisherPipeline, SuppressedEventsNeverReachTheLog) {
  Harness h;
  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1,
                                        Classification::kBusinessReject)),
            OfferResult::kPolicySuppressed);
  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 2,
                                        Classification::kDuplicateSuppressed)),
            OfferResult::kPolicySuppressed);
  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 3,
                                        Classification::kStructuralInvalid)),
            OfferResult::kPolicySuppressed);
  h.publisher.Poll(0);

  EXPECT_TRUE(h.transport->log().empty());
  const LedgerCounters counters = h.ledger.Counters();
  EXPECT_EQ(counters.upstream_accepted, 3u);
  EXPECT_EQ(counters.policy_suppressed, 3u);
  EXPECT_EQ(counters.queued_app, 0u);
  std::string error;
  EXPECT_TRUE(h.ledger.CheckConservation(&error)) << error;
}

// A contract failure is refused before the transport sees anything.
TEST(PublisherPipeline, RouteRejectionsNeverReachTheTransport) {
  Harness h;
  OutputEvent bad_symbol = MakeEvent(EventKind::kQuote, "eurusd", 1);
  EXPECT_EQ(h.publisher.Offer(bad_symbol), OfferResult::kRouteRejected);

  OutputEvent no_sequence = MakeEvent(EventKind::kQuote, "EURUSD", 1);
  no_sequence.event.sequence.reset();
  EXPECT_EQ(h.publisher.Offer(no_sequence), OfferResult::kRouteRejected);

  OutputEvent no_payload = MakeEvent(EventKind::kQuote, "EURUSD", 2);
  no_payload.event.kind.reset();
  EXPECT_EQ(h.publisher.Offer(no_payload), OfferResult::kRouteRejected);

  EXPECT_EQ(h.transport->produce_calls(), 0u);
  const LedgerCounters counters = h.ledger.Counters();
  EXPECT_EQ(counters.route_rejected, 3u);
  EXPECT_EQ(counters.queued_app, 0u);
  std::string error;
  EXPECT_TRUE(h.ledger.CheckConservation(&error)) << error;
}

// Case F-01: produce succeeds while nothing can be delivered. The ledger must
// say QUEUED_LIB, and the drain must turn it into UNKNOWN, never ACKED.
TEST(PublisherPipeline, NeverReportedRecordsBecomeUnknownAtTheDrainDeadline) {
  FakeTransportConfig transport_config;
  for (std::uint64_t i = 1; i <= 3; ++i) transport_config.faults[i] = FakeFault::kNeverReport;
  Harness h(transport_config);

  for (int i = 1; i <= 3; ++i) {
    ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", i)),
              OfferResult::kAcceptedForAsyncDelivery);
  }
  EXPECT_EQ(h.publisher.Poll(0), 0) << "no reports should be available";
  EXPECT_EQ(h.ledger.Counters().acked, 0u);

  const DrainResult drain = h.publisher.Drain(10);
  EXPECT_FALSE(drain.complete);
  EXPECT_EQ(drain.finalized_as_unknown, 3u);

  const LedgerCounters counters = h.ledger.Counters();
  EXPECT_EQ(counters.unknown, 3u);
  EXPECT_EQ(counters.acked, 0u);
  EXPECT_EQ(counters.pending_in_library, 0u);
  std::string error;
  EXPECT_TRUE(h.ledger.CheckConservation(&error)) << error;
}

// Case E-06: the record was appended and the acknowledgement was lost. The
// ledger says UNKNOWN while the log holds the record. Only an independent read
// can resolve it, which is exactly what section 4.2 describes.
TEST(PublisherPipeline, AckLossAfterAppendIsUnknownNotLost) {
  FakeTransportConfig transport_config;
  transport_config.faults[1] = FakeFault::kAckDroppedAfterAppend;
  Harness h(transport_config);

  ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1)),
            OfferResult::kAcceptedForAsyncDelivery);
  h.publisher.Poll(0);

  const LedgerCounters counters = h.ledger.Counters();
  EXPECT_EQ(counters.unknown, 1u);
  EXPECT_EQ(counters.acked, 0u);
  EXPECT_EQ(counters.failed_definite, 0u)
      << "an appended record must never be reported as a definite failure";

  // The record is on the fake broker even though the producer does not know it.
  ASSERT_EQ(h.transport->log().size(), 1u);
  EXPECT_TRUE(h.transport->log().front().ack_withheld);

  AttemptRecord record;
  ASSERT_TRUE(h.ledger.Lookup(1, &record));
  EXPECT_EQ(record.broker_offset, -1)
      << "the producer must not record an offset it was never told";
}

// A provable pre-append failure is the only shape allowed to be definite.
TEST(PublisherPipeline, PreAppendFailureIsDefiniteAndLeavesNoRecord) {
  FakeTransportConfig transport_config;
  transport_config.faults[1] = FakeFault::kFailBeforeAppend;
  Harness h(transport_config);

  ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1)),
            OfferResult::kAcceptedForAsyncDelivery);
  h.publisher.Poll(0);

  EXPECT_EQ(h.ledger.Counters().failed_definite, 1u);
  EXPECT_EQ(h.ledger.Counters().unknown, 0u);
  EXPECT_TRUE(h.transport->log().empty());
}

// Case I-*: the app queue fills, the offer reports backpressure, and nothing is
// dropped. With blocking off, the caller sees kBackpressured.
TEST(PublisherPipeline, FullAppQueueReportsBackpressureAndDropsNothing) {
  FakeTransportConfig transport_config;
  for (std::uint64_t i = 1; i <= 100; ++i) {
    transport_config.faults[i] = FakeFault::kNeverReport;  // nothing ever drains
  }
  PublisherConfig publisher_config;
  publisher_config.block_on_backpressure = false;
  Harness h(transport_config, SmallHandoff(4), publisher_config);

  int accepted = 0;
  int backpressured = 0;
  for (int i = 1; i <= 20; ++i) {
    const OfferResult result = h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", i));
    if (result == OfferResult::kAcceptedForAsyncDelivery) ++accepted;
    if (result == OfferResult::kBackpressured) ++backpressured;
  }
  EXPECT_EQ(accepted, 4) << "the queue cap must bind";
  EXPECT_EQ(backpressured, 16);
  EXPECT_LE(h.handoff.Stats().records, 4u);
  EXPECT_TRUE(h.handoff.ShouldPauseUpstream());

  // Nothing was dropped silently: every offer is accounted for.
  const LedgerCounters counters = h.ledger.Counters();
  EXPECT_EQ(counters.upstream_accepted, 20u);
  EXPECT_EQ(counters.queued_app, 20u);
  EXPECT_EQ(counters.queued_lib, 4u);
  EXPECT_EQ(counters.enqueue_failed, 16u);
  std::string error;
  EXPECT_TRUE(h.ledger.CheckConservation(&error)) << error;
}

// With blocking on and nothing draining, the stall deadline ends the attempt
// rather than hanging (section 4.4).
TEST(PublisherPipeline, BlockingBackpressureHitsTheStallDeadline) {
  FakeTransportConfig transport_config;
  for (std::uint64_t i = 1; i <= 100; ++i) {
    transport_config.faults[i] = FakeFault::kNeverReport;
  }
  PublisherConfig publisher_config;
  publisher_config.block_on_backpressure = true;
  Harness h(transport_config, SmallHandoff(2), publisher_config);

  ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1)),
            OfferResult::kAcceptedForAsyncDelivery);
  ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 2)),
            OfferResult::kAcceptedForAsyncDelivery);
  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 3)),
            OfferResult::kBackpressureTimeout);
}

// The client queue refusing a record is backpressure too, and the budget must
// be given back so the next offer is not refused for a record that never left.
TEST(PublisherPipeline, ClientQueueFullReleasesTheAppBudget) {
  FakeTransportConfig transport_config;
  transport_config.faults[1] = FakeFault::kQueueFull;
  Harness h(transport_config, SmallHandoff(8));

  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1)),
            OfferResult::kBackpressured);
  EXPECT_EQ(h.handoff.Stats().records, 0u)
      << "a refused produce must not leave its reservation behind";
  EXPECT_EQ(h.ledger.Counters().enqueue_failed, 1u);
  EXPECT_EQ(h.ledger.Counters().queued_lib, 0u);

  // The next offer must still work.
  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 2)),
            OfferResult::kAcceptedForAsyncDelivery);
}

TEST(PublisherPipeline, FatalProducerErrorStopsTheRun) {
  FakeTransportConfig transport_config;
  transport_config.faults[1] = FakeFault::kFatal;
  Harness h(transport_config);
  EXPECT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1)),
            OfferResult::kFatal);
  EXPECT_TRUE(h.publisher.contract_violated());
  EXPECT_NE(h.publisher.contract_error().find("fatal"), std::string::npos)
      << h.publisher.contract_error();
}

// Case E-07: an application-level resend creates two records for one identity.
// The pipeline must make that visible, not hide it behind producer idempotence.
TEST(PublisherPipeline, DuplicateEveryEventProducesTwoRecordsPerIdentity) {
  PublisherConfig publisher_config;
  publisher_config.duplicate_every_event = true;
  Harness h({}, HandoffConfig{}, publisher_config);

  ASSERT_EQ(h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", 1)),
            OfferResult::kAcceptedForAsyncDelivery);
  h.publisher.Poll(0);

  EXPECT_EQ(h.transport->log().size(), 2u)
      << "producer idempotence does not dedupe two separate produce calls";
  EXPECT_EQ(h.ledger.Counters().app_level_resends, 1u);
  ASSERT_EQ(h.ledger.ResentIdentities().size(), 1u);
  EXPECT_EQ(h.ledger.Counters().acked, 2u);
}

// Cases E-01 to E-04: the pinned producer policy is validated and a conflicting
// configuration is refused rather than quietly degraded.
TEST(PublisherPipeline, RefusesEveryIncompatibleProducerConfig) {
  struct Case {
    const char* name;
    FakeTransportConfig config;
  };
  std::vector<Case> cases;
  {
    FakeTransportConfig config;
    config.enable_idempotence = false;
    cases.push_back({"idempotence off", config});
  }
  {
    FakeTransportConfig config;
    config.acks = "1";
    cases.push_back({"acks=1", config});
  }
  {
    FakeTransportConfig config;
    config.max_in_flight_requests_per_connection = 10;
    cases.push_back({"in-flight 10", config});
  }
  {
    FakeTransportConfig config;
    config.retries = 0;
    cases.push_back({"retries 0", config});
  }
  {
    FakeTransportConfig config;
    config.message_timeout_ms = 0;
    cases.push_back({"no message timeout", config});
  }
  for (const auto& test_case : cases) {
    FakeTransport transport(test_case.config);
    std::string error;
    EXPECT_FALSE(transport.ValidateEffectiveConfig(&error)) << test_case.name;
    EXPECT_FALSE(error.empty()) << test_case.name;
  }

  // The pinned combination must pass, or the guard is simply refusing
  // everything.
  FakeTransport good({});
  std::string error;
  EXPECT_TRUE(good.ValidateEffectiveConfig(&error)) << error;
}

// Case D-10 through the whole pipeline: an oversized value is refused before
// anything is reserved or queued.
TEST(PublisherPipeline, OversizedValueIsRefusedBeforeAnyReservation) {
  Harness h;
  OutputEvent event = MakeEvent(EventKind::kQuote, "EURUSD", 1);
  // A dataset id long enough to push the serialized value past 4096 bytes.
  event.event.dataset_id = std::string(5000, 'd');
  EXPECT_EQ(h.publisher.Offer(event), OfferResult::kRouteRejected);
  EXPECT_EQ(h.transport->produce_calls(), 0u);
  EXPECT_EQ(h.handoff.Stats().records, 0u);
}

// A longer mixed run with faults interleaved: the conservation equations and
// the queue caps must hold throughout, which is the section 10.4 invariant set.
TEST(PublisherPipeline, InvariantsHoldAcrossAMixedFaultyRun) {
  FakeTransportConfig transport_config;
  transport_config.faults[5] = FakeFault::kAckDroppedAfterAppend;
  transport_config.faults[11] = FakeFault::kFailBeforeAppend;
  transport_config.faults[17] = FakeFault::kTimeoutNoAppend;
  transport_config.faults[23] = FakeFault::kQueueFull;
  transport_config.faults[29] = FakeFault::kNeverReport;
  Harness h(transport_config, SmallHandoff(16));

  const std::vector<std::string> symbols = {"EURUSD", "GBPUSD", "USDJPY"};
  for (int i = 1; i <= 60; ++i) {
    const auto kind = (i % 3 == 0) ? EventKind::kTick : EventKind::kQuote;
    const auto& symbol = symbols[static_cast<std::size_t>(i) % symbols.size()];
    const auto classification =
        (i % 10 == 0) ? Classification::kBusinessReject : Classification::kMarketValid;
    h.publisher.Offer(MakeEvent(kind, symbol, i, classification));
    h.publisher.Poll(0);
    ASSERT_LE(h.handoff.Stats().records, 16u) << "at offer " << i;
    ASSERT_LE(h.handoff.Stats().bytes, h.handoff.config().max_bytes) << "at offer " << i;
  }

  const DrainResult drain = h.publisher.Drain(10);
  (void)drain;
  std::string error;
  EXPECT_TRUE(h.ledger.CheckConservation(&error)) << error;

  const LedgerCounters counters = h.ledger.Counters();
  EXPECT_EQ(counters.upstream_accepted, 60u);
  EXPECT_EQ(counters.policy_suppressed, 6u) << "every tenth event was suppressed";
  EXPECT_EQ(counters.pending_in_library, 0u) << "the drain must leave nothing pending";
  EXPECT_EQ(counters.double_finalize_refused, 0u);
  EXPECT_GT(counters.acked, 40u) << "most records should have been acknowledged";
  EXPECT_GE(counters.unknown, 2u) << "the injected ambiguities must be visible";
  EXPECT_GE(counters.failed_definite, 1u);
  EXPECT_EQ(h.handoff.Stats().records, 0u) << "the budget must be fully released";
}

// The metrics exporter must stay bounded and must agree with the ledger.
TEST(PublisherPipeline, MetricsAgreeWithTheLedgerAndStayBounded) {
  Harness h;
  for (int i = 1; i <= 30; ++i) {
    h.publisher.Offer(MakeEvent(EventKind::kQuote, "EURUSD", i));
    h.publisher.Poll(0);
  }
  EXPECT_EQ(h.metrics.acked_total(), h.ledger.Counters().acked);
  EXPECT_EQ(h.metrics.enqueued_total(), h.ledger.Counters().queued_lib);
  EXPECT_EQ(h.metrics.partition_overflow_count(), 0u);

  const std::string exposition = h.metrics.RenderPrometheus();
  EXPECT_NE(exposition.find("p04_delivery_acked_total"), std::string::npos);
  EXPECT_NE(exposition.find("p04_app_queue_records"), std::string::npos);
  EXPECT_NE(exposition.find("p04_broker_ack_latency_seconds_bucket"), std::string::npos);
  // Section 8.1 forbids a dataset_id label. Checking the rendered text is the
  // only way to be sure none crept in.
  EXPECT_EQ(exposition.find("ds-pipeline"), std::string::npos)
      << "a dataset id leaked into the metric labels";
}

TEST(PublisherPipeline, EveryOfferResultHasAName) {
  for (const auto result : {OfferResult::kAcceptedForAsyncDelivery,
                            OfferResult::kPolicySuppressed,
                            OfferResult::kRoutedToAuditLane,
                            OfferResult::kRouteRejected,
                            OfferResult::kBackpressured,
                            OfferResult::kBackpressureTimeout,
                            OfferResult::kEnqueueFailed, OfferResult::kFatal}) {
    EXPECT_STRNE(ToString(result), "UNMAPPED");
  }
}

}  // namespace
}  // namespace p04
