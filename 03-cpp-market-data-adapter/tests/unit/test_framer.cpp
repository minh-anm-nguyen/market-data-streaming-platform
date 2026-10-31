// MDP1 framer: chunk independence, length bounds, deadlines, EOF semantics.
//
// Group C of the what-if matrix lives here, plus the chunking half of B.
#include <gtest/gtest.h>

#include <numeric>
#include <random>

#include "md_adapter/framer.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::FrameAction;
using md::Mdp1Framer;
using md::Reason;

namespace {

md::FramerLimits no_deadlines(std::uint32_t max_payload = 4096) {
  md::FramerLimits l;
  l.max_payload_bytes = max_payload;
  l.prefix_deadline_ns = 0;
  l.header_partial_deadline_ns = 0;
  l.body_deadline_ns = 0;
  return l;
}

}  // namespace

// --- happy path -------------------------------------------------------------

TEST(Framer, DecodesFramesInOrder) {
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;
  const std::string data = stream({"alpha", "beta", "gamma"});

  const auto r = f.feed(as_bytes(data), 0, h);

  EXPECT_FALSE(r.fatal);
  EXPECT_EQ(r.bytes_consumed, data.size());
  ASSERT_EQ(h.frames.size(), 3u);
  EXPECT_EQ(h.frames[0], "alpha");
  EXPECT_EQ(h.frames[1], "beta");
  EXPECT_EQ(h.frames[2], "gamma");
  EXPECT_TRUE(f.at_frame_boundary());
}

// B-01/B-02/B-03/E2E-02: the recv() boundary must not be observable.
TEST(Framer, ResultIsIndependentOfChunkSize) {
  const std::string data =
      stream({"a", "bb", "ccc", std::string(100, 'x'), std::string(4096, 'y')});
  const std::vector<std::size_t> chunk_sizes = {1,  2,  3,    4,     5,
                                                7,  64, 1024, 4096,  65536};

  for (std::size_t chunk : chunk_sizes) {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    CollectingHandler h;
    const auto r = feed_chunked(f, data, chunk, h);

    EXPECT_FALSE(r.fatal) << "chunk=" << chunk;
    ASSERT_EQ(h.frames.size(), 5u) << "chunk=" << chunk;
    EXPECT_EQ(h.frames[0], "a") << "chunk=" << chunk;
    EXPECT_EQ(h.frames[3], std::string(100, 'x')) << "chunk=" << chunk;
    EXPECT_EQ(h.frames[4], std::string(4096, 'y')) << "chunk=" << chunk;
    EXPECT_EQ(f.frames_complete(), 5u) << "chunk=" << chunk;
  }
}

// B-03 specifically: a BE32 header split across four single-byte reads must
// not be read in host byte order.
TEST(Framer, BigEndianHeaderSplitAcrossReads) {
  const std::string payload(258, 'z');  // 0x0102 -- different in LE and BE
  const std::string data = stream({payload});
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;

  feed_chunked(f, data, 1, h);

  ASSERT_EQ(h.frames.size(), 1u);
  EXPECT_EQ(h.frames[0].size(), 258u);
}

TEST(Framer, CoalescedFramesInOneRead) {
  std::vector<std::string> payloads;
  for (int i = 0; i < 200; ++i) payloads.push_back("p" + std::to_string(i));
  const std::string data = stream(payloads);

  md::FramerLimits l = no_deadlines();
  l.max_frames_per_iteration = 1000;
  Mdp1Framer f(l);
  f.reset(0);
  CollectingHandler h;
  f.feed(as_bytes(data), 0, h);

  EXPECT_EQ(h.frames.size(), 200u);
  EXPECT_EQ(h.frames[199], "p199");
}

// L-group fairness: one feed() must not monopolise the loop.
TEST(Framer, RespectsFramesPerIterationBudget) {
  std::vector<std::string> payloads(50, "x");
  const std::string data = stream(payloads);
  md::FramerLimits l = no_deadlines();
  l.max_frames_per_iteration = 10;
  Mdp1Framer f(l);
  f.reset(0);
  CollectingHandler h;

  const auto r = f.feed(as_bytes(data), 0, h);

  EXPECT_EQ(r.frames_emitted, 10u);
  EXPECT_LT(r.bytes_consumed, data.size());
  EXPECT_FALSE(r.fatal);
}

// --- magic ------------------------------------------------------------------

// A-01/A-10: JSONL or another protocol on the MDP1 port fails closed.
TEST(Framer, RejectsBadMagicWithoutFallback) {
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;
  const std::string jsonl = "{\"symbol\":\"EURUSD\",\"sequence\":1}\n";

  const auto r = f.feed(as_bytes(jsonl), 0, h);

  EXPECT_TRUE(r.fatal);
  EXPECT_EQ(r.reason, Reason::kBadMagic);
  EXPECT_TRUE(h.frames.empty());
  EXPECT_EQ(f.stage(), Mdp1Framer::Stage::kClosed);
}

TEST(Framer, MagicIsNotASyncWordInsidePayload) {
  // A payload that contains "MDP1" must not be treated as a resync point.
  const std::string data = stream({"MDP1MDP1", "ok"});
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;

  f.feed(as_bytes(data), 0, h);

  ASSERT_EQ(h.frames.size(), 2u);
  EXPECT_EQ(h.frames[0], "MDP1MDP1");
}

TEST(Framer, ClosedFramerStaysClosed) {
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;
  f.feed(as_bytes(std::string("XXXX")), 0, h);
  ASSERT_EQ(f.stage(), Mdp1Framer::Stage::kClosed);

  const std::string good = stream({"later"});
  const auto r = f.feed(as_bytes(good), 0, h);

  EXPECT_TRUE(r.fatal);
  EXPECT_TRUE(h.frames.empty());
}

// --- length bounds ----------------------------------------------------------

// C-group: every out-of-range length is rejected before any sizing happens.
TEST(Framer, RejectsLengthOutsideBounds) {
  struct Case {
    std::uint32_t len;
    const char* name;
  };
  const Case cases[] = {
      {0, "zero"},
      {4097, "one past the cap"},
      {0x7fffffffu, "INT32_MAX"},
      {0xffffffffu, "UINT32_MAX"},
  };

  for (const auto& c : cases) {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    CollectingHandler h;
    const std::string data = std::string("MDP1") + be32(c.len) + "payload";

    const auto r = f.feed(as_bytes(data), 0, h);

    EXPECT_TRUE(r.fatal) << c.name;
    EXPECT_EQ(r.reason, Reason::kInvalidLength) << c.name;
    EXPECT_TRUE(h.frames.empty()) << c.name;
  }
}

TEST(Framer, AcceptsExactlyMaxPayload) {
  const std::string payload(4096, 'm');
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;

  f.feed(as_bytes(stream({payload})), 0, h);

  ASSERT_EQ(h.frames.size(), 1u);
  EXPECT_EQ(h.frames[0].size(), 4096u);
}

TEST(Framer, HonoursALowerConfiguredCap) {
  Mdp1Framer f(no_deadlines(64));
  f.reset(0);
  CollectingHandler h;

  const auto r = f.feed(as_bytes(stream({std::string(65, 'q')})), 0, h);

  EXPECT_TRUE(r.fatal);
  EXPECT_EQ(r.reason, Reason::kInvalidLength);
}

// --- truncation and EOF -----------------------------------------------------

TEST(Framer, EofClassification) {
  // Clean: exactly at a frame boundary.
  {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    CollectingHandler h;
    f.feed(as_bytes(stream({"one"})), 0, h);
    const auto e = f.on_eof();
    EXPECT_TRUE(e.clean);
  }
  // Truncated prefix.
  {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    CollectingHandler h;
    f.feed(as_bytes(std::string("MD")), 0, h);
    const auto e = f.on_eof();
    EXPECT_FALSE(e.clean);
    EXPECT_EQ(e.reason, Reason::kTruncatedPrefix);
  }
  // Truncated header.
  {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    CollectingHandler h;
    f.feed(as_bytes(std::string("MDP1") + std::string("\x00\x00", 2)), 0, h);
    const auto e = f.on_eof();
    EXPECT_FALSE(e.clean);
    EXPECT_EQ(e.reason, Reason::kTruncatedHeader);
  }
  // Truncated body: no partial frame may be emitted.
  {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    CollectingHandler h;
    const std::string data = std::string("MDP1") + be32(10) + "abc";
    f.feed(as_bytes(data), 0, h);
    const auto e = f.on_eof();
    EXPECT_FALSE(e.clean);
    EXPECT_EQ(e.reason, Reason::kTruncatedBody);
    EXPECT_TRUE(h.frames.empty());
  }
}

TEST(Framer, EmptyStreamIsNotCleanEof) {
  // Zero bytes is still a missing magic, not an empty-but-valid stream.
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  const auto e = f.on_eof();
  EXPECT_FALSE(e.clean);
  EXPECT_EQ(e.reason, Reason::kTruncatedPrefix);
}

// --- deadlines --------------------------------------------------------------

// B-group slowloris: the body deadline is absolute and is not renewed by
// each byte that trickles in.
TEST(Framer, BodyDeadlineIsNotRenewedPerByte) {
  md::FramerLimits l;
  l.prefix_deadline_ns = 2'000'000'000;
  l.header_partial_deadline_ns = 2'000'000'000;
  l.body_deadline_ns = 1'000'000'000;  // 1 s
  Mdp1Framer f(l);
  f.reset(0);
  CollectingHandler h;

  const std::string data = std::string("MDP1") + be32(10) + "0123456789";
  // Header arrives at t=0, then one byte every 200 ms.
  auto r = f.feed(as_bytes(data.substr(0, 8)), 0, h);
  ASSERT_FALSE(r.fatal);

  std::int64_t t = 0;
  bool timed_out = false;
  for (std::size_t i = 8; i < data.size(); ++i) {
    t += 200'000'000;
    r = f.feed(as_bytes(data.substr(i, 1)), t, h);
    if (r.fatal) {
      timed_out = true;
      EXPECT_EQ(r.reason, Reason::kFrameTimeout);
      break;
    }
  }
  EXPECT_TRUE(timed_out)
      << "a sender trickling one byte at a time held the connection open";
  EXPECT_TRUE(h.frames.empty());
}

// G-group: an idle feed sitting at a frame boundary is legitimate. Only a
// *partial* header is on a clock.
TEST(Framer, IdleAtFrameBoundaryDoesNotTimeOut) {
  md::FramerLimits l;
  l.prefix_deadline_ns = 1'000'000'000;
  l.header_partial_deadline_ns = 1'000'000'000;
  l.body_deadline_ns = 1'000'000'000;
  Mdp1Framer f(l);
  f.reset(0);
  CollectingHandler h;
  f.feed(as_bytes(stream({"one"})), 0, h);
  ASSERT_TRUE(f.at_frame_boundary());

  // Ten seconds of silence at the boundary.
  EXPECT_EQ(f.check_deadlines(10'000'000'000), Reason::kNone);
}

TEST(Framer, PartialHeaderDoesTimeOut) {
  md::FramerLimits l;
  l.prefix_deadline_ns = 1'000'000'000;
  l.header_partial_deadline_ns = 1'000'000'000;
  l.body_deadline_ns = 1'000'000'000;
  Mdp1Framer f(l);
  f.reset(0);
  CollectingHandler h;
  f.feed(as_bytes(stream({"one"})), 0, h);
  // Two bytes of the next header arrive, then nothing.
  f.feed(as_bytes(std::string("\x00\x00", 2)), 100, h);

  EXPECT_EQ(f.check_deadlines(2'000'000'000), Reason::kFrameTimeout);
}

// A-07: a peer that accepts and then says nothing trips the prefix deadline.
TEST(Framer, PrefixDeadlineExpires) {
  md::FramerLimits l;
  l.prefix_deadline_ns = 2'000'000'000;
  Mdp1Framer f(l);
  f.reset(0);

  EXPECT_EQ(f.check_deadlines(1'000'000'000), Reason::kNone);
  EXPECT_EQ(f.check_deadlines(3'000'000'000), Reason::kFrameTimeout);
}

// --- handler control --------------------------------------------------------

TEST(Framer, PauseStopsConsumingAndKeepsRemainder) {
  const std::string data = stream({"one", "two", "three"});
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;
  h.next_action = FrameAction::kPause;

  const auto r = f.feed(as_bytes(data), 0, h);

  EXPECT_TRUE(r.paused);
  EXPECT_EQ(h.frames.size(), 1u);
  // Exactly the first frame was consumed: magic + header + payload.
  EXPECT_EQ(r.bytes_consumed, 4u + 4u + 3u);

  // Resuming from the remainder yields the rest, with nothing duplicated.
  h.next_action = FrameAction::kContinue;
  const std::string rest = data.substr(r.bytes_consumed);
  f.feed(as_bytes(rest), 0, h);
  ASSERT_EQ(h.frames.size(), 3u);
  EXPECT_EQ(h.frames[1], "two");
  EXPECT_EQ(h.frames[2], "three");
}

TEST(Framer, HandlerFatalClosesStreamWithItsReason) {
  const std::string data = stream({"one", "two"});
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;
  h.next_action = FrameAction::kFatal;
  h.fatal_with = Reason::kProtoDecodeError;

  const auto r = f.feed(as_bytes(data), 0, h);

  EXPECT_TRUE(r.fatal);
  EXPECT_EQ(r.reason, Reason::kProtoDecodeError);
  EXPECT_EQ(f.stage(), Mdp1Framer::Stage::kClosed);
  EXPECT_EQ(h.frames.size(), 1u);
}

// --- reset ------------------------------------------------------------------

TEST(Framer, ResetStartsANewStreamButKeepsLifetimeCounters) {
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;
  f.feed(as_bytes(stream({"one"})), 0, h);
  ASSERT_EQ(f.frames_complete(), 1u);

  f.reset(1000);
  f.feed(as_bytes(stream({"two"})), 1000, h);

  // Per-connection state restarts; process-lifetime counters do not.
  EXPECT_EQ(f.frames_complete(), 2u);
  EXPECT_EQ(h.frames.size(), 2u);
}

TEST(Framer, ResetDoesNotCarryPartialBytesIntoTheNextConnection) {
  Mdp1Framer f(no_deadlines());
  f.reset(0);
  CollectingHandler h;
  // Half a frame, then the connection drops.
  f.feed(as_bytes(std::string("MDP1") + be32(10) + "abc"), 0, h);

  f.reset(1000);
  f.feed(as_bytes(stream({"fresh"})), 1000, h);

  ASSERT_EQ(h.frames.size(), 1u);
  EXPECT_EQ(h.frames[0], "fresh");
}

// --- randomised partitions --------------------------------------------------

TEST(Framer, RandomChunkPartitionsAgreeWithOneShot) {
  std::vector<std::string> payloads;
  std::mt19937 rng(20261030);
  for (int i = 0; i < 64; ++i)
    payloads.push_back(std::string(1 + (rng() % 300), static_cast<char>('a' + (i % 26))));
  const std::string data = stream(payloads);

  CollectingHandler ref;
  {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    f.feed(as_bytes(data), 0, ref);
  }

  for (int trial = 0; trial < 50; ++trial) {
    Mdp1Framer f(no_deadlines());
    f.reset(0);
    CollectingHandler h;
    std::size_t off = 0;
    while (off < data.size()) {
      const std::size_t n =
          std::min<std::size_t>(1 + (rng() % 700), data.size() - off);
      f.feed(as_bytes(data.substr(off, n)), 0, h);
      off += n;
    }
    ASSERT_EQ(h.frames, ref.frames) << "trial=" << trial;
  }
}
