// Live TCP against a real loopback server. Groups A and B.
//
// The fake server is deliberately hostile where the matrix asks for it:
// it can dribble bytes, send the wrong magic, close mid-frame, or refuse to
// say anything at all.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "md_adapter/pipeline.hpp"
#include "test_support.hpp"

using namespace mdtest;
using md::Reason;
using md::RunStatus;

namespace {

// A single-connection loopback server driven by a script of byte chunks.
class FakeServer {
 public:
  struct Script {
    // One entry per accepted connection. Each connection gets its own chunk
    // list, so a reconnect test really does see two separate MDP1 streams
    // rather than one stream with a second magic embedded in it.
    std::vector<std::vector<std::string>> sessions{{}};
    std::chrono::microseconds delay_between{0};
    // Wait before sending anything (exercises the prefix deadline).
    std::chrono::milliseconds silence_before{0};
    bool close_after = true;
  };

  explicit FakeServer(Script s) : script_(std::move(s)) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listen_fd_, 0);
    const int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // let the kernel choose
    EXPECT_EQ(
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t len = sizeof(addr);
    EXPECT_EQ(
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    port_ = ::ntohs(addr.sin_port);
    EXPECT_EQ(::listen(listen_fd_, 4), 0);
    th_ = std::thread([this] { run(); });
  }

  ~FakeServer() {
    stop_.store(true);
    // shutdown() wakes a thread blocked in accept() without touching the
    // member, so the descriptor is only closed and cleared after the join.
    // Closing it first would race the server thread's own read of it.
    if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
    if (th_.joinable()) th_.join();
    if (listen_fd_ >= 0) {
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
  }

  int port() const { return port_; }
  int connections() const { return connections_.load(); }

 private:
  void run() {
    for (std::size_t i = 0; i < script_.sessions.size() && !stop_.load(); ++i) {
      const int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) return;
      connections_.fetch_add(1);
      if (script_.silence_before.count() > 0)
        std::this_thread::sleep_for(script_.silence_before);
      for (const auto& c : script_.sessions[i]) {
        if (stop_.load()) break;
        std::size_t off = 0;
        while (off < c.size()) {
          const ssize_t n =
              ::send(fd, c.data() + off, c.size() - off, MSG_NOSIGNAL);
          if (n <= 0) break;
          off += static_cast<std::size_t>(n);
        }
        if (script_.delay_between.count() > 0)
          std::this_thread::sleep_for(script_.delay_between);
      }
      if (script_.close_after) {
        ::close(fd);
      } else {
        // Hold the connection open until the test tears the server down.
        while (!stop_.load())
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
        ::close(fd);
      }
    }
  }

  Script script_;
  int listen_fd_ = -1;
  int port_ = 0;
  std::thread th_;
  std::atomic<bool> stop_{false};
  std::atomic<int> connections_{0};
};

md::Config live_config(int port) {
  md::Config c = base_config();
  c.input.mode = md::InputMode::kTcp;
  c.input.host = "127.0.0.1";
  c.input.port = port;
  c.freshness.clock_mode = md::ClockMode::kMonotonicLive;
  c.output.sink = md::SinkKind::kMetricsOnly;
  // One attempt by default: most tests serve a single session and then let
  // the peer close, and a further retry would just sit on the listening
  // socket until the prefix deadline fired.
  c.reconnect.max_attempts = 1;
  c.reconnect.initial_backoff_ms = 10;
  c.reconnect.max_backoff_ms = 50;
  c.output.shutdown_drain_deadline_ms = 2000;
  return c;
}

std::vector<std::string> contiguous_payloads(int n) {
  std::vector<std::string> out;
  for (int i = 1; i <= n; ++i) {
    EventSpec e;
    e.sequence = i;
    e.event_time_ns = static_cast<std::int64_t>(i) * 1'000'000;
    e.bid = 109000 + i;
    e.ask = e.bid + 5;
    out.push_back(encode(e));
  }
  return out;
}

// Finds a port nothing is listening on, so a connect attempt is refused.
int closed_port() {
  const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ::bind(probe, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  ::getsockname(probe, reinterpret_cast<sockaddr*>(&addr), &len);
  const int p = ::ntohs(addr.sin_port);
  ::close(probe);
  return p;
}

}  // namespace

TEST(TcpLive, ReceivesAStreamAndReportsPartialLive) {
  FakeServer::Script s;
  s.sessions = {{stream(contiguous_payloads(100))}};
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  const auto out = runner.run_live();

  EXPECT_EQ(runner.metrics().frames_complete, 100u);
  EXPECT_EQ(runner.metrics().business_ok, 100u);
  // No app-level ACK and no source manifest: PARTIAL_LIVE is the strongest
  // truthful verdict, even though every byte arrived.
  EXPECT_EQ(out.status, RunStatus::kPartialLive) << out.summary;
  EXPECT_EQ(out.provenance, md::Provenance::kLiveUnverified);
  EXPECT_GE(runner.metrics().connection_epochs, 1u);
  EXPECT_GT(runner.metrics().start_unverified, 0u)
      << "a live feed has no verified baseline";
}

// B-01: one byte per write must give the same result as one large write.
TEST(TcpLive, ByteAtATimeDelivery) {
  const std::string wire = stream(contiguous_payloads(20));
  FakeServer::Script s;
  for (char c : wire) s.sessions[0].push_back(std::string(1, c));
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  runner.run_live();

  EXPECT_EQ(runner.metrics().frames_complete, 20u);
  EXPECT_EQ(runner.metrics().business_ok, 20u);
}

// A-01: JSONL on the MDP1 port fails closed, with no protocol sniffing.
TEST(TcpLive, JsonlOnTheProtobufPortFailsClosed) {
  FakeServer::Script s;
  s.sessions = {
      {"{\"symbol\":\"EURUSD\",\"sequence\":1}\n"
       "{\"symbol\":\"EURUSD\",\"sequence\":2}\n"}};
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  const auto out = runner.run_live();

  EXPECT_EQ(out.terminal_reason, Reason::kBadMagic);
  EXPECT_EQ(out.status, RunStatus::kFailed);
  EXPECT_EQ(runner.metrics().business_ok, 0u);
}

TEST(TcpLive, WrongProtocolMagicIsNotAutoNegotiated) {
  std::string wire = stream(contiguous_payloads(3));
  wire.replace(0, 4, "MDP2");
  FakeServer::Script s;
  s.sessions = {{wire}};
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  EXPECT_EQ(runner.run_live().terminal_reason, Reason::kBadMagic);
}

// A-03/A-09: a port nobody is listening on produces bounded, backed-off
// retries rather than a tight loop.
TEST(TcpLive, ConnectionRefusedRetriesWithinBudgetThenStops) {
  md::Config cfg = live_config(closed_port());
  cfg.reconnect.max_attempts = 3;
  cfg.reconnect.initial_backoff_ms = 5;
  cfg.reconnect.max_backoff_ms = 20;

  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);

  const auto t0 = std::chrono::steady_clock::now();
  const auto out = runner.run_live();
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  EXPECT_EQ(out.status, RunStatus::kFailed);
  EXPECT_NE(out.summary.find("reconnect budget"), std::string::npos)
      << out.summary;
  EXPECT_LE(runner.metrics().reconnects, 5u);
  // Backoff means this cannot have been a spin; the budget means it cannot
  // have run forever.
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(),
            10);
}

// A-07: accept-then-say-nothing must trip the prefix deadline.
TEST(TcpLive, SilentServerTripsThePrefixDeadline) {
  FakeServer::Script s;
  s.silence_before = std::chrono::milliseconds(1500);
  s.close_after = false;
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  cfg.input.prefix_deadline_ms = 200;
  cfg.reconnect.max_attempts = 1;

  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  const auto out = runner.run_live();

  EXPECT_EQ(out.terminal_reason, Reason::kFrameTimeout) << out.summary;
  EXPECT_EQ(runner.metrics().frames_complete, 0u);
}

// B-group slowloris: a trickling body must trip the absolute body deadline.
TEST(TcpLive, SlowlorisBodyTripsTheBodyDeadline) {
  const std::string wire = stream(contiguous_payloads(1));
  FakeServer::Script s;
  // Magic and header first, then the body one byte at a time.
  s.sessions[0].push_back(wire.substr(0, 8));
  for (std::size_t i = 8; i < wire.size(); ++i)
    s.sessions[0].push_back(wire.substr(i, 1));
  s.delay_between = std::chrono::microseconds(60'000);  // 60 ms per byte
  s.close_after = false;
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  cfg.input.body_deadline_ms = 150;
  cfg.reconnect.max_attempts = 1;

  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  const auto out = runner.run_live();

  EXPECT_EQ(out.terminal_reason, Reason::kFrameTimeout) << out.summary;
  EXPECT_EQ(runner.metrics().frames_complete, 0u);
}

TEST(TcpLive, PeerClosingMidFrameIsReportedAsTruncated) {
  const std::string wire = stream(contiguous_payloads(2));
  FakeServer::Script s;
  s.sessions = {{wire.substr(0, wire.size() - 4)}};
  s.close_after = true;
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  cfg.reconnect.max_attempts = 1;
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  const auto out = runner.run_live();

  EXPECT_EQ(out.terminal_reason, Reason::kTruncatedBody) << out.summary;
  EXPECT_EQ(runner.metrics().frames_complete, 1u);
}

// E2E-05: reconnecting inside one process keeps per-key state, so a resend
// is recognised instead of double-counted.
TEST(TcpLive, ReconnectKeepsSequenceStateAndDetectsResends) {
  const auto first = contiguous_payloads(20);
  const auto all = contiguous_payloads(25);
  std::vector<std::string> second(all.begin() + 9, all.end());  // 10..25

  FakeServer::Script s;
  s.sessions = {{stream(first)}, {stream(second)}};
  s.close_after = true;
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  cfg.reconnect.max_attempts = 2;
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  runner.run_live();

  const auto& m = runner.metrics();
  EXPECT_EQ(m.frames_complete, 36u);  // 20 + 16
  EXPECT_EQ(m.duplicate_exact, 11u) << "10..20 resent inside the window";
  EXPECT_EQ(m.connection_epochs, 2u);
  // Resends must not inflate the published count.
  EXPECT_EQ(m.output_success, 25u);
  EXPECT_TRUE(m.check_conservation().ok);
}

TEST(TcpLive, ConnectionEpochsAreCountedSeparatelyFromFeedState) {
  FakeServer::Script s;
  s.sessions = {{stream(contiguous_payloads(2))},
                {stream(contiguous_payloads(2))}};
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  cfg.reconnect.max_attempts = 2;
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);
  runner.run_live();

  EXPECT_EQ(runner.metrics().connection_epochs, 2u);
  // One dataset/symbol key across both sessions: a new socket is not a new
  // feed.
  EXPECT_EQ(runner.pipeline().tracker().key_count(), 1u);
}

TEST(TcpLive, ShutdownFlagStopsTheLoopPromptly) {
  FakeServer::Script s;
  s.sessions = {{stream(contiguous_payloads(5))}};
  s.close_after = false;  // keep the socket open and idle
  FakeServer server(s);

  md::Config cfg = live_config(server.port());
  cfg.reconnect.max_attempts = 1;
  md::SteadyClock clock;
  md::MetricsOnlySink sink;
  md::AdapterRunner runner(cfg, clock, sink);

  std::thread stopper([] {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    md::AdapterRunner::shutdown_flag().store(true);
  });
  const auto out = runner.run_live();
  stopper.join();
  md::AdapterRunner::shutdown_flag().store(false);  // restore for other tests

  EXPECT_EQ(runner.metrics().frames_complete, 5u);
  EXPECT_EQ(out.status, RunStatus::kPartialLive) << out.summary;
}
