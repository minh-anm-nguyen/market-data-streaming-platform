#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "md_adapter/input_source.hpp"

namespace md {
namespace {

// splitmix64: a tiny, fully deterministic generator. Reconnect jitter must
// spread retries without making a test run unreproducible.
std::uint64_t splitmix64(std::uint64_t& s) {
  s += 0x9E3779B97F4A7C15ull;
  std::uint64_t z = s;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

bool set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

Mdp1TcpClient::Mdp1TcpClient(Options opts)
    : opts_(std::move(opts)),
      rng_(static_cast<std::uint64_t>(opts_.jitter_seed)) {}

Mdp1TcpClient::~Mdp1TcpClient() { close_connection(); }

void Mdp1TcpClient::close_connection() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  if (state_ == State::kConnected || state_ == State::kConnecting)
    state_ = State::kIdle;
}

Reason Mdp1TcpClient::start_connect(std::int64_t now_mono_ns) {
  // Numeric-only resolution by default. A feed endpoint is configuration, not
  // something to be discovered, and AI_NUMERICHOST keeps a misconfigured host
  // from turning into an unbounded DNS wait (case A-04).
  struct ::addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_NUMERICSERV;

  const std::string port_s = std::to_string(opts_.port);
  struct ::addrinfo* res = nullptr;
  const int gai = ::getaddrinfo(opts_.host.c_str(), port_s.c_str(), &hints, &res);
  if (gai != 0 || res == nullptr) {
    state_ = State::kFailed;
    return Reason::kIoError;
  }

  // Try each returned address once; a host with an unreachable AAAA must not
  // be able to consume the whole budget on one family.
  Reason last = Reason::kIoError;
  for (struct ::addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    const int fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC,
                            ai->ai_protocol);
    if (fd < 0) continue;
    if (!set_nonblocking(fd)) {
      ::close(fd);
      continue;
    }
    const int one = 1;
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    int rc = 0;
    do {
      rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
      fd_ = fd;
      state_ = State::kConnected;
      connect_started_ns_ = now_mono_ns;
      ++epoch_;
      ::freeaddrinfo(res);
      return Reason::kNone;
    }
    if (errno == EINPROGRESS) {
      fd_ = fd;
      state_ = State::kConnecting;
      connect_started_ns_ = now_mono_ns;
      ::freeaddrinfo(res);
      return Reason::kNone;
    }
    last = (errno == ECONNREFUSED) ? Reason::kConnectRefused : Reason::kIoError;
    ::close(fd);
  }
  ::freeaddrinfo(res);
  state_ = State::kFailed;
  return last;
}

Reason Mdp1TcpClient::finish_connect() {
  // A non-blocking connect reports its outcome through SO_ERROR, not through
  // the writability notification itself.
  int err = 0;
  ::socklen_t len = sizeof(err);
  if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
    close_connection();
    return Reason::kIoError;
  }
  if (err == 0) {
    state_ = State::kConnected;
    ++epoch_;
    return Reason::kNone;
  }
  close_connection();
  return (err == ECONNREFUSED) ? Reason::kConnectRefused : Reason::kIoError;
}

Reason Mdp1TcpClient::connect_step(std::int64_t now_mono_ns, bool& again) {
  again = false;
  if (state_ == State::kConnected) return Reason::kNone;

  if (state_ == State::kIdle || state_ == State::kFailed) {
    ++attempts_;
    const Reason r = start_connect(now_mono_ns);
    if (r != Reason::kNone) return r;
    if (state_ == State::kConnected) return Reason::kNone;
    again = true;
    return Reason::kNone;
  }

  // kConnecting: absolute deadline, never renewed.
  std::int64_t elapsed = 0;
  if (!__builtin_sub_overflow(now_mono_ns, connect_started_ns_, &elapsed) &&
      elapsed > opts_.connect_deadline_ns) {
    close_connection();
    state_ = State::kFailed;
    return Reason::kConnectTimeout;
  }

  // Poll writability without blocking the loop.
  struct ::pollfd p {};
  p.fd = fd_;
  p.events = POLLOUT;
  const int rc = ::poll(&p, 1, 0);
  if (rc < 0) {
    if (errno == EINTR) {
      again = true;
      return Reason::kNone;
    }
    close_connection();
    return Reason::kIoError;
  }
  if (rc == 0) {
    again = true;
    return Reason::kNone;
  }
  return finish_connect();
}

bool Mdp1TcpClient::schedule_retry(std::int64_t now_mono_ns) {
  if (opts_.max_attempts > 0 &&
      attempts_ >= static_cast<std::uint64_t>(opts_.max_attempts))
    return false;

  // Exponential growth with a hard cap, then bounded deterministic jitter.
  std::int64_t backoff = (last_backoff_ns_ == 0)
                             ? opts_.initial_backoff_ns
                             : last_backoff_ns_ * 2;
  if (backoff > opts_.max_backoff_ns || backoff <= 0)
    backoff = opts_.max_backoff_ns;
  last_backoff_ns_ = backoff;

  std::int64_t jitter = 0;
  if (opts_.jitter_pct > 0) {
    const std::uint64_t span =
        static_cast<std::uint64_t>(backoff) *
        static_cast<std::uint64_t>(opts_.jitter_pct) / 100u;
    if (span > 0)
      jitter = static_cast<std::int64_t>(splitmix64(rng_) % (span + 1));
  }
  next_attempt_ns_ = now_mono_ns + backoff + jitter;
  state_ = State::kIdle;
  return true;
}

ReadResult Mdp1TcpClient::read(std::span<std::byte> out) {
  ReadResult r;
  if (fd_ < 0 || state_ != State::kConnected) {
    r.error = Reason::kIoError;
    return r;
  }
  while (true) {
    const ssize_t n = ::recv(fd_, out.data(), out.size(), 0);
    if (n > 0) {
      r.bytes = static_cast<std::size_t>(n);
      total_bytes_ += r.bytes;
      return r;
    }
    if (n == 0) {
      r.eof = true;  // orderly peer close; the framer decides if it was clean
      return r;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      r.would_block = true;
      return r;
    }
    if (errno == ECONNRESET) {
      r.error = Reason::kPeerClosed;
      return r;
    }
    r.error = Reason::kIoError;
    return r;
  }
}

}  // namespace md
