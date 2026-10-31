// md_cpp_tcp_client: consume an MDP1 stream over TCP and project each event.
//
// Exists so the framing tests can run over a real socket rather than only over
// files. A socket is where fragmentation and coalescing actually happen: the
// writer's frame boundaries have no relationship to the reader's recv
// boundaries, so one recv may hold several frames or half of one (D16, D17).
//
// The deadline is absolute, computed once per frame from a monotonic clock, and
// the per-recv timeout is derived from the time *remaining*. A per-recv timeout
// that reset on every byte would let a client trickling one byte at a time hold
// the reader open indefinitely (cases D12, I14).
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>

#include "md/contract.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using socket_t = SOCKET;
constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
constexpr socket_t kInvalidSocket = -1;
#endif

namespace {

using Clock = std::chrono::steady_clock;

class SocketSource final : public md::ByteSource {
 public:
  SocketSource(socket_t sock, double deadline_s) : sock_(sock), deadline_s_(deadline_s) {}

  // Call once per frame to restart the budget; the budget then covers the whole
  // header-plus-body read rather than each recv separately.
  void StartFrame() { frame_start_ = Clock::now(); }

  std::int64_t ReadSome(char* buffer, std::size_t size, md::Error& error) override {
    if (deadline_s_ > 0) {
      const double elapsed =
          std::chrono::duration<double>(Clock::now() - frame_start_).count();
      const double remaining = deadline_s_ - elapsed;
      if (remaining <= 0) {
        error = {md::Reason::kReadDeadlineExceeded, "frame deadline passed"};
        return -1;
      }
      SetTimeout(remaining);
    }
    const int n = ::recv(sock_, buffer, static_cast<int>(size), 0);
    if (n < 0) {
      error = {md::Reason::kReadDeadlineExceeded, "recv timed out or failed"};
      return -1;
    }
    return n;
  }

 private:
  void SetTimeout(double seconds) {
#ifdef _WIN32
    DWORD ms = static_cast<DWORD>(seconds * 1000.0);
    if (ms == 0) ms = 1;
    ::setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO,
                 reinterpret_cast<const char*>(&ms), sizeof(ms));
#else
    struct timeval tv;
    tv.tv_sec = static_cast<time_t>(seconds);
    tv.tv_usec = static_cast<suseconds_t>((seconds - static_cast<double>(tv.tv_sec)) * 1e6);
    if (tv.tv_sec == 0 && tv.tv_usec == 0) tv.tv_usec = 1000;
    ::setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
  }

  socket_t sock_;
  double deadline_s_;
  Clock::time_point frame_start_ = Clock::now();
};

struct Options {
  std::string host = "127.0.0.1";
  int port = 9010;
  std::string output;
  double deadline_s = 2.0;
  std::int64_t max_events = 0;  // 0 = until the stream ends
};

void PrintUsage() {
  std::fprintf(stderr,
               "usage: md_cpp_tcp_client [--host H] [--port P] [--output FILE.jsonl]\n"
               "                         [--deadline-s S] [--max-events N]\n");
}

bool ParseArgs(int argc, char** argv, Options& opts) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&](std::string& dst) {
      if (i + 1 >= argc) return false;
      dst = argv[++i];
      return true;
    };
    std::string value;
    if (arg == "--host") { if (!next(opts.host)) return false; }
    else if (arg == "--port") { if (!next(value)) return false; opts.port = std::atoi(value.c_str()); }
    else if (arg == "--output") { if (!next(opts.output)) return false; }
    else if (arg == "--deadline-s") { if (!next(value)) return false; opts.deadline_s = std::atof(value.c_str()); }
    else if (arg == "--max-events") { if (!next(value)) return false; opts.max_events = std::atoll(value.c_str()); }
    else if (arg == "--help" || arg == "-h") { PrintUsage(); std::exit(0); }
    else { std::fprintf(stderr, "unknown argument: %s\n", arg.c_str()); return false; }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options opts;
  if (!ParseArgs(argc, argv, opts)) {
    PrintUsage();
    return 2;
  }

#ifdef _WIN32
  WSADATA wsa;
  if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
    std::fprintf(stderr, "WSAStartup failed\n");
    return 2;
  }
#endif

  socket_t sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock == kInvalidSocket) {
    std::fprintf(stderr, "socket() failed\n");
    return 2;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<unsigned short>(opts.port));
  if (::inet_pton(AF_INET, opts.host.c_str(), &addr.sin_addr) != 1) {
    std::fprintf(stderr, "bad host %s\n", opts.host.c_str());
    return 2;
  }
  if (::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::fprintf(stderr, "connect to %s:%d failed\n", opts.host.c_str(), opts.port);
    return 2;
  }

  std::FILE* out = nullptr;
  if (!opts.output.empty()) {
    out = std::fopen(opts.output.c_str(), "wb");
    if (out == nullptr) {
      std::fprintf(stderr, "cannot write %s\n", opts.output.c_str());
      return 2;
    }
  }

  SocketSource source(sock, opts.deadline_s);
  md::FrameReader reader(source);

  std::int64_t accepted = 0, rejected = 0;
  std::map<std::string, std::int64_t> reasons;
  std::string first_error, payload;
  md::Error error;

  while (opts.max_events == 0 || accepted < opts.max_events) {
    source.StartFrame();
    if (!reader.ReadFrame(payload, error)) {
      if (reader.end() == md::StreamEnd::kCompleted) break;
      ++rejected;
      ++reasons[std::string(md::ReasonName(error.reason))];
      if (first_error.empty()) first_error = error.ToString();
      break;
    }
    md::DomainEvent event;
    md::Error decode = md::DecodePayload(
        std::span<const char>(payload.data(), payload.size()), event);
    if (decode.ok()) decode = md::CheckEnvelope(event);
    if (!decode.ok()) {
      ++rejected;
      ++reasons[std::string(md::ReasonName(decode.reason))];
      if (first_error.empty()) first_error = decode.ToString();
      break;
    }
    ++accepted;
    if (out != nullptr) {
      const std::string line = md::ProjectionJson(event);
      std::fwrite(line.data(), 1, line.size(), out);
      std::fputc('\n', out);
    }
  }

  if (out != nullptr) {
    std::fflush(out);
    std::fclose(out);
  }
#ifdef _WIN32
  ::closesocket(sock);
  ::WSACleanup();
#else
  ::close(sock);
#endif

  std::string report = "{";
  report += "\"events_accepted\":" + std::to_string(accepted);
  report += ",\"frames_read\":" + std::to_string(reader.frames_read());
  report += ",\"rejected\":" + std::to_string(rejected);
  report += ",\"stream_end\":\"" + std::string(md::StreamEndName(reader.end())) + "\"";
  // Deliberately absent: any count of events the *peer* accepted. This client
  // knows what it read; it cannot know what the sender believes was delivered,
  // and MDP1 has no application ACK to tell it (FR-13, section 4.1).
  report += ",\"first_error\":";
  std::string escaped;
  md::AppendJsonString(escaped, first_error);
  report += escaped;
  report += ",\"reasons\":{";
  bool first = true;
  for (const auto& [name, count] : reasons) {
    if (!first) report += ',';
    first = false;
    std::string key;
    md::AppendJsonString(key, name);
    report += key + ":" + std::to_string(count);
  }
  report += "}}";
  std::printf("%s\n", report.c_str());

  return rejected == 0 ? 0 : 1;
}
