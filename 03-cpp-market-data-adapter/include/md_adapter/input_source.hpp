// Byte sources.
//
// A source only moves bytes and reports transport-level facts. It never
// frames, never decodes and never decides whether data is complete -- those
// are separate stages so that the same framer is exercised identically by the
// file path and the socket path (property: chunking must not change results).
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "md_adapter/config.hpp"
#include "md_adapter/status.hpp"

namespace md {

struct ReadResult {
  std::size_t bytes = 0;
  bool eof = false;         // orderly end of file / peer close
  bool would_block = false; // nothing available right now (live only)
  Reason error = Reason::kNone;
};

class InputSource {
 public:
  virtual ~InputSource() = default;
  virtual ReadResult read(std::span<std::byte> out) = 0;
  virtual const char* name() const = 0;
  // Live sources cannot prove completeness; offline verified ones can.
  virtual Provenance provenance() const = 0;
  std::uint64_t total_bytes_read() const noexcept { return total_bytes_; }

 protected:
  std::uint64_t total_bytes_ = 0;
};

// ---------------------------------------------------------------------------
// Offline .pbstream file.
//
// The provenance rule is the whole point: a file is only VERIFIED_COMPLETE
// when a sibling manifest says COMPLETED and its digest and byte count match
// what is on disk. EOF alone proves nothing -- a truncated file also ends.
// ---------------------------------------------------------------------------
struct PbstreamManifest {
  bool present = false;
  bool parsed = false;
  std::string dataset_id;
  std::string sha256;
  std::string status;
  std::string descriptor_sha256;
  std::int64_t frames = -1;
  std::int64_t bytes = -1;
};

class Mdp1FileSource final : public InputSource {
 public:
  Mdp1FileSource(std::string path, bool require_manifest);
  ~Mdp1FileSource() override;

  // Opens the file and, when required, verifies the manifest. Returns kNone
  // on success; kSourceUnverified when provenance could not be established.
  Reason open();

  ReadResult read(std::span<std::byte> out) override;
  const char* name() const override { return "pbstream_file"; }
  Provenance provenance() const override { return provenance_; }

  const PbstreamManifest& manifest() const noexcept { return manifest_; }
  const std::string& file_sha256() const noexcept { return file_sha256_; }
  std::int64_t file_bytes() const noexcept { return file_bytes_; }
  // Non-empty when provenance checks failed; explains exactly what was wrong.
  const std::vector<std::string>& provenance_notes() const noexcept {
    return notes_;
  }

  // Reads and validates the manifest next to `pbstream_path`.
  static PbstreamManifest load_manifest(const std::string& pbstream_path,
                                        std::vector<std::string>& notes);

 private:
  std::string path_;
  bool require_manifest_;
  int fd_ = -1;
  PbstreamManifest manifest_;
  Provenance provenance_ = Provenance::kVerifiedPartialFile;
  std::string file_sha256_;
  std::int64_t file_bytes_ = -1;
  std::vector<std::string> notes_;
};

// ---------------------------------------------------------------------------
// Live MDP1 TCP client: non-blocking connect, epoll level-triggered reads,
// absolute connect deadline, bounded reconnect with deterministic jitter.
// ---------------------------------------------------------------------------
class Mdp1TcpClient final : public InputSource {
 public:
  struct Options {
    std::string host = "127.0.0.1";
    int port = 9010;
    std::int64_t connect_deadline_ns = 3'000'000'000;
    std::int64_t initial_backoff_ns = 100'000'000;
    std::int64_t max_backoff_ns = 10'000'000'000;
    std::int64_t max_attempts = 50;
    std::int64_t jitter_seed = 0;
    std::int64_t jitter_pct = 20;
  };

  explicit Mdp1TcpClient(Options opts);
  ~Mdp1TcpClient() override;

  // Non-blocking connect driven by the caller's event loop. Returns:
  //   kNone        connected (or already connected)
  //   kConnectRefused / kConnectTimeout / kIoError on a terminal attempt
  // `again` is set when the connection is still in progress.
  Reason connect_step(std::int64_t now_mono_ns, bool& again);

  ReadResult read(std::span<std::byte> out) override;
  const char* name() const override { return "mdp1_tcp"; }
  Provenance provenance() const override { return Provenance::kLiveUnverified; }

  void close_connection();
  bool connected() const noexcept { return state_ == State::kConnected; }

  // Schedules the next attempt. Returns false when the retry budget is spent.
  bool schedule_retry(std::int64_t now_mono_ns);
  std::int64_t next_attempt_at_ns() const noexcept { return next_attempt_ns_; }

  int fd() const noexcept { return fd_; }
  std::uint64_t epoch() const noexcept { return epoch_; }
  std::uint64_t attempts() const noexcept { return attempts_; }
  std::int64_t last_backoff_ns() const noexcept { return last_backoff_ns_; }

 private:
  enum class State : std::uint8_t { kIdle, kConnecting, kConnected, kFailed };

  Reason start_connect(std::int64_t now_mono_ns);
  Reason finish_connect();

  Options opts_;
  int fd_ = -1;
  State state_ = State::kIdle;
  std::int64_t connect_started_ns_ = 0;
  std::int64_t next_attempt_ns_ = 0;
  std::int64_t last_backoff_ns_ = 0;
  std::uint64_t attempts_ = 0;
  std::uint64_t epoch_ = 0;
  std::uint64_t rng_ = 0;
};

}  // namespace md
