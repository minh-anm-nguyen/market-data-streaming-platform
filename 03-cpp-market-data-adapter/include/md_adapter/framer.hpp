// Incremental MDP1 framer.
//
// Wire format (Project 02): the 4 ASCII bytes "MDP1" exactly once per
// file/connection, then repeated [BE32 length][length bytes of payload].
//
// Guarantees this class is responsible for:
//   * never interprets a partial frame, whatever the recv() boundaries are;
//   * never allocates from an unvalidated length -- the body buffer is sized
//     once, at construction, to max_payload_bytes;
//   * absolute per-stage deadlines that are not renewed byte-by-byte, so a
//     slowloris sender trips the body deadline;
//   * no resynchronisation scan: "MDP1" is a one-shot prefix, not a sync word.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "md_adapter/status.hpp"

namespace md {

inline constexpr std::size_t kMagicSize = 4;
inline constexpr char kMagic[kMagicSize + 1] = "MDP1";
inline constexpr std::size_t kLengthSize = 4;

struct FramerLimits {
  std::uint32_t max_payload_bytes = 4096;
  // 0 disables the stage deadline (used by the offline file source, where a
  // "slow sender" cannot exist).
  std::int64_t prefix_deadline_ns = 2'000'000'000;
  std::int64_t header_partial_deadline_ns = 2'000'000'000;
  std::int64_t body_deadline_ns = 2'000'000'000;
  // Work quantum: how many complete frames one feed() call may emit before
  // returning, so timers and shutdown are never starved by a hot socket.
  std::size_t max_frames_per_iteration = 256;
};

// What the framer should do after handing over a complete frame.
enum class FrameAction : std::uint8_t {
  kContinue,  // keep parsing
  kPause,     // stop consuming; unconsumed input stays with the caller
  kFatal,     // close the stream with `fatal_reason`
};

class FrameHandler {
 public:
  virtual ~FrameHandler() = default;
  virtual FrameAction on_frame(std::span<const std::byte> payload,
                               Reason& fatal_reason) = 0;
};

class Mdp1Framer {
 public:
  enum class Stage : std::uint8_t { kPrefix, kHeader, kBody, kClosed };

  struct FeedResult {
    std::size_t bytes_consumed = 0;
    std::size_t frames_emitted = 0;
    bool fatal = false;             // stream must be closed
    bool paused = false;            // handler asked to stop
    Reason reason = Reason::kNone;  // set when fatal
  };

  explicit Mdp1Framer(FramerLimits limits = {});

  // Resets to the start of a new file or TCP connection. `now_ns` starts the
  // prefix deadline.
  void reset(std::int64_t now_ns);

  FeedResult feed(std::span<const std::byte> in, std::int64_t now_ns,
                  FrameHandler& handler);

  // Called when the peer closed or the file ended. Distinguishes a clean frame
  // boundary from a truncated prefix/header/body.
  struct EofResult {
    bool clean = false;
    Reason reason = Reason::kNone;
  };
  EofResult on_eof() const;

  // Deadline check for stages that are waiting on bytes. Must be driven from
  // the event loop even when no bytes arrive.
  Reason check_deadlines(std::int64_t now_ns) const;

  Stage stage() const noexcept { return stage_; }
  bool at_frame_boundary() const noexcept {
    return stage_ == Stage::kHeader && header_fill_ == 0;
  }
  std::uint64_t frames_complete() const noexcept { return frames_complete_; }
  std::uint64_t bytes_fed() const noexcept { return bytes_fed_; }
  const FramerLimits& limits() const noexcept { return limits_; }

 private:
  void enter_closed(Reason r);

  FramerLimits limits_;
  Stage stage_ = Stage::kPrefix;

  std::array<std::byte, kMagicSize> prefix_{};
  std::size_t prefix_fill_ = 0;

  std::array<std::byte, kLengthSize> header_{};
  std::size_t header_fill_ = 0;

  std::vector<std::byte> body_;  // capacity fixed at construction
  std::uint32_t body_need_ = 0;
  std::uint32_t body_fill_ = 0;

  // Absolute deadline anchors (monotonic ns). -1 means "not armed".
  std::int64_t prefix_anchor_ns_ = -1;
  std::int64_t header_anchor_ns_ = -1;
  std::int64_t body_anchor_ns_ = -1;

  std::uint64_t frames_complete_ = 0;
  std::uint64_t bytes_fed_ = 0;
  Reason closed_reason_ = Reason::kNone;
};

}  // namespace md
