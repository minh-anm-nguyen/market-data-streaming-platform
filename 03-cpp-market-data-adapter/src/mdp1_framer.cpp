#include "md_adapter/framer.hpp"

#include <algorithm>
#include <cstring>

namespace md {
namespace {

std::uint32_t read_be32(const std::array<std::byte, kLengthSize>& b) noexcept {
  return (static_cast<std::uint32_t>(b[0]) << 24) |
         (static_cast<std::uint32_t>(b[1]) << 16) |
         (static_cast<std::uint32_t>(b[2]) << 8) |
         (static_cast<std::uint32_t>(b[3]));
}

bool deadline_expired(std::int64_t anchor_ns, std::int64_t budget_ns,
                      std::int64_t now_ns) noexcept {
  if (anchor_ns < 0 || budget_ns <= 0) return false;
  // Checked subtraction: monotonic clocks are bounded well below int64 range,
  // but the adapter never relies on that implicitly.
  std::int64_t elapsed = 0;
  if (__builtin_sub_overflow(now_ns, anchor_ns, &elapsed)) return false;
  return elapsed > budget_ns;
}

}  // namespace

Mdp1Framer::Mdp1Framer(FramerLimits limits) : limits_(limits) {
  if (limits_.max_payload_bytes == 0) limits_.max_payload_bytes = 1;
  // Single fixed allocation. Nothing later in the life of this object sizes a
  // buffer from a wire-supplied length.
  body_.resize(limits_.max_payload_bytes);
}

void Mdp1Framer::reset(std::int64_t now_ns) {
  stage_ = Stage::kPrefix;
  prefix_fill_ = 0;
  header_fill_ = 0;
  body_need_ = 0;
  body_fill_ = 0;
  prefix_anchor_ns_ = now_ns;
  header_anchor_ns_ = -1;
  body_anchor_ns_ = -1;
  closed_reason_ = Reason::kNone;
  // frames_complete_ / bytes_fed_ are process-lifetime counters and are not
  // reset per connection (section 10.1 property 6).
}

void Mdp1Framer::enter_closed(Reason r) {
  stage_ = Stage::kClosed;
  closed_reason_ = r;
}

Reason Mdp1Framer::check_deadlines(std::int64_t now_ns) const {
  switch (stage_) {
    case Stage::kPrefix:
      if (deadline_expired(prefix_anchor_ns_, limits_.prefix_deadline_ns, now_ns))
        return Reason::kFrameTimeout;
      return Reason::kNone;
    case Stage::kHeader:
      // A feed that is legitimately idle sits here with header_fill_ == 0.
      // Only a *partial* header is on a deadline; whole-feed silence is the
      // freshness engine's business, not the framer's.
      if (header_fill_ != 0 &&
          deadline_expired(header_anchor_ns_, limits_.header_partial_deadline_ns,
                           now_ns))
        return Reason::kFrameTimeout;
      return Reason::kNone;
    case Stage::kBody:
      if (deadline_expired(body_anchor_ns_, limits_.body_deadline_ns, now_ns))
        return Reason::kFrameTimeout;
      return Reason::kNone;
    case Stage::kClosed:
      return closed_reason_;
  }
  return Reason::kNone;
}

Mdp1Framer::EofResult Mdp1Framer::on_eof() const {
  switch (stage_) {
    case Stage::kPrefix:
      // Zero bytes at all is an empty stream: still not a valid MDP1 stream,
      // because the magic is mandatory.
      return {false, Reason::kTruncatedPrefix};
    case Stage::kHeader:
      return header_fill_ == 0 ? EofResult{true, Reason::kNone}
                               : EofResult{false, Reason::kTruncatedHeader};
    case Stage::kBody:
      return {false, Reason::kTruncatedBody};
    case Stage::kClosed:
      return {false, closed_reason_};
  }
  return {false, Reason::kIoError};
}

Mdp1Framer::FeedResult Mdp1Framer::feed(std::span<const std::byte> in,
                                        std::int64_t now_ns,
                                        FrameHandler& handler) {
  FeedResult res;
  if (stage_ == Stage::kClosed) {
    res.fatal = true;
    res.reason = closed_reason_;
    return res;
  }

  std::size_t pos = 0;
  while (pos < in.size()) {
    if (res.frames_emitted >= limits_.max_frames_per_iteration) break;

    switch (stage_) {
      case Stage::kPrefix: {
        const std::size_t want = kMagicSize - prefix_fill_;
        const std::size_t take = std::min(want, in.size() - pos);
        std::memcpy(prefix_.data() + prefix_fill_, in.data() + pos, take);
        prefix_fill_ += take;
        pos += take;
        if (prefix_fill_ < kMagicSize) break;
        if (std::memcmp(prefix_.data(), kMagic, kMagicSize) != 0) {
          enter_closed(Reason::kBadMagic);
          res.fatal = true;
          res.reason = Reason::kBadMagic;
          res.bytes_consumed = pos;
          bytes_fed_ += pos;
          return res;
        }
        stage_ = Stage::kHeader;
        header_fill_ = 0;
        header_anchor_ns_ = -1;
        break;
      }

      case Stage::kHeader: {
        if (header_fill_ == 0) header_anchor_ns_ = now_ns;
        const std::size_t want = kLengthSize - header_fill_;
        const std::size_t take = std::min(want, in.size() - pos);
        std::memcpy(header_.data() + header_fill_, in.data() + pos, take);
        header_fill_ += take;
        pos += take;
        if (header_fill_ < kLengthSize) break;

        const std::uint32_t len = read_be32(header_);
        // Validate BEFORE anything is sized or copied (design 3.4.1).
        if (len == 0 || len > limits_.max_payload_bytes) {
          enter_closed(Reason::kInvalidLength);
          res.fatal = true;
          res.reason = Reason::kInvalidLength;
          res.bytes_consumed = pos;
          bytes_fed_ += pos;
          return res;
        }
        body_need_ = len;
        body_fill_ = 0;
        header_fill_ = 0;
        header_anchor_ns_ = -1;
        body_anchor_ns_ = now_ns;  // armed once; never renewed per byte
        stage_ = Stage::kBody;
        break;
      }

      case Stage::kBody: {
        const std::size_t avail = in.size() - pos;
        std::span<const std::byte> payload;
        if (body_fill_ == 0 && avail >= body_need_) {
          // Fast path: the whole body is contiguous in the input buffer.
          payload = in.subspan(pos, body_need_);
          pos += body_need_;
        } else {
          const std::size_t take =
              std::min(avail, static_cast<std::size_t>(body_need_ - body_fill_));
          std::memcpy(body_.data() + body_fill_, in.data() + pos, take);
          body_fill_ += static_cast<std::uint32_t>(take);
          pos += take;
          if (body_fill_ < body_need_) break;  // still incomplete
          payload = std::span<const std::byte>(body_.data(), body_need_);
        }

        ++frames_complete_;
        ++res.frames_emitted;
        stage_ = Stage::kHeader;
        header_fill_ = 0;
        header_anchor_ns_ = -1;
        body_anchor_ns_ = -1;
        const std::uint32_t emitted_len = body_need_;
        body_need_ = 0;
        body_fill_ = 0;
        (void)emitted_len;

        Reason handler_reason = Reason::kNone;
        const FrameAction action = handler.on_frame(payload, handler_reason);
        if (action == FrameAction::kPause) {
          res.paused = true;
          res.bytes_consumed = pos;
          bytes_fed_ += pos;
          return res;
        }
        if (action == FrameAction::kFatal) {
          enter_closed(handler_reason);
          res.fatal = true;
          res.reason = handler_reason;
          res.bytes_consumed = pos;
          bytes_fed_ += pos;
          return res;
        }
        break;
      }

      case Stage::kClosed:
        res.fatal = true;
        res.reason = closed_reason_;
        res.bytes_consumed = pos;
        bytes_fed_ += pos;
        return res;
    }
  }

  res.bytes_consumed = pos;
  bytes_fed_ += pos;

  const Reason dl = check_deadlines(now_ns);
  if (dl != Reason::kNone && stage_ != Stage::kClosed) {
    enter_closed(dl);
    res.fatal = true;
    res.reason = dl;
  }
  return res;
}

}  // namespace md
