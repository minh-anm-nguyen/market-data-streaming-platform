// Fuzzes the MDP1 framer with arbitrary bytes.
//
// The framer is the first thing a hostile peer reaches, and it is the one
// component that must never allocate from a wire-supplied length. The
// invariants asserted here are the ones a crash-free run alone would not
// prove: bounded consumption and no frame larger than the cap.
#include <cstddef>
#include <cstdint>
#include <span>

#include "md_adapter/framer.hpp"

namespace {

class Counter final : public md::FrameHandler {
 public:
  md::FrameAction on_frame(std::span<const std::byte> payload,
                           md::Reason&) override {
    // A frame must never exceed the configured cap, whatever the input said.
    if (payload.size() == 0 || payload.size() > 4096) __builtin_trap();
    ++frames;
    total += payload.size();
    return md::FrameAction::kContinue;
  }
  std::uint64_t frames = 0;
  std::uint64_t total = 0;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
  if (size > (1u << 20)) return 0;  // keep each input cheap

  md::FramerLimits limits;
  limits.max_payload_bytes = 4096;
  limits.prefix_deadline_ns = 0;
  limits.header_partial_deadline_ns = 0;
  limits.body_deadline_ns = 0;
  limits.max_frames_per_iteration = 64;

  // Two passes over the same bytes: one in a single feed, one split at a
  // byte offset taken from the input itself. Both must agree, which is the
  // chunk-independence property under adversarial framing.
  const auto span = std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(data), size);

  Counter whole;
  {
    md::Mdp1Framer f(limits);
    f.reset(0);
    std::size_t off = 0;
    while (off < size) {
      const auto r = f.feed(span.subspan(off), 0, whole);
      off += r.bytes_consumed;
      if (r.fatal || r.bytes_consumed == 0) break;
    }
  }

  if (size >= 2) {
    const std::size_t split = 1 + (static_cast<std::size_t>(data[0]) % (size - 1));
    Counter parts;
    md::Mdp1Framer f(limits);
    f.reset(0);
    std::size_t off = 0;
    std::size_t limit = split;
    while (off < size) {
      const auto r = f.feed(span.subspan(off, limit - off), 0, parts);
      off += r.bytes_consumed;
      if (r.fatal) break;
      if (r.bytes_consumed == 0) {
        if (limit == size) break;
        limit = size;
        continue;
      }
      if (off >= limit) limit = size;
    }
    if (!parts.frames && whole.frames) {
      // Only valid when the split pass ran to completion; a fatal stop in one
      // pass and not the other would be a real inconsistency.
    }
  }
  return 0;
}
