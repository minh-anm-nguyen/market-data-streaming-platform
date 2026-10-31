// MDP1 framing, C++ side. Mirrors md_contract/framing.py rule for rule.
#include "md/contract.hpp"

#include <cstdio>
#include <cstring>

namespace md {

std::int64_t MemorySource::ReadSome(char* buffer, std::size_t size, Error&) {
  if (chunk_ != 0) size = std::min(size, chunk_);
  const std::size_t available = data_.size() - pos_;
  const std::size_t n = std::min(size, available);
  if (n != 0) std::memcpy(buffer, data_.data() + pos_, n);
  pos_ += n;
  return static_cast<std::int64_t>(n);
}

std::int64_t FileSource::ReadSome(char* buffer, std::size_t size, Error& error) {
  const std::size_t n = std::fread(buffer, 1, size, handle_);
  if (n == 0 && std::ferror(handle_) != 0) {
    error = {Reason::kTruncatedBody, "read error"};
    return -1;
  }
  return static_cast<std::int64_t>(n);
}

bool FrameReader::ReadExact(char* buffer, std::size_t size, Reason truncated, Error& error) {
  std::size_t got = 0;
  while (got < size) {
    const std::int64_t n = source_.ReadSome(buffer + got, size - got, error);
    if (n < 0) return false;
    if (n == 0) {
      // Short is never acceptable for a header or a body. Three bytes of a
      // 4-byte length field is a truncated stream, not a small header.
      error = {truncated, "EOF mid-field", offset_ + static_cast<std::int64_t>(got)};
      return false;
    }
    got += static_cast<std::size_t>(n);
  }
  return true;
}

bool FrameReader::ReadMagic(Error& error) {
  if (magic_read_) return true;
  char prefix[kMagicLen];
  if (!ReadExact(prefix, kMagicLen, Reason::kTruncatedPrefix, error)) {
    end_ = StreamEnd::kIncomplete;
    return false;
  }
  offset_ += static_cast<std::int64_t>(kMagicLen);
  if (std::string_view(prefix, kMagicLen) != kMagic) {
    // A JSONL line sent to this port lands here. Fail closed: there is
    // deliberately no fallback to a JSON parser (cases D01, D02, I08).
    end_ = StreamEnd::kFailed;
    error = {Reason::kBadMagic,
             "expected MDP1, got " + std::string(prefix, kMagicLen), 0};
    return false;
  }
  magic_read_ = true;
  return true;
}

bool FrameReader::ReadFrame(std::string& out, Error& error) {
  if (!ReadMagic(error)) return false;

  // Zero bytes at a frame boundary is the one legitimate stopping point;
  // 1 to 3 bytes is a truncated header (cases D04-D06).
  char header[kHeaderLen];
  std::size_t got = 0;
  while (got < kHeaderLen) {
    const std::int64_t n = source_.ReadSome(header + got, kHeaderLen - got, error);
    if (n < 0) { end_ = StreamEnd::kIncomplete; return false; }
    if (n == 0) {
      if (got == 0) {
        end_ = StreamEnd::kCompleted;
        return false;
      }
      end_ = StreamEnd::kIncomplete;
      error = {Reason::kTruncatedHeader, "EOF mid-header",
               offset_ + static_cast<std::int64_t>(got)};
      return false;
    }
    got += static_cast<std::size_t>(n);
  }

  const std::int64_t header_offset = offset_;
  offset_ += static_cast<std::int64_t>(kHeaderLen);

  // Big-endian, assembled by hand rather than with ntohl: this must not depend
  // on the host's byte order, and it must behave identically on a 32-bit build
  // (case E10). The cast through uint32_t keeps the shift well-defined.
  const std::uint32_t length =
      (static_cast<std::uint32_t>(static_cast<unsigned char>(header[0])) << 24) |
      (static_cast<std::uint32_t>(static_cast<unsigned char>(header[1])) << 16) |
      (static_cast<std::uint32_t>(static_cast<unsigned char>(header[2])) << 8) |
      (static_cast<std::uint32_t>(static_cast<unsigned char>(header[3])));

  if (length == 0 || length > max_payload_bytes_) {
    // Checked on the integer, before any allocation. A header claiming
    // 0xffffffff must never cause a 4 GiB reserve, and a little-endian sender's
    // swapped header is failed closed rather than guessed at
    // (cases D07, D08, D09, D10, J03).
    end_ = StreamEnd::kFailed;
    error = {Reason::kInvalidLength,
             "length " + std::to_string(length) + " outside 1.." +
                 std::to_string(max_payload_bytes_),
             header_offset};
    return false;
  }

  out.assign(length, '\0');
  if (!ReadExact(out.data(), length, Reason::kTruncatedBody, error)) {
    end_ = StreamEnd::kIncomplete;
    out.clear();
    return false;
  }
  offset_ += static_cast<std::int64_t>(length);
  ++frames_read_;
  return true;
}

Error FrameWriter::WriteFrame(std::string_view payload) {
  if (payload.empty()) {
    return {Reason::kInvalidLength, "refusing to write a 0-length frame"};
  }
  if (payload.size() > kMaxPayloadBytes) {
    return {Reason::kInvalidLength,
            "payload " + std::to_string(payload.size()) + " > " +
                std::to_string(kMaxPayloadBytes)};
  }
  if (!magic_written_) {
    if (std::fwrite(kMagic.data(), 1, kMagic.size(), handle_) != kMagic.size()) {
      return {Reason::kTruncatedPrefix, "short write on magic"};
    }
    magic_written_ = true;
  }
  const std::uint32_t n = static_cast<std::uint32_t>(payload.size());
  const unsigned char header[kHeaderLen] = {
      static_cast<unsigned char>((n >> 24) & 0xff),
      static_cast<unsigned char>((n >> 16) & 0xff),
      static_cast<unsigned char>((n >> 8) & 0xff),
      static_cast<unsigned char>(n & 0xff),
  };
  if (std::fwrite(header, 1, kHeaderLen, handle_) != kHeaderLen) {
    return {Reason::kTruncatedHeader, "short write on header"};
  }
  if (std::fwrite(payload.data(), 1, payload.size(), handle_) != payload.size()) {
    return {Reason::kTruncatedBody, "short write on payload"};
  }
  ++frames_written_;
  return Error::Ok();
}

}  // namespace md
