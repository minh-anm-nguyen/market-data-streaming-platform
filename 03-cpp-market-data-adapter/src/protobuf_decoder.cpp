#include "md_adapter/decoder.hpp"

#include <google/protobuf/descriptor.pb.h>
#include <climits>
#include <google/protobuf/stubs/common.h>

#include "md_adapter/sha256.hpp"

namespace md {
namespace {

// Detects unknown fields anywhere in the message. A v1 consumer must not
// invent meaning for tags it does not know (design 3.4.7), but it must know
// they were there. Reflection is used because the generated accessor for the
// unknown field set is not part of the public message API in protobuf 3.12.
bool unknown_in(const google::protobuf::Message& m) {
  return m.GetReflection()->GetUnknownFields(m).field_count() > 0;
}

bool has_unknown_fields(const marketdata::v1::MarketDataEvent& m) {
  if (unknown_in(m)) return true;
  if (m.has_quote() && unknown_in(m.quote())) return true;
  if (m.has_tick() && unknown_in(m.tick())) return true;
  return false;
}

void null_log_handler(google::protobuf::LogLevel, const char*, int,
                      const std::string&) {}

}  // namespace

// Protobuf logs parse failures to stderr by default. At adversarial input
// rates that is itself a denial-of-service vector, and the adapter already
// reports every rejection through its own bounded counters.
void silence_protobuf_logging() {
  static bool done = false;
  if (!done) {
    google::protobuf::SetLogHandler(&null_log_handler);
    done = true;
  }
}

ProtobufDecoder::Result ProtobufDecoder::decode(
    std::span<const std::byte> payload) {
  Result r;
  // Clear explicitly: a failed parse must never leave fields from the previous
  // message visible to the validators.
  msg_.Clear();

  if (payload.size() > static_cast<std::size_t>(INT32_MAX)) {
    r.reason = Reason::kProtoDecodeError;
    return r;
  }
  const bool ok = msg_.ParseFromArray(payload.data(),
                                      static_cast<int>(payload.size()));
  if (!ok) {
    msg_.Clear();
    r.reason = Reason::kProtoDecodeError;
    return r;
  }
  r.ok = true;
  r.unknown_fields = has_unknown_fields(msg_);
  return r;
}

std::string ProtobufDecoder::descriptor_sha256() {
  google::protobuf::FileDescriptorSet set;
  marketdata::v1::MarketDataEvent::descriptor()->file()->CopyTo(set.add_file());
  std::string bytes;
  set.SerializeToString(&bytes);
  return sha256_hex(bytes);
}

ProtobufDecoder::DescriptorCheck ProtobufDecoder::check_descriptor(
    const std::string& expected) {
  DescriptorCheck c;
  c.actual_sha256 = descriptor_sha256();
  c.expected_sha256 = expected;
  c.pinned = !expected.empty() && expected != "UNPINNED";
  c.matches = !c.pinned || (expected == c.actual_sha256);
  return c;
}

}  // namespace md
