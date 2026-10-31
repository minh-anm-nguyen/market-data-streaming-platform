// Protobuf decode stage.
//
// This stage answers exactly one question: are these bytes a well-formed
// marketdata.v1.MarketDataEvent? Parse success is *not* validity -- proto3
// will happily parse a message with no fields set at all. Deciding whether the
// message means anything is the structural and business validators' job.
#pragma once

#include <cstddef>
#include <span>
#include <string>

#include "market_data.pb.h"
#include "md_adapter/status.hpp"

namespace md {

// Installs a no-op protobuf log handler (see protobuf_decoder.cpp).
void silence_protobuf_logging();

class ProtobufDecoder {
 public:
  struct Result {
    bool ok = false;
    Reason reason = Reason::kNone;
    bool unknown_fields = false;
  };

  ProtobufDecoder() = default;

  // Decodes into the internal message, which stays valid until the next call.
  // The message object is reused to keep allocation off the hot path.
  Result decode(std::span<const std::byte> payload);

  const marketdata::v1::MarketDataEvent& message() const noexcept {
    return msg_;
  }

  // Verifies the compiled-in descriptor against the digest the operator
  // pinned in config. An empty or "UNPINNED" expectation is accepted but
  // reported, so a run can never silently claim a verified contract.
  struct DescriptorCheck {
    bool pinned = false;
    bool matches = false;
    std::string actual_sha256;
    std::string expected_sha256;
  };
  static DescriptorCheck check_descriptor(const std::string& expected_sha256);

  // SHA-256 of the serialized FileDescriptorProto this binary was built with.
  static std::string descriptor_sha256();

 private:
  marketdata::v1::MarketDataEvent msg_;
};

}  // namespace md
