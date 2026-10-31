// Protobuf encode and decode for the Kafka record value
// (FR-03, NFR-01, section 3.1, cases D-01, D-02, D-06, D-07, D-09, D-14).
//
// The contract is narrow on purpose: kafka.record.value is exactly
// MarketDataEvent.SerializeToString(). No MDP1 magic, no big-endian length
// prefix, no envelope of any kind. Those framings belong to the Project 02 TCP
// and .pbstream transports and are a different protocol; a Kafka record already
// has a length and a boundary, so repeating it is pure confusion.
//
// Two guards exist because confusion is the realistic failure, not malice:
// LooksLikeFramedPayload rejects a value that was taken from a .pbstream or a
// TCP frame and handed to Kafka unchanged (D-01, D-02).
//
// Equality is semantic, never byte-wise. Section 3.1 and case D-14 both say so:
// two protobuf runtimes may serialise the same message to different valid byte
// strings, so the oracle compares the typed projection, and SemanticEqual is
// that comparison in one place so no test invents its own.
#ifndef P04_SERIALIZATION_MARKETDATA_CODEC_H_
#define P04_SERIALIZATION_MARKETDATA_CODEC_H_

#include <string>
#include <string_view>

#include "handoff/output_event.h"

namespace p04 {

enum class CodecError {
  kNone,
  kParseFailed,          // not a valid MarketDataEvent on the wire
  kFramedPayload,        // MDP1 magic or a length prefix was found (D-01, D-02)
  kEmptyPayload,
  kSerializeFailed,
  kOneofUnset,           // parsed, but the payload oneof is not set (D-04)
};

const char* ToString(CodecError error);

// Serialises the present fields of a typed event, leaving absent fields absent.
//
// This is where proto3 optional earns its place: an absent sequence must not be
// written as 0, because a reader cannot then tell a missing sequence from a real
// sequence 0. Returns false and sets error on failure; out is left untouched.
bool Encode(const TypedEvent& event, std::string* out, CodecError* error);

// Strict decode. Rejects framed payloads before parsing, rejects a parse
// failure, and preserves field presence in the output.
//
// It deliberately does not reject an unset oneof: that is a routing decision
// (D-04) made by RouteTable, and a verifier needs to be able to decode such a
// record in order to report it. Callers that require a payload check
// TypedEvent::kind themselves.
bool Decode(std::string_view bytes, TypedEvent* out, CodecError* error);

// Whether a buffer looks like it carries a transport framing that must never
// appear in a Kafka value.
//
// Detects the 4-byte MDP1 magic, and a big-endian 32-bit length prefix whose
// value equals the remaining byte count. The second heuristic is safe because a
// valid protobuf message can never begin with a 0x00 byte: field number 0 does
// not exist, so a leading zero byte is already malformed. A length prefix for
// any payload under 16 MiB starts with at least one zero byte.
bool LooksLikeFramedPayload(std::string_view bytes);

// Semantic equality over the core contract fields, including presence.
//
// Presence is part of the comparison: an event with price_scale absent is not
// equal to one with price_scale = 0, even though both would serialise to a
// short message. NFR-01 depends on this distinction being visible.
bool SemanticEqual(const TypedEvent& left, const TypedEvent& right);

// A stable human-readable projection of the core fields, used in test failure
// messages and in the diagnostic artifacts. Not a wire format and not an
// equality oracle; SemanticEqual is.
std::string DescribeCore(const TypedEvent& event);

// SHA-256 of the serialized FileDescriptorProto for the pinned schema.
//
// Taken from the descriptor rather than from the .proto text, so reformatting a
// comment does not change the pin while adding a field does. Section 14 records
// this as proto_descriptor_sha256.
std::string PinnedDescriptorSha256();

// The descriptor full name, e.g. "marketdata.v1.MarketDataEvent". Checked at
// startup against the configured expectation so a build linked against a
// different generated schema fails before publishing (case D-08).
std::string PinnedMessageFullName();

// The linked protobuf runtime version, recorded next to the descriptor digest.
// Exposed here rather than read from a protobuf macro at the call site, so that
// protobuf headers stay confined to this translation unit and the rest of the
// project cannot accidentally depend on them.
std::string ProtobufRuntimeVersion();

}  // namespace p04

#endif  // P04_SERIALIZATION_MARKETDATA_CODEC_H_
