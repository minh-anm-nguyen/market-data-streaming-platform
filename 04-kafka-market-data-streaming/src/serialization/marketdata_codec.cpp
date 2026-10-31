#include "serialization/marketdata_codec.h"

#include <google/protobuf/descriptor.pb.h>

#include <cstdint>
#include <cstring>

#include "market_data.pb.h"
#include "ops/sha256.h"

namespace p04 {
namespace {

using marketdata::v1::MarketDataEvent;

// The MDP1 magic belongs to the Project 02 TCP and .pbstream transports. Seeing
// it in a Kafka value means a framed buffer was forwarded without unwrapping.
constexpr char kMdp1Magic[4] = {'M', 'D', 'P', '1'};

std::string OptionalInt(const std::optional<std::int64_t>& value) {
  return value.has_value() ? std::to_string(*value) : std::string("<absent>");
}

std::string OptionalStr(const std::optional<std::string>& value) {
  return value.has_value() ? *value : std::string("<absent>");
}

}  // namespace

const char* ToString(CodecError error) {
  switch (error) {
    case CodecError::kNone:            return "NONE";
    case CodecError::kParseFailed:     return "PARSE_FAILED";
    case CodecError::kFramedPayload:   return "WRONG_FORMAT_FRAMED_PAYLOAD";
    case CodecError::kEmptyPayload:    return "EMPTY_PAYLOAD";
    case CodecError::kSerializeFailed: return "SERIALIZE_FAILED";
    case CodecError::kOneofUnset:      return "MISSING_PAYLOAD";
  }
  return "UNMAPPED";
}

bool LooksLikeFramedPayload(std::string_view bytes) {
  if (bytes.size() >= 4 && std::memcmp(bytes.data(), kMdp1Magic, 4) == 0) {
    return true;
  }
  if (bytes.size() >= 4) {
    const auto* u = reinterpret_cast<const unsigned char*>(bytes.data());
    const std::uint32_t be32 = (static_cast<std::uint32_t>(u[0]) << 24) |
                               (static_cast<std::uint32_t>(u[1]) << 16) |
                               (static_cast<std::uint32_t>(u[2]) << 8) |
                               static_cast<std::uint32_t>(u[3]);
    if (be32 == bytes.size() - 4) return true;
  }
  // A protobuf message can never start with 0x00: the first byte is a tag, and
  // field number 0 is not legal. So a leading zero byte is by itself enough to
  // say this is not a bare MarketDataEvent, which catches a length prefix whose
  // payload length happens not to match.
  return !bytes.empty() && bytes[0] == '\0';
}

bool Encode(const TypedEvent& event, std::string* out, CodecError* error) {
  if (error != nullptr) *error = CodecError::kNone;
  MarketDataEvent message;
  // Only present fields are set. set_x() on a proto3 optional field marks it
  // present, so skipping the call is what keeps an absent field absent; this is
  // the whole reason the typed event carries std::optional rather than a
  // sentinel.
  if (event.dataset_id) message.set_dataset_id(*event.dataset_id);
  if (event.symbol) message.set_symbol(*event.symbol);
  if (event.sequence) message.set_sequence(*event.sequence);
  if (event.event_time_ns) message.set_event_time_ns(*event.event_time_ns);
  if (event.price_scale) message.set_price_scale(*event.price_scale);
  if (event.source_schema_version) {
    message.set_source_schema_version(*event.source_schema_version);
  }
  if (event.kind.has_value()) {
    if (*event.kind == EventKind::kQuote) {
      auto* quote = message.mutable_quote();
      if (event.quote.bid_ticks) quote->set_bid_ticks(*event.quote.bid_ticks);
      if (event.quote.ask_ticks) quote->set_ask_ticks(*event.quote.ask_ticks);
    } else {
      auto* tick = message.mutable_tick();
      if (event.tick.price_ticks) tick->set_price_ticks(*event.tick.price_ticks);
    }
  }

  std::string buffer;
  if (!message.SerializeToString(&buffer)) {
    if (error != nullptr) *error = CodecError::kSerializeFailed;
    return false;
  }
  out->swap(buffer);
  return true;
}

bool Decode(std::string_view bytes, TypedEvent* out, CodecError* error) {
  if (error != nullptr) *error = CodecError::kNone;
  if (bytes.empty()) {
    if (error != nullptr) *error = CodecError::kEmptyPayload;
    return false;
  }
  // Framing is checked before parsing. An MDP1-prefixed buffer would otherwise
  // sometimes parse as a valid-looking message with garbage fields, which is
  // exactly the silent corruption cases D-01 and D-02 exist to prevent.
  if (LooksLikeFramedPayload(bytes)) {
    if (error != nullptr) *error = CodecError::kFramedPayload;
    return false;
  }

  MarketDataEvent message;
  if (!message.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
    if (error != nullptr) *error = CodecError::kParseFailed;
    return false;
  }

  TypedEvent decoded;
  if (message.has_dataset_id()) decoded.dataset_id = message.dataset_id();
  if (message.has_symbol()) decoded.symbol = message.symbol();
  if (message.has_sequence()) decoded.sequence = message.sequence();
  if (message.has_event_time_ns()) decoded.event_time_ns = message.event_time_ns();
  if (message.has_price_scale()) decoded.price_scale = message.price_scale();
  if (message.has_source_schema_version()) {
    decoded.source_schema_version = message.source_schema_version();
  }
  switch (message.payload_case()) {
    case MarketDataEvent::kQuote:
      decoded.kind = EventKind::kQuote;
      if (message.quote().has_bid_ticks()) decoded.quote.bid_ticks = message.quote().bid_ticks();
      if (message.quote().has_ask_ticks()) decoded.quote.ask_ticks = message.quote().ask_ticks();
      break;
    case MarketDataEvent::kTick:
      decoded.kind = EventKind::kTick;
      if (message.tick().has_price_ticks()) decoded.tick.price_ticks = message.tick().price_ticks();
      break;
    case MarketDataEvent::PAYLOAD_NOT_SET:
      // Left as std::nullopt. Not an error here by design: the verifier must be
      // able to decode such a record in order to report it as a contract
      // violation, and RouteTable is what refuses to publish one (D-04).
      break;
  }
  *out = std::move(decoded);
  return true;
}

bool SemanticEqual(const TypedEvent& left, const TypedEvent& right) {
  if (left.dataset_id != right.dataset_id) return false;
  if (left.symbol != right.symbol) return false;
  if (left.sequence != right.sequence) return false;
  if (left.event_time_ns != right.event_time_ns) return false;
  if (left.price_scale != right.price_scale) return false;
  if (left.source_schema_version != right.source_schema_version) return false;
  if (left.kind != right.kind) return false;
  if (!left.kind.has_value()) return true;
  if (*left.kind == EventKind::kQuote) {
    return left.quote.bid_ticks == right.quote.bid_ticks &&
           left.quote.ask_ticks == right.quote.ask_ticks;
  }
  return left.tick.price_ticks == right.tick.price_ticks;
}

std::string DescribeCore(const TypedEvent& event) {
  std::string out = "dataset_id=" + OptionalStr(event.dataset_id);
  out += " symbol=" + OptionalStr(event.symbol);
  out += " sequence=" + OptionalInt(event.sequence);
  out += " event_time_ns=" + OptionalInt(event.event_time_ns);
  out += " price_scale=" + OptionalInt(event.price_scale);
  out += " schema=" + OptionalStr(event.source_schema_version);
  if (!event.kind.has_value()) {
    out += " payload=<unset>";
    return out;
  }
  if (*event.kind == EventKind::kQuote) {
    out += " quote{bid=" + OptionalInt(event.quote.bid_ticks) +
           " ask=" + OptionalInt(event.quote.ask_ticks) + "}";
  } else {
    out += " tick{price=" + OptionalInt(event.tick.price_ticks) + "}";
  }
  return out;
}

std::string PinnedDescriptorSha256() {
  // Hashing the serialized FileDescriptorProto, not the .proto text, so that
  // editing a comment does not change the pin while adding or renumbering a
  // field does. The digest is therefore a function of the schema as this binary
  // understands it, which is what the run report needs to record; it is not
  // guaranteed stable across protobuf major versions, and the report records
  // the protobuf version next to it for exactly that reason.
  google::protobuf::FileDescriptorProto file_proto;
  marketdata::v1::MarketDataEvent::descriptor()->file()->CopyTo(&file_proto);
  std::string serialized;
  if (!file_proto.SerializeToString(&serialized)) return std::string();
  return Sha256::OfString(serialized);
}

std::string PinnedMessageFullName() {
  return marketdata::v1::MarketDataEvent::descriptor()->full_name();
}

std::string ProtobufRuntimeVersion() {
  // GOOGLE_PROTOBUF_VERSION is a packed integer, major*1000000 + minor*1000 +
  // patch. Decoded here so the report shows 3.21.12 rather than 3021012.
  const int packed = GOOGLE_PROTOBUF_VERSION;
  const int major = packed / 1000000;
  const int minor = (packed / 1000) % 1000;
  const int patch = packed % 1000;
  return std::to_string(major) + "." + std::to_string(minor) + "." +
         std::to_string(patch);
}

}  // namespace p04
