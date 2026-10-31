// Protobuf codec, C++ side: payload bytes <-> DomainEvent plus the envelope
// check that must follow every parse.
//
// The same ordering rule as the Python side: ParseFromArray returning true
// means the bytes were syntactically a MarketDataEvent and nothing more. proto3
// has no required fields, so an empty buffer parses into a valid message with
// everything absent. Presence is therefore read with has_...(), never by
// comparing a value against zero, and a successful parse is always followed by
// CheckEnvelope.
#include "md/contract.hpp"

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/runtime_version.h>
#include <google/protobuf/unknown_field_set.h>

#include "marketdata/v1/market_data.pb.h"

namespace md {

namespace {

// An unknown field in the payload tag range means a newer writer used a oneof
// variant this build does not know. That is a different failure from "the oneof
// was never set", and conflating them would report a v2 depth update as an
// event with no payload (cases D20, G06).
bool HasUnknownPayloadVariant(const marketdata::v1::MarketDataEvent& msg) {
  const auto& unknown = msg.unknown_fields();
  for (int i = 0; i < unknown.field_count(); ++i) {
    const int number = unknown.field(i).number();
    if (number >= kPayloadTagMin && number <= kPayloadTagMax) return true;
  }
  return false;
}

}  // namespace

Error EncodeEvent(const DomainEvent& event, std::string& out) {
  marketdata::v1::MarketDataEvent msg;
  if (event.dataset_id) msg.set_dataset_id(*event.dataset_id);
  if (event.symbol) msg.set_symbol(*event.symbol);
  if (event.sequence) msg.set_sequence(*event.sequence);
  if (event.event_time_ns) msg.set_event_time_ns(*event.event_time_ns);
  if (event.price_scale) msg.set_price_scale(*event.price_scale);
  if (event.source_schema_version) {
    msg.set_source_schema_version(*event.source_schema_version);
  }

  switch (event.payload_case) {
    case PayloadCase::kQuote: {
      // mutable_quote() is what sets the oneof case, so a quote with neither
      // price present still encodes as payload_case == quote with both absent.
      auto* quote = msg.mutable_quote();
      if (event.bid_ticks) quote->set_bid_ticks(*event.bid_ticks);
      if (event.ask_ticks) quote->set_ask_ticks(*event.ask_ticks);
      break;
    }
    case PayloadCase::kTick: {
      auto* tick = msg.mutable_tick();
      if (event.price_ticks) tick->set_price_ticks(*event.price_ticks);
      break;
    }
    case PayloadCase::kNone:
      break;
    case PayloadCase::kUnknown:
      return {Reason::kUnsupportedEventKind, "cannot encode an unknown payload variant"};
  }

  out.clear();
  if (!msg.SerializeToString(&out)) {
    return {Reason::kDecodeError, "SerializeToString failed"};
  }
  if (out.size() > kMaxPayloadBytes) {
    return {Reason::kPayloadTooLarge,
            std::to_string(out.size()) + " > " + std::to_string(kMaxPayloadBytes)};
  }
  if (out.empty()) {
    return {Reason::kUnsupportedEventKind, "event serialises to 0 bytes"};
  }
  return Error::Ok();
}

Error DecodePayload(std::span<const char> payload, DomainEvent& out) {
  if (payload.size() > kMaxPayloadBytes) {
    return {Reason::kPayloadTooLarge,
            std::to_string(payload.size()) + " > " + std::to_string(kMaxPayloadBytes)};
  }

  // A fresh message per call. Reusing one and calling Clear() would be faster,
  // but a parse that fails part way leaves fields populated, and a stale oneof
  // from the previous event is exactly the leak cases B12 and E13 describe.
  marketdata::v1::MarketDataEvent msg;
  if (!msg.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    return {Reason::kDecodeError, "ParseFromArray returned false"};
  }

  out = DomainEvent{};
  if (msg.has_dataset_id()) out.dataset_id = msg.dataset_id();
  if (msg.has_symbol()) out.symbol = msg.symbol();
  if (msg.has_sequence()) out.sequence = msg.sequence();
  if (msg.has_event_time_ns()) out.event_time_ns = msg.event_time_ns();
  if (msg.has_price_scale()) out.price_scale = msg.price_scale();
  if (msg.has_source_schema_version()) out.source_schema_version = msg.source_schema_version();

  switch (msg.payload_case()) {
    case marketdata::v1::MarketDataEvent::kQuote: {
      out.payload_case = PayloadCase::kQuote;
      const auto& quote = msg.quote();
      if (quote.has_bid_ticks()) out.bid_ticks = quote.bid_ticks();
      if (quote.has_ask_ticks()) out.ask_ticks = quote.ask_ticks();
      break;
    }
    case marketdata::v1::MarketDataEvent::kTick: {
      out.payload_case = PayloadCase::kTick;
      const auto& tick = msg.tick();
      if (tick.has_price_ticks()) out.price_ticks = tick.price_ticks();
      break;
    }
    default:
      out.payload_case =
          HasUnknownPayloadVariant(msg) ? PayloadCase::kUnknown : PayloadCase::kNone;
      break;
  }
  return Error::Ok();
}

Error CheckEnvelope(const DomainEvent& event) {
  std::string missing;
  auto note = [&missing](std::string_view name) {
    if (!missing.empty()) missing += ',';
    missing += name;
  };
  if (!event.dataset_id) note("dataset_id");
  if (!event.symbol) note("symbol");
  if (!event.sequence) note("sequence");
  if (!event.event_time_ns) note("event_time_ns");
  if (!event.price_scale) note("price_scale");
  if (!event.source_schema_version) note("source_schema_version");
  if (!missing.empty()) {
    // Absent is never defaulted to zero. A message with no sequence is rejected
    // rather than accepted as sequence 0 (case B01).
    return {Reason::kMissingRequiredSemanticField, missing};
  }

  switch (event.payload_case) {
    case PayloadCase::kUnknown:
      return {Reason::kUnsupportedEventKind,
              "payload variant in tag range 10-19 is not known to this build"};
    case PayloadCase::kNone:
      return {Reason::kUnsupportedEventKind, "oneof payload not set"};
    case PayloadCase::kQuote: {
      std::string absent;
      if (!event.bid_ticks) absent += "bid_ticks";
      if (!event.ask_ticks) {
        if (!absent.empty()) absent += ',';
        absent += "ask_ticks";
      }
      if (!absent.empty()) {
        return {Reason::kMissingRequiredSemanticField, "quote." + absent};
      }
      break;
    }
    case PayloadCase::kTick:
      if (!event.price_ticks) {
        return {Reason::kMissingRequiredSemanticField, "tick.price_ticks"};
      }
      break;
  }
  return Error::Ok();
}

std::string ProtobufRuntimeVersion() {
  // Formatted by protobuf itself from the version these headers declare, so the
  // string in a report always matches what the library would say about itself.
  return google::protobuf::internal::VersionString(GOOGLE_PROTOBUF_VERSION);
}

void VerifyProtobufVersion() {
  // Compares the linked library against the headers this file compiled with and
  // aborts on a mismatch. Deliberately fatal: a process that loaded a different
  // libprotobuf than its gencode expects has undefined behaviour ahead of it,
  // and continuing would produce results nobody should trust (case E07).
  GOOGLE_PROTOBUF_VERIFY_VERSION;
}

}  // namespace md
