// Project 02 C++ side of the marketdata.v1 contract.
//
// This is an *independent* implementation of the same rules the Python package
// enforces, and that independence is the point. If the oracle were "decode with
// the same code that encoded", a shared bug would pass every test (section 9.1).
// So the strict JSON reader, the framing reader and the canonical projection
// writer are all written separately here, and the two languages are compared on
// byte-identical projection output.
//
// Synthetic data only. Not market data, not usable for trading.
#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace md {

// ---------------------------------------------------------------------------
// Budgets (section 6.1). Kept identical to the Python constants on purpose; the
// conformance suite asserts the two agree so a limit cannot drift on one side.
// ---------------------------------------------------------------------------
constexpr std::size_t kMaxPayloadBytes = 4096;
constexpr std::size_t kMaxJsonLineBytes = 4096;
constexpr std::size_t kMaxSymbolBytes = 32;
constexpr std::size_t kMaxDatasetIdBytes = 128;
constexpr std::size_t kMagicLen = 4;
constexpr std::size_t kHeaderLen = 4;
constexpr int kMaxNumericDigits = 19;
constexpr std::string_view kMagic = "MDP1";
constexpr std::string_view kSourceSchemaVersion = "sim.v1";

// Tags 10-19 are the payload oneof range; an unknown field in that range is a
// newer writer's variant, not an event without a payload (cases D20, G06).
constexpr int kPayloadTagMin = 10;
constexpr int kPayloadTagMax = 19;

// ---------------------------------------------------------------------------
// Rejection reasons. The spellings match md_contract.errors.Reason exactly, so
// a report produced by either language uses one vocabulary.
// ---------------------------------------------------------------------------
enum class Reason {
  kOk,
  kBadMagic,
  kTruncatedPrefix,
  kTruncatedHeader,
  kInvalidLength,
  kTruncatedBody,
  kReadDeadlineExceeded,
  kDecodeError,
  kPayloadTooLarge,
  kMissingRequiredSemanticField,
  kUnsupportedEventKind,
  kStringTooLong,
  kInvalidUtf8,
  kInvalidSymbol,
  kInvalidDatasetId,
  kUnsupportedSourceSchemaVersion,
  kJsonLineTooLarge,
  kInvalidJson,
  kNotAnObject,
  kDuplicateJsonKey,
  kFloatNotAllowed,
  kNonFiniteNumber,
  kFieldNotInteger,
  kInt64OutOfRange,
  kNumericTokenTooLong,
  kFieldSetMismatch,
  kUnknownEventType,
  kBusinessViolation,
};

std::string_view ReasonName(Reason reason);

// An error carrying a stable reason and a human detail. Returned rather than
// thrown on the hot paths so a corpus run can classify thousands of frames
// without unwinding.
struct Error {
  Reason reason = Reason::kOk;
  std::string detail;
  std::int64_t offset = -1;

  bool ok() const { return reason == Reason::kOk; }
  static Error Ok() { return {}; }
  std::string ToString() const;
};

// ---------------------------------------------------------------------------
// The domain event. std::optional is how presence is carried: an empty optional
// means the field was absent on the wire, and is never conflated with zero.
// ---------------------------------------------------------------------------
enum class PayloadCase { kNone, kQuote, kTick, kUnknown };

std::string_view PayloadCaseName(PayloadCase payload_case);

struct DomainEvent {
  std::optional<std::string> dataset_id;
  std::optional<std::string> symbol;
  std::optional<std::int64_t> sequence;
  std::optional<std::int64_t> event_time_ns;
  std::optional<std::int64_t> price_scale;
  std::optional<std::string> source_schema_version;
  PayloadCase payload_case = PayloadCase::kNone;
  std::optional<std::int64_t> bid_ticks;
  std::optional<std::int64_t> ask_ticks;
  std::optional<std::int64_t> price_ticks;
};

// ---------------------------------------------------------------------------
// Canonical projection (ADR-005). Must be byte-identical to
// md_contract.domain.projection_json for the same event: same key order, same
// escaping, same omission of absent fields.
// ---------------------------------------------------------------------------
std::string ProjectionJson(const DomainEvent& event);

// Minimal JSON string escaping matching Python's json.dumps(ensure_ascii=False):
// backslash and quote escaped, \b \f \n \r \t as short forms, other control
// characters as \u00xx, and every other byte passed through as UTF-8.
void AppendJsonString(std::string& out, std::string_view value);

// ---------------------------------------------------------------------------
// Strict sim.v1 JSONL reader. Independent of the Python implementation.
// ---------------------------------------------------------------------------
Error ParseSimV1Line(std::string_view line, DomainEvent& out);

bool ValidUtf8(std::string_view text);

// ---------------------------------------------------------------------------
// Protobuf codec: payload bytes <-> DomainEvent, plus the envelope check that
// must follow every successful parse.
// ---------------------------------------------------------------------------
Error DecodePayload(std::span<const char> payload, DomainEvent& out);
Error EncodeEvent(const DomainEvent& event, std::string& out);
Error CheckEnvelope(const DomainEvent& event);

// ---------------------------------------------------------------------------
// MDP1 framing.
// ---------------------------------------------------------------------------
enum class StreamEnd { kCompleted, kIncomplete, kFailed };

std::string_view StreamEndName(StreamEnd end);

// A byte source so the framing logic is shared by files, memory and sockets.
class ByteSource {
 public:
  virtual ~ByteSource() = default;
  // Returns the number of bytes read into `buffer` (0 at EOF), or -1 on error
  // with `error` populated.
  virtual std::int64_t ReadSome(char* buffer, std::size_t size, Error& error) = 0;
};

class MemorySource final : public ByteSource {
 public:
  // `chunk` forces fragmentation so reassembly is exercised (cases D16, D17).
  MemorySource(std::span<const char> data, std::size_t chunk = 0)
      : data_(data), chunk_(chunk) {}
  std::int64_t ReadSome(char* buffer, std::size_t size, Error& error) override;

 private:
  std::span<const char> data_;
  std::size_t chunk_;
  std::size_t pos_ = 0;
};

class FileSource final : public ByteSource {
 public:
  explicit FileSource(std::FILE* handle) : handle_(handle) {}
  std::int64_t ReadSome(char* buffer, std::size_t size, Error& error) override;

 private:
  std::FILE* handle_;
};

class FrameReader {
 public:
  explicit FrameReader(ByteSource& source, std::size_t max_payload_bytes = kMaxPayloadBytes)
      : source_(source), max_payload_bytes_(max_payload_bytes) {}

  // Reads the next payload into `out`. Returns true on a frame, false on a
  // clean end of stream, and sets `error` on any structural failure.
  bool ReadFrame(std::string& out, Error& error);

  StreamEnd end() const { return end_; }
  std::int64_t offset() const { return offset_; }
  std::int64_t frames_read() const { return frames_read_; }

 private:
  bool ReadMagic(Error& error);
  bool ReadExact(char* buffer, std::size_t size, Reason truncated, Error& error);

  ByteSource& source_;
  std::size_t max_payload_bytes_;
  bool magic_read_ = false;
  std::int64_t offset_ = 0;
  std::int64_t frames_read_ = 0;
  StreamEnd end_ = StreamEnd::kIncomplete;
};

class FrameWriter {
 public:
  explicit FrameWriter(std::FILE* handle) : handle_(handle) {}
  Error WriteFrame(std::string_view payload);
  std::int64_t frames_written() const { return frames_written_; }

 private:
  std::FILE* handle_;
  bool magic_written_ = false;
  std::int64_t frames_written_ = 0;
};

// Version reporting and enforcement (cases E05, E07, NFR-10).
//
// For C++ there is no ABI promise across protobuf releases, so a gencode and
// runtime mismatch must be a hard stop. Three mechanisms, at three different
// times, and none of them replaces the others:
//
//  * the generated header carries `#if PROTOBUF_VERSION != <n>` / `#error`, so
//    compiling gencode against the wrong headers fails the build;
//  * VerifyProtobufVersion() checks the *linked* library against the headers
//    this translation unit compiled with, which is the only way to catch a
//    process that loaded a different libprotobuf than it was built for;
//  * the version string goes into the report, so a result can be attributed to
//    a specific toolchain rather than to "protobuf".
std::string ProtobufRuntimeVersion();

// Aborts (via protobuf's own check) if the linked library disagrees with the
// headers. Called at the start of every tool rather than being left implicit.
void VerifyProtobufVersion();

}  // namespace md
