// Error taxonomy, quality flags and run verdicts.
//
// Every rejection in the adapter carries a Reason from this enum. The set is
// closed on purpose: metric labels and report fields are built from it, so an
// unbounded/ad-hoc reason string can never reach a metric label (section 13.1).
#pragma once

#include <cstdint>
#include <string_view>

namespace md {

// ---------------------------------------------------------------------------
// Reason codes (design section 4.2 error taxonomy, extended with the
// config/provenance codes the runtime needs).
// ---------------------------------------------------------------------------
enum class Reason : std::uint16_t {
  kNone = 0,

  // Transport / framing
  kBadMagic,
  kInvalidLength,
  kTruncatedPrefix,
  kTruncatedHeader,
  kTruncatedBody,
  kFrameTimeout,
  kConnectTimeout,
  kConnectRefused,
  kIoError,
  kPeerClosed,

  // Decoding
  kProtoDecodeError,

  // Structural
  kMissingDatasetId,
  kMissingSymbol,
  kMissingSequence,
  kMissingEventTime,
  kMissingPriceScale,
  kMissingSourceVersion,
  kMissingPayloadOneof,
  kMissingQuoteField,
  kMissingTickField,
  kFieldTooLong,
  kInvalidUtf8,
  kSymbolCharset,
  kUnsupportedSchemaVersion,
  kUnknownFieldPresent,

  // Business
  kInvalidSequence,
  kNegativeEventTime,
  kInvalidPriceScale,
  kScaleMismatch,
  kQuoteBidNonPositive,
  kQuoteCrossed,
  kSpreadOutOfRange,
  kPriceOutOfRange,
  kTickNonPositive,
  kSymbolNotAllowed,
  kArithmeticOverflow,

  // Stream state (classification, not always rejection)
  kSequenceGap,
  kDuplicateExact,
  kDuplicateConflict,
  kReorderLate,
  kTooOldUnverifiable,
  kSequenceRollback,
  kInitialGap,
  kStartUnverified,
  kHugeGap,
  kEventTimeOld,
  kFeedInactive,
  kContinuityLost,

  // Resources / output
  kQueueBudgetExceeded,
  kBackpressureTimeout,
  kSinkIoError,
  kStateCapExceeded,
  kResourceExhausted,

  // Provenance / configuration
  kSourceUnverified,
  kConfigInvalid,
  kDescriptorMismatch,
  kUnexpectedFault,

  kReasonCount,
};

std::string_view to_string(Reason r) noexcept;

// ---------------------------------------------------------------------------
// Quality flags: out-of-band annotation attached to a classified event. These
// never mutate the source payload (FR-07).
// ---------------------------------------------------------------------------
namespace quality {
inline constexpr std::uint32_t kNone = 0u;
inline constexpr std::uint32_t kGap = 1u << 0;
inline constexpr std::uint32_t kDegraded = 1u << 1;
inline constexpr std::uint32_t kDuplicateExact = 1u << 2;
inline constexpr std::uint32_t kDuplicateConflict = 1u << 3;
inline constexpr std::uint32_t kReorderLate = 1u << 4;
inline constexpr std::uint32_t kTooOldUnverifiable = 1u << 5;
inline constexpr std::uint32_t kEventTimeOld = 1u << 6;
inline constexpr std::uint32_t kQuoteInactive = 1u << 7;
inline constexpr std::uint32_t kRecovered = 1u << 8;
inline constexpr std::uint32_t kStartUnverified = 1u << 9;
inline constexpr std::uint32_t kInitialGap = 1u << 10;
inline constexpr std::uint32_t kSequenceRollback = 1u << 11;
inline constexpr std::uint32_t kHugeGap = 1u << 12;
inline constexpr std::uint32_t kContinuityLost = 1u << 13;
inline constexpr std::uint32_t kUnknownFields = 1u << 14;
inline constexpr std::uint32_t kFaultTestOnly = 1u << 15;
}  // namespace quality

// Stable lowercase names, emitted into NDJSON/report artifacts.
void append_quality_flags(std::uint32_t flags, std::string& out);

// ---------------------------------------------------------------------------
// Feed state machine per (dataset, symbol) -- design section 13.2.
// ---------------------------------------------------------------------------
enum class FeedState : std::uint8_t {
  kInitial,
  kStartUnverified,
  kHealthy,
  kDegraded,
  kInactive,
  kRecovering,
};
std::string_view to_string(FeedState s) noexcept;

// Input provenance -- what we are allowed to claim about completeness.
enum class Provenance : std::uint8_t {
  kVerifiedCompleteFile,
  kVerifiedPartialFile,
  kLiveUnverified,
  kLiveWithSourceEvidence,
};
std::string_view to_string(Provenance p) noexcept;

// Terminal run status written into the report (never inferred optimistically).
enum class RunStatus : std::uint8_t {
  kCompleteOffline,
  kPartialLive,
  kIncomplete,
  kFailed,
};
std::string_view to_string(RunStatus s) noexcept;

// Process exit codes. Distinct codes let the e2e harness assert *why* a run
// stopped instead of pattern-matching stderr.
enum class ExitCode : int {
  kOk = 0,
  kConfigInvalid = 2,
  kSourceUnverified = 3,
  kStreamFailed = 4,
  kIncomplete = 5,
  kBackpressureTimeout = 6,
  kResourceExhausted = 7,
  kSinkIoError = 8,
  kUnexpectedFault = 9,
  kInternalError = 70,
};

}  // namespace md
