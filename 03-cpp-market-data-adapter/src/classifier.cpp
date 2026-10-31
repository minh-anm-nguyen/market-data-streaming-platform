#include "md_adapter/classifier.hpp"

namespace md {

Classifier::Decision Classifier::classify(
    const DomainEvent& e, const SeqResult& seq,
    const FreshnessEngine::Result& fresh) const {
  Decision d;
  d.quality_flags = seq.quality_flags | fresh.quality_flags;
  if (e.unknown_fields) d.quality_flags |= quality::kUnknownFields;
  if (seq.continuity_uncertain) d.quality_flags |= quality::kContinuityLost;

  const bool fault_mode = (mode_ == ValidationMode::kFaultObserve);
  if (fault_mode) d.quality_flags |= quality::kFaultTestOnly;

  // Sequence classification decides publication first; freshness can then veto
  // a quote that would otherwise be forwarded as current.
  switch (seq.klass) {
    case SeqClass::kOk:
      d.publish = true;
      break;

    case SeqClass::kGap:
      // Forward the new event: it is real, newer data. The DEGRADED flag is
      // what tells a consumer not to treat the series as continuous.
      d.publish = true;
      d.primary_reason = Reason::kSequenceGap;
      break;

    case SeqClass::kDuplicateExact:
      d.publish = fault_mode;
      d.primary_reason = Reason::kDuplicateExact;
      d.suppressed_reason = Reason::kDuplicateExact;
      break;

    case SeqClass::kDuplicateConflict:
      // Same logical ID, different payload. Never silently pick one.
      d.publish = fault_mode;
      d.primary_reason = Reason::kDuplicateConflict;
      d.suppressed_reason = Reason::kDuplicateConflict;
      d.integrity_alert = true;
      break;

    case SeqClass::kReorderLate:
      // Observed for continuity accounting, not published: forwarding it
      // would put the sink out of order (ADR-06/07).
      d.publish = fault_mode;
      d.primary_reason = Reason::kReorderLate;
      d.suppressed_reason = Reason::kReorderLate;
      break;

    case SeqClass::kTooOldUnverifiable:
      // Not publishable: an event we cannot place relative to the stream
      // must not be forwarded as current data.
      d.publish = fault_mode;
      d.primary_reason = Reason::kTooOldUnverifiable;
      d.suppressed_reason = Reason::kTooOldUnverifiable;
      if ((seq.quality_flags & quality::kSequenceRollback) != 0) {
        // A sender restarting at the baseline inside one dataset_id is an
        // integrity event even though the message itself is refused: the
        // operator has to start a new verified dataset epoch.
        d.primary_reason = Reason::kSequenceRollback;
        d.integrity_alert = true;
      }
      break;

    case SeqClass::kStateCapExceeded:
      // No state could be allocated, so nothing about this event can be
      // judged. Refusing it is the only safe answer.
      d.publish = false;
      d.primary_reason = Reason::kStateCapExceeded;
      d.suppressed_reason = Reason::kStateCapExceeded;
      return d;
  }

  // Freshness veto: a quote older than the accepted watermark is not a
  // current quote, whatever its sequence says.
  if (fresh.event_time_old) {
    if (!fault_mode) {
      d.publish = false;
      d.suppressed_reason = Reason::kEventTimeOld;
    }
    if (d.primary_reason == Reason::kNone)
      d.primary_reason = Reason::kEventTimeOld;
  }

  if (d.primary_reason == Reason::kNone && seq.reason != Reason::kNone)
    d.primary_reason = seq.reason;

  return d;
}

}  // namespace md
