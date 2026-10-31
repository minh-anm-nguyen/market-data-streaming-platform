#include "md_adapter/pipeline.hpp"

#include <algorithm>
#include <chrono>

namespace md {
namespace {

SequenceLimits sequence_limits_from(const Config& c, bool live) {
  SequenceLimits l;
  l.window = static_cast<std::uint32_t>(c.validation.sequence_window);
  l.max_symbols_per_dataset = static_cast<std::size_t>(c.identity.max_symbols);
  l.max_datasets = static_cast<std::size_t>(c.identity.max_datasets);
  l.max_state_bytes = static_cast<std::uint64_t>(c.identity.max_state_bytes);
  // Only a verified offline source is entitled to assume the stream starts at
  // the contract baseline.
  l.expect_start_at_one =
      !live && c.validation.mode == ValidationMode::kOfflineVerify;
  l.live_baseline_unverified = live;
  return l;
}

FreshnessLimits freshness_limits_from(const Config& c) {
  FreshnessLimits l;
  l.event_time_stale_threshold_ns = c.freshness.event_time_stale_threshold_ns;
  l.quote_inactivity_threshold_ns =
      c.freshness.quote_inactivity_threshold_ms * 1'000'000;
  return l;
}

}  // namespace

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

Pipeline::Pipeline(const Config& cfg, Clock& clock, BoundedQueue& queue,
                   bool needs_raw_wire, bool live)
    : cfg_(cfg),
      clock_(clock),
      queue_(queue),
      needs_raw_wire_(needs_raw_wire),
      structural_limits_(structural_limits_from(cfg)),
      business_(cfg),
      tracker_(sequence_limits_from(cfg, live)),
      freshness_(freshness_limits_from(cfg)),
      classifier_(cfg.validation.mode) {}

FrameAction Pipeline::on_frame(std::span<const std::byte> payload,
                               Reason& fatal_reason) {
  const std::int64_t t0 = perf_clock_.now_mono_ns();
  ++metrics_.frames_complete;

  // --- decode --------------------------------------------------------------
  const ProtobufDecoder::Result dec = decoder_.decode(payload);
  if (!dec.ok) {
    ++metrics_.frames_decode_error;
    // Corrupted wire data means the stream is no longer trustworthy. There is
    // no resynchronisation scan (ADR-11).
    fatal_reason = Reason::kProtoDecodeError;
    return FrameAction::kFatal;
  }
  ++metrics_.frames_decoded;
  if (dec.unknown_fields) ++metrics_.unknown_fields_seen;

  // --- structural ----------------------------------------------------------
  scratch_ = DomainEvent{};
  const Reason sr = validate_structural(decoder_.message(), structural_limits_,
                                        dec.unknown_fields, scratch_);
  if (sr != Reason::kNone) {
    metrics_.count_structural_reject(sr);
    // MVP policy is fail-closed: a message this build cannot interpret ends
    // the stream rather than being skipped silently (design 4.2).
    fatal_reason = sr;
    return FrameAction::kFatal;
  }
  ++metrics_.structural_ok;

  // --- business ------------------------------------------------------------
  const Reason br = business_.validate(scratch_);
  if (br != Reason::kNone) {
    metrics_.count_business_reject(br);
    if (br == Reason::kInvalidSequence)
      ++metrics_.raw_sequence_observed_invalid;
    // A business-invalid event never updates seen-valid state, so a later
    // event may legitimately report a gap across it (design section 5).
    if (cfg_.validation.on_business_reject == "fail_closed") {
      fatal_reason = br;
      return FrameAction::kFatal;
    }
    return FrameAction::kContinue;
  }
  ++metrics_.business_ok;

  // --- stream state --------------------------------------------------------
  const SeqResult seq = tracker_.observe(scratch_);
  const std::int64_t now = clock_.now_mono_ns();
  const FreshnessEngine::Result fresh = freshness_.observe(scratch_, now);

  if (seq.klass == SeqClass::kGap) ++metrics_.gap_detected;
  if (seq.missing_slots_observed > 0) {
    const auto add = static_cast<std::uint64_t>(seq.missing_slots_observed);
    // Saturate rather than wrap: an absurd jump must not corrupt the counter.
    if (metrics_.sequence_missing_slots_observed >
        UINT64_MAX - add)
      metrics_.sequence_missing_slots_observed = UINT64_MAX;
    else
      metrics_.sequence_missing_slots_observed += add;
  }
  if (seq.klass == SeqClass::kDuplicateExact) ++metrics_.duplicate_exact;
  if (seq.klass == SeqClass::kDuplicateConflict) ++metrics_.duplicate_conflict;
  if (seq.klass == SeqClass::kReorderLate) ++metrics_.reorder_late;
  if (seq.klass == SeqClass::kTooOldUnverifiable)
    ++metrics_.too_old_unverifiable;
  if ((seq.quality_flags & quality::kSequenceRollback) != 0)
    ++metrics_.sequence_rollback;
  if (seq.klass == SeqClass::kStateCapExceeded) ++metrics_.state_cap_exceeded;
  if ((seq.quality_flags & quality::kStartUnverified) != 0)
    ++metrics_.start_unverified;
  if (fresh.event_time_old) ++metrics_.stale_event_time;
  if (fresh.recovered) ++metrics_.recovered_transitions;

  // --- output policy -------------------------------------------------------
  const Classifier::Decision d = classifier_.classify(scratch_, seq, fresh);
  if ((d.quality_flags & quality::kDegraded) != 0)
    freshness_.mark_degraded(scratch_.dataset_id, scratch_.symbol);

  if (!d.publish) {
    metrics_.count_policy_suppressed(d.suppressed_reason == Reason::kNone
                                         ? d.primary_reason
                                         : d.suppressed_reason);
    metrics_.processing_latency.record(perf_clock_.now_mono_ns() - t0);
    return FrameAction::kContinue;
  }

  ClassifiedEvent out;
  out.source = scratch_;
  out.quality_flags = d.quality_flags;
  out.arrival_index = arrival_index_++;
  out.feed_state = freshness_.state_of(scratch_.dataset_id, scratch_.symbol);
  out.primary_reason = d.primary_reason;
  if (needs_raw_wire_) {
    // Byte-exact passthrough for the P04 handoff: the original wire message,
    // with no MDP1 magic and no length prefix.
    out.raw_wire.assign(reinterpret_cast<const char*>(payload.data()),
                        payload.size());
  }

  ++metrics_.classified_output_attempted;
  metrics_.processing_latency.record(perf_clock_.now_mono_ns() - t0);

  const InflightRecord rec{seq.handle, scratch_.sequence};
  const auto pr = queue_.try_push(std::move(out));
  if (pr == BoundedQueue::PushResult::kAccepted) {
    inflight_.push_back(rec);
    metrics_.peak_queue_events =
        std::max<std::uint64_t>(metrics_.peak_queue_events, queue_.size());
    metrics_.peak_queue_bytes =
        std::max<std::uint64_t>(metrics_.peak_queue_bytes, queue_.bytes());
    return FrameAction::kContinue;
  }
  if (pr == BoundedQueue::PushResult::kClosed) {
    fatal_reason = Reason::kSinkIoError;
    return FrameAction::kFatal;
  }
  // Full: hold the event and stop reading. Nothing is discarded.
  pending_ = ClassifiedEvent{};
  pending_->source = scratch_;
  pending_->quality_flags = d.quality_flags;
  pending_->arrival_index = arrival_index_ - 1;
  pending_->feed_state =
      freshness_.state_of(scratch_.dataset_id, scratch_.symbol);
  pending_->primary_reason = d.primary_reason;
  if (needs_raw_wire_)
    pending_->raw_wire.assign(reinterpret_cast<const char*>(payload.data()),
                              payload.size());
  pending_record_ = rec;
  return FrameAction::kPause;
}

bool Pipeline::flush_pending() {
  if (!pending_.has_value()) return true;
  // try_push only moves out of its argument when it accepts, so on rejection
  // the held event is still intact and nothing is lost.
  const auto pr = queue_.try_push(std::move(*pending_));
  if (pr != BoundedQueue::PushResult::kAccepted) return false;
  inflight_.push_back(pending_record_);
  pending_.reset();
  return true;
}

void Pipeline::tick(std::int64_t now_mono_ns) {
  const std::size_t before = transitions_.size();
  freshness_.tick(now_mono_ns, transitions_);
  const std::size_t added = transitions_.size() - before;
  metrics_.quote_inactive_transitions += added;
  // Transitions are a bounded diagnostic: keep only the most recent ones so a
  // long soak cannot grow this vector without limit.
  constexpr std::size_t kMaxTransitions = 4096;
  if (transitions_.size() > kMaxTransitions)
    transitions_.erase(transitions_.begin(),
                       transitions_.begin() +
                           static_cast<std::ptrdiff_t>(transitions_.size() -
                                                       kMaxTransitions));
}

void Pipeline::confirm_delivered(std::uint64_t total_confirmed) {
  while (confirmed_ < total_confirmed && !inflight_.empty()) {
    const InflightRecord& r = inflight_.front();
    tracker_.note_published(r.handle, r.sequence);
    inflight_.pop_front();
    ++confirmed_;
  }
}

// ---------------------------------------------------------------------------
// SinkWriter
// ---------------------------------------------------------------------------

SinkWriter::SinkWriter(BoundedQueue& q, Sink& sink) : q_(q), sink_(sink) {}

SinkWriter::~SinkWriter() {
  stop_.store(true);
  q_.close();
  if (th_.joinable()) th_.join();
}

void SinkWriter::start() { th_ = std::thread([this] { run(); }); }

void SinkWriter::run() {
  std::vector<ClassifiedEvent> batch;
  while (true) {
    batch.clear();
    // The batch is capped by the queue's own event limit so that a burst
    // cannot move more unwritten work out of the accounting than the
    // configured budget allows.
    const std::size_t n = q_.pop_batch(
        std::min<std::size_t>(256, q_.limits().max_events), batch);
    if (n == 0) {
      if (stop_.load() && q_.size() == 0) break;
      auto e = q_.pop(5'000'000);  // 5 ms
      if (!e.has_value()) {
        if (stop_.load() && q_.size() == 0) break;
        continue;
      }
      // pop() fully discharges, so this one is not released below.
      const Reason r = sink_.write(*e);
      if (r != Reason::kNone) {
        error_.store(r);
        return;
      }
      written_.fetch_add(1, std::memory_order_release);
      continue;
    }
    for (const auto& e : batch) {
      const Reason r = sink_.write(e);
      if (r != Reason::kNone) {
        error_.store(r);
        q_.release(batch);  // keep the accounting straight even on failure
        return;             // a sink failure stops the run; never ignored
      }
      // Only a count crosses the thread boundary. The owner thread maps it
      // back to keys in confirm_delivered(), so tracker state stays
      // single-threaded.
      written_.fetch_add(1, std::memory_order_release);
    }
    // The batch has reached the sink, so it stops counting against the caps
    // and the reader may resume.
    q_.release(batch);
  }
}

bool SinkWriter::stop_and_drain(std::int64_t deadline_ns) {
  // Phase 1: give the writer until the deadline to clear what is queued.
  const auto start = std::chrono::steady_clock::now();
  while (q_.outstanding() != 0 && error_.load() == Reason::kNone) {
    const auto waited = std::chrono::steady_clock::now() - start;
    if (std::chrono::duration_cast<std::chrono::nanoseconds>(waited).count() >
        deadline_ns)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool drained =
      q_.outstanding() == 0 && error_.load() == Reason::kNone;

  // Phase 2: stop, and always join before returning. Missing the deadline
  // means the run is INCOMPLETE -- it does not mean we may leave a thread
  // inside sink_.write() while the caller aborts and closes that same sink.
  // The writer stops after its current event, so this is bounded by one
  // queue's worth of writes.
  stop_.store(true);
  q_.close();
  if (th_.joinable()) th_.join();
  return drained;
}

}  // namespace md
