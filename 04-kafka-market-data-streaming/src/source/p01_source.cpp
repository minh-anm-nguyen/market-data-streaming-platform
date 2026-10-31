#include "source/p01_source.h"

#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <sstream>

#include "ops/sha256.h"

namespace p04 {
namespace {

// A tiny scanner for the flat JSON shapes this reader needs: the canonical
// JSONL records that Project 01 writes, and the few fields it needs out of
// report.json.
//
// Why not a JSON library: the records are produced by one known writer with a
// documented field allowlist and no nesting inside a record, and the strictness
// requirements run the other way from what a general parser gives. A general
// parser accepts 1.0 for an integer field and accepts unknown members; Project
// 01 rejects both, and Project 04 has to agree with Project 01 or the two
// disagree about what the dataset contains.
class FlatJson {
 public:
  explicit FlatJson(std::string_view text) : text_(text) {}

  // Finds "key": and returns the raw token after it, without interpreting it.
  bool RawValue(std::string_view key, std::string_view* out) const {
    std::string needle;
    needle.reserve(key.size() + 3);
    needle += '"';
    needle.append(key.data(), key.size());
    needle += '"';
    std::size_t search = 0;
    while (true) {
      const auto pos = text_.find(needle, search);
      if (pos == std::string_view::npos) return false;
      std::size_t i = pos + needle.size();
      while (i < text_.size() && (text_[i] == ' ' || text_[i] == '\t')) ++i;
      if (i >= text_.size() || text_[i] != ':') {
        // The name appeared as a value, not as a key; keep looking.
        search = pos + needle.size();
        continue;
      }
      ++i;
      while (i < text_.size() && (text_[i] == ' ' || text_[i] == '\t' ||
                                  text_[i] == '\n' || text_[i] == '\r')) ++i;
      const std::size_t start = i;
      if (i < text_.size() && text_[i] == '"') {
        ++i;
        while (i < text_.size() && text_[i] != '"') {
          if (text_[i] == '\\') ++i;
          ++i;
        }
        if (i < text_.size()) ++i;
      } else {
        while (i < text_.size() && text_[i] != ',' && text_[i] != '}' &&
               text_[i] != ']' && text_[i] != '\n') ++i;
      }
      *out = text_.substr(start, i - start);
      return true;
    }
  }

  bool String(std::string_view key, std::string* out) const {
    std::string_view raw;
    if (!RawValue(key, &raw)) return false;
    if (raw.size() < 2 || raw.front() != '"' || raw.back() != '"') return false;
    out->assign(raw.data() + 1, raw.size() - 2);
    return true;
  }

  // Requires a true integer token. A fractional or exponent form is rejected,
  // matching Project 01, where a float in an integer field is a schema
  // violation rather than a value to round.
  bool Integer(std::string_view key, std::int64_t* out) const {
    std::string_view raw;
    if (!RawValue(key, &raw)) return false;
    if (raw.empty()) return false;
    std::size_t i = (raw[0] == '-') ? 1 : 0;
    if (i >= raw.size()) return false;
    for (std::size_t j = i; j < raw.size(); ++j) {
      if (raw[j] < '0' || raw[j] > '9') return false;
    }
    errno = 0;
    const std::string token(raw);
    char* end = nullptr;
    const long long value = std::strtoll(token.c_str(), &end, 10);
    if (errno == ERANGE || end == token.c_str()) return false;
    *out = static_cast<std::int64_t>(value);
    return true;
  }

  bool Bool(std::string_view key, bool* out) const {
    std::string_view raw;
    if (!RawValue(key, &raw)) return false;
    if (raw == "true") { *out = true; return true; }
    if (raw == "false") { *out = false; return true; }
    return false;
  }

 private:
  std::string_view text_;
};

bool ReadWholeFile(const std::string& path, std::string* out) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  std::ostringstream buffer;
  buffer << in.rdbuf();
  *out = buffer.str();
  return true;
}

std::string JoinPath(const std::string& dir, const std::string& name) {
  if (dir.empty()) return name;
  const char last = dir.back();
  if (last == '/' || last == '\\') return dir + name;
  return dir + "/" + name;
}

}  // namespace

std::string SourceManifest::Explain() const {
  std::ostringstream out;
  out << "dataset_id=" << dataset_id << " schema=" << schema_version
      << " recorded_events=" << recorded_events
      << " completed=" << (completed_marker ? "yes" : "no")
      << " digest_matches=" << (digest_matches ? "yes" : "no")
      << " faults_enabled=" << (faults_enabled ? "yes" : "no")
      << " intentional{dup=" << intentional_duplicates
      << ",omit=" << intentional_omissions
      << ",mutate=" << intentional_mutations << "}";
  return out.str();
}

bool LoadSourceManifest(const std::string& run_dir, SourceManifest* out,
                        std::string* error) {
  const std::string report_path = JoinPath(run_dir, "report.json");
  std::string report_text;
  if (!ReadWholeFile(report_path, &report_text)) {
    *error = "cannot read " + report_path + " (not a Project 01 run directory?)";
    return false;
  }
  const FlatJson report(report_text);

  SourceManifest manifest;
  if (!report.String("dataset_id", &manifest.dataset_id)) {
    *error = "report.json has no dataset_id";
    return false;
  }
  report.String("schema_version", &manifest.schema_version);
  report.String("events_sha256", &manifest.events_sha256);

  std::int64_t value = 0;
  if (report.Integer("recorded_events", &value) && value >= 0) {
    manifest.recorded_events = static_cast<std::uint64_t>(value);
  }
  if (report.Integer("emitted_events", &value) && value >= 0) {
    manifest.emitted_events = static_cast<std::uint64_t>(value);
  }
  if (report.Integer("intentional_duplicates", &value) && value >= 0) {
    manifest.intentional_duplicates = static_cast<std::uint64_t>(value);
  }
  if (report.Integer("intentional_omissions", &value) && value >= 0) {
    manifest.intentional_omissions = static_cast<std::uint64_t>(value);
  }
  if (report.Integer("intentional_mutations", &value) && value >= 0) {
    manifest.intentional_mutations = static_cast<std::uint64_t>(value);
  }
  // "enabled" is not unique in report.json: both the faults object and the file
  // object have one. Scoping the search to the faults object matters, because
  // reading the file object by accident would report faults_enabled=true for
  // every run and could let deliberately faulty events into a market topic
  // (case A-03). The scope is found by the object key, not by document order.
  {
    const auto faults_pos = report_text.find("\"faults\"");
    if (faults_pos != std::string::npos) {
      const auto open_brace = report_text.find('{', faults_pos);
      if (open_brace != std::string::npos) {
        // The faults object is flat apart from its records array, so the first
        // closing brace at depth zero ends it.
        int depth = 0;
        std::size_t close_brace = std::string::npos;
        for (std::size_t i = open_brace; i < report_text.size(); ++i) {
          if (report_text[i] == '{' || report_text[i] == '[') ++depth;
          if (report_text[i] == '}' || report_text[i] == ']') {
            --depth;
            if (depth == 0) {
              close_brace = i;
              break;
            }
          }
        }
        if (close_brace != std::string::npos) {
          const FlatJson faults(
              std::string_view(report_text).substr(open_brace, close_brace - open_brace + 1));
          faults.Bool("enabled", &manifest.faults_enabled);
        }
      }
    }
  }

  manifest.events_path = JoinPath(run_dir, "events.jsonl");
  {
    std::ifstream probe(manifest.events_path, std::ios::binary);
    if (!probe) {
      *error = "events.jsonl missing in " + run_dir +
               " (an uncommitted .part run cannot be a completeness oracle)";
      return false;
    }
  }

  std::ifstream marker(JoinPath(run_dir, "COMPLETED"), std::ios::binary);
  manifest.completed_marker = static_cast<bool>(marker);

  // Recomputing the digest rather than trusting the recorded one. The whole
  // point of the pin is that a changed file is detected, and a reader that only
  // reads the number it is handed detects nothing.
  manifest.computed_sha256 = Sha256::OfFile(manifest.events_path);
  manifest.digest_matches = !manifest.events_sha256.empty() &&
                            manifest.computed_sha256 == manifest.events_sha256;

  *out = std::move(manifest);
  if (error != nullptr) error->clear();
  return true;
}

bool ReadEvents(const std::string& events_path,
                const std::function<bool(const TypedEvent&, const std::string&)>& visitor,
                std::vector<SourceParseError>* errors, std::string* error) {
  std::ifstream in(events_path, std::ios::binary);
  if (!in) {
    *error = "cannot open " + events_path;
    return false;
  }
  std::string line;
  std::uint64_t line_number = 0;
  while (std::getline(in, line)) {
    ++line_number;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.empty()) continue;

    const FlatJson json(line);
    const auto record_error = [&](const std::string& reason, const std::string& detail) {
      if (errors != nullptr) errors->push_back({line_number, reason, detail});
    };

    std::string schema_version;
    if (!json.String("schema_version", &schema_version)) {
      record_error("schema_version_missing", "");
      continue;
    }
    std::string event_type;
    if (!json.String("event_type", &event_type)) {
      record_error("event_type_missing", "");
      continue;
    }
    if (event_type != "quote" && event_type != "tick") {
      record_error("unknown_event_type", event_type);
      continue;
    }

    TypedEvent event;
    event.source_schema_version = schema_version;
    std::string text;
    if (json.String("dataset_id", &text)) event.dataset_id = text;
    if (json.String("symbol", &text)) event.symbol = text;

    std::int64_t number = 0;
    if (json.Integer("sequence", &number)) event.sequence = number;
    if (json.Integer("event_time_ns", &number)) event.event_time_ns = number;
    if (json.Integer("price_scale", &number)) event.price_scale = number;

    if (event_type == "quote") {
      event.kind = EventKind::kQuote;
      if (json.Integer("bid_ticks", &number)) event.quote.bid_ticks = number;
      if (json.Integer("ask_ticks", &number)) event.quote.ask_ticks = number;
    } else {
      event.kind = EventKind::kTick;
      if (json.Integer("price_ticks", &number)) event.tick.price_ticks = number;
    }

    if (!visitor(event, line)) break;
  }
  if (error != nullptr) error->clear();
  return true;
}

MockP03Classifier::MockP03Classifier(MockP03Config config)
    : config_(std::move(config)) {}

OutputEvent MockP03Classifier::Classify(const TypedEvent& event,
                                        const std::string& raw_line) {
  ++counts_.total;
  OutputEvent out;
  out.event = event;
  out.raw_source_payload = raw_line;
  out.provenance.run_id = config_.run_id;
  out.provenance.descriptor_sha256 = config_.descriptor_sha256;
  out.provenance.source_schema_version = event.source_schema_version.value_or("");
  out.provenance.source_dataset_id = event.dataset_id.value_or("");
  out.provenance.arrival_index = ++arrival_index_;
  out.feed_state = FeedState::kLive;

  const auto classify_as = [&](Classification classification) {
    out.classification = classification;
    switch (classification) {
      case Classification::kStructuralInvalid:   ++counts_.structural_invalid; break;
      case Classification::kBusinessReject:      ++counts_.business_reject; break;
      case Classification::kDuplicateSuppressed: ++counts_.duplicate_suppressed; break;
      case Classification::kStaleSuppressed:     ++counts_.stale_suppressed; break;
      case Classification::kFaultTestOnly:       ++counts_.fault_test_only; break;
      case Classification::kUnclassified:        ++counts_.unclassified; break;
      case Classification::kMarketValid:         ++counts_.market_valid; break;
    }
    return out;
  };

  // Structural first: without these fields there is no identity to reason
  // about, so every later rule would be operating on guesses.
  if (!event.kind.has_value() || !event.dataset_id || !event.symbol ||
      !event.sequence || !event.event_time_ns || !event.price_scale) {
    return classify_as(Classification::kStructuralInvalid);
  }
  if (!event.source_schema_version ||
      *event.source_schema_version != config_.expected_schema_version) {
    return classify_as(Classification::kStructuralInvalid);
  }
  if (*event.price_scale <= 0 || *event.sequence < 0) {
    return classify_as(Classification::kStructuralInvalid);
  }
  if (*event.kind == EventKind::kQuote) {
    if (!event.quote.bid_ticks || !event.quote.ask_ticks) {
      return classify_as(Classification::kStructuralInvalid);
    }
  } else if (!event.tick.price_ticks) {
    return classify_as(Classification::kStructuralInvalid);
  }

  const EventIdentity identity = out.Identity();

  // A fixture may pin specific identities as deliberately faulty, so case A-03
  // can be driven from data instead of from a special build.
  if (!config_.force_fault_test_identities.empty() &&
      config_.force_fault_test_identities.count(identity.ToString()) > 0) {
    out.quality_flags |= kQualityFaultInjected;
    return classify_as(Classification::kFaultTestOnly);
  }

  // Business rules. Note what is absent: no clamping, no reordering, no
  // substituting a previous good price. Section 2.3 forbids auto-correction,
  // and a rejected event with a reason is more useful than a repaired one with
  // a hidden provenance.
  if (*event.kind == EventKind::kQuote) {
    if (*event.quote.ask_ticks < *event.quote.bid_ticks) {
      return classify_as(Classification::kBusinessReject);
    }
    if (*event.quote.bid_ticks <= 0 || *event.quote.ask_ticks <= 0) {
      return classify_as(Classification::kBusinessReject);
    }
  } else if (*event.tick.price_ticks <= 0) {
    return classify_as(Classification::kBusinessReject);
  }

  if (!seen_.insert(identity).second) {
    return classify_as(Classification::kDuplicateSuppressed);
  }

  // Sequence gaps are flagged, never rejected. A gap can be a deliberate P01
  // omission; treating it as corruption here would manufacture the false
  // "Kafka lost a tick" conclusion that case B-10 warns against.
  auto& last_seq = last_sequence_[*event.symbol];
  if (last_seq != 0 && *event.sequence > last_seq + 1) {
    out.quality_flags |= kQualitySequenceGapBefore;
    ++counts_.sequence_gaps_flagged;
  }
  if (*event.sequence > last_seq) last_seq = *event.sequence;

  auto& last_time = last_event_time_[*event.symbol];
  if (last_time != 0 && *event.event_time_ns < last_time) {
    out.quality_flags |= kQualityTimestampNonMonotonic;
    if (config_.stale_tolerance_ns > 0 &&
        last_time - *event.event_time_ns > config_.stale_tolerance_ns) {
      out.feed_state = FeedState::kStale;
      return classify_as(Classification::kStaleSuppressed);
    }
  }
  if (*event.event_time_ns > last_time) last_time = *event.event_time_ns;

  return classify_as(Classification::kMarketValid);
}

}  // namespace p04
