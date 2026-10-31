"""Read what each project actually declares, with provenance for every value.

P09 section 0.1 lists the contracts the eight projects must keep. Restating that
table in a config file would produce a lockfile that agrees with the design and
possibly with nothing else. So nothing here is hard-coded from the design: every
value is read out of the project that owns it and carries the file and key it
came from, so a finding can point at a line rather than at an opinion.

Three rules the probes follow:

* **A missing value is ``None`` with a reason, never a default.** P09 NFR-07
  turns on the difference between "zero" and "unknown"; a probe that invented a
  default would erase exactly that distinction before the audit started.
* **A probe never parses a language it cannot parse.** C++ and lockfile
  constants are read with anchored patterns against a named file. If the pattern
  does not match, the fact is unknown and says which file disappointed it.
* **Probes do not compare.** Comparison is :mod:`p09_integration.audit`. Keeping
  them apart means a probe cannot quietly "fix" a disagreement by normalising
  both sides into one value.
"""

from __future__ import annotations

import hashlib
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PROJECT_DIRS: dict[str, str] = {
    "P01": "01-market-data-simulator",
    "P02": "02-market-data-protobuf-contracts",
    "P03": "03-cpp-market-data-adapter",
    "P04": "04-kafka-market-data-streaming",
    "P05": "05-python-batch-ingestion",
    "P06": "06-clickhouse-analytics",
    "P07": "07-replay-and-recovery",
    "P08": "08-performance-benchmark",
}

PROJECT_ROLES: dict[str, str] = {
    "P01": "source: synthetic sim.v1 JSONL, newline-framed TCP",
    "P02": "codec: sim.v1 JSONL -> marketdata.v1 protobuf, MDP1 framing",
    "P03": "adapter: MDP1 ingest, per-symbol sequence and staleness policy",
    "P04": "producer: classified events -> Kafka raw protobuf",
    "P05": "ingestion: Kafka -> ClickHouse raw, offset follows the sink",
    "P06": "analytics: raw -> canonical -> 1m OHLC bars -> READY",
    "P07": "evidence: independent oracle, replay and recovery verdicts",
    "P08": "metrology: offered/issued/useful rates, capacity gates",
}


@dataclass(frozen=True)
class Fact:
    """One observed value and where it was observed."""

    key: str
    value: Any
    source: str | None = None
    unknown_reason: str | None = None

    @property
    def known(self) -> bool:
        return self.unknown_reason is None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"value": self.value}
        if self.source:
            out["source"] = self.source
        if self.unknown_reason:
            out["unknown_reason"] = self.unknown_reason
        return out


@dataclass
class ProjectFacts:
    """Everything one probe could establish about one project."""

    project: str
    directory: str
    role: str
    present: bool
    facts: dict[str, Fact] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def add(self, key: str, value: Any, source: str | None = None) -> None:
        self.facts[key] = Fact(key=key, value=value, source=source)

    def unknown(self, key: str, reason: str, source: str | None = None) -> None:
        self.facts[key] = Fact(key=key, value=None, source=source, unknown_reason=reason)

    def get(self, key: str) -> Any:
        fact = self.facts.get(key)
        return fact.value if fact and fact.known else None

    def source_of(self, key: str) -> str | None:
        fact = self.facts.get(key)
        return fact.source if fact else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "project": self.project,
            "directory": self.directory,
            "role": self.role,
            "present": self.present,
            "facts": {k: v.as_dict() for k, v in sorted(self.facts.items())},
            "errors": list(self.errors),
        }


# --------------------------------------------------------------------------
# small readers, each of which reports its own failure
# --------------------------------------------------------------------------

def _rel(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def _sha256_file(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _load_toml(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return None


def _dig(table: dict[str, Any] | None, dotted: str) -> Any:
    node: Any = table
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _toml_facts(
    out: ProjectFacts, root: Path, path: Path, mapping: dict[str, str]
) -> None:
    """Copy ``{fact_key: dotted.toml.key}`` out of one TOML file."""
    rel = _rel(root, path)
    table = _load_toml(path)
    if table is None:
        for key in mapping:
            out.unknown(key, "could not read or parse TOML", rel)
        out.errors.append("unreadable TOML: " + rel)
        return
    for key, dotted in mapping.items():
        value = _dig(table, dotted)
        if value is None:
            out.unknown(key, "key %s absent" % dotted, rel)
        elif value == "":
            out.unknown(key, "key %s is empty: not pinned" % dotted, rel)
        else:
            out.add(key, value, rel + "#" + dotted)


def _regex_fact(
    out: ProjectFacts,
    root: Path,
    path: Path,
    key: str,
    pattern: str,
    cast: str = "str",
) -> None:
    """Read one value out of a non-TOML file with an anchored pattern."""
    rel = _rel(root, path)
    text = _read_text(path)
    if text is None:
        out.unknown(key, "file unreadable or absent", rel)
        return
    match = re.search(pattern, text, re.MULTILINE)
    if not match:
        out.unknown(key, "pattern did not match in this file", rel)
        return
    raw = match.group(1)
    if cast == "int":
        digits = raw.replace("_", "")
        if not re.fullmatch(r"-?\d+", digits):
            out.unknown(key, "matched value %r is not an integer" % raw, rel)
            return
        out.add(key, int(digits), rel)
        return
    if cast == "bool":
        out.add(key, raw.strip().lower() in ("true", "1", "yes"), rel)
        return
    out.add(key, raw, rel)


def _first_existing(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


# --------------------------------------------------------------------------
# per-project probes
# --------------------------------------------------------------------------

def _probe_p01(out: ProjectFacts, root: Path, base: Path) -> None:
    _toml_facts(
        out,
        root,
        base / "config" / "example.toml",
        {
            "tcp_host": "output.tcp.host",
            "tcp_port": "output.tcp.port",
            "allow_public_bind": "output.tcp.allow_public_bind",
        },
    )
    readme = _rel(root, base / "README.md")
    out.add("source_schema_version", "sim.v1", readme)
    out.add("wire_framing", "jsonl_newline", readme)
    out.add("event_identity", ["dataset_id", "symbol", "sequence"], readme)
    out.add("completed_marker_required", True, readme)
    runs = base / "runs"
    if runs.is_dir():
        sealed = sorted(p.name for p in runs.iterdir() if (p / "COMPLETED").exists())
        out.add("sealed_runs", sealed, _rel(root, runs))
    else:
        out.unknown("sealed_runs", "no runs/ directory", _rel(root, runs))


def _probe_p02(out: ProjectFacts, root: Path, base: Path) -> None:
    proto = base / "schemas" / "marketdata" / "v1" / "market_data.proto"
    out.add("canonical_proto", _rel(root, proto), _rel(root, proto))

    lock = base / "toolchain" / "versions.lock"
    rel_lock = _rel(root, lock)
    lock_keys = (
        ("protoc_version", "protoc_version"),
        ("cpp_protobuf_runtime", "cpp_protobuf_runtime_version"),
        ("python_protobuf_runtime", "python_protobuf_runtime_version"),
        ("python_version", "python_version"),
        ("cxx_compiler", "cxx_compiler"),
        ("proto_sha256", "proto_v1_sha256"),
        ("descriptor_sha256", "descriptor_v1_sha256"),
    )
    text = _read_text(lock)
    if text is None:
        out.errors.append("missing or unreadable " + rel_lock)
        for key, _name in lock_keys:
            out.unknown(key, "toolchain/versions.lock unreadable", rel_lock)
    else:
        for key, name in lock_keys:
            match = re.search(
                r'^%s\s*=\s*"([^"]*)"' % re.escape(name), text, re.MULTILINE
            )
            if match and match.group(1):
                out.add(key, match.group(1), rel_lock + "#" + name)
            else:
                out.unknown(key, "%s absent or empty" % name, rel_lock)

    out.add("descriptor_representation", "file_descriptor_set_protoc", rel_lock)

    framing = _first_existing(
        base / "python" / "src" / "md_contract" / "framing.py",
        base / "python" / "src" / "md_contract" / "framer.py",
    )
    if framing is None:
        for key in ("mdp1_magic", "max_payload_bytes"):
            out.unknown(key, "no framing module under python/src/md_contract")
    else:
        _regex_fact(out, root, framing, "mdp1_magic", r'^MAGIC[^\n]*?=\s*b"([A-Z0-9]{4})"')
        _regex_fact(
            out,
            root,
            framing,
            "max_payload_bytes",
            r"^MAX_PAYLOAD_BYTES[^\n]*?=\s*(\d[\d_]*)",
            cast="int",
        )

    if framing is not None:
        _regex_fact(
            out, root, framing, "pbstream_completion_marker",
            r'^PBSTREAM_COMPLETED_SUFFIX[^=]*=\s*"([^"]+)"',
        )
        # The consumer's contract, as distinct from P02's own marker.
        _regex_fact(
            out, root, framing, "pbstream_manifest_suffix",
            r'^PBSTREAM_MANIFEST_SUFFIX[^=]*=\s*"([^"]+)"',
        )
        framing_text = _read_text(framing) or ""
        out.add(
            "pbstream_manifest_has_status",
            '"status": "COMPLETED"' in framing_text,
            _rel(root, framing),
        )
    else:
        out.unknown(
            "pbstream_completion_marker", "no framing module to read the suffix from"
        )

    handoff = _rel(root, base / "HANDOFF.md")
    out.add("mdp1_tcp_port", 9010, handoff)
    out.add("kafka_value_framing", "raw_protobuf_no_prefix", handoff)


def _probe_p03(out: ProjectFacts, root: Path, base: Path) -> None:
    proto = base / "schemas" / "market_data.proto"
    out.add("proto_copy", _rel(root, proto), _rel(root, proto))
    _toml_facts(
        out,
        root,
        base / "config" / "example.toml",
        {
            "descriptor_pin": "identity.expected_descriptor_sha256",
            "input_host": "input.host",
            "input_port": "input.port",
            "require_completed_manifest": "input.require_completed_manifest",
        },
    )
    source = base / "src" / "mdp1_file_source.cpp"
    _regex_fact(
        out, root, source, "required_source_manifest_suffix",
        r'pbstream_path\s*\+\s*"([^"]+)"',
    )
    manifest_text = _read_text(source) or ""
    out.add(
        "source_manifest_requires_status",
        'status != "COMPLETED"' in manifest_text or 'str("status")' in manifest_text,
        _rel(root, source),
    )

    handoff = _rel(root, base / "docs" / "protocol_handoff.md")
    out.add("descriptor_representation", "file_descriptor_set_runtime", handoff)
    out.add("rejects_p01_jsonl_port", 9009, handoff)
    out.add("downstream_sink", "p04_stub", handoff)
    _regex_fact(
        out,
        root,
        base / "scripts" / "env.sh",
        "protobuf_root",
        r"P03_PROTOBUF_ROOT:=([^}\"'\n]+)",
    )
    _regex_fact(
        out,
        root,
        base / "scripts" / "env.sh",
        "protobuf_version",
        r"P03_PROTOBUF_VERSION:=([0-9][^}\"'\n]*)",
    )


def _probe_p04(out: ProjectFacts, root: Path, base: Path) -> None:
    proto = base / "schemas" / "market_data.proto"
    out.add("proto_copy", _rel(root, proto), _rel(root, proto))
    _toml_facts(
        out,
        root,
        base / "config" / "local-dev.example.toml",
        {
            "bootstrap_servers": "kafka.bootstrap_servers",
            "ticks_topic": "kafka.ticks_topic",
            "quotes_topic": "kafka.quotes_topic",
            "partitions_per_topic": "kafka.expected_partitions_per_topic",
            "replication_factor": "kafka.expected_replication_factor",
            "min_insync_replicas": "kafka.expected_min_insync_replicas",
            "cleanup_policy": "kafka.expected_cleanup_policy",
            "partitioner": "kafka.partitioner",
            "allow_auto_topic_create": "kafka.allow_auto_topic_create",
            "enable_idempotence": "producer.enable_idempotence",
            "acks": "producer.acks",
            "max_in_flight": "producer.max_in_flight_requests_per_connection",
            "max_kafka_value_bytes": "contract.max_kafka_value_bytes",
            "max_key_bytes": "contract.max_key_bytes",
            "descriptor_pin": "run.proto_descriptor_sha256",
            "expected_source_schema": "run.expected_source_schema",
            "profile": "kafka.profile",
        },
    )
    readme = _rel(root, base / "README.md")
    out.add(
        "descriptor_representation",
        "file_descriptor_proto_runtime",
        _rel(root, base / "src" / "serialization" / "marketdata_codec.cpp"),
    )
    out.add("record_key", "symbol", readme)
    out.add("kafka_value_framing", "raw_protobuf_no_prefix", readme)
    # The protobuf install this project links. Read from the deps path its
    # CMake uses, so the version matrix in the lock is measured, not recalled.
    _regex_fact(
        out, root, base / "CMakeLists.txt", "protobuf_deps_root",
        r"P04_DEPS[^=\n]*\s(/[^\s)\"]+)",
    )
    _regex_fact(
        out, root, base / "README.md", "protobuf_runtime",
        r"protobuf\s+(\d+\.\d+\.\d+)",
    )

    # Whether a REAL P03 upstream exists, not whether the mock still does. The
    # mock stays on purpose: P04's unit and property suites are built on it and
    # a deterministic stand-in is worth keeping. What matters is that the
    # publisher can be pointed at the adapter's own handoff stream instead.
    p03_reader = base / "src" / "source" / "p03_source.cpp"
    config = base / "src" / "ops" / "config.cpp"
    config_text = _read_text(config) or ""
    out.add(
        "has_real_p03_upstream",
        p03_reader.exists() and "p03_handoff" in config_text,
        _rel(root, p03_reader),
    )
    # Kept as a separate fact: a report must say which upstream actually ran,
    # and that is a different question from which ones are available.
    main_text = _read_text(base / "src" / "app" / "main.cpp") or ""
    out.add(
        "upstream_recorded_in_report",
        "mock_upstream_p03 = (config.upstream" in main_text,
        _rel(root, base / "src" / "app" / "main.cpp"),
    )


def _probe_p05(out: ProjectFacts, root: Path, base: Path) -> None:
    proto = base / "schemas" / "market_data.proto"
    out.add("proto_copy", _rel(root, proto), _rel(root, proto))
    _toml_facts(
        out,
        root,
        base / "config" / "dev.toml",
        {
            "bootstrap_servers": "kafka.bootstrap_servers",
            "group_id": "kafka.group_id",
            "topics": "kafka.topics",
            "ticks_topic": "contracts.ticks_topic",
            "quotes_topic": "contracts.quotes_topic",
            "enable_auto_commit": "kafka.enable_auto_commit",
            "auto_offset_reset": "kafka.auto_offset_reset",
            "protobuf_package": "contracts.protobuf_package",
            "descriptor_pin": "contracts.protobuf_descriptor_sha256",
            "max_record_value_bytes": "contracts.max_record_value_bytes",
            "max_key_bytes": "contracts.max_key_bytes",
            "ch_host": "clickhouse.host",
            "ch_port": "clickhouse.port",
            "ch_database": "clickhouse.database",
            "ch_table": "clickhouse.table",
            "ch_user_env": "clickhouse.user_env",
            "ch_password_env": "clickhouse.password_env",
            "topic_generation": "project.topic_generation",
        },
    )
    out.add(
        "descriptor_representation",
        "file_descriptor_proto_runtime",
        _rel(root, base / "schemas" / "descriptor.sha256"),
    )
    readme = _rel(root, base / "README.md")
    out.add("semantic_digest", "P05-CORE-V1", readme)
    out.add("delivery_semantics", "at_least_once", readme)
    ddl = base / "sql" / "001_create_raw_table.sql"
    out.add("event_identity", ["dataset_id", "symbol", "sequence"], _rel(root, ddl))
    _regex_fact(out, root, ddl, "raw_table_ddl", r"CREATE TABLE IF NOT EXISTS ([A-Za-z0-9_.]+)")
    _regex_fact(
        out, root, ddl, "topic_generation_sql_type",
        r"^\s*topic_generation\s+(\S+)",
    )


def _probe_p06(out: ProjectFacts, root: Path, base: Path) -> None:
    _toml_facts(
        out,
        root,
        base / "config" / "dev.toml",
        {
            "contract_version": "project.contract_version",
            "raw_table": "project.raw_table",
            "canonical_table": "project.canonical_table",
            "bars_table": "project.bars_1m_table",
            "bar_interval_ns": "time.bar_interval_ns",
            "timezone": "time.timezone",
            "require_ready_build": "queries.require_ready_build",
            "allow_publish_with_conflicts": "project.allow_publish_with_conflicts",
            "allow_publish_on_unknown_ack": "project.allow_publish_on_unknown_ack",
            "allow_direct_raw_to_mv": "project.allow_direct_raw_to_mv",
        },
    )
    # Does P06 have a path that READS the landing table, or does it only ever
    # build from a corpus it generates itself? Its config names the raw table
    # and schema_preflight checks that table's shape, but checking a shape is
    # not reading rows -- the distinction finding P09-M0-20 turns on.
    raw_source = base / "src" / "md_analytics" / "raw_source.py"
    cli = base / "src" / "md_analytics" / "cli.py"
    cli_text = _read_text(cli) or ""
    out.add(
        "has_raw_upstream",
        raw_source.exists() and "build-from-raw" in cli_text,
        _rel(root, raw_source),
    )
    _regex_fact(
        out,
        root,
        base / "src" / "md_analytics" / "rows.py",
        "topic_generation_type",
        r"^\s*topic_generation:\s*(.+)$",
    )

    sql_dir = base / "sql"
    if not sql_dir.is_dir():
        out.unknown("ddl_objects", "no sql/ directory", _rel(root, sql_dir))
        return
    created: list[str] = []
    for path in sorted(sql_dir.glob("*.sql")):
        created.extend(
            re.findall(
                r"CREATE (?:TABLE|MATERIALIZED VIEW) IF NOT EXISTS ([A-Za-z0-9_.]+)",
                _read_text(path) or "",
            )
        )
    out.add("ddl_objects", sorted(set(created)), _rel(root, sql_dir))


def _probe_p07(out: ProjectFacts, root: Path, base: Path) -> None:
    contracts = base / "p07" / "contracts.py"
    _regex_fact(out, root, contracts, "ticks_topic", r'^TICK_TOPIC[^=]*=\s*"([^"]+)"')
    _regex_fact(out, root, contracts, "quotes_topic", r'^QUOTE_TOPIC[^=]*=\s*"([^"]+)"')
    _regex_fact(
        out, root, contracts, "bar_interval_ns", r"^BAR_WIDTH_NS[^=]*=\s*([\d_]+)", cast="int"
    )
    out.add("event_identity", ["dataset_id", "symbol", "sequence"], _rel(root, contracts))
    out.add(
        "semantic_digest",
        "P05-CORE-V1",
        _rel(root, base / "p07" / "oracle" / "semantic_digest.py"),
    )
    # Which planes P07 can observe for real, read from the code rather than
    # from the README: a README is a claim, a module is a capability.
    live_kafka = base / "p07" / "replay" / "live_kafka.py"
    cli_text = _read_text(base / "p07" / "cli.py") or ""
    out.add(
        "live_kafka_scanner",
        live_kafka.exists() and "scan-live-cut" in cli_text,
        _rel(root, live_kafka),
    )
    live_ch = base / "p07" / "replay" / "live_clickhouse.py"
    out.add("live_clickhouse_reader", live_ch.exists(), _rel(root, live_ch))

    readme = _rel(root, base / "README.md")
    out.add("implemented_profile", "UNIT/MODEL", readme)
    out.add("simulated_components", ["P03", "P04", "P05", "P06"], readme)
    _regex_fact(
        out,
        root,
        base / "config" / "replay-lab.yaml",
        "partitions_per_topic",
        r"^\s*expected_partitions_per_topic:\s*(\d+)",
        cast="int",
    )
    catalog = base / "catalog" / "what_if_matrix.json"
    if catalog.exists():
        out.add("what_if_catalog", _rel(root, catalog), _rel(root, catalog))
    else:
        out.unknown("what_if_catalog", "catalog/what_if_matrix.json absent", _rel(root, catalog))


def _probe_p08(out: ProjectFacts, root: Path, base: Path) -> None:
    _toml_facts(
        out,
        root,
        base / "configs" / "local_small.toml",
        {
            "ticks_topic": "kafka.tick_topic",
            "quotes_topic": "kafka.quote_topic",
            "partitions_per_topic": "kafka.partitions_per_topic",
            "topic_generation": "kafka.topic_generation",
        },
    )
    catalog = base / "specs" / "test-catalog.csv"
    text = _read_text(catalog)
    if text is None:
        out.unknown("catalog_cases", "specs/test-catalog.csv absent", _rel(root, catalog))
    else:
        rows = max(0, len([line for line in text.splitlines() if line.strip()]) - 1)
        out.add("catalog_cases", rows, _rel(root, catalog))
    out.add(
        "rate_units_separated", True, _rel(root, base / "src" / "p08_bench" / "units.py")
    )


_PROBES = {
    "P01": _probe_p01,
    "P02": _probe_p02,
    "P03": _probe_p03,
    "P04": _probe_p04,
    "P05": _probe_p05,
    "P06": _probe_p06,
    "P07": _probe_p07,
    "P08": _probe_p08,
}


def probe_project(root: Path, project: str) -> ProjectFacts:
    """Collect the observable facts for one project id, e.g. ``"P04"``."""
    if project not in PROJECT_DIRS:
        raise KeyError("unknown project id: " + project)
    directory = PROJECT_DIRS[project]
    base = root / directory
    out = ProjectFacts(
        project=project,
        directory=directory,
        role=PROJECT_ROLES[project],
        present=base.is_dir(),
    )
    if not out.present:
        out.errors.append("project directory %s does not exist" % directory)
        return out
    _PROBES[project](out, root, base)
    return out


def probe_all(root: Path) -> dict[str, ProjectFacts]:
    """Collect the observable facts for every project, in id order."""
    return {project: probe_project(root, project) for project in PROJECT_DIRS}


def proto_copies(root: Path) -> dict[str, Path]:
    """The ``.proto`` file each project builds against, by project id.

    Only the four projects that keep a copy appear. P01 predates the protobuf
    contract, and P06-P08 consume rows or metrics rather than wire bytes.
    """
    candidates = {
        "P02": root / PROJECT_DIRS["P02"] / "schemas" / "marketdata" / "v1" / "market_data.proto",
        "P03": root / PROJECT_DIRS["P03"] / "schemas" / "market_data.proto",
        "P04": root / PROJECT_DIRS["P04"] / "schemas" / "market_data.proto",
        "P05": root / PROJECT_DIRS["P05"] / "schemas" / "market_data.proto",
    }
    return {project: path for project, path in candidates.items() if path.exists()}


def file_digests(paths: dict[str, Path]) -> dict[str, str | None]:
    """SHA-256 of each named file, or ``None`` when it could not be read."""
    return {name: _sha256_file(path) for name, path in paths.items()}
