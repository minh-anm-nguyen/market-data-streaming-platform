"""The bridge pipeline and the cross-language conformance runner.

Two things live here. The :class:`Pipeline` is the actual
JSONL -> domain -> Protobuf -> frames path with stage metrics, and the matrix
runners drive C++ and the schema variants as *separate processes* and compare
their projections.

On the oracle (section 9.1): the comparison is never "decode with the encoder
that produced it". Each direction is checked against the projection derived
independently from the source JSONL, so a bug shared by one language's encoder
and decoder cannot cancel out. ``verify_pbstream`` therefore takes the source
file, not just the stream.

Byte equality across runtimes is deliberately *not* asserted anywhere.
Protobuf's output is not canonical, so two different byte strings can carry the
same event; comparing hashes would fail on correct code and could pass on two
implementations that share a defect (ADR-005). Where byte stability does matter
-- replaying an exact frame -- the raw frame is kept instead.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence

from .codec import MAX_PAYLOAD_BYTES, decode_payload, encode_event, parse_payload, pb_to_domain
from .codec import check_envelope
from .config import Config
from .domain import DomainEvent, core_projection_v1, projection_json, projection_line
from .errors import CodecError, ContractError, EnvelopeError, PolicyError, StrictJsonError
from .framing import FileSource, FrameReader, PbStreamWriter
from .metrics import Metrics
from .source import FaultIndex, SourceDataset, sha256_file
from .strict_json import parse_sim_v1_line
from .validation import ValidationMode, Verdict, validate_business

#: Where the CMake build puts the C++ tools.
DEFAULT_CPP_BIN_DIR = Path(__file__).resolve().parents[3] / "build" / "cpp"
PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass
class PipelineResult:
    metrics: Metrics
    events: list[DomainEvent] = field(default_factory=list)
    faults_preserved: dict[str, int] = field(default_factory=dict)
    rejections: list[dict[str, Any]] = field(default_factory=list)
    output_path: Path | None = None
    output_sha256: str = ""

    @property
    def encoded(self) -> int:
        """Events that were serialised. The output count of an *encode* run."""
        return self.metrics.get("protobuf_encoded_total")

    @property
    def accepted(self) -> int:
        """Events that decoded and passed the envelope check.

        Zero for an encode run, by design: the two directions have different
        output stages and sharing one "count" between them would make an encode
        result look empty, or worse, make a decode result look complete when the
        encode counter happened to be set.
        """
        return self.metrics.get("events_accepted_total")

    def as_dict(self) -> dict[str, Any]:
        return {
            "metrics": self.metrics.as_dict(),
            "faults_preserved": dict(sorted(self.faults_preserved.items())),
            # Bounded on purpose: a corpus of 80% malformed input would
            # otherwise put a million samples in a report (case J12).
            "rejection_samples": self.rejections[:20],
            "rejection_sample_truncated": len(self.rejections) > 20,
            "output_path": str(self.output_path) if self.output_path else None,
            "output_sha256": self.output_sha256,
        }


@dataclass
class Pipeline:
    """sim.v1 JSONL -> validated domain events -> Protobuf -> MDP1 frames."""

    config: Config
    mode: ValidationMode | None = None
    fault_index: FaultIndex | None = None
    collect_events: bool = False

    def __post_init__(self) -> None:
        if self.mode is None:
            self.mode = self.config.validation.mode

    def _record_rejection(
        self, result: PipelineResult, stage: str, line_no: int, exc: ContractError
    ) -> None:
        result.metrics.reject(stage, exc.reason.value)
        if len(result.rejections) < 1000:
            result.rejections.append({"line": line_no, **exc.as_dict()})

    def run_file(
        self,
        source_path: str | Path,
        output_path: str | Path | None = None,
        dataset_id: str | None = None,
    ) -> PipelineResult:
        """Convert a whole file, writing a committed ``.pbstream`` if asked.

        ``dataset_id`` is written into the source manifest beside the artifact.
        It is optional because a caller converting a bare file may not know it;
        a consumer that requires it then fails loudly rather than reconciling
        against the wrong dataset.
        """
        result = PipelineResult(metrics=Metrics())
        metrics = result.metrics
        contract = self.config.contract

        writer_ctx = (
            PbStreamWriter(
                Path(output_path),
                max_payload_bytes=contract.max_payload_bytes,
                dataset_id=dataset_id,
            )
            if output_path
            else None
        )

        def emit(payload: bytes) -> None:
            if writer_ctx is not None:
                writer_ctx.write_frame(payload)

        if writer_ctx is not None:
            writer_ctx.__enter__()
        try:
            with open(source_path, "rb") as handle:
                for line_no, raw in enumerate(handle, start=1):
                    if not raw.strip():
                        continue
                    metrics.incr("json_lines_read_total")

                    try:
                        event = parse_sim_v1_line(
                            raw,
                            max_line_bytes=contract.max_json_line_bytes,
                            max_symbol_bytes=contract.max_symbol_bytes,
                            max_dataset_id_bytes=contract.max_dataset_id_bytes,
                        )
                    except StrictJsonError as exc:
                        self._record_rejection(result, "json", line_no, exc)
                        continue
                    metrics.incr("mapped_total")

                    try:
                        verdict = validate_business(
                            event, mode=self.mode, fault_index=self.fault_index
                        )
                    except PolicyError as exc:
                        self._record_rejection(result, "policy", line_no, exc)
                        continue

                    for fault_id in verdict.fault_ids:
                        result.faults_preserved[fault_id] = (
                            result.faults_preserved.get(fault_id, 0) + 1
                        )
                        metrics.incr("fault_passthrough_total")

                    start = time.perf_counter()
                    try:
                        payload = encode_event(
                            event, max_payload_bytes=contract.max_payload_bytes
                        )
                    except CodecError as exc:
                        self._record_rejection(result, "policy", line_no, exc)
                        continue
                    metrics.observe("encode_duration_us", (time.perf_counter() - start) * 1e6)
                    metrics.observe("payload_bytes", len(payload))
                    metrics.incr("protobuf_encoded_total")

                    emit(payload)
                    metrics.incr("frames_written_total")
                    if self.collect_events:
                        result.events.append(event)
        except Exception:
            if writer_ctx is not None:
                writer_ctx.__exit__(RuntimeError, None, None)
            raise
        else:
            if writer_ctx is not None:
                writer_ctx.__exit__(None, None, None)
                result.output_path = writer_ctx.path
                result.output_sha256 = sha256_file(writer_ctx.path)

        # Framing/decode stages did not run in this direction, so their
        # identities balance trivially at zero rather than being left undefined.
        metrics.incr("frames_received_total", 0)
        return result

    def run_dataset(
        self, run_dir: str | Path, output_path: str | Path | None = None
    ) -> tuple[PipelineResult, SourceDataset]:
        """Verify a Project 01 run directory, then convert it.

        The verification is not optional by default: an uncommitted source would
        otherwise be laundered into an apparently clean Protobuf stream (L02).
        """
        dataset = SourceDataset.load(
            run_dir,
            require_completed=self.config.artifacts.require_completed_source_manifest,
        )
        if self.mode is ValidationMode.FAULT_PASSTHROUGH:
            self.fault_index = dataset.require_fault_manifest()
        # The dataset identity travels with the artifact, so P03 can
        # reconcile it against a source instead of reporting
        # SOURCE_UNVERIFIED on data that is complete.
        result = self.run_file(
            dataset.events_path, output_path, dataset_id=dataset.dataset_id
        )
        return result, dataset


def decode_pbstream(
    input_path: str | Path,
    output_path: str | Path | None = None,
    *,
    config: Config | None = None,
    continue_on_error: bool = False,
) -> PipelineResult:
    """Read an MDP1 stream, validate every frame, write projections.

    Framing failures always stop the stream, even with ``continue_on_error``:
    once the byte alignment is unknown there is no honest way to find the next
    frame, and scanning for one would silently skip data (ADR-007).
    """
    config = config or Config()
    max_payload = config.contract.max_payload_bytes
    result = PipelineResult(metrics=Metrics())
    metrics = result.metrics

    out = Path(output_path).open("wb") if output_path else None
    try:
        with open(input_path, "rb") as handle:
            reader = FrameReader(
                FileSource(handle), max_payload_bytes=max_payload, deadline_s=None
            )
            while True:
                try:
                    payload = reader.read_frame()
                except ContractError as exc:
                    metrics.incr("frames_received_total")
                    metrics.reject("frame", exc.reason.value)
                    result.rejections.append(exc.as_dict())
                    break
                if payload is None:
                    break

                metrics.incr("frames_received_total")
                metrics.incr("payloads_submitted_total")
                metrics.observe("payload_bytes", len(payload))

                start = time.perf_counter()
                try:
                    msg = parse_payload(payload, max_payload_bytes=max_payload)
                except CodecError as exc:
                    metrics.reject("decode", exc.reason.value)
                    result.rejections.append(exc.as_dict())
                    if continue_on_error:
                        continue
                    break
                metrics.observe("decode_duration_us", (time.perf_counter() - start) * 1e6)
                metrics.incr("protobuf_parsed_total")

                event = pb_to_domain(msg)
                try:
                    check_envelope(event)
                except EnvelopeError as exc:
                    metrics.reject("envelope", exc.reason.value)
                    result.rejections.append(exc.as_dict())
                    if continue_on_error:
                        continue
                    break

                metrics.incr("events_accepted_total")
                result.events.append(event)
                if out is not None:
                    out.write(projection_line(event))

            metrics.gauges["stream_end"] = 0.0
            result.metrics.gauges.pop("stream_end", None)
            metrics.counters["stream_end_completed"] = int(
                reader.end is not None and reader.end.value == "COMPLETED"
            )
    finally:
        if out is not None:
            out.close()
    return result


@dataclass
class VerifyResult:
    """Outcome of comparing a stream against the source it came from."""

    source_events: int
    decoded_events: int
    mismatches: list[dict[str, Any]] = field(default_factory=list)
    first_mismatch_index: int | None = None

    @property
    def passed(self) -> bool:
        return (
            not self.mismatches
            and self.source_events == self.decoded_events
            and self.source_events > 0
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_events": self.source_events,
            "decoded_events": self.decoded_events,
            "semantic_mismatches": len(self.mismatches),
            "first_mismatch_index": self.first_mismatch_index,
            "mismatch_samples": self.mismatches[:10],
            "passed": self.passed,
        }


def verify_pbstream(
    pbstream_path: str | Path,
    expected_jsonl: str | Path,
    *,
    config: Config | None = None,
    mode: ValidationMode = ValidationMode.FAULT_PASSTHROUGH,
) -> VerifyResult:
    """Compare a stream's decoded events against the source JSONL, in order.

    Order is part of the contract: Project 01 may have emitted a duplicate or
    swapped two events on purpose, and a verifier that sorted or de-duplicated
    would hide exactly the anomaly the dataset was built to carry (F01-F03).
    """
    config = config or Config()
    expected: list[DomainEvent] = []
    with open(expected_jsonl, "rb") as handle:
        for raw in handle:
            if raw.strip():
                expected.append(parse_sim_v1_line(raw))

    decoded = decode_pbstream(pbstream_path, config=config).events
    result = VerifyResult(source_events=len(expected), decoded_events=len(decoded))

    for index, (want, got) in enumerate(zip(expected, decoded)):
        want_projection = core_projection_v1(want)
        got_projection = core_projection_v1(got)
        if want_projection != got_projection:
            if result.first_mismatch_index is None:
                result.first_mismatch_index = index
            if len(result.mismatches) < 100:
                differing = sorted(
                    key
                    for key in set(want_projection) | set(got_projection)
                    if want_projection.get(key) != got_projection.get(key)
                )
                result.mismatches.append(
                    {
                        "index": index,
                        "fields": differing,
                        "expected": {k: want_projection.get(k) for k in differing},
                        "actual": {k: got_projection.get(k) for k in differing},
                    }
                )
    if len(expected) != len(decoded):
        result.mismatches.append(
            {
                "index": min(len(expected), len(decoded)),
                "fields": ["<count>"],
                "expected": {"count": len(expected)},
                "actual": {"count": len(decoded)},
            }
        )
    return result


# ---------------------------------------------------------------------------
# C++ tool driving
# ---------------------------------------------------------------------------


class CppToolsUnavailable(RuntimeError):
    """The C++ binaries are not built. Reported as SKIPPED_WITH_REASON, not PASS."""


@dataclass
class CppTools:
    """Locates and runs the C++ tools, which are separate processes by design."""

    bin_dir: Path = DEFAULT_CPP_BIN_DIR

    def path(self, name: str) -> Path:
        exe = self.bin_dir / (f"{name}.exe" if os.name == "nt" else name)
        if not exe.exists():
            raise CppToolsUnavailable(
                f"{exe} not found; build it with cmake --build build/cpp"
            )
        return exe

    @property
    def available(self) -> bool:
        try:
            for name in ("md_cpp_decode", "md_cpp_encode"):
                self.path(name)
        except CppToolsUnavailable:
            return False
        return True

    def run(self, name: str, *args: str, timeout: float = 600.0) -> dict[str, Any]:
        """Run a tool and parse its JSON report. Includes the exit code."""
        exe = self.path(name)
        env = dict(os.environ)
        # The MSYS2 bin directory holds libprotobuf's DLL; without it on PATH
        # the tools fail to start with no useful message.
        msys_bin = r"C:\msys64\ucrt64\bin"
        if os.name == "nt" and Path(msys_bin).exists():
            env["PATH"] = msys_bin + os.pathsep + env.get("PATH", "")
        completed = subprocess.run(
            [str(exe), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        report: dict[str, Any] = {
            "exit_code": completed.returncode,
            "stderr": completed.stderr.strip()[:2000],
        }
        stdout = completed.stdout.strip()
        if stdout:
            try:
                report.update(json.loads(stdout.splitlines()[-1]))
            except json.JSONDecodeError:
                report["stdout"] = stdout[:2000]
        return report

    def decode(
        self,
        input_path: str | Path,
        output_path: str | Path | None = None,
        *,
        continue_on_error: bool = False,
        max_payload_bytes: int | None = None,
    ) -> dict[str, Any]:
        args = ["--input", str(input_path)]
        if output_path:
            args += ["--output", str(output_path)]
        if continue_on_error:
            args.append("--continue-on-error")
        if max_payload_bytes is not None:
            args += ["--max-payload-bytes", str(max_payload_bytes)]
        return self.run("md_cpp_decode", *args)

    def encode(
        self,
        input_path: str | Path,
        output_path: str | Path,
        *,
        projection_out: str | Path | None = None,
        continue_on_error: bool = False,
    ) -> dict[str, Any]:
        args = ["--input", str(input_path), "--output", str(output_path)]
        if projection_out:
            args += ["--projection-out", str(projection_out)]
        if continue_on_error:
            args.append("--continue-on-error")
        return self.run("md_cpp_encode", *args)


def run_variant(
    *args: str, timeout: float = 600.0, cwd: Path | None = None
) -> dict[str, Any]:
    """Run :mod:`md_contract.variant_runner` in a fresh interpreter."""
    env = dict(os.environ)
    src = str(PROJECT_ROOT / "python" / "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-m", "md_contract.variant_runner", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
        cwd=str(cwd or PROJECT_ROOT),
    )
    report: dict[str, Any] = {
        "exit_code": completed.returncode,
        "stderr": completed.stderr.strip()[:2000],
    }
    stdout = completed.stdout.strip()
    if stdout:
        try:
            report.update(json.loads(stdout.splitlines()[-1]))
        except json.JSONDecodeError:
            report["stdout"] = stdout[:2000]
    return report


@dataclass
class MatrixEntry:
    producer: str
    consumer: str
    expected: str
    verdict: str
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.verdict == "PASS"

    def with_expected(self, expected: str) -> "MatrixEntry":
        """Restate what this row was expected to do, for the report table."""
        self.expected = expected
        return self

    def as_dict(self) -> dict[str, Any]:
        return {
            "producer": self.producer,
            "consumer": self.consumer,
            "expected": self.expected,
            "verdict": self.verdict,
            "detail": self.detail,
        }


def _projection_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def cross_language_matrix(
    source_jsonl: str | Path,
    work_dir: str | Path,
    *,
    config: Config | None = None,
    tools: CppTools | None = None,
    limit: int | None = None,
) -> list[MatrixEntry]:
    """Run the four C++/Python directions and compare projections.

    The reference projection is derived from the source JSONL by Python's strict
    reader, and each direction is compared to *that* rather than to another
    direction's output -- so this cannot pass by two components agreeing on the
    same mistake.
    """
    config = config or Config()
    tools = tools or CppTools()
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    entries: list[MatrixEntry] = []

    source = Path(source_jsonl)
    if limit is not None:
        trimmed = work / "source_limited.jsonl"
        with source.open("rb") as src, trimmed.open("wb") as dst:
            for index, line in enumerate(src):
                if index >= limit:
                    break
                dst.write(line)
        source = trimmed

    # The independent reference: source -> strict parse -> projection.
    reference = work / "reference_proj.jsonl"
    pipeline = Pipeline(config=config, mode=ValidationMode.FAULT_PASSTHROUGH)
    py_stream = work / "python_v1.pbstream"
    result = pipeline.run_file(source, py_stream)
    with reference.open("wb") as handle:
        for _, item in _iter_events(source):
            handle.write(projection_line(item))
    reference_lines = _projection_lines(reference)

    if not tools.available:
        entries.append(
            MatrixEntry(
                "python v1",
                "cpp v1",
                "ACCEPT",
                "SKIPPED_WITH_REASON",
                {"reason": "C++ tools not built; run cmake --build build/cpp"},
            )
        )
        return entries

    # 1. Python encode -> C++ decode (E01)
    cpp_out = work / "cpp_decoded_proj.jsonl"
    report = tools.decode(py_stream, cpp_out)
    entries.append(
        _compare_entry(
            "python v1", "cpp v1", reference_lines, cpp_out, report, result.encoded
        )
    )

    # 2. C++ encode -> Python decode (E02)
    cpp_stream = work / "cpp_v1.pbstream"
    cpp_enc_proj = work / "cpp_encoded_proj.jsonl"
    enc_report = tools.encode(source, cpp_stream, projection_out=cpp_enc_proj)
    py_out = work / "python_decoded_proj.jsonl"
    py_result = decode_pbstream(cpp_stream, py_out, config=config)
    entries.append(
        _compare_entry(
            "cpp v1",
            "python v1",
            reference_lines,
            py_out,
            {**enc_report, "python_accepted": py_result.accepted},
            py_result.accepted,
        )
    )

    # 3. The C++ strict JSON reader against the Python one, on the same input.
    # This is the direct differential test of the two independent parsers.
    entries.append(
        _compare_entry(
            "cpp strict_json",
            "reference projection",
            reference_lines,
            cpp_enc_proj,
            {"note": "C++ reader vs Python reader on identical input"},
            len(reference_lines),
        )
    )

    # 4. Byte-level note. Equal bytes are fine but are not the criterion; the
    # point of recording it is to show the suite does not depend on it.
    same_bytes = py_stream.read_bytes() == cpp_stream.read_bytes()
    entries.append(
        MatrixEntry(
            "python v1",
            "cpp v1",
            "SEMANTIC EQUALITY (bytes not required to match)",
            "PASS",
            {
                "wire_bytes_identical": same_bytes,
                "note": (
                    "Protobuf output is not canonical, so this is an observation, "
                    "not an assertion; the oracle is the typed projection (ADR-005)"
                ),
            },
        )
    )
    return entries


def _compare_entry(
    producer: str,
    consumer: str,
    reference_lines: list[str],
    actual_path: Path,
    report: dict[str, Any],
    accepted: int,
) -> MatrixEntry:
    if not actual_path.exists():
        return MatrixEntry(
            producer, consumer, "ACCEPT", "FAIL", {"reason": "no output", **report}
        )
    actual = _projection_lines(actual_path)
    detail: dict[str, Any] = {
        "reference_events": len(reference_lines),
        "actual_events": len(actual),
        "accepted": accepted,
        **report,
    }
    if len(actual) != len(reference_lines):
        detail["reason"] = "event count differs"
        return MatrixEntry(producer, consumer, "ACCEPT", "FAIL", detail)
    for index, (want, got) in enumerate(zip(reference_lines, actual)):
        if want != got:
            detail["reason"] = "projection mismatch"
            detail["first_mismatch_index"] = index
            detail["expected"] = want[:300]
            detail["actual"] = got[:300]
            return MatrixEntry(producer, consumer, "ACCEPT", "FAIL", detail)
    detail["projections_identical"] = True
    return MatrixEntry(producer, consumer, "ACCEPT", "PASS", detail)


def version_matrix(
    source_jsonl: str | Path, work_dir: str | Path, *, limit: int = 500
) -> list[MatrixEntry]:
    """v1 <-> v1.1 <-> v2 in separate processes (section 5.4, cases G01-G06)."""
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    source = str(source_jsonl)
    entries: list[MatrixEntry] = []

    reference = work / "version_reference_proj.jsonl"
    count = 0
    with reference.open("wb") as handle:
        for _, event in _iter_events(Path(source)):
            if count >= limit:
                break
            handle.write(projection_line(event))
            count += 1
    reference_lines = _projection_lines(reference)

    plans = [
        ("v1_1", "v1", "ACCEPT core; field 20 unknown", ["--feed-label", "prod-a"], "source"),
        ("v1", "v1_1", "ACCEPT; feed_label absent", [], "source"),
        ("v1", "v1", "ACCEPT", [], "source"),
        ("v1_1", "v1_1", "ACCEPT", ["--feed-label", "prod-a"], "source"),
    ]
    for writer, reader, expected, extra, payload in plans:
        stream = work / f"{writer}_to_{reader}.pbstream"
        out = work / f"{writer}_to_{reader}_proj.jsonl"
        enc = run_variant(
            "--variant", writer, "--op", "encode", "--input", source,
            "--output", str(stream), "--limit", str(limit), *extra,
        )
        dec = run_variant(
            "--variant", reader, "--op", "decode", "--input", str(stream),
            "--output", str(out),
        )
        entries.append(
            _compare_entry(
                f"python {writer}",
                f"python {reader}",
                reference_lines,
                out,
                {"encode": enc, "decode": dec},
                dec.get("events_accepted", 0),
            ).with_expected(expected)
        )

    # A v2 writer's new oneof variant must FAIL CLOSED at a v1 reader: counted
    # as unsupported, never silently read as a quote or a tick (A11, D20, G06).
    depth_stream = work / "v2_depth.pbstream"
    enc = run_variant(
        "--variant", "v2_probe", "--op", "encode", "--input", source,
        "--output", str(depth_stream), "--payload", "depth_update",
        "--limit", str(min(limit, 100)),
    )
    out = work / "v2_depth_by_v1_proj.jsonl"
    dec = run_variant(
        "--variant", "v1", "--op", "decode", "--input", str(depth_stream),
        "--output", str(out),
    )
    encoded = enc.get("events_encoded", 0)
    unsupported = dec.get("unsupported_event_kind", -1)
    accepted = dec.get("events_accepted", -1)
    verdict = "PASS" if (accepted == 0 and unsupported == encoded and encoded > 0) else "FAIL"
    entries.append(
        MatrixEntry(
            "python v2_probe (depth_update)",
            "python v1",
            "FAIL CLOSED as UNSUPPORTED_EVENT_KIND",
            verdict,
            {"encoded": encoded, "unsupported_event_kind": unsupported,
             "events_accepted": accepted, "encode": enc, "decode": dec},
        )
    )
    return entries


def _iter_events(path: Path) -> Iterator[tuple[int, DomainEvent]]:
    with path.open("rb") as handle:
        for index, raw in enumerate(handle, start=1):
            if raw.strip():
                yield index, parse_sim_v1_line(raw)


def environment_metadata() -> dict[str, Any]:
    """Everything a result must be attributed to (FR-10, section 15)."""
    import google.protobuf
    from google.protobuf.internal import api_implementation

    lock = PROJECT_ROOT / "toolchain" / "versions.lock"
    locked: dict[str, str] = {}
    if lock.exists():
        for line in lock.read_text(encoding="utf-8").splitlines():
            if line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            locked[key.strip()] = value.strip().strip('"')
    return {
        "python_version": platform.python_version(),
        "python_protobuf_runtime_version": google.protobuf.__version__,
        "python_protobuf_implementation": api_implementation.Type(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "toolchain_lock": locked,
    }


def verify_toolchain() -> list[str]:
    """Problems with the pinned toolchain. Empty means the lock holds.

    Checked rather than assumed: a Python gencode/runtime skew or a C++ runtime
    that does not match the lock is a release stop, and the point of pinning is
    that the mismatch is found at build time rather than in production
    (cases E05, E06, E09, K02).
    """
    import google.protobuf

    problems: list[str] = []
    meta = environment_metadata()
    locked = meta["toolchain_lock"]
    if not locked:
        return ["toolchain/versions.lock is missing; run scripts/generate_proto.sh"]

    expected_py = locked.get("python_protobuf_runtime_version")
    if expected_py and expected_py != google.protobuf.__version__:
        problems.append(
            f"python protobuf runtime {google.protobuf.__version__} != pinned {expected_py}"
        )

    proto = PROJECT_ROOT / "schemas" / "marketdata" / "v1" / "market_data.proto"
    if proto.exists():
        actual = sha256_file(proto)
        expected = locked.get("proto_v1_sha256")
        if expected and actual != expected:
            problems.append(
                f"schemas/marketdata/v1/market_data.proto sha256 {actual} != pinned "
                f"{expected}; regenerate code before relying on this build"
            )

    baseline = PROJECT_ROOT / "toolchain" / "descriptor-baselines" / "marketdata-v1.desc"
    if baseline.exists():
        actual = sha256_file(baseline)
        expected = locked.get("descriptor_v1_sha256")
        if expected and actual != expected:
            problems.append(f"descriptor baseline sha256 {actual} != pinned {expected}")
    return problems
