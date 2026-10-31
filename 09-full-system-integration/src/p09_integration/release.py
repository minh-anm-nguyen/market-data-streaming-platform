"""M1/M10: provenance, a preflight, and a release manifest that can say NO-GO.

P09's gate G0 asks whether every artifact is identified by something immutable.
A tag is not: ``apache/kafka:4.2.0`` can be repushed, and two machines that
pulled it a month apart may be running different bytes. So this module reports a
tag-pinned image as a **provenance gap**, not as done, and the release manifest
records which images are pinned by digest and which are not.

Three rules it holds to, each the opposite of a convenient default:

* **A digest that could not be read is ``null``, never the tag.** Unknown
  provenance and verified provenance are different states, and collapsing them
  is how an unreproducible release gets signed off.
* **G0 fails on any unpinned image.** Not a warning. The gate exists to stop
  exactly this, and a gate that passes with findings is decoration.
* **The manifest records the audit verdict and the contract identity it was cut
  against.** A release artifact that cannot say which contract it implements
  cannot be reconciled with the system later.

What this does not do is sign anything or produce an SBOM. Both need tooling
this lab does not have (cosign, syft), and a file claiming to be an SBOM that
was assembled by hand would be worse than none. The manifest names them as
missing instead, so the gap is visible in the release record rather than absent
from it.
"""

from __future__ import annotations

import datetime as _dt
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import yamlsub

#: Images are declared in the compose file; this reads them rather than keeping
#: a second list that could disagree with what actually runs.
COMPOSE_RELATIVE = "compose/compose.dev.yaml"

_IMAGE_LINE = re.compile(r"^\s{2,}image:\s*(\S+)\s*$")
_SERVICE_LINE = re.compile(r"^  ([a-z0-9][a-z0-9_-]*):\s*$")


@dataclass(frozen=True)
class ImageRef:
    """One image a deployment would run."""

    service: str
    ref: str
    digest: str | None
    digest_source: str

    @property
    def pinned_by_digest(self) -> bool:
        return "@sha256:" in self.ref

    @property
    def pinned_by(self) -> str:
        return "digest" if self.pinned_by_digest else "tag"

    def as_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "ref": self.ref,
            "pinned_by": self.pinned_by,
            "digest": self.digest,
            "digest_source": self.digest_source,
        }


def parse_compose_images(path: Path) -> list[ImageRef]:
    """Read ``service:`` / ``image:`` pairs out of the compose file.

    A line reader rather than a YAML parser: the compose file is outside the
    subset :mod:`p09_integration.yamlsub` accepts, and taking a full YAML
    dependency to read two fields would be a poor trade. The shape it relies on
    is narrow, and ``docker compose config`` asserts the file in CI anyway.
    """
    images: list[ImageRef] = []
    service: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("#"):
            continue
        service_match = _SERVICE_LINE.match(line)
        if service_match:
            service = service_match.group(1)
            continue
        image_match = _IMAGE_LINE.match(line)
        if image_match and service is not None:
            images.append(
                ImageRef(
                    service=service,
                    ref=image_match.group(1),
                    digest=None,
                    digest_source="not-resolved",
                )
            )
    return images


def resolve_digests(images: list[ImageRef], *, docker: str = "docker") -> list[ImageRef]:
    """Ask the local daemon what digest each tag currently resolves to.

    A locally-resolved digest is evidence about *this host's* image, which is
    what a release record should capture: what ran, not what the registry holds
    now. An image the daemon does not have stays ``null``.
    """
    resolved: list[ImageRef] = []
    for image in images:
        if image.pinned_by_digest:
            resolved.append(
                ImageRef(
                    image.service,
                    image.ref,
                    image.ref.split("@", 1)[1],
                    "pinned-in-compose",
                )
            )
            continue
        try:
            completed = subprocess.run(
                [
                    docker,
                    "image",
                    "inspect",
                    "--format",
                    "{{index .RepoDigests 0}}",
                    image.ref,
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            resolved.append(
                ImageRef(image.service, image.ref, None, "docker unavailable: %s" % exc)
            )
            continue
        output = completed.stdout.strip()
        if completed.returncode != 0 or "@sha256:" not in output:
            # Unknown, not assumed. A locally built image has no repo digest at
            # all, and inventing one would be a lie about where the bytes came
            # from.
            resolved.append(
                ImageRef(image.service, image.ref, None, "no repo digest on this host")
            )
            continue
        resolved.append(
            ImageRef(
                image.service,
                image.ref,
                output.split("@", 1)[1],
                "resolved from the local daemon",
            )
        )
    return resolved


@dataclass
class PreflightResult:
    """What a release gate can and cannot conclude before anything is deployed."""

    images: list[ImageRef]
    lock_verdict: str
    schema_identity: str | None
    open_findings: int
    notes: list[str]

    @property
    def unpinned(self) -> list[ImageRef]:
        return [i for i in self.images if not i.pinned_by_digest]

    @property
    def g0(self) -> str:
        """Build and provenance. Fails on a tag, deliberately."""
        if not self.images:
            return "INCONCLUSIVE"
        return "NO_GO" if self.unpinned else "GO"

    def as_dict(self) -> dict[str, Any]:
        return {
            "g0_build_provenance": self.g0,
            "images": [i.as_dict() for i in self.images],
            "unpinned_images": [i.ref for i in self.unpinned],
            "contract_lock_verdict": self.lock_verdict,
            "schema_identity": self.schema_identity,
            "open_contract_findings": self.open_findings,
            "notes": list(self.notes),
        }


def preflight(root: Path, project_dir: Path, lock: dict[str, Any]) -> PreflightResult:
    """Check what a release would be made of, before making one."""
    compose = project_dir / COMPOSE_RELATIVE
    notes: list[str] = []
    images: list[ImageRef] = []
    if compose.exists():
        images = resolve_digests(parse_compose_images(compose))
    else:
        notes.append("compose file not found at %s" % COMPOSE_RELATIVE)

    schema = (lock.get("cross_project_identity") or {}).get("wire_schema") or {}
    open_findings = len(lock.get("open_findings") or [])

    for image in images:
        if not image.pinned_by_digest:
            notes.append(
                "%s is pinned by tag (%s); G0 cannot pass on a tag, because a "
                "tag can be repushed" % (image.service, image.ref)
            )
    if open_findings:
        notes.append(
            "%d contract findings are open; a release carrying them must say so"
            % open_findings
        )
    notes.append(
        "no SBOM and no signature: this lab has neither syft nor cosign, and a "
        "hand-assembled SBOM would be worse than a recorded absence"
    )

    return PreflightResult(
        images=images,
        lock_verdict=str(lock.get("audit_verdict", "UNKNOWN")),
        schema_identity=schema.get("normalized_sha256"),
        open_findings=open_findings,
        notes=notes,
    )


_HEADER = [
    "release-manifest.yaml -- generated by `p09 release-manifest`.",
    "",
    "What a release would consist of, and what cannot be claimed about it.",
    "Every gate carries its measured value; none defaults to GO.",
]


def build_release_manifest(
    result: PreflightResult,
    lock: dict[str, Any],
    *,
    infra_verdict: str | None = None,
    pipeline_verdict: str | None = None,
) -> dict[str, Any]:
    """Assemble the release record, with each gate at its measured value."""
    now = (
        _dt.datetime.now(_dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    cross = lock.get("cross_project_identity") or {}
    kafka = cross.get("kafka") or {}
    return {
        "manifest_version": 1,
        "cut_at_utc": now,
        "profile": "DEV",
        "contract": {
            "lock_generated_at_utc": lock.get("generated_at_utc"),
            "audit_verdict": lock.get("audit_verdict"),
            "schema_identity": result.schema_identity,
            "open_findings": result.open_findings,
            "ticks_topic": kafka.get("ticks_topic"),
            "quotes_topic": kafka.get("quotes_topic"),
            "clickhouse_raw_table": (cross.get("clickhouse") or {}).get("raw_table"),
        },
        "images": [i.as_dict() for i in result.images],
        # Each gate is its own claim (P09 section 16.1). A gate with no evidence
        # is NOT_RUN, which is a NO-GO for a required gate and is never silently
        # promoted to a pass.
        "gates": {
            "G0_build_provenance": result.g0,
            "G1_safety_security": "NOT_RUN",
            "G2_startup_persistence": infra_verdict or "NOT_RUN",
            "G3_finite_correctness": pipeline_verdict or "NOT_RUN",
            "G4_fault_recovery": "NOT_RUN",
            "G5_rollback_migration": "NOT_RUN",
            "G6_durability_dr": "NOT_RUN",
            "G7_capacity": "NOT_RUN",
            "G8_observability": "NOT_RUN",
        },
        "missing_evidence": [
            "SBOM (no syft in this lab)",
            "artifact signature (no cosign in this lab)",
            "image digests for tag-pinned images",
        ],
        "notes": result.notes,
        "verdict": "NO_GO",
        "verdict_reason": (
            "G0 fails while any image is pinned by tag, and the required gates "
            "G1 and G4-G8 have not been run. P09 section 16.1: an un-run "
            "required gate is a NO-GO, not a pass."
        ),
    }


def write_release_manifest(path: Path, document: dict[str, Any]) -> str:
    text = yamlsub.dumps(document, header=_HEADER)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")
    return text
