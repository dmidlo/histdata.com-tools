"""Matrix-bound certification with fresh, closed evidence verification.

Historical scalar-aggregation dossiers remain readable in ``certification``.
This successor never promotes one of those reports, a recorded matrix state,
or a deserialized verification receipt into execution authority. Every public
evaluation and dossier verification repeats the concrete evidence dispatch.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar

from histdatacom.synthetic.capability_matrix import (
    CapabilityClaimKindV1,
    CapabilityMatrixPolicyV1,
    CapabilityMatrixV1,
    CapabilityStateV1,
    structural_matrix_blockers,
)
from histdatacom.synthetic.capability_verification import (
    MAX_DECLARATIONS,
    CapabilityEvidenceDeclarationV1,
    CapabilityEvidenceVerificationV1,
    verify_capability_evidence,
)

MAX_CAPABILITY_CERTIFICATION_BYTES = 8 * 1024 * 1024


class CapabilityCertificationStateV1(str, Enum):
    """A narrow claim is never a complete-campaign certification."""

    BLOCKED = "blocked"
    VERIFIED_LIMITED_CLAIM = "verified_limited_claim"
    VERIFIED_FULL_CAMPAIGN = "verified_full_campaign"


def _canonical(value: dict[str, Any]) -> str:
    text = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    if len(text) > MAX_CAPABILITY_CERTIFICATION_BYTES:
        raise ValueError("capability certification exceeds byte bound")
    return text


def _identity(prefix: str, value: dict[str, Any]) -> str:
    return (
        prefix
        + ":sha256:"
        + hashlib.sha256(_canonical(value).encode("ascii")).hexdigest()
    )


def _derive(supplied: str, expected: str) -> str:
    if type(supplied) is not str or supplied not in ("", expected):
        raise ValueError("capability certification identity differs")
    return expected


def _fields(value: Any, expected: set[str], schema: str) -> None:
    if (
        type(value) is not dict
        or set(value) != expected | {"schema_version"}
        or value["schema_version"] != schema
    ):
        raise ValueError("capability certification schema or fields differ")


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in items:
        if key in value:
            raise ValueError("duplicate capability certification field")
        value[key] = item
    return value


def _nonfinite(value: str) -> Any:
    raise ValueError(f"nonfinite capability certification value: {value}")


def _load(text: str) -> dict[str, Any]:
    if (
        type(text) is not str
        or len(text) > MAX_CAPABILITY_CERTIFICATION_BYTES
        or len(text.encode("utf-8")) > MAX_CAPABILITY_CERTIFICATION_BYTES
    ):
        raise ValueError("capability certification exceeds byte bound")
    try:
        value = json.loads(
            text, object_pairs_hook=_pairs, parse_constant=_nonfinite
        )
        if type(value) is not dict:
            raise ValueError("capability certification requires an object")
        _canonical(value)
    except RecursionError as error:
        raise ValueError(
            "capability certification nesting exceeds bound"
        ) from error
    return value


@dataclass(frozen=True, slots=True)
class CapabilityCertificationSpecV1:
    """Frozen complete matrix, exact claim, and concrete evidence declarations."""

    policy: CapabilityMatrixPolicyV1
    matrix: CapabilityMatrixV1
    declarations: tuple[CapabilityEvidenceDeclarationV1, ...]
    spec_id: str = ""
    schema_version: ClassVar[str] = (
        "histdatacom.capability-certification-spec.v1"
    )

    def __post_init__(self) -> None:
        if (
            type(self.policy) is not CapabilityMatrixPolicyV1
            or type(self.matrix) is not CapabilityMatrixV1
        ):
            raise TypeError(
                "certification requires exact policy and matrix types"
            )
        policy = CapabilityMatrixPolicyV1.from_dict(self.policy.to_dict())
        matrix = CapabilityMatrixV1.from_dict(self.matrix.to_dict())
        # Re-admits identities even when the caller mutated a frozen object.
        structural_matrix_blockers(matrix, policy)
        if (
            type(self.declarations) is not tuple
            or len(self.declarations) > MAX_DECLARATIONS
        ):
            raise ValueError(
                "certification declarations require a bounded tuple"
            )
        declarations = []
        for item in self.declarations:
            if type(item) is not CapabilityEvidenceDeclarationV1:
                raise TypeError(
                    "certification requires exact evidence declarations"
                )
            declarations.append(
                CapabilityEvidenceDeclarationV1.from_dict(item.to_dict())
            )
        if len({item.requirement_id for item in declarations}) != len(
            declarations
        ):
            raise ValueError("duplicate capability evidence declaration")
        object.__setattr__(self, "policy", policy)
        object.__setattr__(self, "matrix", matrix)
        object.__setattr__(
            self,
            "declarations",
            tuple(sorted(declarations, key=lambda item: item.requirement_id)),
        )
        object.__setattr__(
            self,
            "spec_id",
            _derive(
                self.spec_id,
                _identity("capability-certification-spec", self._payload()),
            ),
        )

    def _payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.to_dict(),
            "matrix": self.matrix.to_dict(),
            "declarations": [item.to_dict() for item in self.declarations],
        }

    def to_dict(self) -> dict[str, Any]:
        """Return detached structural claims, not verification authority."""
        value = CapabilityCertificationSpecV1(
            self.policy, self.matrix, self.declarations, self.spec_id
        )
        return {**value._payload(), "spec_id": value.spec_id}

    def to_json(self) -> str:
        """Serialize deterministic bounded claims."""
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> CapabilityCertificationSpecV1:
        """Read exact fields and identities without executing any evidence."""
        _fields(
            value,
            {"policy", "matrix", "declarations", "spec_id"},
            cls.schema_version,
        )
        items = value["declarations"]
        if type(items) is not list or len(items) > MAX_DECLARATIONS:
            raise ValueError("certification wire declarations exceed bound")
        return cls(
            CapabilityMatrixPolicyV1.from_dict(value["policy"]),
            CapabilityMatrixV1.from_dict(value["matrix"]),
            tuple(
                CapabilityEvidenceDeclarationV1.from_dict(item)
                for item in items
            ),
            value["spec_id"],
        )

    @classmethod
    def from_json(cls, text: str) -> CapabilityCertificationSpecV1:
        """Read a bounded JSON specification; this is not an execution."""
        return cls.from_dict(_load(text))


def _state(
    spec: CapabilityCertificationSpecV1,
    verification: CapabilityEvidenceVerificationV1,
) -> CapabilityCertificationStateV1:
    if verification.blockers or structural_matrix_blockers(
        spec.matrix, spec.policy
    ):
        return CapabilityCertificationStateV1.BLOCKED
    required = {item.requirement_id for item in spec.policy.requirements}
    exercised_waiver = any(
        row.requirement_id in required
        and row.state is CapabilityStateV1.WAIVED_WITH_LIMITATION
        for row in spec.matrix.rows
    )
    if (
        spec.policy.claim_kind is CapabilityClaimKindV1.NARROWER_PREDECLARED
        or exercised_waiver
    ):
        return CapabilityCertificationStateV1.VERIFIED_LIMITED_CLAIM
    return CapabilityCertificationStateV1.VERIFIED_FULL_CAMPAIGN


@dataclass(frozen=True, slots=True)
class CapabilityCertificationDossierV1:
    """Retained matrix-bound outcome; parsing does not qualify current evidence."""

    spec: CapabilityCertificationSpecV1
    verification: CapabilityEvidenceVerificationV1
    state: CapabilityCertificationStateV1
    dossier_id: str = ""
    schema_version: ClassVar[str] = (
        "histdatacom.capability-certification-dossier.v1"
    )

    def __post_init__(self) -> None:
        if (
            type(self.spec) is not CapabilityCertificationSpecV1
            or type(self.verification) is not CapabilityEvidenceVerificationV1
        ):
            raise TypeError(
                "dossier requires exact specification and receipt types"
            )
        spec = CapabilityCertificationSpecV1.from_dict(self.spec.to_dict())
        receipt = CapabilityEvidenceVerificationV1.from_dict(
            self.verification.to_dict()
        )
        if type(
            self.state
        ) is not CapabilityCertificationStateV1 or self.state is not _state(
            spec, receipt
        ):
            raise ValueError(
                "dossier state differs from recorded evidence and matrix"
            )
        if not receipt.blockers and (
            receipt.release_id != spec.policy.release_id
            or receipt.dataset_id != spec.policy.dataset_id
        ):
            raise ValueError("passing dossier release/dataset binding differs")
        object.__setattr__(self, "spec", spec)
        object.__setattr__(self, "verification", receipt)
        object.__setattr__(
            self,
            "dossier_id",
            _derive(
                self.dossier_id,
                _identity("capability-certification-dossier", self._payload()),
            ),
        )

    def _payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "spec": self.spec.to_dict(),
            "verification": self.verification.to_dict(),
            "state": self.state.value,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return re-admitted recorded evidence; no current pass is conferred."""
        value = CapabilityCertificationDossierV1(
            self.spec, self.verification, self.state, self.dossier_id
        )
        return {**value._payload(), "dossier_id": value.dossier_id}

    def to_json(self) -> str:
        """Serialize the exact policy, matrix and independent verification."""
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(
        cls, value: dict[str, Any]
    ) -> CapabilityCertificationDossierV1:
        """Read retained structure only; use the fresh verifier before reliance."""
        _fields(
            value,
            {"spec", "verification", "state", "dossier_id"},
            cls.schema_version,
        )
        return cls(
            CapabilityCertificationSpecV1.from_dict(value["spec"]),
            CapabilityEvidenceVerificationV1.from_dict(value["verification"]),
            CapabilityCertificationStateV1(value["state"]),
            value["dossier_id"],
        )

    @classmethod
    def from_json(cls, text: str) -> CapabilityCertificationDossierV1:
        """Read recorded structure without trusting or replaying evidence."""
        return cls.from_dict(_load(text))


def evaluate_capability_certification(
    spec: CapabilityCertificationSpecV1, *, evidence_root: str | Path
) -> CapabilityCertificationDossierV1:
    """Execute closed native verifiers afresh, then enforce the frozen matrix."""
    if type(spec) is not CapabilityCertificationSpecV1:
        raise TypeError("exact capability specification required")
    spec = CapabilityCertificationSpecV1.from_dict(spec.to_dict())
    verification = verify_capability_evidence(
        spec.matrix,
        spec.policy,
        evidence_root=evidence_root,
        declarations=spec.declarations,
    )
    return CapabilityCertificationDossierV1(
        spec, verification, _state(spec, verification)
    )


def verify_capability_certification_dossier(
    dossier: CapabilityCertificationDossierV1, *, evidence_root: str | Path
) -> CapabilityCertificationDossierV1:
    """Re-execute native verification and require the exact retained outcome."""
    if type(dossier) is not CapabilityCertificationDossierV1:
        raise TypeError("exact capability dossier required")
    dossier = CapabilityCertificationDossierV1.from_dict(dossier.to_dict())
    fresh = evaluate_capability_certification(
        dossier.spec, evidence_root=evidence_root
    )
    if fresh.to_json() != dossier.to_json():
        raise ValueError(
            "retained capability dossier differs from fresh verification"
        )
    return fresh


def _read_bounded_regular(path: Path, maximum: int) -> bytes:
    """Refuse special files, symlinks and oversized inputs before reading."""
    declared = path.lstat()
    if not stat.S_ISREG(declared.st_mode) or declared.st_size > maximum:
        raise ValueError("capability artifact is not a bounded regular file")
    identity_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size > maximum
            or any(
                getattr(declared, name) != getattr(before, name)
                for name in identity_fields
            )
        ):
            raise ValueError(
                "capability artifact is not a bounded regular file"
            )
        encoded = stream.read(maximum + 1)
        after = os.fstat(stream.fileno())
    if (
        any(
            getattr(before, name) != getattr(after, name)
            for name in identity_fields
        )
        or len(encoded) > maximum
    ):
        raise ValueError("capability artifact changed or exceeds byte bound")
    return encoded


def read_capability_certification_spec(
    path: str | Path,
) -> CapabilityCertificationSpecV1:
    """Read a bounded specification; evidence paths are resolved separately."""
    encoded = _read_bounded_regular(
        Path(path), MAX_CAPABILITY_CERTIFICATION_BYTES
    )
    return CapabilityCertificationSpecV1.from_json(encoded.decode("utf-8"))


def render_capability_certification_markdown(
    dossier: CapabilityCertificationDossierV1,
) -> str:
    """Render the exact claim and blockers without promotion implications."""
    dossier = CapabilityCertificationDossierV1.from_dict(dossier.to_dict())
    policy = dossier.spec.policy

    def escape(value: str) -> str:
        return html.escape(value).replace("|", "&#124;")

    lines = [
        "# Matrix-bound capability certification",
        "",
        f"Recorded state: `{dossier.state.value}`.",
        "",
        f"Claim: {escape(policy.claim_label)} ({policy.claim_kind.value}).",
        f"Scope: {escape(policy.scope)}.",
        "",
        f"Dossier: `{dossier.dossier_id}`.",
        f"Matrix: `{dossier.spec.matrix.matrix_id}`.",
        f"Policy: `{policy.policy_id}`.",
        f"Release: `{policy.release_id}`; dataset: `{policy.dataset_id or 'absent'}`.",
        "",
        "A retained report is not current verification authority. Reverification repeats native checks against the exact input graph. A limited claim never certifies the complete 2002–cutoff campaign or authorizes package publication.",
        "",
        "## Limitations",
        "",
    ]
    lines.extend(f"- {escape(item)}" for item in policy.limitations)
    required = {item.requirement_id for item in policy.requirements}
    lines.extend(
        f"- Waiver `{row.requirement_id}`: {escape(row.waiver_policy_clause or '')}; "
        + escape("; ".join(row.limitations))
        for row in dossier.spec.matrix.rows
        if row.requirement_id in required
        and row.state is CapabilityStateV1.WAIVED_WITH_LIMITATION
    )
    if not policy.limitations:
        lines.append("No additional policy limitations declared.")
    lines.extend(["", "## Blockers", ""])
    lines.extend(
        f"- `{requirement}`: {escape(reason)}"
        for requirement, reason in dossier.verification.blockers
    )
    if not dossier.verification.blockers:
        lines.append("None for this exact frozen claim.")
    return "\n".join(lines) + "\n"


def _publish_exact(path: Path, encoded: bytes) -> None:
    """Publish without replacing an existing, potentially unrelated artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=".capability-", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            try:
                matches = _read_bounded_regular(path, len(encoded)) == encoded
            except (OSError, ValueError):
                matches = False
            if not matches:
                raise ValueError(
                    f"refusing to replace capability artifact: {path}"
                ) from None
    finally:
        Path(temporary).unlink()


def run_capability_certification(
    spec_path: str | Path,
    *,
    evidence_root: str | Path,
    output_directory: str | Path,
) -> CapabilityCertificationDossierV1:
    """Freshly evaluate and retain deterministic JSON/Markdown, including refusal."""
    spec = read_capability_certification_spec(spec_path)
    dossier = evaluate_capability_certification(
        spec, evidence_root=evidence_root
    )
    output = Path(output_directory)
    _publish_exact(
        output / "capability-certification-spec.json",
        (spec.to_json() + "\n").encode("ascii"),
    )
    _publish_exact(
        output / "capability-certification.json",
        (dossier.to_json() + "\n").encode("ascii"),
    )
    _publish_exact(
        output / "capability-certification.md",
        render_capability_certification_markdown(dossier).encode("utf-8"),
    )
    return dossier
