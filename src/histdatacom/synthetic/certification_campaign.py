"""Executable, hash-verified modern-reference certification campaigns.

The campaign is an evidence aggregator.  It never accepts scalar observations
directly: every measured value is extracted from one hash-verified JSON report
through a declared JSON pointer and remains bound to every supporting artifact.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONScalar, JSONValue
from histdatacom.synthetic.certification import (
    DEFAULT_CERTIFICATION_MAX_ARTIFACTS,
    DEFAULT_CERTIFICATION_MAX_METADATA_ITEMS,
    DEFAULT_CERTIFICATION_MAX_OBSERVATIONS,
    DEFAULT_CERTIFICATION_MAX_TEXT_LENGTH,
    PROMOTION_ONLY_CHECK_IDS,
    CertificationArtifactV1,
    CertificationObservationV1,
    CertificationState,
    ReconstructionCertificationDossierV2,
    evaluate_modern_reference_reconstruction_certification,
    modern_reference_triangle_certification_policy,
    write_modern_reference_reconstruction_certification_dossier,
)
from histdatacom.synthetic.contracts import canonical_contract_json

CERTIFICATION_CAMPAIGN_ARTIFACT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-campaign-artifact.v1"
)
CERTIFICATION_CAMPAIGN_OBSERVATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-campaign-observation.v1"
)
CERTIFICATION_CAMPAIGN_SPEC_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-campaign-spec.v1"
)
CERTIFICATION_CAMPAIGN_RESULT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-campaign-result.v1"
)
CERTIFICATION_METHODOLOGY_REPORT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-methodology-report.v1"
)
CAMPAIGN_MANIFEST_EVIDENCE_KEY = "__campaign_manifest__"
METHODOLOGY_REPORT_EVIDENCE_KEY = "__methodology_report__"
_AUTOMATIC_EVIDENCE_KEYS = frozenset(
    {CAMPAIGN_MANIFEST_EVIDENCE_KEY, METHODOLOGY_REPORT_EVIDENCE_KEY}
)
_AUTOMATIC_OBSERVATION_CHECKS = frozenset(
    {
        "methodology_and_limitations_published",
        "machine_evidence_manifest_published",
    }
)


@dataclass(frozen=True, slots=True)
class CertificationCampaignArtifactV1:
    """One strong JSON evidence input and its identity locations."""

    evidence_key: str
    kind: str
    path: str
    content_sha256: str
    subject_id: str
    subject_id_pointer: str
    subject_schema_version: str
    relative_path: str
    metadata: Mapping[str, JSONValue]
    schema_version: str = CERTIFICATION_CAMPAIGN_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            CERTIFICATION_CAMPAIGN_ARTIFACT_SCHEMA_VERSION,
            "campaign artifact",
        )
        for name in (
            "evidence_key",
            "kind",
            "path",
            "subject_id",
            "subject_schema_version",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if "broker" in self.kind:
            raise ValueError(
                "modern-reference campaign rejects broker evidence"
            )
        object.__setattr__(self, "content_sha256", _sha256(self.content_sha256))
        object.__setattr__(
            self, "subject_id_pointer", _json_pointer(self.subject_id_pointer)
        )
        object.__setattr__(
            self, "relative_path", _safe_relative_path(self.relative_path)
        )
        if len(self.metadata) > DEFAULT_CERTIFICATION_MAX_METADATA_ITEMS:
            raise ValueError("campaign artifact metadata exceeds limit")
        object.__setattr__(
            self,
            "metadata",
            {str(key): value for key, value in sorted(self.metadata.items())},
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible input metadata."""
        return {
            "schema_version": self.schema_version,
            "evidence_key": self.evidence_key,
            "kind": self.kind,
            "path": self.path,
            "content_sha256": self.content_sha256,
            "subject_id": self.subject_id,
            "subject_id_pointer": self.subject_id_pointer,
            "subject_schema_version": self.subject_schema_version,
            "relative_path": self.relative_path,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CertificationCampaignArtifactV1":
        """Restore one campaign artifact input."""
        return cls(
            evidence_key=str(data.get("evidence_key", "")),
            kind=str(data.get("kind", "")),
            path=str(data.get("path", "")),
            content_sha256=str(data.get("content_sha256", "")),
            subject_id=str(data.get("subject_id", "")),
            subject_id_pointer=str(data.get("subject_id_pointer", "")),
            subject_schema_version=str(data.get("subject_schema_version", "")),
            relative_path=str(data.get("relative_path", "")),
            metadata=_mapping(data.get("metadata"), "metadata"),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CertificationCampaignObservationV1:
    """One scalar extraction bound to its exact supporting evidence set."""

    check_id: str
    measurement_evidence_key: str
    measurement_pointer: str
    artifact_evidence_keys: tuple[str, ...]
    note: str = ""
    schema_version: str = CERTIFICATION_CAMPAIGN_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            CERTIFICATION_CAMPAIGN_OBSERVATION_SCHEMA_VERSION,
            "campaign observation",
        )
        object.__setattr__(self, "check_id", _required_text(self.check_id))
        object.__setattr__(
            self,
            "measurement_evidence_key",
            _required_text(self.measurement_evidence_key),
        )
        object.__setattr__(
            self, "measurement_pointer", _json_pointer(self.measurement_pointer)
        )
        keys = tuple(
            sorted(
                {_required_text(item) for item in self.artifact_evidence_keys}
            )
        )
        if not keys:
            raise ValueError("campaign observation requires artifact evidence")
        if self.measurement_evidence_key not in keys:
            raise ValueError(
                "measurement artifact must support its observation"
            )
        object.__setattr__(self, "artifact_evidence_keys", keys)
        object.__setattr__(
            self, "note", _bounded_text(self.note, allow_empty=True)
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic scalar-extraction metadata."""
        return {
            "schema_version": self.schema_version,
            "check_id": self.check_id,
            "measurement_evidence_key": self.measurement_evidence_key,
            "measurement_pointer": self.measurement_pointer,
            "artifact_evidence_keys": list(self.artifact_evidence_keys),
            "note": self.note,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CertificationCampaignObservationV1":
        """Restore one scalar-extraction declaration."""
        return cls(
            check_id=str(data.get("check_id", "")),
            measurement_evidence_key=str(
                data.get("measurement_evidence_key", "")
            ),
            measurement_pointer=str(data.get("measurement_pointer", "")),
            artifact_evidence_keys=_string_tuple(
                data.get("artifact_evidence_keys"), "artifact_evidence_keys"
            ),
            note=str(data.get("note", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ModernReferenceCertificationCampaignSpecV1:
    """Frozen policy budgets and evidence extraction plan for one campaign."""

    common_end_period: str
    peak_memory_budget_bytes: int
    scratch_budget_bytes: int
    runtime_budget_seconds: float
    storage_budget_bytes: int
    candidate_amplification_budget: float
    artifacts: tuple[CertificationCampaignArtifactV1, ...]
    observations: tuple[CertificationCampaignObservationV1, ...]
    methodology: str
    accepted_limitations: tuple[str, ...]
    blocking_limitations: tuple[str, ...]
    promotion_boundary: bool = False
    campaign_id: str = ""
    schema_version: str = CERTIFICATION_CAMPAIGN_SPEC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            CERTIFICATION_CAMPAIGN_SPEC_SCHEMA_VERSION,
            "campaign spec",
        )
        policy = modern_reference_triangle_certification_policy(
            common_end_period=self.common_end_period,
            peak_memory_budget_bytes=self.peak_memory_budget_bytes,
            scratch_budget_bytes=self.scratch_budget_bytes,
            runtime_budget_seconds=self.runtime_budget_seconds,
            storage_budget_bytes=self.storage_budget_bytes,
            candidate_amplification_budget=self.candidate_amplification_budget,
        )
        object.__setattr__(self, "common_end_period", policy.common_end_period)
        artifacts = tuple(
            sorted(self.artifacts, key=lambda item: item.evidence_key)
        )
        if (
            not artifacts
            or len(artifacts) > DEFAULT_CERTIFICATION_MAX_ARTIFACTS
        ):
            raise ValueError("campaign artifacts are empty or unbounded")
        if len({item.evidence_key for item in artifacts}) != len(artifacts):
            raise ValueError("campaign artifacts duplicate evidence keys")
        if {item.evidence_key for item in artifacts}.intersection(
            _AUTOMATIC_EVIDENCE_KEYS
        ):
            raise ValueError("campaign artifacts use a reserved evidence key")
        object.__setattr__(self, "artifacts", artifacts)
        observations = tuple(
            sorted(self.observations, key=lambda item: item.check_id)
        )
        if len(observations) > DEFAULT_CERTIFICATION_MAX_OBSERVATIONS:
            raise ValueError("campaign observations exceed limit")
        if len({item.check_id for item in observations}) != len(observations):
            raise ValueError("campaign observations duplicate checks")
        if {item.check_id for item in observations}.intersection(
            _AUTOMATIC_OBSERVATION_CHECKS
        ):
            raise ValueError("campaign publication observations are automatic")
        available = {
            item.evidence_key for item in artifacts
        } | _AUTOMATIC_EVIDENCE_KEYS
        missing_keys = {
            key
            for observation in observations
            for key in observation.artifact_evidence_keys
            if key not in available
        }
        if missing_keys:
            raise ValueError(
                f"campaign evidence keys are missing: {sorted(missing_keys)}"
            )
        if not self.promotion_boundary and any(
            item.check_id in PROMOTION_ONLY_CHECK_IDS for item in observations
        ):
            raise ValueError(
                "promotion-only coverage evidence is forbidden outside promotion"
            )
        if not isinstance(self.promotion_boundary, bool):
            raise ValueError("promotion_boundary must be boolean")
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "methodology", _bounded_text(self.methodology))
        object.__setattr__(
            self,
            "accepted_limitations",
            _bounded_text_tuple(self.accepted_limitations),
        )
        object.__setattr__(
            self,
            "blocking_limitations",
            _bounded_text_tuple(self.blocking_limitations),
        )
        expected = _stable_id(
            "reconstruction-certification-campaign", self.identity_payload()
        )
        supplied = str(self.campaign_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("certification campaign_id differs")
        object.__setattr__(self, "campaign_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return content-addressed campaign inputs."""
        return {
            "schema_version": self.schema_version,
            "common_end_period": self.common_end_period,
            "peak_memory_budget_bytes": self.peak_memory_budget_bytes,
            "scratch_budget_bytes": self.scratch_budget_bytes,
            "runtime_budget_seconds": self.runtime_budget_seconds,
            "storage_budget_bytes": self.storage_budget_bytes,
            "candidate_amplification_budget": self.candidate_amplification_budget,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "observations": [item.to_dict() for item in self.observations],
            "methodology": self.methodology,
            "accepted_limitations": list(self.accepted_limitations),
            "blocking_limitations": list(self.blocking_limitations),
            "promotion_boundary": self.promotion_boundary,
            "observation_values_inline": False,
            "broker_adaptation": "excluded",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible campaign inputs."""
        return {**self.identity_payload(), "campaign_id": self.campaign_id}

    def to_json(self) -> str:
        """Serialize the campaign deterministically."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ModernReferenceCertificationCampaignSpecV1":
        """Restore and verify a campaign specification."""
        if data.get("observation_values_inline") is not False:
            raise ValueError(
                "campaign cannot contain inline observation values"
            )
        if data.get("broker_adaptation") != "excluded":
            raise ValueError("campaign broker boundary differs")
        return cls(
            common_end_period=str(data.get("common_end_period", "")),
            peak_memory_budget_bytes=_strict_int(
                data.get("peak_memory_budget_bytes"), "peak_memory_budget_bytes"
            ),
            scratch_budget_bytes=_strict_int(
                data.get("scratch_budget_bytes"), "scratch_budget_bytes"
            ),
            runtime_budget_seconds=_number(
                data.get("runtime_budget_seconds"), "runtime_budget_seconds"
            ),
            storage_budget_bytes=_strict_int(
                data.get("storage_budget_bytes"), "storage_budget_bytes"
            ),
            candidate_amplification_budget=_number(
                data.get("candidate_amplification_budget"),
                "candidate_amplification_budget",
            ),
            artifacts=tuple(
                CertificationCampaignArtifactV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("artifacts"), "artifacts"
                )
            ),
            observations=tuple(
                CertificationCampaignObservationV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("observations"), "observations"
                )
            ),
            methodology=str(data.get("methodology", "")),
            accepted_limitations=_string_tuple(
                data.get("accepted_limitations"), "accepted_limitations"
            ),
            blocking_limitations=_string_tuple(
                data.get("blocking_limitations"), "blocking_limitations"
            ),
            promotion_boundary=_strict_bool(
                data.get("promotion_boundary"), "promotion_boundary"
            ),
            campaign_id=str(data.get("campaign_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ModernReferenceCertificationCampaignResultV1:
    """Public references and state produced by one campaign execution."""

    campaign_id: str
    policy_id: str
    dossier_id: str
    state: CertificationState
    dossier_json: ArtifactRef
    dossier_markdown: ArtifactRef
    verified_input_count: int
    observation_count: int
    schema_version: str = CERTIFICATION_CAMPAIGN_RESULT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded public campaign receipt."""
        return {
            "schema_version": self.schema_version,
            "campaign_id": self.campaign_id,
            "policy_id": self.policy_id,
            "dossier_id": self.dossier_id,
            "state": self.state.value,
            "dossier_json": self.dossier_json.to_dict(),
            "dossier_markdown": self.dossier_markdown.to_dict(),
            "verified_input_count": self.verified_input_count,
            "observation_count": self.observation_count,
        }


def read_modern_reference_certification_campaign_spec(
    path: str | Path,
) -> ModernReferenceCertificationCampaignSpecV1:
    """Read and identity-check one campaign specification."""
    return ModernReferenceCertificationCampaignSpecV1.from_dict(
        _read_json_mapping(Path(path).expanduser().resolve())
    )


def run_modern_reference_certification_campaign(
    spec_path: str | Path,
    *,
    output_directory: str | Path,
) -> tuple[
    ReconstructionCertificationDossierV2,
    ModernReferenceCertificationCampaignResultV1,
]:
    """Verify inputs, extract observations, and atomically publish a dossier."""
    source = Path(spec_path).expanduser().resolve()
    spec = read_modern_reference_certification_campaign_spec(source)
    output = Path(output_directory).expanduser().resolve()
    policy = modern_reference_triangle_certification_policy(
        common_end_period=spec.common_end_period,
        peak_memory_budget_bytes=spec.peak_memory_budget_bytes,
        scratch_budget_bytes=spec.scratch_budget_bytes,
        runtime_budget_seconds=spec.runtime_budget_seconds,
        storage_budget_bytes=spec.storage_budget_bytes,
        candidate_amplification_budget=spec.candidate_amplification_budget,
    )
    artifacts: list[CertificationArtifactV1] = []
    payloads: dict[str, Mapping[str, Any]] = {}
    evidence_by_key: dict[str, CertificationArtifactV1] = {}
    for declared in spec.artifacts:
        path = Path(declared.path).expanduser()
        if not path.is_absolute():
            path = source.parent / path
        path = path.resolve()
        payload, encoded = _verified_json_artifact(path, declared)
        artifact = CertificationArtifactV1(
            policy_id=policy.policy_id,
            kind=declared.kind,
            subject_id=declared.subject_id,
            subject_schema_version=declared.subject_schema_version,
            content_sha256=hashlib.sha256(encoded).hexdigest(),
            relative_path=declared.relative_path,
            size_bytes=len(encoded),
            verified=True,
            metadata=declared.metadata,
        )
        payloads[declared.evidence_key] = payload
        evidence_by_key[declared.evidence_key] = artifact
        artifacts.append(artifact)

    evidence_directory = output / "evidence"
    campaign_path = evidence_directory / "campaign-spec.json"
    campaign_bytes = spec.to_json().encode("utf-8") + b"\n"
    _atomic_write(campaign_path, campaign_bytes)
    campaign_artifact = CertificationArtifactV1(
        policy_id=policy.policy_id,
        kind="machine-evidence-manifest",
        subject_id=spec.campaign_id,
        subject_schema_version=spec.schema_version,
        content_sha256=hashlib.sha256(campaign_bytes).hexdigest(),
        relative_path="evidence/campaign-spec.json",
        size_bytes=len(campaign_bytes),
        verified=True,
        metadata={"observation_values_inline": False},
    )
    artifacts.append(campaign_artifact)
    payloads[CAMPAIGN_MANIFEST_EVIDENCE_KEY] = spec.to_dict()
    evidence_by_key[CAMPAIGN_MANIFEST_EVIDENCE_KEY] = campaign_artifact

    methodology_payload: dict[str, JSONValue] = {
        "schema_version": CERTIFICATION_METHODOLOGY_REPORT_SCHEMA_VERSION,
        "campaign_id": spec.campaign_id,
        "methodology": spec.methodology,
        "accepted_limitations": list(spec.accepted_limitations),
        "blocking_limitations": list(spec.blocking_limitations),
        "scientific_nonclaim_published": True,
        "historical_truth_claim": False,
        "broker_specific_claim": False,
    }
    methodology_bytes = (
        canonical_contract_json(methodology_payload).encode("utf-8") + b"\n"
    )
    methodology_path = evidence_directory / "methodology.json"
    _atomic_write(methodology_path, methodology_bytes)
    methodology_id = _stable_id(
        "certification-methodology", methodology_payload
    )
    methodology_artifact = CertificationArtifactV1(
        policy_id=policy.policy_id,
        kind="methodology-report",
        subject_id=methodology_id,
        subject_schema_version=CERTIFICATION_METHODOLOGY_REPORT_SCHEMA_VERSION,
        content_sha256=hashlib.sha256(methodology_bytes).hexdigest(),
        relative_path="evidence/methodology.json",
        size_bytes=len(methodology_bytes),
        verified=True,
        metadata={
            "historical_truth_claim": False,
            "broker_specific_claim": False,
        },
    )
    artifacts.append(methodology_artifact)
    payloads[METHODOLOGY_REPORT_EVIDENCE_KEY] = methodology_payload
    evidence_by_key[METHODOLOGY_REPORT_EVIDENCE_KEY] = methodology_artifact

    observations = [
        _extract_observation(declared, payloads, evidence_by_key)
        for declared in spec.observations
    ]
    observations.append(
        CertificationObservationV1(
            check_id="methodology_and_limitations_published",
            actual=True,
            artifact_evidence_ids=(methodology_artifact.evidence_id,),
            note="published atomically by the certification campaign",
        )
    )
    observations.append(
        CertificationObservationV1(
            check_id="machine_evidence_manifest_published",
            actual=True,
            artifact_evidence_ids=(campaign_artifact.evidence_id,),
            note="published atomically by the certification campaign",
        )
    )
    dossier = evaluate_modern_reference_reconstruction_certification(
        policy,
        artifacts=artifacts,
        observations=observations,
        methodology=spec.methodology,
        accepted_limitations=spec.accepted_limitations,
        blocking_limitations=spec.blocking_limitations,
    )
    json_ref, markdown_ref = (
        write_modern_reference_reconstruction_certification_dossier(
            dossier,
            json_path=output / "certification.json",
            markdown_path=output / "certification.md",
        )
    )
    result = ModernReferenceCertificationCampaignResultV1(
        campaign_id=spec.campaign_id,
        policy_id=policy.policy_id,
        dossier_id=dossier.dossier_id,
        state=dossier.state,
        dossier_json=json_ref,
        dossier_markdown=markdown_ref,
        verified_input_count=len(spec.artifacts),
        observation_count=len(observations),
    )
    _atomic_write(
        output / "campaign-result.json",
        canonical_contract_json(result.to_dict()).encode("utf-8") + b"\n",
    )
    return dossier, result


def _verified_json_artifact(
    path: Path, declared: CertificationCampaignArtifactV1
) -> tuple[Mapping[str, Any], bytes]:
    try:
        encoded = path.read_bytes()
    except OSError as error:
        raise ValueError(
            f"cannot read certification artifact {path}: {error}"
        ) from error
    digest = hashlib.sha256(encoded).hexdigest()
    if digest != declared.content_sha256:
        raise ValueError(f"certification artifact hash differs: {path}")
    payload = _json_bytes_mapping(encoded, path)
    if payload.get("schema_version") != declared.subject_schema_version:
        raise ValueError(f"certification artifact schema differs: {path}")
    subject = _resolve_json_pointer(payload, declared.subject_id_pointer)
    if subject != declared.subject_id:
        raise ValueError(
            f"certification artifact subject identity differs: {path}"
        )
    return payload, encoded


def _extract_observation(
    declared: CertificationCampaignObservationV1,
    payloads: Mapping[str, Mapping[str, Any]],
    evidence_by_key: Mapping[str, CertificationArtifactV1],
) -> CertificationObservationV1:
    value = _resolve_json_pointer(
        payloads[declared.measurement_evidence_key],
        declared.measurement_pointer,
    )
    actual = _json_scalar(value, declared.check_id)
    return CertificationObservationV1(
        check_id=declared.check_id,
        actual=actual,
        artifact_evidence_ids=tuple(
            evidence_by_key[key].evidence_id
            for key in declared.artifact_evidence_keys
        ),
        note=declared.note,
    )


def _resolve_json_pointer(value: Any, pointer: str) -> Any:
    selected = value
    if pointer == "":
        return selected
    for raw_token in pointer[1:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(selected, Mapping):
            if token not in selected:
                raise ValueError(f"JSON pointer does not exist: {pointer}")
            selected = selected[token]
        elif isinstance(selected, Sequence) and not isinstance(
            selected, (str, bytes, bytearray)
        ):
            try:
                selected = selected[int(token)]
            except (ValueError, IndexError) as error:
                raise ValueError(
                    f"JSON pointer does not exist: {pointer}"
                ) from error
        else:
            raise ValueError(f"JSON pointer does not exist: {pointer}")
    return selected


def _read_json_mapping(path: Path) -> Mapping[str, Any]:
    try:
        return _json_bytes_mapping(path.read_bytes(), path)
    except OSError as error:
        raise ValueError(
            f"cannot read certification campaign {path}: {error}"
        ) from error


def _json_bytes_mapping(encoded: bytes, path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"invalid JSON certification artifact: {path}"
        ) from error
    return _mapping(value, str(path))


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.tmp-{hashlib.sha256(payload).hexdigest()[:12]}"
    )
    try:
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _json_pointer(value: Any) -> str:
    pointer = str(value)
    if pointer and not pointer.startswith("/"):
        raise ValueError("JSON pointer must be empty or start with a slash")
    return pointer


def _safe_relative_path(value: Any) -> str:
    path = PurePosixPath(_required_text(value))
    if path.is_absolute() or ".." in path.parts or path == PurePosixPath("."):
        raise ValueError("campaign relative path must be relative and safe")
    return str(path)


def _sha256(value: Any) -> str:
    text = _required_text(value).lower()
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError("campaign content hash must be SHA-256")
    return text


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("campaign text is required")
    if len(text) > DEFAULT_CERTIFICATION_MAX_TEXT_LENGTH:
        raise ValueError("campaign text exceeds limit")
    return text


def _bounded_text(value: Any, *, allow_empty: bool = False) -> str:
    text = str(value or "").strip()
    if not text and not allow_empty:
        raise ValueError("campaign text is required")
    if len(text) > DEFAULT_CERTIFICATION_MAX_TEXT_LENGTH:
        raise ValueError("campaign text exceeds limit")
    return text


def _bounded_text_tuple(values: Sequence[str]) -> tuple[str, ...]:
    selected = tuple(sorted({_bounded_text(value) for value in values}))
    if len(selected) > DEFAULT_CERTIFICATION_MAX_METADATA_ITEMS:
        raise ValueError("campaign limitation count exceeds limit")
    return selected


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float):
            _number(value, name)
        return value
    raise ValueError(f"{name} measurement must be a JSON scalar")


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    selected = float(value)
    if selected != selected or selected in {float("inf"), float("-inf")}:
        raise ValueError(f"{name} must be finite")
    return selected


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    return cast(Mapping[str, Any], value)


def _mapping_sequence(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError(f"{name} must be a sequence")
    return tuple(_mapping(item, name) for item in value)


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError(f"{name} must be a sequence")
    return tuple(str(item) for item in value)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _require_schema_value(actual: str, expected: str, name: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {name} schema version")


__all__ = [
    "CERTIFICATION_CAMPAIGN_ARTIFACT_SCHEMA_VERSION",
    "CERTIFICATION_CAMPAIGN_OBSERVATION_SCHEMA_VERSION",
    "CERTIFICATION_CAMPAIGN_RESULT_SCHEMA_VERSION",
    "CERTIFICATION_CAMPAIGN_SPEC_SCHEMA_VERSION",
    "CERTIFICATION_METHODOLOGY_REPORT_SCHEMA_VERSION",
    "CAMPAIGN_MANIFEST_EVIDENCE_KEY",
    "METHODOLOGY_REPORT_EVIDENCE_KEY",
    "CertificationCampaignArtifactV1",
    "CertificationCampaignObservationV1",
    "ModernReferenceCertificationCampaignResultV1",
    "ModernReferenceCertificationCampaignSpecV1",
    "read_modern_reference_certification_campaign_spec",
    "run_modern_reference_certification_campaign",
]
