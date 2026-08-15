"""Fail-closed release certification for reconstructed market products.

The certification layer binds compact report artifacts from the reconstruction
pipeline to predeclared requirements.  It never retains event rows, analytical
frames, model objects, or other tick-sized intermediates.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONScalar, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

CERTIFICATION_ARTIFACT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-artifact.v1"
)
CERTIFICATION_REQUIREMENT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-requirement.v1"
)
CERTIFICATION_OBSERVATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-observation.v1"
)
CERTIFICATION_CHECK_RESULT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-check-result.v1"
)
CERTIFICATION_GATE_RESULT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-gate-result.v1"
)
RECONSTRUCTION_CERTIFICATION_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-policy.v1"
)
RECONSTRUCTION_CERTIFICATION_DOSSIER_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-dossier.v1"
)
RECONSTRUCTION_CERTIFICATION_POLICY_V2_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-policy.v2"
)
RECONSTRUCTION_CERTIFICATION_DOSSIER_V2_SCHEMA_VERSION = (
    "histdatacom.reconstruction-certification-dossier.v2"
)

EURUSD_TRIANGLE_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
EURUSD_TRIANGLE_COMMON_START_PERIOD = "200203"
MODERN_REFERENCE_DELIVERY_MODE = "modern_reference"
MODERN_REFERENCE_DELIVERY_CLAIM = "unconditioned_reference"
PROMOTION_ONLY_CHECK_IDS = frozenset({"coverage_promotion_run_count"})

DEFAULT_CERTIFICATION_MAX_ARTIFACTS = 256
DEFAULT_CERTIFICATION_MAX_REQUIREMENTS = 128
DEFAULT_CERTIFICATION_MAX_OBSERVATIONS = 128
DEFAULT_CERTIFICATION_MAX_PAYLOAD_BYTES = 8_388_608
DEFAULT_CERTIFICATION_MAX_METADATA_ITEMS = 64
DEFAULT_CERTIFICATION_MAX_TEXT_LENGTH = 16_384


class CertificationGate(str, Enum):
    """The fifteen release gates declared by GitHub issue #449."""

    IDENTITY_AND_ANCHORS = "identity-and-anchors"
    INFORMATION_SAFETY = "information-safety"
    REVERSE_DEGRADATION = "reverse-degradation"
    CONDITIONED_SCORECARDS = "conditioned-scorecards"
    CROSS_CURRENCY = "cross-currency"
    ENSEMBLE_EVIDENCE = "ensemble-evidence"
    PRODUCT_RECONCILIATION = "product-reconciliation"
    FAILURE_RESUME = "failure-resume"
    REPLAY = "replay"
    RESOURCES = "resources"
    NEGATIVE_TESTS = "negative-tests"
    STRATEGY_SENSITIVITY = "strategy-sensitivity"
    DOSSIER_PUBLICATION = "dossier-publication"
    REPOSITORY_GATES = "repository-gates"
    TESTPYPI_PREFLIGHT = "testpypi-preflight"


class CertificationComparator(str, Enum):
    """Deterministic comparison applied to one measured observation."""

    EQUAL = "equal"
    LESS_OR_EQUAL = "less-or-equal"
    GREATER_OR_EQUAL = "greater-or-equal"
    TRUE = "true"
    FALSE = "false"
    ZERO = "zero"


class CertificationCheckStatus(str, Enum):
    """Outcome of one predeclared certification check."""

    PASSED = "passed"
    FAILED = "failed"
    MISSING = "missing"


class CertificationState(str, Enum):
    """Overall certification state without implicit promotion."""

    CERTIFIED = "certified"
    READY_FOR_PROMOTION = "ready-for-promotion"
    FAILED = "failed"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True, slots=True)
class CertificationArtifactV1:
    """Compact content identity for one independently produced artifact."""

    policy_id: str
    kind: str
    subject_id: str
    subject_schema_version: str
    content_sha256: str
    relative_path: str
    size_bytes: int
    verified: bool
    metadata: Mapping[str, JSONValue]
    evidence_id: str = ""
    schema_version: str = CERTIFICATION_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CERTIFICATION_ARTIFACT_SCHEMA_VERSION,
            "certification artifact",
        )
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        for name in ("kind", "subject_id", "subject_schema_version"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "content_sha256", _required_sha256(self.content_sha256)
        )
        object.__setattr__(
            self, "relative_path", _safe_relative_path(self.relative_path)
        )
        object.__setattr__(
            self,
            "size_bytes",
            _nonnegative_int(self.size_bytes, "size_bytes"),
        )
        object.__setattr__(
            self, "verified", _strict_bool(self.verified, "verified")
        )
        object.__setattr__(
            self,
            "metadata",
            _bounded_mapping(
                self.metadata,
                "certification artifact metadata",
                DEFAULT_CERTIFICATION_MAX_METADATA_ITEMS,
            ),
        )
        expected = _stable_id("certification-artifact", self.identity_payload())
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError("certification artifact evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    @classmethod
    def from_payload(
        cls,
        *,
        policy_id: str,
        kind: str,
        subject_id: str,
        subject_schema_version: str,
        payload: Mapping[str, JSONValue],
        relative_path: str,
        verified: bool = True,
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> "CertificationArtifactV1":
        """Bind one compact JSON contract without retaining its source rows."""
        content = canonical_contract_json(dict(payload)).encode("utf-8")
        return cls(
            policy_id=policy_id,
            kind=kind,
            subject_id=subject_id,
            subject_schema_version=subject_schema_version,
            content_sha256=hashlib.sha256(content).hexdigest(),
            relative_path=relative_path,
            size_bytes=len(content),
            verified=verified,
            metadata=metadata or {},
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return content-addressed artifact evidence."""
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "kind": self.kind,
            "subject_id": self.subject_id,
            "subject_schema_version": self.subject_schema_version,
            "content_sha256": self.content_sha256,
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "verified": self.verified,
            "metadata": dict(self.metadata),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible evidence."""
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CertificationArtifactV1":
        """Restore and verify artifact evidence."""
        _require_schema(data, CERTIFICATION_ARTIFACT_SCHEMA_VERSION)
        return cls(
            policy_id=str(data.get("policy_id", "")),
            kind=str(data.get("kind", "")),
            subject_id=str(data.get("subject_id", "")),
            subject_schema_version=str(data.get("subject_schema_version", "")),
            content_sha256=str(data.get("content_sha256", "")),
            relative_path=str(data.get("relative_path", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "size_bytes"),
            verified=_strict_bool(data.get("verified"), "verified"),
            metadata=_mapping(data.get("metadata"), "metadata"),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CertificationRequirementV1:
    """One predeclared gate comparison and its required artifact kinds."""

    gate: CertificationGate
    check_id: str
    comparator: CertificationComparator
    expected: JSONScalar
    required_artifact_kinds: tuple[str, ...]
    description: str
    requirement_id: str = ""
    schema_version: str = CERTIFICATION_REQUIREMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CERTIFICATION_REQUIREMENT_SCHEMA_VERSION,
            "certification requirement",
        )
        object.__setattr__(self, "gate", CertificationGate(self.gate))
        object.__setattr__(self, "check_id", _required_name(self.check_id))
        comparator = CertificationComparator(self.comparator)
        object.__setattr__(self, "comparator", comparator)
        _validate_expected(comparator, self.expected)
        kinds = _normalized_text_tuple(self.required_artifact_kinds)
        if not kinds:
            raise ValueError(
                "certification requirement requires artifact kinds"
            )
        object.__setattr__(self, "required_artifact_kinds", kinds)
        object.__setattr__(self, "description", _bounded_text(self.description))
        expected_id = _stable_id(
            "certification-requirement", self.identity_payload()
        )
        supplied = _optional_text(self.requirement_id)
        if supplied is not None and supplied != expected_id:
            raise ValueError("certification requirement_id differs")
        object.__setattr__(self, "requirement_id", expected_id)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic policy content."""
        return {
            "schema_version": self.schema_version,
            "gate": self.gate.value,
            "check_id": self.check_id,
            "comparator": self.comparator.value,
            "expected": self.expected,
            "required_artifact_kinds": list(self.required_artifact_kinds),
            "description": self.description,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy content."""
        return {
            **self.identity_payload(),
            "requirement_id": self.requirement_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CertificationRequirementV1":
        """Restore one predeclared requirement."""
        _require_schema(data, CERTIFICATION_REQUIREMENT_SCHEMA_VERSION)
        return cls(
            gate=CertificationGate(str(data.get("gate", ""))),
            check_id=str(data.get("check_id", "")),
            comparator=CertificationComparator(str(data.get("comparator", ""))),
            expected=_json_scalar(data.get("expected"), "expected"),
            required_artifact_kinds=_string_tuple(
                data.get("required_artifact_kinds"), "required_artifact_kinds"
            ),
            description=str(data.get("description", "")),
            requirement_id=str(data.get("requirement_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CertificationObservationV1:
    """One measured value bound to independently verified artifacts."""

    check_id: str
    actual: JSONScalar
    artifact_evidence_ids: tuple[str, ...]
    note: str = ""
    observation_id: str = ""
    schema_version: str = CERTIFICATION_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CERTIFICATION_OBSERVATION_SCHEMA_VERSION,
            "certification observation",
        )
        object.__setattr__(self, "check_id", _required_name(self.check_id))
        _json_scalar(self.actual, "actual")
        evidence = _normalized_text_tuple(self.artifact_evidence_ids)
        if not evidence:
            raise ValueError(
                "certification observation requires artifact evidence"
            )
        object.__setattr__(self, "artifact_evidence_ids", evidence)
        object.__setattr__(
            self, "note", _bounded_text(self.note, allow_empty=True)
        )
        expected = _stable_id(
            "certification-observation", self.identity_payload()
        )
        supplied = _optional_text(self.observation_id)
        if supplied is not None and supplied != expected:
            raise ValueError("certification observation_id differs")
        object.__setattr__(self, "observation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic observation content."""
        return {
            "schema_version": self.schema_version,
            "check_id": self.check_id,
            "actual": self.actual,
            "artifact_evidence_ids": list(self.artifact_evidence_ids),
            "note": self.note,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible observation content."""
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CertificationObservationV1":
        """Restore one content-bound observation."""
        _require_schema(data, CERTIFICATION_OBSERVATION_SCHEMA_VERSION)
        return cls(
            check_id=str(data.get("check_id", "")),
            actual=_json_scalar(data.get("actual"), "actual"),
            artifact_evidence_ids=_string_tuple(
                data.get("artifact_evidence_ids"), "artifact_evidence_ids"
            ),
            note=str(data.get("note", "")),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CertificationCheckResultV1:
    """Computed outcome for one policy requirement."""

    requirement_id: str
    check_id: str
    status: CertificationCheckStatus
    comparator: CertificationComparator
    expected: JSONScalar
    actual: JSONScalar
    artifact_evidence_ids: tuple[str, ...]
    reason: str
    result_id: str = ""
    schema_version: str = CERTIFICATION_CHECK_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CERTIFICATION_CHECK_RESULT_SCHEMA_VERSION,
            "certification check result",
        )
        object.__setattr__(
            self, "requirement_id", _required_text(self.requirement_id)
        )
        object.__setattr__(self, "check_id", _required_name(self.check_id))
        object.__setattr__(
            self, "status", CertificationCheckStatus(self.status)
        )
        object.__setattr__(
            self, "comparator", CertificationComparator(self.comparator)
        )
        _json_scalar(self.expected, "expected")
        _json_scalar(self.actual, "actual")
        object.__setattr__(
            self,
            "artifact_evidence_ids",
            _normalized_text_tuple(self.artifact_evidence_ids),
        )
        object.__setattr__(self, "reason", _bounded_text(self.reason))
        expected_id = _stable_id(
            "certification-check-result", self.identity_payload()
        )
        supplied = _optional_text(self.result_id)
        if supplied is not None and supplied != expected_id:
            raise ValueError("certification check result_id differs")
        object.__setattr__(self, "result_id", expected_id)

    @property
    def passed(self) -> bool:
        """Return whether the measured result satisfied its policy."""
        return self.status is CertificationCheckStatus.PASSED

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic result content."""
        return {
            "schema_version": self.schema_version,
            "requirement_id": self.requirement_id,
            "check_id": self.check_id,
            "status": self.status.value,
            "comparator": self.comparator.value,
            "expected": self.expected,
            "actual": self.actual,
            "artifact_evidence_ids": list(self.artifact_evidence_ids),
            "reason": self.reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible result content."""
        return {**self.identity_payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CertificationCheckResultV1":
        """Restore one computed check result."""
        _require_schema(data, CERTIFICATION_CHECK_RESULT_SCHEMA_VERSION)
        return cls(
            requirement_id=str(data.get("requirement_id", "")),
            check_id=str(data.get("check_id", "")),
            status=CertificationCheckStatus(str(data.get("status", ""))),
            comparator=CertificationComparator(str(data.get("comparator", ""))),
            expected=_json_scalar(data.get("expected"), "expected"),
            actual=_json_scalar(data.get("actual"), "actual"),
            artifact_evidence_ids=_string_tuple(
                data.get("artifact_evidence_ids"), "artifact_evidence_ids"
            ),
            reason=str(data.get("reason", "")),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CertificationGateResultV1:
    """All computed checks for one issue acceptance gate."""

    gate: CertificationGate
    check_results: tuple[CertificationCheckResultV1, ...]
    gate_result_id: str = ""
    schema_version: str = CERTIFICATION_GATE_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CERTIFICATION_GATE_RESULT_SCHEMA_VERSION,
            "certification gate result",
        )
        object.__setattr__(self, "gate", CertificationGate(self.gate))
        results = tuple(
            sorted(self.check_results, key=lambda item: item.check_id)
        )
        if not results:
            raise ValueError("certification gate result requires checks")
        if len({item.check_id for item in results}) != len(results):
            raise ValueError("certification gate result duplicates checks")
        object.__setattr__(self, "check_results", results)
        expected = _stable_id(
            "certification-gate-result", self.identity_payload()
        )
        supplied = _optional_text(self.gate_result_id)
        if supplied is not None and supplied != expected:
            raise ValueError("certification gate_result_id differs")
        object.__setattr__(self, "gate_result_id", expected)

    @property
    def status(self) -> CertificationCheckStatus:
        """Return the fail-closed aggregate status for the gate."""
        statuses = {item.status for item in self.check_results}
        if CertificationCheckStatus.FAILED in statuses:
            return CertificationCheckStatus.FAILED
        if CertificationCheckStatus.MISSING in statuses:
            return CertificationCheckStatus.MISSING
        return CertificationCheckStatus.PASSED

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic gate evidence."""
        return {
            "schema_version": self.schema_version,
            "gate": self.gate.value,
            "status": self.status.value,
            "check_results": [item.to_dict() for item in self.check_results],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible gate evidence."""
        return {
            **self.identity_payload(),
            "gate_result_id": self.gate_result_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CertificationGateResultV1":
        """Restore one gate result and verify its derived status."""
        _require_schema(data, CERTIFICATION_GATE_RESULT_SCHEMA_VERSION)
        result = cls(
            gate=CertificationGate(str(data.get("gate", ""))),
            check_results=tuple(
                CertificationCheckResultV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("check_results"), "check_results"
                )
            ),
            gate_result_id=str(data.get("gate_result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("status") != result.status.value:
            raise ValueError("certification gate status differs")
        return result


@dataclass(frozen=True, slots=True)
class ReconstructionCertificationPolicyV1:
    """Predeclared product scope, thresholds, and release requirements."""

    product_version: str
    symbols: tuple[str, ...]
    common_start_period: str
    common_end_period: str
    broker_fingerprint_id: str
    requirements: tuple[CertificationRequirementV1, ...]
    max_artifacts: int = DEFAULT_CERTIFICATION_MAX_ARTIFACTS
    max_observations: int = DEFAULT_CERTIFICATION_MAX_OBSERVATIONS
    max_payload_bytes: int = DEFAULT_CERTIFICATION_MAX_PAYLOAD_BYTES
    policy_id: str = ""
    schema_version: str = RECONSTRUCTION_CERTIFICATION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_CERTIFICATION_POLICY_SCHEMA_VERSION,
            "reconstruction certification policy",
        )
        object.__setattr__(
            self, "product_version", _required_text(self.product_version)
        )
        symbols = tuple(
            sorted({_normalized_symbol(item) for item in self.symbols})
        )
        if symbols != tuple(sorted(EURUSD_TRIANGLE_SYMBOLS)):
            raise ValueError("v1 certification scope is the EURUSD triangle")
        object.__setattr__(self, "symbols", symbols)
        start = _required_period(self.common_start_period)
        end = _required_period(self.common_end_period)
        if start != EURUSD_TRIANGLE_COMMON_START_PERIOD or end < start:
            raise ValueError("certification common coverage differs")
        object.__setattr__(self, "common_start_period", start)
        object.__setattr__(self, "common_end_period", end)
        object.__setattr__(
            self,
            "broker_fingerprint_id",
            _required_text(self.broker_fingerprint_id),
        )
        requirements = tuple(
            sorted(self.requirements, key=lambda item: item.check_id)
        )
        if (
            not requirements
            or len(requirements) > DEFAULT_CERTIFICATION_MAX_REQUIREMENTS
        ):
            raise ValueError(
                "certification requirements are empty or unbounded"
            )
        if len({item.check_id for item in requirements}) != len(requirements):
            raise ValueError("certification policy duplicates check IDs")
        if {item.gate for item in requirements} != set(CertificationGate):
            raise ValueError("certification policy must cover all issue gates")
        object.__setattr__(self, "requirements", requirements)
        for name, maximum in (
            ("max_artifacts", DEFAULT_CERTIFICATION_MAX_ARTIFACTS),
            ("max_observations", DEFAULT_CERTIFICATION_MAX_OBSERVATIONS),
            ("max_payload_bytes", DEFAULT_CERTIFICATION_MAX_PAYLOAD_BYTES),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 1, maximum),
            )
        expected = _stable_id(
            "reconstruction-certification-policy", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reconstruction certification policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic predeclared certification semantics."""
        return {
            "schema_version": self.schema_version,
            "product_version": self.product_version,
            "symbols": list(self.symbols),
            "common_start_period": self.common_start_period,
            "common_end_period": self.common_end_period,
            "broker_fingerprint_id": self.broker_fingerprint_id,
            "requirements": [item.to_dict() for item in self.requirements],
            "max_artifacts": self.max_artifacts,
            "max_observations": self.max_observations,
            "max_payload_bytes": self.max_payload_bytes,
            "coverage_policy": "common-supported-period-only",
            "promotion_policy": "coverage-once-at-dev-to-main-boundary",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy content."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionCertificationPolicyV1":
        """Restore and verify a certification policy."""
        _require_schema(
            data, RECONSTRUCTION_CERTIFICATION_POLICY_SCHEMA_VERSION
        )
        for name, expected in (
            ("coverage_policy", "common-supported-period-only"),
            ("promotion_policy", "coverage-once-at-dev-to-main-boundary"),
        ):
            _require_derived(data, name, expected)
        return cls(
            product_version=str(data.get("product_version", "")),
            symbols=_string_tuple(data.get("symbols"), "symbols"),
            common_start_period=str(data.get("common_start_period", "")),
            common_end_period=str(data.get("common_end_period", "")),
            broker_fingerprint_id=str(data.get("broker_fingerprint_id", "")),
            requirements=tuple(
                CertificationRequirementV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("requirements"), "requirements"
                )
            ),
            max_artifacts=_strict_int(
                data.get("max_artifacts"), "max_artifacts"
            ),
            max_observations=_strict_int(
                data.get("max_observations"), "max_observations"
            ),
            max_payload_bytes=_strict_int(
                data.get("max_payload_bytes"), "max_payload_bytes"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionCertificationPolicyV2:
    """Versioned modern-reference scope without broker-specific claims.

    Version one remains replayable with its mandatory broker fingerprint.  This
    version is intentionally a separate contract because removing that field or
    changing required evidence in-place would invalidate the meaning of already
    published V1 policy identities.  The product version remains part of every
    V2 identity, so embedded policies from earlier releases remain replayable
    while the factory can predeclare the current release train.
    """

    product_version: str
    symbols: tuple[str, ...]
    common_start_period: str
    common_end_period: str
    delivery_mode: str
    delivery_claim: str
    requirements: tuple[CertificationRequirementV1, ...]
    max_artifacts: int = DEFAULT_CERTIFICATION_MAX_ARTIFACTS
    max_observations: int = DEFAULT_CERTIFICATION_MAX_OBSERVATIONS
    max_payload_bytes: int = DEFAULT_CERTIFICATION_MAX_PAYLOAD_BYTES
    policy_id: str = ""
    schema_version: str = RECONSTRUCTION_CERTIFICATION_POLICY_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_CERTIFICATION_POLICY_V2_SCHEMA_VERSION,
            "reconstruction certification policy v2",
        )
        object.__setattr__(
            self, "product_version", _required_text(self.product_version)
        )
        symbols = tuple(
            sorted({_normalized_symbol(item) for item in self.symbols})
        )
        if symbols != tuple(sorted(EURUSD_TRIANGLE_SYMBOLS)):
            raise ValueError("v2 certification scope is the EURUSD triangle")
        object.__setattr__(self, "symbols", symbols)
        start = _required_period(self.common_start_period)
        end = _required_period(self.common_end_period)
        if start != EURUSD_TRIANGLE_COMMON_START_PERIOD or end < start:
            raise ValueError("certification common coverage differs")
        object.__setattr__(self, "common_start_period", start)
        object.__setattr__(self, "common_end_period", end)
        if self.delivery_mode != MODERN_REFERENCE_DELIVERY_MODE:
            raise ValueError(
                "v2 certification requires modern-reference delivery"
            )
        if self.delivery_claim != MODERN_REFERENCE_DELIVERY_CLAIM:
            raise ValueError("v2 certification requires an unconditioned claim")
        requirements = tuple(
            sorted(self.requirements, key=lambda item: item.check_id)
        )
        if (
            not requirements
            or len(requirements) > DEFAULT_CERTIFICATION_MAX_REQUIREMENTS
        ):
            raise ValueError(
                "certification requirements are empty or unbounded"
            )
        if len({item.check_id for item in requirements}) != len(requirements):
            raise ValueError("certification policy duplicates check IDs")
        if {item.gate for item in requirements} != set(CertificationGate):
            raise ValueError("certification policy must cover all issue gates")
        forbidden_kinds = {
            kind
            for requirement in requirements
            for kind in requirement.required_artifact_kinds
            if "broker" in kind
        }
        forbidden_checks = {
            requirement.check_id
            for requirement in requirements
            if "broker" in requirement.check_id
        }
        if forbidden_kinds or forbidden_checks:
            raise ValueError(
                "modern-reference certification cannot require broker evidence"
            )
        object.__setattr__(self, "requirements", requirements)
        for name, maximum in (
            ("max_artifacts", DEFAULT_CERTIFICATION_MAX_ARTIFACTS),
            ("max_observations", DEFAULT_CERTIFICATION_MAX_OBSERVATIONS),
            ("max_payload_bytes", DEFAULT_CERTIFICATION_MAX_PAYLOAD_BYTES),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 1, maximum),
            )
        expected = _stable_id(
            "reconstruction-certification-policy-v2", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reconstruction certification policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic modern-reference certification semantics."""
        return {
            "schema_version": self.schema_version,
            "product_version": self.product_version,
            "symbols": list(self.symbols),
            "common_start_period": self.common_start_period,
            "common_end_period": self.common_end_period,
            "delivery_mode": self.delivery_mode,
            "delivery_claim": self.delivery_claim,
            "requirements": [item.to_dict() for item in self.requirements],
            "max_artifacts": self.max_artifacts,
            "max_observations": self.max_observations,
            "max_payload_bytes": self.max_payload_bytes,
            "coverage_policy": "common-supported-period-only",
            "promotion_policy": "coverage-once-at-dev-to-main-boundary",
            "broker_adaptation": (
                f"excluded-from-v{self.product_version}-certification"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy content."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionCertificationPolicyV2":
        """Restore and verify a modern-reference certification policy."""
        _require_schema(
            data, RECONSTRUCTION_CERTIFICATION_POLICY_V2_SCHEMA_VERSION
        )
        for name, expected in (
            ("coverage_policy", "common-supported-period-only"),
            ("promotion_policy", "coverage-once-at-dev-to-main-boundary"),
            (
                "broker_adaptation",
                "excluded-from-v"
                f"{str(data.get('product_version', ''))}-certification",
            ),
        ):
            _require_derived(data, name, expected)
        return cls(
            product_version=str(data.get("product_version", "")),
            symbols=_string_tuple(data.get("symbols"), "symbols"),
            common_start_period=str(data.get("common_start_period", "")),
            common_end_period=str(data.get("common_end_period", "")),
            delivery_mode=str(data.get("delivery_mode", "")),
            delivery_claim=str(data.get("delivery_claim", "")),
            requirements=tuple(
                CertificationRequirementV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("requirements"), "requirements"
                )
            ),
            max_artifacts=_strict_int(
                data.get("max_artifacts"), "max_artifacts"
            ),
            max_observations=_strict_int(
                data.get("max_observations"), "max_observations"
            ),
            max_payload_bytes=_strict_int(
                data.get("max_payload_bytes"), "max_payload_bytes"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionCertificationDossierV1:
    """Bounded scientific and operational acceptance dossier."""

    policy: ReconstructionCertificationPolicyV1
    artifacts: tuple[CertificationArtifactV1, ...]
    gate_results: tuple[CertificationGateResultV1, ...]
    methodology: str
    accepted_limitations: tuple[str, ...]
    blocking_limitations: tuple[str, ...]
    state: CertificationState
    dossier_id: str = ""
    schema_version: str = RECONSTRUCTION_CERTIFICATION_DOSSIER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_CERTIFICATION_DOSSIER_SCHEMA_VERSION,
            "reconstruction certification dossier",
        )
        if not isinstance(self.policy, ReconstructionCertificationPolicyV1):
            raise TypeError("certification dossier requires a v1 policy")
        artifacts = tuple(
            sorted(self.artifacts, key=lambda item: item.evidence_id)
        )
        if not artifacts or len(artifacts) > self.policy.max_artifacts:
            raise ValueError(
                "certification dossier artifacts are empty or unbounded"
            )
        if len({item.evidence_id for item in artifacts}) != len(artifacts):
            raise ValueError("certification dossier duplicates artifacts")
        object.__setattr__(self, "artifacts", artifacts)
        results = tuple(
            sorted(self.gate_results, key=lambda item: item.gate.value)
        )
        if {item.gate for item in results} != set(CertificationGate):
            raise ValueError(
                "certification dossier gate coverage is incomplete"
            )
        requirement_ids = {
            item.requirement_id for item in self.policy.requirements
        }
        result_ids = {
            result.requirement_id
            for gate in results
            for result in gate.check_results
        }
        if result_ids != requirement_ids:
            raise ValueError(
                "certification dossier check coverage differs from policy"
            )
        object.__setattr__(self, "gate_results", results)
        object.__setattr__(self, "methodology", _bounded_text(self.methodology))
        accepted = _normalized_bounded_text_tuple(
            self.accepted_limitations, "accepted limitation"
        )
        blocking = _normalized_bounded_text_tuple(
            self.blocking_limitations, "blocking limitation"
        )
        object.__setattr__(self, "accepted_limitations", accepted)
        object.__setattr__(self, "blocking_limitations", blocking)
        expected_state = _certification_state(results, blocking)
        supplied_state = CertificationState(self.state)
        if supplied_state is not expected_state:
            raise ValueError(
                "certification dossier state differs from evidence"
            )
        object.__setattr__(self, "state", expected_state)
        expected = _stable_id(
            "reconstruction-certification-dossier", self.identity_payload()
        )
        supplied = _optional_text(self.dossier_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reconstruction certification dossier_id differs")
        object.__setattr__(self, "dossier_id", expected)
        _ensure_payload_size(self.to_dict(), self.policy.max_payload_bytes)

    @property
    def certified(self) -> bool:
        """Return whether every scientific, operational, and release gate passed."""
        return self.state is CertificationState.CERTIFIED

    @property
    def ready_for_promotion(self) -> bool:
        """Return whether only the promotion-boundary coverage run remains."""
        return self.state is CertificationState.READY_FOR_PROMOTION

    @property
    def summary(self) -> dict[str, JSONValue]:
        """Return bounded gate and check counts."""
        checks = [
            item for gate in self.gate_results for item in gate.check_results
        ]
        return {
            "gate_count": len(self.gate_results),
            "passed_gate_count": sum(
                item.status is CertificationCheckStatus.PASSED
                for item in self.gate_results
            ),
            "failed_gate_count": sum(
                item.status is CertificationCheckStatus.FAILED
                for item in self.gate_results
            ),
            "missing_gate_count": sum(
                item.status is CertificationCheckStatus.MISSING
                for item in self.gate_results
            ),
            "check_count": len(checks),
            "passed_check_count": sum(item.passed for item in checks),
            "failed_check_count": sum(
                item.status is CertificationCheckStatus.FAILED
                for item in checks
            ),
            "missing_check_count": sum(
                item.status is CertificationCheckStatus.MISSING
                for item in checks
            ),
            "artifact_count": len(self.artifacts),
            "accepted_limitation_count": len(self.accepted_limitations),
            "blocking_limitation_count": len(self.blocking_limitations),
        }

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic machine-readable dossier."""
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.to_dict(),
            "artifacts": [item.to_dict() for item in self.artifacts],
            "gate_results": [item.to_dict() for item in self.gate_results],
            "methodology": self.methodology,
            "accepted_limitations": list(self.accepted_limitations),
            "blocking_limitations": list(self.blocking_limitations),
            "state": self.state.value,
            "summary": self.summary,
            "event_rows_inline": False,
            "analytical_frame_columns_inline": False,
            "automatic_winner": False,
            "historical_truth_claim": False,
            "investment_recommendation": False,
            "release_authorized": self.certified,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible dossier content."""
        return {**self.identity_payload(), "dossier_id": self.dossier_id}

    def to_json(self) -> str:
        """Serialize the dossier deterministically."""
        serialized: str = canonical_contract_json(self.to_dict())
        return serialized

    def to_markdown(self) -> str:
        """Render a deterministic human-readable methodology/limitations report."""
        lines = [
            "# EURUSD Triangle Reconstruction Certification",
            "",
            f"- State: **{self.state.value}**",
            f"- Dossier: `{self.dossier_id}`",
            f"- Policy: `{self.policy.policy_id}`",
            f"- Product version: `{self.policy.product_version}`",
            f"- Symbols: `{', '.join(self.policy.symbols)}`",
            (
                "- Common coverage: "
                f"`{self.policy.common_start_period}`–`{self.policy.common_end_period}`"
            ),
            f"- Broker fingerprint: `{self.policy.broker_fingerprint_id}`",
            "",
            "## Gate results",
            "",
            "| Gate | Status | Passed | Failed | Missing |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
        for gate in self.gate_results:
            lines.append(
                "| "
                f"{gate.gate.value} | {gate.status.value} | "
                f"{sum(item.status is CertificationCheckStatus.PASSED for item in gate.check_results)} | "
                f"{sum(item.status is CertificationCheckStatus.FAILED for item in gate.check_results)} | "
                f"{sum(item.status is CertificationCheckStatus.MISSING for item in gate.check_results)} |"
            )
        lines.extend(["", "## Methodology", "", self.methodology, ""])
        lines.extend(
            _markdown_limitations(
                "Accepted limitations", self.accepted_limitations
            )
        )
        lines.extend(
            _markdown_limitations(
                "Blocking limitations", self.blocking_limitations
            )
        )
        lines.extend(
            [
                "## Trust boundary",
                "",
                "This dossier is bounded derived metadata. It contains no tick rows, "
                "does not select an automatic winner, and does not claim historical truth "
                "or authorize an investment recommendation.",
                "",
            ]
        )
        return "\n".join(lines)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionCertificationDossierV1":
        """Restore and verify a complete dossier."""
        _require_schema(
            data, RECONSTRUCTION_CERTIFICATION_DOSSIER_SCHEMA_VERSION
        )
        for name, expected in (
            ("event_rows_inline", False),
            ("analytical_frame_columns_inline", False),
            ("automatic_winner", False),
            ("historical_truth_claim", False),
            ("investment_recommendation", False),
        ):
            _require_derived(data, name, expected)
        dossier = cls(
            policy=ReconstructionCertificationPolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            artifacts=tuple(
                CertificationArtifactV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("artifacts"), "artifacts"
                )
            ),
            gate_results=tuple(
                CertificationGateResultV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("gate_results"), "gate_results"
                )
            ),
            methodology=str(data.get("methodology", "")),
            accepted_limitations=_string_tuple(
                data.get("accepted_limitations"), "accepted_limitations"
            ),
            blocking_limitations=_string_tuple(
                data.get("blocking_limitations"), "blocking_limitations"
            ),
            state=CertificationState(str(data.get("state", ""))),
            dossier_id=str(data.get("dossier_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "summary", dossier.summary)
        _require_derived(data, "release_authorized", dossier.certified)
        return dossier

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionCertificationDossierV1":
        """Restore a dossier from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionCertificationDossierV2:
    """Bounded modern-reference scientific and operational dossier."""

    policy: ReconstructionCertificationPolicyV2
    artifacts: tuple[CertificationArtifactV1, ...]
    gate_results: tuple[CertificationGateResultV1, ...]
    methodology: str
    accepted_limitations: tuple[str, ...]
    blocking_limitations: tuple[str, ...]
    state: CertificationState
    dossier_id: str = ""
    schema_version: str = RECONSTRUCTION_CERTIFICATION_DOSSIER_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_CERTIFICATION_DOSSIER_V2_SCHEMA_VERSION,
            "reconstruction certification dossier v2",
        )
        if not isinstance(self.policy, ReconstructionCertificationPolicyV2):
            raise TypeError("certification dossier v2 requires a v2 policy")
        artifacts = tuple(
            sorted(self.artifacts, key=lambda item: item.evidence_id)
        )
        if not artifacts or len(artifacts) > self.policy.max_artifacts:
            raise ValueError(
                "certification dossier artifacts are empty or unbounded"
            )
        if len({item.evidence_id for item in artifacts}) != len(artifacts):
            raise ValueError("certification dossier duplicates artifacts")
        if any(item.policy_id != self.policy.policy_id for item in artifacts):
            raise ValueError(
                "certification dossier contains foreign policy evidence"
            )
        object.__setattr__(self, "artifacts", artifacts)
        results = tuple(
            sorted(self.gate_results, key=lambda item: item.gate.value)
        )
        if {item.gate for item in results} != set(CertificationGate):
            raise ValueError(
                "certification dossier gate coverage is incomplete"
            )
        requirement_ids = {
            item.requirement_id for item in self.policy.requirements
        }
        result_ids = {
            result.requirement_id
            for gate in results
            for result in gate.check_results
        }
        if result_ids != requirement_ids:
            raise ValueError(
                "certification dossier check coverage differs from policy"
            )
        object.__setattr__(self, "gate_results", results)
        object.__setattr__(self, "methodology", _bounded_text(self.methodology))
        accepted = _normalized_bounded_text_tuple(
            self.accepted_limitations, "accepted limitation"
        )
        blocking = _normalized_bounded_text_tuple(
            self.blocking_limitations, "blocking limitation"
        )
        object.__setattr__(self, "accepted_limitations", accepted)
        object.__setattr__(self, "blocking_limitations", blocking)
        expected_state = _certification_state(results, blocking)
        supplied_state = CertificationState(self.state)
        if supplied_state is not expected_state:
            raise ValueError(
                "certification dossier state differs from evidence"
            )
        object.__setattr__(self, "state", expected_state)
        expected = _stable_id(
            "reconstruction-certification-dossier-v2", self.identity_payload()
        )
        supplied = _optional_text(self.dossier_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reconstruction certification dossier_id differs")
        object.__setattr__(self, "dossier_id", expected)
        _ensure_payload_size(self.to_dict(), self.policy.max_payload_bytes)

    @property
    def certified(self) -> bool:
        """Return whether every scientific, operational, and release gate passed."""
        return self.state is CertificationState.CERTIFIED

    @property
    def ready_for_promotion(self) -> bool:
        """Return whether only the promotion-boundary coverage run remains."""
        return self.state is CertificationState.READY_FOR_PROMOTION

    @property
    def summary(self) -> dict[str, JSONValue]:
        """Return bounded gate and check counts."""
        checks = [
            item for gate in self.gate_results for item in gate.check_results
        ]
        return {
            "gate_count": len(self.gate_results),
            "passed_gate_count": sum(
                item.status is CertificationCheckStatus.PASSED
                for item in self.gate_results
            ),
            "failed_gate_count": sum(
                item.status is CertificationCheckStatus.FAILED
                for item in self.gate_results
            ),
            "missing_gate_count": sum(
                item.status is CertificationCheckStatus.MISSING
                for item in self.gate_results
            ),
            "check_count": len(checks),
            "passed_check_count": sum(item.passed for item in checks),
            "failed_check_count": sum(
                item.status is CertificationCheckStatus.FAILED
                for item in checks
            ),
            "missing_check_count": sum(
                item.status is CertificationCheckStatus.MISSING
                for item in checks
            ),
            "artifact_count": len(self.artifacts),
            "accepted_limitation_count": len(self.accepted_limitations),
            "blocking_limitation_count": len(self.blocking_limitations),
        }

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic machine-readable dossier."""
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.to_dict(),
            "artifacts": [item.to_dict() for item in self.artifacts],
            "gate_results": [item.to_dict() for item in self.gate_results],
            "methodology": self.methodology,
            "accepted_limitations": list(self.accepted_limitations),
            "blocking_limitations": list(self.blocking_limitations),
            "state": self.state.value,
            "summary": self.summary,
            "delivery_mode": self.policy.delivery_mode,
            "delivery_claim": self.policy.delivery_claim,
            "broker_specific_claim": False,
            "event_rows_inline": False,
            "analytical_frame_columns_inline": False,
            "automatic_winner": False,
            "historical_truth_claim": False,
            "investment_recommendation": False,
            "release_authorized": self.certified,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible dossier content."""
        return {**self.identity_payload(), "dossier_id": self.dossier_id}

    def to_json(self) -> str:
        """Serialize the dossier deterministically."""
        return str(canonical_contract_json(self.to_dict()))

    def to_markdown(self) -> str:
        """Render deterministic modern-reference methodology and limitations."""
        lines = [
            "# EURUSD Triangle Reconstruction Certification",
            "",
            f"- State: **{self.state.value}**",
            f"- Dossier: `{self.dossier_id}`",
            f"- Policy: `{self.policy.policy_id}`",
            f"- Product version: `{self.policy.product_version}`",
            f"- Symbols: `{', '.join(self.policy.symbols)}`",
            (
                "- Common coverage: "
                f"`{self.policy.common_start_period}`–`{self.policy.common_end_period}`"
            ),
            f"- Delivery mode: `{self.policy.delivery_mode}`",
            f"- Delivery claim: `{self.policy.delivery_claim}`",
            "- Broker-specific claim: `false`",
            "",
            "## Gate results",
            "",
            "| Gate | Status | Passed | Failed | Missing |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
        for gate in self.gate_results:
            lines.append(
                "| "
                f"{gate.gate.value} | {gate.status.value} | "
                f"{sum(item.status is CertificationCheckStatus.PASSED for item in gate.check_results)} | "
                f"{sum(item.status is CertificationCheckStatus.FAILED for item in gate.check_results)} | "
                f"{sum(item.status is CertificationCheckStatus.MISSING for item in gate.check_results)} |"
            )
        lines.extend(["", "## Methodology", "", self.methodology, ""])
        lines.extend(
            _markdown_limitations(
                "Accepted limitations", self.accepted_limitations
            )
        )
        lines.extend(
            _markdown_limitations(
                "Blocking limitations", self.blocking_limitations
            )
        )
        lines.extend(
            [
                "## Trust boundary",
                "",
                "This dossier certifies modern-reference, unconditioned output only. "
                "It makes no broker-specific claim, contains no tick rows, does not "
                "select an automatic winner, and does not claim historical truth or "
                "authorize an investment recommendation.",
                "",
            ]
        )
        return "\n".join(lines)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionCertificationDossierV2":
        """Restore and verify a complete modern-reference dossier."""
        _require_schema(
            data, RECONSTRUCTION_CERTIFICATION_DOSSIER_V2_SCHEMA_VERSION
        )
        for name, expected in (
            ("delivery_mode", MODERN_REFERENCE_DELIVERY_MODE),
            ("delivery_claim", MODERN_REFERENCE_DELIVERY_CLAIM),
            ("broker_specific_claim", False),
            ("event_rows_inline", False),
            ("analytical_frame_columns_inline", False),
            ("automatic_winner", False),
            ("historical_truth_claim", False),
            ("investment_recommendation", False),
        ):
            _require_derived(data, name, expected)
        dossier = cls(
            policy=ReconstructionCertificationPolicyV2.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            artifacts=tuple(
                CertificationArtifactV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("artifacts"), "artifacts"
                )
            ),
            gate_results=tuple(
                CertificationGateResultV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("gate_results"), "gate_results"
                )
            ),
            methodology=str(data.get("methodology", "")),
            accepted_limitations=_string_tuple(
                data.get("accepted_limitations"), "accepted_limitations"
            ),
            blocking_limitations=_string_tuple(
                data.get("blocking_limitations"), "blocking_limitations"
            ),
            state=CertificationState(str(data.get("state", ""))),
            dossier_id=str(data.get("dossier_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "summary", dossier.summary)
        _require_derived(data, "release_authorized", dossier.certified)
        return dossier

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionCertificationDossierV2":
        """Restore a modern-reference dossier from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


def eurusd_triangle_certification_policy(
    *,
    broker_fingerprint_id: str,
    common_end_period: str,
    peak_memory_budget_bytes: int,
    scratch_budget_bytes: int,
    runtime_budget_seconds: float,
    storage_budget_bytes: int,
) -> ReconstructionCertificationPolicyV1:
    """Return the complete predeclared v2.1.0 certification policy."""
    requirements = (
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "raw_source_hash_mismatch_count",
            CertificationComparator.ZERO,
            0,
            ("raw-source-inventory", "reconstruction-product-manifest"),
            "Raw source hashes reconcile with the committed product.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "immutable_anchor_mismatch_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-product-manifest",),
            "Immutable observed anchors reconcile exactly.",
        ),
        _requirement(
            CertificationGate.INFORMATION_SAFETY,
            "information_audit_violation_count",
            CertificationComparator.ZERO,
            0,
            ("information-audit-report",),
            "Every claimed use has zero information-leakage violations.",
        ),
        _requirement(
            CertificationGate.INFORMATION_SAFETY,
            "claimed_use_missing_audit_count",
            CertificationComparator.ZERO,
            0,
            ("information-audit-report",),
            "No claimed use lacks a point-in-time audit.",
        ),
        _requirement(
            CertificationGate.REVERSE_DEGRADATION,
            "reverse_thresholds_predeclared",
            CertificationComparator.TRUE,
            True,
            ("reverse-degradation-scorecard",),
            "Reverse-degradation thresholds were fixed before final holdout use.",
        ),
        _requirement(
            CertificationGate.REVERSE_DEGRADATION,
            "reverse_holdout_failure_count",
            CertificationComparator.ZERO,
            0,
            ("reverse-degradation-scorecard",),
            "Untouched final holdouts pass the predeclared thresholds.",
        ),
        _requirement(
            CertificationGate.CONDITIONED_SCORECARDS,
            "conditioned_tolerance_violation_count",
            CertificationComparator.ZERO,
            0,
            ("reverse-degradation-scorecard", "broker-conditioned-scorecard"),
            "Session, event, epoch, cadence, spread, timing, and path tolerances pass.",
        ),
        _requirement(
            CertificationGate.CONDITIONED_SCORECARDS,
            "required_stratum_missing_count",
            CertificationComparator.ZERO,
            0,
            ("reverse-degradation-scorecard",),
            "Every required scientific stratum has measured support.",
        ),
        _requirement(
            CertificationGate.CROSS_CURRENCY,
            "post_broker_cross_currency_failure_count",
            CertificationComparator.ZERO,
            0,
            ("cross-currency-validation-report", "broker-delivery-fingerprint"),
            "Triangle, inverse, and stale-join checks pass after broker rendering.",
        ),
        _requirement(
            CertificationGate.ENSEMBLE_EVIDENCE,
            "ensemble_rates_reported",
            CertificationComparator.TRUE,
            True,
            ("ensemble-calibration-report",),
            "Calibration, diversity, refusal, and unsupported-region rates are reported.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "product_activity_bar_mismatch_count",
            CertificationComparator.ZERO,
            0,
            (
                "reconstruction-product-manifest",
                "activity-manifest",
                "derived-bar-manifest",
            ),
            "Final events, activity semantics, and derived bars reconcile.",
        ),
        _requirement(
            CertificationGate.FAILURE_RESUME,
            "resume_duplicate_or_missing_partition_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-run-report", "failure-injection-report"),
            "A mid-run failure resumes without duplicate or missing partitions.",
        ),
        _requirement(
            CertificationGate.REPLAY,
            "replay_logical_hash_mismatch_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-product-manifest", "replay-report"),
            "A clean replay reproduces logical content hashes.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_peak_memory_bytes",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_int(peak_memory_budget_bytes, "peak_memory_budget_bytes"),
            ("resource-report",),
            "Actual peak memory remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_scratch_bytes",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_int(scratch_budget_bytes, "scratch_budget_bytes"),
            ("resource-report",),
            "Actual scratch use remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_runtime_seconds",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_float(runtime_budget_seconds, "runtime_budget_seconds"),
            ("resource-report",),
            "Actual runtime remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_final_storage_bytes",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_int(storage_budget_bytes, "storage_budget_bytes"),
            ("resource-report",),
            "Actual final storage remains inside its predeclared budget.",
        ),
        *(
            _requirement(
                CertificationGate.NEGATIVE_TESTS,
                check_id,
                CertificationComparator.TRUE,
                True,
                ("negative-test-report",),
                description,
            )
            for check_id, description in (
                ("corruption_refused", "Corrupt products fail closed."),
                (
                    "stale_broker_profile_refused",
                    "Stale broker profiles fail closed.",
                ),
                (
                    "unhealthy_clock_refused",
                    "Unhealthy capture clocks fail closed.",
                ),
                (
                    "missing_context_refused",
                    "Missing required context fails closed.",
                ),
                (
                    "partial_group_refused",
                    "Partial synchronized groups fail closed.",
                ),
            )
        ),
        _requirement(
            CertificationGate.STRATEGY_SENSITIVITY,
            "strategy_uncertainty_reported",
            CertificationComparator.TRUE,
            True,
            ("strategy-sensitivity-report",),
            "Strategy sensitivity reports bounded uncertainty evidence.",
        ),
        _requirement(
            CertificationGate.STRATEGY_SENSITIVITY,
            "strategy_automatic_winner",
            CertificationComparator.FALSE,
            False,
            ("strategy-sensitivity-report",),
            "Strategy sensitivity never selects an automatic winner.",
        ),
        _requirement(
            CertificationGate.DOSSIER_PUBLICATION,
            "methodology_and_limitations_published",
            CertificationComparator.TRUE,
            True,
            ("methodology-report",),
            "Human-readable methodology and limitations are published.",
        ),
        _requirement(
            CertificationGate.DOSSIER_PUBLICATION,
            "machine_evidence_manifest_published",
            CertificationComparator.TRUE,
            True,
            ("machine-evidence-manifest",),
            "The machine-readable evidence manifest is published.",
        ),
        _requirement(
            CertificationGate.REPOSITORY_GATES,
            "full_plain_test_suite_passed",
            CertificationComparator.TRUE,
            True,
            ("repository-gate-report",),
            "The full plain test suite passes.",
        ),
        _requirement(
            CertificationGate.REPOSITORY_GATES,
            "precommit_and_prepush_passed",
            CertificationComparator.TRUE,
            True,
            ("repository-gate-report",),
            "Pre-commit and real pre-push hooks pass.",
        ),
        _requirement(
            CertificationGate.REPOSITORY_GATES,
            "coverage_promotion_run_count",
            CertificationComparator.EQUAL,
            1,
            ("repository-gate-report",),
            "Coverage runs exactly once at the dev-to-main promotion boundary.",
        ),
        _requirement(
            CertificationGate.TESTPYPI_PREFLIGHT,
            "local_simple_registry_preflight_passed",
            CertificationComparator.TRUE,
            True,
            ("testpypi-preflight-report",),
            "The TestPyPI preflight passes through the local simple registry.",
        ),
    )
    return ReconstructionCertificationPolicyV1(
        product_version="2.1.0",
        symbols=EURUSD_TRIANGLE_SYMBOLS,
        common_start_period=EURUSD_TRIANGLE_COMMON_START_PERIOD,
        common_end_period=common_end_period,
        broker_fingerprint_id=broker_fingerprint_id,
        requirements=requirements,
    )


def modern_reference_triangle_certification_policy(
    *,
    common_end_period: str,
    peak_memory_budget_bytes: int,
    scratch_budget_bytes: int,
    runtime_budget_seconds: float,
    storage_budget_bytes: int,
    candidate_amplification_budget: float,
) -> ReconstructionCertificationPolicyV2:
    """Return the complete broker-neutral v2.5.0 certification policy.

    The policy extends the retained #449/#491 qualification evidence with
    #498's complete support, campaign product, dataset, mounted-storage, and
    recovery evidence. Broker capture, fingerprints, clocks, and transfer
    checks remain deliberately absent because they are separately qualified
    optional extensions.
    """
    requirements = (
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "source_inventory_reconciled",
            CertificationComparator.TRUE,
            True,
            ("raw-source-inventory",),
            "Source dimensions, hashes, common range, and readability reconcile.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "duplicate_source_dimension_count",
            CertificationComparator.ZERO,
            0,
            ("raw-source-inventory",),
            "The canonical inventory contains no duplicate source dimension.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "raw_source_hash_mismatch_count",
            CertificationComparator.ZERO,
            0,
            ("raw-source-inventory", "reconstruction-product-manifest"),
            "Raw source hashes reconcile with the committed product.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "immutable_anchor_mismatch_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-product-manifest",),
            "Every recorded historical tick remains logically unchanged.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "synthetic_lineage_missing_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-product-manifest",),
            "Every synthetic event carries deterministic identity and lineage.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "support_map_gap_or_overlap_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-plan-support-map",),
            "Every common planning window appears exactly once in order.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "valid_common_data_refusal_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-plan-support-map", "raw-source-inventory"),
            "No refusal covers otherwise valid common triangle evidence.",
        ),
        _requirement(
            CertificationGate.IDENTITY_AND_ANCHORS,
            "unclassified_terminal_outcome_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-plan-support-map",),
            "Every common window has an executable, empty, closed, or justified refusal outcome.",
        ),
        _requirement(
            CertificationGate.INFORMATION_SAFETY,
            "market_context_corpus_valid",
            CertificationComparator.TRUE,
            True,
            ("market-context-corpus",),
            "The point-in-time context corpus has qualified coverage and lineage.",
        ),
        _requirement(
            CertificationGate.INFORMATION_SAFETY,
            "cftc_positioning_corpus_valid",
            CertificationComparator.TRUE,
            True,
            ("cftc-positioning-corpus",),
            "The point-in-time CFTC corpus has qualified availability and lineage.",
        ),
        _requirement(
            CertificationGate.INFORMATION_SAFETY,
            "information_audit_violation_count",
            CertificationComparator.ZERO,
            0,
            ("information-audit-report",),
            "Every claimed use has zero information-leakage violations.",
        ),
        _requirement(
            CertificationGate.INFORMATION_SAFETY,
            "claimed_use_missing_audit_count",
            CertificationComparator.ZERO,
            0,
            ("information-audit-report",),
            "No ex-post or ex-ante claim lacks a point-in-time audit.",
        ),
        _requirement(
            CertificationGate.INFORMATION_SAFETY,
            "information_modes_audited_separately",
            CertificationComparator.TRUE,
            True,
            ("information-audit-report",),
            "Ex-post reconstruction and ex-ante simulation have distinct passing audits.",
        ),
        _requirement(
            CertificationGate.REVERSE_DEGRADATION,
            "benchmark_corpus_valid",
            CertificationComparator.TRUE,
            True,
            ("benchmark-corpus-manifest",),
            "The benchmark corpus and gate version predate candidate results.",
        ),
        _requirement(
            CertificationGate.REVERSE_DEGRADATION,
            "reverse_thresholds_predeclared",
            CertificationComparator.TRUE,
            True,
            ("reverse-degradation-scorecard",),
            "Reverse-degradation thresholds were fixed before final holdout use.",
        ),
        _requirement(
            CertificationGate.REVERSE_DEGRADATION,
            "reverse_holdout_failure_count",
            CertificationComparator.ZERO,
            0,
            ("reverse-degradation-scorecard",),
            "Untouched blocked holdouts pass and negative controls fail as expected.",
        ),
        _requirement(
            CertificationGate.CONDITIONED_SCORECARDS,
            "feed_epoch_artifact_valid",
            CertificationComparator.TRUE,
            True,
            ("feed-epoch-definition",),
            "The multivariate epoch artifact and stability evidence are valid.",
        ),
        _requirement(
            CertificationGate.CONDITIONED_SCORECARDS,
            "observation_operator_valid",
            CertificationComparator.TRUE,
            True,
            ("observation-operator",),
            "Calibrated observation operators are supported and non-identity where claimed.",
        ),
        _requirement(
            CertificationGate.CONDITIONED_SCORECARDS,
            "qualified_portfolio_artifact_valid",
            CertificationComparator.TRUE,
            True,
            ("powered-qualification-dossier",),
            "Every selected engine and frozen portfolio weight comes from the powered qualification dossier.",
        ),
        _requirement(
            CertificationGate.CONDITIONED_SCORECARDS,
            "conditioned_tolerance_violation_count",
            CertificationComparator.ZERO,
            0,
            (
                "reverse-degradation-scorecard",
                "modern-reference-product-scorecard",
            ),
            "Epoch, session, event, cadence, spread, timing, and path tolerances pass.",
        ),
        _requirement(
            CertificationGate.CONDITIONED_SCORECARDS,
            "required_stratum_missing_count",
            CertificationComparator.ZERO,
            0,
            (
                "reverse-degradation-scorecard",
                "modern-reference-product-scorecard",
            ),
            "Every required scientific stratum has measured support.",
        ),
        _requirement(
            CertificationGate.CROSS_CURRENCY,
            "modern_reference_cross_currency_failure_count",
            CertificationComparator.ZERO,
            0,
            (
                "cross-currency-validation-report",
                "reconstruction-product-manifest",
            ),
            "Triangle, inverse, synchronization, and stale-alignment checks pass before and after identity delivery.",
        ),
        _requirement(
            CertificationGate.ENSEMBLE_EVIDENCE,
            "ensemble_rates_reported",
            CertificationComparator.TRUE,
            True,
            ("ensemble-calibration-report",),
            "Calibration, diversity, refusal, and unsupported-region rates are reported.",
        ),
        _requirement(
            CertificationGate.ENSEMBLE_EVIDENCE,
            "between_seed_and_window_uncertainty_reported",
            CertificationComparator.TRUE,
            True,
            ("ensemble-calibration-report",),
            "Between-seed and between-window uncertainty are reported.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "product_activity_bar_mismatch_count",
            CertificationComparator.ZERO,
            0,
            (
                "reconstruction-product-manifest",
                "activity-manifest",
                "derived-bar-manifest",
            ),
            "Final events, activity semantics, and downstream bars reconcile.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "scientific_nonclaim_published",
            CertificationComparator.TRUE,
            True,
            ("reconstruction-product-manifest", "methodology-report"),
            "Output is labeled a plausible counterfactual ensemble, never historical truth.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "full_range_public_preflight_passed",
            CertificationComparator.TRUE,
            True,
            ("reconstruction-plan-report", "public-interface-report"),
            "The full-range synchronized plan and resource preflight pass publicly.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "full_campaign_execution_passed",
            CertificationComparator.TRUE,
            True,
            (
                "reconstruction-run-report",
                "reconstruction-plan-support-map",
            ),
            "Every executable shard reaches a terminal first-party Temporal outcome.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "executable_retained_product_missing_count",
            CertificationComparator.ZERO,
            0,
            (
                "reconstruction-campaign-product-index",
                "reconstruction-plan-support-map",
            ),
            "Every executable window contains the complete retained-member product rectangle.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "fabricated_liquidity_terminal_outcome_count",
            CertificationComparator.ZERO,
            0,
            (
                "reconstruction-campaign-product-index",
                "reconstruction-plan-support-map",
            ),
            "Empty, closed, and unsupported windows publish no invented liquidity.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "campaign_product_index_valid",
            CertificationComparator.TRUE,
            True,
            ("reconstruction-campaign-product-index",),
            "The content-addressed campaign index fully verifies every product and terminal outcome.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "campaign_dataset_publication_valid",
            CertificationComparator.TRUE,
            True,
            (
                "reconstruction-campaign-dataset-publication",
                "reconstruction-campaign-product-index",
            ),
            "The provider-neutral synthetic dataset version binds the complete campaign index.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "representative_window_class_missing_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-run-report",),
            "Sparse, transitional, dense, ordinary, rollover, news, shock, and refusal windows execute.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "substantial_multi_period_run_passed",
            CertificationComparator.TRUE,
            True,
            ("reconstruction-run-report", "reconstruction-product-manifest"),
            "A substantial multi-period run commits queryable Parquet and compact manifests.",
        ),
        _requirement(
            CertificationGate.PRODUCT_RECONCILIATION,
            "public_cli_api_evidence_chain_passed",
            CertificationComparator.TRUE,
            True,
            ("public-interface-report",),
            "CLI and API expose bounded final ticks and the complete evidence chain.",
        ),
        _requirement(
            CertificationGate.FAILURE_RESUME,
            "resume_duplicate_or_missing_partition_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-run-report", "failure-injection-report"),
            "A mid-run worker/server failure resumes without duplicate or missing partitions.",
        ),
        _requirement(
            CertificationGate.FAILURE_RESUME,
            "storage_disconnect_resume_passed",
            CertificationComparator.TRUE,
            True,
            ("failure-injection-report", "storage-qualification-report"),
            "A qualified storage disconnect fails closed and resumes idempotently after remount.",
        ),
        _requirement(
            CertificationGate.FAILURE_RESUME,
            "cancellation_publishable_partial_count",
            CertificationComparator.ZERO,
            0,
            ("cancellation-report",),
            "Cancellation leaves no publishable partial partition.",
        ),
        _requirement(
            CertificationGate.REPLAY,
            "replay_logical_hash_mismatch_count",
            CertificationComparator.ZERO,
            0,
            ("reconstruction-product-manifest", "replay-report"),
            "Clean replay reproduces logical content hashes across concurrency settings.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_peak_memory_bytes",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_int(peak_memory_budget_bytes, "peak_memory_budget_bytes"),
            ("resource-report",),
            "Actual peak memory remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_scratch_bytes",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_int(scratch_budget_bytes, "scratch_budget_bytes"),
            ("resource-report",),
            "Actual scratch use remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_runtime_seconds",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_float(runtime_budget_seconds, "runtime_budget_seconds"),
            ("resource-report",),
            "Actual runtime remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_candidate_amplification",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_float(
                candidate_amplification_budget,
                "candidate_amplification_budget",
            ),
            ("resource-report",),
            "Candidate amplification remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_final_storage_bytes",
            CertificationComparator.LESS_OR_EQUAL,
            _positive_int(storage_budget_bytes, "storage_budget_bytes"),
            ("resource-report",),
            "Actual final storage remains inside its predeclared budget.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "mounted_storage_integrity_passed",
            CertificationComparator.TRUE,
            True,
            ("storage-qualification-report",),
            "Sustained write/read/hash, remount, device, and no-fallback checks pass on campaign storage.",
        ),
        _requirement(
            CertificationGate.RESOURCES,
            "actual_final_row_count",
            CertificationComparator.GREATER_OR_EQUAL,
            1,
            ("resource-report", "reconstruction-product-manifest"),
            "The committed substantial run contains measured final rows.",
        ),
        *(
            _requirement(
                CertificationGate.NEGATIVE_TESTS,
                check_id,
                CertificationComparator.TRUE,
                True,
                ("negative-test-report",),
                description,
            )
            for check_id, description in (
                ("corruption_refused", "Corrupt artifacts fail closed."),
                ("stale_artifact_refused", "Stale artifacts fail closed."),
                (
                    "missing_context_refused",
                    "Missing required context fails closed.",
                ),
                (
                    "invalid_information_mode_refused",
                    "Invalid information modes fail closed.",
                ),
                ("quota_overflow_refused", "Quota overflow fails closed."),
                (
                    "partial_group_refused",
                    "Partial synchronized groups fail closed.",
                ),
            )
        ),
        _requirement(
            CertificationGate.STRATEGY_SENSITIVITY,
            "strategy_uncertainty_reported",
            CertificationComparator.TRUE,
            True,
            ("strategy-sensitivity-report",),
            "Strategy sensitivity reports bounded uncertainty evidence.",
        ),
        _requirement(
            CertificationGate.STRATEGY_SENSITIVITY,
            "strategy_automatic_winner",
            CertificationComparator.FALSE,
            False,
            ("strategy-sensitivity-report",),
            "Strategy sensitivity never selects an automatic winner.",
        ),
        _requirement(
            CertificationGate.DOSSIER_PUBLICATION,
            "methodology_and_limitations_published",
            CertificationComparator.TRUE,
            True,
            ("methodology-report",),
            "Human-readable methodology, limitations, and nonclaim are published.",
        ),
        _requirement(
            CertificationGate.DOSSIER_PUBLICATION,
            "machine_evidence_manifest_published",
            CertificationComparator.TRUE,
            True,
            ("machine-evidence-manifest",),
            "The machine-readable evidence manifest is published.",
        ),
        _requirement(
            CertificationGate.DOSSIER_PUBLICATION,
            "diagnostic_publication_valid",
            CertificationComparator.TRUE,
            True,
            ("diagnostic-publication-manifest",),
            "All twelve diagnostic families are published from this campaign's verified evidence graph.",
        ),
        _requirement(
            CertificationGate.REPOSITORY_GATES,
            "declared_test_dependencies_installed",
            CertificationComparator.TRUE,
            True,
            ("repository-gate-report",),
            "Every declared test dependency is installed and no suite is silently skipped.",
        ),
        _requirement(
            CertificationGate.REPOSITORY_GATES,
            "full_plain_test_suite_passed",
            CertificationComparator.TRUE,
            True,
            ("repository-gate-report",),
            "The full plain test suite passes.",
        ),
        _requirement(
            CertificationGate.REPOSITORY_GATES,
            "precommit_and_prepush_passed",
            CertificationComparator.TRUE,
            True,
            ("repository-gate-report",),
            "Pre-commit and real pre-push hooks pass.",
        ),
        _requirement(
            CertificationGate.REPOSITORY_GATES,
            "coverage_promotion_run_count",
            CertificationComparator.EQUAL,
            1,
            ("repository-gate-report",),
            "Coverage runs exactly once at the dev-to-main promotion boundary.",
        ),
        _requirement(
            CertificationGate.TESTPYPI_PREFLIGHT,
            "local_simple_registry_preflight_passed",
            CertificationComparator.TRUE,
            True,
            ("testpypi-preflight-report",),
            "The TestPyPI preflight passes through the local simple registry.",
        ),
    )
    return ReconstructionCertificationPolicyV2(
        product_version="2.5.0",
        symbols=EURUSD_TRIANGLE_SYMBOLS,
        common_start_period=EURUSD_TRIANGLE_COMMON_START_PERIOD,
        common_end_period=common_end_period,
        delivery_mode=MODERN_REFERENCE_DELIVERY_MODE,
        delivery_claim=MODERN_REFERENCE_DELIVERY_CLAIM,
        requirements=requirements,
    )


def evaluate_reconstruction_certification(
    policy: ReconstructionCertificationPolicyV1,
    *,
    artifacts: Sequence[CertificationArtifactV1],
    observations: Sequence[CertificationObservationV1],
    methodology: str,
    accepted_limitations: Sequence[str] = (),
    blocking_limitations: Sequence[str] = (),
) -> ReconstructionCertificationDossierV1:
    """Evaluate every predeclared requirement without reading event rows."""
    if not isinstance(policy, ReconstructionCertificationPolicyV1):
        raise TypeError("certification evaluation requires a v1 policy")
    selected_artifacts = tuple(artifacts)
    selected_observations = tuple(observations)
    if not selected_artifacts or len(selected_artifacts) > policy.max_artifacts:
        raise ValueError("certification artifacts are empty or unbounded")
    if len(selected_observations) > policy.max_observations:
        raise ValueError("certification observations exceed policy")
    artifact_by_id = {item.evidence_id: item for item in selected_artifacts}
    if len(artifact_by_id) != len(selected_artifacts):
        raise ValueError("certification artifacts duplicate evidence IDs")
    foreign_policy_artifacts = sorted(
        item.evidence_id
        for item in selected_artifacts
        if item.policy_id != policy.policy_id
    )
    if foreign_policy_artifacts:
        raise ValueError(
            f"certification artifacts differ from policy: {foreign_policy_artifacts}"
        )
    observation_by_check = {
        item.check_id: item for item in selected_observations
    }
    if len(observation_by_check) != len(selected_observations):
        raise ValueError("certification observations duplicate check IDs")
    unknown_checks = set(observation_by_check).difference(
        item.check_id for item in policy.requirements
    )
    if unknown_checks:
        raise ValueError(
            f"certification observations are outside policy: {sorted(unknown_checks)}"
        )
    by_gate: dict[CertificationGate, list[CertificationCheckResultV1]] = {
        gate: [] for gate in CertificationGate
    }
    for requirement in policy.requirements:
        result = _evaluate_requirement(
            policy,
            requirement,
            observation_by_check.get(requirement.check_id),
            artifact_by_id,
        )
        by_gate[requirement.gate].append(result)
    gate_results = tuple(
        CertificationGateResultV1(gate=gate, check_results=tuple(results))
        for gate, results in by_gate.items()
    )
    state = _certification_state(
        gate_results,
        _normalized_bounded_text_tuple(
            blocking_limitations, "blocking limitation"
        ),
    )
    return ReconstructionCertificationDossierV1(
        policy=policy,
        artifacts=selected_artifacts,
        gate_results=gate_results,
        methodology=methodology,
        accepted_limitations=tuple(accepted_limitations),
        blocking_limitations=tuple(blocking_limitations),
        state=state,
    )


def evaluate_modern_reference_reconstruction_certification(
    policy: ReconstructionCertificationPolicyV2,
    *,
    artifacts: Sequence[CertificationArtifactV1],
    observations: Sequence[CertificationObservationV1],
    methodology: str,
    accepted_limitations: Sequence[str] = (),
    blocking_limitations: Sequence[str] = (),
) -> ReconstructionCertificationDossierV2:
    """Evaluate modern-reference evidence without accepting broker artifacts."""
    if not isinstance(policy, ReconstructionCertificationPolicyV2):
        raise TypeError("modern-reference evaluation requires a v2 policy")
    selected_artifacts = tuple(artifacts)
    selected_observations = tuple(observations)
    if not selected_artifacts or len(selected_artifacts) > policy.max_artifacts:
        raise ValueError("certification artifacts are empty or unbounded")
    if len(selected_observations) > policy.max_observations:
        raise ValueError("certification observations exceed policy")
    if any("broker" in item.kind for item in selected_artifacts):
        raise ValueError(
            "modern-reference certification rejects broker-specific evidence"
        )
    artifact_by_id = {item.evidence_id: item for item in selected_artifacts}
    if len(artifact_by_id) != len(selected_artifacts):
        raise ValueError("certification artifacts duplicate evidence IDs")
    foreign_policy_artifacts = sorted(
        item.evidence_id
        for item in selected_artifacts
        if item.policy_id != policy.policy_id
    )
    if foreign_policy_artifacts:
        raise ValueError(
            f"certification artifacts differ from policy: {foreign_policy_artifacts}"
        )
    observation_by_check = {
        item.check_id: item for item in selected_observations
    }
    if len(observation_by_check) != len(selected_observations):
        raise ValueError("certification observations duplicate check IDs")
    unknown_checks = set(observation_by_check).difference(
        item.check_id for item in policy.requirements
    )
    if unknown_checks:
        raise ValueError(
            f"certification observations are outside policy: {sorted(unknown_checks)}"
        )
    by_gate: dict[CertificationGate, list[CertificationCheckResultV1]] = {
        gate: [] for gate in CertificationGate
    }
    for requirement in policy.requirements:
        result = _evaluate_requirement_v2(
            requirement,
            observation_by_check.get(requirement.check_id),
            artifact_by_id,
        )
        by_gate[requirement.gate].append(result)
    gate_results = tuple(
        CertificationGateResultV1(gate=gate, check_results=tuple(results))
        for gate, results in by_gate.items()
    )
    state = _certification_state(
        gate_results,
        _normalized_bounded_text_tuple(
            blocking_limitations, "blocking limitation"
        ),
    )
    return ReconstructionCertificationDossierV2(
        policy=policy,
        artifacts=selected_artifacts,
        gate_results=gate_results,
        methodology=methodology,
        accepted_limitations=tuple(accepted_limitations),
        blocking_limitations=tuple(blocking_limitations),
        state=state,
    )


def write_reconstruction_certification_dossier(
    dossier: ReconstructionCertificationDossierV1,
    *,
    json_path: str | Path,
    markdown_path: str | Path,
) -> tuple[ArtifactRef, ArtifactRef]:
    """Atomically publish machine and human certification reports."""
    if not isinstance(dossier, ReconstructionCertificationDossierV1):
        raise TypeError("certification publication requires a v1 dossier")
    json_target = Path(json_path).expanduser().resolve()
    markdown_target = Path(markdown_path).expanduser().resolve()
    if json_target == markdown_target:
        raise ValueError("certification JSON and Markdown paths must differ")
    json_payload = dossier.to_json().encode("utf-8") + b"\n"
    markdown_payload = dossier.to_markdown().encode("utf-8")
    _atomic_write(json_target, json_payload)
    _atomic_write(markdown_target, markdown_payload)
    restored = ReconstructionCertificationDossierV1.from_json(
        json_target.read_text(encoding="utf-8")
    )
    if restored != dossier:
        raise ValueError("published certification dossier differs on readback")
    return (
        _artifact_ref(
            "reconstruction-certification-json",
            json_target,
            json_payload,
            dossier,
        ),
        _artifact_ref(
            "reconstruction-certification-markdown",
            markdown_target,
            markdown_payload,
            dossier,
        ),
    )


def load_reconstruction_certification_dossier(
    path: str | Path,
) -> ReconstructionCertificationDossierV1:
    """Load and verify a published machine-readable dossier."""
    return ReconstructionCertificationDossierV1.from_json(
        Path(path).expanduser().resolve().read_text(encoding="utf-8")
    )


def write_modern_reference_reconstruction_certification_dossier(
    dossier: ReconstructionCertificationDossierV2,
    *,
    json_path: str | Path,
    markdown_path: str | Path,
) -> tuple[ArtifactRef, ArtifactRef]:
    """Atomically publish machine and human modern-reference reports."""
    if not isinstance(dossier, ReconstructionCertificationDossierV2):
        raise TypeError("modern-reference publication requires a v2 dossier")
    json_target = Path(json_path).expanduser().resolve()
    markdown_target = Path(markdown_path).expanduser().resolve()
    if json_target == markdown_target:
        raise ValueError("certification JSON and Markdown paths must differ")
    json_payload = dossier.to_json().encode("utf-8") + b"\n"
    markdown_payload = dossier.to_markdown().encode("utf-8")
    _atomic_write(json_target, json_payload)
    _atomic_write(markdown_target, markdown_payload)
    restored = ReconstructionCertificationDossierV2.from_json(
        json_target.read_text(encoding="utf-8")
    )
    if restored != dossier:
        raise ValueError("published certification dossier differs on readback")
    return (
        _artifact_ref(
            "reconstruction-certification-json-v2",
            json_target,
            json_payload,
            dossier,
        ),
        _artifact_ref(
            "reconstruction-certification-markdown-v2",
            markdown_target,
            markdown_payload,
            dossier,
        ),
    )


def load_modern_reference_reconstruction_certification_dossier(
    path: str | Path,
) -> ReconstructionCertificationDossierV2:
    """Load and verify a published modern-reference dossier."""
    return ReconstructionCertificationDossierV2.from_json(
        Path(path).expanduser().resolve().read_text(encoding="utf-8")
    )


def _evaluate_requirement(
    policy: ReconstructionCertificationPolicyV1,
    requirement: CertificationRequirementV1,
    observation: CertificationObservationV1 | None,
    artifacts: Mapping[str, CertificationArtifactV1],
) -> CertificationCheckResultV1:
    if observation is None:
        return _missing_result(requirement, "observation is missing")
    selected: list[CertificationArtifactV1] = []
    for evidence_id in observation.artifact_evidence_ids:
        artifact = artifacts.get(evidence_id)
        if artifact is None:
            return _missing_result(
                requirement,
                f"artifact evidence is missing: {evidence_id}",
                observation,
            )
        if not artifact.verified:
            return _missing_result(
                requirement,
                f"artifact evidence is unverified: {evidence_id}",
                observation,
            )
        selected.append(artifact)
    kinds = {item.kind for item in selected}
    missing_kinds = set(requirement.required_artifact_kinds).difference(kinds)
    if missing_kinds:
        return _missing_result(
            requirement,
            f"required artifact kinds are missing: {sorted(missing_kinds)}",
            observation,
        )
    broker_artifacts = [
        item for item in selected if item.kind == "broker-delivery-fingerprint"
    ]
    if broker_artifacts and any(
        item.subject_id != policy.broker_fingerprint_id
        for item in broker_artifacts
    ):
        return CertificationCheckResultV1(
            requirement_id=requirement.requirement_id,
            check_id=requirement.check_id,
            status=CertificationCheckStatus.FAILED,
            comparator=requirement.comparator,
            expected=requirement.expected,
            actual=observation.actual,
            artifact_evidence_ids=observation.artifact_evidence_ids,
            reason="selected broker fingerprint differs from policy",
        )
    passed = _compare(
        requirement.comparator, observation.actual, requirement.expected
    )
    return CertificationCheckResultV1(
        requirement_id=requirement.requirement_id,
        check_id=requirement.check_id,
        status=(
            CertificationCheckStatus.PASSED
            if passed
            else CertificationCheckStatus.FAILED
        ),
        comparator=requirement.comparator,
        expected=requirement.expected,
        actual=observation.actual,
        artifact_evidence_ids=observation.artifact_evidence_ids,
        reason=(
            "requirement satisfied"
            if passed
            else "measured value violates policy"
        ),
    )


def _evaluate_requirement_v2(
    requirement: CertificationRequirementV1,
    observation: CertificationObservationV1 | None,
    artifacts: Mapping[str, CertificationArtifactV1],
) -> CertificationCheckResultV1:
    """Evaluate one V2 requirement with an exact evidence-kind binding."""
    if observation is None:
        return _missing_result(requirement, "observation is missing")
    selected: list[CertificationArtifactV1] = []
    for evidence_id in observation.artifact_evidence_ids:
        artifact = artifacts.get(evidence_id)
        if artifact is None:
            return _missing_result(
                requirement,
                f"artifact evidence is missing: {evidence_id}",
                observation,
            )
        if not artifact.verified:
            return _missing_result(
                requirement,
                f"artifact evidence is unverified: {evidence_id}",
                observation,
            )
        if "broker" in artifact.kind:
            return CertificationCheckResultV1(
                requirement_id=requirement.requirement_id,
                check_id=requirement.check_id,
                status=CertificationCheckStatus.FAILED,
                comparator=requirement.comparator,
                expected=requirement.expected,
                actual=observation.actual,
                artifact_evidence_ids=observation.artifact_evidence_ids,
                reason="broker-specific evidence is outside modern-reference scope",
            )
        selected.append(artifact)
    kinds = {item.kind for item in selected}
    required_kinds = set(requirement.required_artifact_kinds)
    if kinds != required_kinds:
        missing = sorted(required_kinds.difference(kinds))
        extra = sorted(kinds.difference(required_kinds))
        return _missing_result(
            requirement,
            f"evidence kinds differ; missing={missing}, extra={extra}",
            observation,
        )
    passed = _compare(
        requirement.comparator, observation.actual, requirement.expected
    )
    return CertificationCheckResultV1(
        requirement_id=requirement.requirement_id,
        check_id=requirement.check_id,
        status=(
            CertificationCheckStatus.PASSED
            if passed
            else CertificationCheckStatus.FAILED
        ),
        comparator=requirement.comparator,
        expected=requirement.expected,
        actual=observation.actual,
        artifact_evidence_ids=observation.artifact_evidence_ids,
        reason=(
            "requirement satisfied"
            if passed
            else "measured value violates policy"
        ),
    )


def _missing_result(
    requirement: CertificationRequirementV1,
    reason: str,
    observation: CertificationObservationV1 | None = None,
) -> CertificationCheckResultV1:
    return CertificationCheckResultV1(
        requirement_id=requirement.requirement_id,
        check_id=requirement.check_id,
        status=CertificationCheckStatus.MISSING,
        comparator=requirement.comparator,
        expected=requirement.expected,
        actual=None if observation is None else observation.actual,
        artifact_evidence_ids=(
            () if observation is None else observation.artifact_evidence_ids
        ),
        reason=reason,
    )


def _certification_state(
    gate_results: Sequence[CertificationGateResultV1],
    blocking_limitations: Sequence[str],
) -> CertificationState:
    checks = [item for gate in gate_results for item in gate.check_results]
    if any(item.status is CertificationCheckStatus.FAILED for item in checks):
        return CertificationState.FAILED
    if blocking_limitations:
        return CertificationState.INCOMPLETE
    missing = {
        item.check_id
        for item in checks
        if item.status is CertificationCheckStatus.MISSING
    }
    if not missing:
        return CertificationState.CERTIFIED
    if missing.issubset(PROMOTION_ONLY_CHECK_IDS):
        return CertificationState.READY_FOR_PROMOTION
    return CertificationState.INCOMPLETE


def _compare(
    comparator: CertificationComparator,
    actual: JSONScalar,
    expected: JSONScalar,
) -> bool:
    if comparator is CertificationComparator.EQUAL:
        return type(actual) is type(expected) and actual == expected
    if comparator is CertificationComparator.TRUE:
        return actual is True
    if comparator is CertificationComparator.FALSE:
        return actual is False
    if comparator is CertificationComparator.ZERO:
        return _numeric(actual, "actual") == 0
    actual_number = _numeric(actual, "actual")
    expected_number = _numeric(expected, "expected")
    if comparator is CertificationComparator.LESS_OR_EQUAL:
        return actual_number <= expected_number
    if comparator is CertificationComparator.GREATER_OR_EQUAL:
        return actual_number >= expected_number
    raise ValueError(f"unsupported certification comparator: {comparator}")


def _validate_expected(
    comparator: CertificationComparator, expected: JSONScalar
) -> None:
    _json_scalar(expected, "expected")
    if comparator is CertificationComparator.TRUE and expected is not True:
        raise ValueError("true comparator requires expected true")
    if comparator is CertificationComparator.FALSE and expected is not False:
        raise ValueError("false comparator requires expected false")
    if (
        comparator is CertificationComparator.ZERO
        and _numeric(expected, "expected") != 0
    ):
        raise ValueError("zero comparator requires expected zero")
    if comparator in {
        CertificationComparator.LESS_OR_EQUAL,
        CertificationComparator.GREATER_OR_EQUAL,
    }:
        _numeric(expected, "expected")


def _requirement(
    gate: CertificationGate,
    check_id: str,
    comparator: CertificationComparator,
    expected: JSONScalar,
    artifact_kinds: tuple[str, ...],
    description: str,
) -> CertificationRequirementV1:
    return CertificationRequirementV1(
        gate=gate,
        check_id=check_id,
        comparator=comparator,
        expected=expected,
        required_artifact_kinds=artifact_kinds,
        description=description,
    )


def _artifact_ref(
    kind: str,
    path: Path,
    payload: bytes,
    dossier: (
        ReconstructionCertificationDossierV1
        | ReconstructionCertificationDossierV2
    ),
) -> ArtifactRef:
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        metadata={
            "dossier_id": dossier.dossier_id,
            "policy_id": dossier.policy.policy_id,
            "state": dossier.state.value,
        },
    )


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
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    """Best-effort directory fsync where the host supports directory handles."""
    try:
        directory_fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        os.close(directory_fd)


def _markdown_limitations(title: str, values: Sequence[str]) -> list[str]:
    lines = [f"## {title}", ""]
    if values:
        lines.extend(f"- {item}" for item in values)
    else:
        lines.append("- None.")
    lines.append("")
    return lines


def _safe_relative_path(value: Any) -> str:
    text = _required_text(value).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts or text.startswith("~"):
        raise ValueError(
            "certification artifact path must be relative and safe"
        )
    return path.as_posix()


def _required_period(value: Any) -> str:
    text = _required_text(value)
    if len(text) != 6 or not text.isdigit():
        raise ValueError("certification period must be YYYYMM")
    month = int(text[4:])
    if month < 1 or month > 12:
        raise ValueError("certification period month is invalid")
    return text


def _normalized_symbol(value: Any) -> str:
    text = _required_name(value).upper()
    if len(text) != 6 or not text.isalpha():
        raise ValueError("certification symbol must be a six-letter FX pair")
    return text


def _numeric(value: JSONScalar, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    number = float(value)
    if not number == number or number in {float("inf"), float("-inf")}:
        raise ValueError(f"{name} must be finite")
    return number


def _positive_float(value: Any, name: str) -> float:
    selected = _numeric(cast(JSONScalar, value), name)
    if selected <= 0:
        raise ValueError(f"{name} must be positive")
    return selected


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float):
            _numeric(value, name)
        return value
    raise ValueError(f"{name} must be a JSON scalar")


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("certification text is required")
    if len(text) > DEFAULT_CERTIFICATION_MAX_TEXT_LENGTH:
        raise ValueError("certification text exceeds limit")
    return text


def _required_name(value: Any) -> str:
    text = _required_text(value)
    if any(character.isspace() for character in text):
        raise ValueError("certification identifier cannot contain whitespace")
    return text


def _bounded_text(value: Any, *, allow_empty: bool = False) -> str:
    text = str(value or "").strip()
    if not text and not allow_empty:
        raise ValueError("certification text is required")
    if len(text) > DEFAULT_CERTIFICATION_MAX_TEXT_LENGTH:
        raise ValueError("certification text exceeds limit")
    return text


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _required_sha256(value: Any) -> str:
    text = _required_name(value).lower()
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError("certification content hash must be SHA-256")
    return text


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected < 0:
        raise ValueError(f"{name} must be nonnegative")
    return selected


def _positive_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected < 1:
        raise ValueError(f"{name} must be positive")
    return selected


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    selected = _strict_int(value, name)
    if selected < minimum or selected > maximum:
        raise ValueError(f"{name} is outside bounds")
    return selected


def _normalized_text_tuple(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _normalized_bounded_text_tuple(
    values: Iterable[Any], name: str
) -> tuple[str, ...]:
    selected = tuple(sorted({_bounded_text(value) for value in values}))
    if len(selected) > DEFAULT_CERTIFICATION_MAX_METADATA_ITEMS:
        raise ValueError(f"{name} count exceeds limit")
    return selected


def _bounded_mapping(
    value: Mapping[str, JSONValue], name: str, maximum: int
) -> dict[str, JSONValue]:
    if len(value) > maximum:
        raise ValueError(f"{name} exceeds item limit")
    selected = {str(key): item for key, item in sorted(value.items())}
    _ensure_payload_size(selected, DEFAULT_CERTIFICATION_MAX_TEXT_LENGTH)
    return selected


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
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


def _ensure_payload_size(value: Mapping[str, JSONValue], maximum: int) -> None:
    if len(canonical_contract_json(dict(value)).encode("utf-8")) > maximum:
        raise ValueError("certification payload exceeds limit")


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    )
    return f"{prefix}:sha256:{digest.hexdigest()}"


def _require_version(actual: str, expected: str, name: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {name} schema version")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    _require_version(
        str(data.get("schema_version", "")), expected, "certification"
    )


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived certification field {name} differs")


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError("certification JSON is invalid") from error
    return _mapping(value, "certification JSON")
