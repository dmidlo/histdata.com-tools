"""Sealed, one-time release-holdout governance.

The benchmark ``final_holdout`` predates several v2.5 development rounds and
is therefore not eligible to serve as release evidence.  This module defines
a fresh, chronologically blocked and row-free release-holdout manifest.  It
also keeps candidate construction outside the protected role and consumes a
holdout before an evaluation callback can observe protected data.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

RELEASE_HOLDOUT_ACCESS_POLICY_SCHEMA_VERSION = (
    "histdatacom.release-holdout-access-policy.v1"
)
RELEASE_HOLDOUT_DEVELOPMENT_UNIT_SCHEMA_VERSION = (
    "histdatacom.release-holdout-development-unit.v1"
)
PROTECTED_RELEASE_HOLDOUT_WINDOW_SCHEMA_VERSION = (
    "histdatacom.protected-release-holdout-window.v1"
)
RELEASE_HOLDOUT_LEAKAGE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.release-holdout-leakage-audit.v1"
)
RELEASE_HOLDOUT_COVERAGE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.release-holdout-coverage-audit.v1"
)
PROTECTED_RELEASE_HOLDOUT_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.protected-release-holdout-manifest.v1"
)
RELEASE_CANDIDATE_FREEZE_SCHEMA_VERSION = (
    "histdatacom.release-candidate-freeze.v1"
)
RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION = (
    "histdatacom.release-holdout-authorization.v1"
)
RELEASE_HOLDOUT_EVALUATION_RESULT_SCHEMA_VERSION = (
    "histdatacom.release-holdout-evaluation-result.v1"
)
RELEASE_HOLDOUT_EVALUATION_RECEIPT_SCHEMA_VERSION = (
    "histdatacom.release-holdout-evaluation-receipt.v1"
)
RELEASE_HOLDOUT_RETIREMENT_MARKER_SCHEMA_VERSION = (
    "histdatacom.release-holdout-retirement-marker.v1"
)

MAX_RELEASE_HOLDOUT_WINDOWS = 4096
MAX_RELEASE_HOLDOUT_DEVELOPMENT_UNITS = 16384
MAX_RELEASE_HOLDOUT_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_RELEASE_HOLDOUT_FINDINGS = 65536

_PERIOD = re.compile(r"\d{6}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SKETCH = re.compile(r"[0-9a-f]{16}")
_DEVELOPMENT_ROLES = frozenset({"calibration", "validation", "prior_holdout"})
_ALLOWED_INPUT_ROLES = frozenset({"calibration", "validation", "public_policy"})
_FROZEN_STAGES = (
    "fit",
    "preprocess",
    "support_tuning",
    "smoothing",
    "engine_selection",
    "scenario_policy",
    "adaptive_policy",
)


class ReleaseHoldoutAuditStatus(str, Enum):
    """Fail-closed result of a release-holdout audit."""

    PASS = "pass"
    FAIL = "fail"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class ReleaseHoldoutEvaluationOutcome(str, Enum):
    """The single permitted release-holdout evaluation outcome."""

    PASSED = "passed"
    FAILED = "failed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    OPERATIONAL_FAILURE = "operational_failure"


class ReleaseHoldoutAlreadyConsumedError(RuntimeError):
    """Raised when a sealed holdout has already been opened or reserved."""


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutAccessPolicyV1:
    """Predeclared split, access, coverage, and retirement policy."""

    temporal_neighbor_guard_ns: int = 7 * 24 * 60 * 60 * 1_000_000_000
    near_neighbor_hamming_distance: int = 3
    required_feed_epochs: tuple[str, ...] = (
        "early",
        "qualified_transition",
        "modern",
    )
    required_sessions: tuple[str, ...] = (
        "asia",
        "london",
        "new_york",
        "overlap_closure",
    )
    required_event_strata: tuple[str, ...] = ("ordinary", "event")
    required_observation_scenarios: tuple[str, ...] = (
        "high_retention_low_infill",
        "central_fitted_retention",
        "low_retention_high_infill",
    )
    required_alignment_kinds: tuple[str, ...] = ("exact", "bounded_nearest")
    required_deficit_strata: tuple[str, ...] = ("low", "median", "high")
    policy_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_ACCESS_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_ACCESS_POLICY_SCHEMA_VERSION,
        )
        if (
            isinstance(self.temporal_neighbor_guard_ns, bool)
            or not isinstance(self.temporal_neighbor_guard_ns, int)
            or self.temporal_neighbor_guard_ns < 0
        ):
            raise ValueError("holdout temporal neighbor guard is invalid")
        if (
            isinstance(self.near_neighbor_hamming_distance, bool)
            or not isinstance(self.near_neighbor_hamming_distance, int)
            or not 0 <= self.near_neighbor_hamming_distance <= 16
        ):
            raise ValueError("holdout near-neighbor threshold is invalid")
        for name in (
            "required_feed_epochs",
            "required_sessions",
            "required_event_strata",
            "required_observation_scenarios",
            "required_alignment_kinds",
            "required_deficit_strata",
        ):
            object.__setattr__(self, name, _text_tuple(getattr(self, name)))
        expected = _stable_id("release-holdout-access-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("release-holdout policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "temporal_neighbor_guard_ns": self.temporal_neighbor_guard_ns,
            "near_neighbor_hamming_distance": (
                self.near_neighbor_hamming_distance
            ),
            "required_feed_epochs": list(self.required_feed_epochs),
            "required_sessions": list(self.required_sessions),
            "required_event_strata": list(self.required_event_strata),
            "required_observation_scenarios": list(
                self.required_observation_scenarios
            ),
            "required_alignment_kinds": list(self.required_alignment_kinds),
            "required_deficit_strata": list(self.required_deficit_strata),
            "split_unit": "whole-non-overlapping-window-v1",
            "cohesion_policy": (
                "anchors-context-events-and-temporal-neighbors-stay-together-v1"
            ),
            "routine_access": "manifest-and-audit-only",
            "maximum_evaluations": 1,
            "selection_role_permitted": False,
            "candidate_fit_access_permitted": False,
            "preprocessing_access_permitted": False,
            "support_tuning_access_permitted": False,
            "smoothing_access_permitted": False,
            "engine_selection_access_permitted": False,
            "scenario_policy_access_permitted": False,
            "adaptive_policy_access_permitted": False,
            "frozen_graph_required": True,
            "failed_evaluation_tuning_permitted": False,
            "successor_required_after_non_pass": True,
            "retirement_required_after_release_decision": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReleaseHoldoutAccessPolicyV1:
        _require_fixed_policy(data)
        return cls(
            temporal_neighbor_guard_ns=_strict_int(
                data.get("temporal_neighbor_guard_ns"),
                "temporal_neighbor_guard_ns",
            ),
            near_neighbor_hamming_distance=_strict_int(
                data.get("near_neighbor_hamming_distance"),
                "near_neighbor_hamming_distance",
            ),
            required_feed_epochs=_string_tuple(
                data.get("required_feed_epochs")
            ),
            required_sessions=_string_tuple(data.get("required_sessions")),
            required_event_strata=_string_tuple(
                data.get("required_event_strata")
            ),
            required_observation_scenarios=_string_tuple(
                data.get("required_observation_scenarios")
            ),
            required_alignment_kinds=_string_tuple(
                data.get("required_alignment_kinds")
            ),
            required_deficit_strata=_string_tuple(
                data.get("required_deficit_strata")
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutDevelopmentUnitV1:
    """Row-free identity evidence for one non-protected split unit."""

    split_role: str
    period: str
    start_ns: int
    end_ns: int
    source_partition_ids: tuple[str, ...]
    source_hashes: Mapping[str, str]
    source_signature_sha256: str
    motif_signature_sha256: str
    context_signature_sha256: str
    source_neighbor_sketch: str
    motif_neighbor_sketch: str
    context_neighbor_sketch: str
    cohesion_group_ids: tuple[str, ...]
    anchor_neighborhood_ids: tuple[str, ...]
    context_event_ids: tuple[str, ...]
    unit_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_DEVELOPMENT_UNIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_DEVELOPMENT_UNIT_SCHEMA_VERSION,
        )
        role = _required_text(self.split_role)
        if role not in _DEVELOPMENT_ROLES:
            raise ValueError("release-holdout development role is invalid")
        object.__setattr__(self, "split_role", role)
        _validate_identity_unit(self)
        expected = _stable_id(
            "release-holdout-development-unit", self.payload()
        )
        if self.unit_id and self.unit_id != expected:
            raise ValueError(
                "release-holdout development unit identity differs"
            )
        object.__setattr__(self, "unit_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split_role": self.split_role,
            **_identity_unit_payload(self),
            "contains_rows": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "unit_id": self.unit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseHoldoutDevelopmentUnitV1:
        if data.get("contains_rows") is not False:
            raise ValueError("development identity unit must remain row-free")
        return cls(
            split_role=str(data.get("split_role", "")),
            unit_id=str(data.get("unit_id", "")),
            schema_version=str(data.get("schema_version", "")),
            **_identity_unit_from_dict(data),
        )


@dataclass(frozen=True, slots=True)
class ProtectedReleaseHoldoutWindowV1:
    """One whole, row-free protected release-holdout split unit."""

    period: str
    start_ns: int
    end_ns: int
    source_partition_ids: tuple[str, ...]
    source_hashes: Mapping[str, str]
    source_signature_sha256: str
    motif_signature_sha256: str
    context_signature_sha256: str
    source_neighbor_sketch: str
    motif_neighbor_sketch: str
    context_neighbor_sketch: str
    cohesion_group_ids: tuple[str, ...]
    anchor_neighborhood_ids: tuple[str, ...]
    context_event_ids: tuple[str, ...]
    symbol_event_counts: Mapping[str, int]
    epoch_stratum: str
    session_stratum: str
    event_stratum: str
    observation_scenario_id: str
    alignment_kind: str
    deficit_stratum: str
    window_id: str = ""
    schema_version: str = PROTECTED_RELEASE_HOLDOUT_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PROTECTED_RELEASE_HOLDOUT_WINDOW_SCHEMA_VERSION,
        )
        _validate_identity_unit(self)
        counts = {
            _required_text(key): _positive_int(value, f"{key} event count")
            for key, value in self.symbol_event_counts.items()
        }
        if not counts:
            raise ValueError("release-holdout event counts are empty")
        object.__setattr__(
            self, "symbol_event_counts", dict(sorted(counts.items()))
        )
        for name in (
            "epoch_stratum",
            "session_stratum",
            "event_stratum",
            "observation_scenario_id",
            "alignment_kind",
            "deficit_stratum",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        expected = _stable_id(
            "protected-release-holdout-window", self.payload()
        )
        if self.window_id and self.window_id != expected:
            raise ValueError(
                "protected release-holdout window identity differs"
            )
        object.__setattr__(self, "window_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split_role": "protected_release_holdout",
            **_identity_unit_payload(self),
            "symbol_event_counts": dict(self.symbol_event_counts),
            "epoch_stratum": self.epoch_stratum,
            "session_stratum": self.session_stratum,
            "event_stratum": self.event_stratum,
            "observation_scenario_id": self.observation_scenario_id,
            "alignment_kind": self.alignment_kind,
            "deficit_stratum": self.deficit_stratum,
            "contains_rows": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "window_id": self.window_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProtectedReleaseHoldoutWindowV1:
        if (
            data.get("split_role") != "protected_release_holdout"
            or data.get("contains_rows") is not False
        ):
            raise ValueError(
                "protected holdout window role or row policy differs"
            )
        return cls(
            symbol_event_counts={
                str(key): _strict_int(value, f"{key} event count")
                for key, value in _mapping(
                    data.get("symbol_event_counts")
                ).items()
            },
            epoch_stratum=str(data.get("epoch_stratum", "")),
            session_stratum=str(data.get("session_stratum", "")),
            event_stratum=str(data.get("event_stratum", "")),
            observation_scenario_id=str(
                data.get("observation_scenario_id", "")
            ),
            alignment_kind=str(data.get("alignment_kind", "")),
            deficit_stratum=str(data.get("deficit_stratum", "")),
            window_id=str(data.get("window_id", "")),
            schema_version=str(data.get("schema_version", "")),
            **_identity_unit_from_dict(data),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutLeakageAuditV1:
    """Exact, near-neighbor, temporal, and cohesion leakage audit."""

    policy_id: str
    holdout_window_ids: tuple[str, ...]
    development_unit_ids: tuple[str, ...]
    checked_pair_count: int
    finding_codes: tuple[str, ...]
    status: ReleaseHoldoutAuditStatus
    audit_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_LEAKAGE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_LEAKAGE_AUDIT_SCHEMA_VERSION,
        )
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        holdouts = _text_tuple(self.holdout_window_ids)
        development = _text_tuple(self.development_unit_ids)
        findings = _text_tuple(self.finding_codes, allow_empty=True)
        if not holdouts or not development:
            raise ValueError("release-holdout leakage audit scope is empty")
        if len(findings) > MAX_RELEASE_HOLDOUT_FINDINGS:
            raise ValueError("release-holdout leakage findings are unbounded")
        _nonnegative_int(self.checked_pair_count, "checked_pair_count")
        expected_status = (
            ReleaseHoldoutAuditStatus.FAIL
            if findings
            else ReleaseHoldoutAuditStatus.PASS
        )
        if self.status is not expected_status:
            raise ValueError("release-holdout leakage status differs")
        object.__setattr__(self, "holdout_window_ids", holdouts)
        object.__setattr__(self, "development_unit_ids", development)
        object.__setattr__(self, "finding_codes", findings)
        expected = _stable_id("release-holdout-leakage-audit", self.payload())
        if self.audit_id and self.audit_id != expected:
            raise ValueError("release-holdout leakage audit identity differs")
        object.__setattr__(self, "audit_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "holdout_window_ids": list(self.holdout_window_ids),
            "development_unit_ids": list(self.development_unit_ids),
            "checked_pair_count": self.checked_pair_count,
            "finding_codes": list(self.finding_codes),
            "status": self.status.value,
            "rows_inspected": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReleaseHoldoutLeakageAuditV1:
        if data.get("rows_inspected") is not False:
            raise ValueError("release-holdout leakage audit must be row-free")
        return cls(
            policy_id=str(data.get("policy_id", "")),
            holdout_window_ids=_string_tuple(data.get("holdout_window_ids")),
            development_unit_ids=_string_tuple(
                data.get("development_unit_ids")
            ),
            checked_pair_count=_strict_int(
                data.get("checked_pair_count"), "checked_pair_count"
            ),
            finding_codes=_string_tuple(data.get("finding_codes")),
            status=ReleaseHoldoutAuditStatus(str(data.get("status", ""))),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutCoverageAuditV1:
    """Predeclared power/coverage evidence without weakened split rules."""

    policy_id: str
    window_ids: tuple[str, ...]
    stratum_counts: Mapping[str, int]
    missing_strata: tuple[str, ...]
    status: ReleaseHoldoutAuditStatus
    audit_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_COVERAGE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_COVERAGE_AUDIT_SCHEMA_VERSION,
        )
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        windows = _text_tuple(self.window_ids)
        counts = {
            _required_text(key): _nonnegative_int(value, f"{key} count")
            for key, value in self.stratum_counts.items()
        }
        missing = _text_tuple(self.missing_strata, allow_empty=True)
        expected_status = (
            ReleaseHoldoutAuditStatus.INSUFFICIENT_EVIDENCE
            if missing
            else ReleaseHoldoutAuditStatus.PASS
        )
        if self.status is not expected_status:
            raise ValueError("release-holdout coverage status differs")
        object.__setattr__(self, "window_ids", windows)
        object.__setattr__(self, "stratum_counts", dict(sorted(counts.items())))
        object.__setattr__(self, "missing_strata", missing)
        expected = _stable_id("release-holdout-coverage-audit", self.payload())
        if self.audit_id and self.audit_id != expected:
            raise ValueError("release-holdout coverage audit identity differs")
        object.__setattr__(self, "audit_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "window_ids": list(self.window_ids),
            "stratum_counts": dict(self.stratum_counts),
            "missing_strata": list(self.missing_strata),
            "status": self.status.value,
            "split_rules_weakened": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseHoldoutCoverageAuditV1:
        if data.get("split_rules_weakened") is not False:
            raise ValueError("release-holdout coverage weakened split rules")
        return cls(
            policy_id=str(data.get("policy_id", "")),
            window_ids=_string_tuple(data.get("window_ids")),
            stratum_counts={
                str(key): _strict_int(value, f"{key} count")
                for key, value in _mapping(data.get("stratum_counts")).items()
            },
            missing_strata=_string_tuple(data.get("missing_strata")),
            status=ReleaseHoldoutAuditStatus(str(data.get("status", ""))),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProtectedReleaseHoldoutManifestV1:
    """Committed row-free identity of a fresh, still-sealed holdout."""

    policy: ReleaseHoldoutAccessPolicyV1
    windows: tuple[ProtectedReleaseHoldoutWindowV1, ...]
    development_reference_set_id: str
    selection_dossier_id: str
    selection_dossier_ref: ArtifactRef
    source_cutoff_ns: int
    claim_scope: str
    leakage_audit: ReleaseHoldoutLeakageAuditV1
    coverage_audit: ReleaseHoldoutCoverageAuditV1
    frozen_at_utc: str
    manifest_id: str = ""
    schema_version: str = PROTECTED_RELEASE_HOLDOUT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PROTECTED_RELEASE_HOLDOUT_MANIFEST_SCHEMA_VERSION,
        )
        if not isinstance(self.policy, ReleaseHoldoutAccessPolicyV1):
            raise TypeError("release-holdout manifest policy must use v1")
        windows = tuple(sorted(self.windows, key=lambda item: item.window_id))
        if (
            not windows
            or len(windows) > MAX_RELEASE_HOLDOUT_WINDOWS
            or len({item.window_id for item in windows}) != len(windows)
        ):
            raise ValueError("release-holdout manifest windows are invalid")
        if not isinstance(self.selection_dossier_ref, ArtifactRef):
            raise TypeError("release-holdout selection reference is invalid")
        if (
            self.selection_dossier_ref.metadata.get("dossier_id")
            != self.selection_dossier_id
        ):
            raise ValueError("release-holdout selection reference is stale")
        if self.leakage_audit.policy_id != self.policy.policy_id:
            raise ValueError("release-holdout leakage policy is stale")
        if self.coverage_audit.policy_id != self.policy.policy_id:
            raise ValueError("release-holdout coverage policy is stale")
        window_ids = tuple(item.window_id for item in windows)
        if set(self.leakage_audit.holdout_window_ids) != set(window_ids):
            raise ValueError("release-holdout leakage scope differs")
        if set(self.coverage_audit.window_ids) != set(window_ids):
            raise ValueError("release-holdout coverage scope differs")
        cutoff = _nonnegative_int(self.source_cutoff_ns, "source_cutoff_ns")
        if any(item.start_ns <= cutoff for item in windows):
            raise ValueError("release holdout is not after its source cutoff")
        object.__setattr__(self, "windows", windows)
        object.__setattr__(
            self,
            "development_reference_set_id",
            _required_text(self.development_reference_set_id),
        )
        object.__setattr__(
            self,
            "selection_dossier_id",
            _required_text(self.selection_dossier_id),
        )
        object.__setattr__(
            self, "claim_scope", _required_text(self.claim_scope)
        )
        object.__setattr__(
            self, "frozen_at_utc", _timestamp(self.frozen_at_utc)
        )
        expected = _stable_id(
            "protected-release-holdout-manifest", self.payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError(
                "protected release-holdout manifest identity differs"
            )
        object.__setattr__(self, "manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.to_dict(),
            "windows": [item.to_dict() for item in self.windows],
            "development_reference_set_id": self.development_reference_set_id,
            "selection_dossier_id": self.selection_dossier_id,
            "selection_dossier_ref": self.selection_dossier_ref.to_dict(),
            "source_cutoff_ns": self.source_cutoff_ns,
            "claim_scope": self.claim_scope,
            "leakage_audit": self.leakage_audit.to_dict(),
            "coverage_audit": self.coverage_audit.to_dict(),
            "frozen_at_utc": self.frozen_at_utc,
            "sealed": True,
            "results_opened": False,
            "candidate_identity_present": False,
            "contains_rows": False,
            "frozen_before_candidate_fit_or_evaluation": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProtectedReleaseHoldoutManifestV1:
        for name, expected in (
            ("sealed", True),
            ("results_opened", False),
            ("candidate_identity_present", False),
            ("contains_rows", False),
            ("frozen_before_candidate_fit_or_evaluation", True),
        ):
            if data.get(name) is not expected:
                raise ValueError(f"release-holdout manifest {name} differs")
        return cls(
            policy=ReleaseHoldoutAccessPolicyV1.from_dict(
                _mapping(data.get("policy"))
            ),
            windows=tuple(
                ProtectedReleaseHoldoutWindowV1.from_dict(_mapping(item))
                for item in _sequence(data.get("windows"))
            ),
            development_reference_set_id=str(
                data.get("development_reference_set_id", "")
            ),
            selection_dossier_id=str(data.get("selection_dossier_id", "")),
            selection_dossier_ref=ArtifactRef.from_dict(
                _mapping(data.get("selection_dossier_ref"))
            ),
            source_cutoff_ns=_strict_int(
                data.get("source_cutoff_ns"), "source_cutoff_ns"
            ),
            claim_scope=str(data.get("claim_scope", "")),
            leakage_audit=ReleaseHoldoutLeakageAuditV1.from_dict(
                _mapping(data.get("leakage_audit"))
            ),
            coverage_audit=ReleaseHoldoutCoverageAuditV1.from_dict(
                _mapping(data.get("coverage_audit"))
            ),
            frozen_at_utc=str(data.get("frozen_at_utc", "")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateFreezeV1:
    """Immutable candidate graph built solely from non-holdout roles."""

    manifest_id: str
    selection_dossier_id: str
    candidate_id: str
    stage_artifacts: Mapping[str, ArtifactRef]
    frozen_at_utc: str
    graph_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_FREEZE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RELEASE_CANDIDATE_FREEZE_SCHEMA_VERSION
        )
        for name in ("manifest_id", "selection_dossier_id", "candidate_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        artifacts = {
            _required_text(key): value
            for key, value in sorted(self.stage_artifacts.items())
        }
        if set(artifacts) != set(_FROZEN_STAGES) or any(
            not isinstance(ref, ArtifactRef) for ref in artifacts.values()
        ):
            raise ValueError("release candidate frozen stage graph differs")
        for stage, ref in artifacts.items():
            roles = ref.metadata.get("input_roles")
            if not isinstance(roles, list) or not roles:
                raise ValueError(
                    f"release candidate {stage} input roles absent"
                )
            role_set = {_required_text(str(role)) for role in roles}
            if not role_set <= _ALLOWED_INPUT_ROLES:
                raise ValueError(
                    f"release candidate {stage} used holdout input"
                )
        object.__setattr__(self, "stage_artifacts", artifacts)
        object.__setattr__(
            self, "frozen_at_utc", _timestamp(self.frozen_at_utc)
        )
        expected = _stable_id("release-candidate-freeze", self.payload())
        if self.graph_id and self.graph_id != expected:
            raise ValueError("release candidate frozen graph identity differs")
        object.__setattr__(self, "graph_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "selection_dossier_id": self.selection_dossier_id,
            "candidate_id": self.candidate_id,
            "stage_artifacts": {
                key: value.to_dict()
                for key, value in self.stage_artifacts.items()
            },
            "frozen_at_utc": self.frozen_at_utc,
            "candidate_reproduced_without_holdout_results": True,
            "candidate_selected_without_holdout_results": True,
            "holdout_input_role": False,
            "stage_order": list(_FROZEN_STAGES),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "graph_id": self.graph_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReleaseCandidateFreezeV1:
        for name, expected in (
            ("candidate_reproduced_without_holdout_results", True),
            ("candidate_selected_without_holdout_results", True),
            ("holdout_input_role", False),
        ):
            if data.get(name) is not expected:
                raise ValueError(f"release candidate freeze {name} differs")
        if data.get("stage_order") != list(_FROZEN_STAGES):
            raise ValueError("release candidate frozen stage order differs")
        return cls(
            manifest_id=str(data.get("manifest_id", "")),
            selection_dossier_id=str(data.get("selection_dossier_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            stage_artifacts={
                str(key): ArtifactRef.from_dict(_mapping(value))
                for key, value in _mapping(data.get("stage_artifacts")).items()
            },
            frozen_at_utc=str(data.get("frozen_at_utc", "")),
            graph_id=str(data.get("graph_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutAuthorizationV1:
    """Authorization to open a holdout under one frozen graph exactly once."""

    manifest_id: str
    manifest_ref: ArtifactRef
    graph_id: str
    graph_ref: ArtifactRef
    selection_dossier_id: str
    candidate_id: str
    authorized_at_utc: str
    authorization_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION,
        )
        for name in (
            "manifest_id",
            "graph_id",
            "selection_dossier_id",
            "candidate_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.manifest_ref, ArtifactRef) or not isinstance(
            self.graph_ref, ArtifactRef
        ):
            raise TypeError(
                "release-holdout authorization references are invalid"
            )
        object.__setattr__(
            self, "authorized_at_utc", _timestamp(self.authorized_at_utc)
        )
        expected = _stable_id("release-holdout-authorization", self.payload())
        if self.authorization_id and self.authorization_id != expected:
            raise ValueError("release-holdout authorization identity differs")
        object.__setattr__(self, "authorization_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "manifest_ref": self.manifest_ref.to_dict(),
            "graph_id": self.graph_id,
            "graph_ref": self.graph_ref.to_dict(),
            "selection_dossier_id": self.selection_dossier_id,
            "candidate_id": self.candidate_id,
            "authorized_at_utc": self.authorized_at_utc,
            "maximum_evaluations": 1,
            "all_policies_immutable": True,
            "candidate_selected_without_holdout_results": True,
            "holdout_results_inaccessible_before_authorization": True,
            "holdout_selection_role": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "authorization_id": self.authorization_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseHoldoutAuthorizationV1:
        expected = {
            "maximum_evaluations": 1,
            "all_policies_immutable": True,
            "candidate_selected_without_holdout_results": True,
            "holdout_results_inaccessible_before_authorization": True,
            "holdout_selection_role": False,
        }
        if any(data.get(key) != value for key, value in expected.items()):
            raise ValueError("release-holdout authorization policy differs")
        return cls(
            manifest_id=str(data.get("manifest_id", "")),
            manifest_ref=ArtifactRef.from_dict(
                _mapping(data.get("manifest_ref"))
            ),
            graph_id=str(data.get("graph_id", "")),
            graph_ref=ArtifactRef.from_dict(_mapping(data.get("graph_ref"))),
            selection_dossier_id=str(data.get("selection_dossier_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            authorized_at_utc=str(data.get("authorized_at_utc", "")),
            authorization_id=str(data.get("authorization_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutEvaluationResultV1:
    """External row-free report returned by the one-time evaluator."""

    manifest_id: str
    graph_id: str
    candidate_id: str
    outcome: ReleaseHoldoutEvaluationOutcome
    report_ref: ArtifactRef
    reason_codes: tuple[str, ...] = ()
    result_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_EVALUATION_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_EVALUATION_RESULT_SCHEMA_VERSION,
        )
        if self.outcome is ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE:
            raise ValueError("operational failure is recorded by the executor")
        for name in ("manifest_id", "graph_id", "candidate_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.report_ref, ArtifactRef):
            raise TypeError("release-holdout evaluation report ref is invalid")
        object.__setattr__(
            self,
            "reason_codes",
            _text_tuple(self.reason_codes, allow_empty=True),
        )
        expected = _stable_id(
            "release-holdout-evaluation-result", self.payload()
        )
        if self.result_id and self.result_id != expected:
            raise ValueError(
                "release-holdout evaluation result identity differs"
            )
        object.__setattr__(self, "result_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "graph_id": self.graph_id,
            "candidate_id": self.candidate_id,
            "outcome": self.outcome.value,
            "report_ref": self.report_ref.to_dict(),
            "reason_codes": list(self.reason_codes),
            "evaluation_number": 1,
            "holdout_selection_role": False,
            "candidate_mutation_permitted": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseHoldoutEvaluationResultV1:
        if (
            data.get("evaluation_number") != 1
            or data.get("holdout_selection_role") is not False
            or data.get("candidate_mutation_permitted") is not False
        ):
            raise ValueError("release-holdout evaluation result policy differs")
        return cls(
            manifest_id=str(data.get("manifest_id", "")),
            graph_id=str(data.get("graph_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            outcome=ReleaseHoldoutEvaluationOutcome(
                str(data.get("outcome", ""))
            ),
            report_ref=ArtifactRef.from_dict(_mapping(data.get("report_ref"))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutEvaluationReceiptV1:
    """Durable proof that the one authorized evaluation was consumed."""

    authorization_id: str
    manifest_id: str
    graph_id: str
    candidate_id: str
    outcome: ReleaseHoldoutEvaluationOutcome
    evaluated_at_utc: str
    result_id: str = ""
    report_ref: ArtifactRef | None = None
    reason_codes: tuple[str, ...] = ()
    operational_error_type: str = ""
    receipt_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_EVALUATION_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_EVALUATION_RECEIPT_SCHEMA_VERSION,
        )
        for name in (
            "authorization_id",
            "manifest_id",
            "graph_id",
            "candidate_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        operational = (
            self.outcome is ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE
        )
        if operational:
            if self.report_ref is not None or self.result_id:
                raise ValueError(
                    "operational holdout failure cannot claim a report"
                )
            object.__setattr__(
                self,
                "operational_error_type",
                _required_text(self.operational_error_type),
            )
        elif not isinstance(self.report_ref, ArtifactRef) or not self.result_id:
            raise ValueError(
                "release-holdout receipt report evidence is absent"
            )
        elif self.operational_error_type:
            raise ValueError(
                "successful evaluator receipt has operational error"
            )
        object.__setattr__(
            self, "evaluated_at_utc", _timestamp(self.evaluated_at_utc)
        )
        object.__setattr__(
            self,
            "reason_codes",
            _text_tuple(self.reason_codes, allow_empty=True),
        )
        expected = _stable_id(
            "release-holdout-evaluation-receipt", self.payload()
        )
        if self.receipt_id and self.receipt_id != expected:
            raise ValueError(
                "release-holdout evaluation receipt identity differs"
            )
        object.__setattr__(self, "receipt_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "authorization_id": self.authorization_id,
            "manifest_id": self.manifest_id,
            "graph_id": self.graph_id,
            "candidate_id": self.candidate_id,
            "outcome": self.outcome.value,
            "evaluated_at_utc": self.evaluated_at_utc,
            "result_id": self.result_id,
            "report_ref": (
                self.report_ref.to_dict()
                if self.report_ref is not None
                else None
            ),
            "reason_codes": list(self.reason_codes),
            "operational_error_type": self.operational_error_type,
            "evaluation_number": 1,
            "holdout_selection_role": False,
            "holdout_consumed": True,
            "retry_permitted": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "receipt_id": self.receipt_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseHoldoutEvaluationReceiptV1:
        expected = {
            "evaluation_number": 1,
            "holdout_selection_role": False,
            "holdout_consumed": True,
            "retry_permitted": False,
        }
        if any(data.get(key) != value for key, value in expected.items()):
            raise ValueError("release-holdout receipt policy differs")
        raw_ref = data.get("report_ref")
        return cls(
            authorization_id=str(data.get("authorization_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            graph_id=str(data.get("graph_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            outcome=ReleaseHoldoutEvaluationOutcome(
                str(data.get("outcome", ""))
            ),
            evaluated_at_utc=str(data.get("evaluated_at_utc", "")),
            result_id=str(data.get("result_id", "")),
            report_ref=(
                ArtifactRef.from_dict(_mapping(raw_ref))
                if raw_ref is not None
                else None
            ),
            reason_codes=_string_tuple(data.get("reason_codes")),
            operational_error_type=str(data.get("operational_error_type", "")),
            receipt_id=str(data.get("receipt_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutRetirementMarkerV1:
    """Permanent retirement and next-release governance marker."""

    manifest_id: str
    receipt_id: str
    outcome: ReleaseHoldoutEvaluationOutcome
    retired_at_utc: str
    successor_manifest_id: str = ""
    marker_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_RETIREMENT_MARKER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_RETIREMENT_MARKER_SCHEMA_VERSION,
        )
        object.__setattr__(
            self, "manifest_id", _required_text(self.manifest_id)
        )
        object.__setattr__(self, "receipt_id", _required_text(self.receipt_id))
        object.__setattr__(
            self, "retired_at_utc", _timestamp(self.retired_at_utc)
        )
        non_pass = self.outcome is not ReleaseHoldoutEvaluationOutcome.PASSED
        if non_pass or self.successor_manifest_id:
            object.__setattr__(
                self,
                "successor_manifest_id",
                _required_text(self.successor_manifest_id),
            )
        expected = _stable_id(
            "release-holdout-retirement-marker", self.payload()
        )
        if self.marker_id and self.marker_id != expected:
            raise ValueError(
                "release-holdout retirement marker identity differs"
            )
        object.__setattr__(self, "marker_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "receipt_id": self.receipt_id,
            "outcome": self.outcome.value,
            "retired_at_utc": self.retired_at_utc,
            "successor_manifest_id": self.successor_manifest_id,
            "retired": True,
            "reuse_permitted": False,
            "next_release_requires_fresh_manifest": True,
            "same_holdout_tuning_permitted": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "marker_id": self.marker_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseHoldoutRetirementMarkerV1:
        expected = {
            "retired": True,
            "reuse_permitted": False,
            "next_release_requires_fresh_manifest": True,
            "same_holdout_tuning_permitted": False,
        }
        if any(data.get(key) != value for key, value in expected.items()):
            raise ValueError("release-holdout retirement policy differs")
        return cls(
            manifest_id=str(data.get("manifest_id", "")),
            receipt_id=str(data.get("receipt_id", "")),
            outcome=ReleaseHoldoutEvaluationOutcome(
                str(data.get("outcome", ""))
            ),
            retired_at_utc=str(data.get("retired_at_utc", "")),
            successor_manifest_id=str(data.get("successor_manifest_id", "")),
            marker_id=str(data.get("marker_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def audit_release_holdout_leakage(
    policy: ReleaseHoldoutAccessPolicyV1,
    windows: Sequence[ProtectedReleaseHoldoutWindowV1],
    development_units: Sequence[ReleaseHoldoutDevelopmentUnitV1],
) -> ReleaseHoldoutLeakageAuditV1:
    """Audit exact, fuzzy, temporal, and grouped identity separation."""
    holdouts = tuple(windows)
    development = tuple(development_units)
    if not holdouts or not development:
        raise ValueError("release-holdout leakage audit scope is empty")
    if (
        len(holdouts) > MAX_RELEASE_HOLDOUT_WINDOWS
        or len(development) > MAX_RELEASE_HOLDOUT_DEVELOPMENT_UNITS
    ):
        raise ValueError("release-holdout leakage audit scope is unbounded")
    findings: set[str] = set()
    pairs = 0
    for window in holdouts:
        for unit in development:
            pairs += 1
            prefix = f"{window.window_id}|{unit.unit_id}"
            _audit_pair(policy, window, unit, prefix, findings)
    for index, left in enumerate(holdouts):
        for right in holdouts[index + 1 :]:
            pairs += 1
            prefix = f"{left.window_id}|{right.window_id}"
            distance = _interval_distance(left, right)
            if distance == 0:
                findings.add(f"holdout_window_overlap:{prefix}")
            elif distance <= policy.temporal_neighbor_guard_ns:
                findings.add(f"holdout_temporal_neighbor:{prefix}")
            if set(left.source_partition_ids) & set(right.source_partition_ids):
                findings.add(f"holdout_source_partition_reuse:{prefix}")
            if set(left.source_hashes.values()) & set(
                right.source_hashes.values()
            ):
                findings.add(f"holdout_source_hash_reuse:{prefix}")
            for kind in ("source", "motif", "context"):
                signature = getattr(left, f"{kind}_signature_sha256")
                other_signature = getattr(right, f"{kind}_signature_sha256")
                if signature == other_signature:
                    findings.add(f"holdout_exact_{kind}_duplicate:{prefix}")
                else:
                    sketch = getattr(left, f"{kind}_neighbor_sketch")
                    other_sketch = getattr(right, f"{kind}_neighbor_sketch")
                    if _hamming_distance(sketch, other_sketch) <= (
                        policy.near_neighbor_hamming_distance
                    ):
                        findings.add(f"holdout_near_{kind}_duplicate:{prefix}")
            _audit_shared_groups(left, right, prefix, findings, internal=True)
    finding_codes = tuple(sorted(findings))
    return ReleaseHoldoutLeakageAuditV1(
        policy_id=policy.policy_id,
        holdout_window_ids=tuple(item.window_id for item in holdouts),
        development_unit_ids=tuple(item.unit_id for item in development),
        checked_pair_count=pairs,
        finding_codes=finding_codes,
        status=(
            ReleaseHoldoutAuditStatus.FAIL
            if finding_codes
            else ReleaseHoldoutAuditStatus.PASS
        ),
    )


def audit_release_holdout_coverage(
    policy: ReleaseHoldoutAccessPolicyV1,
    windows: Sequence[ProtectedReleaseHoldoutWindowV1],
) -> ReleaseHoldoutCoverageAuditV1:
    """Audit all predeclared coverage axes without relaxing split rules."""
    selected = tuple(windows)
    axes = {
        "feed_epoch": (
            policy.required_feed_epochs,
            tuple(item.epoch_stratum for item in selected),
        ),
        "session": (
            policy.required_sessions,
            tuple(item.session_stratum for item in selected),
        ),
        "event": (
            policy.required_event_strata,
            tuple(item.event_stratum for item in selected),
        ),
        "observation_scenario": (
            policy.required_observation_scenarios,
            tuple(item.observation_scenario_id for item in selected),
        ),
        "alignment": (
            policy.required_alignment_kinds,
            tuple(item.alignment_kind for item in selected),
        ),
        "deficit": (
            policy.required_deficit_strata,
            tuple(item.deficit_stratum for item in selected),
        ),
    }
    counts: dict[str, int] = {}
    missing: list[str] = []
    for axis, (required, observed) in axes.items():
        tally = Counter(observed)
        for value in required:
            key = f"{axis}:{value}"
            counts[key] = tally[value]
            if tally[value] == 0:
                missing.append(key)
        missing.extend(
            f"unexpected:{axis}:{value}"
            for value in sorted(set(observed) - set(required))
        )
    return ReleaseHoldoutCoverageAuditV1(
        policy_id=policy.policy_id,
        window_ids=tuple(item.window_id for item in selected),
        stratum_counts=counts,
        missing_strata=tuple(missing),
        status=(
            ReleaseHoldoutAuditStatus.INSUFFICIENT_EVIDENCE
            if missing
            else ReleaseHoldoutAuditStatus.PASS
        ),
    )


def build_protected_release_holdout_manifest(
    policy: ReleaseHoldoutAccessPolicyV1,
    windows: Sequence[ProtectedReleaseHoldoutWindowV1],
    development_units: Sequence[ReleaseHoldoutDevelopmentUnitV1],
    *,
    selection_dossier_id: str,
    selection_dossier_ref: ArtifactRef,
    source_cutoff_ns: int,
    claim_scope: str,
    frozen_at_utc: str,
) -> ProtectedReleaseHoldoutManifestV1:
    """Freeze a fresh row-free manifest before any candidate is fitted."""
    selected = tuple(windows)
    development = tuple(development_units)
    unit_ids: list[JSONValue] = []
    for item in sorted(development, key=lambda value: value.unit_id):
        unit_ids.append(item.unit_id)
    reference_set_id = _stable_id(
        "release-holdout-development-reference-set",
        {"unit_ids": unit_ids},
    )
    return ProtectedReleaseHoldoutManifestV1(
        policy=policy,
        windows=selected,
        development_reference_set_id=reference_set_id,
        selection_dossier_id=selection_dossier_id,
        selection_dossier_ref=selection_dossier_ref,
        source_cutoff_ns=source_cutoff_ns,
        claim_scope=claim_scope,
        leakage_audit=audit_release_holdout_leakage(
            policy, selected, development
        ),
        coverage_audit=audit_release_holdout_coverage(policy, selected),
        frozen_at_utc=frozen_at_utc,
    )


def freeze_release_candidate(
    manifest: ProtectedReleaseHoldoutManifestV1,
    *,
    candidate_id: str,
    stage_artifacts: Mapping[str, ArtifactRef],
    frozen_at_utc: str,
) -> ReleaseCandidateFreezeV1:
    """Freeze and verify every development-only candidate graph stage."""
    if _timestamp_value(frozen_at_utc) <= _timestamp_value(
        manifest.frozen_at_utc
    ):
        raise ValueError("release candidate graph predates holdout manifest")
    for ref in stage_artifacts.values():
        _verify_artifact_ref(ref)
    return ReleaseCandidateFreezeV1(
        manifest_id=manifest.manifest_id,
        selection_dossier_id=manifest.selection_dossier_id,
        candidate_id=candidate_id,
        stage_artifacts=stage_artifacts,
        frozen_at_utc=frozen_at_utc,
    )


def authorize_release_holdout(
    manifest_ref: ArtifactRef,
    graph_ref: ArtifactRef,
    *,
    authorized_at_utc: str,
) -> ReleaseHoldoutAuthorizationV1:
    """Authorize one evaluation only after all fail-closed checks pass."""
    _verify_artifact_ref(manifest_ref)
    _verify_artifact_ref(graph_ref)
    manifest = read_protected_release_holdout_manifest(manifest_ref.path)
    graph = read_release_candidate_freeze(graph_ref.path)
    _validate_authorization_inputs(manifest, graph, authorized_at_utc)
    return ReleaseHoldoutAuthorizationV1(
        manifest_id=manifest.manifest_id,
        manifest_ref=manifest_ref,
        graph_id=graph.graph_id,
        graph_ref=graph_ref,
        selection_dossier_id=manifest.selection_dossier_id,
        candidate_id=graph.candidate_id,
        authorized_at_utc=authorized_at_utc,
    )


def _validate_authorization_inputs(
    manifest: ProtectedReleaseHoldoutManifestV1,
    graph: ReleaseCandidateFreezeV1,
    authorized_at_utc: str,
) -> None:
    _verify_artifact_ref(manifest.selection_dossier_ref)
    for ref in graph.stage_artifacts.values():
        _verify_artifact_ref(ref)
    if manifest.leakage_audit.status is not ReleaseHoldoutAuditStatus.PASS:
        raise ValueError("release holdout has leakage findings")
    if manifest.coverage_audit.status is not ReleaseHoldoutAuditStatus.PASS:
        raise ValueError("release holdout has insufficient coverage")
    if graph.manifest_id != manifest.manifest_id:
        raise ValueError(
            "release candidate graph uses another holdout manifest"
        )
    if graph.selection_dossier_id != manifest.selection_dossier_id:
        raise ValueError("release candidate graph selection dossier is stale")
    authorized = _timestamp_value(authorized_at_utc)
    if _timestamp_value(graph.frozen_at_utc) <= _timestamp_value(
        manifest.frozen_at_utc
    ):
        raise ValueError("release candidate graph predates holdout manifest")
    if authorized <= _timestamp_value(graph.frozen_at_utc):
        raise ValueError("release-holdout authorization predates frozen graph")


def execute_release_holdout_once(
    authorization: ReleaseHoldoutAuthorizationV1,
    state_directory: str | Path,
    evaluator: Callable[
        [ProtectedReleaseHoldoutManifestV1, ReleaseCandidateFreezeV1],
        ReleaseHoldoutEvaluationResultV1,
    ],
    *,
    evaluated_at_utc: str,
) -> tuple[ReleaseHoldoutEvaluationReceiptV1, ArtifactRef]:
    """Consume the holdout before invoking its sole evaluation callback."""
    _verify_artifact_ref(authorization.manifest_ref)
    _verify_artifact_ref(authorization.graph_ref)
    manifest = read_protected_release_holdout_manifest(
        authorization.manifest_ref.path
    )
    graph = read_release_candidate_freeze(authorization.graph_ref.path)
    _validate_authorization_inputs(
        manifest, graph, authorization.authorized_at_utc
    )
    if (
        authorization.manifest_id != manifest.manifest_id
        or authorization.graph_id != graph.graph_id
        or authorization.candidate_id != graph.candidate_id
        or authorization.selection_dossier_id != manifest.selection_dossier_id
    ):
        raise ValueError("release-holdout authorization binding differs")
    root = Path(state_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(manifest.manifest_id.encode("utf-8")).hexdigest()
    state_path = root / f"release-holdout-access-{key}.json"
    reservation = (
        canonical_contract_json(
            {
                "authorization_id": authorization.authorization_id,
                "manifest_id": manifest.manifest_id,
                "state": "opened-and-consumed",
            }
        ).encode("utf-8")
        + b"\n"
    )
    _reserve_once(state_path, reservation)
    try:
        result = evaluator(manifest, graph)
        if not isinstance(result, ReleaseHoldoutEvaluationResultV1):
            raise TypeError("release-holdout evaluator returned another type")
        if (
            result.manifest_id != manifest.manifest_id
            or result.graph_id != graph.graph_id
            or result.candidate_id != graph.candidate_id
        ):
            raise ValueError(
                "release-holdout evaluation result binding differs"
            )
        _verify_artifact_ref(result.report_ref)
        receipt = ReleaseHoldoutEvaluationReceiptV1(
            authorization_id=authorization.authorization_id,
            manifest_id=manifest.manifest_id,
            graph_id=graph.graph_id,
            candidate_id=graph.candidate_id,
            outcome=result.outcome,
            evaluated_at_utc=evaluated_at_utc,
            result_id=result.result_id,
            report_ref=result.report_ref,
            reason_codes=result.reason_codes,
        )
    except Exception as error:  # noqa: BLE001 - opened holdout stays consumed
        receipt = ReleaseHoldoutEvaluationReceiptV1(
            authorization_id=authorization.authorization_id,
            manifest_id=manifest.manifest_id,
            graph_id=graph.graph_id,
            candidate_id=graph.candidate_id,
            outcome=ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE,
            evaluated_at_utc=evaluated_at_utc,
            reason_codes=("evaluation_callback_failed",),
            operational_error_type=type(error).__name__,
        )
    receipt_ref = write_release_holdout_evaluation_receipt(receipt, root)
    final_state = (
        canonical_contract_json(
            {
                "authorization_id": authorization.authorization_id,
                "manifest_id": manifest.manifest_id,
                "receipt_ref": receipt_ref.to_dict(),
                "state": "retirement-required",
            }
        ).encode("utf-8")
        + b"\n"
    )
    _atomic_replace(state_path, final_state)
    return receipt, receipt_ref


def retire_release_holdout(
    manifest: ProtectedReleaseHoldoutManifestV1,
    receipt: ReleaseHoldoutEvaluationReceiptV1,
    *,
    retired_at_utc: str,
    successor_manifest_id: str = "",
) -> ReleaseHoldoutRetirementMarkerV1:
    """Retire one consumed holdout; non-passes require a fresh successor."""
    if receipt.manifest_id != manifest.manifest_id:
        raise ValueError("release-holdout retirement receipt is stale")
    if receipt.report_ref is not None:
        _verify_artifact_ref(receipt.report_ref)
    return ReleaseHoldoutRetirementMarkerV1(
        manifest_id=manifest.manifest_id,
        receipt_id=receipt.receipt_id,
        outcome=receipt.outcome,
        retired_at_utc=retired_at_utc,
        successor_manifest_id=successor_manifest_id,
    )


def write_release_holdout_access_policy(
    policy: ReleaseHoldoutAccessPolicyV1, output_directory: str | Path
) -> ArtifactRef:
    return _write_contract(
        policy.to_json(),
        output_directory,
        prefix="release-holdout-access-policy",
        kind="release_holdout_access_policy_v1",
        metadata={"policy_id": policy.policy_id},
    )


def read_release_holdout_access_policy(
    path: str | Path,
) -> ReleaseHoldoutAccessPolicyV1:
    return ReleaseHoldoutAccessPolicyV1.from_dict(
        _read_contract(path, "release-holdout-access-policy")
    )


def write_protected_release_holdout_manifest(
    manifest: ProtectedReleaseHoldoutManifestV1,
    output_directory: str | Path,
) -> ArtifactRef:
    return _write_contract(
        manifest.to_json(),
        output_directory,
        prefix="protected-release-holdout-manifest",
        kind="protected_release_holdout_manifest_v1",
        metadata={"manifest_id": manifest.manifest_id},
    )


def read_protected_release_holdout_manifest(
    path: str | Path,
) -> ProtectedReleaseHoldoutManifestV1:
    return ProtectedReleaseHoldoutManifestV1.from_dict(
        _read_contract(path, "protected-release-holdout-manifest")
    )


def write_release_candidate_freeze(
    graph: ReleaseCandidateFreezeV1, output_directory: str | Path
) -> ArtifactRef:
    return _write_contract(
        graph.to_json(),
        output_directory,
        prefix="release-candidate-freeze",
        kind="release_candidate_freeze_v1",
        metadata={"graph_id": graph.graph_id},
    )


def read_release_candidate_freeze(
    path: str | Path,
) -> ReleaseCandidateFreezeV1:
    return ReleaseCandidateFreezeV1.from_dict(
        _read_contract(path, "release-candidate-freeze")
    )


def write_release_holdout_authorization(
    authorization: ReleaseHoldoutAuthorizationV1,
    output_directory: str | Path,
) -> ArtifactRef:
    return _write_contract(
        authorization.to_json(),
        output_directory,
        prefix="release-holdout-authorization",
        kind="release_holdout_authorization_v1",
        metadata={"authorization_id": authorization.authorization_id},
    )


def read_release_holdout_authorization(
    path: str | Path,
) -> ReleaseHoldoutAuthorizationV1:
    return ReleaseHoldoutAuthorizationV1.from_dict(
        _read_contract(path, "release-holdout-authorization")
    )


def write_release_holdout_evaluation_receipt(
    receipt: ReleaseHoldoutEvaluationReceiptV1,
    output_directory: str | Path,
) -> ArtifactRef:
    return _write_contract(
        receipt.to_json(),
        output_directory,
        prefix="release-holdout-evaluation-receipt",
        kind="release_holdout_evaluation_receipt_v1",
        metadata={"receipt_id": receipt.receipt_id},
    )


def read_release_holdout_evaluation_receipt(
    path: str | Path,
) -> ReleaseHoldoutEvaluationReceiptV1:
    return ReleaseHoldoutEvaluationReceiptV1.from_dict(
        _read_contract(path, "release-holdout-evaluation-receipt")
    )


def write_release_holdout_retirement_marker(
    marker: ReleaseHoldoutRetirementMarkerV1,
    output_directory: str | Path,
) -> ArtifactRef:
    return _write_contract(
        marker.to_json(),
        output_directory,
        prefix="release-holdout-retirement-marker",
        kind="release_holdout_retirement_marker_v1",
        metadata={"marker_id": marker.marker_id},
    )


def read_release_holdout_retirement_marker(
    path: str | Path,
) -> ReleaseHoldoutRetirementMarkerV1:
    return ReleaseHoldoutRetirementMarkerV1.from_dict(
        _read_contract(path, "release-holdout-retirement-marker")
    )


def _audit_pair(
    policy: ReleaseHoldoutAccessPolicyV1,
    window: ProtectedReleaseHoldoutWindowV1,
    unit: ReleaseHoldoutDevelopmentUnitV1,
    prefix: str,
    findings: set[str],
) -> None:
    if set(window.source_partition_ids) & set(unit.source_partition_ids):
        findings.add(f"source_partition_reuse:{prefix}")
    if set(window.source_hashes.values()) & set(unit.source_hashes.values()):
        findings.add(f"source_hash_reuse:{prefix}")
    for kind in ("source", "motif", "context"):
        signature = getattr(window, f"{kind}_signature_sha256")
        other_signature = getattr(unit, f"{kind}_signature_sha256")
        if signature == other_signature:
            findings.add(f"exact_{kind}_duplicate:{prefix}")
        else:
            sketch = getattr(window, f"{kind}_neighbor_sketch")
            other_sketch = getattr(unit, f"{kind}_neighbor_sketch")
            if _hamming_distance(sketch, other_sketch) <= (
                policy.near_neighbor_hamming_distance
            ):
                findings.add(f"near_{kind}_duplicate:{prefix}")
    distance = _interval_distance(window, unit)
    if distance == 0:
        findings.add(f"temporal_overlap:{prefix}")
    elif distance <= policy.temporal_neighbor_guard_ns:
        findings.add(f"temporal_neighbor:{prefix}")
    if window.start_ns <= unit.end_ns:
        findings.add(f"chronological_block_violation:{prefix}")
    _audit_shared_groups(window, unit, prefix, findings, internal=False)


def _audit_shared_groups(
    left: Any,
    right: Any,
    prefix: str,
    findings: set[str],
    *,
    internal: bool,
) -> None:
    scope = "holdout" if internal else "cross_role"
    for field_name, label in (
        ("cohesion_group_ids", "cohesion_group"),
        ("anchor_neighborhood_ids", "anchor_neighborhood"),
        ("context_event_ids", "context_event"),
    ):
        if set(getattr(left, field_name)) & set(getattr(right, field_name)):
            findings.add(f"{scope}_{label}_reuse:{prefix}")


def _interval_distance(left: Any, right: Any) -> int:
    if left.end_ns <= right.start_ns:
        return int(right.start_ns - left.end_ns)
    if right.end_ns <= left.start_ns:
        return int(left.start_ns - right.end_ns)
    return 0


def _hamming_distance(left: str, right: str) -> int:
    return (int(left, 16) ^ int(right, 16)).bit_count()


def _validate_identity_unit(value: Any) -> None:
    if not _PERIOD.fullmatch(value.period):
        raise ValueError("release-holdout period must use YYYYMM")
    start = _nonnegative_int(value.start_ns, "start_ns")
    end = _positive_int(value.end_ns, "end_ns")
    if end <= start:
        raise ValueError("release-holdout interval is invalid")
    partitions = _text_tuple(value.source_partition_ids)
    hashes = {
        _required_text(key): _sha256(item, f"{key} source hash")
        for key, item in value.source_hashes.items()
    }
    if not partitions or not hashes:
        raise ValueError("release-holdout source identity is empty")
    object.__setattr__(value, "source_partition_ids", partitions)
    object.__setattr__(value, "source_hashes", dict(sorted(hashes.items())))
    for name in (
        "source_signature_sha256",
        "motif_signature_sha256",
        "context_signature_sha256",
    ):
        object.__setattr__(value, name, _sha256(getattr(value, name), name))
    for name in (
        "source_neighbor_sketch",
        "motif_neighbor_sketch",
        "context_neighbor_sketch",
    ):
        object.__setattr__(value, name, _sketch(getattr(value, name), name))
    for name in (
        "cohesion_group_ids",
        "anchor_neighborhood_ids",
        "context_event_ids",
    ):
        object.__setattr__(value, name, _text_tuple(getattr(value, name)))


def _identity_unit_payload(value: Any) -> dict[str, JSONValue]:
    return {
        "period": value.period,
        "start_ns": value.start_ns,
        "end_ns": value.end_ns,
        "source_partition_ids": list(value.source_partition_ids),
        "source_hashes": dict(value.source_hashes),
        "source_signature_sha256": value.source_signature_sha256,
        "motif_signature_sha256": value.motif_signature_sha256,
        "context_signature_sha256": value.context_signature_sha256,
        "source_neighbor_sketch": value.source_neighbor_sketch,
        "motif_neighbor_sketch": value.motif_neighbor_sketch,
        "context_neighbor_sketch": value.context_neighbor_sketch,
        "cohesion_group_ids": list(value.cohesion_group_ids),
        "anchor_neighborhood_ids": list(value.anchor_neighborhood_ids),
        "context_event_ids": list(value.context_event_ids),
    }


def _identity_unit_from_dict(data: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "period": str(data.get("period", "")),
        "start_ns": _strict_int(data.get("start_ns"), "start_ns"),
        "end_ns": _strict_int(data.get("end_ns"), "end_ns"),
        "source_partition_ids": _string_tuple(data.get("source_partition_ids")),
        "source_hashes": {
            str(key): str(value)
            for key, value in _mapping(data.get("source_hashes")).items()
        },
        "source_signature_sha256": str(data.get("source_signature_sha256", "")),
        "motif_signature_sha256": str(data.get("motif_signature_sha256", "")),
        "context_signature_sha256": str(
            data.get("context_signature_sha256", "")
        ),
        "source_neighbor_sketch": str(data.get("source_neighbor_sketch", "")),
        "motif_neighbor_sketch": str(data.get("motif_neighbor_sketch", "")),
        "context_neighbor_sketch": str(data.get("context_neighbor_sketch", "")),
        "cohesion_group_ids": _string_tuple(data.get("cohesion_group_ids")),
        "anchor_neighborhood_ids": _string_tuple(
            data.get("anchor_neighborhood_ids")
        ),
        "context_event_ids": _string_tuple(data.get("context_event_ids")),
    }


def _verify_artifact_ref(ref: ArtifactRef) -> Path:
    if not isinstance(ref, ArtifactRef):
        raise TypeError("release-holdout strong reference must use ArtifactRef")
    path = Path(ref.path).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"release-holdout artifact is missing: {path}")
    if ref.size_bytes is None or path.stat().st_size != ref.size_bytes:
        raise ValueError("release-holdout artifact size differs")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if not ref.sha256 or digest != ref.sha256:
        raise ValueError("release-holdout artifact SHA-256 differs")
    return path


def _write_contract(
    text: str,
    output_directory: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    payload = text.encode("utf-8") + b"\n"
    if len(payload) > MAX_RELEASE_HOLDOUT_ARTIFACT_BYTES:
        raise ValueError("release-holdout artifact exceeds byte bound")
    digest = hashlib.sha256(payload).hexdigest()
    path = root / f"{prefix}-{digest}.json"
    _write_once(path, payload)
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=len(payload),
        sha256=digest,
        metadata=dict(metadata),
    )


def _read_contract(path: str | Path, prefix: str) -> Mapping[str, Any]:
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > MAX_RELEASE_HOLDOUT_ARTIFACT_BYTES:
        raise ValueError("release-holdout artifact exceeds byte bound")
    payload = source.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if source.name != f"{prefix}-{digest}.json":
        raise ValueError("release-holdout content address differs")
    return _mapping(json.loads(payload))


def _write_once(path: Path, payload: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise ValueError("release-holdout content-address collision")
        return
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _reserve_once(path: Path, payload: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ReleaseHoldoutAlreadyConsumedError(
            "release holdout has already been opened or reserved"
        ) from error
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _atomic_replace(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        str(canonical_contract_json(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _require_fixed_policy(data: Mapping[str, Any]) -> None:
    expected = {
        "split_unit": "whole-non-overlapping-window-v1",
        "cohesion_policy": (
            "anchors-context-events-and-temporal-neighbors-stay-together-v1"
        ),
        "routine_access": "manifest-and-audit-only",
        "maximum_evaluations": 1,
        "selection_role_permitted": False,
        "candidate_fit_access_permitted": False,
        "preprocessing_access_permitted": False,
        "support_tuning_access_permitted": False,
        "smoothing_access_permitted": False,
        "engine_selection_access_permitted": False,
        "scenario_policy_access_permitted": False,
        "adaptive_policy_access_permitted": False,
        "frozen_graph_required": True,
        "failed_evaluation_tuning_permitted": False,
        "successor_required_after_non_pass": True,
        "retirement_required_after_release_decision": True,
    }
    if any(data.get(key) != value for key, value in expected.items()):
        raise ValueError("release-holdout access policy differs")


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"schema version must be {expected}")


def _required_text(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("release-holdout text is required")
    return text


def _text_tuple(
    values: Sequence[str], *, allow_empty: bool = False
) -> tuple[str, ...]:
    selected = tuple(sorted({_required_text(value) for value in values}))
    if not selected and not allow_empty:
        raise ValueError("release-holdout text collection is empty")
    return selected


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected < 0:
        raise ValueError(f"{name} must be nonnegative")
    return selected


def _positive_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected <= 0:
        raise ValueError(f"{name} must be positive")
    return selected


def _sha256(value: str, name: str) -> str:
    selected = str(value).strip().lower()
    if not _SHA256.fullmatch(selected):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return selected


def _sketch(value: str, name: str) -> str:
    selected = str(value).strip().lower()
    if not _SKETCH.fullmatch(selected):
        raise ValueError(f"{name} must be a 64-bit hexadecimal sketch")
    return selected


def _timestamp(value: str) -> str:
    selected = _required_text(value)
    if "T" not in selected or not selected.endswith("Z"):
        raise ValueError("release-holdout timestamp must be UTC ISO-8601")
    _timestamp_value(selected)
    return selected


def _timestamp_value(value: str) -> datetime:
    selected = _required_text(value)
    if "T" not in selected or not selected.endswith("Z"):
        raise ValueError("release-holdout timestamp must be UTC ISO-8601")
    try:
        parsed = datetime.fromisoformat(selected.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            "release-holdout timestamp must be UTC ISO-8601"
        ) from error
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("release-holdout timestamp must use UTC")
    return parsed


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("release-holdout value must be a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("release-holdout value must be a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


__all__ = [
    "PROTECTED_RELEASE_HOLDOUT_MANIFEST_SCHEMA_VERSION",
    "PROTECTED_RELEASE_HOLDOUT_WINDOW_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_FREEZE_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_ACCESS_POLICY_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_COVERAGE_AUDIT_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_DEVELOPMENT_UNIT_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_EVALUATION_RECEIPT_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_EVALUATION_RESULT_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_LEAKAGE_AUDIT_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_RETIREMENT_MARKER_SCHEMA_VERSION",
    "ProtectedReleaseHoldoutManifestV1",
    "ProtectedReleaseHoldoutWindowV1",
    "ReleaseCandidateFreezeV1",
    "ReleaseHoldoutAccessPolicyV1",
    "ReleaseHoldoutAlreadyConsumedError",
    "ReleaseHoldoutAuditStatus",
    "ReleaseHoldoutAuthorizationV1",
    "ReleaseHoldoutCoverageAuditV1",
    "ReleaseHoldoutDevelopmentUnitV1",
    "ReleaseHoldoutEvaluationOutcome",
    "ReleaseHoldoutEvaluationReceiptV1",
    "ReleaseHoldoutEvaluationResultV1",
    "ReleaseHoldoutLeakageAuditV1",
    "ReleaseHoldoutRetirementMarkerV1",
    "audit_release_holdout_coverage",
    "audit_release_holdout_leakage",
    "authorize_release_holdout",
    "build_protected_release_holdout_manifest",
    "execute_release_holdout_once",
    "freeze_release_candidate",
    "read_protected_release_holdout_manifest",
    "read_release_candidate_freeze",
    "read_release_holdout_access_policy",
    "read_release_holdout_authorization",
    "read_release_holdout_evaluation_receipt",
    "read_release_holdout_retirement_marker",
    "retire_release_holdout",
    "write_protected_release_holdout_manifest",
    "write_release_candidate_freeze",
    "write_release_holdout_access_policy",
    "write_release_holdout_authorization",
    "write_release_holdout_evaluation_receipt",
    "write_release_holdout_retirement_marker",
]
