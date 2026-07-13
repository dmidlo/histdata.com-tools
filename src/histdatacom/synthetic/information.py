"""Point-in-time information contracts and leakage auditing.

The contracts in this module separate historically informed reconstruction
from forward-looking simulation.  They describe information use at artifact
granularity; event rows continue to reference their run and source lineage
without carrying a copy of the full information graph.

Version-one schemas are immutable.  A semantic change to a required field,
identity rule, audit rule, or validity claim requires a new schema version.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar, cast

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
    validate_reconstruction_window_plan,
)

RECONSTRUCTION_INFORMATION_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-information-policy.v1"
)
RECONSTRUCTION_INFORMATION_INPUT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-information-input.v1"
)
RECONSTRUCTION_INFORMATION_SPLIT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-information-split.v1"
)
RECONSTRUCTION_INFORMATION_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-information-manifest.v1"
)
RECONSTRUCTION_INFORMATION_AUDIT_FINDING_SCHEMA_VERSION = (
    "histdatacom.reconstruction-information-audit-finding.v1"
)
RECONSTRUCTION_INFORMATION_AUDIT_REPORT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-information-audit-report.v1"
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
MAX_INFORMATION_INPUTS = 4096
MAX_INFORMATION_PARENTS = 64
MAX_INFORMATION_TEXT_LENGTH = 1024
MAX_INFORMATION_EVIDENCE_BYTES = 16_384
MAX_INFORMATION_AUDIT_FINDINGS = 512
DEFAULT_INFORMATION_AUDIT_FINDINGS = 128

_EnumT = TypeVar("_EnumT", bound=Enum)


class InformationMode(str, Enum):
    """Whether a run may use realized future information."""

    EX_POST_RECONSTRUCTION = "ex_post_reconstruction"
    EX_ANTE_SIMULATION = "ex_ante_simulation"

    @classmethod
    def from_value(cls, value: str | "InformationMode") -> "InformationMode":
        """Return a strict normalized information mode."""
        return _enum_value(cls, value, "information mode")


class InformationInputKind(str, Enum):
    """Whether an input is sourced externally or derived in the graph."""

    EXTERNAL = "external"
    DERIVED = "derived"

    @classmethod
    def from_value(
        cls, value: str | "InformationInputKind"
    ) -> "InformationInputKind":
        """Return a strict normalized input kind."""
        return _enum_value(cls, value, "information input kind")


class InformationStage(str, Enum):
    """Consumer stage for one declared information use."""

    SOURCE = "source"
    FEATURE = "feature"
    MOTIF_SELECTION = "motif_selection"
    CALENDAR_CONTEXT = "calendar_context"
    NEWS_CONTEXT = "news_context"
    MODEL_FIT = "model_fit"
    CALIBRATION = "calibration"
    CARVING = "carving"
    GENERATION = "generation"
    VALIDATION = "validation"
    STRATEGY_EVALUATION = "strategy_evaluation"

    @classmethod
    def from_value(cls, value: str | "InformationStage") -> "InformationStage":
        """Return a strict normalized stage."""
        return _enum_value(cls, value, "information stage")


class InformationScope(str, Enum):
    """Temporal scope that makes future-informed uses explicit."""

    POINT_IN_TIME = "point_in_time"
    REVISION = "revision"
    FUTURE_ANCHOR = "future_anchor"
    FULL_PERIOD_SUMMARY = "full_period_summary"
    GLOBAL_NORMALIZATION = "global_normalization"
    EMPIRICAL_MOTIF = "empirical_motif"

    @classmethod
    def from_value(cls, value: str | "InformationScope") -> "InformationScope":
        """Return a strict normalized temporal scope."""
        return _enum_value(cls, value, "information scope")


class InformationSplitKind(str, Enum):
    """Chronological research split kinds."""

    TRAIN = "train"
    CALIBRATION = "calibration"
    VALIDATION = "validation"

    @classmethod
    def from_value(
        cls, value: str | "InformationSplitKind"
    ) -> "InformationSplitKind":
        """Return a strict normalized split kind."""
        return _enum_value(cls, value, "information split kind")


class InformationAuditRule(str, Enum):
    """Stable rule identifiers emitted by the information audit."""

    POLICY_MODE_MISMATCH = "INFORMATION_POLICY_MODE_MISMATCH"
    POLICY_ID_MISMATCH = "INFORMATION_POLICY_ID_MISMATCH"
    POLICY_NOT_BOUND_TO_RUN = "INFORMATION_POLICY_NOT_BOUND_TO_RUN"
    RUN_ID_MISMATCH = "INFORMATION_RUN_ID_MISMATCH"
    WINDOW_PLAN_EMPTY = "INFORMATION_WINDOW_PLAN_EMPTY"
    WINDOW_PLAN_MISMATCH = "INFORMATION_WINDOW_PLAN_MISMATCH"
    WINDOW_PLAN_INVALID = "INFORMATION_WINDOW_PLAN_INVALID"
    WINDOW_RUN_MISMATCH = "INFORMATION_WINDOW_RUN_MISMATCH"
    WINDOW_SCOPE_MISMATCH = "INFORMATION_WINDOW_SCOPE_MISMATCH"
    WINDOW_MEMBER_MISSING = "INFORMATION_WINDOW_MEMBER_MISSING"
    WINDOW_MEMBER_PLAN_MISMATCH = "INFORMATION_WINDOW_MEMBER_PLAN_MISMATCH"
    WINDOW_LOOKAHEAD_EXCEEDS_POLICY = (
        "INFORMATION_WINDOW_LOOKAHEAD_EXCEEDS_POLICY"
    )
    EX_ANTE_WINDOW_LOOKAHEAD = "INFORMATION_EX_ANTE_WINDOW_LOOKAHEAD"
    DUPLICATE_INPUT_ID = "INFORMATION_DUPLICATE_INPUT_ID"
    INPUT_GRAPH_EMPTY = "INFORMATION_INPUT_GRAPH_EMPTY"
    INPUT_RUN_MISMATCH = "INFORMATION_INPUT_RUN_MISMATCH"
    INPUT_MODE_MISMATCH = "INFORMATION_INPUT_MODE_MISMATCH"
    INPUT_LOOKAHEAD_EXCEEDS_POLICY = (
        "INFORMATION_INPUT_LOOKAHEAD_EXCEEDS_POLICY"
    )
    DERIVED_INPUT_WITHOUT_PARENT = "INFORMATION_DERIVED_INPUT_WITHOUT_PARENT"
    MISSING_PARENT_INPUT = "INFORMATION_MISSING_PARENT_INPUT"
    DERIVED_AVAILABLE_BEFORE_PARENT = (
        "INFORMATION_DERIVED_AVAILABLE_BEFORE_PARENT"
    )
    GRAPH_CYCLE = "INFORMATION_GRAPH_CYCLE"
    REVISION_SCOPE_UNDECLARED = "INFORMATION_REVISION_SCOPE_UNDECLARED"
    REVISION_PARENT_MISSING = "INFORMATION_REVISION_PARENT_MISSING"
    REVISION_SEQUENCE_INVALID = "INFORMATION_REVISION_SEQUENCE_INVALID"
    REVISION_AVAILABILITY_INVALID = "INFORMATION_REVISION_AVAILABILITY_INVALID"
    SPLIT_MISSING = "INFORMATION_SPLIT_MISSING"
    SPLIT_DUPLICATE = "INFORMATION_SPLIT_DUPLICATE"
    SPLIT_DECLARATION_ORDER = "INFORMATION_SPLIT_DECLARATION_ORDER"
    SPLIT_TIME_ORDER = "INFORMATION_SPLIT_TIME_ORDER"
    INPUT_SPLIT_MISSING = "INFORMATION_INPUT_SPLIT_MISSING"
    INPUT_SPLIT_MISMATCH = "INFORMATION_INPUT_SPLIT_MISMATCH"
    INPUT_OUTSIDE_SPLIT = "INFORMATION_INPUT_OUTSIDE_SPLIT"
    EX_ANTE_LOOKAHEAD_DECLARED = "INFORMATION_EX_ANTE_LOOKAHEAD_DECLARED"
    EX_ANTE_INPUT_NOT_AVAILABLE = "INFORMATION_EX_ANTE_INPUT_NOT_AVAILABLE"
    EX_ANTE_REVISION_NOT_AVAILABLE = (
        "INFORMATION_EX_ANTE_REVISION_NOT_AVAILABLE"
    )
    EX_ANTE_FUTURE_EVENT = "INFORMATION_EX_ANTE_FUTURE_EVENT"
    EX_ANTE_FUTURE_OBSERVATION = "INFORMATION_EX_ANTE_FUTURE_OBSERVATION"
    EX_ANTE_FUTURE_ANCHOR = "INFORMATION_EX_ANTE_FUTURE_ANCHOR"
    EX_ANTE_FULL_PERIOD_SUMMARY = "INFORMATION_EX_ANTE_FULL_PERIOD_SUMMARY"
    EX_ANTE_GLOBAL_NORMALIZATION = "INFORMATION_EX_ANTE_GLOBAL_NORMALIZATION"
    EX_ANTE_MOTIF_SELECTION_LEAKAGE = (
        "INFORMATION_EX_ANTE_MOTIF_SELECTION_LEAKAGE"
    )
    EX_POST_UNLABELED_FUTURE_INFORMATION = (
        "INFORMATION_EX_POST_UNLABELED_FUTURE_INFORMATION"
    )
    EX_POST_LOOKAHEAD_EXCEEDED = "INFORMATION_EX_POST_LOOKAHEAD_EXCEEDED"

    @classmethod
    def from_value(
        cls, value: str | "InformationAuditRule"
    ) -> "InformationAuditRule":
        """Return a strict stable audit rule."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().upper())
        except ValueError as err:
            raise ValueError("unsupported information audit rule") from err


@dataclass(frozen=True, slots=True)
class ReconstructionInformationPolicyV1:
    """Run-bound mode and fail-closed audit limits."""

    information_mode: InformationMode
    max_allowed_lookahead_ns: int = 0
    max_retained_findings: int = DEFAULT_INFORMATION_AUDIT_FINDINGS
    fail_closed: bool = True
    require_time_ordered_splits: bool = True
    policy_id: str = ""
    schema_version: str = RECONSTRUCTION_INFORMATION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_INFORMATION_POLICY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction information policy schema"
            )
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        lookahead = _nonnegative_int64(
            self.max_allowed_lookahead_ns,
            "max_allowed_lookahead_ns",
        )
        if (
            self.information_mode is InformationMode.EX_ANTE_SIMULATION
            and lookahead != 0
        ):
            raise ValueError(
                "ex-ante information policy requires zero look-ahead"
            )
        object.__setattr__(self, "max_allowed_lookahead_ns", lookahead)
        retained = _positive_int(
            self.max_retained_findings,
            "max_retained_findings",
        )
        if retained > MAX_INFORMATION_AUDIT_FINDINGS:
            raise ValueError("max_retained_findings exceeds the v1 limit")
        object.__setattr__(self, "max_retained_findings", retained)
        if self.fail_closed is not True:
            raise ValueError("v1 information policies must fail closed")
        if self.require_time_ordered_splits is not True:
            raise ValueError("v1 information policies require ordered splits")
        expected = _stable_id("information-policy", self.identity_payload())
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("policy_id does not match deterministic identity")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic policy identity."""
        return {
            "schema_version": self.schema_version,
            "information_mode": self.information_mode.value,
            "max_allowed_lookahead_ns": self.max_allowed_lookahead_ns,
            "fail_closed": self.fail_closed,
            "require_time_ordered_splits": self.require_time_ordered_splits,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy metadata."""
        return {
            **self.identity_payload(),
            "max_retained_findings": self.max_retained_findings,
            "policy_id": self.policy_id,
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionInformationPolicyV1":
        """Restore and verify a version-one information policy."""
        _require_schema(data, RECONSTRUCTION_INFORMATION_POLICY_SCHEMA_VERSION)
        return cls(
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            max_allowed_lookahead_ns=cast(
                int, data.get("max_allowed_lookahead_ns", 0)
            ),
            max_retained_findings=cast(
                int,
                data.get(
                    "max_retained_findings",
                    DEFAULT_INFORMATION_AUDIT_FINDINGS,
                ),
            ),
            fail_closed=_strict_bool(data.get("fail_closed"), "fail_closed"),
            require_time_ordered_splits=_strict_bool(
                data.get("require_time_ordered_splits"),
                "require_time_ordered_splits",
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionInformationPolicyV1":
        """Restore a policy from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionInformationInputV1:
    """One external or derived artifact use with temporal availability."""

    run_id: str
    artifact_id: str
    information_mode: InformationMode
    input_kind: InformationInputKind
    stage: InformationStage
    scope: InformationScope
    event_time_ns: int
    available_at_ns: int
    used_at_ns: int
    observation_start_ns: int
    observation_end_ns: int
    vintage_id: str
    reason: str
    revision_sequence: int = 0
    supersedes_input_id: str | None = None
    allowed_lookahead_ns: int = 0
    parent_input_ids: tuple[str, ...] = ()
    split_kind: InformationSplitKind | None = None
    input_id: str = ""
    schema_version: str = RECONSTRUCTION_INFORMATION_INPUT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_INFORMATION_INPUT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction information input schema"
            )
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self, "artifact_id", _required_text(self.artifact_id)
        )
        object.__setattr__(self, "vintage_id", _required_text(self.vintage_id))
        object.__setattr__(self, "reason", _required_text(self.reason))
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self,
            "input_kind",
            InformationInputKind.from_value(self.input_kind),
        )
        object.__setattr__(
            self,
            "stage",
            InformationStage.from_value(self.stage),
        )
        object.__setattr__(
            self,
            "scope",
            InformationScope.from_value(self.scope),
        )
        for name in (
            "event_time_ns",
            "available_at_ns",
            "used_at_ns",
            "observation_start_ns",
            "observation_end_ns",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int64(getattr(self, name), name),
            )
        if self.observation_end_ns < self.observation_start_ns:
            raise ValueError(
                "observation_end_ns must not precede observation_start_ns"
            )
        if (
            not self.observation_start_ns
            <= self.event_time_ns
            <= self.observation_end_ns
        ):
            raise ValueError(
                "event_time_ns must lie inside the observation interval"
            )
        revision = _nonnegative_int(
            self.revision_sequence,
            "revision_sequence",
        )
        object.__setattr__(self, "revision_sequence", revision)
        supersedes = _optional_text(self.supersedes_input_id)
        if revision > 0 and supersedes is None:
            raise ValueError("revised information requires supersedes_input_id")
        if revision == 0 and supersedes is not None:
            raise ValueError(
                "initial information cannot supersede another input"
            )
        object.__setattr__(self, "supersedes_input_id", supersedes)
        object.__setattr__(
            self,
            "allowed_lookahead_ns",
            _nonnegative_int64(
                self.allowed_lookahead_ns,
                "allowed_lookahead_ns",
            ),
        )
        parents = _normalized_ids(self.parent_input_ids)
        if len(parents) > MAX_INFORMATION_PARENTS:
            raise ValueError("parent_input_ids exceeds the v1 limit")
        object.__setattr__(self, "parent_input_ids", parents)
        if self.split_kind is not None:
            object.__setattr__(
                self,
                "split_kind",
                InformationSplitKind.from_value(self.split_kind),
            )
        expected = _stable_id("information-input", self.identity_payload())
        supplied = _optional_text(self.input_id)
        if supplied is not None and supplied != expected:
            raise ValueError("input_id does not match deterministic identity")
        object.__setattr__(self, "input_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic input identity."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "artifact_id": self.artifact_id,
            "information_mode": self.information_mode.value,
            "input_kind": self.input_kind.value,
            "stage": self.stage.value,
            "scope": self.scope.value,
            "event_time_ns": self.event_time_ns,
            "available_at_ns": self.available_at_ns,
            "used_at_ns": self.used_at_ns,
            "observation_start_ns": self.observation_start_ns,
            "observation_end_ns": self.observation_end_ns,
            "vintage_id": self.vintage_id,
            "reason": self.reason,
            "revision_sequence": self.revision_sequence,
            "supersedes_input_id": self.supersedes_input_id,
            "allowed_lookahead_ns": self.allowed_lookahead_ns,
            "parent_input_ids": list(self.parent_input_ids),
            "split_kind": (
                self.split_kind.value if self.split_kind is not None else None
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible input metadata."""
        return {**self.identity_payload(), "input_id": self.input_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionInformationInputV1":
        """Restore and verify one version-one information input."""
        _require_schema(data, RECONSTRUCTION_INFORMATION_INPUT_SCHEMA_VERSION)
        split_value = data.get("split_kind")
        return cls(
            run_id=str(data.get("run_id", "")),
            artifact_id=str(data.get("artifact_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            input_kind=InformationInputKind.from_value(
                str(data.get("input_kind", ""))
            ),
            stage=InformationStage.from_value(str(data.get("stage", ""))),
            scope=InformationScope.from_value(str(data.get("scope", ""))),
            event_time_ns=cast(int, data.get("event_time_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            used_at_ns=cast(int, data.get("used_at_ns")),
            observation_start_ns=cast(int, data.get("observation_start_ns")),
            observation_end_ns=cast(int, data.get("observation_end_ns")),
            vintage_id=str(data.get("vintage_id", "")),
            reason=str(data.get("reason", "")),
            revision_sequence=cast(int, data.get("revision_sequence", 0)),
            supersedes_input_id=_optional_text(data.get("supersedes_input_id")),
            allowed_lookahead_ns=cast(int, data.get("allowed_lookahead_ns", 0)),
            parent_input_ids=_string_tuple(data.get("parent_input_ids")),
            split_kind=(
                InformationSplitKind.from_value(str(split_value))
                if split_value is not None
                else None
            ),
            input_id=str(data.get("input_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionInformationInputV1":
        """Restore an information input from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionInformationSplitV1:
    """One half-open chronological train, calibration, or validation split."""

    kind: InformationSplitKind
    start_ns: int
    end_ns: int
    split_id: str = ""
    schema_version: str = RECONSTRUCTION_INFORMATION_SPLIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_INFORMATION_SPLIT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction information split schema"
            )
        object.__setattr__(
            self,
            "kind",
            InformationSplitKind.from_value(self.kind),
        )
        object.__setattr__(
            self, "start_ns", _bounded_int64(self.start_ns, "start_ns")
        )
        object.__setattr__(
            self, "end_ns", _bounded_int64(self.end_ns, "end_ns")
        )
        if self.end_ns <= self.start_ns:
            raise ValueError(
                "information split end_ns must be greater than start_ns"
            )
        expected = _stable_id("information-split", self.identity_payload())
        supplied = _optional_text(self.split_id)
        if supplied is not None and supplied != expected:
            raise ValueError("split_id does not match deterministic identity")
        object.__setattr__(self, "split_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic split identity."""
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval": "[start_ns,end_ns)",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible split metadata."""
        return {**self.identity_payload(), "split_id": self.split_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionInformationSplitV1":
        """Restore and verify a version-one split."""
        _require_schema(data, RECONSTRUCTION_INFORMATION_SPLIT_SCHEMA_VERSION)
        return cls(
            kind=InformationSplitKind.from_value(str(data.get("kind", ""))),
            start_ns=cast(int, data.get("start_ns")),
            end_ns=cast(int, data.get("end_ns")),
            split_id=str(data.get("split_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionInformationSplitV1":
        """Restore a split from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionInformationManifestV1:
    """One run's mode, complete artifact-use graph, and research splits."""

    run_id: str
    policy_id: str
    information_mode: InformationMode
    window_plan_id: str
    inputs: tuple[ReconstructionInformationInputV1, ...]
    splits: tuple[ReconstructionInformationSplitV1, ...]
    manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_INFORMATION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_INFORMATION_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction information manifest schema"
            )
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(self, "policy_id", _required_text(self.policy_id))
        object.__setattr__(
            self,
            "window_plan_id",
            _required_text(self.window_plan_id),
        )
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        inputs = tuple(self.inputs)
        if len(inputs) > MAX_INFORMATION_INPUTS:
            raise ValueError("information inputs exceed the v1 limit")
        if any(
            not isinstance(item, ReconstructionInformationInputV1)
            for item in inputs
        ):
            raise ValueError(
                "inputs must contain version-one information inputs"
            )
        object.__setattr__(
            self,
            "inputs",
            tuple(sorted(inputs, key=lambda item: item.input_id)),
        )
        splits = tuple(self.splits)
        if any(
            not isinstance(item, ReconstructionInformationSplitV1)
            for item in splits
        ):
            raise ValueError(
                "splits must contain version-one information splits"
            )
        object.__setattr__(self, "splits", splits)
        expected = _stable_id("information-manifest", self.identity_payload())
        supplied = _optional_text(self.manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "manifest_id does not match deterministic identity"
            )
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic manifest identity."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "policy_id": self.policy_id,
            "information_mode": self.information_mode.value,
            "window_plan_id": self.window_plan_id,
            "inputs": [item.to_dict() for item in self.inputs],
            "splits": [item.to_dict() for item in self.splits],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible manifest metadata."""
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionInformationManifestV1":
        """Restore and verify a version-one information manifest."""
        _require_schema(
            data, RECONSTRUCTION_INFORMATION_MANIFEST_SCHEMA_VERSION
        )
        return cls(
            run_id=str(data.get("run_id", "")),
            policy_id=str(data.get("policy_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            window_plan_id=str(data.get("window_plan_id", "")),
            inputs=tuple(
                ReconstructionInformationInputV1.from_dict(item)
                for item in _mapping_sequence(data.get("inputs"))
            ),
            splits=tuple(
                ReconstructionInformationSplitV1.from_dict(item)
                for item in _mapping_sequence(data.get("splits"))
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionInformationManifestV1":
        """Restore a manifest from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class InformationAuditFindingV1:
    """One deterministic fail-closed information violation."""

    rule_id: InformationAuditRule
    message: str
    input_id: str | None = None
    evidence: Mapping[str, JSONValue] | None = None
    finding_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_INFORMATION_AUDIT_FINDING_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_INFORMATION_AUDIT_FINDING_SCHEMA_VERSION
        ):
            raise ValueError("unsupported information audit finding schema")
        object.__setattr__(
            self,
            "rule_id",
            InformationAuditRule.from_value(self.rule_id),
        )
        object.__setattr__(self, "message", _required_text(self.message))
        object.__setattr__(self, "input_id", _optional_text(self.input_id))
        evidence = dict(self.evidence or {})
        _bounded_json(
            evidence, MAX_INFORMATION_EVIDENCE_BYTES, "finding evidence"
        )
        object.__setattr__(self, "evidence", evidence)
        expected = _stable_id("information-finding", self.identity_payload())
        supplied = _optional_text(self.finding_id)
        if supplied is not None and supplied != expected:
            raise ValueError("finding_id does not match deterministic identity")
        object.__setattr__(self, "finding_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic finding identity."""
        return {
            "schema_version": self.schema_version,
            "severity": "error",
            "rule_id": self.rule_id.value,
            "message": self.message,
            "input_id": self.input_id,
            "evidence": dict(self.evidence or {}),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible finding metadata."""
        return {**self.identity_payload(), "finding_id": self.finding_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "InformationAuditFindingV1":
        """Restore and verify one audit finding."""
        _require_schema(
            data,
            RECONSTRUCTION_INFORMATION_AUDIT_FINDING_SCHEMA_VERSION,
        )
        return cls(
            rule_id=InformationAuditRule.from_value(
                str(data.get("rule_id", ""))
            ),
            message=str(data.get("message", "")),
            input_id=_optional_text(data.get("input_id")),
            evidence=_mapping(data.get("evidence")),
            finding_id=str(data.get("finding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "InformationAuditFindingV1":
        """Restore an audit finding from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class InformationAuditReportV1:
    """Bounded audit result and explicit downstream-validity statement."""

    run_id: str
    policy_id: str
    manifest_id: str
    window_plan_id: str
    information_mode: InformationMode
    accepted: bool
    total_violation_count: int
    findings: tuple[InformationAuditFindingV1, ...]
    evidence_truncated: bool
    valid_for: tuple[str, ...]
    invalid_for: tuple[str, ...]
    summary: str
    audit_id: str = ""
    schema_version: str = RECONSTRUCTION_INFORMATION_AUDIT_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_INFORMATION_AUDIT_REPORT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction information audit report schema"
            )
        for name in (
            "run_id",
            "policy_id",
            "manifest_id",
            "window_plan_id",
            "summary",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self,
            "accepted",
            _strict_bool(self.accepted, "accepted"),
        )
        object.__setattr__(
            self,
            "evidence_truncated",
            _strict_bool(self.evidence_truncated, "evidence_truncated"),
        )
        total = _nonnegative_int(
            self.total_violation_count,
            "total_violation_count",
        )
        object.__setattr__(self, "total_violation_count", total)
        findings = tuple(self.findings)
        if any(
            not isinstance(item, InformationAuditFindingV1) for item in findings
        ):
            raise ValueError("findings must contain version-one audit findings")
        if len(findings) > total:
            raise ValueError("retained findings cannot exceed total violations")
        object.__setattr__(self, "findings", findings)
        if self.accepted is not (total == 0):
            raise ValueError("accepted must equal zero total violations")
        if self.evidence_truncated is not (len(findings) < total):
            raise ValueError("evidence_truncated does not match finding counts")
        object.__setattr__(self, "valid_for", _normalized_ids(self.valid_for))
        object.__setattr__(
            self, "invalid_for", _normalized_ids(self.invalid_for)
        )
        expected = _stable_id("information-audit", self.identity_payload())
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id does not match deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    @property
    def valid_for_strategy_usefulness_claim(self) -> bool:
        """Return whether this report opens the strategy-usefulness gate."""
        return self.accepted and "strategy_usefulness_claims" in self.valid_for

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields used for deterministic report identity."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "policy_id": self.policy_id,
            "manifest_id": self.manifest_id,
            "window_plan_id": self.window_plan_id,
            "information_mode": self.information_mode.value,
            "accepted": self.accepted,
            "total_violation_count": self.total_violation_count,
            "retained_violation_count": len(self.findings),
            "evidence_truncated": self.evidence_truncated,
            "findings": [item.to_dict() for item in self.findings],
            "valid_for": list(self.valid_for),
            "invalid_for": list(self.invalid_for),
            "valid_for_strategy_usefulness_claim": (
                self.valid_for_strategy_usefulness_claim
            ),
            "summary": self.summary,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible report metadata."""
        return {**self.identity_payload(), "audit_id": self.audit_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "InformationAuditReportV1":
        """Restore and verify a version-one audit report."""
        _require_schema(
            data,
            RECONSTRUCTION_INFORMATION_AUDIT_REPORT_SCHEMA_VERSION,
        )
        return cls(
            run_id=str(data.get("run_id", "")),
            policy_id=str(data.get("policy_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            window_plan_id=str(data.get("window_plan_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            accepted=_strict_bool(data.get("accepted"), "accepted"),
            total_violation_count=cast(int, data.get("total_violation_count")),
            findings=tuple(
                InformationAuditFindingV1.from_dict(item)
                for item in _mapping_sequence(data.get("findings"))
            ),
            evidence_truncated=_strict_bool(
                data.get("evidence_truncated"),
                "evidence_truncated",
            ),
            valid_for=_string_tuple(data.get("valid_for")),
            invalid_for=_string_tuple(data.get("invalid_for")),
            summary=str(data.get("summary", "")),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "InformationAuditReportV1":
        """Restore an audit report from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


class InformationLeakageError(ValueError):
    """Fail-closed pre-generation rejection carrying the audit report."""

    def __init__(self, report: InformationAuditReportV1) -> None:
        self.report = report
        super().__init__(
            "reconstruction information audit failed with "
            f"{report.total_violation_count} violation(s)"
        )


def audit_reconstruction_information(
    manifest: ReconstructionInformationManifestV1,
    policy: ReconstructionInformationPolicyV1,
    *,
    run: ReconstructionRunV1,
    windows: Sequence[ReconstructionWindowV1],
) -> InformationAuditReportV1:
    """Audit one complete artifact-use graph before generation begins."""
    findings: list[InformationAuditFindingV1] = []

    def add(
        rule: InformationAuditRule,
        message: str,
        *,
        item: ReconstructionInformationInputV1 | None = None,
        evidence: Mapping[str, JSONValue] | None = None,
    ) -> None:
        findings.append(
            InformationAuditFindingV1(
                rule_id=rule,
                message=message,
                input_id=item.input_id if item is not None else None,
                evidence=evidence,
            )
        )

    if manifest.policy_id != policy.policy_id:
        add(
            InformationAuditRule.POLICY_ID_MISMATCH,
            "manifest policy_id does not match the audited policy",
            evidence={
                "manifest_policy_id": manifest.policy_id,
                "audit_policy_id": policy.policy_id,
            },
        )
    if manifest.information_mode is not policy.information_mode:
        add(
            InformationAuditRule.POLICY_MODE_MISMATCH,
            "manifest information mode does not match the audited policy",
            evidence={
                "manifest_mode": manifest.information_mode.value,
                "policy_mode": policy.information_mode.value,
            },
        )
    if manifest.run_id != run.run_id:
        add(
            InformationAuditRule.RUN_ID_MISMATCH,
            "manifest run_id does not match the reconstruction run",
            evidence={
                "manifest_run_id": manifest.run_id,
                "run_id": run.run_id,
            },
        )
    if policy.policy_id not in run.configuration_ids:
        add(
            InformationAuditRule.POLICY_NOT_BOUND_TO_RUN,
            "information policy is not bound into run configuration_ids",
            evidence={"policy_id": policy.policy_id},
        )

    window_plan = tuple(windows)
    if not window_plan:
        add(
            InformationAuditRule.WINDOW_PLAN_EMPTY,
            "information audit requires the exact non-empty window plan",
        )
    else:
        actual_window_plan_id = reconstruction_information_window_plan_id(
            window_plan
        )
        if actual_window_plan_id != manifest.window_plan_id:
            add(
                InformationAuditRule.WINDOW_PLAN_MISMATCH,
                "audited windows do not match the manifest window plan",
                evidence={
                    "manifest_window_plan_id": manifest.window_plan_id,
                    "audit_window_plan_id": actual_window_plan_id,
                },
            )
        grouped_windows: dict[str, list[ReconstructionWindowV1]] = {}
        for window in window_plan:
            grouped_windows.setdefault(window.ensemble_member_id, []).append(
                window
            )
            if window.run_id != run.run_id:
                add(
                    InformationAuditRule.WINDOW_RUN_MISMATCH,
                    "reconstruction window belongs to a different run",
                    evidence={
                        "window_id": window.window_id,
                        "window_run_id": window.run_id,
                        "run_id": run.run_id,
                    },
                )
            if (
                window.ensemble_member_id not in run.ensemble_member_ids
                or window.symbols != run.symbols
            ):
                add(
                    InformationAuditRule.WINDOW_SCOPE_MISMATCH,
                    "reconstruction window member or symbols differ from the run",
                    evidence={
                        "window_id": window.window_id,
                        "ensemble_member_id": window.ensemble_member_id,
                        "symbols": list(window.symbols),
                    },
                )
            if window.right_lookahead_ns > policy.max_allowed_lookahead_ns:
                add(
                    InformationAuditRule.WINDOW_LOOKAHEAD_EXCEEDS_POLICY,
                    "window right look-ahead exceeds the information policy",
                    evidence={
                        "window_id": window.window_id,
                        "right_lookahead_ns": window.right_lookahead_ns,
                        "policy_max_allowed_lookahead_ns": (
                            policy.max_allowed_lookahead_ns
                        ),
                    },
                )
            if (
                policy.information_mode is InformationMode.EX_ANTE_SIMULATION
                and window.right_lookahead_ns != 0
            ):
                add(
                    InformationAuditRule.EX_ANTE_WINDOW_LOOKAHEAD,
                    "ex-ante reconstruction windows cannot read future rows",
                    evidence={
                        "window_id": window.window_id,
                        "right_lookahead_ns": window.right_lookahead_ns,
                    },
                )
        for member_id in run.ensemble_member_ids:
            member_windows = grouped_windows.get(member_id, [])
            if not member_windows:
                add(
                    InformationAuditRule.WINDOW_MEMBER_MISSING,
                    "window plan omits an ensemble member declared by the run",
                    evidence={"ensemble_member_id": member_id},
                )
                continue
            try:
                validate_reconstruction_window_plan(member_windows)
            except ValueError as err:
                add(
                    InformationAuditRule.WINDOW_PLAN_INVALID,
                    "member window plan is not contiguous and synchronized",
                    evidence={
                        "ensemble_member_id": member_id,
                        "reason": str(err),
                    },
                )
        member_signatures = {
            member_id: tuple(
                (
                    window.core_start_ns,
                    window.core_end_ns,
                    window.left_halo_ns,
                    window.right_lookahead_ns,
                    window.symbols,
                )
                for window in sorted(
                    member_windows,
                    key=lambda item: item.core_start_ns,
                )
            )
            for member_id, member_windows in grouped_windows.items()
            if member_id in run.ensemble_member_ids
        }
        if len(set(member_signatures.values())) > 1:
            mismatched_member_ids = [
                cast(JSONValue, member_id)
                for member_id in sorted(member_signatures)
            ]
            add(
                InformationAuditRule.WINDOW_MEMBER_PLAN_MISMATCH,
                "ensemble members do not share the same window boundaries",
                evidence={
                    "ensemble_member_ids": mismatched_member_ids,
                },
            )

    _audit_splits(manifest, add)
    if not manifest.inputs:
        add(
            InformationAuditRule.INPUT_GRAPH_EMPTY,
            "information manifest does not declare any external or derived input",
        )
    counts = Counter(item.input_id for item in manifest.inputs)
    for input_id in sorted(key for key, count in counts.items() if count > 1):
        add(
            InformationAuditRule.DUPLICATE_INPUT_ID,
            "information manifest contains a duplicate input_id",
            evidence={"input_id": input_id, "count": counts[input_id]},
        )
    by_id: dict[str, ReconstructionInformationInputV1] = {}
    for item in manifest.inputs:
        by_id.setdefault(item.input_id, item)

    _audit_graph(manifest, by_id, add)
    split_by_kind = {split.kind: split for split in manifest.splits}
    for item in manifest.inputs:
        _audit_input_binding(item, manifest, policy, add)
        _audit_input_split(item, split_by_kind, add)
        _audit_revision(item, by_id, add)
        _audit_parent_availability(item, by_id, add)
        if policy.information_mode is InformationMode.EX_ANTE_SIMULATION:
            _audit_ex_ante_input(item, add)
        else:
            _audit_ex_post_input(item, policy, add)

    ordered = sorted(
        {finding.finding_id: finding for finding in findings}.values(),
        key=lambda finding: (
            finding.rule_id.value,
            finding.input_id or "",
            finding.finding_id,
        ),
    )
    total = len(ordered)
    retained = tuple(ordered[: policy.max_retained_findings])
    accepted = total == 0
    valid_for, invalid_for, summary = _validity_statement(
        policy.information_mode,
        accepted,
    )
    return InformationAuditReportV1(
        run_id=manifest.run_id,
        policy_id=policy.policy_id,
        manifest_id=manifest.manifest_id,
        window_plan_id=manifest.window_plan_id,
        information_mode=policy.information_mode,
        accepted=accepted,
        total_violation_count=total,
        findings=retained,
        evidence_truncated=len(retained) < total,
        valid_for=valid_for,
        invalid_for=invalid_for,
        summary=summary,
    )


def require_reconstruction_information_audit(
    manifest: ReconstructionInformationManifestV1,
    policy: ReconstructionInformationPolicyV1,
    *,
    run: ReconstructionRunV1,
    windows: Sequence[ReconstructionWindowV1],
) -> InformationAuditReportV1:
    """Return an accepted audit or fail closed before generation."""
    report = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=windows,
    )
    if not report.accepted:
        raise InformationLeakageError(report)
    return report


def reconstruction_information_window_plan_id(
    windows: Sequence[ReconstructionWindowV1],
) -> str:
    """Return a deterministic identity for the exact audited window plan."""
    selected = tuple(windows)
    if not selected:
        raise ValueError("information audit window plan cannot be empty")
    if any(not isinstance(item, ReconstructionWindowV1) for item in selected):
        raise ValueError("window plan must contain version-one windows")
    ordered = sorted(
        selected,
        key=lambda item: (
            item.ensemble_member_id,
            item.core_start_ns,
            item.core_end_ns,
            item.window_id,
        ),
    )
    return _stable_id(
        "information-window-plan",
        {
            "window_schema_version": ordered[0].schema_version,
            "windows": [item.to_dict() for item in ordered],
        },
    )


_EXPECTED_SPLITS = (
    InformationSplitKind.TRAIN,
    InformationSplitKind.CALIBRATION,
    InformationSplitKind.VALIDATION,
)
_REQUIRED_STAGE_SPLIT = {
    InformationStage.MODEL_FIT: InformationSplitKind.TRAIN,
    InformationStage.CALIBRATION: InformationSplitKind.CALIBRATION,
    InformationStage.VALIDATION: InformationSplitKind.VALIDATION,
    InformationStage.STRATEGY_EVALUATION: InformationSplitKind.VALIDATION,
}
_PREDECISION_STAGES = frozenset(
    stage
    for stage in InformationStage
    if stage
    not in {InformationStage.VALIDATION, InformationStage.STRATEGY_EVALUATION}
)
_EX_POST_LABELS = frozenset(
    {
        InformationScope.FUTURE_ANCHOR,
        InformationScope.FULL_PERIOD_SUMMARY,
        InformationScope.GLOBAL_NORMALIZATION,
        InformationScope.EMPIRICAL_MOTIF,
    }
)


def _audit_splits(
    manifest: ReconstructionInformationManifestV1,
    add: Any,
) -> None:
    kinds = tuple(split.kind for split in manifest.splits)
    counts = Counter(kinds)
    for expected in _EXPECTED_SPLITS:
        if counts[expected] == 0:
            add(
                InformationAuditRule.SPLIT_MISSING,
                "information manifest is missing a required chronological split",
                evidence={"missing_split": expected.value},
            )
        elif counts[expected] > 1:
            add(
                InformationAuditRule.SPLIT_DUPLICATE,
                "information manifest contains a duplicate split kind",
                evidence={
                    "split_kind": expected.value,
                    "count": counts[expected],
                },
            )
    if kinds != _EXPECTED_SPLITS:
        add(
            InformationAuditRule.SPLIT_DECLARATION_ORDER,
            "splits must be declared in train, calibration, validation order",
            evidence={"declared_order": [kind.value for kind in kinds]},
        )
    unique = {split.kind: split for split in manifest.splits}
    if all(kind in unique for kind in _EXPECTED_SPLITS):
        train = unique[InformationSplitKind.TRAIN]
        calibration = unique[InformationSplitKind.CALIBRATION]
        validation = unique[InformationSplitKind.VALIDATION]
        if (
            not train.end_ns
            <= calibration.start_ns
            <= calibration.end_ns
            <= validation.start_ns
        ):
            add(
                InformationAuditRule.SPLIT_TIME_ORDER,
                "train, calibration, and validation intervals overlap or regress",
                evidence={
                    "train_end_ns": train.end_ns,
                    "calibration_start_ns": calibration.start_ns,
                    "calibration_end_ns": calibration.end_ns,
                    "validation_start_ns": validation.start_ns,
                },
            )


def _audit_graph(
    manifest: ReconstructionInformationManifestV1,
    by_id: Mapping[str, ReconstructionInformationInputV1],
    add: Any,
) -> None:
    for item in manifest.inputs:
        if (
            item.input_kind is InformationInputKind.DERIVED
            and not item.parent_input_ids
        ):
            add(
                InformationAuditRule.DERIVED_INPUT_WITHOUT_PARENT,
                "derived information input does not declare a parent artifact",
                item=item,
            )
        for parent_id in item.parent_input_ids:
            if parent_id not in by_id:
                add(
                    InformationAuditRule.MISSING_PARENT_INPUT,
                    "information input references a parent absent from the manifest",
                    item=item,
                    evidence={"missing_parent_input_id": parent_id},
                )
    cycle = _first_graph_cycle(by_id)
    if cycle:
        add(
            InformationAuditRule.GRAPH_CYCLE,
            "information artifact graph contains a dependency cycle",
            evidence={"cycle_input_ids": list(cycle)},
        )


def _first_graph_cycle(
    by_id: Mapping[str, ReconstructionInformationInputV1],
) -> tuple[str, ...]:
    visiting: list[str] = []
    visiting_set: set[str] = set()
    complete: set[str] = set()

    def visit(input_id: str) -> tuple[str, ...]:
        if input_id in complete:
            return ()
        if input_id in visiting_set:
            start = visiting.index(input_id)
            return tuple(visiting[start:] + [input_id])
        visiting.append(input_id)
        visiting_set.add(input_id)
        item = by_id[input_id]
        for parent_id in item.parent_input_ids:
            if parent_id not in by_id:
                continue
            cycle = visit(parent_id)
            if cycle:
                return cycle
        visiting.pop()
        visiting_set.remove(input_id)
        complete.add(input_id)
        return ()

    for input_id in sorted(by_id):
        cycle = visit(input_id)
        if cycle:
            return cycle
    return ()


def _audit_input_binding(
    item: ReconstructionInformationInputV1,
    manifest: ReconstructionInformationManifestV1,
    policy: ReconstructionInformationPolicyV1,
    add: Any,
) -> None:
    if item.run_id != manifest.run_id:
        add(
            InformationAuditRule.INPUT_RUN_MISMATCH,
            "information input belongs to a different reconstruction run",
            item=item,
            evidence={
                "input_run_id": item.run_id,
                "manifest_run_id": manifest.run_id,
            },
        )
    if item.information_mode is not manifest.information_mode:
        add(
            InformationAuditRule.INPUT_MODE_MISMATCH,
            "information input mode differs from the run manifest mode",
            item=item,
            evidence={
                "input_mode": item.information_mode.value,
                "manifest_mode": manifest.information_mode.value,
            },
        )
    if item.allowed_lookahead_ns > policy.max_allowed_lookahead_ns:
        add(
            InformationAuditRule.INPUT_LOOKAHEAD_EXCEEDS_POLICY,
            "input look-ahead exceeds the run information policy",
            item=item,
            evidence={
                "input_allowed_lookahead_ns": item.allowed_lookahead_ns,
                "policy_max_allowed_lookahead_ns": policy.max_allowed_lookahead_ns,
            },
        )


def _audit_input_split(
    item: ReconstructionInformationInputV1,
    split_by_kind: Mapping[
        InformationSplitKind, ReconstructionInformationSplitV1
    ],
    add: Any,
) -> None:
    required = _REQUIRED_STAGE_SPLIT.get(item.stage)
    if required is not None and item.split_kind is None:
        add(
            InformationAuditRule.INPUT_SPLIT_MISSING,
            "information stage requires an explicit research split",
            item=item,
            evidence={
                "required_split": required.value,
                "stage": item.stage.value,
            },
        )
        return
    if required is not None and item.split_kind is not required:
        add(
            InformationAuditRule.INPUT_SPLIT_MISMATCH,
            "information stage is assigned to the wrong research split",
            item=item,
            evidence={
                "required_split": required.value,
                "declared_split": (
                    item.split_kind.value
                    if item.split_kind is not None
                    else None
                ),
            },
        )
    if item.split_kind is None:
        return
    split = split_by_kind.get(item.split_kind)
    if split is None:
        return
    if not (
        split.start_ns <= item.observation_start_ns
        and item.observation_end_ns <= split.end_ns
    ):
        add(
            InformationAuditRule.INPUT_OUTSIDE_SPLIT,
            "input observation interval falls outside its declared split",
            item=item,
            evidence={
                "split_start_ns": split.start_ns,
                "split_end_ns": split.end_ns,
                "observation_start_ns": item.observation_start_ns,
                "observation_end_ns": item.observation_end_ns,
            },
        )


def _audit_revision(
    item: ReconstructionInformationInputV1,
    by_id: Mapping[str, ReconstructionInformationInputV1],
    add: Any,
) -> None:
    if item.revision_sequence == 0:
        return
    if item.scope is not InformationScope.REVISION:
        add(
            InformationAuditRule.REVISION_SCOPE_UNDECLARED,
            "revised information is not labeled with revision scope",
            item=item,
            evidence={"revision_sequence": item.revision_sequence},
        )
    predecessor = by_id.get(item.supersedes_input_id or "")
    if predecessor is None:
        add(
            InformationAuditRule.REVISION_PARENT_MISSING,
            "revised information does not include its superseded vintage",
            item=item,
            evidence={"supersedes_input_id": item.supersedes_input_id},
        )
        return
    if predecessor.revision_sequence >= item.revision_sequence:
        add(
            InformationAuditRule.REVISION_SEQUENCE_INVALID,
            "revision sequence does not advance beyond the superseded input",
            item=item,
            evidence={
                "revision_sequence": item.revision_sequence,
                "superseded_revision_sequence": predecessor.revision_sequence,
            },
        )
    if predecessor.available_at_ns >= item.available_at_ns:
        add(
            InformationAuditRule.REVISION_AVAILABILITY_INVALID,
            "revision availability must follow the superseded vintage",
            item=item,
            evidence={
                "available_at_ns": item.available_at_ns,
                "superseded_available_at_ns": predecessor.available_at_ns,
            },
        )


def _audit_parent_availability(
    item: ReconstructionInformationInputV1,
    by_id: Mapping[str, ReconstructionInformationInputV1],
    add: Any,
) -> None:
    if item.input_kind is not InformationInputKind.DERIVED:
        return
    parent_times = [
        by_id[parent_id].available_at_ns
        for parent_id in item.parent_input_ids
        if parent_id in by_id
    ]
    if parent_times and item.available_at_ns < max(parent_times):
        add(
            InformationAuditRule.DERIVED_AVAILABLE_BEFORE_PARENT,
            "derived information is available before one of its parent inputs",
            item=item,
            evidence={
                "available_at_ns": item.available_at_ns,
                "latest_parent_available_at_ns": max(parent_times),
            },
        )


def _audit_ex_ante_input(
    item: ReconstructionInformationInputV1,
    add: Any,
) -> None:
    if item.allowed_lookahead_ns != 0:
        add(
            InformationAuditRule.EX_ANTE_LOOKAHEAD_DECLARED,
            "ex-ante inputs cannot declare realized look-ahead",
            item=item,
            evidence={"allowed_lookahead_ns": item.allowed_lookahead_ns},
        )
    if item.available_at_ns > item.used_at_ns:
        rule = (
            InformationAuditRule.EX_ANTE_REVISION_NOT_AVAILABLE
            if item.revision_sequence > 0
            else InformationAuditRule.EX_ANTE_INPUT_NOT_AVAILABLE
        )
        add(
            rule,
            "input was not point-in-time available when consumed",
            item=item,
            evidence={
                "available_at_ns": item.available_at_ns,
                "used_at_ns": item.used_at_ns,
                "vintage_id": item.vintage_id,
                "revision_sequence": item.revision_sequence,
            },
        )
    if item.stage not in _PREDECISION_STAGES:
        return
    scope_rule = {
        InformationScope.FUTURE_ANCHOR: InformationAuditRule.EX_ANTE_FUTURE_ANCHOR,
        InformationScope.FULL_PERIOD_SUMMARY: (
            InformationAuditRule.EX_ANTE_FULL_PERIOD_SUMMARY
        ),
        InformationScope.GLOBAL_NORMALIZATION: (
            InformationAuditRule.EX_ANTE_GLOBAL_NORMALIZATION
        ),
    }.get(item.scope)
    if scope_rule is not None:
        add(
            scope_rule,
            "ex-post-only information scope cannot feed an ex-ante stage",
            item=item,
            evidence={"scope": item.scope.value, "stage": item.stage.value},
        )
        return
    if (
        item.stage is InformationStage.MOTIF_SELECTION
        and item.observation_end_ns > item.used_at_ns
    ):
        add(
            InformationAuditRule.EX_ANTE_MOTIF_SELECTION_LEAKAGE,
            "motif selection observes data after its point-in-time use",
            item=item,
            evidence={
                "observation_end_ns": item.observation_end_ns,
                "used_at_ns": item.used_at_ns,
            },
        )
        return
    if item.event_time_ns > item.used_at_ns:
        add(
            InformationAuditRule.EX_ANTE_FUTURE_EVENT,
            "future event information feeds an ex-ante stage",
            item=item,
            evidence={
                "event_time_ns": item.event_time_ns,
                "used_at_ns": item.used_at_ns,
                "stage": item.stage.value,
            },
        )
    elif item.observation_end_ns > item.used_at_ns:
        add(
            InformationAuditRule.EX_ANTE_FUTURE_OBSERVATION,
            "observation window extends beyond its ex-ante use time",
            item=item,
            evidence={
                "observation_end_ns": item.observation_end_ns,
                "used_at_ns": item.used_at_ns,
            },
        )


def _audit_ex_post_input(
    item: ReconstructionInformationInputV1,
    policy: ReconstructionInformationPolicyV1,
    add: Any,
) -> None:
    future_delta = (
        max(
            item.event_time_ns,
            item.available_at_ns,
            item.observation_end_ns,
        )
        - item.used_at_ns
    )
    if future_delta <= 0:
        return
    if item.scope not in _EX_POST_LABELS:
        add(
            InformationAuditRule.EX_POST_UNLABELED_FUTURE_INFORMATION,
            "future-informed ex-post input lacks an explicit future scope label",
            item=item,
            evidence={
                "scope": item.scope.value,
                "required_lookahead_ns": future_delta,
            },
        )
    if (
        future_delta > item.allowed_lookahead_ns
        or future_delta > policy.max_allowed_lookahead_ns
    ):
        add(
            InformationAuditRule.EX_POST_LOOKAHEAD_EXCEEDED,
            "future-informed input exceeds declared ex-post look-ahead",
            item=item,
            evidence={
                "required_lookahead_ns": future_delta,
                "input_allowed_lookahead_ns": item.allowed_lookahead_ns,
                "policy_max_allowed_lookahead_ns": policy.max_allowed_lookahead_ns,
            },
        )


def _validity_statement(
    mode: InformationMode,
    accepted: bool,
) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    if not accepted:
        return (
            (),
            ("generation", "validation", "strategy_usefulness_claims"),
            "Rejected: invalid for generation and downstream claims until all "
            "information violations are corrected.",
        )
    if mode is InformationMode.EX_POST_RECONSTRUCTION:
        return (
            (
                "diagnostic_counterfactuals",
                "historically_informed_reconstruction",
            ),
            ("prospective_simulation", "strategy_usefulness_claims"),
            "Accepted for historically informed reconstruction and diagnostic "
            "counterfactuals; not valid for prospective strategy claims.",
        )
    return (
        (
            "historically_grounded_reconstruction",
            "point_in_time_simulation",
            "strategy_usefulness_claims",
        ),
        (),
        "Accepted for point-in-time simulation and downstream strategy claims "
        "subject to the declared chronological splits.",
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _required_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("required text value must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError("required text value cannot be empty")
    if len(normalized) > MAX_INFORMATION_TEXT_LENGTH:
        raise ValueError("text value exceeds the v1 length limit")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text value must be a string")
    normalized = value.strip()
    if not normalized:
        return None
    if len(normalized) > MAX_INFORMATION_TEXT_LENGTH:
        raise ValueError("text value exceeds the v1 length limit")
    return normalized


def _bounded_int64(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not INT64_MIN <= value <= INT64_MAX:
        raise ValueError(f"{name} is outside signed 64-bit range")
    return value


def _nonnegative_int64(value: Any, name: str) -> int:
    normalized = _bounded_int64(value, name)
    if normalized < 0:
        raise ValueError(f"{name} must be non-negative")
    return normalized


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_int(value: Any, name: str) -> int:
    normalized = _nonnegative_int(value, name)
    if normalized < 1:
        raise ValueError(f"{name} must be positive")
    return normalized


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _normalized_ids(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _enum_value(enum_type: type[_EnumT], value: Any, label: str) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except ValueError as err:
        raise ValueError(f"unsupported {label}") from err


def _bounded_json(
    value: Mapping[str, JSONValue], limit: int, label: str
) -> None:
    encoded = canonical_contract_json(value).encode("utf-8")
    if len(encoded) > limit:
        raise ValueError(f"{label} exceeds the v1 serialized-size limit")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError("unsupported schema version")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return value


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("expected a sequence of mappings")
    return tuple(_mapping(item) for item in value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("expected a sequence of strings")
    if any(not isinstance(item, str) for item in value):
        raise ValueError("expected a sequence of strings")
    return tuple(cast(Sequence[str], value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError) as err:
        raise ValueError("invalid contract JSON") from err
    return _mapping(value)


__all__ = [
    "DEFAULT_INFORMATION_AUDIT_FINDINGS",
    "MAX_INFORMATION_AUDIT_FINDINGS",
    "MAX_INFORMATION_INPUTS",
    "RECONSTRUCTION_INFORMATION_AUDIT_FINDING_SCHEMA_VERSION",
    "RECONSTRUCTION_INFORMATION_AUDIT_REPORT_SCHEMA_VERSION",
    "RECONSTRUCTION_INFORMATION_INPUT_SCHEMA_VERSION",
    "RECONSTRUCTION_INFORMATION_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_INFORMATION_POLICY_SCHEMA_VERSION",
    "RECONSTRUCTION_INFORMATION_SPLIT_SCHEMA_VERSION",
    "InformationAuditFindingV1",
    "InformationAuditReportV1",
    "InformationAuditRule",
    "InformationInputKind",
    "InformationLeakageError",
    "InformationMode",
    "InformationScope",
    "InformationSplitKind",
    "InformationStage",
    "ReconstructionInformationInputV1",
    "ReconstructionInformationManifestV1",
    "ReconstructionInformationPolicyV1",
    "ReconstructionInformationSplitV1",
    "audit_reconstruction_information",
    "reconstruction_information_window_plan_id",
    "require_reconstruction_information_audit",
]
