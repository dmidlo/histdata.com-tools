"""Predeclared promotion gates for the real reverse-degradation benchmark.

The policy in this module is intentionally independent from candidate results.
It is packaged with the distribution and content-addressed before any real
promotion report is produced.  Missing hard-gate evidence fails closed, while
missing advisory evidence remains visible without becoming an implicit pass.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from importlib import resources
from typing import Any

from histdatacom.runtime_contracts import JSONScalar, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

BENCHMARK_GATE_REQUIREMENT_SCHEMA_VERSION = (
    "histdatacom.benchmark-gate-requirement.v1"
)
BENCHMARK_PROMOTION_GATE_POLICY_SCHEMA_VERSION = (
    "histdatacom.benchmark-promotion-gate-policy.v1"
)
BENCHMARK_GATE_OBSERVATION_SCHEMA_VERSION = (
    "histdatacom.benchmark-gate-observation.v1"
)
BENCHMARK_GATE_CHECK_SCHEMA_VERSION = "histdatacom.benchmark-gate-check.v1"
BENCHMARK_PROMOTION_DECISION_SCHEMA_VERSION = (
    "histdatacom.benchmark-promotion-decision.v1"
)

DEFAULT_BENCHMARK_GATE_ASSET = (
    "assets/reverse_degradation_promotion_gates_v1.json"
)
DEFAULT_BENCHMARK_MAX_GATE_REQUIREMENTS = 128
DEFAULT_BENCHMARK_MAX_GATE_OBSERVATIONS = 512
DEFAULT_BENCHMARK_MAX_GATE_EVIDENCE_IDS = 32
DEFAULT_BENCHMARK_MAX_GATE_PAYLOAD_BYTES = 2 * 1024 * 1024

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9]*(?:[-_.][a-z0-9]+)*$")


class BenchmarkGateScope(str, Enum):
    """Whether a requirement evaluates the corpus campaign or one candidate."""

    CAMPAIGN = "campaign"
    CANDIDATE = "candidate"


class BenchmarkGateSeverity(str, Enum):
    """Whether a failed or missing observation blocks promotion."""

    HARD = "hard"
    ADVISORY = "advisory"


class BenchmarkGateComparator(str, Enum):
    """Deterministic comparison applied to one measured observation."""

    EQUAL = "equal"
    LESS_OR_EQUAL = "less-or-equal"
    GREATER_OR_EQUAL = "greater-or-equal"
    TRUE = "true"
    FALSE = "false"
    ZERO = "zero"


class BenchmarkGateStatus(str, Enum):
    """Outcome of a predeclared requirement check."""

    PASSED = "passed"
    FAILED = "failed"
    MISSING = "missing"


@dataclass(frozen=True, slots=True)
class BenchmarkGateRequirementV1:
    """One named threshold frozen before candidate results are inspected."""

    requirement_id: str
    scope: BenchmarkGateScope
    severity: BenchmarkGateSeverity
    metric_name: str
    comparator: BenchmarkGateComparator
    threshold: JSONScalar
    description: str
    schema_version: str = BENCHMARK_GATE_REQUIREMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_GATE_REQUIREMENT_SCHEMA_VERSION,
            "benchmark gate requirement",
        )
        object.__setattr__(
            self,
            "requirement_id",
            _identifier(self.requirement_id, "requirement_id"),
        )
        object.__setattr__(self, "scope", BenchmarkGateScope(self.scope))
        object.__setattr__(
            self, "severity", BenchmarkGateSeverity(self.severity)
        )
        object.__setattr__(
            self, "metric_name", _identifier(self.metric_name, "metric_name")
        )
        comparator = BenchmarkGateComparator(self.comparator)
        object.__setattr__(self, "comparator", comparator)
        object.__setattr__(
            self,
            "threshold",
            _validated_threshold(self.threshold, comparator),
        )
        object.__setattr__(
            self,
            "description",
            _bounded_text(self.description, "description", 1024),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy content."""
        return {
            "schema_version": self.schema_version,
            "requirement_id": self.requirement_id,
            "scope": self.scope.value,
            "severity": self.severity.value,
            "metric_name": self.metric_name,
            "comparator": self.comparator.value,
            "threshold": self.threshold,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkGateRequirementV1":
        """Restore and validate a requirement."""
        _require_schema(
            data,
            BENCHMARK_GATE_REQUIREMENT_SCHEMA_VERSION,
            "benchmark gate requirement",
        )
        return cls(
            requirement_id=str(data.get("requirement_id", "")),
            scope=BenchmarkGateScope(str(data.get("scope", ""))),
            severity=BenchmarkGateSeverity(str(data.get("severity", ""))),
            metric_name=str(data.get("metric_name", "")),
            comparator=BenchmarkGateComparator(str(data.get("comparator", ""))),
            threshold=_json_scalar(data.get("threshold"), "threshold"),
            description=str(data.get("description", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkPromotionGatePolicyV1:
    """Complete model-neutral gate policy committed before real results."""

    policy_name: str
    policy_version: str
    issue_number: int
    frozen_before_candidate_results: bool
    requirements: tuple[BenchmarkGateRequirementV1, ...]
    policy_id: str = ""
    schema_version: str = BENCHMARK_PROMOTION_GATE_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_PROMOTION_GATE_POLICY_SCHEMA_VERSION,
            "benchmark promotion gate policy",
        )
        object.__setattr__(
            self, "policy_name", _identifier(self.policy_name, "policy_name")
        )
        object.__setattr__(
            self,
            "policy_version",
            _identifier(self.policy_version, "policy_version"),
        )
        if isinstance(self.issue_number, bool) or self.issue_number <= 0:
            raise ValueError("issue_number must be a positive integer")
        if self.frozen_before_candidate_results is not True:
            raise ValueError(
                "benchmark gate policy must be frozen before results"
            )
        requirements = tuple(
            sorted(self.requirements, key=lambda item: item.requirement_id)
        )
        if not requirements:
            raise ValueError("benchmark gate policy requires requirements")
        if len(requirements) > DEFAULT_BENCHMARK_MAX_GATE_REQUIREMENTS:
            raise ValueError("benchmark gate requirement count exceeds limit")
        if any(
            not isinstance(item, BenchmarkGateRequirementV1)
            for item in requirements
        ):
            raise TypeError("benchmark gate requirements must use v1 contracts")
        requirement_ids = [item.requirement_id for item in requirements]
        if len(set(requirement_ids)) != len(requirement_ids):
            raise ValueError("benchmark gate requirement IDs must be unique")
        required_pairs = {
            (BenchmarkGateScope.CAMPAIGN, BenchmarkGateSeverity.HARD),
            (BenchmarkGateScope.CAMPAIGN, BenchmarkGateSeverity.ADVISORY),
            (BenchmarkGateScope.CANDIDATE, BenchmarkGateSeverity.HARD),
            (BenchmarkGateScope.CANDIDATE, BenchmarkGateSeverity.ADVISORY),
        }
        observed_pairs = {(item.scope, item.severity) for item in requirements}
        if not required_pairs <= observed_pairs:
            raise ValueError(
                "benchmark gate policy requires hard and advisory campaign and "
                "candidate requirements"
            )
        object.__setattr__(self, "requirements", requirements)
        expected = _stable_id(
            "benchmark-promotion-gates", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("benchmark promotion gate policy_id differs")
        object.__setattr__(self, "policy_id", expected)
        _ensure_payload_size(self.to_dict())

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the semantic policy content used for identity."""
        return {
            "schema_version": self.schema_version,
            "policy_name": self.policy_name,
            "policy_version": self.policy_version,
            "issue_number": self.issue_number,
            "frozen_before_candidate_results": self.frozen_before_candidate_results,
            "requirements": [item.to_dict() for item in self.requirements],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic policy JSON."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        """Serialize the policy canonically."""
        serialized: str = canonical_contract_json(self.to_dict())
        return serialized

    def requirements_for(
        self, scope: BenchmarkGateScope
    ) -> tuple[BenchmarkGateRequirementV1, ...]:
        """Return requirements for one evaluation scope."""
        selected = BenchmarkGateScope(scope)
        return tuple(
            item for item in self.requirements if item.scope is selected
        )

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BenchmarkPromotionGatePolicyV1":
        """Restore and verify a policy."""
        _require_schema(
            data,
            BENCHMARK_PROMOTION_GATE_POLICY_SCHEMA_VERSION,
            "benchmark promotion gate policy",
        )
        raw_requirements = data.get("requirements")
        if not isinstance(raw_requirements, Sequence) or isinstance(
            raw_requirements, (str, bytes)
        ):
            raise ValueError("benchmark gate requirements must be a sequence")
        return cls(
            policy_name=str(data.get("policy_name", "")),
            policy_version=str(data.get("policy_version", "")),
            issue_number=_strict_int(data.get("issue_number"), "issue_number"),
            frozen_before_candidate_results=_strict_bool(
                data.get("frozen_before_candidate_results"),
                "frozen_before_candidate_results",
            ),
            requirements=tuple(
                BenchmarkGateRequirementV1.from_dict(_mapping(item))
                for item in raw_requirements
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BenchmarkPromotionGatePolicyV1":
        """Restore and verify a policy from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BenchmarkGateObservationV1:
    """One measured metric bound to compact evidence identities."""

    scope: BenchmarkGateScope
    subject_id: str
    metric_name: str
    value: JSONScalar
    evidence_ids: tuple[str, ...]
    observation_id: str = ""
    schema_version: str = BENCHMARK_GATE_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_GATE_OBSERVATION_SCHEMA_VERSION,
            "benchmark gate observation",
        )
        object.__setattr__(self, "scope", BenchmarkGateScope(self.scope))
        object.__setattr__(
            self,
            "subject_id",
            _bounded_text(self.subject_id, "subject_id", 512),
        )
        object.__setattr__(
            self, "metric_name", _identifier(self.metric_name, "metric_name")
        )
        object.__setattr__(self, "value", _json_scalar(self.value, "value"))
        evidence = tuple(
            sorted(
                {
                    _bounded_text(v, "evidence_id", 512)
                    for v in self.evidence_ids
                }
            )
        )
        if not evidence:
            raise ValueError("benchmark gate observation requires evidence IDs")
        if len(evidence) > DEFAULT_BENCHMARK_MAX_GATE_EVIDENCE_IDS:
            raise ValueError("benchmark gate evidence count exceeds limit")
        object.__setattr__(self, "evidence_ids", evidence)
        expected = _stable_id(
            "benchmark-gate-observation", self.identity_payload()
        )
        supplied = _optional_text(self.observation_id)
        if supplied is not None and supplied != expected:
            raise ValueError("benchmark gate observation_id differs")
        object.__setattr__(self, "observation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return content-addressed observation fields."""
        return {
            "schema_version": self.schema_version,
            "scope": self.scope.value,
            "subject_id": self.subject_id,
            "metric_name": self.metric_name,
            "value": self.value,
            "evidence_ids": list(self.evidence_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic observation JSON."""
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkGateObservationV1":
        """Restore and verify an observation."""
        _require_schema(
            data,
            BENCHMARK_GATE_OBSERVATION_SCHEMA_VERSION,
            "benchmark gate observation",
        )
        return cls(
            scope=BenchmarkGateScope(str(data.get("scope", ""))),
            subject_id=str(data.get("subject_id", "")),
            metric_name=str(data.get("metric_name", "")),
            value=_json_scalar(data.get("value"), "value"),
            evidence_ids=_string_tuple(data.get("evidence_ids")),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkGateCheckV1:
    """Evaluation of one policy requirement."""

    requirement_id: str
    observation_id: str | None
    status: BenchmarkGateStatus
    blocking: bool
    reason: str
    check_id: str = ""
    schema_version: str = BENCHMARK_GATE_CHECK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_GATE_CHECK_SCHEMA_VERSION,
            "benchmark gate check",
        )
        object.__setattr__(
            self,
            "requirement_id",
            _identifier(self.requirement_id, "requirement_id"),
        )
        observation_id = _optional_text(self.observation_id)
        object.__setattr__(self, "observation_id", observation_id)
        status = BenchmarkGateStatus(self.status)
        object.__setattr__(self, "status", status)
        if not isinstance(self.blocking, bool):
            raise ValueError("blocking must be boolean")
        if status is BenchmarkGateStatus.PASSED and self.blocking:
            raise ValueError("a passing gate check cannot block")
        if status is BenchmarkGateStatus.MISSING and observation_id is not None:
            raise ValueError(
                "a missing gate check cannot reference an observation"
            )
        object.__setattr__(
            self, "reason", _bounded_text(self.reason, "reason", 1024)
        )
        expected = _stable_id("benchmark-gate-check", self.identity_payload())
        supplied = _optional_text(self.check_id)
        if supplied is not None and supplied != expected:
            raise ValueError("benchmark gate check_id differs")
        object.__setattr__(self, "check_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic check identity content."""
        return {
            "schema_version": self.schema_version,
            "requirement_id": self.requirement_id,
            "observation_id": self.observation_id,
            "status": self.status.value,
            "blocking": self.blocking,
            "reason": self.reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic check JSON."""
        return {**self.identity_payload(), "check_id": self.check_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkGateCheckV1":
        """Restore and verify a gate check."""
        _require_schema(
            data,
            BENCHMARK_GATE_CHECK_SCHEMA_VERSION,
            "benchmark gate check",
        )
        raw_observation_id = data.get("observation_id")
        return cls(
            requirement_id=str(data.get("requirement_id", "")),
            observation_id=(
                None if raw_observation_id is None else str(raw_observation_id)
            ),
            status=BenchmarkGateStatus(str(data.get("status", ""))),
            blocking=_strict_bool(data.get("blocking"), "blocking"),
            reason=str(data.get("reason", "")),
            check_id=str(data.get("check_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkPromotionDecisionV1:
    """Fail-closed result for one campaign or candidate subject."""

    policy_id: str
    scope: BenchmarkGateScope
    subject_id: str
    checks: tuple[BenchmarkGateCheckV1, ...]
    promotion_eligible: bool
    automatic_winner: bool = False
    decision_id: str = ""
    schema_version: str = BENCHMARK_PROMOTION_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_PROMOTION_DECISION_SCHEMA_VERSION,
            "benchmark promotion decision",
        )
        object.__setattr__(
            self, "policy_id", _bounded_text(self.policy_id, "policy_id", 512)
        )
        object.__setattr__(self, "scope", BenchmarkGateScope(self.scope))
        object.__setattr__(
            self,
            "subject_id",
            _bounded_text(self.subject_id, "subject_id", 512),
        )
        checks = tuple(
            sorted(self.checks, key=lambda item: item.requirement_id)
        )
        if not checks:
            raise ValueError("benchmark promotion decision requires checks")
        if any(not isinstance(item, BenchmarkGateCheckV1) for item in checks):
            raise TypeError("benchmark promotion checks must use v1 contracts")
        if len({item.requirement_id for item in checks}) != len(checks):
            raise ValueError("benchmark promotion checks must be unique")
        object.__setattr__(self, "checks", checks)
        expected_eligible = not any(item.blocking for item in checks)
        if self.promotion_eligible != expected_eligible:
            raise ValueError(
                "promotion eligibility differs from blocking checks"
            )
        if self.automatic_winner is not False:
            raise ValueError(
                "benchmark promotion decisions never select a winner"
            )
        expected = _stable_id(
            "benchmark-promotion-decision", self.identity_payload()
        )
        supplied = _optional_text(self.decision_id)
        if supplied is not None and supplied != expected:
            raise ValueError("benchmark promotion decision_id differs")
        object.__setattr__(self, "decision_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic decision content."""
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "scope": self.scope.value,
            "subject_id": self.subject_id,
            "checks": [item.to_dict() for item in self.checks],
            "promotion_eligible": self.promotion_eligible,
            "automatic_winner": self.automatic_winner,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic decision JSON."""
        return {**self.identity_payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BenchmarkPromotionDecisionV1":
        """Restore and verify a promotion decision."""
        _require_schema(
            data,
            BENCHMARK_PROMOTION_DECISION_SCHEMA_VERSION,
            "benchmark promotion decision",
        )
        raw_checks = data.get("checks")
        if not isinstance(raw_checks, Sequence) or isinstance(
            raw_checks, (str, bytes)
        ):
            raise ValueError("benchmark promotion checks must be a sequence")
        return cls(
            policy_id=str(data.get("policy_id", "")),
            scope=BenchmarkGateScope(str(data.get("scope", ""))),
            subject_id=str(data.get("subject_id", "")),
            checks=tuple(
                BenchmarkGateCheckV1.from_dict(_mapping(item))
                for item in raw_checks
            ),
            promotion_eligible=_strict_bool(
                data.get("promotion_eligible"), "promotion_eligible"
            ),
            automatic_winner=_strict_bool(
                data.get("automatic_winner"), "automatic_winner"
            ),
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def evaluate_benchmark_promotion_gates(
    policy: BenchmarkPromotionGatePolicyV1,
    observations: Sequence[BenchmarkGateObservationV1],
    *,
    scope: BenchmarkGateScope,
    subject_id: str,
) -> BenchmarkPromotionDecisionV1:
    """Evaluate predeclared gates without ranking or selecting candidates."""
    if not isinstance(policy, BenchmarkPromotionGatePolicyV1):
        raise TypeError("benchmark promotion evaluation requires a v1 policy")
    selected_scope = BenchmarkGateScope(scope)
    selected_subject = _bounded_text(subject_id, "subject_id", 512)
    if len(observations) > DEFAULT_BENCHMARK_MAX_GATE_OBSERVATIONS:
        raise ValueError("benchmark gate observation count exceeds limit")
    relevant = tuple(
        item
        for item in observations
        if item.scope is selected_scope and item.subject_id == selected_subject
    )
    if any(
        not isinstance(item, BenchmarkGateObservationV1) for item in relevant
    ):
        raise TypeError("benchmark gate observations must use v1 contracts")
    by_metric: dict[str, BenchmarkGateObservationV1] = {}
    for item in relevant:
        if item.metric_name in by_metric:
            raise ValueError("duplicate benchmark gate metric observation")
        by_metric[item.metric_name] = item
    checks: list[BenchmarkGateCheckV1] = []
    for requirement in policy.requirements_for(selected_scope):
        observation = by_metric.get(requirement.metric_name)
        if observation is None:
            checks.append(
                BenchmarkGateCheckV1(
                    requirement_id=requirement.requirement_id,
                    observation_id=None,
                    status=BenchmarkGateStatus.MISSING,
                    blocking=requirement.severity is BenchmarkGateSeverity.HARD,
                    reason="required metric observation is missing",
                )
            )
            continue
        passed = _compare(observation.value, requirement)
        checks.append(
            BenchmarkGateCheckV1(
                requirement_id=requirement.requirement_id,
                observation_id=observation.observation_id,
                status=(
                    BenchmarkGateStatus.PASSED
                    if passed
                    else BenchmarkGateStatus.FAILED
                ),
                blocking=(
                    not passed
                    and requirement.severity is BenchmarkGateSeverity.HARD
                ),
                reason=(
                    "measured value satisfies the predeclared threshold"
                    if passed
                    else "measured value violates the predeclared threshold"
                ),
            )
        )
    return BenchmarkPromotionDecisionV1(
        policy_id=policy.policy_id,
        scope=selected_scope,
        subject_id=selected_subject,
        checks=tuple(checks),
        promotion_eligible=not any(item.blocking for item in checks),
    )


def load_default_benchmark_promotion_gate_policy() -> (
    BenchmarkPromotionGatePolicyV1
):
    """Load the packaged issue-#463 policy and verify its content identity."""
    asset = resources.files("histdatacom.synthetic").joinpath(
        DEFAULT_BENCHMARK_GATE_ASSET
    )
    return BenchmarkPromotionGatePolicyV1.from_json(
        asset.read_text(encoding="utf-8")
    )


def _compare(
    measured: JSONScalar, requirement: BenchmarkGateRequirementV1
) -> bool:
    comparator = requirement.comparator
    threshold = requirement.threshold
    if comparator is BenchmarkGateComparator.TRUE:
        return measured is True
    if comparator is BenchmarkGateComparator.FALSE:
        return measured is False
    if comparator is BenchmarkGateComparator.ZERO:
        return _number(measured, "measured value") == 0.0
    if comparator is BenchmarkGateComparator.EQUAL:
        if isinstance(threshold, (int, float)) and not isinstance(
            threshold, bool
        ):
            return _number(measured, "measured value") == float(threshold)
        equal: bool = measured == threshold
        return equal
    measured_number = _number(measured, "measured value")
    threshold_number = _number(threshold, "threshold")
    if comparator is BenchmarkGateComparator.LESS_OR_EQUAL:
        return measured_number <= threshold_number
    return measured_number >= threshold_number


def _validated_threshold(
    value: JSONScalar, comparator: BenchmarkGateComparator
) -> JSONScalar:
    selected = _json_scalar(value, "threshold")
    if comparator in {
        BenchmarkGateComparator.TRUE,
        BenchmarkGateComparator.FALSE,
    }:
        if not isinstance(selected, bool):
            raise ValueError(
                "boolean gate comparator requires boolean threshold"
            )
        expected = comparator is BenchmarkGateComparator.TRUE
        if selected is not expected:
            raise ValueError("boolean gate threshold differs from comparator")
    elif comparator is BenchmarkGateComparator.ZERO:
        if _number(selected, "threshold") != 0.0:
            raise ValueError("zero gate comparator requires a zero threshold")
    elif comparator in {
        BenchmarkGateComparator.LESS_OR_EQUAL,
        BenchmarkGateComparator.GREATER_OR_EQUAL,
    }:
        _number(selected, "threshold")
    return selected


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _ensure_payload_size(payload: Mapping[str, JSONValue]) -> None:
    if (
        len(canonical_contract_json(dict(payload)).encode("utf-8"))
        > DEFAULT_BENCHMARK_MAX_GATE_PAYLOAD_BYTES
    ):
        raise ValueError("benchmark gate payload exceeds size limit")


def _require_schema(data: Mapping[str, Any], expected: str, label: str) -> None:
    _require_schema_value(str(data.get("schema_version", "")), expected, label)


def _require_schema_value(value: str, expected: str, label: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported {label} schema")


def _identifier(value: Any, name: str) -> str:
    selected = _bounded_text(value, name, 256)
    if _IDENTIFIER.fullmatch(selected) is None:
        raise ValueError(f"{name} must be a stable lowercase identifier")
    return selected


def _bounded_text(value: Any, name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be text")
    selected = value.strip()
    if not selected or len(selected) > maximum:
        raise ValueError(f"{name} is empty or exceeds its length limit")
    return selected


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text value must be text")
    selected = value.strip()
    return selected or None


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError(f"{name} must be a finite JSON scalar")


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    selected = float(value)
    if not math.isfinite(selected):
        raise ValueError(f"{name} must be finite")
    return selected


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("expected a string sequence")
    if any(not isinstance(item, str) for item in value):
        raise ValueError("expected a string sequence")
    return tuple(value)


def _json_mapping(text: str) -> Mapping[str, Any]:
    if len(text.encode("utf-8")) > DEFAULT_BENCHMARK_MAX_GATE_PAYLOAD_BYTES:
        raise ValueError("benchmark gate JSON exceeds size limit")
    value = json.loads(text)
    if not isinstance(value, Mapping):
        raise ValueError("benchmark gate JSON must contain an object")
    return value


__all__ = [
    "BENCHMARK_GATE_CHECK_SCHEMA_VERSION",
    "BENCHMARK_GATE_OBSERVATION_SCHEMA_VERSION",
    "BENCHMARK_GATE_REQUIREMENT_SCHEMA_VERSION",
    "BENCHMARK_PROMOTION_DECISION_SCHEMA_VERSION",
    "BENCHMARK_PROMOTION_GATE_POLICY_SCHEMA_VERSION",
    "DEFAULT_BENCHMARK_GATE_ASSET",
    "BenchmarkGateCheckV1",
    "BenchmarkGateComparator",
    "BenchmarkGateObservationV1",
    "BenchmarkGateRequirementV1",
    "BenchmarkGateScope",
    "BenchmarkGateSeverity",
    "BenchmarkGateStatus",
    "BenchmarkPromotionDecisionV1",
    "BenchmarkPromotionGatePolicyV1",
    "evaluate_benchmark_promotion_gates",
    "load_default_benchmark_promotion_gate_policy",
]
