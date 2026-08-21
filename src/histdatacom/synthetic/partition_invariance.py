"""Adaptive-window partition-invariance qualification.

The reconstruction planner may recursively split an interval to keep modeled
cardinality below an engine safety limit.  This module makes the scientific
effect of that operational partitioning measurable.  It binds coarsest,
planner-selected, and deterministic finer partitions to one release candidate,
audits half-open source ownership and prior-only history, derives child seeds
from a common semantic parent, and compares replicate feature distributions.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from itertools import pairwise
from pathlib import Path
from statistics import fmean
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

ADAPTIVE_PARTITION_INTERVAL_SCHEMA_VERSION = (
    "histdatacom.adaptive-partition-interval.v1"
)
ADAPTIVE_PARTITION_SPEC_SCHEMA_VERSION = (
    "histdatacom.adaptive-partition-spec.v1"
)
PARTITION_SOURCE_OWNERSHIP_AUDIT_SCHEMA_VERSION = (
    "histdatacom.partition-source-ownership-audit.v1"
)
PARTITION_HISTORY_AUDIT_SCHEMA_VERSION = (
    "histdatacom.partition-history-audit.v1"
)
PARTITION_SEED_LEDGER_SCHEMA_VERSION = "histdatacom.partition-seed-ledger.v1"
PARTITION_METRIC_TOLERANCE_SCHEMA_VERSION = (
    "histdatacom.partition-metric-tolerance.v1"
)
PARTITION_INVARIANCE_POLICY_SCHEMA_VERSION = (
    "histdatacom.partition-invariance-policy.v1"
)
PARTITION_INVARIANCE_CASE_SCHEMA_VERSION = (
    "histdatacom.partition-invariance-case.v1"
)
PARTITION_INVARIANCE_RUN_SCHEMA_VERSION = (
    "histdatacom.partition-invariance-run.v1"
)
PARTITION_INVARIANCE_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.partition-invariance-comparison.v1"
)
PARTITION_INVARIANCE_QUALIFICATION_SCHEMA_VERSION = (
    "histdatacom.partition-invariance-qualification.v1"
)

MAX_PARTITION_INTERVALS = 16_384
MAX_PARTITION_CASES = 4_096
MAX_PARTITION_RUNS = 65_536
MAX_PARTITION_FEATURES = 256
MAX_PARTITION_HISTORY_EVENTS = 1_000_000
MAX_PARTITION_ARTIFACT_BYTES = 64 * 1024 * 1024

REQUIRED_PARTITION_METRICS = frozenset(
    {
        "boundary_discontinuity",
        "duration_dependence",
        "interarrival_dependence",
        "mark_transition_distance",
        "maximum_excursion",
        "path_variation",
        "post_triangle_residual",
        "pre_triangle_residual",
        "projection_burden",
        "resource_work",
        "reversal_count",
        "runtime_seconds",
        "spread_variation",
        "synthetic_count_eurgbp",
        "synthetic_count_eurusd",
        "synthetic_count_gbpusd",
        "synthetic_count_total",
        "update_transition_distance",
    }
)
OPTIONAL_PARTITION_METRICS = frozenset({"strategy_sensitivity"})

_SHA256 = re.compile(r"[0-9a-f]{64}")


class AdaptivePartitionKind(str, Enum):
    """The three predeclared partition treatments for every source span."""

    COARSEST = "coarsest"
    PLANNER = "planner_selected"
    FINER = "deterministic_finer"


class PartitionToleranceSeverity(str, Enum):
    """Whether a tolerance breach blocks the full campaign."""

    HARD = "hard"
    ADVISORY = "advisory"


class PartitionQualificationStatus(str, Enum):
    """Fail-closed partition-invariance qualification status."""

    PASS = "pass"
    FAIL = "fail"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


@dataclass(frozen=True, slots=True)
class AdaptivePartitionIntervalV1:
    """One exact half-open child interval."""

    start_ns: int
    end_ns: int
    interval_id: str = ""
    schema_version: str = ADAPTIVE_PARTITION_INTERVAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, ADAPTIVE_PARTITION_INTERVAL_SCHEMA_VERSION
        )
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("adaptive partition interval is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        expected = _stable_id("adaptive-partition-interval", self.payload())
        if self.interval_id and self.interval_id != expected:
            raise ValueError("adaptive partition interval identity differs")
        object.__setattr__(self, "interval_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "ownership": "half-open-start-inclusive-end-exclusive",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "interval_id": self.interval_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AdaptivePartitionIntervalV1:
        if data.get("ownership") != ("half-open-start-inclusive-end-exclusive"):
            raise ValueError("adaptive partition ownership policy differs")
        return cls(
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            interval_id=str(data.get("interval_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class AdaptivePartitionSpecV1:
    """One contiguous partition of a common semantic parent span."""

    parent_span_id: str
    parent_start_ns: int
    parent_end_ns: int
    kind: AdaptivePartitionKind
    intervals: tuple[AdaptivePartitionIntervalV1, ...]
    partition_id: str = ""
    schema_version: str = ADAPTIVE_PARTITION_SPEC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, ADAPTIVE_PARTITION_SPEC_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "parent_span_id", _required_text(self.parent_span_id)
        )
        start = _int64(self.parent_start_ns, "parent_start_ns")
        end = _int64(self.parent_end_ns, "parent_end_ns")
        if end <= start:
            raise ValueError("adaptive partition parent span is empty")
        object.__setattr__(self, "parent_start_ns", start)
        object.__setattr__(self, "parent_end_ns", end)
        kind = AdaptivePartitionKind(self.kind)
        object.__setattr__(self, "kind", kind)
        intervals = tuple(
            sorted(self.intervals, key=lambda item: item.start_ns)
        )
        if (
            not intervals
            or len(intervals) > MAX_PARTITION_INTERVALS
            or any(
                not isinstance(item, AdaptivePartitionIntervalV1)
                for item in intervals
            )
        ):
            raise ValueError("adaptive partition intervals are invalid")
        if intervals[0].start_ns != start or intervals[-1].end_ns != end:
            raise ValueError("adaptive partition does not cover parent span")
        for left, right in pairwise(intervals):
            if left.end_ns != right.start_ns:
                raise ValueError("adaptive partition is not contiguous")
        object.__setattr__(self, "intervals", intervals)
        expected = _stable_id("adaptive-partition", self.payload())
        if self.partition_id and self.partition_id != expected:
            raise ValueError("adaptive partition identity differs")
        object.__setattr__(self, "partition_id", expected)

    @property
    def boundaries(self) -> tuple[int, ...]:
        """Return ordered outer and internal half-open boundaries."""
        return (self.parent_start_ns, *(item.end_ns for item in self.intervals))

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "parent_span_id": self.parent_span_id,
            "parent_start_ns": self.parent_start_ns,
            "parent_end_ns": self.parent_end_ns,
            "kind": self.kind.value,
            "intervals": [item.to_dict() for item in self.intervals],
            "contiguous_half_open_cover": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "partition_id": self.partition_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AdaptivePartitionSpecV1:
        if data.get("contiguous_half_open_cover") is not True:
            raise ValueError("adaptive partition cover policy differs")
        return cls(
            parent_span_id=str(data.get("parent_span_id", "")),
            parent_start_ns=_strict_int(
                data.get("parent_start_ns"), "parent_start_ns"
            ),
            parent_end_ns=_strict_int(
                data.get("parent_end_ns"), "parent_end_ns"
            ),
            kind=AdaptivePartitionKind(str(data.get("kind", ""))),
            intervals=tuple(
                AdaptivePartitionIntervalV1.from_dict(_mapping(item))
                for item in _sequence(data.get("intervals"))
            ),
            partition_id=str(data.get("partition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PartitionSourceOwnershipAuditV1:
    """Aggregate proof that a partition owns every source anchor once."""

    partition_id: str
    source_event_count: int
    assigned_event_count: int
    lost_event_count: int
    duplicate_event_count: int
    boundary_event_count: int
    anchor_event_count: int
    missing_anchor_count: int
    source_content_sha256: str
    assignment_content_sha256: str
    audit_id: str = ""
    schema_version: str = PARTITION_SOURCE_OWNERSHIP_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PARTITION_SOURCE_OWNERSHIP_AUDIT_SCHEMA_VERSION,
        )
        object.__setattr__(
            self, "partition_id", _required_text(self.partition_id)
        )
        for name in (
            "source_event_count",
            "assigned_event_count",
            "lost_event_count",
            "duplicate_event_count",
            "boundary_event_count",
            "anchor_event_count",
            "missing_anchor_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "source_content_sha256",
            _sha256(self.source_content_sha256),
        )
        object.__setattr__(
            self,
            "assignment_content_sha256",
            _sha256(self.assignment_content_sha256),
        )
        if self.assigned_event_count + self.lost_event_count != (
            self.source_event_count
        ):
            raise ValueError("partition ownership counts do not reconcile")
        if self.boundary_event_count > self.assigned_event_count:
            raise ValueError("partition boundary count exceeds assignments")
        if self.missing_anchor_count > self.anchor_event_count:
            raise ValueError("partition missing anchor count differs")
        expected = _stable_id("partition-source-ownership", self.payload())
        if self.audit_id and self.audit_id != expected:
            raise ValueError("partition source ownership audit differs")
        object.__setattr__(self, "audit_id", expected)

    @property
    def passed(self) -> bool:
        return (
            self.lost_event_count == 0
            and self.duplicate_event_count == 0
            and self.missing_anchor_count == 0
            and self.assigned_event_count == self.source_event_count
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "partition_id": self.partition_id,
            "source_event_count": self.source_event_count,
            "assigned_event_count": self.assigned_event_count,
            "lost_event_count": self.lost_event_count,
            "duplicate_event_count": self.duplicate_event_count,
            "boundary_event_count": self.boundary_event_count,
            "anchor_event_count": self.anchor_event_count,
            "missing_anchor_count": self.missing_anchor_count,
            "source_content_sha256": self.source_content_sha256,
            "assignment_content_sha256": self.assignment_content_sha256,
            "passed": self.passed,
            "ownership": "strict-half-open-exactly-once-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PartitionSourceOwnershipAuditV1:
        if data.get("ownership") != "strict-half-open-exactly-once-v1":
            raise ValueError("partition ownership audit policy differs")
        result = cls(
            partition_id=str(data.get("partition_id", "")),
            source_event_count=_strict_int(
                data.get("source_event_count"), "source_event_count"
            ),
            assigned_event_count=_strict_int(
                data.get("assigned_event_count"), "assigned_event_count"
            ),
            lost_event_count=_strict_int(
                data.get("lost_event_count"), "lost_event_count"
            ),
            duplicate_event_count=_strict_int(
                data.get("duplicate_event_count"), "duplicate_event_count"
            ),
            boundary_event_count=_strict_int(
                data.get("boundary_event_count"), "boundary_event_count"
            ),
            anchor_event_count=_strict_int(
                data.get("anchor_event_count"), "anchor_event_count"
            ),
            missing_anchor_count=_strict_int(
                data.get("missing_anchor_count"), "missing_anchor_count"
            ),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            assignment_content_sha256=str(
                data.get("assignment_content_sha256", "")
            ),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("passed") is not result.passed:
            raise ValueError("partition ownership audit status differs")
        return result


@dataclass(frozen=True, slots=True)
class PartitionHistoryAuditV1:
    """Aggregate proof that child carry/history is bounded and prior-only."""

    partition_id: str
    maximum_history_ns: int
    history_event_count: int
    interval_history_counts: Mapping[str, int]
    future_event_count: int
    out_of_bound_event_count: int
    unknown_source_event_count: int
    history_content_sha256: str
    audit_id: str = ""
    schema_version: str = PARTITION_HISTORY_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PARTITION_HISTORY_AUDIT_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "partition_id", _required_text(self.partition_id)
        )
        object.__setattr__(
            self,
            "maximum_history_ns",
            _nonnegative_int(self.maximum_history_ns, "maximum_history_ns"),
        )
        for name in (
            "history_event_count",
            "future_event_count",
            "out_of_bound_event_count",
            "unknown_source_event_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        counts = {
            _required_text(key): _nonnegative_int(
                value, f"interval_history_counts.{key}"
            )
            for key, value in sorted(self.interval_history_counts.items())
        }
        if sum(counts.values()) != self.history_event_count:
            raise ValueError("partition history counts do not reconcile")
        object.__setattr__(self, "interval_history_counts", counts)
        object.__setattr__(
            self,
            "history_content_sha256",
            _sha256(self.history_content_sha256),
        )
        expected = _stable_id("partition-history-audit", self.payload())
        if self.audit_id and self.audit_id != expected:
            raise ValueError("partition history audit identity differs")
        object.__setattr__(self, "audit_id", expected)

    @property
    def passed(self) -> bool:
        return (
            self.future_event_count == 0
            and self.out_of_bound_event_count == 0
            and self.unknown_source_event_count == 0
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "partition_id": self.partition_id,
            "maximum_history_ns": self.maximum_history_ns,
            "history_event_count": self.history_event_count,
            "interval_history_counts": dict(self.interval_history_counts),
            "future_event_count": self.future_event_count,
            "out_of_bound_event_count": self.out_of_bound_event_count,
            "unknown_source_event_count": self.unknown_source_event_count,
            "history_content_sha256": self.history_content_sha256,
            "passed": self.passed,
            "history_policy": "bounded-strictly-prior-source-only-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PartitionHistoryAuditV1:
        if data.get("history_policy") != (
            "bounded-strictly-prior-source-only-v1"
        ):
            raise ValueError("partition history audit policy differs")
        result = cls(
            partition_id=str(data.get("partition_id", "")),
            maximum_history_ns=_strict_int(
                data.get("maximum_history_ns"), "maximum_history_ns"
            ),
            history_event_count=_strict_int(
                data.get("history_event_count"), "history_event_count"
            ),
            interval_history_counts=_int_mapping(
                data.get("interval_history_counts")
            ),
            future_event_count=_strict_int(
                data.get("future_event_count"), "future_event_count"
            ),
            out_of_bound_event_count=_strict_int(
                data.get("out_of_bound_event_count"),
                "out_of_bound_event_count",
            ),
            unknown_source_event_count=_strict_int(
                data.get("unknown_source_event_count"),
                "unknown_source_event_count",
            ),
            history_content_sha256=str(data.get("history_content_sha256", "")),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("passed") is not result.passed:
            raise ValueError("partition history audit status differs")
        return result


@dataclass(frozen=True, slots=True)
class PartitionSeedLedgerV1:
    """Hierarchical child seeds derived from one semantic parent identity."""

    candidate_id: str
    partition_id: str
    parent_span_id: str
    model_fit_id: str
    observation_scenario_id: str
    semantic_member_id: str
    base_seed: int
    parent_seed: int
    child_seeds: Mapping[str, int]
    ledger_id: str = ""
    schema_version: str = PARTITION_SEED_LEDGER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PARTITION_SEED_LEDGER_SCHEMA_VERSION
        )
        for name in (
            "candidate_id",
            "partition_id",
            "parent_span_id",
            "model_fit_id",
            "observation_scenario_id",
            "semantic_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in ("base_seed", "parent_seed"):
            object.__setattr__(self, name, _uint64(getattr(self, name), name))
        seeds = {
            _required_text(key): _uint64(value, f"child_seeds.{key}")
            for key, value in sorted(self.child_seeds.items())
        }
        if not seeds or len(seeds) > MAX_PARTITION_INTERVALS:
            raise ValueError("partition child seed ledger is invalid")
        object.__setattr__(self, "child_seeds", seeds)
        expected = _stable_id("partition-seed-ledger", self.payload())
        if self.ledger_id and self.ledger_id != expected:
            raise ValueError("partition seed ledger identity differs")
        object.__setattr__(self, "ledger_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "partition_id": self.partition_id,
            "parent_span_id": self.parent_span_id,
            "model_fit_id": self.model_fit_id,
            "observation_scenario_id": self.observation_scenario_id,
            "semantic_member_id": self.semantic_member_id,
            "base_seed": self.base_seed,
            "parent_seed": self.parent_seed,
            "child_seeds": dict(self.child_seeds),
            "seed_policy": "hierarchical-semantic-parent-and-child-bounds-v1",
            "worker_count_in_seed": False,
            "retry_attempt_in_seed": False,
            "partition_ordinal_in_seed": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "ledger_id": self.ledger_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PartitionSeedLedgerV1:
        expected = {
            "seed_policy": "hierarchical-semantic-parent-and-child-bounds-v1",
            "worker_count_in_seed": False,
            "retry_attempt_in_seed": False,
            "partition_ordinal_in_seed": False,
        }
        for name, value in expected.items():
            if data.get(name) != value:
                raise ValueError(f"partition seed policy {name} differs")
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            partition_id=str(data.get("partition_id", "")),
            parent_span_id=str(data.get("parent_span_id", "")),
            model_fit_id=str(data.get("model_fit_id", "")),
            observation_scenario_id=str(
                data.get("observation_scenario_id", "")
            ),
            semantic_member_id=str(data.get("semantic_member_id", "")),
            base_seed=_strict_int(data.get("base_seed"), "base_seed"),
            parent_seed=_strict_int(data.get("parent_seed"), "parent_seed"),
            child_seeds=_int_mapping(data.get("child_seeds")),
            ledger_id=str(data.get("ledger_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PartitionMetricToleranceV1:
    """Predeclared absolute and relative tolerance for one metric."""

    metric_name: str
    absolute_tolerance: float
    relative_tolerance: float
    severity: PartitionToleranceSeverity
    tolerance_id: str = ""
    schema_version: str = PARTITION_METRIC_TOLERANCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PARTITION_METRIC_TOLERANCE_SCHEMA_VERSION
        )
        metric = _required_text(self.metric_name)
        if (
            metric
            not in REQUIRED_PARTITION_METRICS | OPTIONAL_PARTITION_METRICS
        ):
            raise ValueError("unsupported partition-invariance metric")
        object.__setattr__(self, "metric_name", metric)
        for name in ("absolute_tolerance", "relative_tolerance"):
            value = _nonnegative_finite(getattr(self, name), name)
            object.__setattr__(self, name, value)
        object.__setattr__(
            self, "severity", PartitionToleranceSeverity(self.severity)
        )
        expected = _stable_id("partition-metric-tolerance", self.payload())
        if self.tolerance_id and self.tolerance_id != expected:
            raise ValueError("partition metric tolerance identity differs")
        object.__setattr__(self, "tolerance_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric_name": self.metric_name,
            "absolute_tolerance": self.absolute_tolerance,
            "relative_tolerance": self.relative_tolerance,
            "severity": self.severity.value,
            "comparator": ("breach-only-when-absolute-and-relative-exceed-v1"),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "tolerance_id": self.tolerance_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PartitionMetricToleranceV1:
        if data.get("comparator") != (
            "breach-only-when-absolute-and-relative-exceed-v1"
        ):
            raise ValueError("partition metric comparator differs")
        return cls(
            metric_name=str(data.get("metric_name", "")),
            absolute_tolerance=_strict_number(
                data.get("absolute_tolerance"), "absolute_tolerance"
            ),
            relative_tolerance=_strict_number(
                data.get("relative_tolerance"), "relative_tolerance"
            ),
            severity=PartitionToleranceSeverity(str(data.get("severity", ""))),
            tolerance_id=str(data.get("tolerance_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PartitionInvariancePolicyV1:
    """Predeclared power, coverage, energy, and metric acceptance policy."""

    metric_tolerances: tuple[PartitionMetricToleranceV1, ...]
    energy_distance_hard_limit: float = 0.25
    energy_distance_advisory_limit: float = 0.15
    minimum_replicates_per_partition: int = 4
    minimum_case_count: int = 3
    minimum_feature_dimension: int = 4
    target_power: float = 0.8
    alpha: float = 0.05
    required_split_depth_strata: tuple[str, ...] = (
        "no_split",
        "binary_split",
        "deep_recursive_split",
    )
    required_epoch_strata: tuple[str, ...] = (
        "early_sparse",
        "qualified_transition",
        "modern_dense",
    )
    required_activity_strata: tuple[str, ...] = ("quiet", "high_activity")
    required_context_strata: tuple[str, ...] = ("ordinary", "event")
    required_alignment_kinds: tuple[str, ...] = (
        "exact",
        "bounded_nearest",
    )
    required_observation_scenarios: tuple[str, ...] = (
        "high_retention_low_infill",
        "central_fitted_retention",
        "low_retention_high_infill",
    )
    policy_id: str = ""
    schema_version: str = PARTITION_INVARIANCE_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PARTITION_INVARIANCE_POLICY_SCHEMA_VERSION
        )
        tolerances = tuple(
            sorted(self.metric_tolerances, key=lambda item: item.metric_name)
        )
        if len({item.metric_name for item in tolerances}) != len(tolerances):
            raise ValueError("partition metric tolerances contain duplicates")
        names = {item.metric_name for item in tolerances}
        if not REQUIRED_PARTITION_METRICS.issubset(names):
            raise ValueError("partition metric tolerances are incomplete")
        object.__setattr__(self, "metric_tolerances", tolerances)
        hard = _positive_finite(
            self.energy_distance_hard_limit,
            "energy_distance_hard_limit",
        )
        advisory = _nonnegative_finite(
            self.energy_distance_advisory_limit,
            "energy_distance_advisory_limit",
        )
        if advisory > hard:
            raise ValueError(
                "partition advisory energy limit exceeds hard limit"
            )
        object.__setattr__(self, "energy_distance_hard_limit", hard)
        object.__setattr__(self, "energy_distance_advisory_limit", advisory)
        for name in (
            "minimum_replicates_per_partition",
            "minimum_case_count",
            "minimum_feature_dimension",
        ):
            object.__setattr__(
                self, name, _positive_int(getattr(self, name), name)
            )
        if self.minimum_replicates_per_partition < 2:
            raise ValueError("partition power requires at least two replicates")
        if self.minimum_feature_dimension > MAX_PARTITION_FEATURES:
            raise ValueError("partition feature dimension exceeds limit")
        power = _strict_probability(self.target_power, "target_power")
        alpha = _strict_probability(self.alpha, "alpha")
        object.__setattr__(self, "target_power", power)
        object.__setattr__(self, "alpha", alpha)
        for name in (
            "required_split_depth_strata",
            "required_epoch_strata",
            "required_activity_strata",
            "required_context_strata",
            "required_alignment_kinds",
            "required_observation_scenarios",
        ):
            object.__setattr__(self, name, _text_tuple(getattr(self, name)))
        expected = _stable_id("partition-invariance-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("partition invariance policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def tolerance(self, metric_name: str) -> PartitionMetricToleranceV1 | None:
        for item in self.metric_tolerances:
            if item.metric_name == metric_name:
                return item
        return None

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric_tolerances": [
                item.to_dict() for item in self.metric_tolerances
            ],
            "energy_distance_hard_limit": self.energy_distance_hard_limit,
            "energy_distance_advisory_limit": (
                self.energy_distance_advisory_limit
            ),
            "minimum_replicates_per_partition": (
                self.minimum_replicates_per_partition
            ),
            "minimum_case_count": self.minimum_case_count,
            "minimum_feature_dimension": self.minimum_feature_dimension,
            "target_power": self.target_power,
            "alpha": self.alpha,
            "required_split_depth_strata": list(
                self.required_split_depth_strata
            ),
            "required_epoch_strata": list(self.required_epoch_strata),
            "required_activity_strata": list(self.required_activity_strata),
            "required_context_strata": list(self.required_context_strata),
            "required_alignment_kinds": list(self.required_alignment_kinds),
            "required_observation_scenarios": list(
                self.required_observation_scenarios
            ),
            "required_partition_kinds": [
                item.value for item in AdaptivePartitionKind
            ],
            "energy_distance_estimator": "empirical-euclidean-energy-v1",
            "rowwise_identity_claimed": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PartitionInvariancePolicyV1:
        if data.get("required_partition_kinds") != [
            item.value for item in AdaptivePartitionKind
        ]:
            raise ValueError("partition treatment policy differs")
        if data.get("energy_distance_estimator") != (
            "empirical-euclidean-energy-v1"
        ):
            raise ValueError("partition energy estimator differs")
        if data.get("rowwise_identity_claimed") is not False:
            raise ValueError("partition rowwise identity claim differs")
        return cls(
            metric_tolerances=tuple(
                PartitionMetricToleranceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("metric_tolerances"))
            ),
            energy_distance_hard_limit=_strict_number(
                data.get("energy_distance_hard_limit"),
                "energy_distance_hard_limit",
            ),
            energy_distance_advisory_limit=_strict_number(
                data.get("energy_distance_advisory_limit"),
                "energy_distance_advisory_limit",
            ),
            minimum_replicates_per_partition=_strict_int(
                data.get("minimum_replicates_per_partition"),
                "minimum_replicates_per_partition",
            ),
            minimum_case_count=_strict_int(
                data.get("minimum_case_count"), "minimum_case_count"
            ),
            minimum_feature_dimension=_strict_int(
                data.get("minimum_feature_dimension"),
                "minimum_feature_dimension",
            ),
            target_power=_strict_number(
                data.get("target_power"), "target_power"
            ),
            alpha=_strict_number(data.get("alpha"), "alpha"),
            required_split_depth_strata=_string_tuple(
                data.get("required_split_depth_strata")
            ),
            required_epoch_strata=_string_tuple(
                data.get("required_epoch_strata")
            ),
            required_activity_strata=_string_tuple(
                data.get("required_activity_strata")
            ),
            required_context_strata=_string_tuple(
                data.get("required_context_strata")
            ),
            required_alignment_kinds=_string_tuple(
                data.get("required_alignment_kinds")
            ),
            required_observation_scenarios=_string_tuple(
                data.get("required_observation_scenarios")
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PartitionInvarianceCaseV1:
    """One source span evaluated under all three partition treatments."""

    candidate_id: str
    release_candidate_ref: ArtifactRef
    source_content_sha256: str
    model_fit_id: str
    observation_scenario_id: str
    split_depth_stratum: str
    epoch_stratum: str
    activity_stratum: str
    context_stratum: str
    alignment_kind: str
    partitions: tuple[AdaptivePartitionSpecV1, ...]
    case_id: str = ""
    schema_version: str = PARTITION_INVARIANCE_CASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PARTITION_INVARIANCE_CASE_SCHEMA_VERSION
        )
        for name in (
            "candidate_id",
            "model_fit_id",
            "observation_scenario_id",
            "split_depth_stratum",
            "epoch_stratum",
            "activity_stratum",
            "context_stratum",
            "alignment_kind",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.release_candidate_ref, ArtifactRef):
            raise TypeError("partition case release candidate ref is invalid")
        _require_strong_ref(self.release_candidate_ref)
        if (
            self.release_candidate_ref.metadata.get("candidate_id")
            != self.candidate_id
        ):
            raise ValueError("partition case candidate identity differs")
        object.__setattr__(
            self, "source_content_sha256", _sha256(self.source_content_sha256)
        )
        partitions = tuple(
            sorted(self.partitions, key=lambda item: item.kind.value)
        )
        by_kind = {item.kind: item for item in partitions}
        if set(by_kind) != set(AdaptivePartitionKind) or len(partitions) != 3:
            raise ValueError("partition case treatment set is incomplete")
        parent_identities = {
            (item.parent_span_id, item.parent_start_ns, item.parent_end_ns)
            for item in partitions
        }
        if len(parent_identities) != 1:
            raise ValueError("partition case parent spans differ")
        coarsest = by_kind[AdaptivePartitionKind.COARSEST]
        planner = by_kind[AdaptivePartitionKind.PLANNER]
        finer = by_kind[AdaptivePartitionKind.FINER]
        if len(coarsest.intervals) != 1:
            raise ValueError("partition case coarsest treatment is split")
        if not set(coarsest.boundaries).issubset(planner.boundaries):
            raise ValueError("planner partition does not refine coarsest")
        if not set(planner.boundaries).issubset(finer.boundaries):
            raise ValueError("finer partition does not refine planner")
        if planner.boundaries == finer.boundaries:
            raise ValueError("deterministic finer partition is not stricter")
        object.__setattr__(self, "partitions", partitions)
        expected = _stable_id("partition-invariance-case", self.payload())
        if self.case_id and self.case_id != expected:
            raise ValueError("partition invariance case identity differs")
        object.__setattr__(self, "case_id", expected)

    def partition(self, kind: AdaptivePartitionKind) -> AdaptivePartitionSpecV1:
        for item in self.partitions:
            if item.kind is kind:
                return item
        raise KeyError(kind.value)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "release_candidate_ref": self.release_candidate_ref.to_dict(),
            "source_content_sha256": self.source_content_sha256,
            "model_fit_id": self.model_fit_id,
            "observation_scenario_id": self.observation_scenario_id,
            "split_depth_stratum": self.split_depth_stratum,
            "epoch_stratum": self.epoch_stratum,
            "activity_stratum": self.activity_stratum,
            "context_stratum": self.context_stratum,
            "alignment_kind": self.alignment_kind,
            "partitions": [item.to_dict() for item in self.partitions],
            "same_source_rows": True,
            "same_model_fit": True,
            "same_observation_scenario": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "case_id": self.case_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PartitionInvarianceCaseV1:
        for name in (
            "same_source_rows",
            "same_model_fit",
            "same_observation_scenario",
        ):
            if data.get(name) is not True:
                raise ValueError(f"partition case {name} differs")
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            release_candidate_ref=ArtifactRef.from_dict(
                _mapping(data.get("release_candidate_ref"))
            ),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            model_fit_id=str(data.get("model_fit_id", "")),
            observation_scenario_id=str(
                data.get("observation_scenario_id", "")
            ),
            split_depth_stratum=str(data.get("split_depth_stratum", "")),
            epoch_stratum=str(data.get("epoch_stratum", "")),
            activity_stratum=str(data.get("activity_stratum", "")),
            context_stratum=str(data.get("context_stratum", "")),
            alignment_kind=str(data.get("alignment_kind", "")),
            partitions=tuple(
                AdaptivePartitionSpecV1.from_dict(_mapping(item))
                for item in _sequence(data.get("partitions"))
            ),
            case_id=str(data.get("case_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PartitionInvarianceRunV1:
    """One replicate summary for one case and partition treatment."""

    case_id: str
    partition_id: str
    partition_kind: AdaptivePartitionKind
    replicate_id: str
    semantic_member_id: str
    source_content_sha256: str
    model_fit_id: str
    observation_scenario_id: str
    seed_ledger: PartitionSeedLedgerV1
    ownership_audit: PartitionSourceOwnershipAuditV1
    history_audit: PartitionHistoryAuditV1
    feature_vector: tuple[float, ...]
    metrics: Mapping[str, float]
    strategy_sensitivity_supported: bool = False
    run_id: str = ""
    schema_version: str = PARTITION_INVARIANCE_RUN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PARTITION_INVARIANCE_RUN_SCHEMA_VERSION
        )
        for name in (
            "case_id",
            "partition_id",
            "replicate_id",
            "semantic_member_id",
            "model_fit_id",
            "observation_scenario_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "partition_kind", AdaptivePartitionKind(self.partition_kind)
        )
        object.__setattr__(
            self, "source_content_sha256", _sha256(self.source_content_sha256)
        )
        if not isinstance(self.seed_ledger, PartitionSeedLedgerV1):
            raise TypeError("partition run seed ledger is invalid")
        if (
            self.seed_ledger.partition_id != self.partition_id
            or self.seed_ledger.semantic_member_id != self.semantic_member_id
            or self.seed_ledger.model_fit_id != self.model_fit_id
            or self.seed_ledger.observation_scenario_id
            != self.observation_scenario_id
        ):
            raise ValueError("partition run seed lineage differs")
        if (
            not isinstance(
                self.ownership_audit, PartitionSourceOwnershipAuditV1
            )
            or self.ownership_audit.partition_id != self.partition_id
        ):
            raise ValueError("partition run ownership audit differs")
        if (
            not isinstance(self.history_audit, PartitionHistoryAuditV1)
            or self.history_audit.partition_id != self.partition_id
        ):
            raise ValueError("partition run history audit differs")
        features = tuple(
            _finite(value, f"feature_vector[{index}]")
            for index, value in enumerate(self.feature_vector)
        )
        if not features or len(features) > MAX_PARTITION_FEATURES:
            raise ValueError("partition run feature vector is invalid")
        object.__setattr__(self, "feature_vector", features)
        metrics = {
            _required_text(key): _finite(value, f"metrics.{key}")
            for key, value in sorted(self.metrics.items())
        }
        allowed = REQUIRED_PARTITION_METRICS | OPTIONAL_PARTITION_METRICS
        if not REQUIRED_PARTITION_METRICS.issubset(metrics) or not set(
            metrics
        ).issubset(allowed):
            raise ValueError("partition run metrics are incomplete or unknown")
        has_strategy = "strategy_sensitivity" in metrics
        if has_strategy is not self.strategy_sensitivity_supported:
            raise ValueError("partition strategy-sensitivity support differs")
        object.__setattr__(self, "metrics", metrics)
        expected = _stable_id("partition-invariance-run", self.payload())
        if self.run_id and self.run_id != expected:
            raise ValueError("partition invariance run identity differs")
        object.__setattr__(self, "run_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "case_id": self.case_id,
            "partition_id": self.partition_id,
            "partition_kind": self.partition_kind.value,
            "replicate_id": self.replicate_id,
            "semantic_member_id": self.semantic_member_id,
            "source_content_sha256": self.source_content_sha256,
            "model_fit_id": self.model_fit_id,
            "observation_scenario_id": self.observation_scenario_id,
            "seed_ledger": self.seed_ledger.to_dict(),
            "ownership_audit": self.ownership_audit.to_dict(),
            "history_audit": self.history_audit.to_dict(),
            "feature_vector": list(self.feature_vector),
            "metrics": dict(self.metrics),
            "strategy_sensitivity_supported": (
                self.strategy_sensitivity_supported
            ),
            "rowwise_identity_claimed": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "run_id": self.run_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PartitionInvarianceRunV1:
        if data.get("rowwise_identity_claimed") is not False:
            raise ValueError("partition run rowwise claim differs")
        return cls(
            case_id=str(data.get("case_id", "")),
            partition_id=str(data.get("partition_id", "")),
            partition_kind=AdaptivePartitionKind(
                str(data.get("partition_kind", ""))
            ),
            replicate_id=str(data.get("replicate_id", "")),
            semantic_member_id=str(data.get("semantic_member_id", "")),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            model_fit_id=str(data.get("model_fit_id", "")),
            observation_scenario_id=str(
                data.get("observation_scenario_id", "")
            ),
            seed_ledger=PartitionSeedLedgerV1.from_dict(
                _mapping(data.get("seed_ledger"))
            ),
            ownership_audit=PartitionSourceOwnershipAuditV1.from_dict(
                _mapping(data.get("ownership_audit"))
            ),
            history_audit=PartitionHistoryAuditV1.from_dict(
                _mapping(data.get("history_audit"))
            ),
            feature_vector=tuple(
                _strict_number(value, "feature_vector")
                for value in _sequence(data.get("feature_vector"))
            ),
            metrics=_float_mapping(data.get("metrics")),
            strategy_sensitivity_supported=(
                data.get("strategy_sensitivity_supported") is True
            ),
            run_id=str(data.get("run_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PartitionInvarianceComparisonV1:
    """Distribution and metric comparison for two partition treatments."""

    case_id: str
    left_kind: AdaptivePartitionKind
    right_kind: AdaptivePartitionKind
    replicate_count: int
    feature_dimension: int
    energy_distance_squared: float
    absolute_metric_differences: Mapping[str, float]
    relative_metric_differences: Mapping[str, float]
    hard_violations: tuple[str, ...]
    advisory_violations: tuple[str, ...]
    comparison_id: str = ""
    schema_version: str = PARTITION_INVARIANCE_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PARTITION_INVARIANCE_COMPARISON_SCHEMA_VERSION,
        )
        object.__setattr__(self, "case_id", _required_text(self.case_id))
        left = AdaptivePartitionKind(self.left_kind)
        right = AdaptivePartitionKind(self.right_kind)
        if left is right:
            raise ValueError("partition comparison treatments are identical")
        object.__setattr__(self, "left_kind", left)
        object.__setattr__(self, "right_kind", right)
        object.__setattr__(
            self,
            "replicate_count",
            _positive_int(self.replicate_count, "replicate_count"),
        )
        object.__setattr__(
            self,
            "feature_dimension",
            _positive_int(self.feature_dimension, "feature_dimension"),
        )
        energy = _nonnegative_finite(
            self.energy_distance_squared, "energy_distance_squared"
        )
        object.__setattr__(self, "energy_distance_squared", energy)
        absolute = _nonnegative_float_mapping(
            self.absolute_metric_differences,
            "absolute_metric_differences",
        )
        relative = _nonnegative_float_mapping(
            self.relative_metric_differences,
            "relative_metric_differences",
        )
        if set(absolute) != set(relative):
            raise ValueError("partition comparison metric sets differ")
        object.__setattr__(self, "absolute_metric_differences", absolute)
        object.__setattr__(self, "relative_metric_differences", relative)
        object.__setattr__(
            self, "hard_violations", _optional_text_tuple(self.hard_violations)
        )
        object.__setattr__(
            self,
            "advisory_violations",
            _optional_text_tuple(self.advisory_violations),
        )
        expected = _stable_id("partition-invariance-comparison", self.payload())
        if self.comparison_id and self.comparison_id != expected:
            raise ValueError("partition invariance comparison differs")
        object.__setattr__(self, "comparison_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "case_id": self.case_id,
            "left_kind": self.left_kind.value,
            "right_kind": self.right_kind.value,
            "replicate_count": self.replicate_count,
            "feature_dimension": self.feature_dimension,
            "energy_distance_squared": self.energy_distance_squared,
            "absolute_metric_differences": dict(
                self.absolute_metric_differences
            ),
            "relative_metric_differences": dict(
                self.relative_metric_differences
            ),
            "hard_violations": list(self.hard_violations),
            "advisory_violations": list(self.advisory_violations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "comparison_id": self.comparison_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PartitionInvarianceComparisonV1:
        return cls(
            case_id=str(data.get("case_id", "")),
            left_kind=AdaptivePartitionKind(str(data.get("left_kind", ""))),
            right_kind=AdaptivePartitionKind(str(data.get("right_kind", ""))),
            replicate_count=_strict_int(
                data.get("replicate_count"), "replicate_count"
            ),
            feature_dimension=_strict_int(
                data.get("feature_dimension"), "feature_dimension"
            ),
            energy_distance_squared=_strict_number(
                data.get("energy_distance_squared"),
                "energy_distance_squared",
            ),
            absolute_metric_differences=_float_mapping(
                data.get("absolute_metric_differences")
            ),
            relative_metric_differences=_float_mapping(
                data.get("relative_metric_differences")
            ),
            hard_violations=_string_tuple(data.get("hard_violations")),
            advisory_violations=_string_tuple(data.get("advisory_violations")),
            comparison_id=str(data.get("comparison_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PartitionInvarianceQualificationV1:
    """Candidate-bound release decision for adaptive partitioning."""

    candidate_id: str
    policy: PartitionInvariancePolicyV1
    cases: tuple[PartitionInvarianceCaseV1, ...]
    runs: tuple[PartitionInvarianceRunV1, ...]
    comparisons: tuple[PartitionInvarianceComparisonV1, ...]
    coverage: Mapping[str, tuple[str, ...]]
    status: PartitionQualificationStatus
    findings: tuple[str, ...]
    qualified_at_utc: str
    qualification_id: str = ""
    schema_version: str = PARTITION_INVARIANCE_QUALIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            PARTITION_INVARIANCE_QUALIFICATION_SCHEMA_VERSION,
        )
        object.__setattr__(
            self, "candidate_id", _required_text(self.candidate_id)
        )
        if not isinstance(self.policy, PartitionInvariancePolicyV1):
            raise TypeError("partition qualification policy is invalid")
        cases = tuple(sorted(self.cases, key=lambda item: item.case_id))
        if (
            not cases
            or len(cases) > MAX_PARTITION_CASES
            or len({item.case_id for item in cases}) != len(cases)
        ):
            raise ValueError("partition qualification cases are invalid")
        if any(item.candidate_id != self.candidate_id for item in cases):
            raise ValueError("partition qualification candidate differs")
        object.__setattr__(self, "cases", cases)
        runs = tuple(sorted(self.runs, key=lambda item: item.run_id))
        if (
            not runs
            or len(runs) > MAX_PARTITION_RUNS
            or len({item.run_id for item in runs}) != len(runs)
        ):
            raise ValueError("partition qualification runs are invalid")
        object.__setattr__(self, "runs", runs)
        comparisons = tuple(
            sorted(self.comparisons, key=lambda item: item.comparison_id)
        )
        object.__setattr__(self, "comparisons", comparisons)
        coverage = {
            _required_text(key): _optional_text_tuple(values)
            for key, values in sorted(self.coverage.items())
        }
        if coverage != _partition_case_coverage(cases):
            raise ValueError("partition qualification coverage differs")
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(
            self, "status", PartitionQualificationStatus(self.status)
        )
        object.__setattr__(
            self, "findings", _optional_text_tuple(self.findings)
        )
        object.__setattr__(
            self, "qualified_at_utc", _timestamp(self.qualified_at_utc)
        )
        expected = _stable_id(
            "partition-invariance-qualification", self.payload()
        )
        if self.qualification_id and self.qualification_id != expected:
            raise ValueError("partition invariance qualification differs")
        object.__setattr__(self, "qualification_id", expected)

    @property
    def full_campaign_permitted(self) -> bool:
        return self.status is PartitionQualificationStatus.PASS

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "policy": self.policy.to_dict(),
            "cases": [item.to_dict() for item in self.cases],
            "runs": [item.to_dict() for item in self.runs],
            "comparisons": [item.to_dict() for item in self.comparisons],
            "coverage": {
                key: list(value) for key, value in self.coverage.items()
            },
            "status": self.status.value,
            "findings": list(self.findings),
            "qualified_at_utc": self.qualified_at_utc,
            "full_campaign_permitted": self.full_campaign_permitted,
            "rowwise_identity_claimed": False,
            "partition_effect_response": (
                "redesign-carry-or-first-class-partition-policy-or-reduce-claim"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "qualification_id": self.qualification_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PartitionInvarianceQualificationV1:
        if data.get("rowwise_identity_claimed") is not False:
            raise ValueError("partition qualification rowwise claim differs")
        if data.get("partition_effect_response") != (
            "redesign-carry-or-first-class-partition-policy-or-reduce-claim"
        ):
            raise ValueError("partition qualification failure policy differs")
        result = cls(
            candidate_id=str(data.get("candidate_id", "")),
            policy=PartitionInvariancePolicyV1.from_dict(
                _mapping(data.get("policy"))
            ),
            cases=tuple(
                PartitionInvarianceCaseV1.from_dict(_mapping(item))
                for item in _sequence(data.get("cases"))
            ),
            runs=tuple(
                PartitionInvarianceRunV1.from_dict(_mapping(item))
                for item in _sequence(data.get("runs"))
            ),
            comparisons=tuple(
                PartitionInvarianceComparisonV1.from_dict(_mapping(item))
                for item in _sequence(data.get("comparisons"))
            ),
            coverage={
                str(key): _string_tuple(value)
                for key, value in _mapping(data.get("coverage")).items()
            },
            status=PartitionQualificationStatus(str(data.get("status", ""))),
            findings=_string_tuple(data.get("findings")),
            qualified_at_utc=str(data.get("qualified_at_utc", "")),
            qualification_id=str(data.get("qualification_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("full_campaign_permitted") is not (
            result.full_campaign_permitted
        ):
            raise ValueError("partition campaign permission differs")
        expected = qualify_partition_invariance(
            result.policy,
            result.cases,
            result.runs,
            qualified_at_utc=result.qualified_at_utc,
        )
        if result.to_dict() != expected.to_dict():
            raise ValueError("partition qualification decision differs")
        return result


def audit_partition_source_ownership(
    partition: AdaptivePartitionSpecV1,
    source_events: Sequence[tuple[str, int]],
    *,
    source_content_sha256: str,
    anchor_event_ids: Sequence[str],
) -> PartitionSourceOwnershipAuditV1:
    """Audit ownership while binding the upstream full-source content hash."""
    normalized_events = tuple(
        (_required_text(event_id), _int64(event_time_ns, "event_time_ns"))
        for event_id, event_time_ns in source_events
    )
    if len(normalized_events) > MAX_PARTITION_HISTORY_EVENTS:
        raise ValueError("partition source ownership input exceeds limit")
    anchors = {_required_text(value) for value in anchor_event_ids}
    source_id_counts = Counter(event_id for event_id, _ in normalized_events)
    duplicate_count = sum(value - 1 for value in source_id_counts.values())
    assignments: list[tuple[str, int, str]] = []
    lost = 0
    seam_times = {item.start_ns for item in partition.intervals[1:]}
    boundary_count = 0
    assigned_ids: Counter[str] = Counter()
    for event_id, event_time_ns in normalized_events:
        owners = tuple(
            item
            for item in partition.intervals
            if item.start_ns <= event_time_ns < item.end_ns
        )
        if len(owners) != 1:
            lost += 1
            continue
        owner = owners[0]
        assignments.append((event_id, event_time_ns, owner.interval_id))
        assigned_ids[event_id] += 1
        boundary_count += int(event_time_ns in seam_times)
    missing_anchors = sum(
        assigned_ids.get(anchor, 0) != 1 for anchor in anchors
    )
    assignment_hash = _content_sha256(
        [list(item) for item in sorted(assignments)]
    )
    return PartitionSourceOwnershipAuditV1(
        partition_id=partition.partition_id,
        source_event_count=len(normalized_events),
        assigned_event_count=len(assignments),
        lost_event_count=lost,
        duplicate_event_count=duplicate_count,
        boundary_event_count=boundary_count,
        anchor_event_count=len(anchors),
        missing_anchor_count=missing_anchors,
        source_content_sha256=_sha256(source_content_sha256),
        assignment_content_sha256=assignment_hash,
    )


def audit_partition_history(
    partition: AdaptivePartitionSpecV1,
    history_by_interval: Mapping[str, Sequence[tuple[str, int]]],
    *,
    maximum_history_ns: int,
    known_source_event_ids: Sequence[str],
) -> PartitionHistoryAuditV1:
    """Audit bounded, strictly prior history for every child interval."""
    maximum = _nonnegative_int(maximum_history_ns, "maximum_history_ns")
    known = {_required_text(value) for value in known_source_event_ids}
    expected_intervals = {
        item.interval_id: item for item in partition.intervals
    }
    if set(history_by_interval) != set(expected_intervals):
        raise ValueError("partition history interval set differs")
    future = 0
    out_of_bound = 0
    unknown = 0
    rows: list[tuple[str, str, int]] = []
    counts: dict[str, int] = {}
    for interval_id, values in sorted(history_by_interval.items()):
        interval = expected_intervals[interval_id]
        normalized = tuple(
            (_required_text(event_id), _int64(event_time_ns, "history_time_ns"))
            for event_id, event_time_ns in values
        )
        counts[interval_id] = len(normalized)
        if sum(counts.values()) > MAX_PARTITION_HISTORY_EVENTS:
            raise ValueError("partition history input exceeds limit")
        for event_id, event_time_ns in normalized:
            future += int(event_time_ns >= interval.start_ns)
            out_of_bound += int(event_time_ns < interval.start_ns - maximum)
            unknown += int(event_id not in known)
            rows.append((interval_id, event_id, event_time_ns))
    return PartitionHistoryAuditV1(
        partition_id=partition.partition_id,
        maximum_history_ns=maximum,
        history_event_count=len(rows),
        interval_history_counts=counts,
        future_event_count=future,
        out_of_bound_event_count=out_of_bound,
        unknown_source_event_count=unknown,
        history_content_sha256=_content_sha256(
            [list(item) for item in sorted(rows)]
        ),
    )


def build_partition_seed_ledger(
    partition: AdaptivePartitionSpecV1,
    *,
    candidate_id: str,
    model_fit_id: str,
    observation_scenario_id: str,
    semantic_member_id: str,
    base_seed: int,
) -> PartitionSeedLedgerV1:
    """Derive worker- and retry-independent child seeds from parent lineage."""
    normalized_base = _uint64(base_seed, "base_seed")
    semantic: dict[str, JSONValue] = {
        "candidate_id": _required_text(candidate_id),
        "parent_span_id": partition.parent_span_id,
        "parent_start_ns": partition.parent_start_ns,
        "parent_end_ns": partition.parent_end_ns,
        "model_fit_id": _required_text(model_fit_id),
        "observation_scenario_id": _required_text(observation_scenario_id),
        "semantic_member_id": _required_text(semantic_member_id),
        "base_seed": normalized_base,
    }
    parent_seed = _semantic_uint64(semantic)
    child_seeds = {
        item.interval_id: _semantic_uint64(
            {
                "parent_seed": parent_seed,
                "start_ns": item.start_ns,
                "end_ns": item.end_ns,
            }
        )
        for item in partition.intervals
    }
    return PartitionSeedLedgerV1(
        candidate_id=str(semantic["candidate_id"]),
        partition_id=partition.partition_id,
        parent_span_id=partition.parent_span_id,
        model_fit_id=str(semantic["model_fit_id"]),
        observation_scenario_id=str(semantic["observation_scenario_id"]),
        semantic_member_id=str(semantic["semantic_member_id"]),
        base_seed=normalized_base,
        parent_seed=parent_seed,
        child_seeds=child_seeds,
    )


def energy_distance_squared(
    left: Sequence[Sequence[float]], right: Sequence[Sequence[float]]
) -> float:
    """Return the empirical squared energy distance between feature samples."""
    left_vectors = _feature_matrix(left)
    right_vectors = _feature_matrix(right)
    if len(left_vectors[0]) != len(right_vectors[0]):
        raise ValueError("partition energy feature dimensions differ")
    cross = fmean(
        math.dist(left_item, right_item)
        for left_item in left_vectors
        for right_item in right_vectors
    )
    within_left = fmean(
        math.dist(first, second)
        for first in left_vectors
        for second in left_vectors
    )
    within_right = fmean(
        math.dist(first, second)
        for first in right_vectors
        for second in right_vectors
    )
    return max(0.0, 2.0 * cross - within_left - within_right)


def qualify_partition_invariance(
    policy: PartitionInvariancePolicyV1,
    cases: Sequence[PartitionInvarianceCaseV1],
    runs: Sequence[PartitionInvarianceRunV1],
    *,
    qualified_at_utc: str,
) -> PartitionInvarianceQualificationV1:
    """Compare all partition treatments and issue a fail-closed report."""
    ordered_cases = tuple(sorted(cases, key=lambda item: item.case_id))
    ordered_runs = tuple(sorted(runs, key=lambda item: item.run_id))
    if not ordered_cases:
        raise ValueError("partition qualification requires cases")
    candidate_ids = {item.candidate_id for item in ordered_cases}
    if len(candidate_ids) != 1:
        raise ValueError("partition qualification mixes release candidates")
    candidate_id = next(iter(candidate_ids))
    verified_candidate_refs: set[tuple[str, int | None, str]] = set()
    for candidate_case in ordered_cases:
        key = (
            candidate_case.release_candidate_ref.path,
            candidate_case.release_candidate_ref.size_bytes,
            candidate_case.release_candidate_ref.sha256,
        )
        if key not in verified_candidate_refs:
            _verify_artifact_ref(candidate_case.release_candidate_ref)
            verified_candidate_refs.add(key)
    case_by_id = {item.case_id: item for item in ordered_cases}
    if len(case_by_id) != len(ordered_cases):
        raise ValueError("partition qualification case identities duplicate")
    findings: list[str] = []
    comparisons: list[PartitionInvarianceComparisonV1] = []
    insufficient = len(ordered_cases) < policy.minimum_case_count
    if insufficient:
        findings.append("minimum_case_count_not_met")
    coverage = _partition_case_coverage(ordered_cases)
    for name, required in _required_coverage(policy).items():
        missing = set(required) - set(coverage[name])
        if missing:
            insufficient = True
            findings.extend(
                f"missing_{name}:{value}" for value in sorted(missing)
            )
    runs_by_case_kind: dict[
        tuple[str, AdaptivePartitionKind], list[PartitionInvarianceRunV1]
    ] = defaultdict(list)
    for run in ordered_runs:
        case = case_by_id.get(run.case_id)
        if case is None:
            raise ValueError("partition run references an unknown case")
        partition = case.partition(run.partition_kind)
        if run.partition_id != partition.partition_id:
            raise ValueError("partition run references another treatment")
        if (
            run.source_content_sha256 != case.source_content_sha256
            or run.ownership_audit.source_content_sha256
            != case.source_content_sha256
            or run.model_fit_id != case.model_fit_id
            or run.observation_scenario_id != case.observation_scenario_id
            or run.seed_ledger.candidate_id != candidate_id
            or run.seed_ledger.parent_span_id != partition.parent_span_id
            or set(run.seed_ledger.child_seeds)
            != {item.interval_id for item in partition.intervals}
            or set(run.history_audit.interval_history_counts)
            != {item.interval_id for item in partition.intervals}
        ):
            raise ValueError("partition run scientific lineage differs")
        if not run.ownership_audit.passed:
            findings.append(f"source_ownership_failed:{run.run_id}")
        if not run.history_audit.passed:
            findings.append(f"history_audit_failed:{run.run_id}")
        if len(run.feature_vector) < policy.minimum_feature_dimension:
            insufficient = True
            findings.append(f"feature_dimension_insufficient:{run.run_id}")
        runs_by_case_kind[(run.case_id, run.partition_kind)].append(run)

    hard_failure = any(
        finding.startswith(("source_ownership_failed", "history_audit_failed"))
        for finding in findings
    )
    pair_kinds = (
        (AdaptivePartitionKind.COARSEST, AdaptivePartitionKind.PLANNER),
        (AdaptivePartitionKind.PLANNER, AdaptivePartitionKind.FINER),
        (AdaptivePartitionKind.COARSEST, AdaptivePartitionKind.FINER),
    )
    for case in ordered_cases:
        by_kind = {
            kind: tuple(runs_by_case_kind[(case.case_id, kind)])
            for kind in AdaptivePartitionKind
        }
        for kind, kind_runs in by_kind.items():
            if len(kind_runs) < policy.minimum_replicates_per_partition:
                insufficient = True
                findings.append(
                    f"replicate_count_insufficient:{case.case_id}:{kind.value}"
                )
        if any(not values for values in by_kind.values()):
            continue
        replicate_sets = {
            kind: {item.replicate_id for item in values}
            for kind, values in by_kind.items()
        }
        if len({frozenset(value) for value in replicate_sets.values()}) != 1:
            insufficient = True
            findings.append(f"replicate_pairing_differs:{case.case_id}")
            continue
        _validate_matched_semantics(by_kind, case.case_id)
        for left_kind, right_kind in pair_kinds:
            comparison = _compare_partition_runs(
                policy,
                case.case_id,
                by_kind[left_kind],
                by_kind[right_kind],
            )
            comparisons.append(comparison)
            if comparison.hard_violations:
                hard_failure = True
                findings.extend(
                    f"hard:{comparison.comparison_id}:{value}"
                    for value in comparison.hard_violations
                )
            findings.extend(
                f"advisory:{comparison.comparison_id}:{value}"
                for value in comparison.advisory_violations
            )
    if insufficient:
        status = PartitionQualificationStatus.INSUFFICIENT_EVIDENCE
    elif hard_failure:
        status = PartitionQualificationStatus.FAIL
    else:
        status = PartitionQualificationStatus.PASS
    return PartitionInvarianceQualificationV1(
        candidate_id=candidate_id,
        policy=policy,
        cases=ordered_cases,
        runs=ordered_runs,
        comparisons=tuple(comparisons),
        coverage=coverage,
        status=status,
        findings=tuple(findings),
        qualified_at_utc=qualified_at_utc,
    )


def write_partition_invariance_qualification(
    qualification: PartitionInvarianceQualificationV1,
    output_directory: str | Path,
) -> ArtifactRef:
    """Write one content-addressed candidate-bound qualification artifact."""
    return _write_contract(
        qualification.to_json(),
        output_directory,
        prefix="partition-invariance-qualification",
        kind="partition_invariance_qualification_v1",
        metadata={
            "qualification_id": qualification.qualification_id,
            "candidate_id": qualification.candidate_id,
            "status": qualification.status.value,
        },
    )


def read_partition_invariance_qualification(
    path: str | Path,
) -> PartitionInvarianceQualificationV1:
    """Hash-verify and restore one partition qualification artifact."""
    return PartitionInvarianceQualificationV1.from_dict(
        _read_contract(path, "partition-invariance-qualification")
    )


def _compare_partition_runs(
    policy: PartitionInvariancePolicyV1,
    case_id: str,
    left_runs: Sequence[PartitionInvarianceRunV1],
    right_runs: Sequence[PartitionInvarianceRunV1],
) -> PartitionInvarianceComparisonV1:
    left = tuple(sorted(left_runs, key=lambda item: item.replicate_id))
    right = tuple(sorted(right_runs, key=lambda item: item.replicate_id))
    energy = energy_distance_squared(
        [item.feature_vector for item in left],
        [item.feature_vector for item in right],
    )
    hard: list[str] = []
    advisory: list[str] = []
    if energy > policy.energy_distance_hard_limit:
        hard.append("energy_distance")
    elif energy > policy.energy_distance_advisory_limit:
        advisory.append("energy_distance")
    common_metrics = set.intersection(
        *(set(item.metrics) for item in (*left, *right))
    )
    absolute: dict[str, float] = {}
    relative: dict[str, float] = {}
    for metric_name in sorted(common_metrics):
        left_mean = fmean(item.metrics[metric_name] for item in left)
        right_mean = fmean(item.metrics[metric_name] for item in right)
        difference = abs(left_mean - right_mean)
        relative_difference = difference / max(
            abs(left_mean), abs(right_mean), 1e-12
        )
        absolute[metric_name] = difference
        relative[metric_name] = relative_difference
        tolerance = policy.tolerance(metric_name)
        if tolerance is None:
            continue
        if (
            difference > tolerance.absolute_tolerance
            and relative_difference > tolerance.relative_tolerance
        ):
            target = (
                hard
                if tolerance.severity is PartitionToleranceSeverity.HARD
                else advisory
            )
            target.append(metric_name)
    return PartitionInvarianceComparisonV1(
        case_id=case_id,
        left_kind=left[0].partition_kind,
        right_kind=right[0].partition_kind,
        replicate_count=len(left),
        feature_dimension=len(left[0].feature_vector),
        energy_distance_squared=energy,
        absolute_metric_differences=absolute,
        relative_metric_differences=relative,
        hard_violations=tuple(hard),
        advisory_violations=tuple(advisory),
    )


def _validate_matched_semantics(
    by_kind: Mapping[AdaptivePartitionKind, Sequence[PartitionInvarianceRunV1]],
    case_id: str,
) -> None:
    by_replicate = {
        kind: {item.replicate_id: item for item in values}
        for kind, values in by_kind.items()
    }
    replicate_ids = next(iter(by_replicate.values()))
    for replicate_id in replicate_ids:
        matched = tuple(
            by_replicate[kind][replicate_id] for kind in AdaptivePartitionKind
        )
        if (
            len(
                {
                    (
                        item.semantic_member_id,
                        item.seed_ledger.base_seed,
                        item.seed_ledger.parent_seed,
                    )
                    for item in matched
                }
            )
            != 1
        ):
            raise ValueError(
                "partition replicate semantic identity differs: "
                f"{case_id}:{replicate_id}"
            )


def _partition_case_coverage(
    cases: Sequence[PartitionInvarianceCaseV1],
) -> dict[str, tuple[str, ...]]:
    return {
        "split_depth_strata": tuple(
            sorted({item.split_depth_stratum for item in cases})
        ),
        "epoch_strata": tuple(sorted({item.epoch_stratum for item in cases})),
        "activity_strata": tuple(
            sorted({item.activity_stratum for item in cases})
        ),
        "context_strata": tuple(
            sorted({item.context_stratum for item in cases})
        ),
        "alignment_kinds": tuple(
            sorted({item.alignment_kind for item in cases})
        ),
        "observation_scenarios": tuple(
            sorted({item.observation_scenario_id for item in cases})
        ),
    }


def _required_coverage(
    policy: PartitionInvariancePolicyV1,
) -> dict[str, tuple[str, ...]]:
    return {
        "split_depth_strata": policy.required_split_depth_strata,
        "epoch_strata": policy.required_epoch_strata,
        "activity_strata": policy.required_activity_strata,
        "context_strata": policy.required_context_strata,
        "alignment_kinds": policy.required_alignment_kinds,
        "observation_scenarios": policy.required_observation_scenarios,
    }


def _feature_matrix(
    values: Sequence[Sequence[float]],
) -> tuple[tuple[float, ...], ...]:
    matrix = tuple(
        tuple(_finite(item, "energy feature") for item in row) for row in values
    )
    if not matrix or not matrix[0]:
        raise ValueError("partition energy feature sample is empty")
    dimensions = {len(row) for row in matrix}
    if len(dimensions) != 1 or next(iter(dimensions)) > MAX_PARTITION_FEATURES:
        raise ValueError("partition energy feature dimensions differ")
    return matrix


def _require_strong_ref(ref: ArtifactRef) -> None:
    _required_text(ref.kind)
    path = Path(_required_text(ref.path)).expanduser()
    if not path.is_absolute():
        raise ValueError("partition artifact path is relative")
    if isinstance(ref.size_bytes, bool) or not isinstance(ref.size_bytes, int):
        raise TypeError("partition artifact size is absent")
    if ref.size_bytes < 0:
        raise ValueError("partition artifact size is negative")
    _sha256(ref.sha256)


def _verify_artifact_ref(ref: ArtifactRef) -> Path:
    _require_strong_ref(ref)
    path = Path(ref.path).expanduser()
    if not path.is_file():
        raise ValueError(f"partition artifact is missing: {path}")
    if path.stat().st_size != ref.size_bytes:
        raise ValueError(f"partition artifact size differs: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != ref.sha256:
        raise ValueError(f"partition artifact hash differs: {path}")
    return path


def _write_contract(
    text: str,
    output_directory: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    payload = (text + "\n").encode("utf-8")
    if len(payload) > MAX_PARTITION_ARTIFACT_BYTES:
        raise ValueError("partition qualification artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    directory = Path(output_directory).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}-{digest}.json"
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError("partition qualification artifact collision")
    else:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o644,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=len(payload),
        sha256=digest,
        metadata=dict(metadata),
    )


def _read_contract(path: str | Path, prefix: str) -> Mapping[str, Any]:
    target = Path(path).expanduser()
    payload = target.read_bytes()
    if len(payload) > MAX_PARTITION_ARTIFACT_BYTES:
        raise ValueError("partition qualification artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    if target.name != f"{prefix}-{digest}.json":
        raise ValueError(
            "partition qualification artifact is not content addressed"
        )
    return _mapping(json.loads(payload))


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    return f"{prefix}:sha256:{_content_sha256(payload)}"


def _content_sha256(
    value: JSONValue | Mapping[str, JSONValue],
) -> str:
    return hashlib.sha256(
        canonical_contract_json(value).encode("utf-8")
    ).hexdigest()


def _semantic_uint64(payload: Mapping[str, JSONValue]) -> int:
    return int(_content_sha256(payload)[:16], 16)


def _timestamp(value: str) -> str:
    normalized = _required_text(value)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as err:
        raise ValueError(
            "partition qualification timestamp is invalid"
        ) from err
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("partition qualification timestamp needs timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported partition schema: {actual!r}")


def _required_text(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("partition text value is required")
    return normalized


def _text_tuple(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({_required_text(value) for value in values}))
    if not normalized:
        raise ValueError("partition text set is empty")
    return normalized


def _optional_text_tuple(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _sha256(value: str) -> str:
    normalized = _required_text(value).lower()
    if _SHA256.fullmatch(normalized) is None:
        raise ValueError("partition SHA-256 value is invalid")
    return normalized


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"partition {name} is invalid")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"partition {name} is negative")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result <= 0:
        raise ValueError(f"partition {name} is not positive")
    return result


def _int64(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if not -(2**63) <= result < 2**63:
        raise ValueError(f"partition {name} exceeds int64")
    return result


def _uint64(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if not 0 <= result < 2**64:
        raise ValueError(f"partition {name} exceeds uint64")
    return result


def _strict_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"partition {name} is invalid")
    return float(value)


def _finite(value: Any, name: str) -> float:
    result = _strict_number(value, name)
    if not math.isfinite(result):
        raise ValueError(f"partition {name} is not finite")
    return result


def _nonnegative_finite(value: Any, name: str) -> float:
    result = _finite(value, name)
    if result < 0.0:
        raise ValueError(f"partition {name} is negative")
    return result


def _positive_finite(value: Any, name: str) -> float:
    result = _finite(value, name)
    if result <= 0.0:
        raise ValueError(f"partition {name} is not positive")
    return result


def _strict_probability(value: Any, name: str) -> float:
    result = _finite(value, name)
    if not 0.0 < result < 1.0:
        raise ValueError(f"partition {name} is not a strict probability")
    return result


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("partition object is invalid")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, list):
        raise TypeError("partition sequence is invalid")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _int_mapping(value: Any) -> dict[str, int]:
    return {
        str(key): _strict_int(item, str(key))
        for key, item in _mapping(value).items()
    }


def _float_mapping(value: Any) -> dict[str, float]:
    return {
        str(key): _strict_number(item, str(key))
        for key, item in _mapping(value).items()
    }


def _nonnegative_float_mapping(
    value: Mapping[str, float], name: str
) -> dict[str, float]:
    return {
        _required_text(key): _nonnegative_finite(item, f"{name}.{key}")
        for key, item in sorted(value.items())
    }


__all__ = [
    "ADAPTIVE_PARTITION_INTERVAL_SCHEMA_VERSION",
    "ADAPTIVE_PARTITION_SPEC_SCHEMA_VERSION",
    "OPTIONAL_PARTITION_METRICS",
    "PARTITION_HISTORY_AUDIT_SCHEMA_VERSION",
    "PARTITION_INVARIANCE_CASE_SCHEMA_VERSION",
    "PARTITION_INVARIANCE_COMPARISON_SCHEMA_VERSION",
    "PARTITION_INVARIANCE_POLICY_SCHEMA_VERSION",
    "PARTITION_INVARIANCE_QUALIFICATION_SCHEMA_VERSION",
    "PARTITION_INVARIANCE_RUN_SCHEMA_VERSION",
    "PARTITION_METRIC_TOLERANCE_SCHEMA_VERSION",
    "PARTITION_SEED_LEDGER_SCHEMA_VERSION",
    "PARTITION_SOURCE_OWNERSHIP_AUDIT_SCHEMA_VERSION",
    "REQUIRED_PARTITION_METRICS",
    "AdaptivePartitionIntervalV1",
    "AdaptivePartitionKind",
    "AdaptivePartitionSpecV1",
    "PartitionHistoryAuditV1",
    "PartitionInvarianceCaseV1",
    "PartitionInvarianceComparisonV1",
    "PartitionInvariancePolicyV1",
    "PartitionInvarianceQualificationV1",
    "PartitionInvarianceRunV1",
    "PartitionMetricToleranceV1",
    "PartitionQualificationStatus",
    "PartitionSeedLedgerV1",
    "PartitionSourceOwnershipAuditV1",
    "PartitionToleranceSeverity",
    "audit_partition_history",
    "audit_partition_source_ownership",
    "build_partition_seed_ledger",
    "energy_distance_squared",
    "qualify_partition_invariance",
    "read_partition_invariance_qualification",
    "write_partition_invariance_qualification",
]
