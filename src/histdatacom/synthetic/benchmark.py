"""Streaming reverse-degradation benchmark contracts and scorecards.

The benchmark is deliberately generator-neutral. Dense reference events,
degraded observations, transparent controls, and reconstruction candidates are
adapted to one narrow event contract and scored online. Only bounded aggregate
state and compact scorecards survive a window; dense intermediates remain
process-local.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from itertools import combinations
from typing import Any, Protocol, cast, runtime_checkable

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import (
    SYNTHETIC_EVENT_SCHEMA_VERSION,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.information import (
    InformationSplitKind,
    ReconstructionInformationManifestV1,
)
from histdatacom.synthetic.observation import (
    ObservationApplicationResultV1,
    ObservationCarryStateV1,
    ObservationContextV1,
    ObservationInputEventV1,
    ObservationOperatorV1,
    ObservationOutputEventV1,
)
from histdatacom.synthetic.streaming import ReconstructionWindowV1

BENCHMARK_PROFILE_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-benchmark-profile.v1"
)
BENCHMARK_SPLIT_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-benchmark-split.v1"
)
BENCHMARK_SCENARIO_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-benchmark-scenario.v1"
)
BENCHMARK_CANDIDATE_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-benchmark-candidate.v1"
)
BENCHMARK_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-benchmark-manifest.v1"
)
BENCHMARK_EVENT_SCHEMA_VERSION = "histdatacom.benchmark-event.v1"
BENCHMARK_EXECUTION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.benchmark-execution-evidence.v1"
)
BENCHMARK_CANDIDATE_WINDOW_SCHEMA_VERSION = (
    "histdatacom.benchmark-candidate-window.v1"
)
BENCHMARK_SLICE_SCORE_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-slice-score.v1"
)
BENCHMARK_CANDIDATE_SCORE_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-candidate-score.v1"
)
BENCHMARK_SCORECARD_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-scorecard.v1"
)

DEFAULT_BENCHMARK_MAX_SCENARIOS = 64
DEFAULT_BENCHMARK_MAX_CANDIDATES = 32
DEFAULT_BENCHMARK_MAX_SLICES = 4096
DEFAULT_BENCHMARK_MAX_EVENTS_PER_WINDOW = 250_000
DEFAULT_BENCHMARK_MAX_HOOK_METRICS = 64
DEFAULT_BENCHMARK_MAX_REASON_CODES = 64
DEFAULT_BENCHMARK_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
DEFAULT_BENCHMARK_ROUNDING_DIGITS = 12
MAX_BENCHMARK_SCENARIOS = 256
MAX_BENCHMARK_CANDIDATES = 128
MAX_BENCHMARK_SLICES = 16_384
MAX_BENCHMARK_EVENTS_PER_WINDOW = 1_000_000
MAX_BENCHMARK_HOOK_METRICS = 256
MAX_BENCHMARK_REASON_CODES = 256
MAX_BENCHMARK_PAYLOAD_BYTES = 64 * 1024 * 1024
MAX_BENCHMARK_ENSEMBLE_MEMBERS = 64
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

DEFAULT_INTERARRIVAL_BUCKETS_NS = (
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    5_000_000_000,
    30_000_000_000,
    300_000_000_000,
)
DEFAULT_SPREAD_BUCKETS = (
    0.00001,
    0.00005,
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.005,
)


class BenchmarkSplitKind(str, Enum):
    """Immutable chronological benchmark periods."""

    CALIBRATION = "calibration"
    VALIDATION = "validation"
    FINAL_HOLDOUT = "final_holdout"
    PRODUCT_INPUT = "product_input"

    @classmethod
    def from_value(
        cls, value: str | "BenchmarkSplitKind"
    ) -> "BenchmarkSplitKind":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported benchmark split kind") from err


class BenchmarkCandidateKind(str, Enum):
    """Distinguish transparent controls from reconstruction candidates."""

    CONTROL = "control"
    CANDIDATE = "candidate"

    @classmethod
    def from_value(
        cls, value: str | "BenchmarkCandidateKind"
    ) -> "BenchmarkCandidateKind":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported benchmark candidate kind") from err


class BenchmarkControlKind(str, Enum):
    """Required transparent benchmark controls."""

    NO_FILL = "no_fill"
    LINEAR_INTERPOLATION = "linear_interpolation"
    RESAMPLE_LAST = "resample_last"
    EMPIRICAL_OVERLAY = "empirical_overlay"

    @classmethod
    def from_value(
        cls, value: str | "BenchmarkControlKind"
    ) -> "BenchmarkControlKind":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported benchmark control kind") from err


@dataclass(frozen=True, slots=True)
class BenchmarkProfileV1:
    """Fixed histogram, resource, and payload bounds for one benchmark."""

    max_scenarios: int = DEFAULT_BENCHMARK_MAX_SCENARIOS
    max_candidates: int = DEFAULT_BENCHMARK_MAX_CANDIDATES
    max_slices: int = DEFAULT_BENCHMARK_MAX_SLICES
    max_events_per_window: int = DEFAULT_BENCHMARK_MAX_EVENTS_PER_WINDOW
    max_hook_metrics: int = DEFAULT_BENCHMARK_MAX_HOOK_METRICS
    max_reason_codes: int = DEFAULT_BENCHMARK_MAX_REASON_CODES
    max_payload_bytes: int = DEFAULT_BENCHMARK_MAX_PAYLOAD_BYTES
    burst_threshold_ns: int = 100_000_000
    quiet_threshold_ns: int = 5_000_000_000
    interarrival_buckets_ns: tuple[int, ...] = DEFAULT_INTERARRIVAL_BUCKETS_NS
    spread_buckets: tuple[float, ...] = DEFAULT_SPREAD_BUCKETS
    rounding_digits: int = DEFAULT_BENCHMARK_ROUNDING_DIGITS
    profile_id: str = ""
    schema_version: str = BENCHMARK_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark profile schema")
        _bounded_positive(
            self.max_scenarios, MAX_BENCHMARK_SCENARIOS, "max_scenarios"
        )
        _bounded_positive(
            self.max_candidates, MAX_BENCHMARK_CANDIDATES, "max_candidates"
        )
        _bounded_positive(self.max_slices, MAX_BENCHMARK_SLICES, "max_slices")
        _bounded_positive(
            self.max_events_per_window,
            MAX_BENCHMARK_EVENTS_PER_WINDOW,
            "max_events_per_window",
        )
        _bounded_positive(
            self.max_hook_metrics,
            MAX_BENCHMARK_HOOK_METRICS,
            "max_hook_metrics",
        )
        _bounded_positive(
            self.max_reason_codes,
            MAX_BENCHMARK_REASON_CODES,
            "max_reason_codes",
        )
        _bounded_positive(
            self.max_payload_bytes,
            MAX_BENCHMARK_PAYLOAD_BYTES,
            "max_payload_bytes",
        )
        burst = _positive_int(self.burst_threshold_ns, "burst_threshold_ns")
        quiet = _positive_int(self.quiet_threshold_ns, "quiet_threshold_ns")
        if quiet <= burst:
            raise ValueError("quiet threshold must exceed burst threshold")
        object.__setattr__(self, "burst_threshold_ns", burst)
        object.__setattr__(self, "quiet_threshold_ns", quiet)
        intervals = _strictly_increasing_positive_ints(
            self.interarrival_buckets_ns, "interarrival bucket"
        )
        spreads = _strictly_increasing_positive_floats(
            self.spread_buckets, "spread bucket"
        )
        if len(intervals) > 64 or len(spreads) > 64:
            raise ValueError("benchmark histogram bucket limit exceeded")
        object.__setattr__(self, "interarrival_buckets_ns", intervals)
        object.__setattr__(self, "spread_buckets", spreads)
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between zero and 16")
        expected = _stable_id("benchmark-profile", self.identity_payload())
        supplied = _optional_text(self.profile_id)
        if supplied is not None and supplied != expected:
            raise ValueError("profile_id does not match deterministic identity")
        object.__setattr__(self, "profile_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "max_scenarios": self.max_scenarios,
            "max_candidates": self.max_candidates,
            "max_slices": self.max_slices,
            "max_events_per_window": self.max_events_per_window,
            "max_hook_metrics": self.max_hook_metrics,
            "max_reason_codes": self.max_reason_codes,
            "max_payload_bytes": self.max_payload_bytes,
            "burst_threshold_ns": self.burst_threshold_ns,
            "quiet_threshold_ns": self.quiet_threshold_ns,
            "interarrival_buckets_ns": list(self.interarrival_buckets_ns),
            "spread_buckets": list(self.spread_buckets),
            "rounding_digits": self.rounding_digits,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkProfileV1":
        _require_schema(data, BENCHMARK_PROFILE_SCHEMA_VERSION)
        return cls(
            max_scenarios=_strict_int(
                data.get("max_scenarios"), "max_scenarios"
            ),
            max_candidates=_strict_int(
                data.get("max_candidates"), "max_candidates"
            ),
            max_slices=_strict_int(data.get("max_slices"), "max_slices"),
            max_events_per_window=_strict_int(
                data.get("max_events_per_window"), "max_events_per_window"
            ),
            max_hook_metrics=_strict_int(
                data.get("max_hook_metrics"), "max_hook_metrics"
            ),
            max_reason_codes=_strict_int(
                data.get("max_reason_codes"), "max_reason_codes"
            ),
            max_payload_bytes=_strict_int(
                data.get("max_payload_bytes"), "max_payload_bytes"
            ),
            burst_threshold_ns=_strict_int(
                data.get("burst_threshold_ns"), "burst_threshold_ns"
            ),
            quiet_threshold_ns=_strict_int(
                data.get("quiet_threshold_ns"), "quiet_threshold_ns"
            ),
            interarrival_buckets_ns=tuple(
                _strict_int(value, "interarrival bucket")
                for value in _sequence(data.get("interarrival_buckets_ns"))
            ),
            spread_buckets=tuple(
                _finite_float(value, "spread bucket")
                for value in _sequence(data.get("spread_buckets"))
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            profile_id=str(data.get("profile_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkSplitV1:
    """One immutable half-open calibration, validation, or holdout period."""

    kind: BenchmarkSplitKind
    start_ns: int
    end_ns: int
    split_id: str = ""
    schema_version: str = BENCHMARK_SPLIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_SPLIT_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark split schema")
        object.__setattr__(
            self, "kind", BenchmarkSplitKind.from_value(self.kind)
        )
        start = _bounded_int64(self.start_ns, "start_ns")
        end = _bounded_int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("benchmark split end must follow start")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        expected = _stable_id("benchmark-split", self.identity_payload())
        supplied = _optional_text(self.split_id)
        if supplied is not None and supplied != expected:
            raise ValueError("split_id does not match deterministic identity")
        object.__setattr__(self, "split_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval": "[start_ns,end_ns)",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "split_id": self.split_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkSplitV1":
        _require_schema(data, BENCHMARK_SPLIT_SCHEMA_VERSION)
        return cls(
            kind=BenchmarkSplitKind.from_value(str(data.get("kind", ""))),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            split_id=str(data.get("split_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkScenarioV1:
    """One feed epoch, degradation severity, and evaluation-period cell."""

    split_kind: BenchmarkSplitKind
    epoch_id: str
    severity_id: str
    observation_operator_id: str
    degradation_parameters: Mapping[str, JSONValue]
    scenario_id: str = ""
    degradation_config_id: str = ""
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION
    schema_version: str = BENCHMARK_SCENARIO_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_SCENARIO_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark scenario schema")
        object.__setattr__(
            self, "split_kind", BenchmarkSplitKind.from_value(self.split_kind)
        )
        object.__setattr__(self, "epoch_id", _required_text(self.epoch_id))
        object.__setattr__(
            self, "severity_id", _required_text(self.severity_id)
        )
        object.__setattr__(
            self,
            "observation_operator_id",
            _required_text(self.observation_operator_id),
        )
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError(
                "scenario event interface differs from benchmark v1"
            )
        parameters = _bounded_mapping(
            self.degradation_parameters,
            "degradation_parameters",
            max_items=64,
        )
        object.__setattr__(self, "degradation_parameters", parameters)
        expected_config = _stable_id(
            "benchmark-degradation-config",
            {
                "operator_id": self.observation_operator_id,
                "parameters": parameters,
                "parameter_namespace": "degradation",
            },
        )
        supplied_config = _optional_text(self.degradation_config_id)
        if supplied_config is not None and supplied_config != expected_config:
            raise ValueError(
                "degradation_config_id does not match deterministic identity"
            )
        object.__setattr__(self, "degradation_config_id", expected_config)
        expected = _stable_id("benchmark-scenario", self.identity_payload())
        supplied = _optional_text(self.scenario_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "scenario_id does not match deterministic identity"
            )
        object.__setattr__(self, "scenario_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_schema_version": self.event_schema_version,
            "split_kind": self.split_kind.value,
            "epoch_id": self.epoch_id,
            "severity_id": self.severity_id,
            "observation_operator_id": self.observation_operator_id,
            "degradation_config_id": self.degradation_config_id,
            "degradation_parameters": dict(self.degradation_parameters),
            "parameter_namespace": "degradation",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "scenario_id": self.scenario_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkScenarioV1":
        _require_schema(data, BENCHMARK_SCENARIO_SCHEMA_VERSION)
        return cls(
            split_kind=BenchmarkSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            epoch_id=str(data.get("epoch_id", "")),
            severity_id=str(data.get("severity_id", "")),
            observation_operator_id=str(
                data.get("observation_operator_id", "")
            ),
            degradation_parameters=_mapping(data.get("degradation_parameters")),
            scenario_id=str(data.get("scenario_id", "")),
            degradation_config_id=str(data.get("degradation_config_id", "")),
            event_schema_version=str(data.get("event_schema_version", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkCandidateV1:
    """One independently configured control or reconstruction candidate."""

    kind: BenchmarkCandidateKind
    method_id: str
    implementation_version: str
    parameters: Mapping[str, JSONValue]
    ensemble_member_ids: tuple[str, ...]
    control_kind: BenchmarkControlKind | None = None
    candidate_id: str = ""
    generator_config_id: str = ""
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION
    schema_version: str = BENCHMARK_CANDIDATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_CANDIDATE_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark candidate schema")
        kind = BenchmarkCandidateKind.from_value(self.kind)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "method_id", _required_text(self.method_id))
        object.__setattr__(
            self,
            "implementation_version",
            _required_text(self.implementation_version),
        )
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError(
                "candidate event interface differs from benchmark v1"
            )
        members = _normalized_text_tuple(self.ensemble_member_ids)
        if not members or len(members) > MAX_BENCHMARK_ENSEMBLE_MEMBERS:
            raise ValueError(
                "candidate ensemble-member count is outside limits"
            )
        object.__setattr__(self, "ensemble_member_ids", members)
        parameters = _bounded_mapping(
            self.parameters, "candidate parameters", max_items=64
        )
        object.__setattr__(self, "parameters", parameters)
        control: BenchmarkControlKind | None = None
        if self.control_kind is not None:
            control = BenchmarkControlKind.from_value(self.control_kind)
        if kind is BenchmarkCandidateKind.CONTROL and control is None:
            raise ValueError("control candidate requires control_kind")
        if kind is BenchmarkCandidateKind.CANDIDATE and control is not None:
            raise ValueError("reconstruction candidate cannot be a control")
        object.__setattr__(self, "control_kind", control)
        expected_config = _stable_id(
            "benchmark-generator-config",
            {
                "method_id": self.method_id,
                "implementation_version": self.implementation_version,
                "parameters": parameters,
                "parameter_namespace": "generator",
            },
        )
        supplied_config = _optional_text(self.generator_config_id)
        if supplied_config is not None and supplied_config != expected_config:
            raise ValueError(
                "generator_config_id does not match deterministic identity"
            )
        object.__setattr__(self, "generator_config_id", expected_config)
        expected = _stable_id("benchmark-candidate", self.identity_payload())
        supplied = _optional_text(self.candidate_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "candidate_id does not match deterministic identity"
            )
        object.__setattr__(self, "candidate_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_schema_version": self.event_schema_version,
            "kind": self.kind.value,
            "method_id": self.method_id,
            "implementation_version": self.implementation_version,
            "parameters": dict(self.parameters),
            "parameter_namespace": "generator",
            "generator_config_id": self.generator_config_id,
            "ensemble_member_ids": list(self.ensemble_member_ids),
            "control_kind": (
                self.control_kind.value
                if self.control_kind is not None
                else None
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "candidate_id": self.candidate_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkCandidateV1":
        _require_schema(data, BENCHMARK_CANDIDATE_SCHEMA_VERSION)
        control = data.get("control_kind")
        return cls(
            kind=BenchmarkCandidateKind.from_value(str(data.get("kind", ""))),
            method_id=str(data.get("method_id", "")),
            implementation_version=str(data.get("implementation_version", "")),
            parameters=_mapping(data.get("parameters")),
            ensemble_member_ids=_string_tuple(data.get("ensemble_member_ids")),
            control_kind=(
                BenchmarkControlKind.from_value(str(control))
                if control is not None
                else None
            ),
            candidate_id=str(data.get("candidate_id", "")),
            generator_config_id=str(data.get("generator_config_id", "")),
            event_schema_version=str(data.get("event_schema_version", "")),
            schema_version=str(data.get("schema_version", "")),
        )


_EXPECTED_BENCHMARK_SPLITS = (
    BenchmarkSplitKind.CALIBRATION,
    BenchmarkSplitKind.VALIDATION,
    BenchmarkSplitKind.FINAL_HOLDOUT,
)
_REQUIRED_CONTROLS = frozenset(BenchmarkControlKind)


@dataclass(frozen=True, slots=True)
class ReverseDegradationBenchmarkManifestV1:
    """Immutable benchmark design, interfaces, controls, and split boundary."""

    run_id: str
    information_manifest_id: str
    profile: BenchmarkProfileV1
    splits: tuple[BenchmarkSplitV1, ...]
    scenarios: tuple[BenchmarkScenarioV1, ...]
    candidates: tuple[BenchmarkCandidateV1, ...]
    automatic_winner: bool = False
    manifest_id: str = ""
    schema_version: str = BENCHMARK_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark manifest schema")
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "information_manifest_id",
            _required_text(self.information_manifest_id),
        )
        if not isinstance(self.profile, BenchmarkProfileV1):
            raise ValueError("benchmark manifest requires a v1 profile")
        if _strict_bool(self.automatic_winner, "automatic_winner"):
            raise ValueError("v1 benchmark forbids automatic winner selection")
        splits = tuple(self.splits)
        if any(not isinstance(item, BenchmarkSplitV1) for item in splits):
            raise ValueError("benchmark splits must use the v1 contract")
        if tuple(item.kind for item in splits) != _EXPECTED_BENCHMARK_SPLITS:
            raise ValueError(
                "benchmark splits must be calibration, validation, final_holdout"
            )
        if not (
            splits[0].end_ns
            <= splits[1].start_ns
            <= splits[1].end_ns
            <= splits[2].start_ns
        ):
            raise ValueError("benchmark splits overlap or regress")
        object.__setattr__(self, "splits", splits)
        scenarios = tuple(
            sorted(self.scenarios, key=lambda item: item.scenario_id)
        )
        if not scenarios or len(scenarios) > self.profile.max_scenarios:
            raise ValueError(
                "benchmark scenario count is outside profile limits"
            )
        if any(not isinstance(item, BenchmarkScenarioV1) for item in scenarios):
            raise ValueError("benchmark scenarios must use the v1 contract")
        if len({item.scenario_id for item in scenarios}) != len(scenarios):
            raise ValueError("benchmark scenario IDs must be unique")
        if len({item.epoch_id for item in scenarios}) < 2:
            raise ValueError("benchmark requires multiple feed epochs")
        if len({item.severity_id for item in scenarios}) < 2:
            raise ValueError(
                "benchmark requires multiple degradation severities"
            )
        scenario_splits = {item.split_kind for item in scenarios}
        if scenario_splits != {
            BenchmarkSplitKind.VALIDATION,
            BenchmarkSplitKind.FINAL_HOLDOUT,
        }:
            raise ValueError(
                "calibration data cannot be an evaluation scenario"
            )
        object.__setattr__(self, "scenarios", scenarios)
        candidates = tuple(
            sorted(self.candidates, key=lambda item: item.candidate_id)
        )
        if not candidates or len(candidates) > self.profile.max_candidates:
            raise ValueError(
                "benchmark candidate count is outside profile limits"
            )
        if any(
            not isinstance(item, BenchmarkCandidateV1) for item in candidates
        ):
            raise ValueError("benchmark candidates must use the v1 contract")
        if len({item.candidate_id for item in candidates}) != len(candidates):
            raise ValueError("benchmark candidate IDs must be unique")
        control_counts = Counter(
            item.control_kind
            for item in candidates
            if item.kind is BenchmarkCandidateKind.CONTROL
            and item.control_kind is not None
        )
        controls = set(control_counts)
        if controls != _REQUIRED_CONTROLS:
            missing = sorted(
                item.value for item in _REQUIRED_CONTROLS - controls
            )
            extra = sorted(
                item.value
                for item in controls - _REQUIRED_CONTROLS
                if item is not None
            )
            raise ValueError(
                "benchmark control set differs; "
                f"missing={missing}, extra={extra}"
            )
        if any(count != 1 for count in control_counts.values()):
            raise ValueError("benchmark requires exactly one of each control")
        if not any(
            item.kind is BenchmarkCandidateKind.CANDIDATE for item in candidates
        ):
            raise ValueError("benchmark requires a reconstruction candidate")
        degradation_ids = {item.degradation_config_id for item in scenarios}
        if any(
            item.generator_config_id in degradation_ids for item in candidates
        ):
            raise ValueError(
                "generator and degradation parameters are not independent"
            )
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "automatic_winner", False)
        expected = _stable_id("benchmark-manifest", self.identity_payload())
        supplied = _optional_text(self.manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "manifest_id does not match deterministic identity"
            )
        object.__setattr__(self, "manifest_id", expected)
        _ensure_payload_size(self.to_dict(), self.profile.max_payload_bytes)

    def split_for(self, kind: BenchmarkSplitKind) -> BenchmarkSplitV1:
        selected = BenchmarkSplitKind.from_value(kind)
        return next(item for item in self.splits if item.kind is selected)

    def scenario_by_id(self, scenario_id: str) -> BenchmarkScenarioV1:
        selected = _required_text(scenario_id)
        for item in self.scenarios:
            if item.scenario_id == selected:
                return item
        raise ValueError("scenario is not part of benchmark manifest")

    def candidate_by_id(self, candidate_id: str) -> BenchmarkCandidateV1:
        selected = _required_text(candidate_id)
        for item in self.candidates:
            if item.candidate_id == selected:
                return item
        raise ValueError("candidate is not part of benchmark manifest")

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "information_manifest_id": self.information_manifest_id,
            "profile": self.profile.to_dict(),
            "splits": [item.to_dict() for item in self.splits],
            "scenarios": [item.to_dict() for item in self.scenarios],
            "candidates": [item.to_dict() for item in self.candidates],
            "automatic_winner": False,
            "selection_policy": "report_only_no_automatic_winner",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReverseDegradationBenchmarkManifestV1":
        _require_schema(data, BENCHMARK_MANIFEST_SCHEMA_VERSION)
        return cls(
            run_id=str(data.get("run_id", "")),
            information_manifest_id=str(
                data.get("information_manifest_id", "")
            ),
            profile=BenchmarkProfileV1.from_dict(_mapping(data.get("profile"))),
            splits=tuple(
                BenchmarkSplitV1.from_dict(item)
                for item in _mapping_sequence(data.get("splits"))
            ),
            scenarios=tuple(
                BenchmarkScenarioV1.from_dict(item)
                for item in _mapping_sequence(data.get("scenarios"))
            ),
            candidates=tuple(
                BenchmarkCandidateV1.from_dict(item)
                for item in _mapping_sequence(data.get("candidates"))
            ),
            automatic_winner=_strict_bool(
                data.get("automatic_winner", False), "automatic_winner"
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReverseDegradationBenchmarkManifestV1":
        return cls.from_dict(_json_mapping(text))


def validate_benchmark_information_boundary(
    benchmark: ReverseDegradationBenchmarkManifestV1,
    information: ReconstructionInformationManifestV1,
) -> None:
    """Bind benchmark splits to #433 without changing its immutable v1 schema."""
    if information.run_id != benchmark.run_id:
        raise ValueError(
            "benchmark and information manifests use different runs"
        )
    if information.manifest_id != benchmark.information_manifest_id:
        raise ValueError("benchmark information manifest identity differs")
    if tuple(item.kind for item in information.splits) != (
        InformationSplitKind.TRAIN,
        InformationSplitKind.CALIBRATION,
        InformationSplitKind.VALIDATION,
    ):
        raise ValueError("information manifest split declaration is invalid")
    by_kind = {item.kind: item for item in information.splits}
    calibration = by_kind.get(InformationSplitKind.CALIBRATION)
    validation = by_kind.get(InformationSplitKind.VALIDATION)
    if calibration is None or validation is None:
        raise ValueError("information manifest lacks benchmark source splits")
    benchmark_calibration = benchmark.split_for(BenchmarkSplitKind.CALIBRATION)
    benchmark_validation = benchmark.split_for(BenchmarkSplitKind.VALIDATION)
    final_holdout = benchmark.split_for(BenchmarkSplitKind.FINAL_HOLDOUT)
    if (
        benchmark_calibration.start_ns != calibration.start_ns
        or benchmark_calibration.end_ns != calibration.end_ns
    ):
        raise ValueError(
            "benchmark calibration differs from information manifest"
        )
    if not (
        validation.start_ns
        <= benchmark_validation.start_ns
        < benchmark_validation.end_ns
        <= final_holdout.start_ns
        < final_holdout.end_ns
        <= validation.end_ns
    ):
        raise ValueError(
            "validation and final holdout must partition information validation"
        )


@dataclass(frozen=True, slots=True)
class BenchmarkEventV1:
    """Shared event interface for reference, degradation, and generation."""

    source_event_id: str
    symbol: str
    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    epoch_id: str
    session: str
    event_state: str
    sparsity: str
    ensemble_member_id: str | None = None
    anchor_id: str | None = None
    support_lower_mid: float | None = None
    support_upper_mid: float | None = None
    benchmark_event_id: str = ""
    schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark event schema")
        object.__setattr__(
            self, "source_event_id", _required_text(self.source_event_id)
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self,
            "event_time_ns",
            _bounded_int64(self.event_time_ns, "event_time_ns"),
        )
        sequence = _nonnegative_int(self.event_sequence, "event_sequence")
        object.__setattr__(self, "event_sequence", sequence)
        bid = _positive_float(self.bid, "bid")
        ask = _positive_float(self.ask, "ask")
        if ask < bid:
            raise ValueError("benchmark event ask precedes bid")
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        for name in ("epoch_id", "session", "event_state", "sparsity"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "ensemble_member_id",
            _optional_text(self.ensemble_member_id),
        )
        object.__setattr__(self, "anchor_id", _optional_text(self.anchor_id))
        if (self.support_lower_mid is None) != (self.support_upper_mid is None):
            raise ValueError("benchmark support interval must be paired")
        if self.support_lower_mid is not None:
            lower = _finite_float(self.support_lower_mid, "support_lower_mid")
            upper = _finite_float(self.support_upper_mid, "support_upper_mid")
            if not lower <= self.mid <= upper:
                raise ValueError(
                    "benchmark support interval excludes candidate mid"
                )
            object.__setattr__(self, "support_lower_mid", lower)
            object.__setattr__(self, "support_upper_mid", upper)
        expected = _stable_id("benchmark-event", self.identity_payload())
        supplied = _optional_text(self.benchmark_event_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "benchmark_event_id does not match deterministic identity"
            )
        object.__setattr__(self, "benchmark_event_id", expected)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def slice_key(self) -> tuple[str, str, str, str, str]:
        return (
            self.symbol,
            self.epoch_id,
            self.session,
            self.event_state,
            self.sparsity,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "symbol": self.symbol,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "bid": self.bid,
            "ask": self.ask,
            "epoch_id": self.epoch_id,
            "session": self.session,
            "event_state": self.event_state,
            "sparsity": self.sparsity,
            "ensemble_member_id": self.ensemble_member_id,
            "anchor_id": self.anchor_id,
            "support_lower_mid": self.support_lower_mid,
            "support_upper_mid": self.support_upper_mid,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "benchmark_event_id": self.benchmark_event_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkEventV1":
        _require_schema(data, BENCHMARK_EVENT_SCHEMA_VERSION)
        return cls(
            source_event_id=str(data.get("source_event_id", "")),
            symbol=str(data.get("symbol", "")),
            event_time_ns=_strict_int(
                data.get("event_time_ns"), "event_time_ns"
            ),
            event_sequence=_strict_int(
                data.get("event_sequence"), "event_sequence"
            ),
            bid=_finite_float(data.get("bid"), "bid"),
            ask=_finite_float(data.get("ask"), "ask"),
            epoch_id=str(data.get("epoch_id", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            sparsity=str(data.get("sparsity", "")),
            ensemble_member_id=_optional_text(data.get("ensemble_member_id")),
            anchor_id=_optional_text(data.get("anchor_id")),
            support_lower_mid=_optional_float(data.get("support_lower_mid")),
            support_upper_mid=_optional_float(data.get("support_upper_mid")),
            benchmark_event_id=str(data.get("benchmark_event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_observation_input(
        cls,
        event: ObservationInputEventV1,
        *,
        sparsity: str,
        ensemble_member_id: str | None = None,
    ) -> "BenchmarkEventV1":
        return cls(
            source_event_id=event.source_event_id,
            symbol=event.symbol,
            event_time_ns=event.event_time_ns,
            event_sequence=event.event_sequence,
            bid=event.bid,
            ask=event.ask,
            epoch_id=event.context.epoch_id,
            session=event.context.session or "unclassified",
            event_state=event.context.state or "unclassified",
            sparsity=sparsity,
            ensemble_member_id=ensemble_member_id,
            anchor_id=(
                event.source_event_id if event.protected_anchor else None
            ),
        )

    @classmethod
    def from_observation_output(
        cls,
        event: ObservationOutputEventV1,
        *,
        context: ObservationContextV1,
        sparsity: str,
        ensemble_member_id: str | None = None,
        anchor_id: str | None = None,
    ) -> "BenchmarkEventV1":
        return cls(
            source_event_id=event.source_event_id,
            symbol=event.symbol,
            event_time_ns=event.observed_time_ns,
            event_sequence=event.observed_sequence,
            bid=event.bid,
            ask=event.ask,
            epoch_id=context.epoch_id,
            session=context.session or "unclassified",
            event_state=context.state or "unclassified",
            sparsity=sparsity,
            ensemble_member_id=ensemble_member_id,
            anchor_id=(
                anchor_id or event.source_event_id
                if event.protected_anchor
                else None
            ),
        )

    @classmethod
    def from_synthetic_event(
        cls,
        event: SyntheticEventV1,
        *,
        epoch_id: str,
        session: str,
        event_state: str,
        sparsity: str,
        support_lower_mid: float | None = None,
        support_upper_mid: float | None = None,
    ) -> "BenchmarkEventV1":
        if event.schema_version != SYNTHETIC_EVENT_SCHEMA_VERSION:
            raise ValueError("synthetic event interface is not version one")
        return cls(
            source_event_id=event.event_id,
            symbol=event.symbol,
            event_time_ns=event.event_time_ns,
            event_sequence=event.event_sequence,
            bid=event.bid,
            ask=event.ask,
            epoch_id=epoch_id,
            session=session,
            event_state=event_state,
            sparsity=sparsity,
            ensemble_member_id=event.ensemble_member_id,
            anchor_id=event.anchor_interval_id,
            support_lower_mid=support_lower_mid,
            support_upper_mid=support_upper_mid,
        )


@dataclass(frozen=True, slots=True)
class BenchmarkExecutionEvidenceV1:
    """Bounded fit/generation resource, convergence, and failure metadata."""

    attempted: bool
    converged: bool
    wall_time_ms: int = 0
    peak_memory_bytes: int = 0
    scratch_bytes: int = 0
    durable_bytes: int = 0
    failure_reason: str | None = None
    evidence_id: str = ""
    schema_version: str = BENCHMARK_EXECUTION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_EXECUTION_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark execution evidence schema")
        attempted = _strict_bool(self.attempted, "attempted")
        converged = _strict_bool(self.converged, "converged")
        if converged and not attempted:
            raise ValueError("unattempted benchmark work cannot converge")
        object.__setattr__(self, "attempted", attempted)
        object.__setattr__(self, "converged", converged)
        for name in (
            "wall_time_ms",
            "peak_memory_bytes",
            "scratch_bytes",
            "durable_bytes",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        failure = _optional_text(self.failure_reason)
        if converged and failure is not None:
            raise ValueError("converged benchmark work cannot have a failure")
        if attempted and not converged and failure is None:
            raise ValueError("failed benchmark work requires a reason")
        object.__setattr__(self, "failure_reason", failure)
        expected = _stable_id("benchmark-execution", self.identity_payload())
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "evidence_id does not match deterministic identity"
            )
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "attempted": self.attempted,
            "converged": self.converged,
            "wall_time_ms": self.wall_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
            "scratch_bytes": self.scratch_bytes,
            "durable_bytes": self.durable_bytes,
            "failure_reason": self.failure_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BenchmarkExecutionEvidenceV1":
        _require_schema(data, BENCHMARK_EXECUTION_EVIDENCE_SCHEMA_VERSION)
        return cls(
            attempted=_strict_bool(data.get("attempted"), "attempted"),
            converged=_strict_bool(data.get("converged"), "converged"),
            wall_time_ms=_strict_int(
                data.get("wall_time_ms", 0), "wall_time_ms"
            ),
            peak_memory_bytes=_strict_int(
                data.get("peak_memory_bytes", 0), "peak_memory_bytes"
            ),
            scratch_bytes=_strict_int(
                data.get("scratch_bytes", 0), "scratch_bytes"
            ),
            durable_bytes=_strict_int(
                data.get("durable_bytes", 0), "durable_bytes"
            ),
            failure_reason=_optional_text(data.get("failure_reason")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkCandidateWindowV1:
    """Process-local candidate events plus bounded control-plane evidence."""

    scenario_id: str
    candidate_id: str
    window_id: str
    ensemble_member_id: str
    events: tuple[BenchmarkEventV1, ...]
    execution: BenchmarkExecutionEvidenceV1
    hard_constraint_violations: Mapping[str, int] = field(default_factory=dict)
    cross_series_hooks: Mapping[str, float] = field(default_factory=dict)
    strategy_hooks: Mapping[str, float] = field(default_factory=dict)
    schema_version: str = BENCHMARK_CANDIDATE_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_CANDIDATE_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark candidate-window schema")
        for name in (
            "scenario_id",
            "candidate_id",
            "window_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        events = tuple(
            sorted(
                self.events,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.benchmark_event_id,
                ),
            )
        )
        if any(not isinstance(item, BenchmarkEventV1) for item in events):
            raise ValueError("candidate-window events must use benchmark v1")
        if any(
            item.ensemble_member_id not in {None, self.ensemble_member_id}
            for item in events
        ):
            raise ValueError("candidate-window event ensemble member differs")
        object.__setattr__(self, "events", events)
        if not isinstance(self.execution, BenchmarkExecutionEvidenceV1):
            raise ValueError("candidate window requires v1 execution evidence")
        object.__setattr__(
            self,
            "hard_constraint_violations",
            _bounded_count_mapping(
                self.hard_constraint_violations,
                "hard constraint",
                MAX_BENCHMARK_REASON_CODES,
            ),
        )
        object.__setattr__(
            self,
            "cross_series_hooks",
            _bounded_metric_mapping(
                self.cross_series_hooks,
                "cross-series hook",
                MAX_BENCHMARK_HOOK_METRICS,
            ),
        )
        object.__setattr__(
            self,
            "strategy_hooks",
            _bounded_metric_mapping(
                self.strategy_hooks,
                "strategy hook",
                MAX_BENCHMARK_HOOK_METRICS,
            ),
        )

    def metadata(self) -> dict[str, JSONValue]:
        """Return workflow-safe evidence without embedding event rows."""
        return {
            "schema_version": self.schema_version,
            "scenario_id": self.scenario_id,
            "candidate_id": self.candidate_id,
            "window_id": self.window_id,
            "ensemble_member_id": self.ensemble_member_id,
            "event_count": len(self.events),
            "execution": self.execution.to_dict(),
            "hard_constraint_violations": dict(self.hard_constraint_violations),
            "cross_series_hooks": dict(self.cross_series_hooks),
            "strategy_hooks": dict(self.strategy_hooks),
            "events_inline": False,
        }


@runtime_checkable
class BenchmarkGeneratorV1(Protocol):
    """Minimal generator interface consumed by the benchmark harness."""

    candidate_id: str
    event_schema_version: str

    def generate(
        self,
        degraded_events: Sequence[BenchmarkEventV1],
        *,
        scenario: BenchmarkScenarioV1,
        window: ReconstructionWindowV1,
        ensemble_member_id: str,
    ) -> Sequence[BenchmarkEventV1]:
        """Generate one bounded member/window candidate stream."""


def generate_benchmark_candidate_window(
    generator: BenchmarkGeneratorV1,
    candidate: BenchmarkCandidateV1,
    degraded_events: Sequence[BenchmarkEventV1],
    *,
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    ensemble_member_id: str,
    execution: BenchmarkExecutionEvidenceV1,
    hard_constraint_violations: Mapping[str, int] | None = None,
    cross_series_hooks: Mapping[str, float] | None = None,
    strategy_hooks: Mapping[str, float] | None = None,
) -> BenchmarkCandidateWindowV1:
    """Invoke and validate one generator through the shared event interface."""
    if candidate.kind is not BenchmarkCandidateKind.CANDIDATE:
        raise ValueError("generator invocation requires a candidate definition")
    if generator.candidate_id != candidate.candidate_id:
        raise ValueError("generator candidate identity differs")
    if generator.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
        raise ValueError("generator does not implement benchmark event v1")
    if ensemble_member_id not in candidate.ensemble_member_ids:
        raise ValueError("ensemble member is not configured for candidate")
    events = tuple(
        generator.generate(
            degraded_events,
            scenario=scenario,
            window=window,
            ensemble_member_id=ensemble_member_id,
        )
    )
    return BenchmarkCandidateWindowV1(
        scenario_id=scenario.scenario_id,
        candidate_id=candidate.candidate_id,
        window_id=window.window_id,
        ensemble_member_id=ensemble_member_id,
        events=events,
        execution=execution,
        hard_constraint_violations=hard_constraint_violations or {},
        cross_series_hooks=cross_series_hooks or {},
        strategy_hooks=strategy_hooks or {},
    )


def degrade_benchmark_window(
    operator: ObservationOperatorV1,
    reference_events: Sequence[BenchmarkEventV1],
    *,
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    carry: ObservationCarryStateV1 | None = None,
    protected_event_ids: Sequence[str] = (),
    source_start: bool = False,
    degraded_sparsity: str = "degraded",
) -> tuple[tuple[BenchmarkEventV1, ...], ObservationApplicationResultV1]:
    """Run #435 degradation and adapt its outputs without retaining a frame."""
    if operator.operator_id != scenario.observation_operator_id:
        raise ValueError("scenario observation operator identity differs")
    source_by_id: dict[str, BenchmarkEventV1] = {}
    observation_inputs: list[ObservationInputEventV1] = []
    for event in _validated_events(reference_events):
        if event.source_event_id in source_by_id:
            raise ValueError(
                "reference source_event_id must be unique per window"
            )
        source_by_id[event.source_event_id] = event
        observation_inputs.append(
            ObservationInputEventV1(
                source_event_id=event.source_event_id,
                symbol=event.symbol,
                event_time_ns=event.event_time_ns,
                event_sequence=event.event_sequence,
                bid=event.bid,
                ask=event.ask,
                context=ObservationContextV1(
                    symbol=event.symbol,
                    epoch_id=event.epoch_id,
                    state=event.event_state,
                    session=event.session,
                    event_tag=None,
                ),
                protected_anchor=event.anchor_id is not None,
            )
        )
    result = operator.degrade(
        observation_inputs,
        window=window,
        carry=carry,
        protected_event_ids=protected_event_ids,
        source_start=source_start,
    )
    outputs: list[BenchmarkEventV1] = []
    for output in result.output_events:
        source = source_by_id[output.source_event_id]
        outputs.append(
            BenchmarkEventV1.from_observation_output(
                output,
                context=ObservationContextV1(
                    symbol=source.symbol,
                    epoch_id=source.epoch_id,
                    state=source.event_state,
                    session=source.session,
                    event_tag=None,
                ),
                sparsity=degraded_sparsity,
                ensemble_member_id=source.ensemble_member_id,
                anchor_id=(
                    source.anchor_id if output.protected_anchor else None
                ),
            )
        )
    return tuple(outputs), result


def build_benchmark_control_events(
    candidate: BenchmarkCandidateV1,
    degraded_events: Sequence[BenchmarkEventV1],
    *,
    ensemble_member_id: str,
    empirical_overlay_events: Sequence[BenchmarkEventV1] = (),
    max_events: int = DEFAULT_BENCHMARK_MAX_EVENTS_PER_WINDOW,
) -> tuple[BenchmarkEventV1, ...]:
    """Build a transparent control without reading withheld reference values."""
    if candidate.kind is not BenchmarkCandidateKind.CONTROL:
        raise ValueError("control builder requires a control candidate")
    if ensemble_member_id not in candidate.ensemble_member_ids:
        raise ValueError("control ensemble member is not configured")
    events = _validated_events(degraded_events)
    if len(events) > max_events:
        raise ValueError("degraded control input exceeds event limit")
    control = cast(BenchmarkControlKind, candidate.control_kind)
    if control is BenchmarkControlKind.NO_FILL:
        selected = events
    elif control is BenchmarkControlKind.LINEAR_INTERPOLATION:
        interval_ns = _parameter_positive_int(
            candidate.parameters, "interval_ns"
        )
        selected = _linear_interpolation_control(
            events,
            interval_ns=interval_ns,
            ensemble_member_id=ensemble_member_id,
            max_events=max_events,
        )
    elif control is BenchmarkControlKind.RESAMPLE_LAST:
        interval_ns = _parameter_positive_int(
            candidate.parameters, "interval_ns"
        )
        selected = _resample_last_control(
            events,
            interval_ns=interval_ns,
            ensemble_member_id=ensemble_member_id,
        )
    else:
        overlay = _validated_events(empirical_overlay_events)
        if len(overlay) != len(events):
            raise ValueError(
                "empirical overlay must preserve degraded-row cardinality"
            )
        selected = overlay
    if len(selected) > max_events:
        raise ValueError("control output exceeds event limit")
    return tuple(
        _with_ensemble_member(item, ensemble_member_id) for item in selected
    )


def benchmark_events_from_empirical_overlay(
    frame: Any,
    *,
    symbol: str,
    epoch_id: str,
    session: str,
    event_state: str,
    sparsity: str = "empirical_overlay",
    ensemble_member_id: str = "control",
    max_events: int = DEFAULT_BENCHMARK_MAX_EVENTS_PER_WINDOW,
) -> tuple[BenchmarkEventV1, ...]:
    """Adapt #81 row-aligned ``synth_*`` columns to benchmark events."""
    required = {"timestamp_utc_ms", "synth_bid", "synth_ask"}
    columns = set(getattr(frame, "columns", ()))
    missing = sorted(required - columns)
    if missing:
        raise ValueError(f"empirical overlay frame lacks columns: {missing}")
    selected = frame.select(sorted(required))
    if selected.height > max_events:
        raise ValueError(
            "empirical overlay frame exceeds benchmark event limit"
        )
    events: list[BenchmarkEventV1] = []
    for sequence, row in enumerate(selected.iter_rows(named=True)):
        bid = row["synth_bid"]
        ask = row["synth_ask"]
        if bid is None or ask is None:
            raise ValueError(
                "empirical overlay contains unavailable synthetic values"
            )
        timestamp_ms = _strict_int(row["timestamp_utc_ms"], "timestamp_utc_ms")
        events.append(
            BenchmarkEventV1(
                source_event_id=f"empirical-overlay:{sequence}:{timestamp_ms}",
                symbol=symbol,
                event_time_ns=timestamp_ms * 1_000_000,
                event_sequence=sequence,
                bid=_finite_float(bid, "synth_bid"),
                ask=_finite_float(ask, "synth_ask"),
                epoch_id=epoch_id,
                session=session,
                event_state=event_state,
                sparsity=sparsity,
                ensemble_member_id=ensemble_member_id,
            )
        )
    return tuple(events)


def build_benchmark_control_windows(
    manifest: ReverseDegradationBenchmarkManifestV1,
    scenario: BenchmarkScenarioV1,
    window: ReconstructionWindowV1,
    degraded_events: Sequence[BenchmarkEventV1],
    *,
    empirical_overlay_events: Sequence[BenchmarkEventV1],
) -> tuple[BenchmarkCandidateWindowV1, ...]:
    """Materialize every mandatory transparent control for one window."""
    results: list[BenchmarkCandidateWindowV1] = []
    for candidate in manifest.candidates:
        if candidate.kind is not BenchmarkCandidateKind.CONTROL:
            continue
        for member_id in candidate.ensemble_member_ids:
            events = build_benchmark_control_events(
                candidate,
                degraded_events,
                ensemble_member_id=member_id,
                empirical_overlay_events=(
                    empirical_overlay_events
                    if candidate.control_kind
                    is BenchmarkControlKind.EMPIRICAL_OVERLAY
                    else ()
                ),
                max_events=manifest.profile.max_events_per_window,
            )
            results.append(
                BenchmarkCandidateWindowV1(
                    scenario_id=scenario.scenario_id,
                    candidate_id=candidate.candidate_id,
                    window_id=window.window_id,
                    ensemble_member_id=member_id,
                    events=events,
                    execution=BenchmarkExecutionEvidenceV1(
                        attempted=True,
                        converged=True,
                    ),
                )
            )
    return tuple(results)


@dataclass(frozen=True, slots=True)
class BenchmarkSliceScoreV1:
    """One fully stratified reference/degraded/candidate comparison."""

    scenario_id: str
    candidate_id: str
    symbol: str
    epoch_id: str
    session: str
    event_state: str
    sparsity: str
    reference_event_count: int
    degraded_event_count: int
    candidate_event_count_mean: float
    metrics: Mapping[str, float]
    support: Mapping[str, JSONValue]
    slice_score_id: str = ""
    schema_version: str = BENCHMARK_SLICE_SCORE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_SLICE_SCORE_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark slice-score schema")
        for name in (
            "scenario_id",
            "candidate_id",
            "symbol",
            "epoch_id",
            "session",
            "event_state",
            "sparsity",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        for name in ("reference_event_count", "degraded_event_count"):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "candidate_event_count_mean",
            _nonnegative_float(
                self.candidate_event_count_mean,
                "candidate_event_count_mean",
            ),
        )
        object.__setattr__(
            self,
            "metrics",
            _bounded_metric_mapping(self.metrics, "slice metric", 64),
        )
        object.__setattr__(
            self,
            "support",
            _bounded_mapping(self.support, "slice support", max_items=64),
        )
        expected = _stable_id("benchmark-slice-score", self.identity_payload())
        supplied = _optional_text(self.slice_score_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "slice_score_id does not match deterministic identity"
            )
        object.__setattr__(self, "slice_score_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scenario_id": self.scenario_id,
            "candidate_id": self.candidate_id,
            "stratum": {
                "symbol": self.symbol,
                "epoch_id": self.epoch_id,
                "session": self.session,
                "event_state": self.event_state,
                "sparsity": self.sparsity,
            },
            "reference_event_count": self.reference_event_count,
            "degraded_event_count": self.degraded_event_count,
            "candidate_event_count_mean": self.candidate_event_count_mean,
            "metrics": dict(self.metrics),
            "support": dict(self.support),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "slice_score_id": self.slice_score_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkSliceScoreV1":
        _require_schema(data, BENCHMARK_SLICE_SCORE_SCHEMA_VERSION)
        stratum = _mapping(data.get("stratum"))
        return cls(
            scenario_id=str(data.get("scenario_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            symbol=str(stratum.get("symbol", "")),
            epoch_id=str(stratum.get("epoch_id", "")),
            session=str(stratum.get("session", "")),
            event_state=str(stratum.get("event_state", "")),
            sparsity=str(stratum.get("sparsity", "")),
            reference_event_count=_strict_int(
                data.get("reference_event_count"), "reference_event_count"
            ),
            degraded_event_count=_strict_int(
                data.get("degraded_event_count"), "degraded_event_count"
            ),
            candidate_event_count_mean=_finite_float(
                data.get("candidate_event_count_mean"),
                "candidate_event_count_mean",
            ),
            metrics=cast(Mapping[str, float], _mapping(data.get("metrics"))),
            support=_mapping(data.get("support")),
            slice_score_id=str(data.get("slice_score_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkCandidateScoreV1:
    """Bounded scenario/candidate score without a winner declaration."""

    scenario_id: str
    candidate_id: str
    split_kind: BenchmarkSplitKind
    slice_scores: tuple[BenchmarkSliceScoreV1, ...]
    aggregate_metrics: Mapping[str, float]
    uncertainty_metrics: Mapping[str, JSONValue]
    execution_summary: Mapping[str, JSONValue]
    cross_series_hooks: Mapping[str, JSONValue]
    strategy_hooks: Mapping[str, JSONValue]
    hard_constraint_violations: Mapping[str, int]
    relative_to_no_fill: Mapping[str, float]
    promotion_eligible: bool
    automatic_winner: bool = False
    candidate_score_id: str = ""
    schema_version: str = BENCHMARK_CANDIDATE_SCORE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_CANDIDATE_SCORE_SCHEMA_VERSION:
            raise ValueError("unsupported benchmark candidate-score schema")
        object.__setattr__(
            self, "scenario_id", _required_text(self.scenario_id)
        )
        object.__setattr__(
            self, "candidate_id", _required_text(self.candidate_id)
        )
        object.__setattr__(
            self, "split_kind", BenchmarkSplitKind.from_value(self.split_kind)
        )
        slices = tuple(
            sorted(self.slice_scores, key=lambda item: item.slice_score_id)
        )
        if not slices:
            raise ValueError(
                "candidate score requires stratified slice evidence"
            )
        if any(not isinstance(item, BenchmarkSliceScoreV1) for item in slices):
            raise ValueError("candidate score slices must use v1 contracts")
        if any(
            item.scenario_id != self.scenario_id
            or item.candidate_id != self.candidate_id
            for item in slices
        ):
            raise ValueError("candidate score slice identity differs")
        object.__setattr__(self, "slice_scores", slices)
        object.__setattr__(
            self,
            "aggregate_metrics",
            _bounded_metric_mapping(
                self.aggregate_metrics, "aggregate metric", 64
            ),
        )
        for name in (
            "uncertainty_metrics",
            "execution_summary",
            "cross_series_hooks",
            "strategy_hooks",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_mapping(getattr(self, name), name, max_items=256),
            )
        object.__setattr__(
            self,
            "hard_constraint_violations",
            _bounded_count_mapping(
                self.hard_constraint_violations,
                "hard constraint",
                MAX_BENCHMARK_REASON_CODES,
            ),
        )
        object.__setattr__(
            self,
            "relative_to_no_fill",
            _bounded_metric_mapping(
                self.relative_to_no_fill, "no-fill delta", 16
            ),
        )
        object.__setattr__(
            self,
            "promotion_eligible",
            _strict_bool(self.promotion_eligible, "promotion_eligible"),
        )
        if self.promotion_eligible and self.hard_constraint_violations:
            raise ValueError(
                "hard constraint violations always block promotion eligibility"
            )
        if _strict_bool(self.automatic_winner, "automatic_winner"):
            raise ValueError(
                "candidate score cannot select an automatic winner"
            )
        object.__setattr__(self, "automatic_winner", False)
        expected = _stable_id(
            "benchmark-candidate-score", self.identity_payload()
        )
        supplied = _optional_text(self.candidate_score_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "candidate_score_id does not match deterministic identity"
            )
        object.__setattr__(self, "candidate_score_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scenario_id": self.scenario_id,
            "candidate_id": self.candidate_id,
            "split_kind": self.split_kind.value,
            "slice_scores": [item.to_dict() for item in self.slice_scores],
            "aggregate_metrics": dict(self.aggregate_metrics),
            "uncertainty_metrics": dict(self.uncertainty_metrics),
            "execution_summary": dict(self.execution_summary),
            "cross_series_hooks": dict(self.cross_series_hooks),
            "strategy_hooks": dict(self.strategy_hooks),
            "hard_constraint_violations": dict(self.hard_constraint_violations),
            "relative_to_no_fill": dict(self.relative_to_no_fill),
            "promotion_eligible": self.promotion_eligible,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "candidate_score_id": self.candidate_score_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkCandidateScoreV1":
        _require_schema(data, BENCHMARK_CANDIDATE_SCORE_SCHEMA_VERSION)
        return cls(
            scenario_id=str(data.get("scenario_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            split_kind=BenchmarkSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            slice_scores=tuple(
                BenchmarkSliceScoreV1.from_dict(item)
                for item in _mapping_sequence(data.get("slice_scores"))
            ),
            aggregate_metrics=cast(
                Mapping[str, float], _mapping(data.get("aggregate_metrics"))
            ),
            uncertainty_metrics=_mapping(data.get("uncertainty_metrics")),
            execution_summary=_mapping(data.get("execution_summary")),
            cross_series_hooks=_mapping(data.get("cross_series_hooks")),
            strategy_hooks=_mapping(data.get("strategy_hooks")),
            hard_constraint_violations=cast(
                Mapping[str, int],
                _mapping(data.get("hard_constraint_violations")),
            ),
            relative_to_no_fill=cast(
                Mapping[str, float], _mapping(data.get("relative_to_no_fill"))
            ),
            promotion_eligible=_strict_bool(
                data.get("promotion_eligible"), "promotion_eligible"
            ),
            automatic_winner=_strict_bool(
                data.get("automatic_winner", False), "automatic_winner"
            ),
            candidate_score_id=str(data.get("candidate_score_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReverseDegradationScorecardV1:
    """Reproducible bounded benchmark result with no automatic winner."""

    manifest_id: str
    candidate_scores: tuple[BenchmarkCandidateScoreV1, ...]
    automatic_winner: bool = False
    scorecard_id: str = ""
    schema_version: str = BENCHMARK_SCORECARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_SCORECARD_SCHEMA_VERSION:
            raise ValueError("unsupported reverse-degradation scorecard schema")
        object.__setattr__(
            self, "manifest_id", _required_text(self.manifest_id)
        )
        scores = tuple(
            sorted(
                self.candidate_scores, key=lambda item: item.candidate_score_id
            )
        )
        if not scores:
            raise ValueError("benchmark scorecard requires candidate scores")
        if any(
            not isinstance(item, BenchmarkCandidateScoreV1) for item in scores
        ):
            raise ValueError("scorecard candidates must use v1 score contracts")
        if len(
            {(item.scenario_id, item.candidate_id) for item in scores}
        ) != len(scores):
            raise ValueError(
                "scorecard scenario/candidate cells must be unique"
            )
        object.__setattr__(self, "candidate_scores", scores)
        if _strict_bool(self.automatic_winner, "automatic_winner"):
            raise ValueError("scorecard cannot select an automatic winner")
        object.__setattr__(self, "automatic_winner", False)
        expected = _stable_id(
            "reverse-degradation-scorecard", self.identity_payload()
        )
        supplied = _optional_text(self.scorecard_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "scorecard_id does not match deterministic identity"
            )
        object.__setattr__(self, "scorecard_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "candidate_scores": [
                item.to_dict() for item in self.candidate_scores
            ],
            "automatic_winner": False,
            "winner_candidate_id": None,
            "interpretation": (
                "stratified evidence; aggregate soft loss is advisory and hard "
                "constraint violations always block promotion"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "scorecard_id": self.scorecard_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReverseDegradationScorecardV1":
        _require_schema(data, BENCHMARK_SCORECARD_SCHEMA_VERSION)
        return cls(
            manifest_id=str(data.get("manifest_id", "")),
            candidate_scores=tuple(
                BenchmarkCandidateScoreV1.from_dict(item)
                for item in _mapping_sequence(data.get("candidate_scores"))
            ),
            automatic_winner=_strict_bool(
                data.get("automatic_winner", False), "automatic_winner"
            ),
            scorecard_id=str(data.get("scorecard_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReverseDegradationScorecardV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(slots=True)
class _OnlineMetric:
    """Constant-space summary for one numeric hook or diversity metric."""

    count: int = 0
    total: float = 0.0
    minimum: float | None = None
    maximum: float | None = None

    def update(self, value: float) -> None:
        selected = _finite_float(value, "online metric")
        self.count += 1
        self.total += selected
        self.minimum = (
            selected if self.minimum is None else min(self.minimum, selected)
        )
        self.maximum = (
            selected if self.maximum is None else max(self.maximum, selected)
        )

    @property
    def mean(self) -> float:
        return self.total / self.count if self.count else 0.0

    def payload(self, digits: int) -> dict[str, JSONValue]:
        return {
            "count": self.count,
            "mean": _rounded(self.mean, digits),
            "minimum": (
                _rounded(self.minimum, digits)
                if self.minimum is not None
                else None
            ),
            "maximum": (
                _rounded(self.maximum, digits)
                if self.maximum is not None
                else None
            ),
        }


@dataclass(slots=True)
class _StreamStats:
    """Constant-space event, timing, quote, path, and histogram state."""

    interarrival_buckets: tuple[int, ...]
    spread_buckets: tuple[float, ...]
    burst_threshold_ns: int
    quiet_threshold_ns: int
    event_count: int = 0
    exposure_ns: int = 0
    interarrival_count: int = 0
    burst_count: int = 0
    quiet_count: int = 0
    burst_duration_total_ns: int = 0
    burst_duration_count: int = 0
    current_burst_duration_ns: int = 0
    quiet_duration_total_ns: int = 0
    quiet_duration_count: int = 0
    bid_total: float = 0.0
    ask_total: float = 0.0
    spread_total: float = 0.0
    bid_transition_total: float = 0.0
    ask_transition_total: float = 0.0
    mid_transition_total: float = 0.0
    spread_transition_total: float = 0.0
    quote_transition_count: int = 0
    spread_transition_count: int = 0
    first_mid: float | None = None
    last_mid: float | None = None
    minimum_mid: float | None = None
    maximum_mid: float | None = None
    previous_time_ns: int | None = None
    previous_bid: float | None = None
    previous_ask: float | None = None
    previous_mid: float | None = None
    previous_spread: float | None = None
    interarrival_histogram: list[int] = field(default_factory=list)
    spread_histogram: list[int] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.interarrival_histogram = [0] * (len(self.interarrival_buckets) + 1)
        self.spread_histogram = [0] * (len(self.spread_buckets) + 1)

    def add_exposure(self, duration_ns: int) -> None:
        self.exposure_ns += _nonnegative_int(duration_ns, "exposure_ns")

    def update(self, events: Sequence[BenchmarkEventV1]) -> None:
        for event in sorted(
            events,
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        ):
            self.event_count += 1
            spread = event.spread
            mid = event.mid
            self.bid_total += event.bid
            self.ask_total += event.ask
            self.spread_total += spread
            self.spread_histogram[
                _bucket_index(spread, self.spread_buckets)
            ] += 1
            if self.first_mid is None:
                self.first_mid = mid
            self.last_mid = mid
            self.minimum_mid = (
                mid if self.minimum_mid is None else min(self.minimum_mid, mid)
            )
            self.maximum_mid = (
                mid if self.maximum_mid is None else max(self.maximum_mid, mid)
            )
            if self.previous_time_ns is not None:
                gap = max(0, event.event_time_ns - self.previous_time_ns)
                self.interarrival_count += 1
                self.interarrival_histogram[
                    _bucket_index(gap, self.interarrival_buckets)
                ] += 1
                if gap <= self.burst_threshold_ns:
                    self.burst_count += 1
                    self.current_burst_duration_ns += gap
                elif self.current_burst_duration_ns:
                    self.burst_duration_total_ns += (
                        self.current_burst_duration_ns
                    )
                    self.burst_duration_count += 1
                    self.current_burst_duration_ns = 0
                if gap >= self.quiet_threshold_ns:
                    self.quiet_count += 1
                    self.quiet_duration_total_ns += gap
                    self.quiet_duration_count += 1
            if self.previous_bid is not None:
                self.bid_transition_total += abs(event.bid - self.previous_bid)
                self.ask_transition_total += abs(
                    event.ask - cast(float, self.previous_ask)
                )
                self.mid_transition_total += abs(
                    mid - cast(float, self.previous_mid)
                )
                self.quote_transition_count += 1
            if self.previous_spread is not None:
                self.spread_transition_total += abs(
                    spread - self.previous_spread
                )
                self.spread_transition_count += 1
            self.previous_time_ns = event.event_time_ns
            self.previous_bid = event.bid
            self.previous_ask = event.ask
            self.previous_mid = mid
            self.previous_spread = spread

    @property
    def intensity_per_second(self) -> float:
        if not self.exposure_ns:
            return 0.0
        return self.event_count / (self.exposure_ns / 1_000_000_000)

    @property
    def spread_mean(self) -> float:
        return self.spread_total / self.event_count if self.event_count else 0.0

    @property
    def bid_mean(self) -> float:
        return self.bid_total / self.event_count if self.event_count else 0.0

    @property
    def ask_mean(self) -> float:
        return self.ask_total / self.event_count if self.event_count else 0.0

    @property
    def bid_transition_mean(self) -> float:
        if not self.quote_transition_count:
            return 0.0
        return self.bid_transition_total / self.quote_transition_count

    @property
    def ask_transition_mean(self) -> float:
        if not self.quote_transition_count:
            return 0.0
        return self.ask_transition_total / self.quote_transition_count

    @property
    def mid_transition_mean(self) -> float:
        if not self.quote_transition_count:
            return 0.0
        return self.mid_transition_total / self.quote_transition_count

    @property
    def spread_transition_mean(self) -> float:
        if not self.spread_transition_count:
            return 0.0
        return self.spread_transition_total / self.spread_transition_count

    @property
    def mid_range(self) -> float:
        if self.minimum_mid is None or self.maximum_mid is None:
            return 0.0
        return self.maximum_mid - self.minimum_mid

    @property
    def burst_rate(self) -> float:
        return (
            self.burst_count / self.interarrival_count
            if self.interarrival_count
            else 0.0
        )

    @property
    def quiet_rate(self) -> float:
        return (
            self.quiet_count / self.interarrival_count
            if self.interarrival_count
            else 0.0
        )

    @property
    def burst_duration_mean_ns(self) -> float:
        total = self.burst_duration_total_ns + self.current_burst_duration_ns
        count = self.burst_duration_count + int(
            self.current_burst_duration_ns > 0
        )
        return total / count if count else 0.0

    @property
    def quiet_duration_mean_ns(self) -> float:
        if not self.quiet_duration_count:
            return 0.0
        return self.quiet_duration_total_ns / self.quiet_duration_count


@dataclass(slots=True)
class _MemberState:
    """Per-member stratified statistics and support accounting."""

    slices: dict[tuple[str, str, str, str, str], _StreamStats] = field(
        default_factory=dict
    )
    support_interval_count: int = 0
    covered_reference_count: int = 0
    anchor_reference_count: int = 0
    anchor_preserved_count: int = 0


@dataclass(slots=True)
class _ComparisonState:
    """Bounded online state for one scenario and candidate cell."""

    reference_slices: dict[tuple[str, str, str, str, str], _StreamStats] = (
        field(default_factory=dict)
    )
    degraded_slices: dict[tuple[str, str, str, str, str], _StreamStats] = field(
        default_factory=dict
    )
    member_states: dict[str, _MemberState] = field(default_factory=dict)
    hard_violations: Counter[str] = field(default_factory=Counter)
    failure_reasons: Counter[str] = field(default_factory=Counter)
    attempted_count: int = 0
    converged_count: int = 0
    wall_time_ms: int = 0
    peak_memory_bytes: int = 0
    scratch_bytes: int = 0
    durable_bytes: int = 0
    cross_series_hooks: dict[str, _OnlineMetric] = field(default_factory=dict)
    strategy_hooks: dict[str, _OnlineMetric] = field(default_factory=dict)
    diversity_by_slice: dict[tuple[str, str, str, str, str], _OnlineMetric] = (
        field(default_factory=dict)
    )
    first_window_start_ns: int | None = None
    last_window_end_ns: int | None = None
    window_count: int = 0


class ReverseDegradationBenchmarkV1:
    """Online benchmark engine retaining bounded aggregate state only."""

    def __init__(self, manifest: ReverseDegradationBenchmarkManifestV1) -> None:
        if not isinstance(manifest, ReverseDegradationBenchmarkManifestV1):
            raise ValueError("benchmark engine requires a v1 manifest")
        self.manifest = manifest
        self._states: dict[tuple[str, str], _ComparisonState] = {}
        self._slice_count = 0
        self._finalized = False

    def consume_window(
        self,
        *,
        scenario_id: str,
        window: ReconstructionWindowV1,
        reference_events: Sequence[BenchmarkEventV1],
        degraded_events: Sequence[BenchmarkEventV1],
        candidate_windows: Sequence[BenchmarkCandidateWindowV1],
    ) -> None:
        """Consume one complete window without retaining any event rows."""
        if self._finalized:
            raise ValueError("benchmark engine is already finalized")
        scenario = self.manifest.scenario_by_id(scenario_id)
        if window.run_id != self.manifest.run_id:
            raise ValueError("benchmark window run differs from manifest")
        split = self.manifest.split_for(scenario.split_kind)
        if not (
            split.start_ns
            <= window.core_start_ns
            < window.core_end_ns
            <= split.end_ns
        ):
            raise ValueError("benchmark window falls outside scenario split")
        reference = self._validate_window_events(
            reference_events, window, scenario, "reference"
        )
        degraded = self._validate_window_events(
            degraded_events, window, scenario, "degraded"
        )
        grouped = self._validate_candidate_windows(
            candidate_windows, scenario=scenario, window=window
        )
        duration_ns = window.core_end_ns - window.core_start_ns
        for candidate in self.manifest.candidates:
            key = (scenario.scenario_id, candidate.candidate_id)
            state = self._states.setdefault(key, _ComparisonState())
            if state.first_window_start_ns is None:
                if window.core_start_ns != split.start_ns:
                    raise ValueError(
                        "benchmark scenario does not start at split boundary"
                    )
                state.first_window_start_ns = window.core_start_ns
            elif window.core_start_ns != state.last_window_end_ns:
                raise ValueError(
                    "benchmark windows must be contiguous and ordered"
                )
            state.last_window_end_ns = window.core_end_ns
            state.window_count += 1
            windows = grouped[candidate.candidate_id]
            self._consume_candidate_cell(
                state,
                candidate,
                reference,
                degraded,
                windows,
                duration_ns=duration_ns,
            )

    def finalize(self) -> ReverseDegradationScorecardV1:
        """Freeze deterministic stratified scorecards and discard engine use."""
        if self._finalized:
            raise ValueError("benchmark engine is already finalized")
        expected = {
            (scenario.scenario_id, candidate.candidate_id)
            for scenario in self.manifest.scenarios
            for candidate in self.manifest.candidates
        }
        if set(self._states) != expected:
            missing = sorted(expected - set(self._states))
            raise ValueError(
                f"benchmark has unprocessed scenario cells: {missing}"
            )
        for scenario in self.manifest.scenarios:
            split = self.manifest.split_for(scenario.split_kind)
            for candidate in self.manifest.candidates:
                state = self._states[
                    (scenario.scenario_id, candidate.candidate_id)
                ]
                if state.last_window_end_ns != split.end_ns:
                    raise ValueError(
                        "benchmark scenario does not cover its complete split"
                    )
        raw: dict[tuple[str, str], BenchmarkCandidateScoreV1] = {}
        for scenario in self.manifest.scenarios:
            for candidate in self.manifest.candidates:
                key = (scenario.scenario_id, candidate.candidate_id)
                raw[key] = self._candidate_score(
                    scenario,
                    candidate,
                    self._states[key],
                    relative_to_no_fill={},
                )
        no_fill_ids = {
            item.candidate_id
            for item in self.manifest.candidates
            if item.control_kind is BenchmarkControlKind.NO_FILL
        }
        no_fill_id = next(iter(no_fill_ids))
        completed: list[BenchmarkCandidateScoreV1] = []
        for scenario in self.manifest.scenarios:
            baseline = raw[(scenario.scenario_id, no_fill_id)]
            for candidate in self.manifest.candidates:
                score = raw[(scenario.scenario_id, candidate.candidate_id)]
                deltas = {
                    "mean_soft_loss_delta": _rounded(
                        score.aggregate_metrics["mean_soft_loss"]
                        - baseline.aggregate_metrics["mean_soft_loss"],
                        self.manifest.profile.rounding_digits,
                    ),
                    "worst_slice_soft_loss_delta": _rounded(
                        score.aggregate_metrics["worst_slice_soft_loss"]
                        - baseline.aggregate_metrics["worst_slice_soft_loss"],
                        self.manifest.profile.rounding_digits,
                    ),
                }
                completed.append(
                    BenchmarkCandidateScoreV1(
                        scenario_id=score.scenario_id,
                        candidate_id=score.candidate_id,
                        split_kind=score.split_kind,
                        slice_scores=score.slice_scores,
                        aggregate_metrics=score.aggregate_metrics,
                        uncertainty_metrics=score.uncertainty_metrics,
                        execution_summary=score.execution_summary,
                        cross_series_hooks=score.cross_series_hooks,
                        strategy_hooks=score.strategy_hooks,
                        hard_constraint_violations=(
                            score.hard_constraint_violations
                        ),
                        relative_to_no_fill=deltas,
                        promotion_eligible=score.promotion_eligible,
                    )
                )
        result = ReverseDegradationScorecardV1(
            manifest_id=self.manifest.manifest_id,
            candidate_scores=tuple(completed),
        )
        _ensure_payload_size(
            result.to_dict(), self.manifest.profile.max_payload_bytes
        )
        self._finalized = True
        return result

    def _validate_window_events(
        self,
        values: Sequence[BenchmarkEventV1],
        window: ReconstructionWindowV1,
        scenario: BenchmarkScenarioV1,
        label: str,
    ) -> tuple[BenchmarkEventV1, ...]:
        events = _validated_events(values)
        if len(events) > self.manifest.profile.max_events_per_window:
            raise ValueError(f"{label} events exceed window limit")
        for event in events:
            if not window.owns_event_time(event.event_time_ns):
                raise ValueError(f"{label} event is outside window ownership")
            if event.epoch_id != scenario.epoch_id:
                raise ValueError(f"{label} event epoch differs from scenario")
            if event.symbol not in {item.upper() for item in window.symbols}:
                raise ValueError(f"{label} event symbol differs from window")
        return events

    def _validate_candidate_windows(
        self,
        values: Sequence[BenchmarkCandidateWindowV1],
        *,
        scenario: BenchmarkScenarioV1,
        window: ReconstructionWindowV1,
    ) -> dict[str, tuple[BenchmarkCandidateWindowV1, ...]]:
        grouped: dict[str, list[BenchmarkCandidateWindowV1]] = {}
        for value in values:
            if not isinstance(value, BenchmarkCandidateWindowV1):
                raise ValueError("candidate window must use the v1 contract")
            if value.scenario_id != scenario.scenario_id:
                raise ValueError("candidate-window scenario differs")
            if value.window_id != window.window_id:
                raise ValueError("candidate-window identity differs")
            candidate = self.manifest.candidate_by_id(value.candidate_id)
            if value.ensemble_member_id not in candidate.ensemble_member_ids:
                raise ValueError("candidate-window member is not configured")
            self._validate_window_events(
                value.events, window, scenario, "candidate"
            )
            grouped.setdefault(candidate.candidate_id, []).append(value)
        result: dict[str, tuple[BenchmarkCandidateWindowV1, ...]] = {}
        for candidate in self.manifest.candidates:
            selected = tuple(
                sorted(
                    grouped.get(candidate.candidate_id, ()),
                    key=lambda item: item.ensemble_member_id,
                )
            )
            if tuple(item.ensemble_member_id for item in selected) != (
                candidate.ensemble_member_ids
            ):
                raise ValueError(
                    "candidate window does not cover configured ensemble members"
                )
            result[candidate.candidate_id] = selected
        return result

    def _consume_candidate_cell(
        self,
        state: _ComparisonState,
        candidate: BenchmarkCandidateV1,
        reference: Sequence[BenchmarkEventV1],
        degraded: Sequence[BenchmarkEventV1],
        windows: Sequence[BenchmarkCandidateWindowV1],
        *,
        duration_ns: int,
    ) -> None:
        reference_by_slice = _events_by_slice(reference)
        degraded_by_slice = _events_by_slice(degraded)
        member_by_slice: dict[
            str,
            dict[tuple[str, str, str, str, str], tuple[BenchmarkEventV1, ...]],
        ] = {}
        for window in windows:
            member_by_slice[window.ensemble_member_id] = _events_by_slice(
                window.events
            )
        slice_keys = set(reference_by_slice) | set(degraded_by_slice)
        for selected in member_by_slice.values():
            slice_keys.update(selected)
        for slice_key in sorted(slice_keys):
            reference_stats = self._stream_stats(
                state.reference_slices, slice_key
            )
            degraded_stats = self._stream_stats(
                state.degraded_slices, slice_key
            )
            reference_stats.add_exposure(duration_ns)
            degraded_stats.add_exposure(duration_ns)
            reference_stats.update(reference_by_slice.get(slice_key, ()))
            degraded_stats.update(degraded_by_slice.get(slice_key, ()))
            for member_id in candidate.ensemble_member_ids:
                member = state.member_states.setdefault(
                    member_id, _MemberState()
                )
                candidate_stats = self._stream_stats(member.slices, slice_key)
                candidate_stats.add_exposure(duration_ns)
                candidate_stats.update(
                    member_by_slice[member_id].get(slice_key, ())
                )
        reference_mid = {
            (item.symbol, item.event_time_ns): item.mid for item in reference
        }
        reference_anchors = {
            item.anchor_id for item in reference if item.anchor_id
        }
        for window in windows:
            member = state.member_states[window.ensemble_member_id]
            member.anchor_reference_count += len(reference_anchors)
            candidate_anchors = {
                item.anchor_id for item in window.events if item.anchor_id
            }
            preserved = len(reference_anchors & candidate_anchors)
            member.anchor_preserved_count += preserved
            missing_anchors = len(reference_anchors) - preserved
            if missing_anchors:
                state.hard_violations[
                    "historical_anchor_missing"
                ] += missing_anchors
            for event in window.events:
                lower = event.support_lower_mid
                upper = event.support_upper_mid
                if lower is None or upper is None:
                    continue
                member.support_interval_count += 1
                reference_value = reference_mid.get(
                    (event.symbol, event.event_time_ns)
                )
                if (
                    reference_value is not None
                    and lower <= reference_value <= upper
                ):
                    member.covered_reference_count += 1
            execution = window.execution
            state.attempted_count += int(execution.attempted)
            state.converged_count += int(execution.converged)
            state.wall_time_ms += execution.wall_time_ms
            state.peak_memory_bytes = max(
                state.peak_memory_bytes, execution.peak_memory_bytes
            )
            state.scratch_bytes += execution.scratch_bytes
            state.durable_bytes += execution.durable_bytes
            if execution.failure_reason is not None:
                state.failure_reasons[execution.failure_reason] += 1
            state.hard_violations.update(window.hard_constraint_violations)
            self._update_hooks(
                state.cross_series_hooks, window.cross_series_hooks
            )
            self._update_hooks(state.strategy_hooks, window.strategy_hooks)
        self._update_diversity(state, windows)

    def _stream_stats(
        self,
        target: dict[tuple[str, str, str, str, str], _StreamStats],
        key: tuple[str, str, str, str, str],
    ) -> _StreamStats:
        existing = target.get(key)
        if existing is not None:
            return existing
        self._slice_count += 1
        if self._slice_count > self.manifest.profile.max_slices:
            raise ValueError("benchmark slice limit exceeded")
        profile = self.manifest.profile
        created = _StreamStats(
            profile.interarrival_buckets_ns,
            profile.spread_buckets,
            profile.burst_threshold_ns,
            profile.quiet_threshold_ns,
        )
        target[key] = created
        return created

    def _update_hooks(
        self,
        target: dict[str, _OnlineMetric],
        values: Mapping[str, float],
    ) -> None:
        for name, value in sorted(values.items()):
            metric = target.setdefault(name, _OnlineMetric())
            metric.update(value)
        if len(target) > self.manifest.profile.max_hook_metrics:
            raise ValueError("benchmark hook metric limit exceeded")

    def _update_diversity(
        self,
        state: _ComparisonState,
        windows: Sequence[BenchmarkCandidateWindowV1],
    ) -> None:
        if len(windows) < 2:
            return
        member_maps: dict[str, dict[tuple[str, int], BenchmarkEventV1]] = {
            window.ensemble_member_id: {
                (event.symbol, event.event_time_ns): event
                for event in window.events
            }
            for window in windows
        }
        for left_id, right_id in combinations(sorted(member_maps), 2):
            left = member_maps[left_id]
            right = member_maps[right_id]
            for key in sorted(set(left) & set(right)):
                left_event = left[key]
                right_event = right[key]
                metric = state.diversity_by_slice.setdefault(
                    left_event.slice_key, _OnlineMetric()
                )
                metric.update(abs(left_event.mid - right_event.mid))

    def _candidate_score(
        self,
        scenario: BenchmarkScenarioV1,
        candidate: BenchmarkCandidateV1,
        state: _ComparisonState,
        *,
        relative_to_no_fill: Mapping[str, float],
    ) -> BenchmarkCandidateScoreV1:
        keys = set(state.reference_slices) | set(state.degraded_slices)
        for member in state.member_states.values():
            keys.update(member.slices)
        slices = tuple(
            self._slice_score(scenario, candidate, state, key)
            for key in sorted(keys)
        )
        losses = [item.metrics["soft_loss"] for item in slices]
        gains = [
            item.metrics["restoration_gain_vs_degraded"] for item in slices
        ]
        aggregate = {
            "mean_soft_loss": _rounded(
                sum(losses) / len(losses), self.manifest.profile.rounding_digits
            ),
            "worst_slice_soft_loss": _rounded(
                max(losses), self.manifest.profile.rounding_digits
            ),
            "mean_restoration_gain_vs_degraded": _rounded(
                sum(gains) / len(gains), self.manifest.profile.rounding_digits
            ),
            "slice_count": float(len(slices)),
            "window_count": float(state.window_count),
        }
        support_count = sum(
            item.support_interval_count for item in state.member_states.values()
        )
        covered_count = sum(
            item.covered_reference_count
            for item in state.member_states.values()
        )
        anchor_count = sum(
            item.anchor_reference_count for item in state.member_states.values()
        )
        anchors_preserved = sum(
            item.anchor_preserved_count for item in state.member_states.values()
        )
        diversity_count = sum(
            item.count for item in state.diversity_by_slice.values()
        )
        diversity_total = sum(
            item.total for item in state.diversity_by_slice.values()
        )
        uncertainty: dict[str, JSONValue] = {
            "support_interval_count": support_count,
            "covered_reference_count": covered_count,
            "coverage_rate": _rounded(
                covered_count / support_count if support_count else 0.0,
                self.manifest.profile.rounding_digits,
            ),
            "anchor_reference_count": anchor_count,
            "anchor_preserved_count": anchors_preserved,
            "anchor_preservation_rate": _rounded(
                anchors_preserved / anchor_count if anchor_count else 1.0,
                self.manifest.profile.rounding_digits,
            ),
            "ensemble_member_count": len(candidate.ensemble_member_ids),
            "ensemble_common_event_comparison_count": diversity_count,
            "ensemble_mean_absolute_mid_diversity": _rounded(
                diversity_total / diversity_count if diversity_count else 0.0,
                self.manifest.profile.rounding_digits,
            ),
        }
        failures = sum(state.failure_reasons.values())
        execution: dict[str, JSONValue] = {
            "attempted_count": state.attempted_count,
            "converged_count": state.converged_count,
            "failure_count": failures,
            "failure_reasons": dict(sorted(state.failure_reasons.items())),
            "wall_time_ms": state.wall_time_ms,
            "peak_memory_bytes": state.peak_memory_bytes,
            "scratch_bytes": state.scratch_bytes,
            "durable_bytes": state.durable_bytes,
        }
        promotion_eligible = (
            candidate.kind is BenchmarkCandidateKind.CANDIDATE
            and not state.hard_violations
            and failures == 0
            and state.attempted_count > 0
            and state.attempted_count == state.converged_count
        )
        return BenchmarkCandidateScoreV1(
            scenario_id=scenario.scenario_id,
            candidate_id=candidate.candidate_id,
            split_kind=scenario.split_kind,
            slice_scores=slices,
            aggregate_metrics=aggregate,
            uncertainty_metrics=uncertainty,
            execution_summary=execution,
            cross_series_hooks={
                name: value.payload(self.manifest.profile.rounding_digits)
                for name, value in sorted(state.cross_series_hooks.items())
            },
            strategy_hooks={
                name: value.payload(self.manifest.profile.rounding_digits)
                for name, value in sorted(state.strategy_hooks.items())
            },
            hard_constraint_violations=dict(
                sorted(state.hard_violations.items())
            ),
            relative_to_no_fill=relative_to_no_fill,
            promotion_eligible=promotion_eligible,
        )

    def _slice_score(
        self,
        scenario: BenchmarkScenarioV1,
        candidate: BenchmarkCandidateV1,
        state: _ComparisonState,
        key: tuple[str, str, str, str, str],
    ) -> BenchmarkSliceScoreV1:
        reference = state.reference_slices.get(key) or self._empty_stats()
        degraded = state.degraded_slices.get(key) or self._empty_stats()
        member_stats = [
            member.slices.get(key) or self._empty_stats()
            for member in state.member_states.values()
        ]
        candidate_metrics = [
            _stream_loss_metrics(reference, item) for item in member_stats
        ]
        degraded_metrics = _stream_loss_metrics(reference, degraded)
        averaged = {
            name: sum(item[name] for item in candidate_metrics)
            / len(candidate_metrics)
            for name in candidate_metrics[0]
        }
        soft_names = (
            "event_count_relative_error",
            "intensity_relative_error",
            "interarrival_hist_l1",
            "burst_rate_absolute_error",
            "burst_duration_relative_error",
            "quiet_rate_absolute_error",
            "quiet_duration_relative_error",
            "bid_mean_relative_error",
            "ask_mean_relative_error",
            "spread_mean_relative_error",
            "spread_hist_l1",
            "bid_transition_relative_error",
            "ask_transition_relative_error",
            "mid_transition_relative_error",
            "spread_transition_relative_error",
            "mid_range_relative_error",
            "endpoint_relative_error",
        )
        soft_loss = sum(averaged[name] for name in soft_names) / len(soft_names)
        degraded_loss = sum(
            degraded_metrics[name] for name in soft_names
        ) / len(soft_names)
        metrics = {
            **averaged,
            "soft_loss": soft_loss,
            "degraded_soft_loss": degraded_loss,
            "restoration_gain_vs_degraded": degraded_loss - soft_loss,
        }
        diversity = state.diversity_by_slice.get(key, _OnlineMetric())
        support: dict[str, JSONValue] = {
            "stratification_dimensions": [
                "symbol",
                "epoch_id",
                "session",
                "event_state",
                "sparsity",
            ],
            "reference_interarrival_count": reference.interarrival_count,
            "degraded_interarrival_count": degraded.interarrival_count,
            "ensemble_member_count": len(member_stats),
            "ensemble_diversity_comparison_count": diversity.count,
            "ensemble_mean_absolute_mid_diversity": _rounded(
                diversity.mean, self.manifest.profile.rounding_digits
            ),
            "aggregate_metrics_are_advisory": True,
        }
        return BenchmarkSliceScoreV1(
            scenario_id=scenario.scenario_id,
            candidate_id=candidate.candidate_id,
            symbol=key[0],
            epoch_id=key[1],
            session=key[2],
            event_state=key[3],
            sparsity=key[4],
            reference_event_count=reference.event_count,
            degraded_event_count=degraded.event_count,
            candidate_event_count_mean=sum(
                item.event_count for item in member_stats
            )
            / len(member_stats),
            metrics={
                name: _rounded(value, self.manifest.profile.rounding_digits)
                for name, value in metrics.items()
            },
            support=support,
        )

    def _empty_stats(self) -> _StreamStats:
        profile = self.manifest.profile
        return _StreamStats(
            profile.interarrival_buckets_ns,
            profile.spread_buckets,
            profile.burst_threshold_ns,
            profile.quiet_threshold_ns,
        )


def _stream_loss_metrics(
    reference: _StreamStats, candidate: _StreamStats
) -> dict[str, float]:
    return {
        "event_count_relative_error": _relative_error(
            float(reference.event_count), float(candidate.event_count)
        ),
        "intensity_relative_error": _relative_error(
            reference.intensity_per_second, candidate.intensity_per_second
        ),
        "interarrival_hist_l1": _histogram_l1(
            reference.interarrival_histogram,
            candidate.interarrival_histogram,
        ),
        "burst_rate_absolute_error": abs(
            reference.burst_rate - candidate.burst_rate
        ),
        "burst_duration_relative_error": _relative_error(
            reference.burst_duration_mean_ns,
            candidate.burst_duration_mean_ns,
        ),
        "quiet_rate_absolute_error": abs(
            reference.quiet_rate - candidate.quiet_rate
        ),
        "quiet_duration_relative_error": _relative_error(
            reference.quiet_duration_mean_ns,
            candidate.quiet_duration_mean_ns,
        ),
        "bid_mean_relative_error": _relative_error(
            reference.bid_mean, candidate.bid_mean
        ),
        "ask_mean_relative_error": _relative_error(
            reference.ask_mean, candidate.ask_mean
        ),
        "spread_mean_relative_error": _relative_error(
            reference.spread_mean, candidate.spread_mean
        ),
        "spread_hist_l1": _histogram_l1(
            reference.spread_histogram, candidate.spread_histogram
        ),
        "spread_transition_relative_error": _relative_error(
            reference.spread_transition_mean,
            candidate.spread_transition_mean,
        ),
        "bid_transition_relative_error": _relative_error(
            reference.bid_transition_mean,
            candidate.bid_transition_mean,
        ),
        "ask_transition_relative_error": _relative_error(
            reference.ask_transition_mean,
            candidate.ask_transition_mean,
        ),
        "mid_transition_relative_error": _relative_error(
            reference.mid_transition_mean,
            candidate.mid_transition_mean,
        ),
        "mid_range_relative_error": _relative_error(
            reference.mid_range, candidate.mid_range
        ),
        "endpoint_relative_error": _endpoint_error(reference, candidate),
    }


def _endpoint_error(reference: _StreamStats, candidate: _StreamStats) -> float:
    if reference.last_mid is None:
        return 0.0 if candidate.last_mid is None else 1.0
    if candidate.last_mid is None:
        return 1.0
    return abs(candidate.last_mid - reference.last_mid) / max(
        abs(reference.last_mid), 1e-12
    )


def _relative_error(reference: float, candidate: float) -> float:
    if reference == 0.0:
        return 0.0 if candidate == 0.0 else 1.0
    return abs(candidate - reference) / abs(reference)


def _histogram_l1(left: Sequence[int], right: Sequence[int]) -> float:
    left_total = sum(left)
    right_total = sum(right)
    if left_total == 0:
        return 0.0 if right_total == 0 else 1.0
    if right_total == 0:
        return 1.0
    return 0.5 * sum(
        abs(a / left_total - b / right_total) for a, b in zip(left, right)
    )


def _events_by_slice(
    events: Sequence[BenchmarkEventV1],
) -> dict[tuple[str, str, str, str, str], tuple[BenchmarkEventV1, ...]]:
    grouped: dict[tuple[str, str, str, str, str], list[BenchmarkEventV1]] = {}
    for event in events:
        grouped.setdefault(event.slice_key, []).append(event)
    return {key: tuple(value) for key, value in grouped.items()}


def _linear_interpolation_control(
    events: Sequence[BenchmarkEventV1],
    *,
    interval_ns: int,
    ensemble_member_id: str,
    max_events: int,
) -> tuple[BenchmarkEventV1, ...]:
    if len(events) < 2:
        return tuple(events)
    output: list[BenchmarkEventV1] = []
    for left, right in zip(events, events[1:]):
        if not output:
            output.append(_with_ensemble_member(left, ensemble_member_id))
        if left.symbol != right.symbol or left.slice_key != right.slice_key:
            output.append(_with_ensemble_member(right, ensemble_member_id))
            continue
        cursor = ((left.event_time_ns // interval_ns) + 1) * interval_ns
        while cursor < right.event_time_ns:
            fraction = (cursor - left.event_time_ns) / (
                right.event_time_ns - left.event_time_ns
            )
            bid = left.bid + fraction * (right.bid - left.bid)
            ask = left.ask + fraction * (right.ask - left.ask)
            output.append(
                BenchmarkEventV1(
                    source_event_id=(
                        "linear-interpolation:"
                        f"{left.source_event_id}:"
                        f"{right.source_event_id}:{cursor}"
                    ),
                    symbol=left.symbol,
                    event_time_ns=cursor,
                    event_sequence=left.event_sequence,
                    bid=bid,
                    ask=ask,
                    epoch_id=left.epoch_id,
                    session=left.session,
                    event_state=left.event_state,
                    sparsity=left.sparsity,
                    ensemble_member_id=ensemble_member_id,
                )
            )
            if len(output) > max_events:
                raise ValueError("linear interpolation exceeds event limit")
            cursor += interval_ns
        output.append(_with_ensemble_member(right, ensemble_member_id))
    return tuple(
        sorted(
            {item.benchmark_event_id: item for item in output}.values(),
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
    )


def _resample_last_control(
    events: Sequence[BenchmarkEventV1],
    *,
    interval_ns: int,
    ensemble_member_id: str,
) -> tuple[BenchmarkEventV1, ...]:
    buckets: dict[
        tuple[tuple[str, str, str, str, str], int], BenchmarkEventV1
    ] = {}
    for event in events:
        key = (event.slice_key, event.event_time_ns // interval_ns)
        current = buckets.get(key)
        if current is None or (
            event.event_time_ns,
            event.event_sequence,
            event.benchmark_event_id,
        ) > (
            current.event_time_ns,
            current.event_sequence,
            current.benchmark_event_id,
        ):
            buckets[key] = event
    return tuple(
        _with_ensemble_member(item, ensemble_member_id)
        for item in sorted(
            buckets.values(),
            key=lambda value: (
                value.event_time_ns,
                value.event_sequence,
                value.benchmark_event_id,
            ),
        )
    )


def _with_ensemble_member(
    event: BenchmarkEventV1, ensemble_member_id: str
) -> BenchmarkEventV1:
    member = _required_text(ensemble_member_id)
    if event.ensemble_member_id == member:
        return event
    return BenchmarkEventV1(
        source_event_id=event.source_event_id,
        symbol=event.symbol,
        event_time_ns=event.event_time_ns,
        event_sequence=event.event_sequence,
        bid=event.bid,
        ask=event.ask,
        epoch_id=event.epoch_id,
        session=event.session,
        event_state=event.event_state,
        sparsity=event.sparsity,
        ensemble_member_id=member,
        anchor_id=event.anchor_id,
        support_lower_mid=event.support_lower_mid,
        support_upper_mid=event.support_upper_mid,
    )


def _validated_events(
    values: Sequence[BenchmarkEventV1],
) -> tuple[BenchmarkEventV1, ...]:
    events = tuple(values)
    if any(not isinstance(item, BenchmarkEventV1) for item in events):
        raise ValueError("events must use the benchmark v1 contract")
    return tuple(
        sorted(
            events,
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
    )


def _parameter_positive_int(
    parameters: Mapping[str, JSONValue], name: str
) -> int:
    if name not in parameters:
        raise ValueError(f"control parameters require {name}")
    return _positive_int(parameters[name], name)


def _bucket_index(value: int | float, boundaries: Sequence[int | float]) -> int:
    for index, boundary in enumerate(boundaries):
        if value <= boundary:
            return index
    return len(boundaries)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _required_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("required text value must be a string")
    selected = value.strip()
    if not selected or len(selected) > 512:
        raise ValueError("required text value is empty or too long")
    return selected


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text value must be a string")
    selected = value.strip()
    if not selected:
        return None
    return _required_text(selected)


def _normalized_symbol(value: Any) -> str:
    return _required_text(value).upper()


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a bool")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected < 0:
        raise ValueError(f"{name} must be non-negative")
    return selected


def _positive_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected <= 0:
        raise ValueError(f"{name} must be positive")
    return selected


def _bounded_int64(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if not INT64_MIN <= selected <= INT64_MAX:
        raise ValueError(f"{name} is outside signed 64-bit bounds")
    return selected


def _bounded_positive(value: Any, maximum: int, name: str) -> int:
    selected = _positive_int(value, name)
    if selected > maximum:
        raise ValueError(f"{name} exceeds the v1 maximum")
    return selected


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    selected = float(value)
    if not math.isfinite(selected):
        raise ValueError(f"{name} must be finite")
    return selected


def _nonnegative_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected < 0:
        raise ValueError(f"{name} must be non-negative")
    return selected


def _positive_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected <= 0:
        raise ValueError(f"{name} must be positive")
    return selected


def _optional_float(value: Any) -> float | None:
    return None if value is None else _finite_float(value, "optional float")


def _strictly_increasing_positive_ints(
    values: Iterable[Any], label: str
) -> tuple[int, ...]:
    selected = tuple(_positive_int(value, label) for value in values)
    if not selected or any(
        left >= right for left, right in zip(selected, selected[1:])
    ):
        raise ValueError(f"{label}s must be strictly increasing")
    return selected


def _strictly_increasing_positive_floats(
    values: Iterable[Any], label: str
) -> tuple[float, ...]:
    selected = tuple(_positive_float(value, label) for value in values)
    if not selected or any(
        left >= right for left, right in zip(selected, selected[1:])
    ):
        raise ValueError(f"{label}s must be strictly increasing")
    return selected


def _normalized_text_tuple(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _rounded(value: float | None, digits: int) -> float:
    if value is None:
        return 0.0
    selected = round(_finite_float(value, "rounded value"), digits)
    return 0.0 if selected == 0 else selected


def _bounded_count_mapping(
    values: Mapping[str, Any], label: str, maximum: int
) -> dict[str, int]:
    if len(values) > maximum:
        raise ValueError(f"{label} mapping exceeds limit")
    result: dict[str, int] = {}
    for name, value in sorted(values.items()):
        result[_required_text(name)] = _nonnegative_int(value, label)
    return result


def _bounded_metric_mapping(
    values: Mapping[str, Any], label: str, maximum: int
) -> dict[str, float]:
    if len(values) > maximum:
        raise ValueError(f"{label} mapping exceeds limit")
    result: dict[str, float] = {}
    for name, value in sorted(values.items()):
        result[_required_text(name)] = _finite_float(value, label)
    return result


def _bounded_mapping(
    values: Mapping[str, Any], label: str, *, max_items: int
) -> dict[str, JSONValue]:
    if not isinstance(values, Mapping):
        raise ValueError(f"{label} must be a mapping")
    if len(values) > max_items:
        raise ValueError(f"{label} exceeds item limit")
    result: dict[str, JSONValue] = {}
    for name, value in sorted(values.items()):
        key = _required_text(name)
        _validate_json_value(value, f"{label}.{key}")
        result[key] = cast(JSONValue, value)
    if len(canonical_contract_json(result).encode("utf-8")) > 65_536:
        raise ValueError(f"{label} exceeds byte limit")
    return result


def _validate_json_value(value: Any, path: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for name, item in value.items():
            if not isinstance(name, str):
                raise ValueError(f"{path} contains a non-string key")
            _validate_json_value(item, f"{path}.{name}")
        return
    raise ValueError(f"{path} is not JSON-compatible")


def _ensure_payload_size(value: Mapping[str, JSONValue], maximum: int) -> None:
    size = len(canonical_contract_json(value).encode("utf-8"))
    if size > maximum:
        raise ValueError("benchmark payload exceeds configured byte limit")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError("unsupported benchmark schema version")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("expected a sequence")
    return value


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item) for item in _sequence(value))


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)
