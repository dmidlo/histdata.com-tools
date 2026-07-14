"""Calibrated reconstruction-ensemble contracts and deterministic reporting.

The ensemble layer represents missing-tick uncertainty without treating one
generated path as historical truth.  It plans stable members, calibrates
bounded metric intervals against reverse-degradation validation/final-holdout
windows, diagnoses collapsed or false diversity, selects a representative
primary member, enforces the existing reconstruction storage budget, and
hash-gates deterministic on-demand regeneration.

Dense event rows remain process-local.  Every durable object in this module is
bounded metadata derived from canonical hashes and aggregate evidence.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
import hashlib
from itertools import combinations
import json
import math
from typing import Any, cast

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.benchmark import (
    MAX_BENCHMARK_ENSEMBLE_MEMBERS,
    BenchmarkCandidateWindowV1,
    BenchmarkEventV1,
    BenchmarkScenarioV1,
    BenchmarkSplitKind,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    canonical_contract_json,
)
from histdatacom.synthetic.streaming import (
    ReconstructionResourceEstimateV1,
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
)

ENSEMBLE_CONFIG_SCHEMA_VERSION = "histdatacom.reconstruction-ensemble-config.v1"
ENSEMBLE_ARTIFACT_DIGEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-artifact-digest.v1"
)
ENSEMBLE_MEMBER_PLAN_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-member-plan.v1"
)
ENSEMBLE_PLAN_SCHEMA_VERSION = "histdatacom.reconstruction-ensemble-plan.v1"
ENSEMBLE_STORAGE_ESTIMATE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-storage-estimate.v1"
)
ENSEMBLE_STRATUM_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-stratum.v1"
)
ENSEMBLE_MEMBER_CALIBRATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-member-calibration.v1"
)
ENSEMBLE_CALIBRATION_SAMPLE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-calibration-sample.v1"
)
ENSEMBLE_METRIC_CALIBRATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-metric-calibration.v1"
)
ENSEMBLE_DIVERSITY_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-diversity-summary.v1"
)
ENSEMBLE_OUTCOME_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-outcome-summary.v1"
)
ENSEMBLE_MEMBER_SELECTION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-member-selection.v1"
)
ENSEMBLE_CALIBRATION_REPORT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-calibration-report.v1"
)
ENSEMBLE_REGENERATION_REQUEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-regeneration-request.v1"
)

ENSEMBLE_ENGINE_ID = "histdatacom.reconstruction-ensemble-calibration"
ENSEMBLE_ENGINE_VERSION = "1.0.0"
ENSEMBLE_CONFIDENCE_QUANTITY = "finite-sample-interval-coverage-v1"
ENSEMBLE_PRIMARY_SELECTION_BASIS = (
    "validation-medoid-distance-with-failure-penalty-v1"
)
ENSEMBLE_CALIBRATION_FIT_SPLIT = BenchmarkSplitKind.VALIDATION
ENSEMBLE_CALIBRATION_EVALUATION_SPLIT = BenchmarkSplitKind.FINAL_HOLDOUT

ENSEMBLE_CALIBRATION_METRIC_GROUPS: Mapping[str, str] = {
    "event_count": "count",
    "observed_duration_ns": "duration",
    "mean_interarrival_ns": "duration",
    "mean_spread": "spread",
    "mid_path_range": "path",
    "endpoint_mid": "path",
    "downstream_sensitivity": "downstream_sensitivity",
}
ENSEMBLE_CALIBRATION_METRIC_NAMES = tuple(ENSEMBLE_CALIBRATION_METRIC_GROUPS)

DEFAULT_ENSEMBLE_HORIZONS_NS = (
    60 * 1_000_000_000,
    5 * 60 * 1_000_000_000,
    60 * 60 * 1_000_000_000,
)
DEFAULT_ENSEMBLE_MAX_SAMPLES = 4096
DEFAULT_ENSEMBLE_MAX_SLICES = 4096
DEFAULT_ENSEMBLE_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
MAX_ENSEMBLE_SAMPLES = 65_536
MAX_ENSEMBLE_SLICES = 16_384
MAX_ENSEMBLE_PAYLOAD_BYTES = 64 * 1024 * 1024
MAX_ENSEMBLE_ARTIFACTS = 256
MAX_ENSEMBLE_REASON_CODES = 256
MAX_ENSEMBLE_TEXT = 1024
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1


class EnsembleArtifactKind(str, Enum):
    """Whether a hash binds source evidence or semantic configuration."""

    SOURCE = "source"
    CONFIGURATION = "configuration"


class EnsembleMemberStatus(str, Enum):
    """One member outcome in one reverse-degradation holdout cell."""

    COMPLETED = "completed"
    REFUSED = "refused"
    FAILED = "failed"


class EnsembleMetricStatus(str, Enum):
    """Whether one calibrated metric cell has trustworthy support."""

    CALIBRATED = "calibrated"
    INSUFFICIENT_SUPPORT = "insufficient_support"
    MISCALIBRATED = "miscalibrated"


class EnsembleDiversityStatus(str, Enum):
    """Whether logical paths provide substantive member diversity."""

    DIVERSE = "diverse"
    COLLAPSED = "collapsed"
    FALSE_DIVERSITY = "false_diversity"
    INSUFFICIENT_SUPPORT = "insufficient_support"


class EnsembleReportStatus(str, Enum):
    """Top-level calibrated-ensemble trust result."""

    CALIBRATED = "calibrated"
    INSUFFICIENT_SUPPORT = "insufficient_support"
    MISCALIBRATED = "miscalibrated"


@dataclass(frozen=True, slots=True)
class EnsembleCalibrationConfigV1:
    """Versioned ensemble size, calibration, diversity, and retention policy."""

    member_count: int = 4
    retained_member_count: int = 2
    horizons_ns: tuple[int, ...] = DEFAULT_ENSEMBLE_HORIZONS_NS
    nominal_coverage: float = 0.8
    minimum_achieved_coverage: float = 0.75
    minimum_fit_samples: int = 2
    maximum_collapse_rate: float = 0.0
    maximum_false_diversity_rate: float = 0.0
    logical_distance_tolerance: float = 1e-12
    failure_penalty: float = 1.0
    estimated_bytes_per_event: int = 512
    rounding_digits: int = 12
    max_samples: int = DEFAULT_ENSEMBLE_MAX_SAMPLES
    max_slices: int = DEFAULT_ENSEMBLE_MAX_SLICES
    max_payload_bytes: int = DEFAULT_ENSEMBLE_MAX_PAYLOAD_BYTES
    config_id: str = ""
    schema_version: str = ENSEMBLE_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_CONFIG_SCHEMA_VERSION,
            "ensemble config",
        )
        member_count = _positive_int(self.member_count, "member_count")
        if not 2 <= member_count <= MAX_BENCHMARK_ENSEMBLE_MEMBERS:
            raise ValueError(
                "ensemble member_count is outside supported limits"
            )
        retained = _positive_int(
            self.retained_member_count, "retained_member_count"
        )
        if retained > member_count:
            raise ValueError("retained members exceed ensemble member count")
        object.__setattr__(self, "member_count", member_count)
        object.__setattr__(self, "retained_member_count", retained)
        horizons = tuple(
            sorted(
                {
                    _positive_int(value, "horizon_ns")
                    for value in self.horizons_ns
                }
            )
        )
        if not horizons or any(value > INT64_MAX for value in horizons):
            raise ValueError("ensemble horizons are empty or outside int64")
        object.__setattr__(self, "horizons_ns", horizons)
        nominal = _unit_float(self.nominal_coverage, "nominal_coverage")
        minimum = _unit_float(
            self.minimum_achieved_coverage,
            "minimum_achieved_coverage",
        )
        if nominal <= 0.0 or nominal >= 1.0:
            raise ValueError(
                "nominal coverage must be strictly between zero and one"
            )
        if minimum > nominal:
            raise ValueError(
                "minimum achieved coverage exceeds nominal coverage"
            )
        object.__setattr__(self, "nominal_coverage", nominal)
        object.__setattr__(self, "minimum_achieved_coverage", minimum)
        object.__setattr__(
            self,
            "minimum_fit_samples",
            _positive_int(self.minimum_fit_samples, "minimum_fit_samples"),
        )
        for name in (
            "maximum_collapse_rate",
            "maximum_false_diversity_rate",
        ):
            object.__setattr__(
                self, name, _unit_float(getattr(self, name), name)
            )
        tolerance = _nonnegative_float(
            self.logical_distance_tolerance,
            "logical_distance_tolerance",
        )
        penalty = _nonnegative_float(self.failure_penalty, "failure_penalty")
        object.__setattr__(self, "logical_distance_tolerance", tolerance)
        object.__setattr__(self, "failure_penalty", penalty)
        object.__setattr__(
            self,
            "estimated_bytes_per_event",
            _positive_int(
                self.estimated_bytes_per_event,
                "estimated_bytes_per_event",
            ),
        )
        digits = _nonnegative_int(self.rounding_digits, "rounding_digits")
        if digits > 15:
            raise ValueError("rounding_digits exceeds stable float precision")
        object.__setattr__(self, "rounding_digits", digits)
        for name, upper in (
            ("max_samples", MAX_ENSEMBLE_SAMPLES),
            ("max_slices", MAX_ENSEMBLE_SLICES),
            ("max_payload_bytes", MAX_ENSEMBLE_PAYLOAD_BYTES),
        ):
            value = _positive_int(getattr(self, name), name)
            if value > upper:
                raise ValueError(f"{name} exceeds ensemble v1 limit")
            object.__setattr__(self, name, value)
        if self.max_slices < len(horizons):
            raise ValueError("max_slices cannot cover configured horizons")
        minimum_samples = len(horizons) * (self.minimum_fit_samples + 1)
        if self.max_samples < minimum_samples:
            raise ValueError(
                "max_samples cannot cover fit/final horizon support"
            )
        expected = _stable_id("ensemble-config", self.identity_payload())
        supplied = _optional_text(self.config_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": ENSEMBLE_ENGINE_ID,
            "engine_version": ENSEMBLE_ENGINE_VERSION,
            "member_count": self.member_count,
            "retained_member_count": self.retained_member_count,
            "horizons_ns": list(self.horizons_ns),
            "metric_names": list(ENSEMBLE_CALIBRATION_METRIC_NAMES),
            "nominal_coverage": self.nominal_coverage,
            "minimum_achieved_coverage": self.minimum_achieved_coverage,
            "minimum_fit_samples": self.minimum_fit_samples,
            "maximum_collapse_rate": self.maximum_collapse_rate,
            "maximum_false_diversity_rate": (self.maximum_false_diversity_rate),
            "logical_distance_tolerance": self.logical_distance_tolerance,
            "failure_penalty": self.failure_penalty,
            "calibration_fit_split": ENSEMBLE_CALIBRATION_FIT_SPLIT.value,
            "calibration_evaluation_split": (
                ENSEMBLE_CALIBRATION_EVALUATION_SPLIT.value
            ),
            "confidence_quantity": ENSEMBLE_CONFIDENCE_QUANTITY,
            "primary_selection_basis": ENSEMBLE_PRIMARY_SELECTION_BASIS,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "estimated_bytes_per_event": self.estimated_bytes_per_event,
            "rounding_digits": self.rounding_digits,
            "max_samples": self.max_samples,
            "max_slices": self.max_slices,
            "max_payload_bytes": self.max_payload_bytes,
            "config_id": self.config_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnsembleCalibrationConfigV1":
        _require_schema(data, ENSEMBLE_CONFIG_SCHEMA_VERSION)
        _require_derived_value(
            data,
            "metric_names",
            list(ENSEMBLE_CALIBRATION_METRIC_NAMES),
        )
        _require_derived_value(
            data,
            "calibration_fit_split",
            ENSEMBLE_CALIBRATION_FIT_SPLIT.value,
        )
        _require_derived_value(
            data,
            "calibration_evaluation_split",
            ENSEMBLE_CALIBRATION_EVALUATION_SPLIT.value,
        )
        _require_derived_value(
            data,
            "confidence_quantity",
            ENSEMBLE_CONFIDENCE_QUANTITY,
        )
        _require_derived_value(
            data,
            "primary_selection_basis",
            ENSEMBLE_PRIMARY_SELECTION_BASIS,
        )
        return cls(
            member_count=_strict_int(data.get("member_count"), "member_count"),
            retained_member_count=_strict_int(
                data.get("retained_member_count"), "retained_member_count"
            ),
            horizons_ns=_int_tuple(data.get("horizons_ns"), "horizons_ns"),
            nominal_coverage=_finite_float(
                data.get("nominal_coverage"), "nominal_coverage"
            ),
            minimum_achieved_coverage=_finite_float(
                data.get("minimum_achieved_coverage"),
                "minimum_achieved_coverage",
            ),
            minimum_fit_samples=_strict_int(
                data.get("minimum_fit_samples"), "minimum_fit_samples"
            ),
            maximum_collapse_rate=_finite_float(
                data.get("maximum_collapse_rate"),
                "maximum_collapse_rate",
            ),
            maximum_false_diversity_rate=_finite_float(
                data.get("maximum_false_diversity_rate"),
                "maximum_false_diversity_rate",
            ),
            logical_distance_tolerance=_finite_float(
                data.get("logical_distance_tolerance"),
                "logical_distance_tolerance",
            ),
            failure_penalty=_finite_float(
                data.get("failure_penalty"), "failure_penalty"
            ),
            estimated_bytes_per_event=_strict_int(
                data.get("estimated_bytes_per_event"),
                "estimated_bytes_per_event",
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            max_samples=_strict_int(data.get("max_samples"), "max_samples"),
            max_slices=_strict_int(data.get("max_slices"), "max_slices"),
            max_payload_bytes=_strict_int(
                data.get("max_payload_bytes"), "max_payload_bytes"
            ),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EnsembleCalibrationConfigV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class EnsembleArtifactDigestV1:
    """One content hash required to reproduce an ensemble member."""

    artifact_id: str
    sha256: str
    kind: EnsembleArtifactKind
    digest_id: str = ""
    schema_version: str = ENSEMBLE_ARTIFACT_DIGEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_ARTIFACT_DIGEST_SCHEMA_VERSION,
            "ensemble artifact digest",
        )
        object.__setattr__(
            self, "artifact_id", _required_text(self.artifact_id)
        )
        object.__setattr__(self, "sha256", _required_sha256(self.sha256))
        object.__setattr__(self, "kind", EnsembleArtifactKind(self.kind))
        expected = _stable_id("ensemble-artifact", self.identity_payload())
        supplied = _optional_text(self.digest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble artifact digest_id differs")
        object.__setattr__(self, "digest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "sha256": self.sha256,
            "kind": self.kind.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "digest_id": self.digest_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnsembleArtifactDigestV1":
        _require_schema(data, ENSEMBLE_ARTIFACT_DIGEST_SCHEMA_VERSION)
        return cls(
            artifact_id=str(data.get("artifact_id", "")),
            sha256=str(data.get("sha256", "")),
            kind=EnsembleArtifactKind(str(data.get("kind", ""))),
            digest_id=str(data.get("digest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EnsembleMemberPlanV1:
    """One stable ensemble-member identity and semantic seed."""

    ordinal: int
    member_id: str
    seed: int
    schema_version: str = ENSEMBLE_MEMBER_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_MEMBER_PLAN_SCHEMA_VERSION,
            "ensemble member plan",
        )
        object.__setattr__(
            self, "ordinal", _positive_int(self.ordinal, "ordinal")
        )
        object.__setattr__(self, "member_id", _required_text(self.member_id))
        seed = _nonnegative_int(self.seed, "seed")
        if seed > UINT64_MAX:
            raise ValueError("ensemble member seed exceeds uint64")
        object.__setattr__(self, "seed", seed)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "ordinal": self.ordinal,
            "member_id": self.member_id,
            "seed": self.seed,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnsembleMemberPlanV1":
        _require_schema(data, ENSEMBLE_MEMBER_PLAN_SCHEMA_VERSION)
        return cls(
            ordinal=_strict_int(data.get("ordinal"), "ordinal"),
            member_id=str(data.get("member_id", "")),
            seed=_strict_int(data.get("seed"), "seed"),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionEnsemblePlanV1:
    """Deterministic run/member plan bound to exact source/config hashes."""

    run: ReconstructionRunV1
    config: EnsembleCalibrationConfigV1
    source_artifacts: tuple[EnsembleArtifactDigestV1, ...]
    configuration_artifacts: tuple[EnsembleArtifactDigestV1, ...]
    members: tuple[EnsembleMemberPlanV1, ...]
    plan_id: str = ""
    schema_version: str = ENSEMBLE_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_PLAN_SCHEMA_VERSION,
            "ensemble plan",
        )
        if not isinstance(self.run, ReconstructionRunV1):
            raise ValueError("ensemble plan requires a reconstruction run")
        if not isinstance(self.config, EnsembleCalibrationConfigV1):
            raise ValueError("ensemble plan requires a v1 config")
        sources = _normalized_artifacts(
            self.source_artifacts, EnsembleArtifactKind.SOURCE
        )
        configs = _normalized_artifacts(
            self.configuration_artifacts,
            EnsembleArtifactKind.CONFIGURATION,
        )
        if not sources or not configs:
            raise ValueError("ensemble plan requires source and config hashes")
        if (
            tuple(item.artifact_id for item in sources)
            != self.run.source_version_ids
        ):
            raise ValueError("ensemble source hashes differ from run sources")
        if (
            tuple(item.artifact_id for item in configs)
            != self.run.configuration_ids
        ):
            raise ValueError("ensemble config hashes differ from run configs")
        expected_config_hash = _content_sha256(self.config.to_dict())
        config_digest = next(
            (
                item
                for item in configs
                if item.artifact_id == self.config.config_id
            ),
            None,
        )
        if (
            config_digest is None
            or config_digest.sha256 != expected_config_hash
        ):
            raise ValueError("ensemble config artifact hash differs")
        object.__setattr__(self, "source_artifacts", sources)
        object.__setattr__(self, "configuration_artifacts", configs)
        members = tuple(sorted(self.members, key=lambda item: item.ordinal))
        if tuple(item.ordinal for item in members) != tuple(
            range(1, self.config.member_count + 1)
        ):
            raise ValueError("ensemble member ordinals are incomplete")
        expected_ids = tuple(
            _derive_member_id(
                self.config,
                sources,
                configs,
                self.run.base_seed,
                ordinal,
            )
            for ordinal in range(1, self.config.member_count + 1)
        )
        if tuple(item.member_id for item in members) != expected_ids:
            raise ValueError(
                "ensemble member IDs are not deterministically derived"
            )
        if set(expected_ids) != set(self.run.ensemble_member_ids):
            raise ValueError("ensemble members differ from reconstruction run")
        for member in members:
            expected_seed = self.run.seed_for(
                member.member_id,
                f"{ENSEMBLE_ENGINE_ID}:{self.config.config_id}:member",
            )
            if member.seed != expected_seed:
                raise ValueError("ensemble member seed differs")
        object.__setattr__(self, "members", members)
        expected = _stable_id("ensemble-plan", self.identity_payload())
        supplied = _optional_text(self.plan_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble plan_id differs")
        object.__setattr__(self, "plan_id", expected)

    @property
    def source_hashes(self) -> dict[str, str]:
        return {item.artifact_id: item.sha256 for item in self.source_artifacts}

    @property
    def configuration_hashes(self) -> dict[str, str]:
        return {
            item.artifact_id: item.sha256
            for item in self.configuration_artifacts
        }

    def member(self, member_id: str) -> EnsembleMemberPlanV1:
        wanted = _required_text(member_id)
        for member in self.members:
            if member.member_id == wanted:
                return member
        raise KeyError(wanted)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": ENSEMBLE_ENGINE_ID,
            "engine_version": ENSEMBLE_ENGINE_VERSION,
            "run": self.run.to_dict(),
            "config": self.config.to_dict(),
            "source_artifacts": [
                item.to_dict() for item in self.source_artifacts
            ],
            "configuration_artifacts": [
                item.to_dict() for item in self.configuration_artifacts
            ],
            "members": [item.to_dict() for item in self.members],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "plan_id": self.plan_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionEnsemblePlanV1":
        _require_schema(data, ENSEMBLE_PLAN_SCHEMA_VERSION)
        return cls(
            run=ReconstructionRunV1.from_dict(_mapping(data.get("run"))),
            config=EnsembleCalibrationConfigV1.from_dict(
                _mapping(data.get("config"))
            ),
            source_artifacts=tuple(
                EnsembleArtifactDigestV1.from_dict(item)
                for item in _mapping_sequence(data, "source_artifacts")
            ),
            configuration_artifacts=tuple(
                EnsembleArtifactDigestV1.from_dict(item)
                for item in _mapping_sequence(data, "configuration_artifacts")
            ),
            members=tuple(
                EnsembleMemberPlanV1.from_dict(item)
                for item in _mapping_sequence(data, "members")
            ),
            plan_id=str(data.get("plan_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionEnsemblePlanV1":
        return cls.from_dict(_json_mapping(text))


def plan_reconstruction_ensemble(
    *,
    symbols: Iterable[str],
    source_artifact_hashes: Mapping[str, str],
    configuration_artifact_hashes: Mapping[str, str],
    base_seed: int,
    config: EnsembleCalibrationConfigV1 | None = None,
    storage_policy: ReconstructionStoragePolicyV1 | None = None,
) -> ReconstructionEnsemblePlanV1:
    """Build stable member IDs and seeds without worker-count inputs."""
    selected = config or EnsembleCalibrationConfigV1()
    sources = _artifact_digests(
        source_artifact_hashes, EnsembleArtifactKind.SOURCE
    )
    config_hashes = dict(configuration_artifact_hashes)
    expected_config_hash = _content_sha256(selected.to_dict())
    supplied = config_hashes.get(selected.config_id)
    if (
        supplied is not None
        and _required_sha256(supplied) != expected_config_hash
    ):
        raise ValueError("supplied ensemble config hash differs")
    config_hashes[selected.config_id] = expected_config_hash
    configurations = _artifact_digests(
        config_hashes, EnsembleArtifactKind.CONFIGURATION
    )
    member_ids = tuple(
        _derive_member_id(
            selected,
            sources,
            configurations,
            base_seed,
            ordinal,
        )
        for ordinal in range(1, selected.member_count + 1)
    )
    run = ReconstructionRunV1(
        symbols=tuple(symbols),
        source_version_ids=tuple(item.artifact_id for item in sources),
        configuration_ids=tuple(item.artifact_id for item in configurations),
        ensemble_member_ids=member_ids,
        base_seed=base_seed,
        storage_policy=storage_policy or ReconstructionStoragePolicyV1(),
    )
    members = tuple(
        EnsembleMemberPlanV1(
            ordinal=ordinal,
            member_id=member_id,
            seed=run.seed_for(
                member_id,
                f"{ENSEMBLE_ENGINE_ID}:{selected.config_id}:member",
            ),
        )
        for ordinal, member_id in enumerate(member_ids, start=1)
    )
    return ReconstructionEnsemblePlanV1(
        run=run,
        config=selected,
        source_artifacts=sources,
        configuration_artifacts=configurations,
        members=members,
    )


@dataclass(frozen=True, slots=True)
class EnsembleStorageEstimateV1:
    """Conservative all-member estimate checked by the #432 storage policy."""

    run_id: str
    plan_id: str
    member_event_counts: Mapping[str, int]
    retained_member_count: int
    conservative_retained_event_count: int
    estimated_bytes_per_event: int
    resource_estimate: ReconstructionResourceEstimateV1
    estimate_id: str = ""
    schema_version: str = ENSEMBLE_STORAGE_ESTIMATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_STORAGE_ESTIMATE_SCHEMA_VERSION,
            "ensemble storage estimate",
        )
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(self, "plan_id", _required_text(self.plan_id))
        counts = _count_mapping(self.member_event_counts, "member event")
        if not counts:
            raise ValueError("ensemble storage estimate requires members")
        object.__setattr__(self, "member_event_counts", counts)
        object.__setattr__(
            self,
            "retained_member_count",
            _positive_int(self.retained_member_count, "retained_member_count"),
        )
        object.__setattr__(
            self,
            "conservative_retained_event_count",
            _nonnegative_int(
                self.conservative_retained_event_count,
                "conservative_retained_event_count",
            ),
        )
        object.__setattr__(
            self,
            "estimated_bytes_per_event",
            _positive_int(
                self.estimated_bytes_per_event,
                "estimated_bytes_per_event",
            ),
        )
        if not isinstance(
            self.resource_estimate, ReconstructionResourceEstimateV1
        ):
            raise ValueError(
                "ensemble estimate requires a #432 resource estimate"
            )
        values = tuple(counts.values())
        if self.retained_member_count > len(values):
            raise ValueError("ensemble estimate retains too many members")
        expected_retained = sum(
            sorted(values, reverse=True)[: self.retained_member_count]
        )
        if self.conservative_retained_event_count != expected_retained:
            raise ValueError("ensemble retained-event estimate differs")
        estimate = self.resource_estimate
        expected_fields = {
            "candidate_event_count": sum(values),
            "retained_ensemble_members": self.retained_member_count,
            "peak_events_per_batch": max(values, default=0),
            "estimated_memory_bytes": (
                max(values, default=0) * self.estimated_bytes_per_event
            ),
            "estimated_scratch_bytes": (
                sum(values) * self.estimated_bytes_per_event
            ),
            "estimated_output_bytes": (
                expected_retained * self.estimated_bytes_per_event
            ),
        }
        if any(
            getattr(estimate, name) != expected
            for name, expected in expected_fields.items()
        ):
            raise ValueError("ensemble resource estimate arithmetic differs")
        expected = _stable_id("ensemble-storage-estimate", self.payload())
        supplied = _optional_text(self.estimate_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble storage estimate_id differs")
        object.__setattr__(self, "estimate_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "plan_id": self.plan_id,
            "member_event_counts": dict(self.member_event_counts),
            "retained_member_count": self.retained_member_count,
            "conservative_retained_event_count": (
                self.conservative_retained_event_count
            ),
            "estimated_bytes_per_event": self.estimated_bytes_per_event,
            "resource_estimate": self.resource_estimate.to_dict(),
            "retention_estimate_basis": "largest-member-counts-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "estimate_id": self.estimate_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnsembleStorageEstimateV1":
        _require_schema(data, ENSEMBLE_STORAGE_ESTIMATE_SCHEMA_VERSION)
        _require_derived_value(
            data,
            "retention_estimate_basis",
            "largest-member-counts-v1",
        )
        return cls(
            run_id=str(data.get("run_id", "")),
            plan_id=str(data.get("plan_id", "")),
            member_event_counts={
                str(key): _strict_int(value, "member event count")
                for key, value in _mapping(
                    data.get("member_event_counts")
                ).items()
            },
            retained_member_count=_strict_int(
                data.get("retained_member_count"), "retained_member_count"
            ),
            conservative_retained_event_count=_strict_int(
                data.get("conservative_retained_event_count"),
                "conservative_retained_event_count",
            ),
            estimated_bytes_per_event=_strict_int(
                data.get("estimated_bytes_per_event"),
                "estimated_bytes_per_event",
            ),
            resource_estimate=ReconstructionResourceEstimateV1.from_dict(
                _mapping(data.get("resource_estimate"))
            ),
            estimate_id=str(data.get("estimate_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EnsembleStorageEstimateV1":
        return cls.from_dict(_json_mapping(text))


def estimate_reconstruction_ensemble_resources(
    plan: ReconstructionEnsemblePlanV1,
    *,
    input_event_count: int,
    member_event_counts: Mapping[str, int],
) -> EnsembleStorageEstimateV1:
    """Conservatively estimate all computation and retained-member output."""
    if not isinstance(plan, ReconstructionEnsemblePlanV1):
        raise ValueError("ensemble resource estimate requires a v1 plan")
    input_count = _nonnegative_int(input_event_count, "input_event_count")
    counts = _count_mapping(member_event_counts, "member event")
    expected_members = {item.member_id for item in plan.members}
    if set(counts) != expected_members:
        raise ValueError("ensemble event counts do not cover planned members")
    values = tuple(counts.values())
    retained_count = plan.config.retained_member_count
    retained_events = sum(sorted(values, reverse=True)[:retained_count])
    bytes_per_event = plan.config.estimated_bytes_per_event
    maximum = max(values, default=0)
    batch_limit = plan.run.storage_policy.max_events_per_batch
    estimate = ReconstructionResourceEstimateV1(
        input_event_count=input_count,
        candidate_event_count=sum(values),
        retained_ensemble_members=retained_count,
        inflight_batches=1 if any(values) else 0,
        peak_events_per_batch=maximum,
        estimated_memory_bytes=maximum * bytes_per_event,
        estimated_scratch_bytes=sum(values) * bytes_per_event,
        estimated_output_bytes=retained_events * bytes_per_event,
        estimated_batch_count=sum(
            math.ceil(value / batch_limit) for value in values if value
        ),
    )
    plan.run.storage_policy.preflight(estimate)
    return EnsembleStorageEstimateV1(
        run_id=plan.run.run_id,
        plan_id=plan.plan_id,
        member_event_counts=counts,
        retained_member_count=retained_count,
        conservative_retained_event_count=retained_events,
        estimated_bytes_per_event=bytes_per_event,
        resource_estimate=estimate,
    )


@dataclass(frozen=True, slots=True)
class EnsembleCalibrationStratumV1:
    """Exact epoch/session/event/symbol/horizon/sparsity calibration cell."""

    epoch_id: str
    session: str
    event_state: str
    symbol: str
    horizon_ns: int
    sparsity: str
    stratum_id: str = ""
    schema_version: str = ENSEMBLE_STRATUM_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_STRATUM_SCHEMA_VERSION,
            "ensemble stratum",
        )
        for name in ("epoch_id", "session", "event_state", "sparsity"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        horizon = _positive_int(self.horizon_ns, "horizon_ns")
        if horizon > INT64_MAX:
            raise ValueError("ensemble horizon exceeds int64")
        object.__setattr__(self, "horizon_ns", horizon)
        expected = _stable_id("ensemble-stratum", self.identity_payload())
        supplied = _optional_text(self.stratum_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble stratum_id differs")
        object.__setattr__(self, "stratum_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "epoch_id": self.epoch_id,
            "session": self.session,
            "event_state": self.event_state,
            "symbol": self.symbol,
            "horizon_ns": self.horizon_ns,
            "sparsity": self.sparsity,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "stratum_id": self.stratum_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnsembleCalibrationStratumV1":
        _require_schema(data, ENSEMBLE_STRATUM_SCHEMA_VERSION)
        return cls(
            epoch_id=str(data.get("epoch_id", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            symbol=str(data.get("symbol", "")),
            horizon_ns=_strict_int(data.get("horizon_ns"), "horizon_ns"),
            sparsity=str(data.get("sparsity", "")),
            stratum_id=str(data.get("stratum_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EnsembleMemberCalibrationV1:
    """One member's compact result in a calibration sample."""

    member_id: str
    status: EnsembleMemberStatus
    metrics: Mapping[str, float] = field(default_factory=dict)
    logical_content_sha256: str | None = None
    reason: str | None = None
    result_id: str = ""
    schema_version: str = ENSEMBLE_MEMBER_CALIBRATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_MEMBER_CALIBRATION_SCHEMA_VERSION,
            "ensemble member calibration",
        )
        object.__setattr__(self, "member_id", _required_text(self.member_id))
        status = EnsembleMemberStatus(self.status)
        object.__setattr__(self, "status", status)
        metrics = _metric_mapping(self.metrics, allow_empty=True)
        digest = (
            _required_sha256(self.logical_content_sha256)
            if self.logical_content_sha256 is not None
            else None
        )
        reason = _optional_bounded_text(self.reason, "reason")
        if status is EnsembleMemberStatus.COMPLETED:
            if set(metrics) != set(ENSEMBLE_CALIBRATION_METRIC_NAMES):
                raise ValueError("completed member metrics are incomplete")
            if digest is None or reason is not None:
                raise ValueError(
                    "completed member hash/reason state is invalid"
                )
        elif metrics or digest is not None or reason is None:
            raise ValueError("failed/refused member must retain only a reason")
        object.__setattr__(self, "metrics", metrics)
        object.__setattr__(self, "logical_content_sha256", digest)
        object.__setattr__(self, "reason", reason)
        expected = _stable_id("ensemble-member-result", self.payload())
        supplied = _optional_text(self.result_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble member result_id differs")
        object.__setattr__(self, "result_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "member_id": self.member_id,
            "status": self.status.value,
            "metrics": dict(self.metrics),
            "logical_content_sha256": self.logical_content_sha256,
            "reason": self.reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnsembleMemberCalibrationV1":
        _require_schema(data, ENSEMBLE_MEMBER_CALIBRATION_SCHEMA_VERSION)
        return cls(
            member_id=str(data.get("member_id", "")),
            status=EnsembleMemberStatus(str(data.get("status", ""))),
            metrics=cast(Mapping[str, float], _mapping(data.get("metrics"))),
            logical_content_sha256=_optional_text(
                data.get("logical_content_sha256")
            ),
            reason=_optional_text(data.get("reason")),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EnsembleCalibrationSampleV1:
    """One reverse-degradation holdout cell with no embedded event rows."""

    benchmark_manifest_id: str
    scenario_id: str
    candidate_id: str
    window_id: str
    split_kind: BenchmarkSplitKind
    stratum: EnsembleCalibrationStratumV1
    reference_metrics: Mapping[str, float]
    reference_content_sha256: str
    members: tuple[EnsembleMemberCalibrationV1, ...]
    sample_id: str = ""
    schema_version: str = ENSEMBLE_CALIBRATION_SAMPLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_CALIBRATION_SAMPLE_SCHEMA_VERSION,
            "ensemble calibration sample",
        )
        for name in (
            "benchmark_manifest_id",
            "scenario_id",
            "candidate_id",
            "window_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        split = BenchmarkSplitKind.from_value(self.split_kind)
        if split not in {
            ENSEMBLE_CALIBRATION_FIT_SPLIT,
            ENSEMBLE_CALIBRATION_EVALUATION_SPLIT,
        }:
            raise ValueError(
                "ensemble samples require validation/final holdout"
            )
        object.__setattr__(self, "split_kind", split)
        if not isinstance(self.stratum, EnsembleCalibrationStratumV1):
            raise ValueError("ensemble sample requires a v1 stratum")
        metrics = _metric_mapping(self.reference_metrics)
        object.__setattr__(self, "reference_metrics", metrics)
        object.__setattr__(
            self,
            "reference_content_sha256",
            _required_sha256(self.reference_content_sha256),
        )
        members = tuple(sorted(self.members, key=lambda item: item.member_id))
        if not members or len({item.member_id for item in members}) != len(
            members
        ):
            raise ValueError("ensemble sample members are empty or duplicated")
        if len(members) > MAX_BENCHMARK_ENSEMBLE_MEMBERS:
            raise ValueError("ensemble sample member count exceeds limit")
        object.__setattr__(self, "members", members)
        expected = _stable_id("ensemble-calibration-sample", self.payload())
        supplied = _optional_text(self.sample_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble calibration sample_id differs")
        object.__setattr__(self, "sample_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "benchmark_manifest_id": self.benchmark_manifest_id,
            "scenario_id": self.scenario_id,
            "candidate_id": self.candidate_id,
            "window_id": self.window_id,
            "split_kind": self.split_kind.value,
            "stratum": self.stratum.to_dict(),
            "reference_metrics": dict(self.reference_metrics),
            "reference_content_sha256": self.reference_content_sha256,
            "members": [item.to_dict() for item in self.members],
            "event_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "sample_id": self.sample_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnsembleCalibrationSampleV1":
        _require_schema(data, ENSEMBLE_CALIBRATION_SAMPLE_SCHEMA_VERSION)
        _require_derived_value(data, "event_rows_inline", False)
        return cls(
            benchmark_manifest_id=str(data.get("benchmark_manifest_id", "")),
            scenario_id=str(data.get("scenario_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            window_id=str(data.get("window_id", "")),
            split_kind=BenchmarkSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            stratum=EnsembleCalibrationStratumV1.from_dict(
                _mapping(data.get("stratum"))
            ),
            reference_metrics=cast(
                Mapping[str, float], _mapping(data.get("reference_metrics"))
            ),
            reference_content_sha256=str(
                data.get("reference_content_sha256", "")
            ),
            members=tuple(
                EnsembleMemberCalibrationV1.from_dict(item)
                for item in _mapping_sequence(data, "members")
            ),
            sample_id=str(data.get("sample_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EnsembleCalibrationSampleV1":
        return cls.from_dict(_json_mapping(text))


def benchmark_ensemble_calibration_sample(
    plan: ReconstructionEnsemblePlanV1,
    *,
    benchmark_manifest_id: str,
    scenario: BenchmarkScenarioV1,
    candidate_id: str,
    window_id: str,
    horizon_ns: int,
    reference_events: Sequence[BenchmarkEventV1],
    member_windows: Sequence[BenchmarkCandidateWindowV1],
    reference_downstream_sensitivity: float,
) -> EnsembleCalibrationSampleV1:
    """Adapt one bounded reverse-degradation cell into calibration metadata."""
    if not isinstance(plan, ReconstructionEnsemblePlanV1):
        raise ValueError("benchmark ensemble sample requires a v1 plan")
    if not isinstance(scenario, BenchmarkScenarioV1):
        raise ValueError("benchmark ensemble sample requires a v1 scenario")
    if scenario.split_kind not in {
        ENSEMBLE_CALIBRATION_FIT_SPLIT,
        ENSEMBLE_CALIBRATION_EVALUATION_SPLIT,
    }:
        raise ValueError(
            "ensemble calibration requires validation/final scenario"
        )
    horizon = _positive_int(horizon_ns, "horizon_ns")
    if horizon not in plan.config.horizons_ns:
        raise ValueError("calibration horizon is absent from ensemble config")
    reference = _validated_benchmark_events(reference_events)
    if not reference:
        raise ValueError("calibration reference events cannot be empty")
    horizon_start_ns = reference[0].event_time_ns
    horizon_end_ns = horizon_start_ns + horizon
    if reference[-1].event_time_ns > horizon_end_ns:
        raise ValueError("calibration reference exceeds declared horizon")
    slice_keys = {item.slice_key for item in reference}
    if len(slice_keys) != 1:
        raise ValueError("calibration reference must occupy one exact stratum")
    symbol, epoch, session, event_state, sparsity = next(iter(slice_keys))
    if epoch != scenario.epoch_id:
        raise ValueError("calibration reference epoch differs from scenario")
    stratum = EnsembleCalibrationStratumV1(
        epoch_id=epoch,
        session=session,
        event_state=event_state,
        symbol=symbol,
        horizon_ns=horizon,
        sparsity=sparsity,
    )
    selected_candidate = _required_text(candidate_id)
    selected_window = _required_text(window_id)
    by_member: dict[str, BenchmarkCandidateWindowV1] = {}
    for candidate_window in member_windows:
        if candidate_window.ensemble_member_id in by_member:
            raise ValueError("duplicate ensemble member window")
        if (
            candidate_window.scenario_id != scenario.scenario_id
            or candidate_window.candidate_id != selected_candidate
            or candidate_window.window_id != selected_window
        ):
            raise ValueError("candidate window differs from calibration scope")
        by_member[candidate_window.ensemble_member_id] = candidate_window
    member_results: list[EnsembleMemberCalibrationV1] = []
    for member in plan.members:
        candidate_window = by_member.get(member.member_id)
        if candidate_window is None:
            member_results.append(
                EnsembleMemberCalibrationV1(
                    member_id=member.member_id,
                    status=EnsembleMemberStatus.FAILED,
                    reason="missing_member_window",
                )
            )
            continue
        execution = candidate_window.execution
        if not execution.attempted:
            member_results.append(
                EnsembleMemberCalibrationV1(
                    member_id=member.member_id,
                    status=EnsembleMemberStatus.REFUSED,
                    reason="not_attempted",
                )
            )
            continue
        if not execution.converged:
            member_results.append(
                EnsembleMemberCalibrationV1(
                    member_id=member.member_id,
                    status=EnsembleMemberStatus.FAILED,
                    reason=execution.failure_reason or "generation_failed",
                )
            )
            continue
        if candidate_window.hard_constraint_violations:
            first_reason = sorted(candidate_window.hard_constraint_violations)[
                0
            ]
            member_results.append(
                EnsembleMemberCalibrationV1(
                    member_id=member.member_id,
                    status=EnsembleMemberStatus.REFUSED,
                    reason=f"hard_constraint:{first_reason}",
                )
            )
            continue
        events = _validated_benchmark_events(candidate_window.events)
        if not events:
            member_results.append(
                EnsembleMemberCalibrationV1(
                    member_id=member.member_id,
                    status=EnsembleMemberStatus.REFUSED,
                    reason="empty_member_stream",
                )
            )
            continue
        if {item.slice_key for item in events} != slice_keys:
            raise ValueError("candidate member crosses calibration strata")
        if (
            events[0].event_time_ns < horizon_start_ns
            or events[-1].event_time_ns > horizon_end_ns
        ):
            raise ValueError("candidate member exceeds calibration horizon")
        downstream = candidate_window.strategy_hooks.get(
            "downstream_sensitivity"
        )
        if downstream is None:
            member_results.append(
                EnsembleMemberCalibrationV1(
                    member_id=member.member_id,
                    status=EnsembleMemberStatus.REFUSED,
                    reason="missing_downstream_sensitivity",
                )
            )
            continue
        member_results.append(
            EnsembleMemberCalibrationV1(
                member_id=member.member_id,
                status=EnsembleMemberStatus.COMPLETED,
                metrics=_benchmark_metrics(events, downstream),
                logical_content_sha256=benchmark_logical_content_sha256(events),
            )
        )
    unknown = set(by_member).difference(item.member_id for item in plan.members)
    if unknown:
        raise ValueError("calibration contains unplanned ensemble members")
    return EnsembleCalibrationSampleV1(
        benchmark_manifest_id=benchmark_manifest_id,
        scenario_id=scenario.scenario_id,
        candidate_id=selected_candidate,
        window_id=selected_window,
        split_kind=scenario.split_kind,
        stratum=stratum,
        reference_metrics=_benchmark_metrics(
            reference,
            reference_downstream_sensitivity,
        ),
        reference_content_sha256=benchmark_logical_content_sha256(reference),
        members=tuple(member_results),
    )


def benchmark_logical_content_sha256(
    events: Iterable[BenchmarkEventV1],
) -> str:
    """Hash market content independently of row order, IDs, seeds, and member."""
    rows = sorted(
        (
            {
                "symbol": item.symbol,
                "event_time_ns": item.event_time_ns,
                "event_sequence": item.event_sequence,
                "bid": item.bid,
                "ask": item.ask,
                "epoch_id": item.epoch_id,
                "session": item.session,
                "event_state": item.event_state,
                "sparsity": item.sparsity,
                "anchor_present": item.anchor_id is not None,
            }
            for item in events
        ),
        key=lambda row: (
            str(row["symbol"]),
            int(row["event_time_ns"]),
            int(row["event_sequence"]),
            float(row["bid"]),
            float(row["ask"]),
        ),
    )
    return _content_sha256(rows)


def ensemble_logical_content_sha256(
    streams: Iterable[SyntheticEventStreamV1],
) -> str:
    """Hash synchronized market rows without member, run, or lineage identity."""
    rows: list[dict[str, JSONValue]] = []
    for stream in streams:
        if not isinstance(stream, SyntheticEventStreamV1):
            raise ValueError("ensemble logical hash requires event streams")
        rows.extend(
            {
                "origin": event.origin.value,
                "symbol": event.symbol,
                "event_time_ns": event.event_time_ns,
                "event_sequence": event.event_sequence,
                "bid": event.bid,
                "ask": event.ask,
            }
            for event in stream.events
        )
    rows.sort(
        key=lambda row: (
            str(row["symbol"]),
            int(row["event_time_ns"]),
            int(row["event_sequence"]),
            str(row["origin"]),
            float(row["bid"]),
            float(row["ask"]),
        )
    )
    return _content_sha256(rows)


@dataclass(frozen=True, slots=True)
class EnsembleMetricCalibrationV1:
    """Compact conformal-style coverage evidence for one metric cell."""

    stratum: EnsembleCalibrationStratumV1
    metric_name: str
    metric_group: str
    nominal_coverage: float
    minimum_achieved_coverage: float
    fit_sample_count: int
    evaluation_sample_count: int
    calibration_adjustment: float | None
    raw_covered_count: int
    calibrated_covered_count: int
    raw_coverage_rate: float | None
    calibrated_coverage_rate: float | None
    mean_raw_interval_width: float | None
    mean_calibrated_interval_width: float | None
    mean_absolute_median_error: float | None
    status: EnsembleMetricStatus
    summary_id: str = ""
    schema_version: str = ENSEMBLE_METRIC_CALIBRATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_METRIC_CALIBRATION_SCHEMA_VERSION,
            "ensemble metric calibration",
        )
        if not isinstance(self.stratum, EnsembleCalibrationStratumV1):
            raise ValueError("metric calibration requires a v1 stratum")
        metric = _required_text(self.metric_name)
        expected_group = ENSEMBLE_CALIBRATION_METRIC_GROUPS.get(metric)
        if expected_group is None or self.metric_group != expected_group:
            raise ValueError("ensemble metric name/group differs")
        object.__setattr__(self, "metric_name", metric)
        object.__setattr__(self, "metric_group", expected_group)
        object.__setattr__(
            self,
            "nominal_coverage",
            _unit_float(self.nominal_coverage, "nominal_coverage"),
        )
        object.__setattr__(
            self,
            "minimum_achieved_coverage",
            _unit_float(
                self.minimum_achieved_coverage,
                "minimum_achieved_coverage",
            ),
        )
        if not 0.0 < self.nominal_coverage < 1.0:
            raise ValueError("metric nominal coverage must be inside (0,1)")
        if self.minimum_achieved_coverage > self.nominal_coverage:
            raise ValueError("metric minimum coverage exceeds nominal")
        for name in (
            "fit_sample_count",
            "evaluation_sample_count",
            "raw_covered_count",
            "calibrated_covered_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.raw_covered_count > self.evaluation_sample_count or (
            self.calibrated_covered_count > self.evaluation_sample_count
        ):
            raise ValueError(
                "ensemble coverage counts exceed evaluation support"
            )
        for name in (
            "calibration_adjustment",
            "mean_raw_interval_width",
            "mean_calibrated_interval_width",
            "mean_absolute_median_error",
        ):
            object.__setattr__(
                self,
                name,
                _optional_nonnegative_float(getattr(self, name), name),
            )
        for name in ("raw_coverage_rate", "calibrated_coverage_rate"):
            value = getattr(self, name)
            object.__setattr__(
                self,
                name,
                _unit_float(value, name) if value is not None else None,
            )
        status = EnsembleMetricStatus(self.status)
        if status is EnsembleMetricStatus.INSUFFICIENT_SUPPORT:
            if (
                any(
                    value is not None
                    for value in (
                        self.calibration_adjustment,
                        self.raw_coverage_rate,
                        self.calibrated_coverage_rate,
                        self.mean_raw_interval_width,
                        self.mean_calibrated_interval_width,
                        self.mean_absolute_median_error,
                    )
                )
                or self.evaluation_sample_count != 0
            ):
                raise ValueError("unsupported metric cannot claim evidence")
        elif (
            self.calibration_adjustment is None
            or self.raw_coverage_rate is None
            or self.calibrated_coverage_rate is None
            or self.mean_raw_interval_width is None
            or self.mean_calibrated_interval_width is None
            or self.mean_absolute_median_error is None
            or self.evaluation_sample_count == 0
        ):
            raise ValueError(
                "supported metric requires adjustment and coverage"
            )
        if self.calibrated_covered_count < self.raw_covered_count:
            raise ValueError("calibration cannot reduce covered count")
        if self.evaluation_sample_count:
            expected_raw = self.raw_covered_count / self.evaluation_sample_count
            expected_calibrated = (
                self.calibrated_covered_count / self.evaluation_sample_count
            )
            if not math.isclose(
                cast(float, self.raw_coverage_rate),
                expected_raw,
                rel_tol=0.0,
                abs_tol=1e-15,
            ) or not math.isclose(
                cast(float, self.calibrated_coverage_rate),
                expected_calibrated,
                rel_tol=0.0,
                abs_tol=1e-15,
            ):
                raise ValueError("ensemble coverage rates do not reconcile")
            if cast(float, self.mean_calibrated_interval_width) < cast(
                float, self.mean_raw_interval_width
            ):
                raise ValueError("calibration cannot narrow mean interval")
            expected_status = (
                EnsembleMetricStatus.CALIBRATED
                if expected_calibrated >= self.minimum_achieved_coverage
                else EnsembleMetricStatus.MISCALIBRATED
            )
            if status is not expected_status:
                raise ValueError("ensemble metric status differs from coverage")
        object.__setattr__(self, "status", status)
        expected = _stable_id("ensemble-metric-calibration", self.payload())
        supplied = _optional_text(self.summary_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble metric summary_id differs")
        object.__setattr__(self, "summary_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "stratum": self.stratum.to_dict(),
            "metric_name": self.metric_name,
            "metric_group": self.metric_group,
            "confidence_quantity": ENSEMBLE_CONFIDENCE_QUANTITY,
            "nominal_coverage": self.nominal_coverage,
            "minimum_achieved_coverage": self.minimum_achieved_coverage,
            "fit_sample_count": self.fit_sample_count,
            "evaluation_sample_count": self.evaluation_sample_count,
            "calibration_adjustment": self.calibration_adjustment,
            "raw_covered_count": self.raw_covered_count,
            "calibrated_covered_count": self.calibrated_covered_count,
            "raw_coverage_rate": self.raw_coverage_rate,
            "calibrated_coverage_rate": self.calibrated_coverage_rate,
            "mean_raw_interval_width": self.mean_raw_interval_width,
            "mean_calibrated_interval_width": (
                self.mean_calibrated_interval_width
            ),
            "mean_absolute_median_error": self.mean_absolute_median_error,
            "status": self.status.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "summary_id": self.summary_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnsembleMetricCalibrationV1":
        _require_schema(data, ENSEMBLE_METRIC_CALIBRATION_SCHEMA_VERSION)
        _require_derived_value(
            data, "confidence_quantity", ENSEMBLE_CONFIDENCE_QUANTITY
        )
        return cls(
            stratum=EnsembleCalibrationStratumV1.from_dict(
                _mapping(data.get("stratum"))
            ),
            metric_name=str(data.get("metric_name", "")),
            metric_group=str(data.get("metric_group", "")),
            nominal_coverage=_finite_float(
                data.get("nominal_coverage"), "nominal_coverage"
            ),
            minimum_achieved_coverage=_finite_float(
                data.get("minimum_achieved_coverage"),
                "minimum_achieved_coverage",
            ),
            fit_sample_count=_strict_int(
                data.get("fit_sample_count"), "fit_sample_count"
            ),
            evaluation_sample_count=_strict_int(
                data.get("evaluation_sample_count"),
                "evaluation_sample_count",
            ),
            calibration_adjustment=_optional_float(
                data.get("calibration_adjustment")
            ),
            raw_covered_count=_strict_int(
                data.get("raw_covered_count"), "raw_covered_count"
            ),
            calibrated_covered_count=_strict_int(
                data.get("calibrated_covered_count"),
                "calibrated_covered_count",
            ),
            raw_coverage_rate=_optional_float(data.get("raw_coverage_rate")),
            calibrated_coverage_rate=_optional_float(
                data.get("calibrated_coverage_rate")
            ),
            mean_raw_interval_width=_optional_float(
                data.get("mean_raw_interval_width")
            ),
            mean_calibrated_interval_width=_optional_float(
                data.get("mean_calibrated_interval_width")
            ),
            mean_absolute_median_error=_optional_float(
                data.get("mean_absolute_median_error")
            ),
            status=EnsembleMetricStatus(str(data.get("status", ""))),
            summary_id=str(data.get("summary_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EnsembleDiversitySummaryV1:
    """Content-aware pairwise diversity for one split and stratum."""

    split_kind: BenchmarkSplitKind
    stratum: EnsembleCalibrationStratumV1
    sample_count: int
    pair_count: int
    collapsed_pair_count: int
    false_diversity_pair_count: int
    distinct_content_count: int
    mean_normalized_metric_distance: float | None
    collapse_rate: float | None
    false_diversity_rate: float | None
    status: EnsembleDiversityStatus
    summary_id: str = ""
    schema_version: str = ENSEMBLE_DIVERSITY_SUMMARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_DIVERSITY_SUMMARY_SCHEMA_VERSION,
            "ensemble diversity summary",
        )
        object.__setattr__(
            self,
            "split_kind",
            BenchmarkSplitKind.from_value(self.split_kind),
        )
        if not isinstance(self.stratum, EnsembleCalibrationStratumV1):
            raise ValueError("diversity summary requires a v1 stratum")
        for name in (
            "sample_count",
            "pair_count",
            "collapsed_pair_count",
            "false_diversity_pair_count",
            "distinct_content_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.collapsed_pair_count + self.false_diversity_pair_count > (
            self.pair_count
        ):
            raise ValueError("diversity diagnostic counts exceed pair support")
        object.__setattr__(
            self,
            "mean_normalized_metric_distance",
            _optional_nonnegative_float(
                self.mean_normalized_metric_distance,
                "mean_normalized_metric_distance",
            ),
        )
        for name in ("collapse_rate", "false_diversity_rate"):
            value = getattr(self, name)
            object.__setattr__(
                self,
                name,
                _unit_float(value, name) if value is not None else None,
            )
        status = EnsembleDiversityStatus(self.status)
        if self.pair_count == 0 and status is not (
            EnsembleDiversityStatus.INSUFFICIENT_SUPPORT
        ):
            raise ValueError("unsupported diversity cannot claim a diagnosis")
        if self.pair_count > 0 and status is (
            EnsembleDiversityStatus.INSUFFICIENT_SUPPORT
        ):
            raise ValueError("supported diversity cannot be insufficient")
        if self.pair_count == 0:
            if any(
                value is not None
                for value in (
                    self.mean_normalized_metric_distance,
                    self.collapse_rate,
                    self.false_diversity_rate,
                )
            ):
                raise ValueError("unsupported diversity cannot claim rates")
        else:
            if (
                self.mean_normalized_metric_distance is None
                or self.collapse_rate is None
                or self.false_diversity_rate is None
            ):
                raise ValueError("supported diversity requires rates")
            expected_collapse = self.collapsed_pair_count / self.pair_count
            expected_false = self.false_diversity_pair_count / self.pair_count
            if not math.isclose(
                self.collapse_rate,
                expected_collapse,
                rel_tol=0.0,
                abs_tol=1e-15,
            ) or not math.isclose(
                self.false_diversity_rate,
                expected_false,
                rel_tol=0.0,
                abs_tol=1e-15,
            ):
                raise ValueError("ensemble diversity rates do not reconcile")
        object.__setattr__(self, "status", status)
        expected = _stable_id("ensemble-diversity", self.payload())
        supplied = _optional_text(self.summary_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble diversity summary_id differs")
        object.__setattr__(self, "summary_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split_kind": self.split_kind.value,
            "stratum": self.stratum.to_dict(),
            "sample_count": self.sample_count,
            "pair_count": self.pair_count,
            "collapsed_pair_count": self.collapsed_pair_count,
            "false_diversity_pair_count": self.false_diversity_pair_count,
            "distinct_content_count": self.distinct_content_count,
            "mean_normalized_metric_distance": (
                self.mean_normalized_metric_distance
            ),
            "collapse_rate": self.collapse_rate,
            "false_diversity_rate": self.false_diversity_rate,
            "status": self.status.value,
            "row_order_in_identity": False,
            "member_id_or_seed_in_logical_content": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "summary_id": self.summary_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnsembleDiversitySummaryV1":
        _require_schema(data, ENSEMBLE_DIVERSITY_SUMMARY_SCHEMA_VERSION)
        _require_derived_value(data, "row_order_in_identity", False)
        _require_derived_value(
            data, "member_id_or_seed_in_logical_content", False
        )
        return cls(
            split_kind=BenchmarkSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            stratum=EnsembleCalibrationStratumV1.from_dict(
                _mapping(data.get("stratum"))
            ),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            pair_count=_strict_int(data.get("pair_count"), "pair_count"),
            collapsed_pair_count=_strict_int(
                data.get("collapsed_pair_count"), "collapsed_pair_count"
            ),
            false_diversity_pair_count=_strict_int(
                data.get("false_diversity_pair_count"),
                "false_diversity_pair_count",
            ),
            distinct_content_count=_strict_int(
                data.get("distinct_content_count"), "distinct_content_count"
            ),
            mean_normalized_metric_distance=_optional_float(
                data.get("mean_normalized_metric_distance")
            ),
            collapse_rate=_optional_float(data.get("collapse_rate")),
            false_diversity_rate=_optional_float(
                data.get("false_diversity_rate")
            ),
            status=EnsembleDiversityStatus(str(data.get("status", ""))),
            summary_id=str(data.get("summary_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EnsembleOutcomeSummaryV1:
    """Failure and refusal rates for one split and exact calibration stratum."""

    split_kind: BenchmarkSplitKind
    stratum: EnsembleCalibrationStratumV1
    attempt_count: int
    completed_count: int
    refused_count: int
    failed_count: int
    completion_rate: float
    refusal_rate: float
    failure_rate: float
    reason_counts: Mapping[str, int]
    summary_id: str = ""
    schema_version: str = ENSEMBLE_OUTCOME_SUMMARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_OUTCOME_SUMMARY_SCHEMA_VERSION,
            "ensemble outcome summary",
        )
        object.__setattr__(
            self,
            "split_kind",
            BenchmarkSplitKind.from_value(self.split_kind),
        )
        if not isinstance(self.stratum, EnsembleCalibrationStratumV1):
            raise ValueError("outcome summary requires a v1 stratum")
        for name in (
            "attempt_count",
            "completed_count",
            "refused_count",
            "failed_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.attempt_count != (
            self.completed_count + self.refused_count + self.failed_count
        ):
            raise ValueError("ensemble outcome counts do not reconcile")
        for name in ("completion_rate", "refusal_rate", "failure_rate"):
            object.__setattr__(
                self, name, _unit_float(getattr(self, name), name)
            )
        reasons = _count_mapping(self.reason_counts, "outcome reason")
        if len(reasons) > MAX_ENSEMBLE_REASON_CODES:
            raise ValueError("ensemble outcome reason count exceeds limit")
        if sum(reasons.values()) != self.refused_count + self.failed_count:
            raise ValueError("ensemble outcome reasons do not reconcile")
        if self.attempt_count:
            expected_rates = (
                self.completed_count / self.attempt_count,
                self.refused_count / self.attempt_count,
                self.failed_count / self.attempt_count,
            )
        else:
            expected_rates = (0.0, 0.0, 0.0)
        actual_rates = (
            self.completion_rate,
            self.refusal_rate,
            self.failure_rate,
        )
        if any(
            not math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-15)
            for actual, expected in zip(actual_rates, expected_rates)
        ):
            raise ValueError("ensemble outcome rates do not reconcile")
        object.__setattr__(self, "reason_counts", reasons)
        expected = _stable_id("ensemble-outcome", self.payload())
        supplied = _optional_text(self.summary_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble outcome summary_id differs")
        object.__setattr__(self, "summary_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split_kind": self.split_kind.value,
            "stratum": self.stratum.to_dict(),
            "attempt_count": self.attempt_count,
            "completed_count": self.completed_count,
            "refused_count": self.refused_count,
            "failed_count": self.failed_count,
            "completion_rate": self.completion_rate,
            "refusal_rate": self.refusal_rate,
            "failure_rate": self.failure_rate,
            "reason_counts": dict(self.reason_counts),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "summary_id": self.summary_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnsembleOutcomeSummaryV1":
        _require_schema(data, ENSEMBLE_OUTCOME_SUMMARY_SCHEMA_VERSION)
        return cls(
            split_kind=BenchmarkSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            stratum=EnsembleCalibrationStratumV1.from_dict(
                _mapping(data.get("stratum"))
            ),
            attempt_count=_strict_int(
                data.get("attempt_count"), "attempt_count"
            ),
            completed_count=_strict_int(
                data.get("completed_count"), "completed_count"
            ),
            refused_count=_strict_int(
                data.get("refused_count"), "refused_count"
            ),
            failed_count=_strict_int(data.get("failed_count"), "failed_count"),
            completion_rate=_finite_float(
                data.get("completion_rate"), "completion_rate"
            ),
            refusal_rate=_finite_float(
                data.get("refusal_rate"), "refusal_rate"
            ),
            failure_rate=_finite_float(
                data.get("failure_rate"), "failure_rate"
            ),
            reason_counts={
                str(key): _strict_int(value, "outcome reason count")
                for key, value in _mapping(data.get("reason_counts")).items()
            },
            summary_id=str(data.get("summary_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EnsembleMemberSelectionV1:
    """Representative-member evidence; never a claim of historical truth."""

    member_id: str
    selection_rank: int
    representative_distance: float | None
    completed_count: int
    refused_count: int
    failed_count: int
    primary: bool
    retained: bool
    selection_id: str = ""
    schema_version: str = ENSEMBLE_MEMBER_SELECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_MEMBER_SELECTION_SCHEMA_VERSION,
            "ensemble member selection",
        )
        object.__setattr__(self, "member_id", _required_text(self.member_id))
        object.__setattr__(
            self,
            "selection_rank",
            _positive_int(self.selection_rank, "selection_rank"),
        )
        object.__setattr__(
            self,
            "representative_distance",
            _optional_nonnegative_float(
                self.representative_distance,
                "representative_distance",
            ),
        )
        for name in ("completed_count", "refused_count", "failed_count"):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        object.__setattr__(
            self, "primary", _strict_bool(self.primary, "primary")
        )
        object.__setattr__(
            self, "retained", _strict_bool(self.retained, "retained")
        )
        if self.primary and not self.retained:
            raise ValueError("primary ensemble member must be retained")
        if self.primary and self.representative_distance is None:
            raise ValueError("primary member requires representative evidence")
        expected = _stable_id("ensemble-member-selection", self.payload())
        supplied = _optional_text(self.selection_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble member selection_id differs")
        object.__setattr__(self, "selection_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "member_id": self.member_id,
            "selection_rank": self.selection_rank,
            "representative_distance": self.representative_distance,
            "completed_count": self.completed_count,
            "refused_count": self.refused_count,
            "failed_count": self.failed_count,
            "primary": self.primary,
            "retained": self.retained,
            "selection_basis": ENSEMBLE_PRIMARY_SELECTION_BASIS,
            "interpretation": "representative_member_not_historical_truth",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "selection_id": self.selection_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnsembleMemberSelectionV1":
        _require_schema(data, ENSEMBLE_MEMBER_SELECTION_SCHEMA_VERSION)
        _require_derived_value(
            data, "selection_basis", ENSEMBLE_PRIMARY_SELECTION_BASIS
        )
        _require_derived_value(
            data,
            "interpretation",
            "representative_member_not_historical_truth",
        )
        return cls(
            member_id=str(data.get("member_id", "")),
            selection_rank=_strict_int(
                data.get("selection_rank"), "selection_rank"
            ),
            representative_distance=_optional_float(
                data.get("representative_distance")
            ),
            completed_count=_strict_int(
                data.get("completed_count"), "completed_count"
            ),
            refused_count=_strict_int(
                data.get("refused_count"), "refused_count"
            ),
            failed_count=_strict_int(data.get("failed_count"), "failed_count"),
            primary=_strict_bool(data.get("primary"), "primary"),
            retained=_strict_bool(data.get("retained"), "retained"),
            selection_id=str(data.get("selection_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EnsembleCalibrationReportV1:
    """Bounded calibrated uncertainty and retention report for one plan."""

    run_id: str
    plan_id: str
    config_id: str
    benchmark_manifest_id: str
    candidate_id: str
    storage_estimate_id: str
    retained_member_count: int
    status: EnsembleReportStatus
    primary_member_id: str | None
    retained_member_ids: tuple[str, ...]
    regenerable_member_ids: tuple[str, ...]
    member_selections: tuple[EnsembleMemberSelectionV1, ...]
    metric_calibrations: tuple[EnsembleMetricCalibrationV1, ...]
    diversity_summaries: tuple[EnsembleDiversitySummaryV1, ...]
    outcome_summaries: tuple[EnsembleOutcomeSummaryV1, ...]
    fit_sample_count: int
    evaluation_sample_count: int
    automatic_winner: bool = False
    default_generator_id: str | None = None
    report_id: str = ""
    schema_version: str = ENSEMBLE_CALIBRATION_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_CALIBRATION_REPORT_SCHEMA_VERSION,
            "ensemble calibration report",
        )
        for name in (
            "run_id",
            "plan_id",
            "config_id",
            "benchmark_manifest_id",
            "candidate_id",
            "storage_estimate_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "status", EnsembleReportStatus(self.status))
        object.__setattr__(
            self,
            "retained_member_count",
            _positive_int(self.retained_member_count, "retained_member_count"),
        )
        primary = _optional_text(self.primary_member_id)
        retained = _normalized_text_tuple(
            self.retained_member_ids, allow_empty=True
        )
        regenerable = _normalized_text_tuple(
            self.regenerable_member_ids, allow_empty=True
        )
        selections = tuple(
            sorted(self.member_selections, key=lambda item: item.selection_rank)
        )
        if not selections or tuple(
            item.selection_rank for item in selections
        ) != tuple(range(1, len(selections) + 1)):
            raise ValueError("ensemble member selection ranks are incomplete")
        selection_ids = {item.member_id for item in selections}
        if set(retained) | set(regenerable) != selection_ids or (
            set(retained) & set(regenerable)
        ):
            raise ValueError("ensemble retention sets do not partition members")
        declared_retained = {
            item.member_id for item in selections if item.retained
        }
        declared_primary = [
            item.member_id for item in selections if item.primary
        ]
        if declared_retained != set(retained):
            raise ValueError("ensemble retained selections differ")
        if self.retained_member_count > len(selections):
            raise ValueError("ensemble retained-member count exceeds members")
        if primary is None:
            if declared_primary:
                raise ValueError("ensemble primary selection differs")
        elif declared_primary != [primary] or primary not in retained:
            raise ValueError("ensemble primary member differs")
        if self.status is EnsembleReportStatus.CALIBRATED and primary is None:
            raise ValueError("calibrated ensemble requires a primary member")
        object.__setattr__(self, "primary_member_id", primary)
        object.__setattr__(self, "retained_member_ids", retained)
        object.__setattr__(self, "regenerable_member_ids", regenerable)
        object.__setattr__(self, "member_selections", selections)
        metrics = tuple(
            sorted(
                self.metric_calibrations,
                key=lambda item: (item.stratum.stratum_id, item.metric_name),
            )
        )
        diversity = tuple(
            sorted(
                self.diversity_summaries,
                key=lambda item: (
                    item.split_kind.value,
                    item.stratum.stratum_id,
                ),
            )
        )
        outcomes = tuple(
            sorted(
                self.outcome_summaries,
                key=lambda item: (
                    item.split_kind.value,
                    item.stratum.stratum_id,
                ),
            )
        )
        if not metrics or not diversity or not outcomes:
            raise ValueError(
                "ensemble report requires metric/diversity/outcome evidence"
            )
        metric_keys = [
            (item.stratum.stratum_id, item.metric_name) for item in metrics
        ]
        diversity_keys = [
            (item.split_kind, item.stratum.stratum_id) for item in diversity
        ]
        outcome_keys = [
            (item.split_kind, item.stratum.stratum_id) for item in outcomes
        ]
        if len(set(metric_keys)) != len(metric_keys):
            raise ValueError("ensemble metric summaries are duplicated")
        if len(set(diversity_keys)) != len(diversity_keys):
            raise ValueError("ensemble diversity summaries are duplicated")
        if len(set(outcome_keys)) != len(outcome_keys):
            raise ValueError("ensemble outcome summaries are duplicated")
        if set(diversity_keys) != set(outcome_keys):
            raise ValueError("ensemble diversity/outcome cells differ")
        stratum_ids = {item.stratum.stratum_id for item in diversity}
        expected_metric_keys = {
            (stratum_id, metric_name)
            for stratum_id in stratum_ids
            for metric_name in ENSEMBLE_CALIBRATION_METRIC_NAMES
        }
        if set(metric_keys) != expected_metric_keys:
            raise ValueError("ensemble metric cells are incomplete")
        outcome_by_key = {
            (item.split_kind, item.stratum.stratum_id): item
            for item in outcomes
        }
        for item in diversity:
            outcome = outcome_by_key[(item.split_kind, item.stratum.stratum_id)]
            if outcome.attempt_count != item.sample_count * len(selections):
                raise ValueError("ensemble sample/outcome counts differ")
        object.__setattr__(self, "metric_calibrations", metrics)
        object.__setattr__(self, "diversity_summaries", diversity)
        object.__setattr__(self, "outcome_summaries", outcomes)
        for name in ("fit_sample_count", "evaluation_sample_count"):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        expected_fit = sum(
            item.sample_count
            for item in diversity
            if item.split_kind is ENSEMBLE_CALIBRATION_FIT_SPLIT
        )
        expected_evaluation = sum(
            item.sample_count
            for item in diversity
            if item.split_kind is ENSEMBLE_CALIBRATION_EVALUATION_SPLIT
        )
        if (
            self.fit_sample_count != expected_fit
            or self.evaluation_sample_count != expected_evaluation
        ):
            raise ValueError("ensemble report sample counts differ")
        expected_status = _report_status_from_evidence(
            metrics,
            diversity,
            primary,
            len(retained),
            self.retained_member_count,
        )
        if self.status is not expected_status:
            raise ValueError("ensemble report status differs from evidence")
        if _strict_bool(self.automatic_winner, "automatic_winner"):
            raise ValueError(
                "ensemble report cannot select an automatic winner"
            )
        if self.default_generator_id is not None:
            raise ValueError(
                "ensemble report cannot select a default generator"
            )
        object.__setattr__(self, "automatic_winner", False)
        object.__setattr__(self, "default_generator_id", None)
        expected = _stable_id("ensemble-calibration-report", self.payload())
        supplied = _optional_text(self.report_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble calibration report_id differs")
        object.__setattr__(self, "report_id", expected)

    @property
    def calibrated(self) -> bool:
        return self.status is EnsembleReportStatus.CALIBRATED

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": ENSEMBLE_ENGINE_ID,
            "engine_version": ENSEMBLE_ENGINE_VERSION,
            "run_id": self.run_id,
            "plan_id": self.plan_id,
            "config_id": self.config_id,
            "benchmark_manifest_id": self.benchmark_manifest_id,
            "candidate_id": self.candidate_id,
            "storage_estimate_id": self.storage_estimate_id,
            "status": self.status.value,
            "confidence_quantity": ENSEMBLE_CONFIDENCE_QUANTITY,
            "confidence_scope": "stratum_metric_horizon_summary_not_per_event",
            "primary_member_id": self.primary_member_id,
            "primary_interpretation": (
                "representative_member_not_historical_truth"
            ),
            "retained_member_ids": list(self.retained_member_ids),
            "retained_member_count": self.retained_member_count,
            "regenerable_member_ids": list(self.regenerable_member_ids),
            "member_selections": [
                item.to_dict() for item in self.member_selections
            ],
            "metric_calibrations": [
                item.to_dict() for item in self.metric_calibrations
            ],
            "diversity_summaries": [
                item.to_dict() for item in self.diversity_summaries
            ],
            "outcome_summaries": [
                item.to_dict() for item in self.outcome_summaries
            ],
            "fit_sample_count": self.fit_sample_count,
            "evaluation_sample_count": self.evaluation_sample_count,
            "automatic_winner": False,
            "winner_member_id": None,
            "default_generator_id": None,
            "event_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnsembleCalibrationReportV1":
        _require_schema(data, ENSEMBLE_CALIBRATION_REPORT_SCHEMA_VERSION)
        _require_derived_value(
            data, "confidence_quantity", ENSEMBLE_CONFIDENCE_QUANTITY
        )
        _require_derived_value(
            data,
            "confidence_scope",
            "stratum_metric_horizon_summary_not_per_event",
        )
        _require_derived_value(
            data,
            "primary_interpretation",
            "representative_member_not_historical_truth",
        )
        _require_derived_value(data, "winner_member_id", None)
        _require_derived_value(data, "event_rows_inline", False)
        return cls(
            run_id=str(data.get("run_id", "")),
            plan_id=str(data.get("plan_id", "")),
            config_id=str(data.get("config_id", "")),
            benchmark_manifest_id=str(data.get("benchmark_manifest_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            storage_estimate_id=str(data.get("storage_estimate_id", "")),
            retained_member_count=_strict_int(
                data.get("retained_member_count"), "retained_member_count"
            ),
            status=EnsembleReportStatus(str(data.get("status", ""))),
            primary_member_id=_optional_text(data.get("primary_member_id")),
            retained_member_ids=_string_tuple(
                data.get("retained_member_ids"), "retained_member_ids"
            ),
            regenerable_member_ids=_string_tuple(
                data.get("regenerable_member_ids"),
                "regenerable_member_ids",
            ),
            member_selections=tuple(
                EnsembleMemberSelectionV1.from_dict(item)
                for item in _mapping_sequence(data, "member_selections")
            ),
            metric_calibrations=tuple(
                EnsembleMetricCalibrationV1.from_dict(item)
                for item in _mapping_sequence(data, "metric_calibrations")
            ),
            diversity_summaries=tuple(
                EnsembleDiversitySummaryV1.from_dict(item)
                for item in _mapping_sequence(data, "diversity_summaries")
            ),
            outcome_summaries=tuple(
                EnsembleOutcomeSummaryV1.from_dict(item)
                for item in _mapping_sequence(data, "outcome_summaries")
            ),
            fit_sample_count=_strict_int(
                data.get("fit_sample_count"), "fit_sample_count"
            ),
            evaluation_sample_count=_strict_int(
                data.get("evaluation_sample_count"),
                "evaluation_sample_count",
            ),
            automatic_winner=_strict_bool(
                data.get("automatic_winner", False), "automatic_winner"
            ),
            default_generator_id=_optional_text(
                data.get("default_generator_id")
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EnsembleCalibrationReportV1":
        return cls.from_dict(_json_mapping(text))


def calibrate_reconstruction_ensemble(
    plan: ReconstructionEnsemblePlanV1,
    *,
    samples: Sequence[EnsembleCalibrationSampleV1],
    storage_estimate: EnsembleStorageEstimateV1,
) -> EnsembleCalibrationReportV1:
    """Fit validation intervals and evaluate them only on final holdouts."""
    if not isinstance(plan, ReconstructionEnsemblePlanV1):
        raise ValueError("ensemble calibration requires a v1 plan")
    if not isinstance(storage_estimate, EnsembleStorageEstimateV1):
        raise ValueError("ensemble calibration requires a storage estimate")
    if (
        storage_estimate.plan_id != plan.plan_id
        or storage_estimate.run_id != plan.run.run_id
        or set(storage_estimate.member_event_counts)
        != {item.member_id for item in plan.members}
        or storage_estimate.retained_member_count
        != plan.config.retained_member_count
    ):
        raise ValueError("ensemble storage estimate differs from plan")
    values = tuple(sorted(samples, key=lambda item: item.sample_id))
    if not values or len(values) > plan.config.max_samples:
        raise ValueError("ensemble sample count is outside configured limits")
    if len({item.sample_id for item in values}) != len(values):
        raise ValueError("ensemble calibration samples are duplicated")
    benchmark_ids = {item.benchmark_manifest_id for item in values}
    candidate_ids = {item.candidate_id for item in values}
    if len(benchmark_ids) != 1 or len(candidate_ids) != 1:
        raise ValueError(
            "ensemble calibration mixes benchmark/candidate identity"
        )
    expected_members = {item.member_id for item in plan.members}
    for sample in values:
        if {item.member_id for item in sample.members} != expected_members:
            raise ValueError("ensemble sample does not cover planned members")
        if sample.stratum.horizon_ns not in plan.config.horizons_ns:
            raise ValueError("ensemble sample horizon differs from config")
    if {item.stratum.horizon_ns for item in values} != set(
        plan.config.horizons_ns
    ):
        raise ValueError("ensemble samples do not cover configured horizons")
    strata = {item.stratum.stratum_id: item.stratum for item in values}
    if len(strata) > plan.config.max_slices:
        raise ValueError("ensemble calibration slice count exceeds config")
    grouped: dict[
        tuple[BenchmarkSplitKind, str], list[EnsembleCalibrationSampleV1]
    ] = {}
    for sample in values:
        grouped.setdefault(
            (sample.split_kind, sample.stratum.stratum_id), []
        ).append(sample)
    metric_summaries = tuple(
        _calibrate_metric_cell(
            plan.config,
            stratum,
            metric_name,
            grouped.get((ENSEMBLE_CALIBRATION_FIT_SPLIT, stratum_id), []),
            grouped.get(
                (ENSEMBLE_CALIBRATION_EVALUATION_SPLIT, stratum_id), []
            ),
        )
        for stratum_id, stratum in sorted(strata.items())
        for metric_name in ENSEMBLE_CALIBRATION_METRIC_NAMES
    )
    diversity = tuple(
        _diversity_summary(plan.config, split, stratum, selected)
        for (split, stratum_id), selected in sorted(
            grouped.items(), key=lambda item: (item[0][0].value, item[0][1])
        )
        for stratum in (strata[stratum_id],)
    )
    outcomes = tuple(
        _outcome_summary(split, stratum, selected)
        for (split, stratum_id), selected in sorted(
            grouped.items(), key=lambda item: (item[0][0].value, item[0][1])
        )
        for stratum in (strata[stratum_id],)
    )
    selections, primary, retained = _member_selections(plan, values)
    regenerable = tuple(sorted(expected_members.difference(retained)))
    status = _report_status(
        plan.config,
        metric_summaries,
        diversity,
        primary,
        retained,
    )
    report = EnsembleCalibrationReportV1(
        run_id=plan.run.run_id,
        plan_id=plan.plan_id,
        config_id=plan.config.config_id,
        benchmark_manifest_id=next(iter(benchmark_ids)),
        candidate_id=next(iter(candidate_ids)),
        storage_estimate_id=storage_estimate.estimate_id,
        retained_member_count=plan.config.retained_member_count,
        status=status,
        primary_member_id=primary,
        retained_member_ids=retained,
        regenerable_member_ids=regenerable,
        member_selections=selections,
        metric_calibrations=metric_summaries,
        diversity_summaries=diversity,
        outcome_summaries=outcomes,
        fit_sample_count=sum(
            item.split_kind is ENSEMBLE_CALIBRATION_FIT_SPLIT for item in values
        ),
        evaluation_sample_count=sum(
            item.split_kind is ENSEMBLE_CALIBRATION_EVALUATION_SPLIT
            for item in values
        ),
    )
    _ensure_payload_size(report.to_dict(), plan.config.max_payload_bytes)
    return report


@dataclass(frozen=True, slots=True)
class EnsembleRegenerationRequestV1:
    """Hash-bound request for deterministic regeneration of omitted members."""

    plan_id: str
    report_id: str
    member_ids: tuple[str, ...]
    source_artifact_hashes: Mapping[str, str]
    configuration_artifact_hashes: Mapping[str, str]
    request_id: str = ""
    schema_version: str = ENSEMBLE_REGENERATION_REQUEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            ENSEMBLE_REGENERATION_REQUEST_SCHEMA_VERSION,
            "ensemble regeneration request",
        )
        object.__setattr__(self, "plan_id", _required_text(self.plan_id))
        object.__setattr__(self, "report_id", _required_text(self.report_id))
        members = _normalized_text_tuple(self.member_ids)
        if len(members) > MAX_BENCHMARK_ENSEMBLE_MEMBERS:
            raise ValueError("regeneration member count exceeds limit")
        object.__setattr__(self, "member_ids", members)
        sources = _hash_mapping(self.source_artifact_hashes, "source artifact")
        configurations = _hash_mapping(
            self.configuration_artifact_hashes,
            "configuration artifact",
        )
        if not sources or not configurations:
            raise ValueError(
                "regeneration request requires source/config hashes"
            )
        object.__setattr__(self, "source_artifact_hashes", sources)
        object.__setattr__(
            self, "configuration_artifact_hashes", configurations
        )
        expected = _stable_id("ensemble-regeneration", self.payload())
        supplied = _optional_text(self.request_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble regeneration request_id differs")
        object.__setattr__(self, "request_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "report_id": self.report_id,
            "member_ids": list(self.member_ids),
            "source_artifact_hashes": dict(self.source_artifact_hashes),
            "configuration_artifact_hashes": dict(
                self.configuration_artifact_hashes
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "request_id": self.request_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnsembleRegenerationRequestV1":
        _require_schema(data, ENSEMBLE_REGENERATION_REQUEST_SCHEMA_VERSION)
        return cls(
            plan_id=str(data.get("plan_id", "")),
            report_id=str(data.get("report_id", "")),
            member_ids=_string_tuple(data.get("member_ids"), "member_ids"),
            source_artifact_hashes={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("source_artifact_hashes")
                ).items()
            },
            configuration_artifact_hashes={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("configuration_artifact_hashes")
                ).items()
            },
            request_id=str(data.get("request_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EnsembleRegenerationRequestV1":
        return cls.from_dict(_json_mapping(text))


def build_ensemble_regeneration_request(
    plan: ReconstructionEnsemblePlanV1,
    report: EnsembleCalibrationReportV1,
    *,
    member_ids: Iterable[str],
) -> EnsembleRegenerationRequestV1:
    """Build a request using the exact hashes frozen by the ensemble plan."""
    return EnsembleRegenerationRequestV1(
        plan_id=plan.plan_id,
        report_id=report.report_id,
        member_ids=tuple(member_ids),
        source_artifact_hashes=plan.source_hashes,
        configuration_artifact_hashes=plan.configuration_hashes,
    )


def verify_ensemble_regeneration(
    plan: ReconstructionEnsemblePlanV1,
    report: EnsembleCalibrationReportV1,
    request: EnsembleRegenerationRequestV1,
    *,
    available_source_artifact_hashes: Mapping[str, str],
    available_configuration_artifact_hashes: Mapping[str, str],
) -> tuple[EnsembleMemberPlanV1, ...]:
    """Fail closed unless plan/report/member scope and every hash still match."""
    if not report.calibrated:
        raise ValueError("only calibrated ensemble reports may regenerate")
    if (
        request.plan_id != plan.plan_id
        or report.plan_id != plan.plan_id
        or request.report_id != report.report_id
        or report.run_id != plan.run.run_id
        or report.config_id != plan.config.config_id
    ):
        raise ValueError("ensemble regeneration scope differs")
    if request.source_artifact_hashes != plan.source_hashes:
        raise ValueError("ensemble regeneration source hashes differ")
    if request.configuration_artifact_hashes != plan.configuration_hashes:
        raise ValueError("ensemble regeneration config hashes differ")
    available_sources = _hash_mapping(
        available_source_artifact_hashes, "available source artifact"
    )
    available_configurations = _hash_mapping(
        available_configuration_artifact_hashes,
        "available configuration artifact",
    )
    if available_sources != plan.source_hashes:
        raise ValueError("available regeneration source hashes differ")
    if available_configurations != plan.configuration_hashes:
        raise ValueError("available regeneration config hashes differ")
    allowed = set(report.regenerable_member_ids)
    if not set(request.member_ids).issubset(allowed):
        raise ValueError(
            "regeneration request contains retained/unknown members"
        )
    return tuple(plan.member(member_id) for member_id in request.member_ids)


def _calibrate_metric_cell(
    config: EnsembleCalibrationConfigV1,
    stratum: EnsembleCalibrationStratumV1,
    metric_name: str,
    fit_samples: Sequence[EnsembleCalibrationSampleV1],
    evaluation_samples: Sequence[EnsembleCalibrationSampleV1],
) -> EnsembleMetricCalibrationV1:
    fit_nonconformity: list[float] = []
    for sample in fit_samples:
        interval = _sample_interval(
            sample, metric_name, config.nominal_coverage
        )
        if interval is None:
            continue
        lower, upper, _ = interval
        reference = sample.reference_metrics[metric_name]
        fit_nonconformity.append(max(lower - reference, reference - upper, 0.0))
    if len(fit_nonconformity) < config.minimum_fit_samples:
        return EnsembleMetricCalibrationV1(
            stratum=stratum,
            metric_name=metric_name,
            metric_group=ENSEMBLE_CALIBRATION_METRIC_GROUPS[metric_name],
            nominal_coverage=config.nominal_coverage,
            minimum_achieved_coverage=config.minimum_achieved_coverage,
            fit_sample_count=len(fit_nonconformity),
            evaluation_sample_count=0,
            calibration_adjustment=None,
            raw_covered_count=0,
            calibrated_covered_count=0,
            raw_coverage_rate=None,
            calibrated_coverage_rate=None,
            mean_raw_interval_width=None,
            mean_calibrated_interval_width=None,
            mean_absolute_median_error=None,
            status=EnsembleMetricStatus.INSUFFICIENT_SUPPORT,
        )
    adjustment = _conformal_adjustment(
        fit_nonconformity, config.nominal_coverage
    )
    raw_covered = 0
    calibrated_covered = 0
    raw_widths: list[float] = []
    calibrated_widths: list[float] = []
    median_errors: list[float] = []
    for sample in evaluation_samples:
        interval = _sample_interval(
            sample, metric_name, config.nominal_coverage
        )
        if interval is None:
            continue
        lower, upper, median = interval
        reference = sample.reference_metrics[metric_name]
        raw_covered += int(lower <= reference <= upper)
        adjusted_lower = lower - adjustment
        adjusted_upper = upper + adjustment
        calibrated_covered += int(adjusted_lower <= reference <= adjusted_upper)
        raw_widths.append(upper - lower)
        calibrated_widths.append(adjusted_upper - adjusted_lower)
        median_errors.append(abs(median - reference))
    support = len(raw_widths)
    if support == 0:
        return EnsembleMetricCalibrationV1(
            stratum=stratum,
            metric_name=metric_name,
            metric_group=ENSEMBLE_CALIBRATION_METRIC_GROUPS[metric_name],
            nominal_coverage=config.nominal_coverage,
            minimum_achieved_coverage=config.minimum_achieved_coverage,
            fit_sample_count=len(fit_nonconformity),
            evaluation_sample_count=0,
            calibration_adjustment=None,
            raw_covered_count=0,
            calibrated_covered_count=0,
            raw_coverage_rate=None,
            calibrated_coverage_rate=None,
            mean_raw_interval_width=None,
            mean_calibrated_interval_width=None,
            mean_absolute_median_error=None,
            status=EnsembleMetricStatus.INSUFFICIENT_SUPPORT,
        )
    raw_rate = raw_covered / support
    calibrated_rate = calibrated_covered / support
    status = (
        EnsembleMetricStatus.CALIBRATED
        if calibrated_rate >= config.minimum_achieved_coverage
        else EnsembleMetricStatus.MISCALIBRATED
    )
    return EnsembleMetricCalibrationV1(
        stratum=stratum,
        metric_name=metric_name,
        metric_group=ENSEMBLE_CALIBRATION_METRIC_GROUPS[metric_name],
        nominal_coverage=config.nominal_coverage,
        minimum_achieved_coverage=config.minimum_achieved_coverage,
        fit_sample_count=len(fit_nonconformity),
        evaluation_sample_count=support,
        calibration_adjustment=adjustment,
        raw_covered_count=raw_covered,
        calibrated_covered_count=calibrated_covered,
        raw_coverage_rate=raw_rate,
        calibrated_coverage_rate=calibrated_rate,
        mean_raw_interval_width=_rounded(
            sum(raw_widths) / support, config.rounding_digits
        ),
        mean_calibrated_interval_width=_rounded(
            sum(calibrated_widths) / support, config.rounding_digits
        ),
        mean_absolute_median_error=_rounded(
            sum(median_errors) / support, config.rounding_digits
        ),
        status=status,
    )


def _sample_interval(
    sample: EnsembleCalibrationSampleV1,
    metric_name: str,
    nominal_coverage: float,
) -> tuple[float, float, float] | None:
    values = sorted(
        item.metrics[metric_name]
        for item in sample.members
        if item.status is EnsembleMemberStatus.COMPLETED
    )
    if len(values) < 2:
        return None
    alpha = (1.0 - nominal_coverage) / 2.0
    lower_index = math.floor(alpha * (len(values) - 1))
    upper_index = math.ceil((1.0 - alpha) * (len(values) - 1))
    return values[lower_index], values[upper_index], _median(values)


def _conformal_adjustment(values: Sequence[float], coverage: float) -> float:
    ordered = sorted(values)
    rank = math.ceil((len(ordered) + 1) * coverage) - 1
    return ordered[min(len(ordered) - 1, max(0, rank))]


def _diversity_summary(
    config: EnsembleCalibrationConfigV1,
    split: BenchmarkSplitKind,
    stratum: EnsembleCalibrationStratumV1,
    samples: Sequence[EnsembleCalibrationSampleV1],
) -> EnsembleDiversitySummaryV1:
    distances: list[float] = []
    collapsed = 0
    false_diversity = 0
    distinct_total = 0
    for sample in samples:
        completed = tuple(
            item
            for item in sample.members
            if item.status is EnsembleMemberStatus.COMPLETED
        )
        distinct_total += len(
            {item.logical_content_sha256 for item in completed}
        )
        for left, right in combinations(completed, 2):
            distance = _normalized_metric_distance(left.metrics, right.metrics)
            distances.append(distance)
            if left.logical_content_sha256 == right.logical_content_sha256:
                collapsed += 1
            elif distance <= config.logical_distance_tolerance:
                false_diversity += 1
    pair_count = len(distances)
    if not pair_count:
        status = EnsembleDiversityStatus.INSUFFICIENT_SUPPORT
        collapse_rate = None
        false_rate = None
        mean_distance = None
    else:
        collapse_rate = collapsed / pair_count
        false_rate = false_diversity / pair_count
        mean_distance = sum(distances) / pair_count
        if collapse_rate > config.maximum_collapse_rate:
            status = EnsembleDiversityStatus.COLLAPSED
        elif false_rate > config.maximum_false_diversity_rate:
            status = EnsembleDiversityStatus.FALSE_DIVERSITY
        else:
            status = EnsembleDiversityStatus.DIVERSE
    return EnsembleDiversitySummaryV1(
        split_kind=split,
        stratum=stratum,
        sample_count=len(samples),
        pair_count=pair_count,
        collapsed_pair_count=collapsed,
        false_diversity_pair_count=false_diversity,
        distinct_content_count=distinct_total,
        mean_normalized_metric_distance=(
            _rounded(mean_distance, config.rounding_digits)
            if mean_distance is not None
            else None
        ),
        collapse_rate=collapse_rate,
        false_diversity_rate=false_rate,
        status=status,
    )


def _outcome_summary(
    split: BenchmarkSplitKind,
    stratum: EnsembleCalibrationStratumV1,
    samples: Sequence[EnsembleCalibrationSampleV1],
) -> EnsembleOutcomeSummaryV1:
    results = tuple(item for sample in samples for item in sample.members)
    counts = Counter(item.status for item in results)
    reasons = Counter(
        item.reason for item in results if item.reason is not None
    )
    attempts = len(results)
    completed = counts[EnsembleMemberStatus.COMPLETED]
    refused = counts[EnsembleMemberStatus.REFUSED]
    failed = counts[EnsembleMemberStatus.FAILED]
    return EnsembleOutcomeSummaryV1(
        split_kind=split,
        stratum=stratum,
        attempt_count=attempts,
        completed_count=completed,
        refused_count=refused,
        failed_count=failed,
        completion_rate=completed / attempts if attempts else 0.0,
        refusal_rate=refused / attempts if attempts else 0.0,
        failure_rate=failed / attempts if attempts else 0.0,
        reason_counts={str(reason): count for reason, count in reasons.items()},
    )


def _member_selections(
    plan: ReconstructionEnsemblePlanV1,
    samples: Sequence[EnsembleCalibrationSampleV1],
) -> tuple[
    tuple[EnsembleMemberSelectionV1, ...],
    str | None,
    tuple[str, ...],
]:
    fit_samples = tuple(
        item
        for item in samples
        if item.split_kind is ENSEMBLE_CALIBRATION_FIT_SPLIT
    )
    distances: dict[str, list[float]] = {
        item.member_id: [] for item in plan.members
    }
    counts: dict[str, Counter[EnsembleMemberStatus]] = {
        item.member_id: Counter() for item in plan.members
    }
    for sample in fit_samples:
        completed = tuple(
            item
            for item in sample.members
            if item.status is EnsembleMemberStatus.COMPLETED
        )
        medians = (
            {
                metric: _median(
                    sorted(item.metrics[metric] for item in completed)
                )
                for metric in ENSEMBLE_CALIBRATION_METRIC_NAMES
            }
            if completed
            else {}
        )
        for member in sample.members:
            counts[member.member_id][member.status] += 1
            if member.status is EnsembleMemberStatus.COMPLETED:
                distances[member.member_id].append(
                    _normalized_metric_distance(member.metrics, medians)
                )
    scored: list[tuple[float | None, str]] = []
    total_fit = len(fit_samples)
    for plan_member in plan.members:
        values = distances[plan_member.member_id]
        failures = (
            counts[plan_member.member_id][EnsembleMemberStatus.REFUSED]
            + counts[plan_member.member_id][EnsembleMemberStatus.FAILED]
        )
        score = None
        if values:
            score = sum(values) / len(values)
            if total_fit:
                score += plan.config.failure_penalty * failures / total_fit
        scored.append((score, plan_member.member_id))
    ordered = sorted(
        scored,
        key=lambda item: (
            item[0] is None,
            item[0] if item[0] is not None else math.inf,
            item[1],
        ),
    )
    eligible = [item for item in ordered if item[0] is not None]
    primary = eligible[0][1] if eligible else None
    retained = tuple(
        item[1] for item in eligible[: plan.config.retained_member_count]
    )
    rank_by_member = {
        member_id: rank for rank, (_, member_id) in enumerate(ordered, start=1)
    }
    score_by_member = {member_id: score for score, member_id in ordered}
    selections: list[EnsembleMemberSelectionV1] = []
    for _, member_id in ordered:
        raw_score = score_by_member[member_id]
        selections.append(
            EnsembleMemberSelectionV1(
                member_id=member_id,
                selection_rank=rank_by_member[member_id],
                representative_distance=(
                    _rounded(raw_score, plan.config.rounding_digits)
                    if raw_score is not None
                    else None
                ),
                completed_count=counts[member_id][
                    EnsembleMemberStatus.COMPLETED
                ],
                refused_count=counts[member_id][EnsembleMemberStatus.REFUSED],
                failed_count=counts[member_id][EnsembleMemberStatus.FAILED],
                primary=member_id == primary,
                retained=member_id in retained,
            )
        )
    return tuple(selections), primary, tuple(sorted(retained))


def _report_status(
    config: EnsembleCalibrationConfigV1,
    metrics: Sequence[EnsembleMetricCalibrationV1],
    diversity: Sequence[EnsembleDiversitySummaryV1],
    primary: str | None,
    retained: Sequence[str],
) -> EnsembleReportStatus:
    return _report_status_from_evidence(
        metrics,
        diversity,
        primary,
        len(retained),
        config.retained_member_count,
    )


def _report_status_from_evidence(
    metrics: Sequence[EnsembleMetricCalibrationV1],
    diversity: Sequence[EnsembleDiversitySummaryV1],
    primary: str | None,
    retained_count: int,
    required_retained_count: int,
) -> EnsembleReportStatus:
    if (
        primary is None
        or retained_count != required_retained_count
        or any(
            item.status is EnsembleMetricStatus.INSUFFICIENT_SUPPORT
            for item in metrics
        )
        or any(
            item.status is EnsembleDiversityStatus.INSUFFICIENT_SUPPORT
            for item in diversity
        )
    ):
        return EnsembleReportStatus.INSUFFICIENT_SUPPORT
    if any(
        item.status is EnsembleMetricStatus.MISCALIBRATED for item in metrics
    ) or any(
        item.status
        in {
            EnsembleDiversityStatus.COLLAPSED,
            EnsembleDiversityStatus.FALSE_DIVERSITY,
        }
        for item in diversity
    ):
        return EnsembleReportStatus.MISCALIBRATED
    return EnsembleReportStatus.CALIBRATED


def _benchmark_metrics(
    events: Sequence[BenchmarkEventV1], downstream_sensitivity: float
) -> dict[str, float]:
    ordered = _validated_benchmark_events(events)
    if not ordered:
        raise ValueError("benchmark metrics require events")
    interarrivals = [
        right.event_time_ns - left.event_time_ns
        for left, right in zip(ordered, ordered[1:])
    ]
    mids = [item.mid for item in ordered]
    return {
        "event_count": float(len(ordered)),
        "observed_duration_ns": float(
            ordered[-1].event_time_ns - ordered[0].event_time_ns
        ),
        "mean_interarrival_ns": (
            sum(interarrivals) / len(interarrivals) if interarrivals else 0.0
        ),
        "mean_spread": sum(item.spread for item in ordered) / len(ordered),
        "mid_path_range": max(mids) - min(mids),
        "endpoint_mid": mids[-1],
        "downstream_sensitivity": _finite_float(
            downstream_sensitivity, "downstream_sensitivity"
        ),
    }


def _validated_benchmark_events(
    values: Iterable[BenchmarkEventV1],
) -> tuple[BenchmarkEventV1, ...]:
    events = tuple(values)
    if any(not isinstance(item, BenchmarkEventV1) for item in events):
        raise ValueError("ensemble calibration requires benchmark v1 events")
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


def _normalized_metric_distance(
    left: Mapping[str, float], right: Mapping[str, float]
) -> float:
    names = ENSEMBLE_CALIBRATION_METRIC_NAMES
    return sum(
        abs(left[name] - right[name])
        / max(abs(left[name]), abs(right[name]), 1e-15)
        for name in names
    ) / len(names)


def _derive_member_id(
    config: EnsembleCalibrationConfigV1,
    sources: Sequence[EnsembleArtifactDigestV1],
    configurations: Sequence[EnsembleArtifactDigestV1],
    base_seed: int,
    ordinal: int,
) -> str:
    return _stable_id(
        "ensemble-member",
        {
            "member_contract": ENSEMBLE_MEMBER_PLAN_SCHEMA_VERSION,
            "config_id": config.config_id,
            "source_artifacts": [item.to_dict() for item in sources],
            "configuration_artifacts": [
                item.to_dict() for item in configurations
            ],
            "base_seed": _nonnegative_int(base_seed, "base_seed"),
            "ordinal": _positive_int(ordinal, "ordinal"),
        },
    )


def _artifact_digests(
    values: Mapping[str, str], kind: EnsembleArtifactKind
) -> tuple[EnsembleArtifactDigestV1, ...]:
    if not values or len(values) > MAX_ENSEMBLE_ARTIFACTS:
        raise ValueError("ensemble artifact count is outside limits")
    return tuple(
        EnsembleArtifactDigestV1(
            artifact_id=artifact_id,
            sha256=digest,
            kind=kind,
        )
        for artifact_id, digest in sorted(values.items())
    )


def _normalized_artifacts(
    values: Iterable[EnsembleArtifactDigestV1], kind: EnsembleArtifactKind
) -> tuple[EnsembleArtifactDigestV1, ...]:
    artifacts = tuple(sorted(values, key=lambda item: item.artifact_id))
    if len(artifacts) > MAX_ENSEMBLE_ARTIFACTS or any(
        not isinstance(item, EnsembleArtifactDigestV1) or item.kind is not kind
        for item in artifacts
    ):
        raise ValueError("ensemble artifact digest kind/count differs")
    if len({item.artifact_id for item in artifacts}) != len(artifacts):
        raise ValueError("ensemble artifact IDs are duplicated")
    return artifacts


def _content_sha256(payload: Any) -> str:
    encoded = canonical_contract_json(cast(JSONValue, payload)).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("median requires values")
    center = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[center]
    return (ordered[center - 1] + ordered[center]) / 2.0


def _rounded(value: float, digits: int) -> float:
    return round(_finite_float(value, "rounded value"), digits)


def _metric_mapping(
    values: Mapping[str, float], *, allow_empty: bool = False
) -> dict[str, float]:
    result = {
        _required_text(str(key)): _finite_float(value, f"metric {key}")
        for key, value in values.items()
    }
    if not result and allow_empty:
        return {}
    if set(result) != set(ENSEMBLE_CALIBRATION_METRIC_NAMES):
        raise ValueError("ensemble calibration metrics differ")
    return dict(sorted(result.items()))


def _count_mapping(values: Mapping[str, int], label: str) -> dict[str, int]:
    return dict(
        sorted(
            (
                _required_text(str(key)),
                _nonnegative_int(value, f"{label} count"),
            )
            for key, value in values.items()
        )
    )


def _hash_mapping(values: Mapping[str, str], label: str) -> dict[str, str]:
    return dict(
        sorted(
            (
                _required_text(str(key)),
                _required_sha256(value),
            )
            for key, value in values.items()
        )
    )


def _required_sha256(value: Any) -> str:
    text = _required_text(value).lower()
    digest = text.removeprefix("sha256:")
    if len(digest) != 64 or any(
        char not in "0123456789abcdef" for char in digest
    ):
        raise ValueError("value must be a SHA-256 digest")
    return "sha256:" + digest


def _normalized_text_tuple(
    values: Iterable[str], *, allow_empty: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(value) for value in values}))
    if not result and not allow_empty:
        raise ValueError("value requires non-empty identifiers")
    return result


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("value must be non-empty text")
    result = value.strip()
    if len(result) > MAX_ENSEMBLE_TEXT:
        raise ValueError("text exceeds ensemble bound")
    return result


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text value must be a string")
    selected = value.strip()
    return _required_text(selected) if selected else None


def _optional_bounded_text(value: Any, name: str) -> str | None:
    if value is None:
        return None
    result = _required_text(value)
    if len(result) > MAX_ENSEMBLE_TEXT:
        raise ValueError(f"{name} exceeds ensemble bound")
    return result


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{name} must be boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _nonnegative_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result < 0.0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _optional_nonnegative_float(value: Any, name: str) -> float | None:
    if value is None:
        return None
    return _nonnegative_float(value, name)


def _unit_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{name} must be between zero and one")
    return result


def _normalized_symbol(value: Any) -> str:
    result = _required_text(value).upper()
    if not result.isalnum():
        raise ValueError("symbol must be alphanumeric")
    return result


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return _finite_float(value, "optional float")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("value must be a mapping")
    return cast(Mapping[str, Any], value)


def _mapping_sequence(
    data: Mapping[str, Any], key: str
) -> tuple[Mapping[str, Any], ...]:
    value = data.get(key)
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{key} must be a sequence")
    result: list[Mapping[str, Any]] = []
    for item in value:
        result.append(_mapping(item))
    return tuple(result)


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")
    return tuple(str(item) for item in value)


def _int_tuple(value: Any, name: str) -> tuple[int, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")
    return tuple(_strict_int(item, name) for item in value)


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)


def _require_version(actual: str, expected: str, label: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {label} schema")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError("unsupported ensemble schema version")


def _require_derived_value(
    data: Mapping[str, Any], key: str, expected: Any
) -> None:
    if data.get(key) != expected:
        raise ValueError(f"ensemble derived field {key} differs")


def _ensure_payload_size(
    payload: Mapping[str, JSONValue], maximum: int
) -> None:
    size = len(canonical_contract_json(payload).encode("utf-8"))
    if size > maximum:
        raise ValueError("ensemble report exceeds bounded payload size")
