"""Independent replay and final indexing for adaptive reconstruction support.

The planner is deliberately not an oracle in this module.  The verifier reads
immutable Arrow partitions, reconstructs half-open row domains, alignment
support, contextual availability, and generator admission from lower-level
artifacts, then compares those decisions with the published plan/support map.
Only a fully reconciled replay can produce the final support-map index used by
campaign execution requests.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import struct
import tempfile
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import pairwise
from pathlib import Path
from typing import Any, cast

from histdatacom.cross_series_constraints import (
    CrossSeriesAlignmentPolicy,
    read_cross_series_constraint_policy,
)
from histdatacom.data_analytics.feed_epochs_v2 import (
    read_active_time_feed_epoch_definition,
)
from histdatacom.market_context import (
    CftcPositioningQueryStatus,
    CftcReportFamily,
    CftcReportScope,
    MarketContextKind,
    MarketContextView,
    market_context_benchmark_event_state,
    market_context_calendar_state,
    preflight_market_context_corpus,
    query_cftc_positioning_corpus,
    query_market_context_corpus,
    read_cftc_positioning_corpus,
    read_market_context_corpus,
)
from histdatacom.orchestration.reconstruction import (
    artifact_ref_for_file,
    verify_artifact_ref,
)
from histdatacom.reconstruction import (
    RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION,
    RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION,
    RECONSTRUCTION_SYMBOLS,
    ReconstructionPlanSetV1,
    ReconstructionPlanSupportMapV1,
    ReconstructionPlanSupportWindowV1,
    iter_reconstruction_plan_support_maps,
    read_reconstruction_plan_set,
    read_reconstruction_plan_support_map,
    read_reconstruction_plan_support_map_index,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.feed_epoch_transition import (
    FeedEpochTransitionPolicyV1,
    read_feed_epoch_transition_policy,
)
from histdatacom.synthetic.hawkes_selection import (
    read_hawkes_product_selection_dossier,
)
from histdatacom.synthetic.historical_conditioning import (
    historical_product_observation_conditioning,
    historical_product_retention_probability,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.marked_hawkes import MarkedHawkesConfigV1
from histdatacom.synthetic.observation import (
    ObservationOperatorV1,
    read_observation_operator_artifact,
)
from histdatacom.synthetic.observation_uncertainty import (
    ObservationUncertaintyPolicyV1,
    observation_admission_missing_count_bound,
    read_observation_uncertainty_policy,
)
from histdatacom.synthetic.persistence import (
    DEFAULT_ESTIMATED_COMPRESSION_RATIO,
    DEFAULT_MANIFEST_BYTES_PER_PARTITION,
    DEFAULT_MANIFEST_BYTES_PER_PRODUCT,
)
from histdatacom.synthetic.proposal_engines import (
    proposal_evaluation_engine_artifacts,
    read_proposal_portfolio_evaluation,
)
from histdatacom.synthetic.qualification import (
    read_powered_qualification_dossier,
)
from histdatacom.synthetic.reconstruction_plan import (
    CFTC_READY_CONDITIONING_MODE,
    CFTC_UNAVAILABLE_CONDITIONING_MODE,
    CFTC_UNCONDITIONED_AVAILABILITY_STATUSES,
    ReconstructionCftcConditioningMode,
    ReconstructionPlanConfiguration,
    ReconstructionPlanConfigurationV2,
    ReconstructionPlanRefusalCode,
    ReconstructionPlanSourceSupportStatus,
    ReconstructionSourcePartitionV1,
    ReconstructionWindowSizingAuditV1,
    SyntheticInfillPlanV1,
    read_reconstruction_context_availability_qualification,
    read_reconstruction_plan_configuration,
    read_reconstruction_source_inventory,
    read_reconstruction_window_sizing_audit,
    read_synthetic_infill_plan,
    validate_synthetic_infill_plan_for_execution,
)
from histdatacom.synthetic.release_candidate import (
    ReconstructionReleaseCandidateV1,
    read_reconstruction_release_candidate,
    verify_reconstruction_release_candidate,
)
from histdatacom.synthetic.streaming import ReconstructionResourceEstimateV1

FINAL_SUPPORT_PARTITION_REPLAY_SCHEMA_VERSION = (
    "histdatacom.final-support-partition-replay.v1"
)
FINAL_SUPPORT_WINDOW_VERIFICATION_SCHEMA_VERSION = (
    "histdatacom.final-support-window-verification.v1"
)
FINAL_SUPPORT_CENSUS_SCHEMA_VERSION = "histdatacom.final-support-census.v1"
FINAL_SUPPORT_VERIFICATION_SHARD_SCHEMA_VERSION = (
    "histdatacom.final-support-verification-shard.v1"
)
FINAL_ADAPTIVE_SUPPORT_MAP_INDEX_SCHEMA_VERSION = (
    "histdatacom.final-adaptive-support-map-index.v1"
)

FINAL_SUPPORT_VERIFICATION_SHARD_ARTIFACT_KIND = (
    "final_support_verification_shard_v1"
)
FINAL_ADAPTIVE_SUPPORT_MAP_INDEX_ARTIFACT_KIND = (
    "final_adaptive_support_map_index_v1"
)

INDEPENDENT_VERIFIER_ID = "histdatacom.independent-source-support-verifier.v1"
INDEPENDENT_DECISION_POLICY = (
    "raw-arrow-replay-independent-of-planner-decision-helper-v1"
)
ROW_DOMAIN_POLICY = "strict-half-open-core-domain-exactly-once-v1"
ROW_IDENTITY_POLICY = "partition-id-plus-zero-based-arrow-row-ordinal-v1"
ALIGNMENT_EVENT_POLICY = (
    "exact-or-bounded-nearest-prior-source-row-identities-v1"
)

MAX_FINAL_SUPPORT_WINDOWS_PER_SHARD = 4096
MAX_FINAL_SUPPORT_SHARDS = 4096
MAX_FINAL_SUPPORT_PARTITIONS_PER_SHARD = 256
MAX_FINAL_SUPPORT_ARTIFACT_BYTES = 64 * 1024 * 1024
_RESOURCE_FIXED_OVERHEAD_BYTES = 512 * 1024 * 1024
_RESOURCE_LEDGER_BYTES_PER_INTERVAL = 8 * 1024
_REQUIRED_CONTEXT = (
    ("EUR", MarketContextKind.POLICY_RATE_CHANGE),
    ("GBP", MarketContextKind.POLICY_RATE_CHANGE),
    ("USD", MarketContextKind.CENTRAL_BANK_DECISION),
)


class FinalSupportVerificationError(ValueError):
    """Independent source replay or reconciliation failed closed."""


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FinalSupportVerificationError(f"{name} must be non-empty text")
    return value.strip()


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FinalSupportVerificationError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise FinalSupportVerificationError(f"{name} cannot be negative")
    return result


def _mapping(value: Any, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FinalSupportVerificationError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any, name: str = "sequence") -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise FinalSupportVerificationError(f"{name} must be a sequence")
    return value


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value, name)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise FinalSupportVerificationError(f"{name} must be lowercase SHA-256")
    return text


def _float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FinalSupportVerificationError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise FinalSupportVerificationError(f"{name} must be finite")
    return result


def _counter(value: Mapping[str, Any], name: str) -> dict[str, int]:
    result = {
        _required_text(str(key), f"{name} key"): _nonnegative_int(
            count, f"{name}[{key}]"
        )
        for key, count in value.items()
    }
    return dict(sorted(result.items()))


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    index = max(0, math.ceil(len(ordered) * probability) - 1)
    return round(ordered[index], 12)


_QUANTILE_KEYS = ("p00", "p25", "p50", "p75", "p95", "p99", "p100")


def _quantiles(values: Sequence[float]) -> dict[str, float]:
    return {
        "p00": _quantile(values, 0.0),
        "p25": _quantile(values, 0.25),
        "p50": _quantile(values, 0.5),
        "p75": _quantile(values, 0.75),
        "p95": _quantile(values, 0.95),
        "p99": _quantile(values, 0.99),
        "p100": _quantile(values, 1.0),
    }


def _digest(parts: Iterable[bytes]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(struct.pack("!Q", len(part)))
        digest.update(part)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class FinalSupportPartitionReplayV1:
    """Observed lower-level facts for one independently reread partition."""

    partition_id: str
    symbol: str
    period: str
    artifact_sha256: str
    row_count: int
    coverage_start_ns: int
    coverage_end_ns: int
    first_timestamp_ms: int
    last_timestamp_ms: int
    in_requested_domain_row_count: int
    outside_requested_domain_row_count: int
    row_identity_digest: str
    replay_id: str = ""
    schema_version: str = FINAL_SUPPORT_PARTITION_REPLAY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FINAL_SUPPORT_PARTITION_REPLAY_SCHEMA_VERSION:
            raise FinalSupportVerificationError(
                "unsupported final-support partition replay schema"
            )
        for name in ("partition_id", "symbol", "period"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "artifact_sha256",
            _sha256(self.artifact_sha256, "artifact_sha256"),
        )
        for name in (
            "row_count",
            "in_requested_domain_row_count",
            "outside_requested_domain_row_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.row_count != (
            self.in_requested_domain_row_count
            + self.outside_requested_domain_row_count
        ):
            raise FinalSupportVerificationError(
                "partition requested-domain counts do not reconcile"
            )
        for name in (
            "coverage_start_ns",
            "coverage_end_ns",
            "first_timestamp_ms",
            "last_timestamp_ms",
        ):
            object.__setattr__(
                self, name, _strict_int(getattr(self, name), name)
            )
        if self.coverage_end_ns <= self.coverage_start_ns:
            raise FinalSupportVerificationError("partition coverage is empty")
        if self.last_timestamp_ms < self.first_timestamp_ms:
            raise FinalSupportVerificationError("partition timestamps regress")
        object.__setattr__(
            self,
            "row_identity_digest",
            _sha256(self.row_identity_digest, "row_identity_digest"),
        )
        expected = _stable_id("final-support-partition-replay", self.payload())
        if self.replay_id and self.replay_id != expected:
            raise FinalSupportVerificationError(
                "partition replay identity differs"
            )
        object.__setattr__(self, "replay_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "partition_id": self.partition_id,
            "symbol": self.symbol,
            "period": self.period,
            "artifact_sha256": self.artifact_sha256,
            "row_count": self.row_count,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "first_timestamp_ms": self.first_timestamp_ms,
            "last_timestamp_ms": self.last_timestamp_ms,
            "in_requested_domain_row_count": self.in_requested_domain_row_count,
            "outside_requested_domain_row_count": (
                self.outside_requested_domain_row_count
            ),
            "row_identity_digest": self.row_identity_digest,
            "row_identity_policy": ROW_IDENTITY_POLICY,
            "domain_policy": ROW_DOMAIN_POLICY,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "replay_id": self.replay_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FinalSupportPartitionReplayV1:
        if data.get("row_identity_policy") != ROW_IDENTITY_POLICY:
            raise FinalSupportVerificationError("row identity policy differs")
        if data.get("domain_policy") != ROW_DOMAIN_POLICY:
            raise FinalSupportVerificationError("row domain policy differs")
        return cls(
            partition_id=str(data.get("partition_id", "")),
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            artifact_sha256=str(data.get("artifact_sha256", "")),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            coverage_start_ns=_strict_int(
                data.get("coverage_start_ns"), "coverage_start_ns"
            ),
            coverage_end_ns=_strict_int(
                data.get("coverage_end_ns"), "coverage_end_ns"
            ),
            first_timestamp_ms=_strict_int(
                data.get("first_timestamp_ms"), "first_timestamp_ms"
            ),
            last_timestamp_ms=_strict_int(
                data.get("last_timestamp_ms"), "last_timestamp_ms"
            ),
            in_requested_domain_row_count=_strict_int(
                data.get("in_requested_domain_row_count"),
                "in_requested_domain_row_count",
            ),
            outside_requested_domain_row_count=_strict_int(
                data.get("outside_requested_domain_row_count"),
                "outside_requested_domain_row_count",
            ),
            row_identity_digest=str(data.get("row_identity_digest", "")),
            replay_id=str(data.get("replay_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FinalSupportWindowVerificationV1:
    """One terminal decision independently reconstructed from raw facts."""

    start_ns: int
    end_ns: int
    plan_id: str
    plan_shard_id: str
    claimed_support_id: str
    status: str
    core_event_counts: Mapping[str, int]
    input_event_counts: Mapping[str, int]
    core_row_identity_digest: str
    input_anchor_identity_digest: str
    alignment_source_event_digest: str
    common_exact_core_timestamp_count: int
    bounded_nearest_core_timestamp_count: int
    bounded_nearest_core_stale_timestamp_count: int
    bounded_nearest_core_maximum_age_ns: int
    bounded_nearest_core_p95_age_ns: int
    selected_cross_series_alignment: str
    recommended_cross_series_event_time_ns: int | None
    feed_epoch_label: str
    feed_epoch_assignment_ids: tuple[str, ...]
    transition_scenario_ids: tuple[str, ...]
    session: str
    event_state: str
    cftc_query_status: str
    cftc_conditioning_mode: str
    modeled_missing_event_count: int
    candidate_amplification: float
    split_depth: int
    member_count: int
    workflow_task_count: int
    refusal_code: str | None = None
    refusal_reason: str | None = None
    verification_id: str = ""
    schema_version: str = FINAL_SUPPORT_WINDOW_VERIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != FINAL_SUPPORT_WINDOW_VERIFICATION_SCHEMA_VERSION
        ):
            raise FinalSupportVerificationError(
                "unsupported final-support window verification schema"
            )
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if end <= start:
            raise FinalSupportVerificationError(
                "verified support window is empty"
            )
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        for name in ("plan_id", "plan_shard_id", "claimed_support_id"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if self.status not in {"executable", "empty", "refused"}:
            raise FinalSupportVerificationError(
                "verified terminal status is invalid"
            )
        core = _counter(self.core_event_counts, "core_event_counts")
        inputs = _counter(self.input_event_counts, "input_event_counts")
        if (
            tuple(core) != RECONSTRUCTION_SYMBOLS
            or tuple(inputs) != RECONSTRUCTION_SYMBOLS
        ):
            raise FinalSupportVerificationError(
                "verified source triangle differs"
            )
        if any(
            core[symbol] > inputs[symbol] for symbol in RECONSTRUCTION_SYMBOLS
        ):
            raise FinalSupportVerificationError(
                "verified core counts exceed inputs"
            )
        object.__setattr__(self, "core_event_counts", core)
        object.__setattr__(self, "input_event_counts", inputs)
        for name in (
            "core_row_identity_digest",
            "input_anchor_identity_digest",
            "alignment_source_event_digest",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        for name in (
            "common_exact_core_timestamp_count",
            "bounded_nearest_core_timestamp_count",
            "bounded_nearest_core_stale_timestamp_count",
            "bounded_nearest_core_maximum_age_ns",
            "bounded_nearest_core_p95_age_ns",
            "modeled_missing_event_count",
            "split_depth",
            "member_count",
            "workflow_task_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if (
            self.bounded_nearest_core_stale_timestamp_count
            > self.bounded_nearest_core_timestamp_count
            or self.bounded_nearest_core_p95_age_ns
            > self.bounded_nearest_core_maximum_age_ns
            or self.common_exact_core_timestamp_count > min(core.values())
            or self.bounded_nearest_core_timestamp_count > sum(core.values())
        ):
            raise FinalSupportVerificationError(
                "verified alignment facts differ"
            )
        if self.selected_cross_series_alignment not in {
            "exact_event_sequence",
            "nearest_prior_bounded",
            "unavailable",
        }:
            raise FinalSupportVerificationError(
                "verified alignment policy is invalid"
            )
        recommended = self.recommended_cross_series_event_time_ns
        if recommended is not None:
            recommended = _strict_int(
                recommended, "recommended_cross_series_event_time_ns"
            )
            if not start <= recommended < end:
                raise FinalSupportVerificationError(
                    "verified alignment recommendation is outside the window"
                )
        if (self.selected_cross_series_alignment == "unavailable") != (
            recommended is None
        ):
            raise FinalSupportVerificationError(
                "verified alignment recommendation differs from policy"
            )
        object.__setattr__(
            self, "recommended_cross_series_event_time_ns", recommended
        )
        for name in (
            "feed_epoch_label",
            "session",
            "event_state",
            "cftc_query_status",
            "cftc_conditioning_mode",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        for name in ("feed_epoch_assignment_ids", "transition_scenario_ids"):
            values = tuple(
                sorted(
                    _required_text(value, name) for value in getattr(self, name)
                )
            )
            if len(values) != len(set(values)):
                raise FinalSupportVerificationError(
                    f"{name} contains duplicates"
                )
            object.__setattr__(self, name, values)
        amplification = _float(
            self.candidate_amplification, "candidate_amplification"
        )
        if amplification < 0.0:
            raise FinalSupportVerificationError(
                "candidate amplification is negative"
            )
        object.__setattr__(
            self, "candidate_amplification", round(amplification, 12)
        )
        if self.status == "refused":
            object.__setattr__(
                self,
                "refusal_code",
                _required_text(self.refusal_code, "refusal_code"),
            )
            object.__setattr__(
                self,
                "refusal_reason",
                _required_text(self.refusal_reason, "refusal_reason"),
            )
            if (
                self.member_count
                or self.workflow_task_count
                or self.modeled_missing_event_count
                or self.candidate_amplification
            ):
                raise FinalSupportVerificationError(
                    "refused window contains work"
                )
        elif self.refusal_code is not None or self.refusal_reason is not None:
            raise FinalSupportVerificationError(
                "non-refused window has refusal metadata"
            )
        if self.status == "empty" and (
            any(core.values())
            or self.selected_cross_series_alignment != "unavailable"
            or self.member_count
            or self.workflow_task_count
            or self.modeled_missing_event_count
            or self.candidate_amplification
        ):
            raise FinalSupportVerificationError(
                "empty window contains source support or work"
            )
        if self.status == "executable" and (
            not self.member_count
            or self.workflow_task_count != self.member_count
            or not all(core.values())
            or not all(inputs[symbol] >= 2 for symbol in RECONSTRUCTION_SYMBOLS)
            or self.selected_cross_series_alignment == "unavailable"
        ):
            raise FinalSupportVerificationError(
                "executable window member/task rectangle differs"
            )
        expected = _stable_id(
            "final-support-window-verification", self.payload()
        )
        if self.verification_id and self.verification_id != expected:
            raise FinalSupportVerificationError(
                "window verification identity differs"
            )
        object.__setattr__(self, "verification_id", expected)

    @property
    def duration_ns(self) -> int:
        return self.end_ns - self.start_ns

    @property
    def has_valid_common_data(self) -> bool:
        return (
            all(self.core_event_counts.values())
            and all(
                self.input_event_counts[symbol] >= 2
                for symbol in RECONSTRUCTION_SYMBOLS
            )
            and self.selected_cross_series_alignment != "unavailable"
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "plan_id": self.plan_id,
            "plan_shard_id": self.plan_shard_id,
            "claimed_support_id": self.claimed_support_id,
            "status": self.status,
            "core_event_counts": dict(self.core_event_counts),
            "input_event_counts": dict(self.input_event_counts),
            "core_row_identity_digest": self.core_row_identity_digest,
            "input_anchor_identity_digest": self.input_anchor_identity_digest,
            "alignment_source_event_digest": self.alignment_source_event_digest,
            "common_exact_core_timestamp_count": (
                self.common_exact_core_timestamp_count
            ),
            "bounded_nearest_core_timestamp_count": (
                self.bounded_nearest_core_timestamp_count
            ),
            "bounded_nearest_core_stale_timestamp_count": (
                self.bounded_nearest_core_stale_timestamp_count
            ),
            "bounded_nearest_core_maximum_age_ns": (
                self.bounded_nearest_core_maximum_age_ns
            ),
            "bounded_nearest_core_p95_age_ns": (
                self.bounded_nearest_core_p95_age_ns
            ),
            "selected_cross_series_alignment": self.selected_cross_series_alignment,
            "recommended_cross_series_event_time_ns": (
                self.recommended_cross_series_event_time_ns
            ),
            "feed_epoch_label": self.feed_epoch_label,
            "feed_epoch_assignment_ids": list(self.feed_epoch_assignment_ids),
            "transition_scenario_ids": list(self.transition_scenario_ids),
            "session": self.session,
            "event_state": self.event_state,
            "cftc_query_status": self.cftc_query_status,
            "cftc_conditioning_mode": self.cftc_conditioning_mode,
            "modeled_missing_event_count": self.modeled_missing_event_count,
            "candidate_amplification": self.candidate_amplification,
            "split_depth": self.split_depth,
            "member_count": self.member_count,
            "workflow_task_count": self.workflow_task_count,
            "refusal_code": self.refusal_code,
            "refusal_reason": self.refusal_reason,
            "row_identity_policy": ROW_IDENTITY_POLICY,
            "alignment_event_policy": ALIGNMENT_EVENT_POLICY,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "verification_id": self.verification_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FinalSupportWindowVerificationV1:
        if data.get("row_identity_policy") != ROW_IDENTITY_POLICY:
            raise FinalSupportVerificationError(
                "window row identity policy differs"
            )
        if data.get("alignment_event_policy") != ALIGNMENT_EVENT_POLICY:
            raise FinalSupportVerificationError(
                "alignment event policy differs"
            )
        return cls(
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            plan_id=str(data.get("plan_id", "")),
            plan_shard_id=str(data.get("plan_shard_id", "")),
            claimed_support_id=str(data.get("claimed_support_id", "")),
            status=str(data.get("status", "")),
            core_event_counts={
                str(key): _strict_int(value, f"core_event_counts[{key}]")
                for key, value in _mapping(
                    data.get("core_event_counts")
                ).items()
            },
            input_event_counts={
                str(key): _strict_int(value, f"input_event_counts[{key}]")
                for key, value in _mapping(
                    data.get("input_event_counts")
                ).items()
            },
            core_row_identity_digest=str(
                data.get("core_row_identity_digest", "")
            ),
            input_anchor_identity_digest=str(
                data.get("input_anchor_identity_digest", "")
            ),
            alignment_source_event_digest=str(
                data.get("alignment_source_event_digest", "")
            ),
            common_exact_core_timestamp_count=_strict_int(
                data.get("common_exact_core_timestamp_count"),
                "common_exact_core_timestamp_count",
            ),
            bounded_nearest_core_timestamp_count=_strict_int(
                data.get("bounded_nearest_core_timestamp_count"),
                "bounded_nearest_core_timestamp_count",
            ),
            bounded_nearest_core_stale_timestamp_count=_strict_int(
                data.get("bounded_nearest_core_stale_timestamp_count"),
                "bounded_nearest_core_stale_timestamp_count",
            ),
            bounded_nearest_core_maximum_age_ns=_strict_int(
                data.get("bounded_nearest_core_maximum_age_ns"),
                "bounded_nearest_core_maximum_age_ns",
            ),
            bounded_nearest_core_p95_age_ns=_strict_int(
                data.get("bounded_nearest_core_p95_age_ns"),
                "bounded_nearest_core_p95_age_ns",
            ),
            selected_cross_series_alignment=str(
                data.get("selected_cross_series_alignment", "")
            ),
            recommended_cross_series_event_time_ns=(
                None
                if data.get("recommended_cross_series_event_time_ns") is None
                else _strict_int(
                    data.get("recommended_cross_series_event_time_ns"),
                    "recommended_cross_series_event_time_ns",
                )
            ),
            feed_epoch_label=str(data.get("feed_epoch_label", "")),
            feed_epoch_assignment_ids=tuple(
                str(value)
                for value in _sequence(data.get("feed_epoch_assignment_ids"))
            ),
            transition_scenario_ids=tuple(
                str(value)
                for value in _sequence(data.get("transition_scenario_ids"))
            ),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            cftc_query_status=str(data.get("cftc_query_status", "")),
            cftc_conditioning_mode=str(data.get("cftc_conditioning_mode", "")),
            modeled_missing_event_count=_strict_int(
                data.get("modeled_missing_event_count"),
                "modeled_missing_event_count",
            ),
            candidate_amplification=_float(
                data.get("candidate_amplification"), "candidate_amplification"
            ),
            split_depth=_strict_int(data.get("split_depth"), "split_depth"),
            member_count=_strict_int(data.get("member_count"), "member_count"),
            workflow_task_count=_strict_int(
                data.get("workflow_task_count"), "workflow_task_count"
            ),
            refusal_code=(
                None
                if data.get("refusal_code") is None
                else str(data.get("refusal_code"))
            ),
            refusal_reason=(
                None
                if data.get("refusal_reason") is None
                else str(data.get("refusal_reason"))
            ),
            verification_id=str(data.get("verification_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FinalSupportCensusV1:
    """Content-addressed full support census derived from verified windows."""

    window_count: int
    total_duration_ns: int
    duration_counts_ns: Mapping[str, int]
    terminal_counts: Mapping[str, int]
    alignment_counts: Mapping[str, int]
    alignment_age_quantiles_ns: Mapping[str, float]
    feed_epoch_counts: Mapping[str, int]
    transition_counts: Mapping[str, int]
    session_counts: Mapping[str, int]
    event_state_counts: Mapping[str, int]
    cftc_mode_counts: Mapping[str, int]
    modeled_deficit_quantiles: Mapping[str, float]
    candidate_amplification_quantiles: Mapping[str, float]
    split_depth_counts: Mapping[str, int]
    minimum_window_size_ns: int
    maximum_window_size_ns: int
    refusal_reason_counts_by_era: Mapping[str, int]
    valid_common_data_implementation_refusal_count: int
    census_id: str = ""
    schema_version: str = FINAL_SUPPORT_CENSUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FINAL_SUPPORT_CENSUS_SCHEMA_VERSION:
            raise FinalSupportVerificationError(
                "unsupported final support census"
            )
        object.__setattr__(
            self,
            "window_count",
            _nonnegative_int(self.window_count, "window_count"),
        )
        object.__setattr__(
            self,
            "total_duration_ns",
            _nonnegative_int(self.total_duration_ns, "total_duration_ns"),
        )
        for name in (
            "duration_counts_ns",
            "terminal_counts",
            "alignment_counts",
            "feed_epoch_counts",
            "transition_counts",
            "session_counts",
            "event_state_counts",
            "cftc_mode_counts",
            "split_depth_counts",
            "refusal_reason_counts_by_era",
        ):
            object.__setattr__(self, name, _counter(getattr(self, name), name))
        if sum(self.terminal_counts.values()) != self.window_count:
            raise FinalSupportVerificationError("census terminal counts differ")
        if not set(self.terminal_counts).issubset(
            {"executable", "empty", "refused"}
        ):
            raise FinalSupportVerificationError(
                "census contains a non-planning terminal state"
            )
        if sum(self.duration_counts_ns.values()) != self.window_count:
            raise FinalSupportVerificationError("census duration counts differ")
        try:
            duration_total = sum(
                _nonnegative_int(int(duration), "duration_counts_ns key")
                * count
                for duration, count in self.duration_counts_ns.items()
            )
        except ValueError as err:
            raise FinalSupportVerificationError(
                "census duration key is not an integer"
            ) from err
        if duration_total != self.total_duration_ns:
            raise FinalSupportVerificationError("census duration total differs")
        if (
            not set(self.alignment_counts).issubset(
                {"exact_event_sequence", "nearest_prior_bounded", "unavailable"}
            )
            or sum(self.alignment_counts.values()) != self.window_count
        ):
            raise FinalSupportVerificationError(
                "census alignment counts differ"
            )
        for name in (
            "feed_epoch_counts",
            "session_counts",
            "event_state_counts",
            "cftc_mode_counts",
            "split_depth_counts",
        ):
            if sum(getattr(self, name).values()) != self.window_count:
                raise FinalSupportVerificationError(f"census {name} differ")
        refused_count = self.terminal_counts.get("refused", 0)
        if sum(self.refusal_reason_counts_by_era.values()) != refused_count:
            raise FinalSupportVerificationError(
                "census refusal reasons differ from terminal refusals"
            )
        for name in (
            "alignment_age_quantiles_ns",
            "modeled_deficit_quantiles",
            "candidate_amplification_quantiles",
        ):
            supplied = {
                _required_text(str(key), f"{name} key"): round(
                    _float(value, f"{name}[{key}]"), 12
                )
                for key, value in getattr(self, name).items()
            }
            if set(supplied) != set(_QUANTILE_KEYS):
                raise FinalSupportVerificationError(f"{name} keys differ")
            values = {key: supplied[key] for key in _QUANTILE_KEYS}
            if tuple(values.values()) != tuple(sorted(values.values())):
                raise FinalSupportVerificationError(f"{name} is not monotone")
            object.__setattr__(self, name, values)
        minimum = _nonnegative_int(
            self.minimum_window_size_ns, "minimum_window_size_ns"
        )
        maximum = _nonnegative_int(
            self.maximum_window_size_ns, "maximum_window_size_ns"
        )
        if self.window_count and (minimum <= 0 or maximum < minimum):
            raise FinalSupportVerificationError("census window sizes differ")
        object.__setattr__(self, "minimum_window_size_ns", minimum)
        object.__setattr__(self, "maximum_window_size_ns", maximum)
        refusals = _nonnegative_int(
            self.valid_common_data_implementation_refusal_count,
            "valid_common_data_implementation_refusal_count",
        )
        object.__setattr__(
            self, "valid_common_data_implementation_refusal_count", refusals
        )
        expected = _stable_id("final-support-census", self.payload())
        if self.census_id and self.census_id != expected:
            raise FinalSupportVerificationError(
                "support census identity differs"
            )
        object.__setattr__(self, "census_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_count": self.window_count,
            "total_duration_ns": self.total_duration_ns,
            "duration_counts_ns": dict(self.duration_counts_ns),
            "terminal_counts": dict(self.terminal_counts),
            "alignment_counts": dict(self.alignment_counts),
            "alignment_age_quantiles_ns": dict(self.alignment_age_quantiles_ns),
            "feed_epoch_counts": dict(self.feed_epoch_counts),
            "transition_counts": dict(self.transition_counts),
            "session_counts": dict(self.session_counts),
            "event_state_counts": dict(self.event_state_counts),
            "cftc_mode_counts": dict(self.cftc_mode_counts),
            "modeled_deficit_quantiles": dict(self.modeled_deficit_quantiles),
            "candidate_amplification_quantiles": dict(
                self.candidate_amplification_quantiles
            ),
            "split_depth_counts": dict(self.split_depth_counts),
            "minimum_window_size_ns": self.minimum_window_size_ns,
            "maximum_window_size_ns": self.maximum_window_size_ns,
            "refusal_reason_counts_by_era": dict(
                self.refusal_reason_counts_by_era
            ),
            "valid_common_data_implementation_refusal_count": (
                self.valid_common_data_implementation_refusal_count
            ),
            "duration_semantics": "strict-half-open-end-minus-start-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "census_id": self.census_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FinalSupportCensusV1:
        if (
            data.get("duration_semantics")
            != "strict-half-open-end-minus-start-v1"
        ):
            raise FinalSupportVerificationError(
                "census duration semantics differ"
            )
        return cls(
            window_count=_strict_int(data.get("window_count"), "window_count"),
            total_duration_ns=_strict_int(
                data.get("total_duration_ns"), "total_duration_ns"
            ),
            duration_counts_ns=_mapping(data.get("duration_counts_ns")),
            terminal_counts=_mapping(data.get("terminal_counts")),
            alignment_counts=_mapping(data.get("alignment_counts")),
            alignment_age_quantiles_ns=_mapping(
                data.get("alignment_age_quantiles_ns")
            ),
            feed_epoch_counts=_mapping(data.get("feed_epoch_counts")),
            transition_counts=_mapping(data.get("transition_counts")),
            session_counts=_mapping(data.get("session_counts")),
            event_state_counts=_mapping(data.get("event_state_counts")),
            cftc_mode_counts=_mapping(data.get("cftc_mode_counts")),
            modeled_deficit_quantiles=_mapping(
                data.get("modeled_deficit_quantiles")
            ),
            candidate_amplification_quantiles=_mapping(
                data.get("candidate_amplification_quantiles")
            ),
            split_depth_counts=_mapping(data.get("split_depth_counts")),
            minimum_window_size_ns=_strict_int(
                data.get("minimum_window_size_ns"), "minimum_window_size_ns"
            ),
            maximum_window_size_ns=_strict_int(
                data.get("maximum_window_size_ns"), "maximum_window_size_ns"
            ),
            refusal_reason_counts_by_era=_mapping(
                data.get("refusal_reason_counts_by_era")
            ),
            valid_common_data_implementation_refusal_count=_strict_int(
                data.get("valid_common_data_implementation_refusal_count"),
                "valid_common_data_implementation_refusal_count",
            ),
            census_id=str(data.get("census_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def build_final_support_census(
    windows: Sequence[FinalSupportWindowVerificationV1],
) -> FinalSupportCensusV1:
    """Aggregate one exact census from independently verified windows."""
    ordered = tuple(sorted(windows, key=lambda item: item.start_ns))
    durations = [item.duration_ns for item in ordered]
    refusal_by_era: Counter[str] = Counter()
    for item in ordered:
        if item.status == "refused":
            refusal_by_era[
                f"{item.feed_epoch_label}|{item.refusal_code or 'unclassified'}"
            ] += 1
    implementation_codes = {
        ReconstructionPlanRefusalCode.FEED_EPOCH_UNSUPPORTED.value,
        ReconstructionPlanRefusalCode.MARKET_CONTEXT_UNSUPPORTED.value,
        ReconstructionPlanRefusalCode.CFTC_POSITIONING_UNSUPPORTED.value,
        ReconstructionPlanRefusalCode.INFORMATION_LEAKAGE.value,
    }
    alignment_ages = [
        (
            0.0
            if item.selected_cross_series_alignment == "exact_event_sequence"
            else float(item.bounded_nearest_core_maximum_age_ns)
        )
        for item in ordered
        if item.selected_cross_series_alignment
        in {"exact_event_sequence", "nearest_prior_bounded"}
    ]
    return FinalSupportCensusV1(
        window_count=len(ordered),
        total_duration_ns=sum(durations),
        duration_counts_ns=Counter(str(value) for value in durations),
        terminal_counts=Counter(item.status for item in ordered),
        alignment_counts=Counter(
            item.selected_cross_series_alignment for item in ordered
        ),
        alignment_age_quantiles_ns=_quantiles(alignment_ages),
        feed_epoch_counts=Counter(item.feed_epoch_label for item in ordered),
        transition_counts=Counter(
            scenario
            for item in ordered
            for scenario in item.transition_scenario_ids
        ),
        session_counts=Counter(item.session for item in ordered),
        event_state_counts=Counter(item.event_state for item in ordered),
        cftc_mode_counts=Counter(
            item.cftc_conditioning_mode for item in ordered
        ),
        modeled_deficit_quantiles=_quantiles(
            [float(item.modeled_missing_event_count) for item in ordered]
        ),
        candidate_amplification_quantiles=_quantiles(
            [item.candidate_amplification for item in ordered]
        ),
        split_depth_counts=Counter(str(item.split_depth) for item in ordered),
        minimum_window_size_ns=min(durations, default=0),
        maximum_window_size_ns=max(durations, default=0),
        refusal_reason_counts_by_era=refusal_by_era,
        valid_common_data_implementation_refusal_count=sum(
            item.status == "refused"
            and item.has_valid_common_data
            and item.refusal_code in implementation_codes
            for item in ordered
        ),
    )


@dataclass(frozen=True, slots=True)
class FinalSupportVerificationShardV1:
    """Bounded verifier output for one immutable plan shard."""

    plan_set_id: str
    plan_shard_id: str
    plan_id: str
    release_candidate_id: str
    source_inventory_id: str
    claimed_support_map_id: str
    requested_start_ns: int
    requested_end_ns: int
    partition_replays: tuple[FinalSupportPartitionReplayV1, ...]
    windows: tuple[FinalSupportWindowVerificationV1, ...]
    census: FinalSupportCensusV1
    verifier_id: str = INDEPENDENT_VERIFIER_ID
    verification_shard_id: str = ""
    schema_version: str = FINAL_SUPPORT_VERIFICATION_SHARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != FINAL_SUPPORT_VERIFICATION_SHARD_SCHEMA_VERSION
        ):
            raise FinalSupportVerificationError(
                "unsupported final support verification shard"
            )
        for name in (
            "plan_set_id",
            "plan_shard_id",
            "plan_id",
            "release_candidate_id",
            "source_inventory_id",
            "claimed_support_map_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if self.verifier_id != INDEPENDENT_VERIFIER_ID:
            raise FinalSupportVerificationError(
                "support verifier identity differs"
            )
        start = _strict_int(self.requested_start_ns, "requested_start_ns")
        end = _strict_int(self.requested_end_ns, "requested_end_ns")
        if end <= start:
            raise FinalSupportVerificationError(
                "verification shard bounds are empty"
            )
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        partitions = tuple(
            sorted(
                self.partition_replays,
                key=lambda item: (item.period, item.symbol),
            )
        )
        if (
            not partitions
            or len(partitions) > MAX_FINAL_SUPPORT_PARTITIONS_PER_SHARD
            or len({item.partition_id for item in partitions})
            != len(partitions)
        ):
            raise FinalSupportVerificationError(
                "verification shard partition replays are invalid"
            )
        object.__setattr__(self, "partition_replays", partitions)
        windows = tuple(sorted(self.windows, key=lambda item: item.start_ns))
        if (
            not windows
            or len(windows) > MAX_FINAL_SUPPORT_WINDOWS_PER_SHARD
            or len({item.verification_id for item in windows}) != len(windows)
        ):
            raise FinalSupportVerificationError(
                "verification shard windows are invalid"
            )
        if windows[0].start_ns != start or windows[-1].end_ns != end:
            raise FinalSupportVerificationError(
                "verification shard bounds differ from windows"
            )
        for previous, current in pairwise(windows):
            if previous.end_ns != current.start_ns:
                raise FinalSupportVerificationError(
                    "verification shard windows are not contiguous"
                )
        if any(
            item.plan_id != self.plan_id
            or item.plan_shard_id != self.plan_shard_id
            for item in windows
        ):
            raise FinalSupportVerificationError(
                "verified window plan identity differs from shard"
            )
        object.__setattr__(self, "windows", windows)
        expected_census = build_final_support_census(windows)
        if self.census != expected_census:
            raise FinalSupportVerificationError(
                "verification shard census differs from verified windows"
            )
        if self.census.valid_common_data_implementation_refusal_count:
            raise FinalSupportVerificationError(
                "valid-common-data implementation refusals remain"
            )
        expected = _stable_id(
            "final-support-verification-shard", self.payload()
        )
        if (
            self.verification_shard_id
            and self.verification_shard_id != expected
        ):
            raise FinalSupportVerificationError(
                "support verification shard identity differs"
            )
        object.__setattr__(self, "verification_shard_id", expected)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_FINAL_SUPPORT_ARTIFACT_BYTES
        ):
            raise FinalSupportVerificationError(
                "support verification shard exceeds size limit"
            )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "plan_set_id": self.plan_set_id,
            "plan_shard_id": self.plan_shard_id,
            "plan_id": self.plan_id,
            "release_candidate_id": self.release_candidate_id,
            "source_inventory_id": self.source_inventory_id,
            "claimed_support_map_id": self.claimed_support_map_id,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "partition_replays": [
                item.to_dict() for item in self.partition_replays
            ],
            "windows": [item.to_dict() for item in self.windows],
            "census": self.census.to_dict(),
            "verifier_id": self.verifier_id,
            "decision_policy": INDEPENDENT_DECISION_POLICY,
            "operational_failure_is_planning_state": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "verification_shard_id": self.verification_shard_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FinalSupportVerificationShardV1:
        if data.get("decision_policy") != INDEPENDENT_DECISION_POLICY:
            raise FinalSupportVerificationError(
                "verifier decision policy differs"
            )
        if data.get("operational_failure_is_planning_state") is not False:
            raise FinalSupportVerificationError(
                "operational failure was relabeled as a planning state"
            )
        return cls(
            plan_set_id=str(data.get("plan_set_id", "")),
            plan_shard_id=str(data.get("plan_shard_id", "")),
            plan_id=str(data.get("plan_id", "")),
            release_candidate_id=str(data.get("release_candidate_id", "")),
            source_inventory_id=str(data.get("source_inventory_id", "")),
            claimed_support_map_id=str(data.get("claimed_support_map_id", "")),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            partition_replays=tuple(
                FinalSupportPartitionReplayV1.from_dict(_mapping(item))
                for item in _sequence(data.get("partition_replays"))
            ),
            windows=tuple(
                FinalSupportWindowVerificationV1.from_dict(_mapping(item))
                for item in _sequence(data.get("windows"))
            ),
            census=FinalSupportCensusV1.from_dict(_mapping(data.get("census"))),
            verifier_id=str(data.get("verifier_id", "")),
            verification_shard_id=str(data.get("verification_shard_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FinalAdaptiveSupportMapIndexV1:
    """Candidate-bound immutable root over independently verified shards."""

    plan_set_ref: ArtifactRef
    claimed_support_ref: ArtifactRef
    release_candidate_ref: ArtifactRef
    verification_shard_refs: tuple[ArtifactRef, ...]
    selected_engine_ids: tuple[str, ...]
    selected_scenario_ids: tuple[str, ...]
    source_cutoff_ns: int
    requested_start_ns: int
    requested_end_ns: int
    census: FinalSupportCensusV1
    status: str = "qualified"
    verifier_id: str = INDEPENDENT_VERIFIER_ID
    final_support_map_id: str = ""
    schema_version: str = FINAL_ADAPTIVE_SUPPORT_MAP_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != FINAL_ADAPTIVE_SUPPORT_MAP_INDEX_SCHEMA_VERSION
        ):
            raise FinalSupportVerificationError(
                "unsupported final adaptive support-map index"
            )
        if self.plan_set_ref.kind != "reconstruction_plan_set_v1":
            raise FinalSupportVerificationError(
                "final index plan-set kind differs"
            )
        if self.claimed_support_ref.kind not in {
            "reconstruction_plan_support_map_v1",
            "reconstruction_plan_support_map_index_v2",
        }:
            raise FinalSupportVerificationError(
                "final index claimed-support kind differs"
            )
        if (
            self.release_candidate_ref.kind
            != "reconstruction_release_candidate_v1"
        ):
            raise FinalSupportVerificationError(
                "final index release-candidate kind differs"
            )
        plan_set_id = _required_text(
            self.plan_set_ref.metadata.get("plan_set_id"), "plan_set_id"
        )
        candidate_id = _required_text(
            self.release_candidate_ref.metadata.get("candidate_id"),
            "candidate_id",
        )
        if self.claimed_support_ref.metadata.get("plan_set_id") != plan_set_id:
            raise FinalSupportVerificationError(
                "final index support and plan-set identities differ"
            )
        start = _strict_int(self.requested_start_ns, "requested_start_ns")
        end = _strict_int(self.requested_end_ns, "requested_end_ns")
        cutoff = _strict_int(self.source_cutoff_ns, "source_cutoff_ns")
        if end <= start or end > cutoff:
            raise FinalSupportVerificationError(
                "final index bounds exceed frozen source cutoff"
            )
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        object.__setattr__(self, "source_cutoff_ns", cutoff)
        refs = tuple(
            sorted(
                self.verification_shard_refs,
                key=lambda ref: _strict_int(
                    ref.metadata.get("requested_start_ns"),
                    "verification shard start",
                ),
            )
        )
        if not refs or len(refs) > MAX_FINAL_SUPPORT_SHARDS:
            raise FinalSupportVerificationError(
                "final index verification shard count is invalid"
            )
        if len({ref.sha256 for ref in refs}) != len(refs):
            raise FinalSupportVerificationError(
                "final index contains duplicate verification shards"
            )
        previous_end: int | None = None
        counts: Counter[str] = Counter()
        for ref in refs:
            if ref.kind != FINAL_SUPPORT_VERIFICATION_SHARD_ARTIFACT_KIND:
                raise FinalSupportVerificationError(
                    "final index verification shard kind differs"
                )
            metadata = ref.metadata
            if (
                metadata.get("plan_set_id") != plan_set_id
                or metadata.get("release_candidate_id") != candidate_id
            ):
                raise FinalSupportVerificationError(
                    "final index verification shard binding differs"
                )
            shard_start = _strict_int(
                metadata.get("requested_start_ns"), "verification shard start"
            )
            shard_end = _strict_int(
                metadata.get("requested_end_ns"), "verification shard end"
            )
            if shard_end <= shard_start or (
                previous_end is not None and previous_end != shard_start
            ):
                raise FinalSupportVerificationError(
                    "final index verification shards are not contiguous"
                )
            previous_end = shard_end
            for terminal in ("executable", "empty", "refused"):
                counts[terminal] += _nonnegative_int(
                    metadata.get(f"{terminal}_window_count", 0),
                    f"{terminal}_window_count",
                )
        if (
            _strict_int(
                refs[0].metadata.get("requested_start_ns"), "first start"
            )
            != start
            or previous_end != end
        ):
            raise FinalSupportVerificationError(
                "final index bounds differ from verification shards"
            )
        object.__setattr__(self, "verification_shard_refs", refs)
        for name in ("selected_engine_ids", "selected_scenario_ids"):
            values = tuple(
                sorted(
                    _required_text(value, name) for value in getattr(self, name)
                )
            )
            if not values or len(values) != len(set(values)):
                raise FinalSupportVerificationError(f"{name} is invalid")
            object.__setattr__(self, name, values)
        claimed_engine_ids = tuple(
            sorted(
                _required_text(value, "claimed selected engine")
                for value in _sequence(
                    self.claimed_support_ref.metadata.get(
                        "selected_proposal_engine_ids"
                    ),
                    "claimed selected engine ids",
                )
            )
        )
        candidate_engine_id = _required_text(
            self.release_candidate_ref.metadata.get("selected_engine_id"),
            "candidate selected engine id",
        )
        if (
            self.selected_engine_ids != claimed_engine_ids
            or candidate_engine_id not in self.selected_engine_ids
        ):
            raise FinalSupportVerificationError(
                "final index selected engines differ from frozen evidence"
            )
        if (
            self.release_candidate_ref.metadata.get("source_cutoff_ns")
            != cutoff
        ):
            raise FinalSupportVerificationError(
                "final index source cutoff differs from release candidate"
            )
        if counts != Counter(self.census.terminal_counts):
            raise FinalSupportVerificationError(
                "final index terminal counts differ from census"
            )
        if self.census.valid_common_data_implementation_refusal_count:
            raise FinalSupportVerificationError(
                "final index contains valid-common-data implementation refusals"
            )
        if (
            self.status != "qualified"
            or self.verifier_id != INDEPENDENT_VERIFIER_ID
        ):
            raise FinalSupportVerificationError("final index is not qualified")
        expected = _stable_id("final-adaptive-support-map", self.payload())
        if self.final_support_map_id and self.final_support_map_id != expected:
            raise FinalSupportVerificationError(
                "final support-map identity differs"
            )
        object.__setattr__(self, "final_support_map_id", expected)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_FINAL_SUPPORT_ARTIFACT_BYTES
        ):
            raise FinalSupportVerificationError(
                "final support-map index exceeds size limit"
            )

    @property
    def plan_set_id(self) -> str:
        return _required_text(
            self.plan_set_ref.metadata.get("plan_set_id"), "plan_set_id"
        )

    @property
    def release_candidate_id(self) -> str:
        return _required_text(
            self.release_candidate_ref.metadata.get("candidate_id"),
            "candidate_id",
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "plan_set_ref": self.plan_set_ref.to_dict(),
            "claimed_support_ref": self.claimed_support_ref.to_dict(),
            "release_candidate_ref": self.release_candidate_ref.to_dict(),
            "verification_shard_refs": [
                item.to_dict() for item in self.verification_shard_refs
            ],
            "selected_engine_ids": list(self.selected_engine_ids),
            "selected_scenario_ids": list(self.selected_scenario_ids),
            "source_cutoff_ns": self.source_cutoff_ns,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "census": self.census.to_dict(),
            "status": self.status,
            "verifier_id": self.verifier_id,
            "decision_policy": INDEPENDENT_DECISION_POLICY,
            "scientific_nonclaim": (
                "Synthetic reconstruction is not recovered historical truth."
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "final_support_map_id": self.final_support_map_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FinalAdaptiveSupportMapIndexV1:
        if data.get("decision_policy") != INDEPENDENT_DECISION_POLICY:
            raise FinalSupportVerificationError(
                "final index decision policy differs"
            )
        if data.get("scientific_nonclaim") != (
            "Synthetic reconstruction is not recovered historical truth."
        ):
            raise FinalSupportVerificationError("final index nonclaim differs")
        return cls(
            plan_set_ref=ArtifactRef.from_dict(
                _mapping(data.get("plan_set_ref"))
            ),
            claimed_support_ref=ArtifactRef.from_dict(
                _mapping(data.get("claimed_support_ref"))
            ),
            release_candidate_ref=ArtifactRef.from_dict(
                _mapping(data.get("release_candidate_ref"))
            ),
            verification_shard_refs=tuple(
                ArtifactRef.from_dict(_mapping(item))
                for item in _sequence(data.get("verification_shard_refs"))
            ),
            selected_engine_ids=tuple(
                str(item) for item in _sequence(data.get("selected_engine_ids"))
            ),
            selected_scenario_ids=tuple(
                str(item)
                for item in _sequence(data.get("selected_scenario_ids"))
            ),
            source_cutoff_ns=_strict_int(
                data.get("source_cutoff_ns"), "source_cutoff_ns"
            ),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            census=FinalSupportCensusV1.from_dict(_mapping(data.get("census"))),
            status=str(data.get("status", "")),
            verifier_id=str(data.get("verifier_id", "")),
            final_support_map_id=str(data.get("final_support_map_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(slots=True)
class _PartitionRows:
    partition: ReconstructionSourcePartitionV1
    timestamps_ms: Any
    row_ordinals: Any


@dataclass(slots=True)
class _WindowArrays:
    timestamps_ms: Any
    source_indexes: Any
    row_ordinals: Any


@dataclass(frozen=True, slots=True)
class _AlignmentFacts:
    common_exact_count: int
    nearest_count: int
    nearest_stale_count: int
    nearest_maximum_age_ns: int
    nearest_p95_age_ns: int
    selected_alignment: str
    recommended_time_ns: int | None
    source_event_digest: str


def _ceil_ns_to_ms(value: int) -> int:
    return -(-value // 1_000_000)


def _load_support_maps(
    path: str | Path,
) -> tuple[ReconstructionPlanSupportMapV1, ...]:
    payload = _mapping(json.loads(Path(path).read_text(encoding="utf-8")))
    schema = str(payload.get("schema_version", ""))
    if schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION:
        return (read_reconstruction_plan_support_map(path),)
    if schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION:
        index = read_reconstruction_plan_support_map_index(path)
        return tuple(iter_reconstruction_plan_support_maps(index))
    raise FinalSupportVerificationError(
        "unsupported claimed support-map artifact"
    )


def _claimed_support_ref(
    path: str | Path,
    maps: Sequence[ReconstructionPlanSupportMapV1],
) -> ArtifactRef:
    payload = _mapping(json.loads(Path(path).read_text(encoding="utf-8")))
    schema = str(payload.get("schema_version", ""))
    if schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION:
        support = maps[0]
        return artifact_ref_for_file(
            path,
            kind="reconstruction_plan_support_map_v1",
            metadata={
                "support_map_id": support.support_map_id,
                "plan_set_id": support.plan_set_id,
                "window_count": len(support.windows),
                "status": support.status,
                "selected_proposal_engine_ids": list(
                    support.selected_proposal_engine_ids
                ),
            },
        )
    index = read_reconstruction_plan_support_map_index(path)
    return artifact_ref_for_file(
        path,
        kind="reconstruction_plan_support_map_index_v2",
        metadata={
            "support_map_index_id": index.support_map_index_id,
            "plan_set_id": index.plan_set_id,
            "window_count": index.window_count,
            "status": index.status,
            "selected_proposal_engine_ids": list(
                index.selected_proposal_engine_ids
            ),
        },
    )


def _scan_partition(
    partition: ReconstructionSourcePartitionV1,
    *,
    requested_start_ns: int,
    requested_end_ns: int,
) -> tuple[_PartitionRows, FinalSupportPartitionReplayV1]:
    try:
        import numpy as np  # pylint: disable=import-outside-toplevel
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
        from pyarrow import ipc  # pylint: disable=import-outside-toplevel
    except ImportError as err:
        raise RuntimeError(
            "independent support verification requires pyarrow"
        ) from err
    verify_artifact_ref(partition.artifact)
    chunks: list[Any] = []
    path = Path(partition.artifact.path)
    with pa.memory_map(str(path), "r") as source:
        reader = ipc.open_file(source)
        datetime_index = reader.schema.get_field_index("datetime")
        if datetime_index < 0 or not pa.types.is_integer(
            reader.schema.field(datetime_index).type
        ):
            raise FinalSupportVerificationError(
                f"source partition lacks integer datetime: {path}"
            )
        for batch_index in range(reader.num_record_batches):
            values = (
                reader.get_batch(batch_index)
                .column(datetime_index)
                .to_numpy(zero_copy_only=False)
            )
            if len(values):
                chunks.append(np.asarray(values, dtype=np.int64).copy())
    timestamps = (
        np.concatenate(chunks)
        if len(chunks) > 1
        else (chunks[0] if chunks else np.asarray([], dtype=np.int64))
    )
    if len(timestamps) != partition.row_count:
        raise FinalSupportVerificationError(
            "source partition row count changed"
        )
    if not len(timestamps):
        raise FinalSupportVerificationError(
            "source partition is unexpectedly empty"
        )
    observed_first = int(np.min(timestamps))
    observed_last = int(np.max(timestamps))
    if (
        observed_first != partition.first_timestamp_ms
        or observed_last != partition.last_timestamp_ms
    ):
        raise FinalSupportVerificationError(
            "source partition timestamp coverage changed"
        )
    ordinals = np.arange(len(timestamps), dtype=np.int64)
    order = np.argsort(timestamps, kind="stable")
    timestamps = timestamps[order]
    ordinals = ordinals[order]
    domain = (timestamps * 1_000_000 >= requested_start_ns) & (
        timestamps * 1_000_000 < requested_end_ns
    )
    identity_digest = _digest(
        (
            partition.partition_id.encode("utf-8"),
            np.asarray(timestamps, dtype="<i8").tobytes(),
            np.asarray(ordinals, dtype="<i8").tobytes(),
        )
    )
    replay = FinalSupportPartitionReplayV1(
        partition_id=partition.partition_id,
        symbol=partition.symbol,
        period=partition.period,
        artifact_sha256=partition.artifact.sha256,
        row_count=partition.row_count,
        coverage_start_ns=partition.coverage_start_ns,
        coverage_end_ns=partition.coverage_end_ns,
        first_timestamp_ms=observed_first,
        last_timestamp_ms=observed_last,
        in_requested_domain_row_count=int(np.count_nonzero(domain)),
        outside_requested_domain_row_count=(
            partition.row_count - int(np.count_nonzero(domain))
        ),
        row_identity_digest=identity_digest,
    )
    return (
        _PartitionRows(
            partition=partition,
            timestamps_ms=timestamps,
            row_ordinals=ordinals,
        ),
        replay,
    )


def _window_arrays(
    rows: Sequence[_PartitionRows],
    *,
    start_ns: int,
    end_ns: int,
) -> _WindowArrays:
    import numpy as np  # pylint: disable=import-outside-toplevel

    start_ms = _ceil_ns_to_ms(start_ns)
    end_ms = _ceil_ns_to_ms(end_ns)
    timestamp_chunks: list[Any] = []
    source_chunks: list[Any] = []
    ordinal_chunks: list[Any] = []
    for source_index, item in enumerate(rows):
        left = int(np.searchsorted(item.timestamps_ms, start_ms, side="left"))
        right = int(np.searchsorted(item.timestamps_ms, end_ms, side="left"))
        if right <= left:
            continue
        timestamp_chunks.append(item.timestamps_ms[left:right])
        source_chunks.append(
            np.full(right - left, source_index, dtype=np.int32)
        )
        ordinal_chunks.append(item.row_ordinals[left:right])
    if not timestamp_chunks:
        return _WindowArrays(
            timestamps_ms=np.asarray([], dtype=np.int64),
            source_indexes=np.asarray([], dtype=np.int32),
            row_ordinals=np.asarray([], dtype=np.int64),
        )
    timestamps = (
        timestamp_chunks[0]
        if len(timestamp_chunks) == 1
        else np.concatenate(timestamp_chunks)
    )
    sources = (
        source_chunks[0]
        if len(source_chunks) == 1
        else np.concatenate(source_chunks)
    )
    ordinals = (
        ordinal_chunks[0]
        if len(ordinal_chunks) == 1
        else np.concatenate(ordinal_chunks)
    )
    order = np.argsort(timestamps, kind="stable")
    return _WindowArrays(timestamps[order], sources[order], ordinals[order])


def _row_identity_digest(
    arrays_by_symbol: Mapping[str, _WindowArrays],
    sources_by_symbol: Mapping[str, Sequence[_PartitionRows]],
) -> str:
    parts: list[bytes] = []
    for symbol in RECONSTRUCTION_SYMBOLS:
        arrays = arrays_by_symbol[symbol]
        sources = sources_by_symbol[symbol]
        parts.append(symbol.encode("ascii"))
        for index in range(len(arrays.timestamps_ms)):
            source = sources[int(arrays.source_indexes[index])]
            parts.extend(
                (
                    source.partition.partition_id.encode("utf-8"),
                    struct.pack("!q", int(arrays.row_ordinals[index])),
                    struct.pack("!q", int(arrays.timestamps_ms[index])),
                )
            )
    return _digest(parts)


def _alignment_facts(
    core_by_period: Mapping[str, Mapping[str, _WindowArrays]],
    sources_by_period: Mapping[str, Mapping[str, Sequence[_PartitionRows]]],
    *,
    start_ns: int,
    end_ns: int,
    nearest_prior_max_age_ns: int,
    minimum_alignment_support: int,
) -> _AlignmentFacts:
    import numpy as np  # pylint: disable=import-outside-toplevel

    common_exact_count = 0
    nearest_count = 0
    stale_count = 0
    all_ages_ms: list[Any] = []
    candidates: list[tuple[int, int, str, int, str, str]] = []
    probe_by_period: dict[str, tuple[str, Any, Any, Any]] = {}
    for period in sorted(core_by_period):
        grouped = core_by_period[period]
        unique = [
            np.unique(grouped[symbol].timestamps_ms)
            for symbol in RECONSTRUCTION_SYMBOLS
        ]
        common = unique[0]
        for values in unique[1:]:
            common = np.intersect1d(common, values, assume_unique=True)
            if not len(common):
                break
        common_exact_count += len(common)
        exact_times = np.asarray([], dtype=np.int64)
        if len(common):
            cardinality = np.minimum.reduce(
                [
                    np.searchsorted(
                        grouped[symbol].timestamps_ms, common, side="right"
                    )
                    - np.searchsorted(
                        grouped[symbol].timestamps_ms, common, side="left"
                    )
                    for symbol in RECONSTRUCTION_SYMBOLS
                ]
            )
            exact_times = np.repeat(common, cardinality)
        if not all(
            len(grouped[symbol].timestamps_ms)
            for symbol in RECONSTRUCTION_SYMBOLS
        ):
            continue
        maximum_age_ms = nearest_prior_max_age_ns // 1_000_000
        probes: list[tuple[int, int, str, Any, Any, Any]] = []
        for probe_symbol in RECONSTRUCTION_SYMBOLS:
            probe_values = grouped[probe_symbol].timestamps_ms
            supported = np.ones(len(probe_values), dtype=bool)
            maximum_ages = np.zeros(len(probe_values), dtype=np.int64)
            for symbol in RECONSTRUCTION_SYMBOLS:
                values = grouped[symbol].timestamps_ms
                indexes = (
                    np.searchsorted(values, probe_values, side="right") - 1
                )
                valid = indexes >= 0
                ages = np.zeros(len(probe_values), dtype=np.int64)
                if np.any(valid):
                    ages[valid] = probe_values[valid] - values[indexes[valid]]
                supported &= valid & (ages <= maximum_age_ms)
                maximum_ages = np.maximum(maximum_ages, ages)
            probes.append(
                (
                    -int(np.count_nonzero(supported)),
                    len(probe_values),
                    probe_symbol,
                    probe_values,
                    supported,
                    maximum_ages,
                )
            )
        _, _, probe_symbol, probe_values, supported, ages = min(
            probes, key=lambda item: item[:3]
        )
        probe_by_period[period] = (probe_symbol, probe_values, supported, ages)
        supported_times = probe_values[supported]
        supported_ages = ages[supported]
        nearest_count += len(supported_times)
        stale_count += int(np.count_nonzero(supported_ages))
        if len(supported_ages):
            all_ages_ms.append(supported_ages)
        if len(exact_times) >= minimum_alignment_support:
            recommended = int(exact_times[len(exact_times) // 2]) * 1_000_000
            candidates.append(
                (
                    0,
                    abs(2 * recommended - (start_ns + end_ns)),
                    period,
                    recommended,
                    CrossSeriesAlignmentPolicy.EXACT_EVENT_SEQUENCE.value,
                    probe_symbol,
                )
            )
        elif len(supported_times) >= minimum_alignment_support:
            recommended = (
                int(supported_times[len(supported_times) // 2]) * 1_000_000
            )
            candidates.append(
                (
                    1,
                    abs(2 * recommended - (start_ns + end_ns)),
                    period,
                    recommended,
                    CrossSeriesAlignmentPolicy.NEAREST_PRIOR_BOUNDED.value,
                    probe_symbol,
                )
            )
    maximum_age_ns = 0
    p95_age_ns = 0
    if all_ages_ms:
        ages = np.concatenate(all_ages_ms)
        maximum_age_ns = int(np.max(ages)) * 1_000_000
        p95_age_ns = (
            int(np.sort(ages)[max(0, math.ceil(len(ages) * 0.95) - 1)])
            * 1_000_000
        )
    selected = "unavailable"
    recommended_ns: int | None = None
    digest_parts: list[bytes] = [b"unavailable"]
    if candidates:
        _, _, period, recommended_ns, selected, probe_symbol = min(candidates)
        recommended_ms = recommended_ns // 1_000_000
        grouped = core_by_period[period]
        source_groups = sources_by_period[period]
        digest_parts = [selected.encode("ascii"), period.encode("ascii")]
        for symbol in RECONSTRUCTION_SYMBOLS:
            arrays = grouped[symbol]
            if (
                selected
                == CrossSeriesAlignmentPolicy.EXACT_EVENT_SEQUENCE.value
            ):
                left = int(
                    np.searchsorted(
                        arrays.timestamps_ms, recommended_ms, side="left"
                    )
                )
                right = int(
                    np.searchsorted(
                        arrays.timestamps_ms, recommended_ms, side="right"
                    )
                )
                indexes = range(left, right)
            else:
                index = (
                    int(
                        np.searchsorted(
                            arrays.timestamps_ms, recommended_ms, side="right"
                        )
                    )
                    - 1
                )
                if index < 0:
                    raise FinalSupportVerificationError(
                        "selected nearest-prior event has no source row"
                    )
                indexes = (index,)
            digest_parts.append(symbol.encode("ascii"))
            for index in indexes:
                source = source_groups[symbol][
                    int(arrays.source_indexes[index])
                ]
                digest_parts.extend(
                    (
                        source.partition.partition_id.encode("utf-8"),
                        struct.pack("!q", int(arrays.row_ordinals[index])),
                        struct.pack("!q", int(arrays.timestamps_ms[index])),
                    )
                )
        digest_parts.append(probe_symbol.encode("ascii"))
    return _AlignmentFacts(
        common_exact_count=common_exact_count,
        nearest_count=nearest_count,
        nearest_stale_count=stale_count,
        nearest_maximum_age_ns=maximum_age_ns,
        nearest_p95_age_ns=p95_age_ns,
        selected_alignment=selected,
        recommended_time_ns=recommended_ns,
        source_event_digest=_digest(digest_parts),
    )


def _load_proposal_config(
    configuration: ReconstructionPlanConfiguration,
) -> Any | None:
    if not isinstance(configuration, ReconstructionPlanConfigurationV2):
        return None
    engine_ids = configuration.proposal_portfolio.selected_engine_ids
    if len(engine_ids) != 1:
        return None
    binding = configuration.proposal_portfolio.binding(engine_ids[0])
    verify_artifact_ref(binding.config_ref)
    payload = _mapping(
        json.loads(Path(binding.config_ref.path).read_text(encoding="utf-8"))
    )
    if engine_ids[0].startswith("histdatacom.marked-hawkes."):
        return MarkedHawkesConfigV1.from_dict(payload)
    return None


def _artifact_identity(ref: ArtifactRef) -> tuple[str, str, int | None]:
    return (ref.kind, ref.sha256, ref.size_bytes)


def _candidate_source_keys_for_bounds(
    source_partition_hashes: Mapping[str, str],
    *,
    requested_start_ns: int,
    requested_end_ns: int,
) -> set[str]:
    """Select the frozen monthly inventory intersecting one plan-set range."""
    if requested_end_ns <= requested_start_ns:
        raise FinalSupportVerificationError(
            "release-candidate source selection bounds are invalid"
        )
    start_period = datetime.fromtimestamp(
        requested_start_ns // 1_000_000_000, tz=timezone.utc
    ).strftime("%Y%m")
    end_period = datetime.fromtimestamp(
        (requested_end_ns - 1) // 1_000_000_000, tz=timezone.utc
    ).strftime("%Y%m")
    return {
        key
        for key in source_partition_hashes
        if start_period <= key.rsplit(":", 1)[-1] <= end_period
    }


def _verify_release_candidate_dependencies(
    plan: SyntheticInfillPlanV1,
    candidate: ReconstructionReleaseCandidateV1,
    configuration: ReconstructionPlanConfiguration,
) -> None:
    """Require the plan to use the scientific graph frozen by the candidate."""
    direct_roles = {
        "benchmark_corpus": "benchmark_manifest",
        "cftc_positioning": "cftc_positioning",
        "dataset_catalog": "dataset_catalog",
        "feed_epoch_definition": "feed_epochs",
        "feed_epoch_transition_policy": "feed_epoch_transition_policy",
        "market_context": "market_context",
        "observation_operator": "observation_operator",
        "observation_uncertainty_policy": "observation_uncertainty_policy",
        "powered_qualification_dossier": "powered_qualification_dossier",
        "product_selection_dossier": "hawkes_product_selection_dossier",
        "proposal_evaluation": "proposal_portfolio_evaluation",
        "reconciliation_policy": "cross_series_constraint_policy",
        "scientific_ledger": "scientific_ledger",
    }
    for dependency_name, graph_role in direct_roles.items():
        dependency_ref = candidate.dependency(dependency_name).artifact_ref
        plan_ref = plan.artifact_graph.get(graph_role)
        if plan_ref is None or _artifact_identity(
            plan_ref
        ) != _artifact_identity(dependency_ref):
            raise FinalSupportVerificationError(
                "plan scientific dependency differs from frozen release candidate: "
                f"{dependency_name}"
            )
    try:
        qualification = read_powered_qualification_dossier(
            candidate.dependency(
                "powered_qualification_dossier"
            ).artifact_ref.path
        )
        selection = read_hawkes_product_selection_dossier(
            candidate.dependency("product_selection_dossier").artifact_ref.path
        )
        evaluation = read_proposal_portfolio_evaluation(
            candidate.dependency("proposal_evaluation").artifact_ref.path
        )
        selected_model_refs = proposal_evaluation_engine_artifacts(
            evaluation, candidate.selected_engine_id
        )
    except (KeyError, OSError, TypeError, ValueError) as err:
        raise FinalSupportVerificationError(
            "release-candidate qualification graph is invalid"
        ) from err
    if (
        qualification.dossier_id
        != candidate.dependency("powered_qualification_dossier").artifact_id
        or qualification.experiment_id != candidate.experiment_id
        or qualification.evaluation_id
        != candidate.dependency("proposal_evaluation").artifact_id
        or qualification.corpus_id
        != candidate.dependency("benchmark_corpus").artifact_id
        or candidate.selected_engine_id
        not in qualification.reconstruction_eligible_engine_ids
    ):
        raise FinalSupportVerificationError(
            "release-candidate powered qualification binding differs"
        )
    qualification_input = selection.input_artifacts.get("qualification")
    if (
        selection.dossier_id
        != candidate.dependency("product_selection_dossier").artifact_id
        or selection.qualification_dossier_id != qualification.dossier_id
        or selection.selected_engine_id != candidate.selected_engine_id
        or qualification_input is None
        or _artifact_identity(qualification_input)
        != _artifact_identity(
            candidate.dependency("powered_qualification_dossier").artifact_ref
        )
    ):
        raise FinalSupportVerificationError(
            "release-candidate product selection binding differs"
        )
    selected_fit = selected_model_refs.get("fit")
    if (
        evaluation.evaluation_id
        != candidate.dependency("proposal_evaluation").artifact_id
        or evaluation.corpus_id != qualification.corpus_id
        or selected_fit is None
        or _artifact_identity(selected_fit)
        != _artifact_identity(
            candidate.dependency("selected_engine_fit").artifact_ref
        )
    ):
        raise FinalSupportVerificationError(
            "release-candidate proposal evaluation binding differs"
        )
    if not isinstance(configuration, ReconstructionPlanConfigurationV2):
        raise FinalSupportVerificationError(
            "final support verification requires candidate-bound v2 planning"
        )
    try:
        binding = configuration.proposal_portfolio.binding(
            candidate.selected_engine_id
        )
    except KeyError as err:
        raise FinalSupportVerificationError(
            "selected engine differs from frozen release candidate"
        ) from err
    if _artifact_identity(binding.config_ref) != _artifact_identity(
        candidate.dependency("selected_engine_config").artifact_ref
    ):
        raise FinalSupportVerificationError(
            "selected engine config differs from frozen release candidate"
        )
    if binding.fit_ref is None or _artifact_identity(
        binding.fit_ref
    ) != _artifact_identity(
        candidate.dependency("selected_engine_fit").artifact_ref
    ):
        raise FinalSupportVerificationError(
            "selected engine fit differs from frozen release candidate"
        )


def _independent_modeled_count(
    input_count: int,
    *,
    midpoint_ns: int,
    symbols: Sequence[str],
    information_mode: InformationMode,
    configuration: ReconstructionPlanConfiguration,
    proposal_config: Any | None,
    definition: Any,
    observation_operator: Any,
    uncertainty_policy: ObservationUncertaintyPolicyV1 | None,
    transition_policy: FeedEpochTransitionPolicyV1 | None,
) -> tuple[int, int, int]:
    interval_count = max(0, input_count - len(symbols))
    candidates = min(
        configuration.generator_config.max_events_per_interval * interval_count,
        math.floor(
            input_count
            * configuration.storage_policy.max_candidate_amplification
        ),
    )
    runtime_limit = candidates
    amplification_limit = candidates
    if isinstance(proposal_config, MarkedHawkesConfigV1):
        if (
            not isinstance(observation_operator, ObservationOperatorV1)
            or uncertainty_policy is None
        ):
            raise FinalSupportVerificationError(
                "marked-Hawkes cardinality verification lacks frozen evidence"
            )
        assignments = tuple(
            definition.assign(
                symbol=symbol, timestamp_utc_ms=midpoint_ns // 1_000_000
            )
            for symbol in symbols
        )
        labels = {str(item.label) for item in assignments}
        if (
            any(item.assignment_kind == "out_of_scope" for item in assignments)
            or len(labels) != 1
        ):
            raise FinalSupportVerificationError(
                "modeled-cardinality window lacks one qualified feed epoch"
            )
        label = next(iter(labels))
        scenario_kinds: tuple[Any | None, ...] = (
            tuple(transition_policy.scenario_order)
            if label.startswith("transition:") and transition_policy is not None
            else (None,)
        )
        retentions = tuple(
            historical_product_retention_probability(
                observation_operator,
                feed_epoch_label=label,
                information_mode=information_mode,
                used_at_ns=midpoint_ns,
                feed_epoch_definition=definition,
                retention_endpoint="lower",
                symbols=symbols,
                transition_policy=transition_policy,
                transition_scenario_kind=scenario,
            )
            for scenario in scenario_kinds
        )
        candidates = observation_admission_missing_count_bound(
            input_count,
            min(retentions),
            uncertainty_policy.admission_quantile,
        )
        runtime_limit = proposal_config.limits.max_generated_events_per_window
        amplification_limit = min(
            math.floor(
                input_count * proposal_config.limits.max_candidate_amplification
            ),
            math.floor(
                input_count
                * configuration.storage_policy.max_candidate_amplification
            ),
        )
    return candidates, runtime_limit, amplification_limit


def _independent_cardinality_refusal(
    modeled_count: int,
    *,
    runtime_limit: int,
    amplification_limit: int,
    duration_ns: int,
    sizing_audit: ReconstructionWindowSizingAuditV1 | None,
) -> str | None:
    """Classify only an independently proven irreducible count overflow."""
    effective_runtime_limit = min(
        runtime_limit,
        (
            sizing_audit.modeled_missing_event_limit
            if sizing_audit is not None
            else runtime_limit
        ),
    )
    if (
        modeled_count <= effective_runtime_limit
        and modeled_count <= amplification_limit
    ):
        return None
    if modeled_count > amplification_limit:
        return (
            "independent replay found qualified observation cardinality "
            f"requiring {modeled_count} modeled missing events above candidate "
            f"amplification headroom {amplification_limit}; subdivision cannot "
            "repair the ratio constraint"
        )
    if duration_ns > 1_000_000:
        raise FinalSupportVerificationError(
            "independent modeled cardinality exceeds generator safety before "
            "the irreducible one-millisecond boundary"
        )
    return (
        "independent replay found irreducible one-millisecond observation "
        f"cardinality requiring {modeled_count} modeled missing events above "
        f"runtime safety headroom {effective_runtime_limit}"
    )


def _independent_resource_estimate(
    input_count: int,
    modeled_count: int,
    configuration: ReconstructionPlanConfiguration,
) -> ReconstructionResourceEstimateV1:
    """Reconstruct one task estimate without calling the planner estimator."""
    interval_count = max(0, input_count - len(RECONSTRUCTION_SYMBOLS))
    inflight = min(
        interval_count, configuration.storage_policy.max_inflight_batches
    )
    peak = min(modeled_count, configuration.storage_policy.max_events_per_batch)
    bytes_per_event = configuration.generator_config.estimated_bytes_per_event
    return ReconstructionResourceEstimateV1(
        input_event_count=input_count,
        candidate_event_count=modeled_count,
        retained_ensemble_members=1,
        inflight_batches=inflight,
        peak_events_per_batch=peak,
        estimated_memory_bytes=(
            _RESOURCE_FIXED_OVERHEAD_BYTES
            + input_count * bytes_per_event
            + peak * bytes_per_event * max(1, inflight)
        ),
        estimated_scratch_bytes=(
            modeled_count * bytes_per_event
            + interval_count * _RESOURCE_LEDGER_BYTES_PER_INTERVAL
        ),
        estimated_output_bytes=(
            modeled_count * bytes_per_event + DEFAULT_MANIFEST_BYTES_PER_PRODUCT
        ),
        estimated_batch_count=interval_count,
    )


def _expected_terminal_decision(
    *,
    source_status: ReconstructionPlanSourceSupportStatus,
    alignment: _AlignmentFacts,
    assignments: Sequence[Any],
    observation_operator: Any,
    information_mode: InformationMode,
    definition: Any,
    transition_policy: FeedEpochTransitionPolicyV1 | None,
    context: Any,
    positioning: Any,
    context_qualification_id: str | None,
    start_ns: int,
    end_ns: int,
    symbols: Sequence[str],
) -> tuple[str, str | None, str | None, str, str]:
    if source_status is ReconstructionPlanSourceSupportStatus.EMPTY:
        return (
            "empty",
            None,
            None,
            "not_evaluated_empty",
            ReconstructionCftcConditioningMode.NOT_EVALUATED.value,
        )
    if source_status is ReconstructionPlanSourceSupportStatus.INCOMPLETE:
        return (
            "refused",
            ReconstructionPlanRefusalCode.SOURCE_TRIANGLE_INCOMPLETE.value,
            "independent replay found incomplete source triangle",
            "not_evaluated_incomplete",
            ReconstructionCftcConditioningMode.NOT_EVALUATED.value,
        )
    if alignment.selected_alignment == "unavailable":
        return (
            "refused",
            ReconstructionPlanRefusalCode.CROSS_SERIES_UNSUPPORTED.value,
            "independent replay found no qualified exact or bounded-nearest support",
            "not_evaluated_complete",
            ReconstructionCftcConditioningMode.NOT_EVALUATED.value,
        )
    labels = {
        str(getattr(item, "label", item.assignment_kind))
        for item in assignments
    }
    if (
        any(item.assignment_kind == "out_of_scope" for item in assignments)
        or len(labels) != 1
    ):
        return (
            "refused",
            ReconstructionPlanRefusalCode.FEED_EPOCH_UNSUPPORTED.value,
            "independent replay found unsupported feed-epoch assignment",
            "not_evaluated_complete",
            ReconstructionCftcConditioningMode.NOT_EVALUATED.value,
        )
    label = next(iter(labels))
    if isinstance(observation_operator, ObservationOperatorV1):
        scenarios: tuple[Any | None, ...] = (
            tuple(transition_policy.scenario_order)
            if label.startswith("transition:") and transition_policy is not None
            else (None,)
        )
        try:
            for scenario in scenarios:
                historical_product_observation_conditioning(
                    observation_operator,
                    feed_epoch_label=label,
                    symbols=symbols,
                    information_mode=information_mode,
                    used_at_ns=(start_ns + end_ns) // 2,
                    feed_epoch_definition=definition,
                    transition_policy=transition_policy,
                    transition_scenario_kind=scenario,
                )
        except (TypeError, ValueError) as err:
            return (
                "refused",
                ReconstructionPlanRefusalCode.FEED_EPOCH_UNSUPPORTED.value,
                f"independent cardinality conditioning failed: {err}",
                "not_evaluated_complete",
                ReconstructionCftcConditioningMode.NOT_EVALUATED.value,
            )
    context_reasons: list[str] = []
    for currency, kind in _REQUIRED_CONTEXT:
        decision = preflight_market_context_corpus(
            context,
            start_ns=start_ns,
            end_ns=end_ns,
            currencies=(currency,),
            kinds=(kind,),
        )
        context_reasons.extend(decision.reasons)
    if context_reasons:
        return (
            "refused",
            ReconstructionPlanRefusalCode.MARKET_CONTEXT_UNSUPPORTED.value,
            "; ".join(sorted(set(context_reasons))),
            "not_evaluated_complete",
            ReconstructionCftcConditioningMode.NOT_EVALUATED.value,
        )
    query = query_cftc_positioning_corpus(
        positioning,
        start_ns=start_ns,
        end_ns=end_ns,
        information_mode=information_mode,
        as_of_ns=(
            start_ns
            if information_mode is InformationMode.EX_ANTE_SIMULATION
            else None
        ),
        symbols=symbols,
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    if query.status is CftcPositioningQueryStatus.READY:
        return (
            "executable",
            None,
            None,
            query.status.value,
            CFTC_READY_CONDITIONING_MODE,
        )
    if (
        query.status in CFTC_UNCONDITIONED_AVAILABILITY_STATUSES
        and context_qualification_id is not None
    ):
        return (
            "executable",
            None,
            None,
            query.status.value,
            CFTC_UNAVAILABLE_CONDITIONING_MODE,
        )
    return (
        "refused",
        ReconstructionPlanRefusalCode.CFTC_POSITIONING_UNSUPPORTED.value,
        query.reason,
        query.status.value,
        ReconstructionCftcConditioningMode.REFUSED.value,
    )


def _context_labels(
    context: Any,
    *,
    start_ns: int,
    end_ns: int,
    information_mode: InformationMode,
    symbols: Sequence[str],
) -> tuple[str, str]:
    midpoint = (start_ns + end_ns) // 2
    calendar = market_context_calendar_state(midpoint)
    query = query_market_context_corpus(
        context,
        start_ns=start_ns,
        end_ns=end_ns,
        view=(
            MarketContextView.EX_ANTE
            if information_mode is InformationMode.EX_ANTE_SIMULATION
            else MarketContextView.EX_POST
        ),
        as_of_ns=(
            start_ns
            if information_mode is InformationMode.EX_ANTE_SIMULATION
            else None
        ),
        symbols=symbols,
        require_supported=False,
    )
    return calendar.session_state, market_context_benchmark_event_state(query)


def _assert_claimed_window(
    claimed: ReconstructionPlanSupportWindowV1,
    *,
    status: str,
    refusal_code: str | None,
    core_counts: Mapping[str, int],
    input_counts: Mapping[str, int],
    alignment: _AlignmentFacts,
    cftc_status: str,
    cftc_mode: str,
    task_count: int,
    member_ids: Sequence[str],
    resource_estimate: ReconstructionResourceEstimateV1 | None,
) -> None:
    mismatches: list[str] = []
    comparisons: dict[str, tuple[object, object]] = {
        "status": (claimed.status, status),
        "refusal_code": (claimed.refusal_code, refusal_code),
        "core_event_counts": (
            dict(claimed.core_source_event_counts or {}),
            dict(core_counts),
        ),
        "input_event_counts": (
            dict(claimed.input_source_event_counts or {}),
            dict(input_counts),
        ),
        "common_exact_core_timestamp_count": (
            claimed.common_exact_core_timestamp_count,
            alignment.common_exact_count,
        ),
        "bounded_nearest_core_timestamp_count": (
            claimed.bounded_nearest_core_timestamp_count,
            alignment.nearest_count,
        ),
        "bounded_nearest_core_stale_timestamp_count": (
            claimed.bounded_nearest_core_stale_timestamp_count,
            alignment.nearest_stale_count,
        ),
        "bounded_nearest_core_maximum_age_ns": (
            claimed.bounded_nearest_core_maximum_age_ns,
            alignment.nearest_maximum_age_ns,
        ),
        "bounded_nearest_core_p95_age_ns": (
            claimed.bounded_nearest_core_p95_age_ns,
            alignment.nearest_p95_age_ns,
        ),
        "selected_cross_series_alignment": (
            claimed.selected_cross_series_alignment,
            alignment.selected_alignment,
        ),
        "recommended_cross_series_event_time_ns": (
            claimed.recommended_cross_series_event_time_ns,
            alignment.recommended_time_ns,
        ),
        "cftc_query_status": (claimed.cftc_query_status, cftc_status),
        "cftc_conditioning_mode": (claimed.cftc_conditioning_mode, cftc_mode),
        "member_ids": (tuple(claimed.member_ids), tuple(sorted(member_ids))),
        "task_count": (task_count, len(member_ids)),
    }
    comparisons["resource_estimate"] = (
        (
            None
            if claimed.resource_estimate is None
            else dict(claimed.resource_estimate)
        ),
        None if resource_estimate is None else resource_estimate.to_dict(),
    )
    for name, (observed, expected) in comparisons.items():
        if observed != expected:
            mismatches.append(name)
    if mismatches:
        raise FinalSupportVerificationError(
            "claimed support differs from independent replay at "
            f"[{claimed.start_ns},{claimed.end_ns}): " + ", ".join(mismatches)
        )


def _verify_plan_shard(
    *,
    plan_set: ReconstructionPlanSetV1,
    plan_shard: Any,
    plan: SyntheticInfillPlanV1,
    claimed_windows: Sequence[ReconstructionPlanSupportWindowV1],
    claimed_support_map_id: str,
    candidate: ReconstructionReleaseCandidateV1,
) -> FinalSupportVerificationShardV1:
    import numpy as np  # pylint: disable=import-outside-toplevel

    validate_synthetic_infill_plan_for_execution(plan)
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )
    configuration = read_reconstruction_plan_configuration(
        plan.artifact_graph["configuration"].path
    )
    definition = read_active_time_feed_epoch_definition(
        plan.artifact_graph["feed_epochs"].path
    )
    observation_operator = read_observation_operator_artifact(
        plan.artifact_graph["observation_operator"]
    )
    context = read_market_context_corpus(
        plan.artifact_graph["market_context"].path
    )
    positioning = read_cftc_positioning_corpus(
        plan.artifact_graph["cftc_positioning"].path
    )
    cross_policy = read_cross_series_constraint_policy(
        plan.artifact_graph["cross_series_constraint_policy"].path
    )
    transition_policy = (
        read_feed_epoch_transition_policy(
            plan.artifact_graph["feed_epoch_transition_policy"].path
        )
        if "feed_epoch_transition_policy" in plan.artifact_graph
        else None
    )
    uncertainty_policy = (
        read_observation_uncertainty_policy(
            plan.artifact_graph["observation_uncertainty_policy"].path
        )
        if "observation_uncertainty_policy" in plan.artifact_graph
        else None
    )
    context_qualification_id: str | None = None
    if "context_availability_qualification" in plan.artifact_graph:
        qualification = read_reconstruction_context_availability_qualification(
            plan.artifact_graph["context_availability_qualification"].path
        )
        context_qualification_id = qualification.qualification_id
    sizing_audit: ReconstructionWindowSizingAuditV1 | None = None
    if "window_sizing_audit" in plan.artifact_graph:
        sizing_audit = read_reconstruction_window_sizing_audit(
            plan.artifact_graph["window_sizing_audit"].path
        )
    proposal_config = _load_proposal_config(configuration)
    if isinstance(candidate, ReconstructionReleaseCandidateV1):
        _verify_release_candidate_dependencies(plan, candidate, configuration)

    ordered_claims = tuple(
        sorted(claimed_windows, key=lambda item: item.start_ns)
    )
    if not ordered_claims:
        raise FinalSupportVerificationError("plan shard lacks claimed support")
    if (
        ordered_claims[0].start_ns != plan.requested_start_ns
        or ordered_claims[-1].end_ns != plan.requested_end_ns
    ):
        raise FinalSupportVerificationError(
            "claimed support bounds differ from plan"
        )
    for previous, current in pairwise(ordered_claims):
        if previous.end_ns != current.start_ns:
            raise FinalSupportVerificationError(
                "claimed support is not contiguous"
            )

    sources_by_period: dict[str, dict[str, list[_PartitionRows]]] = {
        period: {symbol: [] for symbol in RECONSTRUCTION_SYMBOLS}
        for period in inventory.periods
    }
    replays: list[FinalSupportPartitionReplayV1] = []
    for partition in inventory.partitions:
        candidate_key = f"{partition.symbol}:{partition.period}"
        candidate_hash = candidate.source_partition_hashes.get(candidate_key)
        if candidate_hash is None:
            raise FinalSupportVerificationError(
                f"release candidate lacks source hash: {candidate_key}"
            )
        if candidate_hash != partition.artifact.sha256:
            raise FinalSupportVerificationError(
                f"release-candidate source hash differs: {candidate_key}"
            )
        partition_rows, replay = _scan_partition(
            partition,
            requested_start_ns=plan.requested_start_ns,
            requested_end_ns=plan.requested_end_ns,
        )
        sources_by_period[partition.period][partition.symbol].append(
            partition_rows
        )
        replays.append(replay)

    tasks_by_boundary: dict[tuple[int, int], list[Any]] = {}
    for request in plan.workflow_requests:
        for task in request.tasks:
            boundary = (task.window.core_start_ns, task.window.core_end_ns)
            tasks_by_boundary.setdefault(boundary, []).append(task)
    refusals_by_boundary = {
        (item.start_ns, item.end_ns): item for item in plan.refusals
    }
    source_by_boundary = {
        (item.start_ns, item.end_ns): item for item in plan.source_support
    }
    cftc_by_boundary = {
        (item.start_ns, item.end_ns): item for item in plan.cftc_support
    }
    verified: list[FinalSupportWindowVerificationV1] = []
    resource_estimates: list[ReconstructionResourceEstimateV1] = []
    candidate_events_by_member: Counter[str] = Counter()
    observed_core_rows = 0
    for claimed in ordered_claims:
        boundary = (claimed.start_ns, claimed.end_ns)
        core_by_period: dict[str, dict[str, _WindowArrays]] = {}
        input_by_symbol: dict[str, list[_WindowArrays]] = {
            symbol: [] for symbol in RECONSTRUCTION_SYMBOLS
        }
        input_start = claimed.start_ns - configuration.left_halo_ns
        input_end = claimed.end_ns + configuration.right_lookahead_ns
        for period in inventory.periods:
            core_by_period[period] = {}
            for symbol in RECONSTRUCTION_SYMBOLS:
                period_rows = sources_by_period[period][symbol]
                core_by_period[period][symbol] = _window_arrays(
                    period_rows,
                    start_ns=claimed.start_ns,
                    end_ns=claimed.end_ns,
                )
                input_by_symbol[symbol].append(
                    _window_arrays(
                        period_rows, start_ns=input_start, end_ns=input_end
                    )
                )
        core_counts = {
            symbol: sum(
                len(core_by_period[period][symbol].timestamps_ms)
                for period in inventory.periods
            )
            for symbol in RECONSTRUCTION_SYMBOLS
        }
        input_counts = {
            symbol: sum(
                len(item.timestamps_ms) for item in input_by_symbol[symbol]
            )
            for symbol in RECONSTRUCTION_SYMBOLS
        }
        observed_core_rows += sum(core_counts.values())
        core_combined: dict[str, _WindowArrays] = {}
        input_combined: dict[str, _WindowArrays] = {}
        core_sources: dict[str, list[_PartitionRows]] = {
            symbol: [] for symbol in RECONSTRUCTION_SYMBOLS
        }
        input_sources: dict[str, list[_PartitionRows]] = {
            symbol: [] for symbol in RECONSTRUCTION_SYMBOLS
        }
        for symbol in RECONSTRUCTION_SYMBOLS:
            timestamps: list[Any] = []
            indexes: list[Any] = []
            ordinals: list[Any] = []
            for period in inventory.periods:
                arrays = core_by_period[period][symbol]
                period_rows = sources_by_period[period][symbol]
                offset = len(core_sources[symbol])
                core_sources[symbol].extend(period_rows)
                if len(arrays.timestamps_ms):
                    timestamps.append(arrays.timestamps_ms)
                    indexes.append(arrays.source_indexes + offset)
                    ordinals.append(arrays.row_ordinals)
            core_combined[symbol] = _WindowArrays(
                (
                    np.concatenate(timestamps)
                    if len(timestamps) > 1
                    else (
                        timestamps[0]
                        if timestamps
                        else np.asarray([], dtype=np.int64)
                    )
                ),
                (
                    np.concatenate(indexes)
                    if len(indexes) > 1
                    else (
                        indexes[0]
                        if indexes
                        else np.asarray([], dtype=np.int32)
                    )
                ),
                (
                    np.concatenate(ordinals)
                    if len(ordinals) > 1
                    else (
                        ordinals[0]
                        if ordinals
                        else np.asarray([], dtype=np.int64)
                    )
                ),
            )
            timestamps = []
            indexes = []
            ordinals = []
            for period_index, period in enumerate(inventory.periods):
                arrays = input_by_symbol[symbol][period_index]
                period_rows = sources_by_period[period][symbol]
                offset = len(input_sources[symbol])
                input_sources[symbol].extend(period_rows)
                if len(arrays.timestamps_ms):
                    timestamps.append(arrays.timestamps_ms)
                    indexes.append(arrays.source_indexes + offset)
                    ordinals.append(arrays.row_ordinals)
            input_combined[symbol] = _WindowArrays(
                (
                    np.concatenate(timestamps)
                    if len(timestamps) > 1
                    else (
                        timestamps[0]
                        if timestamps
                        else np.asarray([], dtype=np.int64)
                    )
                ),
                (
                    np.concatenate(indexes)
                    if len(indexes) > 1
                    else (
                        indexes[0]
                        if indexes
                        else np.asarray([], dtype=np.int32)
                    )
                ),
                (
                    np.concatenate(ordinals)
                    if len(ordinals) > 1
                    else (
                        ordinals[0]
                        if ordinals
                        else np.asarray([], dtype=np.int64)
                    )
                ),
            )
        alignment = _alignment_facts(
            core_by_period,
            sources_by_period,
            start_ns=claimed.start_ns,
            end_ns=claimed.end_ns,
            nearest_prior_max_age_ns=cross_policy.nearest_prior_max_age_ns,
            minimum_alignment_support=cross_policy.minimum_alignment_support,
        )
        complete = all(
            core_counts[symbol] > 0 and input_counts[symbol] >= 2
            for symbol in RECONSTRUCTION_SYMBOLS
        )
        empty = all(
            core_counts[symbol] == 0 for symbol in RECONSTRUCTION_SYMBOLS
        )
        source_status = (
            ReconstructionPlanSourceSupportStatus.COMPLETE
            if complete
            else (
                ReconstructionPlanSourceSupportStatus.EMPTY
                if empty
                else ReconstructionPlanSourceSupportStatus.INCOMPLETE
            )
        )
        midpoint = (claimed.start_ns + claimed.end_ns) // 2
        assignments = tuple(
            definition.assign(
                symbol=symbol, timestamp_utc_ms=midpoint // 1_000_000
            )
            for symbol in RECONSTRUCTION_SYMBOLS
        )
        assignment_ids = tuple(
            sorted(
                f"{symbol}:"
                + str(
                    getattr(assignment, "epoch_id", None)
                    or getattr(assignment, "boundary_id", None)
                    or assignment.assignment_kind
                )
                for symbol, assignment in zip(
                    RECONSTRUCTION_SYMBOLS, assignments, strict=True
                )
            )
        )
        labels = tuple(
            sorted(
                {
                    str(getattr(item, "label", item.assignment_kind))
                    for item in assignments
                }
            )
        )
        feed_label = "+".join(labels)
        transition_scenarios = (
            tuple(item.value for item in transition_policy.scenario_order)
            if feed_label.startswith("transition:")
            and transition_policy is not None
            else ()
        )
        status, refusal_code, refusal_reason, cftc_status, cftc_mode = (
            _expected_terminal_decision(
                source_status=source_status,
                alignment=alignment,
                assignments=assignments,
                observation_operator=observation_operator,
                information_mode=plan.information_mode,
                definition=definition,
                transition_policy=transition_policy,
                context=context,
                positioning=positioning,
                context_qualification_id=context_qualification_id,
                start_ns=claimed.start_ns,
                end_ns=claimed.end_ns,
                symbols=RECONSTRUCTION_SYMBOLS,
            )
        )
        tasks = tasks_by_boundary.get(boundary, [])
        member_ids = tuple(
            sorted(task.window.ensemble_member_id for task in tasks)
        )
        if len(member_ids) != len(set(member_ids)):
            raise FinalSupportVerificationError(
                "workflow task members are duplicated"
            )
        input_count = sum(input_counts.values())
        modeled_count = 0
        modeled_limit = 0
        resource_estimate: ReconstructionResourceEstimateV1 | None = None
        if status == "executable":
            (
                required_modeled_count,
                modeled_limit,
                amplification_limit,
            ) = _independent_modeled_count(
                input_count,
                midpoint_ns=midpoint,
                symbols=RECONSTRUCTION_SYMBOLS,
                information_mode=plan.information_mode,
                configuration=configuration,
                proposal_config=proposal_config,
                definition=definition,
                observation_operator=observation_operator,
                uncertainty_policy=uncertainty_policy,
                transition_policy=transition_policy,
            )
            cardinality_reason = _independent_cardinality_refusal(
                required_modeled_count,
                runtime_limit=modeled_limit,
                amplification_limit=amplification_limit,
                duration_ns=claimed.end_ns - claimed.start_ns,
                sizing_audit=sizing_audit,
            )
            if cardinality_reason is not None:
                status = "refused"
                refusal_code = (
                    ReconstructionPlanRefusalCode.OBSERVATION_CARDINALITY_UNSUPPORTED.value
                )
                refusal_reason = cardinality_reason
            else:
                modeled_count = required_modeled_count
                resource_estimate = _independent_resource_estimate(
                    input_count, modeled_count, configuration
                )
                resource_estimates.append(resource_estimate)
                for task in tasks:
                    if task.resource_estimate != resource_estimate:
                        raise FinalSupportVerificationError(
                            "workflow task resource estimate differs from "
                            "independent reconciliation"
                        )
                    candidate_events_by_member[
                        task.window.ensemble_member_id
                    ] += modeled_count
        _assert_claimed_window(
            claimed,
            status=status,
            refusal_code=refusal_code,
            core_counts=core_counts,
            input_counts=input_counts,
            alignment=alignment,
            cftc_status=cftc_status,
            cftc_mode=cftc_mode,
            task_count=len(tasks),
            member_ids=member_ids,
            resource_estimate=resource_estimate,
        )
        source_claim = source_by_boundary.get(boundary)
        if (
            source_claim is None
            or source_claim.support_id != claimed.source_support_id
        ):
            raise FinalSupportVerificationError(
                "claimed support lacks its scientific source-support identity"
            )
        if source_claim.status is not source_status:
            raise FinalSupportVerificationError(
                "scientific source-support status differs from independent replay"
            )
        cftc_claim = cftc_by_boundary.get(boundary)
        if cftc_claim is None or (
            cftc_claim.query_status != cftc_status
            or cftc_claim.conditioning_mode.value != cftc_mode
        ):
            raise FinalSupportVerificationError(
                "scientific CFTC decision differs from independent replay"
            )
        refusal_claim = refusals_by_boundary.get(boundary)
        if status == "refused" and (
            refusal_claim is None or refusal_claim.code.value != refusal_code
        ):
            raise FinalSupportVerificationError(
                "scientific refusal differs from independent replay"
            )
        if status != "refused" and refusal_claim is not None:
            raise FinalSupportVerificationError(
                "non-refused independent window has a planner refusal"
            )
        session, event_state = _context_labels(
            context,
            start_ns=claimed.start_ns,
            end_ns=claimed.end_ns,
            information_mode=plan.information_mode,
            symbols=RECONSTRUCTION_SYMBOLS,
        )
        maximum = (
            sizing_audit.requested_max_window_size_ns
            if sizing_audit is not None
            else configuration.window_size_ns
        )
        ratio = max(1.0, maximum / (claimed.end_ns - claimed.start_ns))
        split_depth = math.ceil(math.log2(ratio))
        core_digest = _row_identity_digest(core_combined, core_sources)
        input_digest = _row_identity_digest(input_combined, input_sources)
        amplification = modeled_count / input_count if input_count else 0.0
        verified.append(
            FinalSupportWindowVerificationV1(
                start_ns=claimed.start_ns,
                end_ns=claimed.end_ns,
                plan_id=plan.plan_id,
                plan_shard_id=plan_shard.shard_id,
                claimed_support_id=claimed.support_id,
                status=status,
                core_event_counts=core_counts,
                input_event_counts=input_counts,
                core_row_identity_digest=core_digest,
                input_anchor_identity_digest=input_digest,
                alignment_source_event_digest=alignment.source_event_digest,
                common_exact_core_timestamp_count=alignment.common_exact_count,
                bounded_nearest_core_timestamp_count=alignment.nearest_count,
                bounded_nearest_core_stale_timestamp_count=alignment.nearest_stale_count,
                bounded_nearest_core_maximum_age_ns=alignment.nearest_maximum_age_ns,
                bounded_nearest_core_p95_age_ns=alignment.nearest_p95_age_ns,
                selected_cross_series_alignment=alignment.selected_alignment,
                recommended_cross_series_event_time_ns=alignment.recommended_time_ns,
                feed_epoch_label=feed_label,
                feed_epoch_assignment_ids=assignment_ids,
                transition_scenario_ids=transition_scenarios,
                session=session,
                event_state=event_state,
                cftc_query_status=cftc_status,
                cftc_conditioning_mode=cftc_mode,
                modeled_missing_event_count=modeled_count,
                candidate_amplification=amplification,
                split_depth=split_depth,
                member_count=len(member_ids),
                workflow_task_count=len(tasks),
                refusal_code=refusal_code,
                refusal_reason=refusal_reason,
            )
        )
    expected_domain_rows = sum(
        item.in_requested_domain_row_count for item in replays
    )
    if observed_core_rows != expected_domain_rows:
        raise FinalSupportVerificationError(
            "source rows were lost or duplicated across half-open planning domains"
        )
    if len(verified) != plan.resources.planned_window_count:
        raise FinalSupportVerificationError(
            "verified window count differs from plan resources"
        )
    terminal = Counter(item.status for item in verified)
    if (
        terminal["executable"] != plan.resources.executable_window_count
        or terminal["empty"] != plan.resources.empty_window_count
        or terminal["refused"] != plan.resources.refused_window_count
        or sum(item.workflow_task_count for item in verified)
        != sum(len(request.tasks) for request in plan.workflow_requests)
    ):
        raise FinalSupportVerificationError(
            "verified terminal/task counts differ from plan resources"
        )
    retained_members = tuple(sorted(candidate_events_by_member))
    retained_count = (
        len(retained_members)
        if terminal["executable"]
        else plan.resources.retained_member_count
    )
    total_input = sum(
        sum(item.input_event_counts.values()) * item.member_count
        for item in verified
        if item.status == "executable"
    )
    total_candidates = sum(
        item.modeled_missing_event_count * item.member_count
        for item in verified
        if item.status == "executable"
    )
    partition_count = (
        terminal["executable"] * retained_count * len(RECONSTRUCTION_SYMBOLS)
        if total_candidates
        else 0
    )
    product_count = terminal["executable"] * retained_count
    bytes_per_output_event = (
        configuration.ensemble_config.estimated_bytes_per_event
    )
    retained_bytes = sum(
        math.ceil(
            count * bytes_per_output_event * DEFAULT_ESTIMATED_COMPRESSION_RATIO
        )
        for count in candidate_events_by_member.values()
    )
    expected_resources = {
        "source_event_count": sum(item.row_count for item in replays),
        "source_size_bytes": inventory.total_size_bytes,
        "planned_window_count": len(verified),
        "executable_window_count": terminal["executable"],
        "refused_window_count": terminal["refused"],
        "empty_window_count": terminal["empty"],
        "ensemble_member_count": len(plan.run.ensemble_member_ids),
        "retained_member_count": retained_count,
        "workflow_request_count": len(plan.workflow_requests),
        "estimated_input_event_count": total_input,
        "estimated_candidate_event_count": total_candidates,
        "estimated_candidate_bytes": (
            total_candidates * bytes_per_output_event
        ),
        "estimated_peak_memory_bytes": (
            max(
                (item.estimated_memory_bytes for item in resource_estimates),
                default=0,
            )
            * configuration.max_parallel_windows
        ),
        "estimated_peak_scratch_bytes": (
            max(
                (item.estimated_scratch_bytes for item in resource_estimates),
                default=0,
            )
            * configuration.max_parallel_windows
        ),
        "estimated_output_bytes": (
            retained_bytes
            + partition_count * DEFAULT_MANIFEST_BYTES_PER_PARTITION
            + product_count * DEFAULT_MANIFEST_BYTES_PER_PRODUCT
        ),
        "estimated_partition_count": partition_count,
    }
    resource_mismatches = [
        name
        for name, expected in expected_resources.items()
        if getattr(plan.resources, name) != expected
    ]
    if resource_mismatches:
        raise FinalSupportVerificationError(
            "plan resources differ from independent reconciliation: "
            + ", ".join(resource_mismatches)
        )
    census = build_final_support_census(verified)
    return FinalSupportVerificationShardV1(
        plan_set_id=plan_set.plan_set_id,
        plan_shard_id=plan_shard.shard_id,
        plan_id=plan.plan_id,
        release_candidate_id=candidate.candidate_id,
        source_inventory_id=inventory.inventory_id,
        claimed_support_map_id=claimed_support_map_id,
        requested_start_ns=plan.requested_start_ns,
        requested_end_ns=plan.requested_end_ns,
        partition_replays=tuple(replays),
        windows=tuple(verified),
        census=census,
    )


def _write_json(path: Path, payload: Mapping[str, JSONValue]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (canonical_contract_json(payload) + "\n").encode("utf-8")
    if len(encoded) > MAX_FINAL_SUPPORT_ARTIFACT_BYTES:
        raise FinalSupportVerificationError(
            "final support artifact exceeds limit"
        )
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return path


def write_final_support_verification_shard(
    shard: FinalSupportVerificationShardV1,
    output_directory: str | Path,
) -> ArtifactRef:
    root = Path(output_directory).expanduser().resolve()
    suffix = shard.verification_shard_id.rsplit(":", 1)[-1]
    path = _write_json(
        root / f"final-support-verification-shard-{suffix}.json",
        shard.to_dict(),
    )
    counts = Counter(item.status for item in shard.windows)
    return artifact_ref_for_file(
        path,
        kind=FINAL_SUPPORT_VERIFICATION_SHARD_ARTIFACT_KIND,
        metadata={
            "verification_shard_id": shard.verification_shard_id,
            "plan_set_id": shard.plan_set_id,
            "plan_shard_id": shard.plan_shard_id,
            "release_candidate_id": shard.release_candidate_id,
            "requested_start_ns": shard.requested_start_ns,
            "requested_end_ns": shard.requested_end_ns,
            "window_count": len(shard.windows),
            "executable_window_count": counts["executable"],
            "empty_window_count": counts["empty"],
            "refused_window_count": counts["refused"],
            "census_id": shard.census.census_id,
            "status": "qualified",
        },
    )


def read_final_support_verification_shard(
    path: str | Path,
) -> FinalSupportVerificationShardV1:
    payload = _mapping(json.loads(Path(path).read_text(encoding="utf-8")))
    return FinalSupportVerificationShardV1.from_dict(payload)


def write_final_adaptive_support_map_index(
    index: FinalAdaptiveSupportMapIndexV1,
    output_directory: str | Path,
) -> ArtifactRef:
    root = Path(output_directory).expanduser().resolve()
    suffix = index.final_support_map_id.rsplit(":", 1)[-1]
    path = _write_json(
        root / f"final-adaptive-support-map-index-{suffix}.json",
        index.to_dict(),
    )
    return artifact_ref_for_file(
        path,
        kind=FINAL_ADAPTIVE_SUPPORT_MAP_INDEX_ARTIFACT_KIND,
        metadata={
            "final_support_map_id": index.final_support_map_id,
            "plan_set_id": index.plan_set_id,
            "release_candidate_id": index.release_candidate_id,
            "requested_start_ns": index.requested_start_ns,
            "requested_end_ns": index.requested_end_ns,
            "window_count": index.census.window_count,
            "status": index.status,
            "census_id": index.census.census_id,
        },
    )


def read_final_adaptive_support_map_index(
    path: str | Path,
    *,
    verify_shards: bool = True,
) -> FinalAdaptiveSupportMapIndexV1:
    payload = _mapping(json.loads(Path(path).read_text(encoding="utf-8")))
    index = FinalAdaptiveSupportMapIndexV1.from_dict(payload)
    if verify_shards:
        for ref in (
            index.plan_set_ref,
            index.claimed_support_ref,
            index.release_candidate_ref,
        ):
            verify_artifact_ref(ref)
        plan_set = read_reconstruction_plan_set(index.plan_set_ref.path)
        if plan_set.plan_set_id != index.plan_set_id:
            raise FinalSupportVerificationError(
                "final index plan-set content binding differs"
            )
        expected_engines: set[str] = set()
        expected_scenarios: set[str] = set()
        expected_shards: list[tuple[str, str, int, int]] = []
        for plan_shard in plan_set.shards:
            verify_artifact_ref(plan_shard.plan_ref)
            plan = read_synthetic_infill_plan(plan_shard.plan_ref.path)
            configuration = read_reconstruction_plan_configuration(
                plan.artifact_graph["configuration"].path
            )
            if not isinstance(configuration, ReconstructionPlanConfigurationV2):
                raise FinalSupportVerificationError(
                    "final index references a non-v2 plan shard"
                )
            expected_engines.update(
                configuration.proposal_portfolio.selected_engine_ids
            )
            if "observation_uncertainty_policy" in plan.artifact_graph:
                uncertainty = read_observation_uncertainty_policy(
                    plan.artifact_graph["observation_uncertainty_policy"].path
                )
                expected_scenarios.update(
                    f"observation:{item.value}"
                    for item in uncertainty.scenario_order
                )
            if "feed_epoch_transition_policy" in plan.artifact_graph:
                transition = read_feed_epoch_transition_policy(
                    plan.artifact_graph["feed_epoch_transition_policy"].path
                )
                expected_scenarios.update(
                    f"transition:{item.value}"
                    for item in transition.scenario_order
                )
            expected_shards.append(
                (
                    plan_shard.shard_id,
                    plan.plan_id,
                    plan.requested_start_ns,
                    plan.requested_end_ns,
                )
            )
        if (
            tuple(sorted(expected_engines)) != index.selected_engine_ids
            or tuple(sorted(expected_scenarios)) != index.selected_scenario_ids
        ):
            raise FinalSupportVerificationError(
                "final index selected engine/scenario identities differ from plans"
            )
        windows: list[FinalSupportWindowVerificationV1] = []
        observed_shards: list[tuple[str, str, int, int]] = []
        for ref in index.verification_shard_refs:
            verify_artifact_ref(ref)
            shard = read_final_support_verification_shard(ref.path)
            counts = Counter(item.status for item in shard.windows)
            expected_metadata = {
                "verification_shard_id": shard.verification_shard_id,
                "plan_set_id": shard.plan_set_id,
                "plan_shard_id": shard.plan_shard_id,
                "release_candidate_id": shard.release_candidate_id,
                "requested_start_ns": shard.requested_start_ns,
                "requested_end_ns": shard.requested_end_ns,
                "window_count": len(shard.windows),
                "executable_window_count": counts["executable"],
                "empty_window_count": counts["empty"],
                "refused_window_count": counts["refused"],
                "census_id": shard.census.census_id,
                "status": "qualified",
            }
            if any(
                ref.metadata.get(name) != expected
                for name, expected in expected_metadata.items()
            ) or (
                shard.plan_set_id != index.plan_set_id
                or shard.release_candidate_id != index.release_candidate_id
            ):
                raise FinalSupportVerificationError(
                    "final index shard content binding differs"
                )
            observed_shards.append(
                (
                    shard.plan_shard_id,
                    shard.plan_id,
                    shard.requested_start_ns,
                    shard.requested_end_ns,
                )
            )
            windows.extend(shard.windows)
        if observed_shards != expected_shards:
            raise FinalSupportVerificationError(
                "final verification shards differ from the exact plan-set shards"
            )
        if build_final_support_census(windows) != index.census:
            raise FinalSupportVerificationError(
                "final index census differs from replayed verification shards"
            )
    return index


def build_final_adaptive_support_map(
    plan_set_path: str | Path,
    claimed_support_path: str | Path,
    release_candidate_path: str | Path,
    *,
    output_directory: str | Path,
) -> ArtifactRef:
    """Independently replay and publish the final candidate-bound support map."""
    plan_set = read_reconstruction_plan_set(plan_set_path)
    candidate = read_reconstruction_release_candidate(release_candidate_path)
    verify_reconstruction_release_candidate(candidate)
    if plan_set.requested_end_ns > candidate.source_cutoff_ns:
        raise FinalSupportVerificationError(
            "plan set exceeds frozen release-candidate source cutoff"
        )
    maps = _load_support_maps(claimed_support_path)
    if any(item.plan_set_id != plan_set.plan_set_id for item in maps):
        raise FinalSupportVerificationError(
            "claimed support map differs from plan set"
        )
    plan_set_ref = artifact_ref_for_file(
        plan_set_path,
        kind="reconstruction_plan_set_v1",
        metadata={
            "plan_set_id": plan_set.plan_set_id,
            "requested_start_ns": plan_set.requested_start_ns,
            "requested_end_ns": plan_set.requested_end_ns,
            "status": plan_set.status,
        },
    )
    support_ref = _claimed_support_ref(claimed_support_path, maps)
    candidate_ref = artifact_ref_for_file(
        release_candidate_path,
        kind="reconstruction_release_candidate_v1",
        metadata={
            "candidate_id": candidate.candidate_id,
            "source_cutoff_ns": candidate.source_cutoff_ns,
            "selected_engine_id": candidate.selected_engine_id,
        },
    )
    windows_by_shard: dict[str, list[ReconstructionPlanSupportWindowV1]] = {}
    map_id_by_shard: dict[str, str] = {}
    selected_engine_ids: set[str] = set()
    for support_map in maps:
        selected_engine_ids.update(support_map.selected_proposal_engine_ids)
        for window in support_map.windows:
            windows_by_shard.setdefault(window.shard_id, []).append(window)
            existing = map_id_by_shard.setdefault(
                window.shard_id, support_map.support_map_id
            )
            if existing != support_map.support_map_id:
                raise FinalSupportVerificationError(
                    "one plan shard spans multiple claimed support-map shards"
                )
    if candidate.selected_engine_id not in selected_engine_ids:
        raise FinalSupportVerificationError(
            "release-candidate engine differs from support-map selection"
        )
    root = Path(output_directory).expanduser().resolve()
    shard_refs: list[ArtifactRef] = []
    all_windows: list[FinalSupportWindowVerificationV1] = []
    selected_scenarios: set[str] = set()
    observed_candidate_hash_keys: set[str] = set()
    for plan_shard in plan_set.shards:
        verify_artifact_ref(plan_shard.plan_ref)
        plan = read_synthetic_infill_plan(plan_shard.plan_ref.path)
        inventory = read_reconstruction_source_inventory(
            plan.artifact_graph["source_inventory"].path
        )
        observed_candidate_hash_keys.update(
            f"{item.symbol}:{item.period}" for item in inventory.partitions
        )
        verification = _verify_plan_shard(
            plan_set=plan_set,
            plan_shard=plan_shard,
            plan=plan,
            claimed_windows=windows_by_shard.get(plan_shard.shard_id, ()),
            claimed_support_map_id=map_id_by_shard.get(plan_shard.shard_id, ""),
            candidate=candidate,
        )
        all_windows.extend(verification.windows)
        if "observation_uncertainty_policy" in plan.artifact_graph:
            uncertainty_policy = read_observation_uncertainty_policy(
                plan.artifact_graph["observation_uncertainty_policy"].path
            )
            selected_scenarios.update(
                f"observation:{item.value}"
                for item in uncertainty_policy.scenario_order
            )
        if "feed_epoch_transition_policy" in plan.artifact_graph:
            transition_policy = read_feed_epoch_transition_policy(
                plan.artifact_graph["feed_epoch_transition_policy"].path
            )
            selected_scenarios.update(
                f"transition:{item.value}"
                for item in transition_policy.scenario_order
            )
        selected_scenarios.update(
            f"transition:{scenario}"
            for window in verification.windows
            for scenario in window.transition_scenario_ids
        )
        shard_refs.append(
            write_final_support_verification_shard(
                verification, root / "verification-shards"
            )
        )
    expected_candidate_keys = _candidate_source_keys_for_bounds(
        candidate.source_partition_hashes,
        requested_start_ns=plan_set.requested_start_ns,
        requested_end_ns=plan_set.requested_end_ns,
    )
    missing_candidate_keys = expected_candidate_keys.difference(
        observed_candidate_hash_keys
    )
    if missing_candidate_keys:
        raise FinalSupportVerificationError(
            "release-candidate source partitions are absent from the plan set: "
            + ", ".join(sorted(missing_candidate_keys))
        )
    census = build_final_support_census(all_windows)
    if census.window_count != _strict_int(
        plan_set.resource_summary.get("planned_window_count"),
        "planned_window_count",
    ):
        raise FinalSupportVerificationError(
            "final verified census differs from plan-set resources"
        )
    index = FinalAdaptiveSupportMapIndexV1(
        plan_set_ref=plan_set_ref,
        claimed_support_ref=support_ref,
        release_candidate_ref=candidate_ref,
        verification_shard_refs=tuple(shard_refs),
        selected_engine_ids=tuple(selected_engine_ids),
        selected_scenario_ids=tuple(selected_scenarios),
        source_cutoff_ns=candidate.source_cutoff_ns,
        requested_start_ns=plan_set.requested_start_ns,
        requested_end_ns=plan_set.requested_end_ns,
        census=census,
    )
    ref = write_final_adaptive_support_map_index(index, root)
    restored = read_final_adaptive_support_map_index(ref.path)
    if restored != index:
        raise FinalSupportVerificationError(
            "final support-map write/read verification differs"
        )
    return ref


__all__ = [
    "ALIGNMENT_EVENT_POLICY",
    "FINAL_ADAPTIVE_SUPPORT_MAP_INDEX_ARTIFACT_KIND",
    "FINAL_ADAPTIVE_SUPPORT_MAP_INDEX_SCHEMA_VERSION",
    "FINAL_SUPPORT_CENSUS_SCHEMA_VERSION",
    "FINAL_SUPPORT_PARTITION_REPLAY_SCHEMA_VERSION",
    "FINAL_SUPPORT_VERIFICATION_SHARD_ARTIFACT_KIND",
    "FINAL_SUPPORT_VERIFICATION_SHARD_SCHEMA_VERSION",
    "FINAL_SUPPORT_WINDOW_VERIFICATION_SCHEMA_VERSION",
    "INDEPENDENT_DECISION_POLICY",
    "INDEPENDENT_VERIFIER_ID",
    "ROW_DOMAIN_POLICY",
    "ROW_IDENTITY_POLICY",
    "FinalAdaptiveSupportMapIndexV1",
    "FinalSupportCensusV1",
    "FinalSupportPartitionReplayV1",
    "FinalSupportVerificationError",
    "FinalSupportVerificationShardV1",
    "FinalSupportWindowVerificationV1",
    "build_final_adaptive_support_map",
    "build_final_support_census",
    "read_final_adaptive_support_map_index",
    "read_final_support_verification_shard",
    "write_final_adaptive_support_map_index",
    "write_final_support_verification_shard",
]
