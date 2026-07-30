"""Deterministic broker-style proposal conditioning and delivery rendering.

The broker fingerprint is an observation/delivery model.  It may condition
motif retrieval and render already accepted synthetic events, but it never
changes immutable observations or supplies historical price-path content.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, cast

from histdatacom.broker_capture.fingerprint_contracts import (
    BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION,
    BrokerDeliveryCellV1,
    BrokerDeliveryFingerprintComparisonV1,
    BrokerDeliveryFingerprintV1,
    BrokerDeliverySupportStatus,
)
from histdatacom.data_quality.contracts import QualityStatus
from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.benchmark import ReverseDegradationScorecardV1
from histdatacom.synthetic.carving import HistoricalCarvingConstraintSetV1
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyGroupStatus,
    CrossCurrencyReconciledGroupV1,
    CrossCurrencyValidationReportV1,
    CrossCurrencyValidationStage,
    CrossCurrencyValidationStatus,
    cross_currency_quality_report,
    validate_cross_currency_output,
)
from histdatacom.synthetic.motifs import ReferenceMotifQueryV1
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

BROKER_TRANSFER_CONFIG_SCHEMA_VERSION = "histdatacom.broker-transfer-config.v1"
BROKER_PROFILE_SELECTION_SCHEMA_VERSION = (
    "histdatacom.broker-profile-selection.v1"
)
BROKER_CONDITIONED_PROPOSAL_SCHEMA_VERSION = (
    "histdatacom.broker-conditioned-proposal.v1"
)
BROKER_RENDER_LINEAGE_SCHEMA_VERSION = "histdatacom.broker-render-lineage.v1"
BROKER_TRANSFER_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.broker-transfer-manifest.v1"
)
BROKER_RENDERED_GROUP_SCHEMA_VERSION = "histdatacom.broker-rendered-group.v1"
BROKER_BENCHMARK_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.broker-benchmark-comparison.v1"
)
BROKER_TRANSFER_ENGINE_ID = "histdatacom.broker-delivery-transfer"
BROKER_TRANSFER_ENGINE_VERSION = "1.0.0"

MAX_BROKER_TRANSFER_EVENTS = 1_000_000
MAX_BROKER_TRANSFER_ACTIONS = 32
MAX_BROKER_TRANSFER_REASONS = 128
MAX_BROKER_TRANSFER_METRICS = 64
MAX_BROKER_TRANSFER_TEXT = 1_024
INT64_MAX = 2**63 - 1

_PROPOSAL_METRIC_TARGETS = {
    "timestamp_precision_ns": ("source_timestamp_precision_ns",),
    "spread": ("spread",),
    "price_precision_digits": ("price_decimal_places",),
}


class BrokerTransferStatus(str, Enum):
    """Whether a requested broker-style application was supported."""

    APPLIED = "applied"
    BACKED_OFF = "backed_off"
    REFUSED = "refused"


@dataclass(frozen=True, slots=True)
class BrokerTransferConfigV1:
    """Versioned, bounded semantics for a measurable gentle transfer."""

    strength: float = 0.25
    max_timestamp_shift_ns: int = 1_000_000_000
    max_spread_multiplier: float = 2.0
    max_batch_size: int = 64
    max_events_per_group: int = 250_000
    input_price_decimal_places: int = 8
    minimum_price_decimal_places: int = 1
    apply_stale_behavior: bool = True
    apply_exact_duplicates: bool = True
    apply_batching: bool = True
    rounding_digits: int = 12
    config_id: str = ""
    schema_version: str = BROKER_TRANSFER_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_TRANSFER_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported broker transfer config")
        strength = _finite_float(self.strength, "strength")
        if not 0.0 <= strength <= 1.0:
            raise ValueError("strength must be inside [0,1]")
        object.__setattr__(self, "strength", strength)
        shift = _bounded_int(
            self.max_timestamp_shift_ns,
            "max_timestamp_shift_ns",
            0,
            INT64_MAX,
        )
        object.__setattr__(self, "max_timestamp_shift_ns", shift)
        multiplier = _finite_float(
            self.max_spread_multiplier, "max_spread_multiplier"
        )
        if multiplier < 1.0:
            raise ValueError("max_spread_multiplier must be at least one")
        object.__setattr__(self, "max_spread_multiplier", multiplier)
        object.__setattr__(
            self,
            "max_batch_size",
            _bounded_int(self.max_batch_size, "max_batch_size", 1, 65_536),
        )
        object.__setattr__(
            self,
            "max_events_per_group",
            _bounded_int(
                self.max_events_per_group,
                "max_events_per_group",
                1,
                MAX_BROKER_TRANSFER_EVENTS,
            ),
        )
        for name in (
            "input_price_decimal_places",
            "minimum_price_decimal_places",
            "rounding_digits",
        ):
            value = _bounded_int(getattr(self, name), name, 0, 15)
            object.__setattr__(self, name, value)
        if self.minimum_price_decimal_places > self.input_price_decimal_places:
            raise ValueError(
                "minimum price precision exceeds input price precision"
            )
        for name in (
            "apply_stale_behavior",
            "apply_exact_duplicates",
            "apply_batching",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be boolean")
        expected = _stable_id("broker-transfer-config", self.identity_payload())
        supplied = _optional_text(self.config_id)
        if supplied is not None and supplied != expected:
            raise ValueError("broker transfer config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": BROKER_TRANSFER_ENGINE_ID,
            "engine_version": BROKER_TRANSFER_ENGINE_VERSION,
            "strength": self.strength,
            "strength_semantics": (
                "convex blend from historical value 0 to broker target 1"
            ),
            "max_timestamp_shift_ns": self.max_timestamp_shift_ns,
            "max_spread_multiplier": self.max_spread_multiplier,
            "max_batch_size": self.max_batch_size,
            "input_price_decimal_places": self.input_price_decimal_places,
            "minimum_price_decimal_places": self.minimum_price_decimal_places,
            "apply_stale_behavior": self.apply_stale_behavior,
            "apply_exact_duplicates": self.apply_exact_duplicates,
            "apply_batching": self.apply_batching,
            "rounding_digits": self.rounding_digits,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "max_events_per_group": self.max_events_per_group,
            "execution_limits_in_config_identity": False,
            "config_id": self.config_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerTransferConfigV1":
        _require_schema(data, BROKER_TRANSFER_CONFIG_SCHEMA_VERSION)
        return cls(
            strength=_finite_float(data.get("strength"), "strength"),
            max_timestamp_shift_ns=_strict_int(
                data.get("max_timestamp_shift_ns"), "max_timestamp_shift_ns"
            ),
            max_spread_multiplier=_finite_float(
                data.get("max_spread_multiplier"), "max_spread_multiplier"
            ),
            max_batch_size=_strict_int(
                data.get("max_batch_size"), "max_batch_size"
            ),
            max_events_per_group=_strict_int(
                data.get("max_events_per_group"), "max_events_per_group"
            ),
            input_price_decimal_places=_strict_int(
                data.get("input_price_decimal_places"),
                "input_price_decimal_places",
            ),
            minimum_price_decimal_places=_strict_int(
                data.get("minimum_price_decimal_places"),
                "minimum_price_decimal_places",
            ),
            apply_stale_behavior=_strict_bool(
                data.get("apply_stale_behavior"), "apply_stale_behavior"
            ),
            apply_exact_duplicates=_strict_bool(
                data.get("apply_exact_duplicates"), "apply_exact_duplicates"
            ),
            apply_batching=_strict_bool(
                data.get("apply_batching"), "apply_batching"
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerTransferConfigV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerProfileSelectionV1:
    """One explicit fingerprint-cell selection and metric fallback trace."""

    fingerprint_id: str
    fingerprint_schema_version: str
    requested_condition: Mapping[str, str]
    requested_condition_id: str | None
    effective_condition_id: str | None
    support_status: str
    status: BrokerTransferStatus
    selected_at_utc_ns: int
    profile_effective_start_utc_ns: int
    profile_effective_end_utc_ns: int | None
    supersedes_fingerprint_id: str | None
    metrics: Mapping[str, float]
    metric_condition_ids: Mapping[str, str]
    reason_codes: tuple[str, ...] = ()
    drift_comparison_id: str | None = None
    material_drift_count: int | None = None
    selection_id: str = ""
    schema_version: str = BROKER_PROFILE_SELECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_PROFILE_SELECTION_SCHEMA_VERSION:
            raise ValueError("unsupported broker profile selection")
        if self.fingerprint_schema_version != (
            BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION
        ):
            raise ValueError("selection fingerprint schema is unsupported")
        object.__setattr__(
            self, "fingerprint_id", _required_text(self.fingerprint_id)
        )
        condition = {
            _required_name(str(name)): _required_name(str(value))
            for name, value in sorted(self.requested_condition.items())
        }
        object.__setattr__(self, "requested_condition", condition)
        for name in (
            "requested_condition_id",
            "effective_condition_id",
            "supersedes_fingerprint_id",
            "drift_comparison_id",
        ):
            object.__setattr__(self, name, _optional_text(getattr(self, name)))
        support = _required_name(self.support_status)
        object.__setattr__(self, "support_status", support)
        status = BrokerTransferStatus(self.status)
        object.__setattr__(self, "status", status)
        selected = _bounded_int(
            self.selected_at_utc_ns, "selected_at_utc_ns", 0, INT64_MAX
        )
        start = _bounded_int(
            self.profile_effective_start_utc_ns,
            "profile_effective_start_utc_ns",
            0,
            INT64_MAX,
        )
        end = self.profile_effective_end_utc_ns
        if end is not None:
            end = _bounded_int(
                end, "profile_effective_end_utc_ns", 0, INT64_MAX
            )
            if end <= start:
                raise ValueError(
                    "selection profile effective interval is empty"
                )
        object.__setattr__(self, "selected_at_utc_ns", selected)
        object.__setattr__(self, "profile_effective_start_utc_ns", start)
        object.__setattr__(self, "profile_effective_end_utc_ns", end)
        metrics = _metric_mapping(self.metrics)
        sources = {
            _required_name(str(name)): _required_text(value)
            for name, value in sorted(self.metric_condition_ids.items())
        }
        if set(metrics) != set(sources):
            raise ValueError("selection metric sources do not cover metrics")
        object.__setattr__(self, "metrics", metrics)
        object.__setattr__(self, "metric_condition_ids", sources)
        reasons = _bounded_text_tuple(
            self.reason_codes, MAX_BROKER_TRANSFER_REASONS
        )
        object.__setattr__(self, "reason_codes", reasons)
        if status is BrokerTransferStatus.REFUSED:
            if not reasons or self.effective_condition_id is not None:
                raise ValueError(
                    "refused selection requires reasons and no cell"
                )
        elif self.effective_condition_id is None:
            raise ValueError("applied selection requires an effective cell")
        if self.material_drift_count is not None:
            object.__setattr__(
                self,
                "material_drift_count",
                _bounded_int(
                    self.material_drift_count,
                    "material_drift_count",
                    0,
                    INT64_MAX,
                ),
            )
        expected = _stable_id(
            "broker-profile-selection", self.identity_payload()
        )
        supplied = _optional_text(self.selection_id)
        if supplied is not None and supplied != expected:
            raise ValueError("broker profile selection_id differs")
        object.__setattr__(self, "selection_id", expected)

    @property
    def applied(self) -> bool:
        return self.status is not BrokerTransferStatus.REFUSED

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "fingerprint_id": self.fingerprint_id,
            "fingerprint_schema_version": self.fingerprint_schema_version,
            "requested_condition": dict(self.requested_condition),
            "requested_condition_id": self.requested_condition_id,
            "effective_condition_id": self.effective_condition_id,
            "support_status": self.support_status,
            "status": self.status.value,
            "selected_at_utc_ns": self.selected_at_utc_ns,
            "profile_effective_start_utc_ns": (
                self.profile_effective_start_utc_ns
            ),
            "profile_effective_end_utc_ns": self.profile_effective_end_utc_ns,
            "supersedes_fingerprint_id": self.supersedes_fingerprint_id,
            "metrics": dict(self.metrics),
            "metric_condition_ids": dict(self.metric_condition_ids),
            "reason_codes": list(self.reason_codes),
            "drift_comparison_id": self.drift_comparison_id,
            "material_drift_count": self.material_drift_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "selection_id": self.selection_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerProfileSelectionV1":
        _require_schema(data, BROKER_PROFILE_SELECTION_SCHEMA_VERSION)
        return cls(
            fingerprint_id=str(data.get("fingerprint_id", "")),
            fingerprint_schema_version=str(
                data.get("fingerprint_schema_version", "")
            ),
            requested_condition={
                str(name): str(value)
                for name, value in _mapping(
                    data.get("requested_condition")
                ).items()
            },
            requested_condition_id=_optional_text(
                data.get("requested_condition_id")
            ),
            effective_condition_id=_optional_text(
                data.get("effective_condition_id")
            ),
            support_status=str(data.get("support_status", "")),
            status=BrokerTransferStatus(str(data.get("status", ""))),
            selected_at_utc_ns=_strict_int(
                data.get("selected_at_utc_ns"), "selected_at_utc_ns"
            ),
            profile_effective_start_utc_ns=_strict_int(
                data.get("profile_effective_start_utc_ns"),
                "profile_effective_start_utc_ns",
            ),
            profile_effective_end_utc_ns=_optional_int(
                data.get("profile_effective_end_utc_ns")
            ),
            supersedes_fingerprint_id=_optional_text(
                data.get("supersedes_fingerprint_id")
            ),
            metrics={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(data.get("metrics")).items()
            },
            metric_condition_ids={
                str(name): str(value)
                for name, value in _mapping(
                    data.get("metric_condition_ids")
                ).items()
            },
            reason_codes=_string_tuple(data.get("reason_codes")),
            drift_comparison_id=_optional_text(data.get("drift_comparison_id")),
            material_drift_count=_optional_int(
                data.get("material_drift_count")
            ),
            selection_id=str(data.get("selection_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerConditionedProposalV1:
    """A broker-conditioned motif query produced before candidate generation."""

    selection: BrokerProfileSelectionV1
    transfer_config: BrokerTransferConfigV1
    original_query_id: str
    conditioned_query: ReferenceMotifQueryV1 | None
    metrics_before: Mapping[str, float]
    metrics_after: Mapping[str, float]
    proposal_id: str = ""
    schema_version: str = BROKER_CONDITIONED_PROPOSAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_CONDITIONED_PROPOSAL_SCHEMA_VERSION:
            raise ValueError("unsupported broker-conditioned proposal")
        if not isinstance(self.selection, BrokerProfileSelectionV1):
            raise TypeError("proposal requires a profile selection")
        if not isinstance(self.transfer_config, BrokerTransferConfigV1):
            raise TypeError("proposal requires a transfer config")
        object.__setattr__(
            self, "original_query_id", _required_text(self.original_query_id)
        )
        before = _metric_mapping(self.metrics_before)
        after = _metric_mapping(self.metrics_after)
        object.__setattr__(self, "metrics_before", before)
        object.__setattr__(self, "metrics_after", after)
        if self.selection.applied:
            if self.conditioned_query is None:
                raise ValueError(
                    "applied proposal requires a conditioned query"
                )
            if (
                self.conditioned_query.query_id == self.original_query_id
                and self.transfer_config.strength > 0.0
            ):
                raise ValueError(
                    "conditioned proposal did not change query identity"
                )
            if self.transfer_config.strength == 0.0 and before != after:
                raise ValueError("zero-strength proposal changed query metrics")
        elif self.conditioned_query is not None:
            raise ValueError("refused proposal cannot contain a query")
        expected = _stable_id(
            "broker-conditioned-proposal", self.identity_payload()
        )
        supplied = _optional_text(self.proposal_id)
        if supplied is not None and supplied != expected:
            raise ValueError("broker proposal_id differs")
        object.__setattr__(self, "proposal_id", expected)

    @property
    def status(self) -> BrokerTransferStatus:
        return self.selection.status

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "selection": self.selection.to_dict(),
            "transfer_config": self.transfer_config.to_dict(),
            "original_query_id": self.original_query_id,
            "conditioned_query": (
                self.conditioned_query.to_dict()
                if self.conditioned_query is not None
                else None
            ),
            "metrics_before": dict(self.metrics_before),
            "metrics_after": dict(self.metrics_after),
            "stage": "pre_retrieval_and_generation",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "proposal_id": self.proposal_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerConditionedProposalV1":
        _require_schema(data, BROKER_CONDITIONED_PROPOSAL_SCHEMA_VERSION)
        query = data.get("conditioned_query")
        return cls(
            selection=BrokerProfileSelectionV1.from_dict(
                _mapping(data.get("selection"))
            ),
            transfer_config=BrokerTransferConfigV1.from_dict(
                _mapping(data.get("transfer_config"))
            ),
            original_query_id=str(data.get("original_query_id", "")),
            conditioned_query=(
                ReferenceMotifQueryV1.from_dict(_mapping(query))
                if query is not None
                else None
            ),
            metrics_before={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(data.get("metrics_before")).items()
            },
            metrics_after={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(data.get("metrics_after")).items()
            },
            proposal_id=str(data.get("proposal_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerConditionedProposalV1":
        return cls.from_dict(_json_mapping(text))


def select_broker_profile(
    fingerprint: BrokerDeliveryFingerprintV1,
    *,
    requested_condition: Mapping[str, str],
    selected_at_utc_ns: int,
    drift_comparison: BrokerDeliveryFingerprintComparisonV1 | None = None,
) -> BrokerProfileSelectionV1:
    """Resolve one exact cell and its recorded backoff without invention."""
    selected_at = _bounded_int(
        selected_at_utc_ns, "selected_at_utc_ns", 0, INT64_MAX
    )
    requested = {
        _required_name(str(name)): _required_name(str(value))
        for name, value in sorted(requested_condition.items())
    }
    requested_id = _condition_id(requested)
    comparison_id, material_count, drift_reason = _drift_evidence(
        fingerprint, drift_comparison
    )
    reasons: list[str] = []
    if drift_reason is not None:
        reasons.append(drift_reason)
        return _refused_selection(
            fingerprint,
            requested,
            requested_id,
            selected_at,
            reasons,
            comparison_id,
            material_count,
        )
    if selected_at < fingerprint.effective_start_utc_ns or (
        fingerprint.effective_end_utc_ns is not None
        and selected_at >= fingerprint.effective_end_utc_ns
    ):
        reasons.append("profile_not_effective_at_selection_time")
        return _refused_selection(
            fingerprint,
            requested,
            requested_id,
            selected_at,
            reasons,
            comparison_id,
            material_count,
        )
    cells = {item.condition.condition_id: item for item in fingerprint.cells}
    requested_cell = cells.get(requested_id)
    if requested_cell is None:
        reasons.append("requested_condition_absent")
        return _refused_selection(
            fingerprint,
            requested,
            requested_id,
            selected_at,
            reasons,
            comparison_id,
            material_count,
        )
    if requested_cell.support_status is BrokerDeliverySupportStatus.UNSUPPORTED:
        reasons.extend(
            ("requested_condition_unsupported", *requested_cell.limitations)
        )
        return _refused_selection(
            fingerprint,
            requested,
            requested_id,
            selected_at,
            reasons,
            comparison_id,
            material_count,
        )
    effective_id = requested_cell.effective_condition_id
    effective_cell = cells.get(str(effective_id))
    if effective_cell is None:
        reasons.append("recorded_effective_condition_absent")
        return _refused_selection(
            fingerprint,
            requested,
            requested_id,
            selected_at,
            reasons,
            comparison_id,
            material_count,
        )
    status = BrokerTransferStatus.APPLIED
    if requested_cell.support_status is BrokerDeliverySupportStatus.BACKED_OFF:
        status = BrokerTransferStatus.BACKED_OFF
        reasons.append("requested_condition_backed_off")
    global_cell = next(
        item for item in fingerprint.cells if not item.condition.dimensions
    )
    metrics, sources = _resolved_metrics(effective_cell, global_cell)
    return BrokerProfileSelectionV1(
        fingerprint_id=fingerprint.fingerprint_id,
        fingerprint_schema_version=fingerprint.schema_version,
        requested_condition=requested,
        requested_condition_id=requested_id,
        effective_condition_id=effective_cell.condition.condition_id,
        support_status=requested_cell.support_status.value,
        status=status,
        selected_at_utc_ns=selected_at,
        profile_effective_start_utc_ns=fingerprint.effective_start_utc_ns,
        profile_effective_end_utc_ns=fingerprint.effective_end_utc_ns,
        supersedes_fingerprint_id=fingerprint.supersedes_fingerprint_id,
        metrics=metrics,
        metric_condition_ids=sources,
        reason_codes=tuple(sorted(set(reasons))),
        drift_comparison_id=comparison_id,
        material_drift_count=material_count,
    )


def condition_broker_proposal(
    query: ReferenceMotifQueryV1,
    fingerprint: BrokerDeliveryFingerprintV1,
    *,
    requested_condition: Mapping[str, str],
    selected_at_utc_ns: int,
    config: BrokerTransferConfigV1 | None = None,
    drift_comparison: BrokerDeliveryFingerprintComparisonV1 | None = None,
) -> BrokerConditionedProposalV1:
    """Blend broker delivery metrics into a motif query before retrieval."""
    policy = config or BrokerTransferConfigV1()
    selection = select_broker_profile(
        fingerprint,
        requested_condition=requested_condition,
        selected_at_utc_ns=selected_at_utc_ns,
        drift_comparison=drift_comparison,
    )
    before = dict(query.condition.metrics)
    if not selection.applied:
        return BrokerConditionedProposalV1(
            selection=selection,
            transfer_config=policy,
            original_query_id=query.query_id,
            conditioned_query=None,
            metrics_before=before,
            metrics_after=before,
        )
    if policy.strength == 0.0:
        return BrokerConditionedProposalV1(
            selection=selection,
            transfer_config=policy,
            original_query_id=query.query_id,
            conditioned_query=query,
            metrics_before=before,
            metrics_after=before,
        )
    after = dict(before)
    cadence_target = _broker_cadence_target(selection.metrics)
    historical_cadence = after.get("interarrival_ns")
    if historical_cadence is None:
        historical_intensity = after.get("tick_intensity")
        if historical_intensity is not None and historical_intensity > 0.0:
            historical_cadence = 1_000_000_000.0 / historical_intensity
    if cadence_target is not None and historical_cadence is not None:
        cadence = _rounded(
            _blend(historical_cadence, cadence_target, policy.strength),
            policy.rounding_digits,
        )
        after["interarrival_ns"] = cadence
        historical_intensity = after.get(
            "tick_intensity", 1_000_000_000.0 / historical_cadence
        )
        after["tick_intensity"] = _rounded(
            _blend(
                historical_intensity,
                1_000_000_000.0 / cadence_target,
                policy.strength,
            ),
            policy.rounding_digits,
        )
    for target_name, source_names in _PROPOSAL_METRIC_TARGETS.items():
        target = next(
            (
                selection.metrics[source_name]
                for source_name in source_names
                if source_name in selection.metrics
            ),
            None,
        )
        if target is None:
            continue
        historical = after.get(target_name)
        if historical is None:
            if target_name == "timestamp_precision_ns":
                historical = 1.0
            elif target_name == "price_precision_digits":
                historical = float(policy.input_price_decimal_places)
            else:
                continue
        after[target_name] = _rounded(
            _blend(historical, target, policy.strength),
            policy.rounding_digits,
        )
    conditioned = replace(
        query.condition,
        metrics=after,
    )
    conditioned_query = replace(query, condition=conditioned, query_id="")
    return BrokerConditionedProposalV1(
        selection=selection,
        transfer_config=policy,
        original_query_id=query.query_id,
        conditioned_query=conditioned_query,
        metrics_before=before,
        metrics_after=after,
    )


def _broker_cadence_target(metrics: Mapping[str, float]) -> float | None:
    cadence = metrics.get("active_quote_interarrival_ns")
    if cadence is None:
        cadence = metrics.get("quote_interarrival_ns")
    intensity = metrics.get("quote_intensity_hz")
    if cadence is None and intensity is not None and intensity > 0.0:
        cadence = 1_000_000_000.0 / intensity
    if cadence is None or cadence <= 0.0:
        return None
    burst = max(0.0, metrics.get("burst_interval_rate", 0.0))
    quiet = max(0.0, metrics.get("quiet_interval_rate", 0.0))
    outage_rate = max(0.0, metrics.get("event_kind.outage_end_rate", 0.0))
    outage_duration = max(0.0, metrics.get("outage_or_gap_duration_ns", 0.0))
    outage_weight = outage_rate * min(10.0, outage_duration / cadence)
    structure_factor = max(0.1, 1.0 + quiet + outage_weight - 0.5 * burst)
    return max(1.0, cadence * structure_factor)


def _condition_id(dimensions: Mapping[str, str]) -> str:
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryConditionV1,
    )

    return str(BrokerDeliveryConditionV1(dict(dimensions)).condition_id)


def _refused_selection(
    fingerprint: BrokerDeliveryFingerprintV1,
    requested: Mapping[str, str],
    requested_id: str,
    selected_at: int,
    reasons: Sequence[str],
    comparison_id: str | None,
    material_count: int | None,
) -> BrokerProfileSelectionV1:
    return BrokerProfileSelectionV1(
        fingerprint_id=fingerprint.fingerprint_id,
        fingerprint_schema_version=fingerprint.schema_version,
        requested_condition=requested,
        requested_condition_id=requested_id,
        effective_condition_id=None,
        support_status=BrokerDeliverySupportStatus.UNSUPPORTED.value,
        status=BrokerTransferStatus.REFUSED,
        selected_at_utc_ns=selected_at,
        profile_effective_start_utc_ns=fingerprint.effective_start_utc_ns,
        profile_effective_end_utc_ns=fingerprint.effective_end_utc_ns,
        supersedes_fingerprint_id=fingerprint.supersedes_fingerprint_id,
        metrics={},
        metric_condition_ids={},
        reason_codes=tuple(sorted(set(reasons))),
        drift_comparison_id=comparison_id,
        material_drift_count=material_count,
    )


def _resolved_metrics(
    effective: BrokerDeliveryCellV1,
    global_cell: BrokerDeliveryCellV1,
) -> tuple[dict[str, float], dict[str, str]]:
    metrics: dict[str, float] = {}
    sources: dict[str, str] = {}
    for cell in (effective, global_cell):
        for metric in cell.metrics:
            if metric.name in metrics:
                continue
            value = metric.estimate
            if value is None:
                value = metric.quantiles.get("q0.5")
            if value is None or not math.isfinite(value):
                continue
            metrics[metric.name] = float(value)
            sources[metric.name] = cell.condition.condition_id
    if len(metrics) > MAX_BROKER_TRANSFER_METRICS:
        selected = sorted(metrics)[:MAX_BROKER_TRANSFER_METRICS]
        metrics = {name: metrics[name] for name in selected}
        sources = {name: sources[name] for name in selected}
    return dict(sorted(metrics.items())), dict(sorted(sources.items()))


def _drift_evidence(
    fingerprint: BrokerDeliveryFingerprintV1,
    comparison: BrokerDeliveryFingerprintComparisonV1 | None,
) -> tuple[str | None, int | None, str | None]:
    if comparison is None:
        return None, None, None
    if fingerprint.fingerprint_id not in {
        comparison.reference_fingerprint_id,
        comparison.candidate_fingerprint_id,
    }:
        return None, None, "drift_comparison_does_not_include_profile"
    return (
        comparison.comparison_id,
        comparison.material_drift_count,
        None,
    )


@dataclass(frozen=True, slots=True)
class BrokerRenderLineageV1:
    """Recoverable input/output identity for one rendered synthetic event."""

    symbol: str
    input_event_id: str
    output_event_id: str
    selection_id: str
    input_event_time_ns: int
    output_event_time_ns: int
    actions: tuple[str, ...]
    lineage_id: str = ""
    schema_version: str = BROKER_RENDER_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_RENDER_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported broker render lineage")
        object.__setattr__(self, "symbol", _required_name(self.symbol).upper())
        for name in ("input_event_id", "output_event_id", "selection_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in ("input_event_time_ns", "output_event_time_ns"):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 0, INT64_MAX),
            )
        actions = _bounded_text_tuple(self.actions, MAX_BROKER_TRANSFER_ACTIONS)
        if not actions:
            actions = ("profile_lineage_only",)
        object.__setattr__(self, "actions", actions)
        expected = _stable_id("broker-render-lineage", self.identity_payload())
        supplied = _optional_text(self.lineage_id)
        if supplied is not None and supplied != expected:
            raise ValueError("broker render lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "input_event_id": self.input_event_id,
            "output_event_id": self.output_event_id,
            "selection_id": self.selection_id,
            "input_event_time_ns": self.input_event_time_ns,
            "output_event_time_ns": self.output_event_time_ns,
            "time_shift_ns": (
                self.output_event_time_ns - self.input_event_time_ns
            ),
            "actions": list(self.actions),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerRenderLineageV1":
        _require_schema(data, BROKER_RENDER_LINEAGE_SCHEMA_VERSION)
        return cls(
            symbol=str(data.get("symbol", "")),
            input_event_id=str(data.get("input_event_id", "")),
            output_event_id=str(data.get("output_event_id", "")),
            selection_id=str(data.get("selection_id", "")),
            input_event_time_ns=_strict_int(
                data.get("input_event_time_ns"), "input_event_time_ns"
            ),
            output_event_time_ns=_strict_int(
                data.get("output_event_time_ns"), "output_event_time_ns"
            ),
            actions=_string_tuple(data.get("actions")),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerTransferManifestV1:
    """Compact profile, validation, and content evidence for one render."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    input_group_id: str
    fingerprint_id: str
    transfer_config: BrokerTransferConfigV1
    selections: tuple[BrokerProfileSelectionV1, ...]
    status: BrokerTransferStatus
    reason_codes: tuple[str, ...]
    input_content_sha256: str
    output_content_sha256: str | None
    observed_event_count: int
    synthetic_event_count: int
    action_counts: Mapping[str, int]
    lineage_count: int
    lineage_content_sha256: str | None
    local_validation_passed: bool
    post_broker_validation_id: str | None
    post_broker_validation_status: str | None
    cross_instrument_quality_status: str | None
    cross_instrument_quality_sha256: str | None
    benchmark_comparison_ids: tuple[str, ...] = ()
    manifest_id: str = ""
    schema_version: str = BROKER_TRANSFER_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_TRANSFER_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported broker transfer manifest")
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
            "input_group_id",
            "fingerprint_id",
            "input_content_sha256",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.transfer_config, BrokerTransferConfigV1):
            raise TypeError("transfer manifest requires a v1 config")
        selections = tuple(
            sorted(self.selections, key=lambda item: item.selection_id)
        )
        if not selections:
            raise ValueError("transfer manifest requires profile selections")
        if any(
            item.fingerprint_id != self.fingerprint_id for item in selections
        ):
            raise ValueError("transfer selections use different fingerprints")
        object.__setattr__(self, "selections", selections)
        status = BrokerTransferStatus(self.status)
        object.__setattr__(self, "status", status)
        reasons = _bounded_text_tuple(
            self.reason_codes, MAX_BROKER_TRANSFER_REASONS
        )
        object.__setattr__(self, "reason_codes", reasons)
        for name in ("output_content_sha256", "lineage_content_sha256"):
            object.__setattr__(
                self, name, _optional_sha256(getattr(self, name))
            )
        for name in (
            "post_broker_validation_id",
            "post_broker_validation_status",
            "cross_instrument_quality_status",
        ):
            object.__setattr__(self, name, _optional_text(getattr(self, name)))
        object.__setattr__(
            self,
            "cross_instrument_quality_sha256",
            _optional_sha256(self.cross_instrument_quality_sha256),
        )
        for name in (
            "observed_event_count",
            "synthetic_event_count",
            "lineage_count",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(
                    getattr(self, name), name, 0, MAX_BROKER_TRANSFER_EVENTS
                ),
            )
        actions = _count_mapping(self.action_counts)
        object.__setattr__(self, "action_counts", actions)
        if type(self.local_validation_passed) is not bool:
            raise ValueError("local_validation_passed must be boolean")
        comparisons = _bounded_text_tuple(
            self.benchmark_comparison_ids, MAX_BROKER_TRANSFER_REASONS
        )
        object.__setattr__(self, "benchmark_comparison_ids", comparisons)
        if status is BrokerTransferStatus.REFUSED:
            if not reasons or self.output_content_sha256 is not None:
                raise ValueError(
                    "refused render must retain reasons, not output"
                )
            if self.local_validation_passed:
                raise ValueError("refused render cannot pass local validation")
        else:
            if reasons:
                raise ValueError("applied render cannot retain refusal reasons")
            if (
                self.output_content_sha256 is None
                or self.lineage_content_sha256 is None
                or not self.local_validation_passed
                or self.post_broker_validation_status
                != CrossCurrencyValidationStatus.PASSED.value
                or self.cross_instrument_quality_status
                == QualityStatus.FAILED.value
            ):
                raise ValueError("applied render lacks passing final evidence")
            if self.synthetic_event_count != self.lineage_count:
                raise ValueError(
                    "render lineage count differs from synthetic rows"
                )
        expected = _stable_id(
            "broker-transfer-manifest", self.identity_payload()
        )
        supplied = _optional_text(self.manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("broker transfer manifest_id differs")
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": BROKER_TRANSFER_ENGINE_ID,
            "engine_version": BROKER_TRANSFER_ENGINE_VERSION,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "input_group_id": self.input_group_id,
            "fingerprint_id": self.fingerprint_id,
            "transfer_config": self.transfer_config.to_dict(),
            "selections": [item.to_dict() for item in self.selections],
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "input_content_sha256": self.input_content_sha256,
            "output_content_sha256": self.output_content_sha256,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "action_counts": dict(self.action_counts),
            "lineage_count": self.lineage_count,
            "lineage_content_sha256": self.lineage_content_sha256,
            "local_validation_passed": self.local_validation_passed,
            "post_broker_validation_id": self.post_broker_validation_id,
            "post_broker_validation_status": self.post_broker_validation_status,
            "cross_instrument_quality_status": (
                self.cross_instrument_quality_status
            ),
            "cross_instrument_quality_sha256": (
                self.cross_instrument_quality_sha256
            ),
            "benchmark_comparison_ids": list(self.benchmark_comparison_ids),
            "durable_event_rows_inline": False,
            "profile_effective_periods_embedded_in_selections": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerTransferManifestV1":
        _require_schema(data, BROKER_TRANSFER_MANIFEST_SCHEMA_VERSION)
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            input_group_id=str(data.get("input_group_id", "")),
            fingerprint_id=str(data.get("fingerprint_id", "")),
            transfer_config=BrokerTransferConfigV1.from_dict(
                _mapping(data.get("transfer_config"))
            ),
            selections=tuple(
                BrokerProfileSelectionV1.from_dict(item)
                for item in _mapping_sequence(data.get("selections"))
            ),
            status=BrokerTransferStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            input_content_sha256=str(data.get("input_content_sha256", "")),
            output_content_sha256=_optional_text(
                data.get("output_content_sha256")
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count"), "synthetic_event_count"
            ),
            action_counts={
                str(name): _strict_int(value, str(name))
                for name, value in _mapping(data.get("action_counts")).items()
            },
            lineage_count=_strict_int(
                data.get("lineage_count"), "lineage_count"
            ),
            lineage_content_sha256=_optional_text(
                data.get("lineage_content_sha256")
            ),
            local_validation_passed=_strict_bool(
                data.get("local_validation_passed"),
                "local_validation_passed",
            ),
            post_broker_validation_id=_optional_text(
                data.get("post_broker_validation_id")
            ),
            post_broker_validation_status=_optional_text(
                data.get("post_broker_validation_status")
            ),
            cross_instrument_quality_status=_optional_text(
                data.get("cross_instrument_quality_status")
            ),
            cross_instrument_quality_sha256=_optional_text(
                data.get("cross_instrument_quality_sha256")
            ),
            benchmark_comparison_ids=_string_tuple(
                data.get("benchmark_comparison_ids")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerRenderedGroupV1:
    """Process-local rendered streams plus compact durable evidence."""

    manifest: BrokerTransferManifestV1
    streams: tuple[SyntheticEventStreamV1, ...]
    event_lineage: tuple[BrokerRenderLineageV1, ...]
    post_broker_validation: CrossCurrencyValidationReportV1 | None
    cross_instrument_quality_payload: Mapping[str, JSONValue] | None
    schema_version: str = BROKER_RENDERED_GROUP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_RENDERED_GROUP_SCHEMA_VERSION:
            raise ValueError("unsupported broker rendered group")
        if not isinstance(self.manifest, BrokerTransferManifestV1):
            raise TypeError("rendered group requires a transfer manifest")
        streams = tuple(sorted(self.streams, key=lambda item: item.symbol))
        lineage = tuple(
            sorted(
                self.event_lineage,
                key=lambda item: (item.symbol, item.output_event_id),
            )
        )
        object.__setattr__(self, "streams", streams)
        object.__setattr__(self, "event_lineage", lineage)
        quality = self.cross_instrument_quality_payload
        if quality is not None:
            quality = _json_mapping_value(quality)
            object.__setattr__(
                self, "cross_instrument_quality_payload", quality
            )
        if self.manifest.status is BrokerTransferStatus.REFUSED:
            if streams or lineage or self.post_broker_validation is not None:
                raise ValueError("refused rendered group cannot expose output")
            if quality is not None:
                raise ValueError("refused rendered group cannot expose quality")
        else:
            if not streams or len(lineage) != self.manifest.lineage_count:
                raise ValueError("applied rendered group rows do not reconcile")
            validation = self.post_broker_validation
            if (
                validation is None
                or validation.validation_id
                != self.manifest.post_broker_validation_id
                or not validation.passed
            ):
                raise ValueError("rendered group lacks passing validation")
            if quality is None:
                raise ValueError("rendered group lacks #331 quality evidence")

    @property
    def status(self) -> BrokerTransferStatus:
        return self.manifest.status

    def metadata(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest": self.manifest.to_dict(),
            "stream_ids": [item.stream_id for item in self.streams],
            "event_rows_inline": False,
            "lineage_rows_inline": False,
            "post_broker_validation": (
                self.post_broker_validation.to_dict()
                if self.post_broker_validation is not None
                else None
            ),
            "cross_instrument_quality_sha256": (
                _content_sha256(self.cross_instrument_quality_payload)
                if self.cross_instrument_quality_payload is not None
                else None
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.metadata(),
            "streams": [item.to_dict() for item in self.streams],
            "event_lineage": [item.to_dict() for item in self.event_lineage],
            "cross_instrument_quality": (
                dict(self.cross_instrument_quality_payload)
                if self.cross_instrument_quality_payload is not None
                else None
            ),
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerRenderedGroupV1":
        _require_schema(data, BROKER_RENDERED_GROUP_SCHEMA_VERSION)
        validation = data.get("post_broker_validation")
        quality = data.get("cross_instrument_quality")
        return cls(
            manifest=BrokerTransferManifestV1.from_dict(
                _mapping(data.get("manifest"))
            ),
            streams=tuple(
                SyntheticEventStreamV1.from_dict(item)
                for item in _mapping_sequence(data.get("streams"))
            ),
            event_lineage=tuple(
                BrokerRenderLineageV1.from_dict(item)
                for item in _mapping_sequence(data.get("event_lineage"))
            ),
            post_broker_validation=(
                CrossCurrencyValidationReportV1.from_dict(_mapping(validation))
                if validation is not None
                else None
            ),
            cross_instrument_quality_payload=(
                _json_mapping_value(_mapping(quality))
                if quality is not None
                else None
            ),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerRenderedGroupV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerBenchmarkComparisonV1:
    """Paired reverse-degradation evidence without selecting a winner."""

    scorecard_id: str
    conditioned_candidate_id: str
    unconditioned_candidate_id: str
    scenario_ids: tuple[str, ...]
    conditioned_score_ids: tuple[str, ...]
    unconditioned_score_ids: tuple[str, ...]
    aggregate_metric_deltas: Mapping[str, Mapping[str, float]]
    comparison_id: str = ""
    schema_version: str = BROKER_BENCHMARK_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_BENCHMARK_COMPARISON_SCHEMA_VERSION:
            raise ValueError("unsupported broker benchmark comparison")
        for name in (
            "scorecard_id",
            "conditioned_candidate_id",
            "unconditioned_candidate_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.conditioned_candidate_id == self.unconditioned_candidate_id:
            raise ValueError("benchmark candidates must be distinct")
        scenarios = _bounded_text_tuple(
            self.scenario_ids, MAX_BROKER_TRANSFER_REASONS
        )
        conditioned = _bounded_text_tuple(
            self.conditioned_score_ids, MAX_BROKER_TRANSFER_REASONS
        )
        unconditioned = _bounded_text_tuple(
            self.unconditioned_score_ids, MAX_BROKER_TRANSFER_REASONS
        )
        if (
            not scenarios
            or len(conditioned) != len(scenarios)
            or len(unconditioned) != len(scenarios)
        ):
            raise ValueError("benchmark comparison cells do not reconcile")
        object.__setattr__(self, "scenario_ids", scenarios)
        object.__setattr__(self, "conditioned_score_ids", conditioned)
        object.__setattr__(self, "unconditioned_score_ids", unconditioned)
        deltas = {
            _required_text(scenario): _metric_mapping(values)
            for scenario, values in sorted(self.aggregate_metric_deltas.items())
        }
        if set(deltas) != set(scenarios):
            raise ValueError("benchmark deltas do not cover scenarios")
        object.__setattr__(self, "aggregate_metric_deltas", deltas)
        expected = _stable_id(
            "broker-benchmark-comparison", self.identity_payload()
        )
        supplied = _optional_text(self.comparison_id)
        if supplied is not None and supplied != expected:
            raise ValueError("broker benchmark comparison_id differs")
        object.__setattr__(self, "comparison_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scorecard_id": self.scorecard_id,
            "conditioned_candidate_id": self.conditioned_candidate_id,
            "unconditioned_candidate_id": self.unconditioned_candidate_id,
            "scenario_ids": list(self.scenario_ids),
            "conditioned_score_ids": list(self.conditioned_score_ids),
            "unconditioned_score_ids": list(self.unconditioned_score_ids),
            "aggregate_metric_deltas": {
                scenario: dict(values)
                for scenario, values in self.aggregate_metric_deltas.items()
            },
            "delta_semantics": "conditioned_minus_unconditioned",
            "automatic_winner": False,
            "winner_candidate_id": None,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "comparison_id": self.comparison_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerBenchmarkComparisonV1":
        _require_schema(data, BROKER_BENCHMARK_COMPARISON_SCHEMA_VERSION)
        return cls(
            scorecard_id=str(data.get("scorecard_id", "")),
            conditioned_candidate_id=str(
                data.get("conditioned_candidate_id", "")
            ),
            unconditioned_candidate_id=str(
                data.get("unconditioned_candidate_id", "")
            ),
            scenario_ids=_string_tuple(data.get("scenario_ids")),
            conditioned_score_ids=_string_tuple(
                data.get("conditioned_score_ids")
            ),
            unconditioned_score_ids=_string_tuple(
                data.get("unconditioned_score_ids")
            ),
            aggregate_metric_deltas={
                str(scenario): {
                    str(name): _finite_float(value, str(name))
                    for name, value in _mapping(values).items()
                }
                for scenario, values in _mapping(
                    data.get("aggregate_metric_deltas")
                ).items()
            },
            comparison_id=str(data.get("comparison_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerBenchmarkComparisonV1":
        return cls.from_dict(_json_mapping(text))


def compare_broker_benchmark_results(
    scorecard: ReverseDegradationScorecardV1,
    *,
    conditioned_candidate_id: str,
    unconditioned_candidate_id: str,
) -> BrokerBenchmarkComparisonV1:
    """Pair conditioned/unconditioned scorecard cells by scenario."""
    conditioned = {
        item.scenario_id: item
        for item in scorecard.candidate_scores
        if item.candidate_id == conditioned_candidate_id
    }
    unconditioned = {
        item.scenario_id: item
        for item in scorecard.candidate_scores
        if item.candidate_id == unconditioned_candidate_id
    }
    if not conditioned or set(conditioned) != set(unconditioned):
        raise ValueError(
            "conditioned and unconditioned benchmark scenario support differs"
        )
    scenarios = tuple(sorted(conditioned))
    deltas: dict[str, dict[str, float]] = {}
    for scenario in scenarios:
        left = conditioned[scenario]
        right = unconditioned[scenario]
        names = sorted(
            set(left.aggregate_metrics).intersection(right.aggregate_metrics)
        )
        if not names:
            raise ValueError("paired benchmark cell has no common metrics")
        deltas[scenario] = {
            name: _rounded(
                left.aggregate_metrics[name] - right.aggregate_metrics[name],
                12,
            )
            for name in names
        }
    return BrokerBenchmarkComparisonV1(
        scorecard_id=scorecard.scorecard_id,
        conditioned_candidate_id=conditioned_candidate_id,
        unconditioned_candidate_id=unconditioned_candidate_id,
        scenario_ids=scenarios,
        conditioned_score_ids=tuple(
            conditioned[item].candidate_score_id for item in scenarios
        ),
        unconditioned_score_ids=tuple(
            unconditioned[item].candidate_score_id for item in scenarios
        ),
        aggregate_metric_deltas=deltas,
    )


@dataclass(slots=True)
class _PendingRender:
    source: SyntheticEventV1
    event_time_ns: int
    bid: float
    ask: float
    actions: list[str] = field(default_factory=list)


def render_broker_delivery(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    group: CrossCurrencyReconciledGroupV1,
    fingerprint: BrokerDeliveryFingerprintV1,
    constraints: HistoricalCarvingConstraintSetV1,
    selected_at_utc_ns: int,
    requested_conditions: Mapping[str, Mapping[str, str]] | None = None,
    config: BrokerTransferConfigV1 | None = None,
    drift_comparison: BrokerDeliveryFingerprintComparisonV1 | None = None,
    benchmark_comparisons: Sequence[BrokerBenchmarkComparisonV1] = (),
    quality_period: str = "broker-render",
) -> BrokerRenderedGroupV1:
    """Render one reconciled synchronized group and fail closed on validation."""
    policy = config or BrokerTransferConfigV1()
    _validate_render_scope(run, window, group, constraints)
    conditions = {
        str(symbol).strip().lower(): dict(value)
        for symbol, value in (requested_conditions or {}).items()
    }
    selections = tuple(
        select_broker_profile(
            fingerprint,
            requested_condition=conditions.get(symbol.lower(), {}),
            selected_at_utc_ns=selected_at_utc_ns,
            drift_comparison=drift_comparison,
        )
        for symbol in group.symbols
    )
    input_hash = _streams_content_sha256(group.streams)
    observed_count = sum(item.observed_event_count for item in group.streams)
    synthetic_count = sum(item.synthetic_event_count for item in group.streams)
    comparison_ids = tuple(
        sorted(item.comparison_id for item in benchmark_comparisons)
    )
    if group.status is not CrossCurrencyGroupStatus.RECONCILED:
        return _refused_render(
            run,
            window,
            group,
            fingerprint,
            policy,
            selections,
            input_hash,
            observed_count,
            synthetic_count,
            ("input_cross_currency_group_not_reconciled",),
            comparison_ids,
        )
    refused = tuple(item for item in selections if not item.applied)
    if refused:
        reasons = tuple(
            sorted(
                {
                    f"profile_selection:{reason}"
                    for item in refused
                    for reason in item.reason_codes
                }
            )
        )
        return _refused_render(
            run,
            window,
            group,
            fingerprint,
            policy,
            selections,
            input_hash,
            observed_count,
            synthetic_count,
            reasons or ("profile_selection_refused",),
            comparison_ids,
        )
    total_events = observed_count + synthetic_count
    if total_events > policy.max_events_per_group:
        return _refused_render(
            run,
            window,
            group,
            fingerprint,
            policy,
            selections,
            input_hash,
            observed_count,
            synthetic_count,
            ("max_events_per_group_exceeded",),
            comparison_ids,
        )
    by_symbol = {
        symbol: next(
            item
            for item in selections
            if _selection_symbol(item) in {None, symbol.upper()}
        )
        for symbol in group.symbols
    }
    shared = _shared_render_parameters(tuple(by_symbol.values()), policy)
    streams: list[SyntheticEventStreamV1] = []
    lineage: list[BrokerRenderLineageV1] = []
    render_reasons: list[str] = []
    for stream in group.streams:
        rendered, rows, reason = _render_stream(
            run=run,
            window=window,
            stream=stream,
            fingerprint_id=fingerprint.fingerprint_id,
            selection=by_symbol[stream.symbol],
            config=policy,
            constraints=constraints,
            shared=shared,
        )
        if reason is not None:
            render_reasons.append(f"{stream.symbol}:{reason}")
            continue
        assert rendered is not None
        streams.append(rendered)
        lineage.extend(rows)
    if render_reasons or len(streams) != len(group.symbols):
        return _refused_render(
            run,
            window,
            group,
            fingerprint,
            policy,
            selections,
            input_hash,
            observed_count,
            synthetic_count,
            tuple(sorted(render_reasons or ("rendered_symbol_missing",))),
            comparison_ids,
        )
    stream_tuple = tuple(sorted(streams, key=lambda item: item.symbol))
    local_reasons = _local_validation_reasons(
        group.streams,
        stream_tuple,
        fingerprint.fingerprint_id,
        constraints,
    )
    if local_reasons:
        return _refused_render(
            run,
            window,
            group,
            fingerprint,
            policy,
            selections,
            input_hash,
            observed_count,
            synthetic_count,
            local_reasons,
            comparison_ids,
        )
    observed_anchors = tuple(
        event
        for stream in group.streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    post_broker = validate_cross_currency_output(
        run=run,
        window=window,
        streams={item.symbol: item for item in stream_tuple},
        config=group.config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=observed_anchors,
    )
    if not post_broker.passed:
        reasons = tuple(
            sorted(
                f"post_broker:{reason}"
                for reason in post_broker.failure_reasons
            )
        )
        return _refused_render(
            run,
            window,
            group,
            fingerprint,
            policy,
            selections,
            input_hash,
            observed_count,
            synthetic_count,
            reasons or ("post_broker_cross_currency_validation_failed",),
            comparison_ids,
        )
    quality_validation = validate_cross_currency_output(
        run=run,
        window=window,
        streams={item.symbol: item for item in stream_tuple},
        config=group.config,
        stage=CrossCurrencyValidationStage.GENERATION,
        observed_anchors=observed_anchors,
    )
    quality_group = CrossCurrencyReconciledGroupV1(
        run_id=group.run_id,
        window_id=group.window_id,
        synchronization_unit_id=group.synchronization_unit_id,
        ensemble_member_id=group.ensemble_member_id,
        symbols=group.symbols,
        status=CrossCurrencyGroupStatus.RECONCILED,
        streams=stream_tuple,
        missing_symbols=(),
        input_stream_ids={item.symbol: item.stream_id for item in stream_tuple},
        config=group.config,
        condition_ids=group.condition_ids,
        projection_lineage=(),
        generation_validation=quality_validation,
    )
    quality_report = cross_currency_quality_report(
        quality_group,
        period=quality_period,
    )
    if quality_report.status is QualityStatus.FAILED:
        return _refused_render(
            run,
            window,
            group,
            fingerprint,
            policy,
            selections,
            input_hash,
            observed_count,
            synthetic_count,
            ("cross_instrument_quality_failed",),
            comparison_ids,
        )
    lineage_tuple = tuple(
        sorted(lineage, key=lambda item: (item.symbol, item.output_event_id))
    )
    quality_payload = cast(Mapping[str, JSONValue], quality_report.to_dict())
    action_counts = Counter(
        action for item in lineage_tuple for action in item.actions
    )
    manifest = BrokerTransferManifestV1(
        run_id=run.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        ensemble_member_id=window.ensemble_member_id,
        input_group_id=group.group_id,
        fingerprint_id=fingerprint.fingerprint_id,
        transfer_config=policy,
        selections=selections,
        status=(
            BrokerTransferStatus.BACKED_OFF
            if any(
                item.status is BrokerTransferStatus.BACKED_OFF
                for item in selections
            )
            else BrokerTransferStatus.APPLIED
        ),
        reason_codes=(),
        input_content_sha256=input_hash,
        output_content_sha256=_streams_content_sha256(stream_tuple),
        observed_event_count=observed_count,
        synthetic_event_count=synthetic_count,
        action_counts=dict(action_counts),
        lineage_count=len(lineage_tuple),
        lineage_content_sha256=_content_sha256(
            [item.to_dict() for item in lineage_tuple]
        ),
        local_validation_passed=True,
        post_broker_validation_id=post_broker.validation_id,
        post_broker_validation_status=post_broker.status.value,
        cross_instrument_quality_status=quality_report.status.value,
        cross_instrument_quality_sha256=_content_sha256(quality_payload),
        benchmark_comparison_ids=comparison_ids,
    )
    return BrokerRenderedGroupV1(
        manifest=manifest,
        streams=stream_tuple,
        event_lineage=lineage_tuple,
        post_broker_validation=post_broker,
        cross_instrument_quality_payload=quality_payload,
    )


def _validate_render_scope(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    group: CrossCurrencyReconciledGroupV1,
    constraints: HistoricalCarvingConstraintSetV1,
) -> None:
    if run.run_id != window.run_id or group.run_id != run.run_id:
        raise ValueError("broker render run scope differs")
    if group.window_id != window.window_id:
        raise ValueError("broker render window scope differs")
    if (
        group.synchronization_unit_id != window.synchronization_unit_id
        or group.ensemble_member_id != window.ensemble_member_id
        or group.symbols != window.symbols
    ):
        raise ValueError("broker render synchronization scope differs")
    if not isinstance(constraints, HistoricalCarvingConstraintSetV1):
        raise TypeError("broker render requires historical constraints")


def _selection_symbol(selection: BrokerProfileSelectionV1) -> str | None:
    value = selection.requested_condition.get("symbol")
    return str(value).upper() if value is not None else None


def _shared_render_parameters(
    selections: Sequence[BrokerProfileSelectionV1],
    config: BrokerTransferConfigV1,
) -> dict[str, float | int]:
    precisions = [
        item.metrics["source_timestamp_precision_ns"]
        for item in selections
        if "source_timestamp_precision_ns" in item.metrics
    ]
    target_precision = max(precisions) if precisions else 1.0
    effective_precision = max(
        1,
        round(_blend(1.0, target_precision, config.strength)),
    )
    batches = [
        item.metrics["source_batch_quote_count"]
        for item in selections
        if "source_batch_quote_count" in item.metrics
    ]
    target_batch = min(batches) if batches else 1.0
    effective_batch = max(
        1,
        min(
            config.max_batch_size,
            round(_blend(1.0, target_batch, config.strength)),
        ),
    )
    stale_rates = [
        item.metrics.get("stale_quote_rate", 0.0) for item in selections
    ]
    duplicate_rates = [
        item.metrics.get("exact_duplicate_rate", 0.0) for item in selections
    ]
    return {
        "precision_ns": effective_precision,
        "batch_size": effective_batch,
        "stale_rate": min(stale_rates, default=0.0),
        "duplicate_rate": min(duplicate_rates, default=0.0),
    }


def _render_stream(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    stream: SyntheticEventStreamV1,
    fingerprint_id: str,
    selection: BrokerProfileSelectionV1,
    config: BrokerTransferConfigV1,
    constraints: HistoricalCarvingConstraintSetV1,
    shared: Mapping[str, float | int],
) -> tuple[
    SyntheticEventStreamV1 | None,
    tuple[BrokerRenderLineageV1, ...],
    str | None,
]:
    observed = tuple(
        item
        for item in stream.events
        if item.origin is SyntheticEventOrigin.OBSERVED
    )
    synthetic = tuple(
        item
        for item in stream.events
        if item.origin is SyntheticEventOrigin.SYNTHETIC
    )
    anchors = {item.event_id: item for item in observed}
    if any(
        item.constraint_set_id != constraints.constraint_set_id
        for item in synthetic
    ):
        return None, (), "constraint_set_id_differs"
    precision_ns = cast(int, shared["precision_ns"])
    batch_size = cast(int, shared["batch_size"])
    batch_targets = _batch_targets(
        synthetic,
        batch_size if config.apply_batching else 1,
    )
    pending: list[_PendingRender] = []
    previous_quote: tuple[float, float] | None = None
    for source in synthetic:
        left = anchors.get(cast(str, source.left_anchor_event_id))
        right = anchors.get(cast(str, source.right_anchor_event_id))
        if left is None or right is None:
            return None, (), "anchor_lineage_missing"
        if not left.event_time_ns < source.event_time_ns < right.event_time_ns:
            return None, (), "input_synthetic_event_outside_anchors"
        actions: list[str] = []
        requested_time = batch_targets.get(
            source.event_id, source.event_time_ns
        )
        if requested_time != source.event_time_ns:
            actions.append("batched_timestamp")
        quantized_time = _quantize_ns(requested_time, precision_ns)
        if quantized_time != requested_time:
            actions.append("timestamp_precision")
        lower = max(
            left.event_time_ns + 1,
            source.event_time_ns - config.max_timestamp_shift_ns,
        )
        upper = min(
            right.event_time_ns - 1,
            source.event_time_ns + config.max_timestamp_shift_ns,
        )
        event_time = max(lower, min(upper, quantized_time))
        if _is_quarantined(stream.symbol, event_time, constraints):
            if _is_quarantined(
                stream.symbol, source.event_time_ns, constraints
            ):
                return None, (), "input_event_inside_quarantine"
            event_time = source.event_time_ns
            actions.append("constraint_time_preserved")
        score = _deterministic_rate(
            run,
            window,
            source,
            config,
        )
        duplicate_rate = (
            cast(float, shared["duplicate_rate"]) * config.strength
            if config.apply_exact_duplicates
            else 0.0
        )
        stale_rate = (
            cast(float, shared["stale_rate"]) * config.strength
            if config.apply_stale_behavior
            else 0.0
        )
        bid = source.bid
        ask = source.ask
        if previous_quote is not None and score < duplicate_rate:
            bid, ask = previous_quote
            actions.append("exact_duplicate_quote")
        elif previous_quote is not None and score < duplicate_rate + stale_rate:
            bid, ask = previous_quote
            actions.append("stale_quote")
        else:
            bid, ask, projected = _render_quote(
                source,
                selection,
                config,
                constraints,
            )
            if projected:
                actions.append("spread_projection")
        digits = _effective_price_digits(selection, config)
        rounded_bid = round(bid, digits)
        rounded_ask = round(ask, digits)
        if rounded_bid != bid or rounded_ask != ask:
            actions.append("price_precision")
        bid, ask = rounded_bid, rounded_ask
        if not (
            math.isfinite(bid)
            and math.isfinite(ask)
            and bid > 0.0
            and ask > 0.0
            and ask >= bid
        ):
            return None, (), "rendered_quote_domain_invalid"
        previous_quote = (bid, ask)
        pending.append(
            _PendingRender(
                source=source,
                event_time_ns=event_time,
                bid=bid,
                ask=ask,
                actions=actions,
            )
        )
    used_positions = {
        (item.event_time_ns, item.event_sequence) for item in observed
    }
    rendered: list[SyntheticEventV1] = []
    lineage: list[BrokerRenderLineageV1] = []
    for row in sorted(
        pending,
        key=lambda item: (
            item.event_time_ns,
            item.source.event_time_ns,
            item.source.event_sequence,
            item.source.event_id,
        ),
    ):
        sequence = row.source.event_sequence
        while (row.event_time_ns, sequence) in used_positions:
            sequence += 1
        used_positions.add((row.event_time_ns, sequence))
        source = row.source
        event = SyntheticEventV1.generated(
            symbol=source.symbol,
            event_time_ns=row.event_time_ns,
            event_sequence=sequence,
            bid=row.bid,
            ask=row.ask,
            run_id=source.run_id,
            ensemble_member_id=source.ensemble_member_id,
            source_version_id=source.source_version_id,
            anchor_interval_id=source.anchor_interval_id,
            left_anchor_event_id=cast(str, source.left_anchor_event_id),
            right_anchor_event_id=cast(str, source.right_anchor_event_id),
            generator_id=cast(str, source.generator_id),
            generator_version=cast(str, source.generator_version),
            generator_config_id=cast(str, source.generator_config_id),
            reference_id=source.reference_id,
            motif_id=source.motif_id,
            feed_epoch_id=source.feed_epoch_id,
            broker_profile_id=fingerprint_id,
            constraint_set_id=cast(str, source.constraint_set_id),
            confidence=source.confidence,
        )
        rendered.append(event)
        lineage.append(
            BrokerRenderLineageV1(
                symbol=source.symbol,
                input_event_id=source.event_id,
                output_event_id=event.event_id,
                selection_id=selection.selection_id,
                input_event_time_ns=source.event_time_ns,
                output_event_time_ns=event.event_time_ns,
                actions=tuple(sorted(set(row.actions))),
            )
        )
    try:
        output = SyntheticEventStreamV1.merge(
            run_id=stream.run_id,
            ensemble_member_id=stream.ensemble_member_id,
            symbol=stream.symbol,
            observed_events=observed,
            synthetic_events=rendered,
            source_version_ids=stream.source_version_ids,
        )
    except ValueError as err:
        return None, (), f"rendered_stream_invalid:{err}"
    return output, tuple(lineage), None


def _batch_targets(
    events: Sequence[SyntheticEventV1], batch_size: int
) -> dict[str, int]:
    if batch_size <= 1:
        return {}
    by_interval: dict[str, list[SyntheticEventV1]] = {}
    for event in events:
        by_interval.setdefault(cast(str, event.anchor_interval_id), []).append(
            event
        )
    targets: dict[str, int] = {}
    for interval_events in by_interval.values():
        ordered = sorted(
            interval_events,
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.event_id,
            ),
        )
        for start in range(0, len(ordered), batch_size):
            chunk = ordered[start : start + batch_size]
            if len(chunk) <= 1:
                continue
            target = chunk[-1].event_time_ns
            targets.update({item.event_id: target for item in chunk})
    return targets


def _render_quote(
    event: SyntheticEventV1,
    selection: BrokerProfileSelectionV1,
    config: BrokerTransferConfigV1,
    constraints: HistoricalCarvingConstraintSetV1,
) -> tuple[float, float, bool]:
    target = selection.metrics.get("spread")
    if target is None or target < 0.0:
        return event.bid, event.ask, False
    spread = event.ask - event.bid
    maximum_multiplier = min(
        config.max_spread_multiplier,
        constraints.max_combined_spread_multiplier,
    )
    bounded_target = min(target, spread * maximum_multiplier)
    rendered_spread = _blend(spread, bounded_target, config.strength)
    midpoint = (event.bid + event.ask) / 2.0
    bid = midpoint - rendered_spread / 2.0
    ask = midpoint + rendered_spread / 2.0
    return (
        bid,
        ask,
        not math.isclose(rendered_spread, spread, rel_tol=0.0, abs_tol=1e-15),
    )


def _effective_price_digits(
    selection: BrokerProfileSelectionV1,
    config: BrokerTransferConfigV1,
) -> int:
    target = selection.metrics.get(
        "price_decimal_places", float(config.input_price_decimal_places)
    )
    value = round(
        _blend(
            float(config.input_price_decimal_places),
            target,
            config.strength,
        )
    )
    return max(
        config.minimum_price_decimal_places,
        min(config.input_price_decimal_places, value),
    )


def _deterministic_rate(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    event: SyntheticEventV1,
    config: BrokerTransferConfigV1,
) -> float:
    key = (
        f"{BROKER_TRANSFER_ENGINE_ID}:{config.config_id}:"
        f"{window.synchronization_unit_id}:{event.event_time_ns}:"
        f"{event.event_sequence}:delivery-behavior"
    )
    return float(run.seed_for(window.ensemble_member_id, key)) / float(2**64)


def _is_quarantined(
    symbol: str,
    event_time_ns: int,
    constraints: HistoricalCarvingConstraintSetV1,
) -> bool:
    normalized = symbol.upper()
    return any(
        item.symbol == normalized
        and item.start_ns <= event_time_ns < item.end_ns
        for item in constraints.quarantines
    )


def _local_validation_reasons(
    inputs: Sequence[SyntheticEventStreamV1],
    outputs: Sequence[SyntheticEventStreamV1],
    fingerprint_id: str,
    constraints: HistoricalCarvingConstraintSetV1,
) -> tuple[str, ...]:
    input_observed = {
        item.event_id: item.to_dict()
        for stream in inputs
        for item in stream.events
        if item.origin is SyntheticEventOrigin.OBSERVED
    }
    output_observed = {
        item.event_id: item.to_dict()
        for stream in outputs
        for item in stream.events
        if item.origin is SyntheticEventOrigin.OBSERVED
    }
    reasons: list[str] = []
    if input_observed != output_observed:
        reasons.append("observed_anchor_content_changed")
    for stream in outputs:
        anchors = {
            item.event_id: item
            for item in stream.events
            if item.origin is SyntheticEventOrigin.OBSERVED
        }
        previous: tuple[int, int] | None = None
        for event in stream.events:
            position = (event.event_time_ns, event.event_sequence)
            if previous is not None and position <= previous:
                reasons.append(f"event_order_invalid:{stream.symbol}")
            previous = position
            if event.ask < event.bid:
                reasons.append(f"negative_spread:{stream.symbol}")
            if event.origin is SyntheticEventOrigin.OBSERVED:
                continue
            if event.broker_profile_id != fingerprint_id:
                reasons.append(
                    f"broker_profile_lineage_missing:{stream.symbol}"
                )
            if event.constraint_set_id != constraints.constraint_set_id:
                reasons.append(f"constraint_lineage_changed:{stream.symbol}")
            left = anchors.get(cast(str, event.left_anchor_event_id))
            right = anchors.get(cast(str, event.right_anchor_event_id))
            if (
                left is None
                or right is None
                or not left.event_time_ns
                < event.event_time_ns
                < right.event_time_ns
            ):
                reasons.append(f"anchor_interval_violation:{stream.symbol}")
            if _is_quarantined(stream.symbol, event.event_time_ns, constraints):
                reasons.append(f"forbidden_interval_violation:{stream.symbol}")
    return tuple(sorted(set(reasons)))


def _refused_render(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    group: CrossCurrencyReconciledGroupV1,
    fingerprint: BrokerDeliveryFingerprintV1,
    config: BrokerTransferConfigV1,
    selections: Sequence[BrokerProfileSelectionV1],
    input_hash: str,
    observed_count: int,
    synthetic_count: int,
    reasons: Sequence[str],
    benchmark_comparison_ids: Sequence[str],
) -> BrokerRenderedGroupV1:
    manifest = BrokerTransferManifestV1(
        run_id=run.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        ensemble_member_id=window.ensemble_member_id,
        input_group_id=group.group_id,
        fingerprint_id=fingerprint.fingerprint_id,
        transfer_config=config,
        selections=tuple(selections),
        status=BrokerTransferStatus.REFUSED,
        reason_codes=tuple(sorted(set(reasons))),
        input_content_sha256=input_hash,
        output_content_sha256=None,
        observed_event_count=observed_count,
        synthetic_event_count=synthetic_count,
        action_counts={},
        lineage_count=0,
        lineage_content_sha256=None,
        local_validation_passed=False,
        post_broker_validation_id=None,
        post_broker_validation_status=None,
        cross_instrument_quality_status=None,
        cross_instrument_quality_sha256=None,
        benchmark_comparison_ids=tuple(benchmark_comparison_ids),
    )
    return BrokerRenderedGroupV1(
        manifest=manifest,
        streams=(),
        event_lineage=(),
        post_broker_validation=None,
        cross_instrument_quality_payload=None,
    )


def _quantize_ns(value: int, precision: int) -> int:
    if precision <= 1:
        return value
    return ((2 * value + precision) // (2 * precision)) * precision


def _blend(historical: float, broker: float, strength: float) -> float:
    return historical + strength * (broker - historical)


def _rounded(value: float, digits: int) -> float:
    rounded = round(float(value), digits)
    return 0.0 if rounded == 0.0 else rounded


def _streams_content_sha256(
    streams: Iterable[SyntheticEventStreamV1],
) -> str:
    return _content_sha256(
        [
            item.to_dict()
            for item in sorted(streams, key=lambda stream: stream.symbol)
        ]
    )


def _content_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    return f"{prefix}:sha256:{_content_sha256(payload)}"


def _required_text(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > MAX_BROKER_TRANSFER_TEXT:
        raise ValueError("required broker transfer text is empty or unbounded")
    return normalized


def _required_name(value: Any) -> str:
    normalized = _required_text(value)
    if any(character.isspace() for character in normalized):
        raise ValueError("broker transfer names cannot contain whitespace")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return _required_text(normalized) if normalized else None


def _optional_sha256(value: Any) -> str | None:
    normalized = _optional_text(value)
    if normalized is None:
        return None
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError("expected a lowercase SHA-256 digest")
    return normalized


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    normalized = _strict_int(value, name)
    if not minimum <= normalized <= maximum:
        raise ValueError(f"{name} is outside supported bounds")
    return normalized


def _strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{name} must be boolean")
    return value


def _optional_int(value: Any) -> int | None:
    return None if value is None else _strict_int(value, "optional integer")


def _finite_float(value: Any, name: str) -> float:
    if type(value) not in {int, float}:
        raise ValueError(f"{name} must be numeric")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{name} must be finite")
    return normalized


def _metric_mapping(values: Mapping[str, Any]) -> dict[str, float]:
    if len(values) > MAX_BROKER_TRANSFER_METRICS:
        raise ValueError("broker transfer metrics exceed bounded limit")
    return {
        _required_name(str(name)): _finite_float(value, str(name))
        for name, value in sorted(values.items())
    }


def _count_mapping(values: Mapping[str, Any]) -> dict[str, int]:
    if len(values) > MAX_BROKER_TRANSFER_ACTIONS:
        raise ValueError("broker transfer actions exceed bounded limit")
    return {
        _required_name(str(name)): _bounded_int(
            value, str(name), 0, MAX_BROKER_TRANSFER_EVENTS
        )
        for name, value in sorted(values.items())
    }


def _bounded_text_tuple(values: Iterable[Any], maximum: int) -> tuple[str, ...]:
    normalized = tuple(_required_text(value) for value in values)
    if len(normalized) > maximum:
        raise ValueError("broker transfer text collection exceeds limit")
    return normalized


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
    return tuple(str(item) for item in value)


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)


def _json_mapping_value(value: Mapping[str, Any]) -> Mapping[str, JSONValue]:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return _mapping(json.loads(encoded))


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError("unsupported broker transfer schema")


__all__ = [
    "BROKER_BENCHMARK_COMPARISON_SCHEMA_VERSION",
    "BROKER_CONDITIONED_PROPOSAL_SCHEMA_VERSION",
    "BROKER_PROFILE_SELECTION_SCHEMA_VERSION",
    "BROKER_RENDERED_GROUP_SCHEMA_VERSION",
    "BROKER_RENDER_LINEAGE_SCHEMA_VERSION",
    "BROKER_TRANSFER_CONFIG_SCHEMA_VERSION",
    "BROKER_TRANSFER_ENGINE_ID",
    "BROKER_TRANSFER_ENGINE_VERSION",
    "BROKER_TRANSFER_MANIFEST_SCHEMA_VERSION",
    "BrokerBenchmarkComparisonV1",
    "BrokerConditionedProposalV1",
    "BrokerProfileSelectionV1",
    "BrokerRenderedGroupV1",
    "BrokerRenderLineageV1",
    "BrokerTransferConfigV1",
    "BrokerTransferManifestV1",
    "BrokerTransferStatus",
    "compare_broker_benchmark_results",
    "condition_broker_proposal",
    "render_broker_delivery",
    "select_broker_profile",
]
