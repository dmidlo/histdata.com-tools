"""Qualified cardinality conditioning for historical reconstruction products."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.observation import (
    ObservationContextV1,
    ObservationOperatorV1,
    ObservationParameterEstimateV1,
    ObservationStratumV1,
)

HISTORICAL_PRODUCT_OBSERVATION_CONDITIONING_SCHEMA_VERSION = (
    "histdatacom.historical-product-observation-conditioning.v2"
)
TRANSITION_RETENTION_BRIDGE_MODE = "ex-post-adjacent-epoch-linear-transition-v1"


def historical_product_observation_conditioning(
    operator: ObservationOperatorV1,
    *,
    feed_epoch_label: str,
    symbols: Sequence[str],
    information_mode: InformationMode,
    used_at_ns: int | None = None,
    feed_epoch_definition: Any | None = None,
) -> dict[str, JSONValue]:
    """Resolve stable-epoch or explicit transition cardinality evidence."""
    label = str(feed_epoch_label).strip()
    normalized_symbols = tuple(sorted({str(item).upper() for item in symbols}))
    if not label or not normalized_symbols:
        raise ValueError(
            "historical product conditioning requires epoch and symbols"
        )
    mode = InformationMode.from_value(information_mode)
    if label.startswith("transition:"):
        joint, symbol_evidence, resolution = _transition_conditioning(
            operator,
            feed_epoch_label=label,
            symbols=normalized_symbols,
            information_mode=mode,
            used_at_ns=used_at_ns,
            feed_epoch_definition=feed_epoch_definition,
        )
    else:
        joint, symbol_evidence = _stable_epoch_conditioning(
            operator,
            feed_epoch_label=label,
            symbols=normalized_symbols,
        )
        resolution = {
            "resolution_basis": (
                "synchronized-epoch-aggregate-for-qualified-"
                "multivariate-cardinality-v1"
            ),
            "conditioning_mode": "fitted-stable-epoch-v1",
        }
    identity: dict[str, JSONValue] = {
        "schema_version": (
            HISTORICAL_PRODUCT_OBSERVATION_CONDITIONING_SCHEMA_VERSION
        ),
        "observation_operator_id": operator.operator_id,
        "feed_epoch_definition_id": operator.feed_epoch_definition_id,
        "feed_epoch_id": label,
        "information_mode": mode.value,
        **resolution,
        "parameter_policy": "fitted-point-estimate-with-recorded-uncertainty-v1",
        "joint_retention": joint,
        "symbol_retention_diagnostics_only": True,
        "symbols": symbol_evidence,
    }
    digest = hashlib.sha256(
        canonical_contract_json(identity).encode("utf-8")
    ).hexdigest()
    return {
        **identity,
        "conditioning_id": (
            "historical-product-observation-conditioning:sha256:" + digest
        ),
    }


def historical_product_retention_probability(
    operator: ObservationOperatorV1,
    *,
    feed_epoch_label: str,
    information_mode: InformationMode,
    used_at_ns: int | None = None,
    feed_epoch_definition: Any | None = None,
    retention_endpoint: str = "central",
) -> float:
    """Resolve a declared joint endpoint without materializing full evidence."""
    label = str(feed_epoch_label).strip()
    if not label:
        raise ValueError("historical product conditioning requires an epoch")
    mode = InformationMode.from_value(information_mode)
    if label.startswith("transition:"):
        joint, _, _ = _transition_conditioning(
            operator,
            feed_epoch_label=label,
            symbols=(),
            information_mode=mode,
            used_at_ns=used_at_ns,
            feed_epoch_definition=feed_epoch_definition,
        )
    else:
        joint = _resolved_retention_evidence(
            operator,
            ObservationContextV1(symbol="GLOBAL", epoch_id=label),
        )
        if joint["stratum_level"] != "epoch":
            raise ValueError(
                "historical product joint retention lacks an epoch aggregate"
            )
    endpoint = str(retention_endpoint).strip().lower()
    key = {
        "central": "retention_probability",
        "lower": "retention_lower_bound",
        "upper": "retention_upper_bound",
    }.get(endpoint)
    if key is None:
        raise ValueError("unknown historical retention endpoint")
    retention = joint[key]
    if isinstance(retention, bool) or not isinstance(retention, (int, float)):
        raise TypeError("historical product retention is not numeric")
    selected = float(retention)
    if not 0.0 < selected <= 1.0:
        raise ValueError("historical product retention endpoint is invalid")
    return selected


def _stable_epoch_conditioning(
    operator: ObservationOperatorV1,
    *,
    feed_epoch_label: str,
    symbols: Sequence[str],
) -> tuple[dict[str, JSONValue], dict[str, JSONValue]]:
    symbol_evidence: dict[str, JSONValue] = {
        symbol: _resolved_retention_evidence(
            operator,
            ObservationContextV1(symbol=symbol, epoch_id=feed_epoch_label),
        )
        for symbol in symbols
    }
    joint = _resolved_retention_evidence(
        operator,
        ObservationContextV1(symbol="GLOBAL", epoch_id=feed_epoch_label),
    )
    if joint["stratum_level"] != "epoch":
        raise ValueError(
            "historical product joint retention lacks an epoch aggregate"
        )
    return joint, symbol_evidence


def _transition_conditioning(
    operator: ObservationOperatorV1,
    *,
    feed_epoch_label: str,
    symbols: Sequence[str],
    information_mode: InformationMode,
    used_at_ns: int | None,
    feed_epoch_definition: Any | None,
) -> tuple[dict[str, JSONValue], dict[str, JSONValue], dict[str, JSONValue]]:
    if information_mode is not InformationMode.EX_POST_RECONSTRUCTION:
        raise ValueError(
            "transition retention bridging is qualified only for ex-post "
            "reconstruction"
        )
    if feed_epoch_definition is None or used_at_ns is None:
        raise ValueError(
            "transition conditioning lacks definition or decision time"
        )
    if (
        getattr(feed_epoch_definition, "definition_id", None)
        != operator.feed_epoch_definition_id
    ):
        raise ValueError(
            "transition definition differs from observation operator"
        )
    boundaries = tuple(getattr(feed_epoch_definition, "boundaries", ()))
    matches = tuple(
        (index, boundary)
        for index, boundary in enumerate(boundaries)
        if getattr(boundary, "transition_label", None) == feed_epoch_label
    )
    if len(matches) != 1:
        raise ValueError("transition label does not resolve exactly once")
    boundary_index, boundary = matches[0]
    epochs = tuple(getattr(feed_epoch_definition, "epochs", ()))
    if boundary_index + 1 >= len(epochs):
        raise ValueError("transition lacks adjacent fitted epochs")
    left_label = str(epochs[boundary_index].label)
    right_label = str(epochs[boundary_index + 1].label)
    start_ns = _period_start_ns(str(boundary.uncertainty_start_period))
    end_ns = _next_period_start_ns(str(boundary.uncertainty_end_period))
    decision_ns = int(used_at_ns)
    if not start_ns <= decision_ns < end_ns:
        raise ValueError(
            "transition decision time lies outside uncertainty interval"
        )
    right_weight = (decision_ns - start_ns) / (end_ns - start_ns)
    left_weight = 1.0 - right_weight
    symbol_evidence: dict[str, JSONValue] = {}
    for symbol in symbols:
        left = _resolved_retention(
            operator,
            ObservationContextV1(symbol=symbol, epoch_id=left_label),
        )
        right = _resolved_retention(
            operator,
            ObservationContextV1(symbol=symbol, epoch_id=right_label),
        )
        symbol_evidence[symbol] = _blended_retention_evidence(
            left,
            right,
            left_weight=left_weight,
            right_weight=right_weight,
        )
    left_joint = _resolved_retention(
        operator,
        ObservationContextV1(symbol="GLOBAL", epoch_id=left_label),
    )
    right_joint = _resolved_retention(
        operator,
        ObservationContextV1(symbol="GLOBAL", epoch_id=right_label),
    )
    if left_joint[0].level != "epoch" or right_joint[0].level != "epoch":
        raise ValueError("transition bridge lacks adjacent epoch aggregates")
    joint = _blended_retention_evidence(
        left_joint,
        right_joint,
        left_weight=left_weight,
        right_weight=right_weight,
    )
    return (
        joint,
        symbol_evidence,
        {
            "resolution_basis": (
                "adjacent-fitted-epoch-linear-transition-cardinality-v1"
            ),
            "conditioning_mode": TRANSITION_RETENTION_BRIDGE_MODE,
            "transition_boundary_id": str(boundary.boundary_id),
            "transition_start_ns": start_ns,
            "transition_end_ns": end_ns,
            "transition_left_epoch_id": left_label,
            "transition_right_epoch_id": right_label,
            "transition_left_weight": left_weight,
            "transition_right_weight": right_weight,
            "transition_future_evidence_use": "declared-ex-post-only",
        },
    )


def _resolved_retention(
    operator: ObservationOperatorV1,
    context: ObservationContextV1,
) -> tuple[
    ObservationStratumV1,
    ObservationParameterEstimateV1,
    tuple[str, ...],
]:
    stratum, attempted = operator.resolve_stratum(context)
    estimate = stratum.parameter_map.get("retention_probability")
    if (
        estimate is None
        or estimate.support_status != "supported"
        or not 0.0 < estimate.value <= 1.0
    ):
        raise ValueError(
            "historical product retention is unsupported or invalid"
        )
    return stratum, estimate, attempted


def _resolved_retention_evidence(
    operator: ObservationOperatorV1,
    context: ObservationContextV1,
) -> dict[str, JSONValue]:
    stratum, estimate, attempted = _resolved_retention(operator, context)
    return {
        "context": context.to_dict(),
        "stratum_id": stratum.stratum_id,
        "stratum_key": stratum.key,
        "stratum_level": stratum.level,
        "attempted_stratum_keys": list(attempted),
        **_parameter_evidence(estimate),
    }


def _blended_retention_evidence(
    left: tuple[
        ObservationStratumV1,
        ObservationParameterEstimateV1,
        tuple[str, ...],
    ],
    right: tuple[
        ObservationStratumV1,
        ObservationParameterEstimateV1,
        tuple[str, ...],
    ],
    *,
    left_weight: float,
    right_weight: float,
) -> dict[str, JSONValue]:
    left_stratum, left_estimate, left_attempted = left
    right_stratum, right_estimate, right_attempted = right
    evidence_ids = tuple(
        sorted({*left_estimate.evidence_ids, *right_estimate.evidence_ids})
    )
    provenance = tuple(
        sorted({*left_estimate.provenance, *right_estimate.provenance})
    )
    bases = tuple(
        sorted(
            {*left_estimate.estimation_bases, *right_estimate.estimation_bases}
        )
    )
    return {
        "stratum_level": "transition_bridge",
        "left_stratum_id": left_stratum.stratum_id,
        "right_stratum_id": right_stratum.stratum_id,
        "left_attempted_stratum_keys": list(left_attempted),
        "right_attempted_stratum_keys": list(right_attempted),
        "retention_probability": (
            left_estimate.value * left_weight
            + right_estimate.value * right_weight
        ),
        "retention_lower_bound": (
            left_estimate.lower * left_weight
            + right_estimate.lower * right_weight
        ),
        "retention_upper_bound": (
            left_estimate.upper * left_weight
            + right_estimate.upper * right_weight
        ),
        "support_count": min(
            left_estimate.support_count, right_estimate.support_count
        ),
        "evidence_count": len(evidence_ids),
        "evidence_ids": list(evidence_ids),
        "estimation_bases": list(bases),
        "provenance": list(provenance),
    }


def _parameter_evidence(
    estimate: ObservationParameterEstimateV1,
) -> dict[str, JSONValue]:
    return {
        "retention_probability": estimate.value,
        "retention_lower_bound": estimate.lower,
        "retention_upper_bound": estimate.upper,
        "support_count": estimate.support_count,
        "evidence_count": estimate.evidence_count,
        "evidence_ids": list(estimate.evidence_ids),
        "estimation_bases": list(estimate.estimation_bases),
        "provenance": list(estimate.provenance),
    }


def _period_start_ns(period: str) -> int:
    if len(period) != 6 or not period.isdigit():
        raise ValueError("transition period must be YYYYMM")
    value = datetime(int(period[:4]), int(period[4:]), 1, tzinfo=timezone.utc)
    return int(value.timestamp()) * 1_000_000_000


def _next_period_start_ns(period: str) -> int:
    year = int(period[:4])
    month = int(period[4:])
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    return _period_start_ns(f"{year:04d}{month:02d}")


__all__ = [
    "HISTORICAL_PRODUCT_OBSERVATION_CONDITIONING_SCHEMA_VERSION",
    "TRANSITION_RETENTION_BRIDGE_MODE",
    "historical_product_observation_conditioning",
]
