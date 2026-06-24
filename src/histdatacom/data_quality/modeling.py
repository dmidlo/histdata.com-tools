"""Modeling-readiness advisory checks for HistData artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import cast

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
)
from histdatacom.data_quality.symbols import symbol_metadata_for
from histdatacom.histdata_ascii import M1, TICK
from histdatacom.runtime_contracts import JSONValue

MODELING_READINESS_RULE_ID = "modeling.readiness"
MODELING_READINESS_METADATA_KEY = "modeling_readiness"

MODELING_FINDING_DOMAIN = "modeling_readiness"
MODELING_FINDING_KIND = "modeling_assumption"

M1_GRANULARITY_MS = 60_000
M1_TIMESTAMP_PRECISION_MS = 1_000
TICK_MINIMUM_INTERVAL_MS = 1
TICK_TIMESTAMP_PRECISION_MS = 1

MISSING_ASSUMPTION_MARKERS = {
    "",
    "0",
    "disabled",
    "false",
    "missing",
    "none",
    "no",
    "off",
    "unavailable",
    "unknown",
}

_SLIPPAGE_MODEL_KEYS = (
    "slippage_model",
    "slippage_assumption",
    "slippage_cost_model",
    "slippage_sensitivity",
    "include_slippage",
)
_ROLLOVER_MODEL_KEYS = (
    "rollover_cost_model",
    "rollover_assumption",
    "swap_cost_model",
    "financing_cost_model",
    "include_rollover",
)
_HIGH_LOW_FILL_KEYS = (
    "uses_bar_high_low_for_fills",
    "allow_high_low_fills",
    "fills_at_bar_extremes",
    "execution_price",
    "entry_price",
    "exit_price",
    "fill_price",
    "fill_model",
)
_HIGH_LOW_MODEL_KEYS = (
    "high_low_execution_model",
    "intrabar_execution_model",
    "bar_path_model",
    "execution_path_model",
    "stop_limit_execution_model",
)
_FORWARD_FILL_KEYS = (
    "forward_fill",
    "allow_forward_fill",
    "uses_forward_fill",
    "join_fill_method",
    "feature_fill_method",
    "missing_data_fill_method",
)
_STALE_FORWARD_FILL_POLICY_KEYS = (
    "stale_forward_fill_policy",
    "stale_feature_policy",
    "forward_fill_limit",
    "max_forward_fill_bars",
    "max_forward_fill_ms",
    "max_stale_feature_ms",
)
_CROSS_INSTRUMENT_FEATURE_KEYS = (
    "cross_instrument_features",
    "multi_symbol_features",
    "joined_symbols",
    "cross_symbols",
    "triangular_features",
    "correlated_pair_features",
)
_CROSS_INSTRUMENT_ALIGNMENT_KEYS = (
    "cross_instrument_alignment",
    "timestamp_alignment",
    "join_policy",
    "common_timestamp_grid",
    "feature_lag_policy",
    "asof_join_tolerance_ms",
    "max_timestamp_skew_ms",
)
_LEAKAGE_SENSITIVE_KEYS = (
    "scaling",
    "standardization",
    "normalization",
    "imputation",
    "resampling",
    "feature_selection",
    "rolling_feature_fit",
    "indicator_fit_policy",
)
_TRAINING_ONLY_POLICY_KEYS = (
    "train_test_split_policy",
    "fit_transform_policy",
    "fit_on_training_only",
    "walk_forward_validation",
    "purged_cv",
    "temporal_cv",
)
_CALENDAR_REGIME_REQUIRED_KEYS = (
    "uses_calendar_regime_features",
    "calendar_event_sensitive",
    "needs_calendar_regime_tags",
    "event_regime_model",
    "exclude_event_regimes",
)
_CALENDAR_REGIME_POLICY_KEYS = (
    "calendar_regime_policy",
    "calendar_event_policy",
    "calendar_profile_name",
    "calendar_profile_version",
    "calendar_regime_tags",
    "event_regime_tags",
    "excluded_calendar_tags",
)
_CALENDAR_REGIME_TAG_KEYS = (
    "calendar_regime_tags",
    "event_regime_tags",
    "calendar_event_tags",
    "excluded_calendar_tags",
)


@dataclass(slots=True)
class HistDataModelingReadinessRule:
    """Emit advisory modeling/backtest assumption findings."""

    assumptions: Mapping[str, JSONValue] = field(default_factory=dict)
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = MODELING_READINESS_RULE_ID
    description: str = (
        "Report modeling-readiness assumptions for leakage, tradability, "
        "execution-cost availability, and target-horizon feasibility."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return modeling-readiness findings for one target."""
        if not _is_histdata_ascii_market_target(target):
            return ()

        assumptions = {
            **self.assumptions,
            **_modeling_assumptions(target),
        }
        profile = _modeling_readiness_metadata(target, assumptions)
        findings: list[QualityFinding] = [
            _modeling_finding(
                target,
                code="MODELING_READINESS_SUMMARY",
                message="Modeling-readiness assumptions for this target.",
                severity=QualitySeverity.INFO,
                rule_id=self.rule_id,
                metadata=profile,
            )
        ]

        if _needs_bid_only_execution_warning(target, assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_BID_ONLY_EXECUTION_RISK",
                    message=(
                        "M1 OHLC bars are bid-only; long-entry and "
                        "short-exit assumptions need an ask-side execution "
                        "model or joined spread data."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "ask_side_execution_model",
                    },
                )
            )

        if _needs_current_bar_leakage_warning(target, assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_CURRENT_BAR_LEAKAGE_RISK",
                    message=(
                        "Current-bar high, low, and close values are only "
                        "known after the M1 bar completes; acting inside the "
                        "same bar risks feature or label leakage."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "after_bar_close_action_timing",
                    },
                )
            )

        if _needs_spread_cost_warning(target, assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_SPREAD_COST_MISSING",
                    message=(
                        "Spread cost is not available from bid-only M1 bars; "
                        "configure a spread-cost model or use tick bid/ask "
                        "data for execution-cost estimates."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "spread_cost_model",
                    },
                )
            )

        if _needs_high_low_execution_warning(target, assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_HIGH_LOW_EXECUTION_REALISM_RISK",
                    message=(
                        "M1 high/low values are only known after the bar and "
                        "do not prove an executable intrabar path; configure "
                        "an intrabar or high/low execution model."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "high_low_execution_model",
                    },
                )
            )

        if _needs_slippage_cost_warning(assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_SLIPPAGE_COST_MISSING",
                    message=(
                        "Execution/backtest assumptions require slippage, "
                        "but no slippage model or sensitivity setting is "
                        "configured."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "slippage_model",
                    },
                )
            )

        if _needs_rollover_cost_warning(assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_ROLLOVER_COST_MISSING",
                    message=(
                        "Overnight/held-position assumptions require explicit "
                        "rollover or swap-cost handling."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "rollover_cost_model",
                    },
                )
            )

        if _needs_stale_forward_fill_warning(assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_STALE_FORWARD_FILL_RISK",
                    message=(
                        "Forward-filled features need a stale-feature policy "
                        "or maximum fill window to avoid carrying inactive "
                        "prices into active samples."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "stale_forward_fill_policy",
                    },
                )
            )

        if _needs_cross_instrument_alignment_warning(assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_CROSS_INSTRUMENT_ALIGNMENT_RISK",
                    message=(
                        "Cross-instrument features need an explicit timestamp "
                        "alignment, join, or lag policy."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "cross_instrument_alignment",
                    },
                )
            )

        if _needs_train_test_leakage_warning(assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_TRAIN_TEST_LEAKAGE_RISK",
                    message=(
                        "Scaling, imputation, resampling, or feature "
                        "selection assumptions need a training-only fit policy."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "train_test_leakage_policy",
                    },
                )
            )

        if _needs_calendar_regime_policy_warning(target, assumptions):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_CALENDAR_REGIME_POLICY_MISSING",
                    message=(
                        "Calendar/event regime usage needs an explicit policy "
                        "for included, excluded, or tagged regimes."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "calendar_regime_policy",
                    },
                )
            )

        target_horizon = profile["target_horizon"]
        if (
            isinstance(target_horizon, dict)
            and target_horizon["status"] == "too_short"
        ):
            findings.append(
                _modeling_finding(
                    target,
                    code="MODELING_TARGET_HORIZON_FEASIBILITY_WARNING",
                    message=(
                        "Configured target horizon is not larger than the "
                        "target's data granularity and timestamp precision."
                    ),
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata={
                        **profile,
                        "missing_assumption": "larger_target_horizon",
                    },
                )
            )

        return tuple(findings)


def modeling_quality_rules() -> tuple[QualityRule, ...]:
    """Return modeling-readiness quality rules."""
    modeling_rule: QualityRule = HistDataModelingReadinessRule()
    return (modeling_rule,)


def _is_histdata_ascii_market_target(target: QualityTarget) -> bool:
    return target.data_format == "ascii" and target.timeframe in {M1, TICK}


def _modeling_assumptions(
    target: QualityTarget,
) -> dict[str, JSONValue]:
    for key in ("modeling_assumptions", "modeling"):
        value = target.metadata.get(key)
        if isinstance(value, Mapping):
            return {str(name): setting for name, setting in value.items()}
    return {}


def _modeling_readiness_metadata(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    symbol = str(target.symbol or target.metadata.get("symbol", "") or "")
    symbol_metadata = symbol_metadata_for(symbol)
    format_profile = _format_profile(target)
    granularity_ms = _metadata_int(format_profile["granularity_ms"])
    timestamp_precision_ms = _metadata_int(
        format_profile["timestamp_precision_ms"]
    )
    cost_assumptions = _cost_assumptions(target, assumptions)
    execution_assumptions = _execution_assumptions(target, assumptions)
    feature_alignment = _feature_alignment_assumptions(assumptions)
    leakage_assumptions = _leakage_assumptions(assumptions)
    calendar_regime_assumptions = _calendar_regime_assumptions(
        target, assumptions
    )
    target_horizon = _target_horizon_metadata(
        assumptions,
        granularity_ms=granularity_ms,
        timestamp_precision_ms=timestamp_precision_ms,
    )
    assumption_key_text = sorted(str(key) for key in assumptions)
    assumption_keys: list[str] = []
    assumption_keys.extend(assumption_key_text)
    return {
        MODELING_READINESS_METADATA_KEY: {
            "advisory": True,
            "data_defect": False,
            "finding_domain": MODELING_FINDING_DOMAIN,
            "finding_kind": MODELING_FINDING_KIND,
            "symbol_metadata": symbol_metadata.to_metadata(),
            "format_profile": format_profile,
            "cost_assumptions": cost_assumptions,
            "execution_assumptions": execution_assumptions,
            "feature_alignment_assumptions": feature_alignment,
            "leakage_assumptions": leakage_assumptions,
            "calendar_regime_assumptions": calendar_regime_assumptions,
            "target_horizon": target_horizon,
            "configured_assumption_keys": cast(JSONValue, assumption_keys),
        },
        "advisory": True,
        "data_defect": False,
        "finding_domain": MODELING_FINDING_DOMAIN,
        "finding_kind": MODELING_FINDING_KIND,
        "symbol": symbol_metadata.normalized_symbol,
        "timeframe": target.timeframe,
        "format_profile": format_profile,
        "cost_assumptions": cost_assumptions,
        "execution_assumptions": execution_assumptions,
        "feature_alignment_assumptions": feature_alignment,
        "leakage_assumptions": leakage_assumptions,
        "calendar_regime_assumptions": calendar_regime_assumptions,
        "target_horizon": target_horizon,
    }


def _format_profile(target: QualityTarget) -> dict[str, JSONValue]:
    if target.timeframe == M1:
        return {
            "data_format": target.data_format,
            "timeframe": target.timeframe,
            "price_basis": "bid_ohlc",
            "quote_sides": ["bid"],
            "tradable_quote_sides_available": False,
            "bar_close_required_for_ohlc": True,
            "granularity_ms": M1_GRANULARITY_MS,
            "timestamp_precision_ms": M1_TIMESTAMP_PRECISION_MS,
            "timestamp_precision": "seconds",
        }

    return {
        "data_format": target.data_format,
        "timeframe": target.timeframe,
        "price_basis": "bid_ask_tick",
        "quote_sides": ["bid", "ask"],
        "tradable_quote_sides_available": True,
        "bar_close_required_for_ohlc": False,
        "granularity_ms": TICK_MINIMUM_INTERVAL_MS,
        "timestamp_precision_ms": TICK_TIMESTAMP_PRECISION_MS,
        "timestamp_precision": "milliseconds",
    }


def _cost_assumptions(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    spread_status = "available" if target.timeframe == TICK else "missing"
    spread_source = "tick_bid_ask" if target.timeframe == TICK else ""
    if _spread_cost_configured(assumptions):
        spread_status = "configured"
        spread_source = _configured_source(
            assumptions,
            (
                "spread_cost_model",
                "spread_cost_assumption",
                "spread_cost_source",
                "spread_source",
            ),
        )

    slippage_status = _configured_status(
        assumptions,
        _SLIPPAGE_MODEL_KEYS,
    )
    rollover_status = _configured_status(
        assumptions,
        _ROLLOVER_MODEL_KEYS,
    )

    return {
        "spread_cost": {
            "status": spread_status,
            "source": spread_source,
            "required_for_short_horizon_backtests": True,
        },
        "slippage": {
            "status": slippage_status,
            "source": _configured_source(assumptions, _SLIPPAGE_MODEL_KEYS),
            "required": _slippage_cost_required(assumptions),
        },
        "rollover": {
            "status": rollover_status,
            "source": _configured_source(assumptions, _ROLLOVER_MODEL_KEYS),
            "required": _rollover_cost_required(assumptions),
        },
    }


def _execution_assumptions(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    uses_high_low = _uses_high_low_execution(assumptions)
    high_low_modeled = _high_low_execution_modeled(assumptions)
    return {
        "ask_side_execution_model": _ask_side_execution_modeled(assumptions),
        "bar_close_action_timing": _bar_close_action_timing(assumptions),
        "uses_high_low_fills": uses_high_low,
        "high_low_execution_model": high_low_modeled,
        "bid_only_long_entry_short_exit_risk": (
            target.timeframe == M1
            and not _ask_side_execution_modeled(assumptions)
        ),
        "current_bar_leakage_risk": (
            target.timeframe == M1 and not _bar_close_action_timing(assumptions)
        ),
        "high_low_execution_risk": (
            target.timeframe == M1 and uses_high_low and not high_low_modeled
        ),
    }


def _feature_alignment_assumptions(
    assumptions: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    uses_forward_fill = _uses_forward_fill(assumptions)
    stale_policy_configured = _stale_forward_fill_configured(assumptions)
    uses_cross_instrument = _uses_cross_instrument_features(assumptions)
    cross_alignment_configured = _cross_instrument_alignment_configured(
        assumptions
    )
    return {
        "uses_forward_fill": uses_forward_fill,
        "stale_forward_fill_policy": stale_policy_configured,
        "stale_forward_fill_risk": (
            uses_forward_fill and not stale_policy_configured
        ),
        "uses_cross_instrument_features": uses_cross_instrument,
        "cross_instrument_alignment": cross_alignment_configured,
        "cross_instrument_alignment_risk": (
            uses_cross_instrument and not cross_alignment_configured
        ),
        "forward_fill_source": _configured_source(
            assumptions,
            _FORWARD_FILL_KEYS,
        ),
        "alignment_source": _configured_source(
            assumptions,
            _CROSS_INSTRUMENT_ALIGNMENT_KEYS,
        ),
    }


def _leakage_assumptions(
    assumptions: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    transform_keys = _configured_keys(assumptions, _LEAKAGE_SENSITIVE_KEYS)
    policy_keys = _configured_keys(assumptions, _TRAINING_ONLY_POLICY_KEYS)
    return {
        "leakage_sensitive_transform_keys": cast(JSONValue, transform_keys),
        "training_only_policy_keys": cast(JSONValue, policy_keys),
        "training_only_policy_configured": bool(policy_keys),
        "train_test_leakage_risk": bool(transform_keys) and not policy_keys,
    }


def _calendar_regime_assumptions(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    required = _calendar_regime_policy_required(target, assumptions)
    configured_keys = _configured_keys(
        assumptions,
        _CALENDAR_REGIME_POLICY_KEYS,
    )
    target_tags = _target_calendar_event_tags(target)
    return {
        "required": required,
        "configured_policy_keys": cast(JSONValue, configured_keys),
        "configured_tags": _assumption_values(
            assumptions,
            _CALENDAR_REGIME_TAG_KEYS,
        ),
        "target_event_tags": target_tags,
        "calendar_profile_source": _target_calendar_profile_source(target),
        "calendar_regime_policy_risk": required and not configured_keys,
    }


def _target_horizon_metadata(
    assumptions: Mapping[str, JSONValue],
    *,
    granularity_ms: int,
    timestamp_precision_ms: int,
) -> dict[str, JSONValue]:
    horizon_ms = _target_horizon_ms(
        assumptions,
        granularity_ms=granularity_ms,
    )
    minimum_recommended_ms = max(granularity_ms, timestamp_precision_ms)
    if horizon_ms is None:
        status = "unconfigured"
    elif horizon_ms <= minimum_recommended_ms:
        status = "too_short"
    else:
        status = "feasible"

    return {
        "configured": horizon_ms is not None,
        "value_ms": horizon_ms,
        "granularity_ms": granularity_ms,
        "timestamp_precision_ms": timestamp_precision_ms,
        "minimum_recommended_ms": minimum_recommended_ms,
        "status": status,
    }


def _needs_bid_only_execution_warning(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return target.timeframe == M1 and not _ask_side_execution_modeled(
        assumptions
    )


def _needs_current_bar_leakage_warning(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return target.timeframe == M1 and not _bar_close_action_timing(assumptions)


def _needs_spread_cost_warning(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return target.timeframe == M1 and not _spread_cost_configured(assumptions)


def _needs_high_low_execution_warning(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return (
        target.timeframe == M1
        and _uses_high_low_execution(assumptions)
        and not _high_low_execution_modeled(assumptions)
    )


def _needs_slippage_cost_warning(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return _slippage_cost_required(assumptions) and not _slippage_configured(
        assumptions
    )


def _needs_rollover_cost_warning(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return _rollover_cost_required(assumptions) and not _rollover_configured(
        assumptions
    )


def _needs_stale_forward_fill_warning(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return _uses_forward_fill(
        assumptions
    ) and not _stale_forward_fill_configured(assumptions)


def _needs_cross_instrument_alignment_warning(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return _uses_cross_instrument_features(
        assumptions
    ) and not _cross_instrument_alignment_configured(assumptions)


def _needs_train_test_leakage_warning(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return bool(
        _configured_keys(assumptions, _LEAKAGE_SENSITIVE_KEYS)
    ) and not bool(_configured_keys(assumptions, _TRAINING_ONLY_POLICY_KEYS))


def _needs_calendar_regime_policy_warning(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return _calendar_regime_policy_required(target, assumptions) and not bool(
        _configured_keys(assumptions, _CALENDAR_REGIME_POLICY_KEYS)
    )


def _ask_side_execution_modeled(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    if _configured_bool(assumptions.get("ask_side_execution_model")):
        return True
    if _configured_bool(assumptions.get("ask_side_execution_modeled")):
        return True

    execution_side = _normalized_assumption(
        assumptions.get("execution_price_side")
    )
    return execution_side in {
        "ask",
        "bid/ask",
        "bid_ask",
        "mid_with_spread",
        "spread_adjusted",
    }


def _uses_high_low_execution(assumptions: Mapping[str, JSONValue]) -> bool:
    if assumptions.get("uses_bar_high_low_for_fills") is False:
        return False
    if assumptions.get("allow_high_low_fills") is False:
        return False
    if assumptions.get("fills_at_bar_extremes") is False:
        return False
    if any(
        _configured_bool(assumptions.get(key))
        for key in (
            "uses_bar_high_low_for_fills",
            "allow_high_low_fills",
            "fills_at_bar_extremes",
        )
    ):
        return True
    for key in (
        "execution_price",
        "entry_price",
        "exit_price",
        "fill_price",
        "fill_model",
    ):
        value = _normalized_assumption(assumptions.get(key))
        if value in {
            "high",
            "low",
            "bar_high",
            "bar_low",
            "bar_high_low",
            "high_low",
            "same_bar_high_low",
            "intrabar_extreme",
            "perfect_foresight_high_low",
        }:
            return True
    return False


def _high_low_execution_modeled(assumptions: Mapping[str, JSONValue]) -> bool:
    if assumptions.get("uses_bar_high_low_for_fills") is False:
        return True
    if any(
        _configured_value(assumptions.get(key)) for key in _HIGH_LOW_MODEL_KEYS
    ):
        return True
    fill_model = _normalized_assumption(assumptions.get("fill_model"))
    return fill_model in {
        "tick_replay",
        "intrabar_path_model",
        "ohlc_path_model",
        "stop_limit_path_model",
        "next_bar_open",
    }


def _bar_close_action_timing(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    if _configured_bool(assumptions.get("bar_close_execution")):
        return True
    if _configured_bool(assumptions.get("after_bar_close_action_timing")):
        return True
    if assumptions.get("features_use_current_bar") is False:
        return True

    timing = _normalized_assumption(
        assumptions.get("current_bar_action_timing")
    )
    return timing in {
        "after_bar_close",
        "bar_close",
        "next_bar",
        "next_bar_open",
    }


def _spread_cost_configured(assumptions: Mapping[str, JSONValue]) -> bool:
    return any(
        _configured_value(assumptions.get(key))
        for key in (
            "spread_cost_model",
            "spread_cost_assumption",
            "spread_cost_source",
            "spread_source",
            "include_spread_costs",
        )
    )


def _slippage_configured(assumptions: Mapping[str, JSONValue]) -> bool:
    return any(
        _configured_value(assumptions.get(key)) for key in _SLIPPAGE_MODEL_KEYS
    )


def _rollover_configured(assumptions: Mapping[str, JSONValue]) -> bool:
    return any(
        _configured_value(assumptions.get(key)) for key in _ROLLOVER_MODEL_KEYS
    )


def _slippage_cost_required(assumptions: Mapping[str, JSONValue]) -> bool:
    if any(
        _configured_bool(assumptions.get(key))
        for key in (
            "requires_slippage_model",
            "include_execution_costs",
            "backtest_execution",
            "simulate_trades",
            "trade_simulation",
            "execution_costs_required",
        )
    ):
        return True
    execution_mode = _normalized_assumption(assumptions.get("execution_mode"))
    return execution_mode in {"backtest", "trade_simulation", "live_simulation"}


def _rollover_cost_required(assumptions: Mapping[str, JSONValue]) -> bool:
    if any(
        _configured_bool(assumptions.get(key))
        for key in (
            "holds_overnight",
            "overnight_positions",
            "requires_rollover_model",
            "include_rollover_costs",
            "include_swap_costs",
        )
    ):
        return True
    holding_period_ms = _holding_period_ms(assumptions)
    return holding_period_ms is not None and holding_period_ms >= 86_400_000


def _uses_forward_fill(assumptions: Mapping[str, JSONValue]) -> bool:
    if any(
        _configured_bool(assumptions.get(key))
        for key in ("forward_fill", "allow_forward_fill", "uses_forward_fill")
    ):
        return True
    return any(
        _normalized_assumption(assumptions.get(key))
        in {"ffill", "forward_fill", "last_observation_carried_forward"}
        for key in (
            "join_fill_method",
            "feature_fill_method",
            "missing_data_fill_method",
        )
    )


def _stale_forward_fill_configured(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return any(
        _configured_value(assumptions.get(key))
        for key in _STALE_FORWARD_FILL_POLICY_KEYS
    )


def _uses_cross_instrument_features(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return any(
        _configured_value(assumptions.get(key))
        for key in _CROSS_INSTRUMENT_FEATURE_KEYS
    )


def _cross_instrument_alignment_configured(
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return any(
        _configured_value(assumptions.get(key))
        for key in _CROSS_INSTRUMENT_ALIGNMENT_KEYS
    )


def _calendar_regime_policy_required(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> bool:
    return any(
        _configured_value(assumptions.get(key))
        for key in _CALENDAR_REGIME_REQUIRED_KEYS
    ) or bool(_target_calendar_event_tags(target))


def _configured_status(
    assumptions: Mapping[str, JSONValue],
    keys: tuple[str, ...],
) -> str:
    return (
        "configured"
        if any(_configured_value(assumptions.get(key)) for key in keys)
        else "missing"
    )


def _configured_source(
    assumptions: Mapping[str, JSONValue],
    keys: tuple[str, ...],
) -> str:
    for key in keys:
        value = assumptions.get(key)
        if _configured_value(value):
            return str(value)
    return ""


def _target_horizon_ms(
    assumptions: Mapping[str, JSONValue],
    *,
    granularity_ms: int,
) -> int | None:
    milliseconds = _positive_float(assumptions.get("target_horizon_ms"))
    if milliseconds is not None:
        return round(milliseconds)

    seconds = _positive_float(assumptions.get("target_horizon_seconds"))
    if seconds is not None:
        return round(seconds * 1_000)

    minutes = _positive_float(assumptions.get("target_horizon_minutes"))
    if minutes is not None:
        return round(minutes * 60_000)

    bars = _positive_float(assumptions.get("target_horizon_bars"))
    if bars is not None:
        return round(bars * granularity_ms)

    return None


def _holding_period_ms(assumptions: Mapping[str, JSONValue]) -> int | None:
    milliseconds = _positive_float(assumptions.get("holding_period_ms"))
    if milliseconds is not None:
        return round(milliseconds)

    seconds = _positive_float(assumptions.get("holding_period_seconds"))
    if seconds is not None:
        return round(seconds * 1_000)

    minutes = _positive_float(assumptions.get("holding_period_minutes"))
    if minutes is not None:
        return round(minutes * 60_000)

    hours = _positive_float(assumptions.get("holding_period_hours"))
    if hours is not None:
        return round(hours * 3_600_000)

    days = _positive_float(assumptions.get("holding_period_days"))
    if days is not None:
        return round(days * 86_400_000)

    return None


def _positive_float(value: JSONValue | None) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value) if value > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = float(text)
        except ValueError:
            return None
        return parsed if parsed > 0 else None
    return None


def _configured_bool(value: JSONValue | None) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        normalized = _normalized_assumption(value)
        return normalized not in MISSING_ASSUMPTION_MARKERS
    return False


def _configured_value(value: JSONValue | None) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        return _normalized_assumption(value) not in MISSING_ASSUMPTION_MARKERS
    if isinstance(value, list):
        return any(_configured_value(item) for item in value)
    if isinstance(value, dict):
        return any(_configured_value(item) for item in value.values())
    return value is not None


def _normalized_assumption(value: JSONValue | None) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _configured_keys(
    assumptions: Mapping[str, JSONValue],
    keys: tuple[str, ...],
) -> list[str]:
    return [key for key in keys if _configured_value(assumptions.get(key))]


def _assumption_values(
    assumptions: Mapping[str, JSONValue],
    keys: tuple[str, ...],
) -> list[JSONValue]:
    values: list[JSONValue] = []
    for key in keys:
        value = assumptions.get(key)
        if not _configured_value(value):
            continue
        if isinstance(value, list):
            values.extend(item for item in value if _configured_value(item))
        else:
            values.append(value)
    return values


def _target_calendar_event_tags(target: QualityTarget) -> list[JSONValue]:
    values: list[JSONValue] = []
    for key in ("event_tags", "calendar_event_tags", "calendar_regime_tags"):
        value = target.metadata.get(key)
        if isinstance(value, list):
            values.extend(item for item in value if _configured_value(item))
        elif _configured_value(value):
            values.append(value)
    calendar = target.metadata.get("calendar")
    if isinstance(calendar, Mapping):
        for key in (
            "event_tags",
            "calendar_event_tags",
            "calendar_regime_tags",
        ):
            value = calendar.get(key)
            if isinstance(value, list):
                values.extend(item for item in value if _configured_value(item))
            elif _configured_value(value):
                values.append(value)
    return values


def _target_calendar_profile_source(target: QualityTarget) -> str:
    profile = target.metadata.get("calendar_profile")
    if isinstance(profile, Mapping):
        return str(profile.get("source") or profile.get("name") or "")
    calendar_policy = target.metadata.get("calendar_policy")
    if isinstance(calendar_policy, Mapping):
        return str(
            calendar_policy.get("holiday_calendar_source")
            or calendar_policy.get("calendar_profile")
            or ""
        )
    return ""


def _modeling_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    metadata: dict[str, JSONValue],
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.from_value(severity),
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=QualityLocation(
            path=target.path,
            column="modeling_assumptions",
            metadata={
                "finding_domain": MODELING_FINDING_DOMAIN,
                "finding_kind": MODELING_FINDING_KIND,
                "data_defect": False,
                "advisory": True,
            },
        ),
        metadata=dict(metadata),
    )


def _metadata_int(value: JSONValue) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    msg = f"metadata value is not an integer: {value!r}"
    raise TypeError(msg)
