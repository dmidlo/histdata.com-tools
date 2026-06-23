"""Modeling-readiness advisory checks for HistData artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

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
    "false",
    "missing",
    "none",
    "no",
    "off",
    "unavailable",
    "unknown",
}


@dataclass(slots=True)
class HistDataModelingReadinessRule:
    """Emit advisory modeling/backtest assumption findings."""

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

        assumptions = _modeling_assumptions(target)
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
    target_horizon = _target_horizon_metadata(
        assumptions,
        granularity_ms=granularity_ms,
        timestamp_precision_ms=timestamp_precision_ms,
    )
    assumption_key_text = sorted(str(key) for key in assumptions)
    assumption_keys: list[JSONValue] = []
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
            "execution_assumptions": _execution_assumptions(
                target, assumptions
            ),
            "target_horizon": target_horizon,
            "configured_assumption_keys": assumption_keys,
        },
        "advisory": True,
        "data_defect": False,
        "finding_domain": MODELING_FINDING_DOMAIN,
        "finding_kind": MODELING_FINDING_KIND,
        "symbol": symbol_metadata.normalized_symbol,
        "timeframe": target.timeframe,
        "format_profile": format_profile,
        "cost_assumptions": cost_assumptions,
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
        (
            "slippage_model",
            "slippage_assumption",
            "slippage_cost_model",
            "include_slippage",
        ),
    )
    rollover_status = _configured_status(
        assumptions,
        (
            "rollover_cost_model",
            "rollover_assumption",
            "swap_cost_model",
            "include_rollover",
        ),
    )

    return {
        "spread_cost": {
            "status": spread_status,
            "source": spread_source,
            "required_for_short_horizon_backtests": True,
        },
        "slippage": {
            "status": slippage_status,
            "source": _configured_source(
                assumptions,
                (
                    "slippage_model",
                    "slippage_assumption",
                    "slippage_cost_model",
                    "include_slippage",
                ),
            ),
        },
        "rollover": {
            "status": rollover_status,
            "source": _configured_source(
                assumptions,
                (
                    "rollover_cost_model",
                    "rollover_assumption",
                    "swap_cost_model",
                    "include_rollover",
                ),
            ),
        },
    }


def _execution_assumptions(
    target: QualityTarget,
    assumptions: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        "ask_side_execution_model": _ask_side_execution_modeled(assumptions),
        "bar_close_action_timing": _bar_close_action_timing(assumptions),
        "bid_only_long_entry_short_exit_risk": (
            target.timeframe == M1
            and not _ask_side_execution_modeled(assumptions)
        ),
        "current_bar_leakage_risk": (
            target.timeframe == M1 and not _bar_close_action_timing(assumptions)
        ),
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
    return value is not None


def _normalized_assumption(value: JSONValue | None) -> str:
    return str(value or "").strip().lower().replace("-", "_")


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
