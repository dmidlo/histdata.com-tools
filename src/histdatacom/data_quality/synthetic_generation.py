"""Deterministic synthetic ASCII tick generation from a reference set.

The generator augments the canonical enriched tick frame in place.  It never
overwrites observed bid/ask values or changes durable row identity.  Generated
midpoint returns and spreads are sampled together in deterministic contiguous
blocks, then the candidate is always evaluated through the ordinary
fingerprint and synthetic-constraint validation path.
"""

from __future__ import annotations

import hashlib
import json
import math
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, cast

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.fingerprints import (
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    HistDataFingerprintProfile,
    HistDataSeriesFingerprintRule,
)
from histdatacom.data_quality.synthetic_constraints import (
    synthetic_constraints_from_training_frame,
    validate_synthetic_constraint_reports,
)
from histdatacom.data_quality.training_features import (
    HARD_ISSUE_COLUMNS,
    SYNTHETIC_PLACEHOLDER_COLUMNS,
    ensure_tick_training_features,
)
from histdatacom.histdata_ascii import (
    TICK,
    format_influx_line,
    read_polars_cache,
    write_polars_cache,
)
from histdatacom.runtime_contracts import JSONValue

SYNTHETIC_TICK_GENERATION_SCHEMA_VERSION = (
    "histdatacom.synthetic-tick-generation.v1"
)
SYNTHETIC_TICK_GENERATION_CONFIGURATION_SCHEMA_VERSION = (
    "histdatacom.synthetic-tick-generation-configuration.v1"
)
SYNTHETIC_TICK_GENERATION_VALIDATION_SCHEMA_VERSION = (
    "histdatacom.synthetic-tick-generation-validation.v1"
)

SYNTHETIC_TICK_METHOD_CODES = {"empirical_block_bootstrap": 1}
SYNTHETIC_TICK_STATUS_CODES = {
    "unavailable": 1,
    "limited": 2,
    "ready": 3,
}
SYNTHETIC_TICK_REASON_CODES = {
    "none": 0,
    "reference_fingerprint_required": 1,
    "unsupported_base_grain": 2,
    "reference_constraints_unavailable": 3,
    "reference_stylized_facts_unavailable": 4,
    "insufficient_reference_rows": 5,
    "existing_synthetic_values": 6,
    "generation_row_limit": 7,
    "unsupported_reference_fingerprint_schema": 8,
    "reference_fingerprint_id_required": 9,
    "reference_target_axis_mismatch": 10,
}

DEFAULT_SYNTHETIC_TICK_SEED = 0
DEFAULT_SYNTHETIC_TICK_BLOCK_SIZE = 32
DEFAULT_SYNTHETIC_TICK_MINIMUM_REFERENCE_ROWS = 16
DEFAULT_SYNTHETIC_TICK_MAX_REFERENCE_ROWS = 1_000_000
DEFAULT_SYNTHETIC_TICK_MAX_GENERATED_ROWS = 1_000_000
DEFAULT_SYNTHETIC_TICK_MAX_ABS_LOG_RETURN = 0.25
DEFAULT_SYNTHETIC_TICK_ROUNDING_DIGITS = 8
DEFAULT_SYNTHETIC_TICK_DIAGNOSTIC_SAMPLE_LIMIT = 12

MAX_SYNTHETIC_TICK_BLOCK_SIZE = 100_000
MAX_SYNTHETIC_TICK_REFERENCE_ROWS = 10_000_000
MAX_SYNTHETIC_TICK_GENERATED_ROWS = 10_000_000
MAX_SYNTHETIC_TICK_DIAGNOSTIC_SAMPLE_LIMIT = 128
MAX_SYNTHETIC_TICK_PRICE = 1_000_000_000_000.0


@dataclass(frozen=True, slots=True)
class SyntheticTickGenerationProfile:
    """Bounded deterministic generator configuration."""

    method: str = "empirical_block_bootstrap"
    seed: int = DEFAULT_SYNTHETIC_TICK_SEED
    block_size: int = DEFAULT_SYNTHETIC_TICK_BLOCK_SIZE
    minimum_reference_rows: int = DEFAULT_SYNTHETIC_TICK_MINIMUM_REFERENCE_ROWS
    max_reference_rows: int = DEFAULT_SYNTHETIC_TICK_MAX_REFERENCE_ROWS
    max_generated_rows: int = DEFAULT_SYNTHETIC_TICK_MAX_GENERATED_ROWS
    max_abs_log_return: float = DEFAULT_SYNTHETIC_TICK_MAX_ABS_LOG_RETURN
    rounding_digits: int = DEFAULT_SYNTHETIC_TICK_ROUNDING_DIGITS
    diagnostic_sample_limit: int = (
        DEFAULT_SYNTHETIC_TICK_DIAGNOSTIC_SAMPLE_LIMIT
    )
    anchor_mode: str = "first_valid_mid"
    overwrite_existing: bool = False

    def __post_init__(self) -> None:
        if self.method not in SYNTHETIC_TICK_METHOD_CODES:
            raise ValueError(f"unsupported synthetic method: {self.method}")
        _bounded_positive(
            self.block_size,
            MAX_SYNTHETIC_TICK_BLOCK_SIZE,
            "block_size",
        )
        if self.minimum_reference_rows < 2:
            raise ValueError("minimum_reference_rows must be at least 2")
        _bounded_positive(
            self.max_reference_rows,
            MAX_SYNTHETIC_TICK_REFERENCE_ROWS,
            "max_reference_rows",
        )
        if self.max_reference_rows < self.minimum_reference_rows:
            raise ValueError(
                "max_reference_rows must cover minimum_reference_rows"
            )
        _bounded_positive(
            self.max_generated_rows,
            MAX_SYNTHETIC_TICK_GENERATED_ROWS,
            "max_generated_rows",
        )
        if not 0 < self.max_abs_log_return <= 1:
            raise ValueError("max_abs_log_return must be in (0, 1]")
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between 0 and 16")
        _bounded_positive(
            self.diagnostic_sample_limit,
            MAX_SYNTHETIC_TICK_DIAGNOSTIC_SAMPLE_LIMIT,
            "diagnostic_sample_limit",
        )
        if self.anchor_mode not in {"first_valid_mid", "median_valid_mid"}:
            raise ValueError(f"unsupported anchor_mode: {self.anchor_mode}")

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible configuration metadata."""
        return {
            "schema_version": (
                SYNTHETIC_TICK_GENERATION_CONFIGURATION_SCHEMA_VERSION
            ),
            "method": self.method,
            "method_code": SYNTHETIC_TICK_METHOD_CODES[self.method],
            "seed": self.seed,
            "block_size": self.block_size,
            "minimum_reference_rows": self.minimum_reference_rows,
            "max_reference_rows": self.max_reference_rows,
            "max_generated_rows": self.max_generated_rows,
            "max_abs_log_return": self.max_abs_log_return,
            "rounding_digits": self.rounding_digits,
            "diagnostic_sample_limit": self.diagnostic_sample_limit,
            "anchor_mode": self.anchor_mode,
            "overwrite_existing": self.overwrite_existing,
        }


@dataclass(frozen=True, slots=True)
class SyntheticTickGenerationResult:
    """Generated enriched frame and bounded diagnostic evidence."""

    frame: Any
    diagnostics: Mapping[str, JSONValue]
    candidate_report: QualityReport | None = None


def generate_synthetic_ticks_from_reference(
    reference_frame: Any,
    reference_fingerprint: Mapping[str, JSONValue],
    *,
    profile: SyntheticTickGenerationProfile | None = None,
    target: QualityTarget | None = None,
) -> SyntheticTickGenerationResult:
    """Populate synthetic columns from one fingerprint-backed reference set."""
    selected = profile or SyntheticTickGenerationProfile()
    _require_reference_fingerprint(reference_fingerprint)
    reference_axis = _mapping(reference_fingerprint.get("target_axis"))
    selected_target = target or _target_from_axis(reference_axis)
    _require_tick_axis(reference_axis, selected_target)
    if not _axis_matches_target(reference_axis, selected_target):
        raise ValueError("reference_target_axis_mismatch")
    enriched = ensure_tick_training_features(
        reference_frame, target=selected_target
    )
    if (
        _has_existing_synthetic_values(enriched)
        and not selected.overwrite_existing
    ):
        raise ValueError("existing_synthetic_values")
    constraints = _mapping(reference_fingerprint.get("synthetic_constraints"))
    if not constraints:
        constraints = synthetic_constraints_from_training_frame(
            enriched,
            fingerprint=reference_fingerprint,
            target=selected_target,
        )
    constraint_status = _text(constraints.get("status"))
    stylized_facts = _mapping_rows(
        constraints.get("stylized_facts_to_preserve")
    )
    base = _base_diagnostics(
        selected,
        reference_fingerprint,
        constraints,
        selected_target,
        input_row_count=int(getattr(enriched, "height", 0) or 0),
    )
    if constraint_status == "unavailable":
        return _unavailable_result(
            enriched,
            base,
            "reference_constraints_unavailable",
        )
    if not stylized_facts:
        return _unavailable_result(
            enriched,
            base,
            "reference_stylized_facts_unavailable",
        )

    reference_rows = _reference_rows(enriched, selected)
    transitions = _reference_transitions(reference_rows)
    if len(reference_rows) < selected.minimum_reference_rows or not transitions:
        base["reference_evidence"] = _reference_evidence(
            enriched,
            reference_rows,
            transitions,
            selected,
        )
        return _unavailable_result(
            enriched,
            base,
            "insufficient_reference_rows",
        )

    output, generation = _generate_frame(
        enriched,
        reference_rows,
        transitions,
        constraint_id=_text(constraints.get("constraint_id")),
        constraint_status=constraint_status,
        profile=selected,
    )
    base["reference_evidence"] = _reference_evidence(
        enriched,
        reference_rows,
        transitions,
        selected,
    )
    base["generation"] = generation
    generated_count = _int(generation.get("generated_row_count"))
    input_count = int(getattr(enriched, "height", 0) or 0)
    status = "ready" if generated_count == input_count else "limited"
    reason = None if status == "ready" else "generation_row_limit"
    base["status"] = status
    base["status_code"] = SYNTHETIC_TICK_STATUS_CODES[status]
    base["reason"] = reason
    base["reason_code"] = SYNTHETIC_TICK_REASON_CODES[reason or "none"]

    candidate_report, validation = validate_synthetic_tick_frame(
        output,
        reference_fingerprint,
        target=selected_target,
    )
    base["validation"] = validation
    base["generation_id"] = _stable_id("synthetic-generation", base)
    return SyntheticTickGenerationResult(output, base, candidate_report)


def validate_synthetic_tick_frame(
    generated_frame: Any,
    reference_fingerprint: Mapping[str, JSONValue],
    *,
    target: QualityTarget | None = None,
) -> tuple[QualityReport, dict[str, JSONValue]]:
    """Validate a generated frame through the ordinary fingerprint rule."""
    _require_reference_fingerprint(reference_fingerprint)
    reference_axis = _mapping(reference_fingerprint.get("target_axis"))
    selected_target = target or _target_from_axis(reference_axis)
    _require_tick_axis(reference_axis, selected_target)
    if not _axis_matches_target(reference_axis, selected_target):
        raise ValueError("reference_target_axis_mismatch")
    symbol = _text(reference_axis.get("symbol")) or selected_target.symbol
    period = _text(reference_axis.get("period")) or selected_target.period
    with tempfile.TemporaryDirectory(prefix="histdatacom-synthetic-") as root:
        cache_path = Path(root) / f"DAT_ASCII_{symbol}_T_{period}.data"
        _write_candidate_market_cache(generated_frame, cache_path)
        candidate_target = QualityTarget(
            path=str(cache_path),
            kind=QualityTargetKind.CACHE,
            data_format="ascii",
            timeframe=TICK,
            symbol=symbol,
            period=period,
        )
        report, validation = validate_synthetic_tick_cache(
            cache_path,
            reference_fingerprint,
            target=candidate_target,
        )
        validation["same_point_influx_projection"] = (
            _influx_projection_evidence(generated_frame, selected_target)
        )
        return report, validation


def validate_synthetic_tick_cache(
    cache_path: str | Path,
    reference_fingerprint: Mapping[str, JSONValue],
    *,
    target: QualityTarget | None = None,
) -> tuple[QualityReport, dict[str, JSONValue]]:
    """Fingerprint and validate a generated candidate cache."""
    _require_reference_fingerprint(reference_fingerprint)
    source = Path(cache_path)
    reference_axis = _mapping(reference_fingerprint.get("target_axis"))
    candidate_target = target or QualityTarget(
        path=str(source),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol=_text(reference_axis.get("symbol")),
        period=_text(reference_axis.get("period")),
    )
    _require_tick_axis(reference_axis, candidate_target)
    if not _axis_matches_target(reference_axis, candidate_target):
        raise ValueError("reference_target_axis_mismatch")
    [candidate_finding] = HistDataSeriesFingerprintRule(
        profile=HistDataFingerprintProfile()
    ).evaluate(candidate_target)
    candidate_report = QualityReport(
        targets=(candidate_target,),
        rule_results=(
            QualityRuleResult(
                rule_id=candidate_finding.rule_id,
                target=candidate_target,
                findings=(candidate_finding,),
            ),
        ),
    )
    reference_report = _fingerprint_report(
        reference_fingerprint,
        target=_target_from_axis(reference_axis),
    )
    comparison = validate_synthetic_constraint_reports(
        reference_report,
        candidate_report,
    )
    candidate_fingerprint = _mapping(
        candidate_finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
    )
    validation: dict[str, JSONValue] = {
        "schema_version": SYNTHETIC_TICK_GENERATION_VALIDATION_SCHEMA_VERSION,
        "status": comparison.get("status"),
        "same_fingerprint_path_used": True,
        "candidate_fingerprint_schema_version": candidate_fingerprint.get(
            "schema_version"
        ),
        "candidate_fingerprint_id": candidate_fingerprint.get("fingerprint_id"),
        "constraint_validation": comparison,
        "same_point_influx_projection": _influx_projection_evidence(
            read_polars_cache(source), candidate_target
        ),
        "hard_quality_gate": False,
    }
    return candidate_report, validation


def reference_fingerprint_from_report(
    report: QualityReport,
    *,
    target: QualityTarget | None = None,
) -> dict[str, JSONValue]:
    """Select one fingerprint from a saved report by durable target axis."""
    candidates: list[dict[str, JSONValue]] = []
    for finding in report.findings:
        payload = _mapping(
            finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        )
        if not payload:
            continue
        if target is not None and not _axis_matches_target(
            _mapping(payload.get("target_axis")), target
        ):
            continue
        candidates.append(payload)
    if not candidates:
        raise ValueError("reference_fingerprint_required")
    if len(candidates) > 1 and target is None:
        raise ValueError("multiple_reference_fingerprints_require_target")
    candidates.sort(key=_fingerprint_sort_key)
    return candidates[0]


def format_synthetic_tick_generation(
    diagnostics: Mapping[str, JSONValue],
) -> str:
    """Return concise human-readable generator diagnostics."""
    generation = _mapping(diagnostics.get("generation"))
    validation = _mapping(diagnostics.get("validation"))
    axis = _mapping(diagnostics.get("target_axis"))
    return "\n".join(
        (
            "Synthetic tick generation",
            (
                "target: "
                f"{_text(axis.get('symbol'))} "
                f"{_text(axis.get('timeframe'))} "
                f"{_text(axis.get('period'))}"
            ),
            (
                f"status: {_text(diagnostics.get('status'))} "
                f"reason: {_text(diagnostics.get('reason')) or 'none'}"
            ),
            (
                "rows: "
                f"{_int(generation.get('generated_row_count'))} generated, "
                f"{_int(generation.get('omitted_row_count'))} omitted"
            ),
            (
                "method: "
                f"{_text(generation.get('method'))} "
                f"seed={_int(generation.get('seed'))}"
            ),
            f"validation: {_text(validation.get('status')) or 'not_run'}",
            "observed bid/ask preserved: yes",
        )
    )


def _generate_frame(
    frame: Any,
    reference_rows: Sequence[Mapping[str, Any]],
    transitions: Sequence[tuple[float, float, int]],
    *,
    constraint_id: str,
    constraint_status: str,
    profile: SyntheticTickGenerationProfile,
) -> tuple[Any, dict[str, JSONValue]]:
    import polars as pl

    row_count = int(getattr(frame, "height", 0) or 0)
    generated_count = min(row_count, profile.max_generated_rows)
    indices = _sample_transition_indices(
        len(transitions),
        generated_count,
        block_size=profile.block_size,
        seed=profile.seed,
        constraint_id=constraint_id,
    )
    valid_mids = [_float(row.get("mid")) for row in reference_rows]
    anchor_mid = (
        median(valid_mids)
        if profile.anchor_mode == "median_valid_mid"
        else valid_mids[0]
    )
    minimum_price = 10 ** (-profile.rounding_digits)
    previous_mid = max(
        minimum_price,
        min(MAX_SYNTHETIC_TICK_PRICE, anchor_mid),
    )
    confidence = _confidence(
        valid_count=len(reference_rows),
        inspected_count=min(row_count, profile.max_reference_rows),
        constraint_status=constraint_status,
        digits=profile.rounding_digits,
    )
    bids: list[float | None] = []
    asks: list[float | None] = []
    spreads: list[float | None] = []
    mids: list[float | None] = []
    method_codes: list[int | None] = []
    confidences: list[float | None] = []
    usable: list[bool | None] = []
    samples: list[dict[str, JSONValue]] = []
    clipped_count = 0
    price_clamp_count = int(previous_mid != anchor_mid)
    for output_index in range(generated_count):
        transition_index = indices[output_index]
        log_return, sampled_spread, source_row_id = transitions[
            transition_index
        ]
        bounded_return = max(
            -profile.max_abs_log_return,
            min(profile.max_abs_log_return, log_return),
        )
        clipped_count += bounded_return != log_return
        if output_index:
            candidate_mid = previous_mid * math.exp(bounded_return)
            previous_mid = max(
                minimum_price,
                min(MAX_SYNTHETIC_TICK_PRICE, candidate_mid),
            )
            price_clamp_count += previous_mid != candidate_mid
        spread = max(0.0, sampled_spread)
        mid = previous_mid
        bid = mid - spread / 2.0
        ask = mid + spread / 2.0
        if bid <= 0:
            bid = max(mid * 0.5, minimum_price)
            ask = bid + spread
            mid = (bid + ask) / 2.0
        rounded_bid = round(bid, profile.rounding_digits)
        bid = max(
            minimum_price,
            min(MAX_SYNTHETIC_TICK_PRICE, rounded_bid),
        )
        rounded_ask = round(max(ask, bid), profile.rounding_digits)
        ask = max(bid, min(MAX_SYNTHETIC_TICK_PRICE, rounded_ask))
        price_clamp_count += bid != rounded_bid or ask != rounded_ask
        # Derive these fields from the persisted synthetic quotes so the row
        # contract remains exact for downstream scalar consumers.
        spread = ask - bid
        mid = round((ask + bid) / 2.0, profile.rounding_digits)
        bids.append(bid)
        asks.append(ask)
        spreads.append(spread)
        mids.append(mid)
        method_codes.append(SYNTHETIC_TICK_METHOD_CODES[profile.method])
        confidences.append(confidence)
        usable.append(True)
        if len(samples) < profile.diagnostic_sample_limit:
            samples.append(
                {
                    "output_row_offset": output_index,
                    "reference_transition_index": transition_index,
                    "reference_source_row_id": source_row_id,
                }
            )
    omitted = row_count - generated_count
    bids.extend([None] * omitted)
    asks.extend([None] * omitted)
    spreads.extend([None] * omitted)
    mids.extend([None] * omitted)
    method_codes.extend([None] * omitted)
    confidences.extend([None] * omitted)
    usable.extend([False] * omitted)
    output = frame.drop(
        [
            name
            for name in SYNTHETIC_PLACEHOLDER_COLUMNS
            if name in frame.columns
        ]
    ).with_columns(
        [
            pl.Series("synth_bid", bids, dtype=pl.Float64),
            pl.Series("synth_ask", asks, dtype=pl.Float64),
            pl.Series("synth_spread", spreads, dtype=pl.Float64),
            pl.Series("synth_mid", mids, dtype=pl.Float64),
            pl.Series("synth_method_code", method_codes, dtype=pl.Int32),
            pl.Series("synth_confidence", confidences, dtype=pl.Float64),
            pl.Series("synth_usable", usable, dtype=pl.Boolean),
        ]
    )
    return output, {
        "method": profile.method,
        "method_code": SYNTHETIC_TICK_METHOD_CODES[profile.method],
        "seed": profile.seed,
        "block_size": profile.block_size,
        "calculation_basis": "paired_mid_log_return_and_spread_blocks",
        "input_row_count": row_count,
        "generated_row_count": generated_count,
        "omitted_row_count": omitted,
        "return_clip_count": clipped_count,
        "price_clamp_count": price_clamp_count,
        "price_bounds": {
            "minimum": minimum_price,
            "maximum": MAX_SYNTHETIC_TICK_PRICE,
        },
        "confidence": confidence,
        "sample_count": len(samples),
        "samples": cast(JSONValue, samples),
        "samples_truncated": generated_count > len(samples),
        "same_row_grain": True,
        "timestamp_generation": False,
        "observed_columns_overwritten": False,
        "synthetic_columns_only": True,
    }


def _reference_rows(
    frame: Any,
    profile: SyntheticTickGenerationProfile,
) -> list[dict[str, Any]]:
    columns = ["row_id", "mid", "spread", *HARD_ISSUE_COLUMNS]
    rows = cast(
        list[dict[str, Any]],
        frame.select(columns).head(profile.max_reference_rows).to_dicts(),
    )
    return [row for row in rows if _valid_reference_row(row)]


def _valid_reference_row(row: Mapping[str, Any]) -> bool:
    mid = _optional_float(row.get("mid"))
    spread = _optional_float(row.get("spread"))
    return (
        mid is not None
        and mid > 0
        and spread is not None
        and spread >= 0
        and not any(bool(row.get(column)) for column in HARD_ISSUE_COLUMNS)
    )


def _reference_transitions(
    rows: Sequence[Mapping[str, Any]],
) -> list[tuple[float, float, int]]:
    transitions: list[tuple[float, float, int]] = []
    previous: Mapping[str, Any] | None = None
    for row in rows:
        if previous is None:
            previous = row
            continue
        current_row_id = _int(row.get("row_id"))
        previous_row_id = _int(previous.get("row_id"))
        if current_row_id != previous_row_id + 1:
            previous = row
            continue
        current_mid = _float(row.get("mid"))
        previous_mid = _float(previous.get("mid"))
        log_return = math.log(current_mid / previous_mid)
        if math.isfinite(log_return):
            transitions.append(
                (
                    log_return,
                    max(0.0, _float(row.get("spread"))),
                    current_row_id,
                )
            )
        previous = row
    return transitions


def _sample_transition_indices(
    transition_count: int,
    output_count: int,
    *,
    block_size: int,
    seed: int,
    constraint_id: str,
) -> list[int]:
    indices: list[int] = []
    block_number = 0
    while len(indices) < output_count:
        material = (
            f"{seed}:{constraint_id}:{block_number}:{transition_count}"
        ).encode("utf-8")
        start = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
        start %= transition_count
        for offset in range(block_size):
            indices.append((start + offset) % transition_count)
            if len(indices) >= output_count:
                break
        block_number += 1
    return indices


def _reference_evidence(
    frame: Any,
    rows: Sequence[Mapping[str, Any]],
    transitions: Sequence[tuple[float, float, int]],
    profile: SyntheticTickGenerationProfile,
) -> dict[str, JSONValue]:
    inspected = min(
        int(getattr(frame, "height", 0) or 0), profile.max_reference_rows
    )
    return {
        "inspected_row_count": inspected,
        "valid_reference_row_count": len(rows),
        "filtered_reference_row_count": max(0, inspected - len(rows)),
        "transition_count": len(transitions),
        "reference_rows_truncated": int(getattr(frame, "height", 0) or 0)
        > inspected,
        "hard_issue_columns": list(HARD_ISSUE_COLUMNS),
        "defective_rows_used": False,
    }


def _base_diagnostics(
    profile: SyntheticTickGenerationProfile,
    fingerprint: Mapping[str, JSONValue],
    constraints: Mapping[str, JSONValue],
    target: QualityTarget,
    *,
    input_row_count: int,
) -> dict[str, JSONValue]:
    return {
        "schema_version": SYNTHETIC_TICK_GENERATION_SCHEMA_VERSION,
        "advisory": True,
        "status": "unavailable",
        "status_code": SYNTHETIC_TICK_STATUS_CODES["unavailable"],
        "reason": None,
        "reason_code": SYNTHETIC_TICK_REASON_CODES["none"],
        "target_axis": {
            "data_format": target.data_format,
            "timeframe": target.timeframe,
            "symbol": target.symbol,
            "period": target.period,
            "kind": target.kind.value,
        },
        "reference_fingerprint_id": fingerprint.get("fingerprint_id"),
        "reference_fingerprint_count": 1,
        "reference_fingerprint_ids": [fingerprint.get("fingerprint_id")],
        "reference_constraint_id": constraints.get("constraint_id"),
        "reference_constraint_status": constraints.get("status"),
        "constraint_application": _constraint_application(constraints),
        "reference_fingerprint_required": True,
        "configuration": profile.to_metadata(),
        "input_row_count": input_row_count,
        "base_grain": {"data_format": "ascii", "timeframe": TICK},
        "output_contract": {
            "columns": list(SYNTHETIC_PLACEHOLDER_COLUMNS),
            "durable_identity": ["series_id", "period", "row_id"],
            "timestamp_is_sole_identity": False,
            "observed_bid_ask_preserved": True,
            "same_frame_projection": True,
            "volume_generation": False,
            "raw_m1_generation": False,
        },
        "resource_policy": {
            "max_reference_rows": profile.max_reference_rows,
            "max_generated_rows": profile.max_generated_rows,
            "bounded": True,
        },
        "quality_gate": False,
    }


def _unavailable_result(
    frame: Any,
    diagnostics: dict[str, JSONValue],
    reason: str,
) -> SyntheticTickGenerationResult:
    diagnostics["status"] = "unavailable"
    diagnostics["status_code"] = SYNTHETIC_TICK_STATUS_CODES["unavailable"]
    diagnostics["reason"] = reason
    diagnostics["reason_code"] = SYNTHETIC_TICK_REASON_CODES[reason]
    diagnostics["validation"] = {
        "schema_version": SYNTHETIC_TICK_GENERATION_VALIDATION_SCHEMA_VERSION,
        "status": "not_run",
        "reason": reason,
        "same_fingerprint_path_used": False,
    }
    diagnostics["generation_id"] = _stable_id(
        "synthetic-generation", diagnostics
    )
    return SyntheticTickGenerationResult(frame, diagnostics)


def _write_candidate_market_cache(frame: Any, path: Path) -> None:
    import polars as pl

    columns = set(getattr(frame, "columns", ()))
    required = {"datetime", "synth_bid", "synth_ask"}
    if not required.issubset(columns):
        raise ValueError("generated frame is missing candidate market columns")
    expressions = [
        pl.col("datetime").cast(pl.Int64),
        pl.col("synth_bid").cast(pl.Float64).alias("bid"),
        pl.col("synth_ask").cast(pl.Float64).alias("ask"),
        (
            pl.col("vol").cast(pl.Int64)
            if "vol" in columns
            else pl.lit(0).cast(pl.Int64).alias("vol")
        ),
    ]
    candidate = frame.filter(pl.col("synth_usable").fill_null(False)).select(
        expressions
    )
    if not candidate.height:
        raise ValueError("generated frame has no usable synthetic rows")
    write_polars_cache(candidate, path)


def _constraint_application(
    constraints: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    defects = _mapping_rows(constraints.get("defects_to_avoid"))
    stylized = _mapping_rows(constraints.get("stylized_facts_to_preserve"))
    artifacts = _mapping_rows(
        constraints.get("source_artifacts_to_parameterize")
    )
    return {
        "defects_to_avoid": {
            "count": len(defects),
            "codes": [_text(item.get("code")) for item in defects],
            "application": "filter_flagged_reference_rows_and_enforce_quote_invariants",
        },
        "stylized_facts_to_preserve": {
            "count": len(stylized),
            "codes": [_text(item.get("code")) for item in stylized],
            "application": "paired_empirical_blocks_then_fingerprint_validation",
        },
        "source_artifacts_to_parameterize": {
            "count": len(artifacts),
            "codes": [_text(item.get("code")) for item in artifacts],
            "application": "preserve_reference_row_topology_and_expose_configuration",
        },
    }


def _influx_projection_evidence(
    frame: Any,
    target: QualityTarget,
) -> dict[str, JSONValue]:
    columns = set(getattr(frame, "columns", ()))
    required = {"datetime", "bid", "ask", "synth_bid", "synth_ask"}
    if not required.issubset(columns) or not int(
        getattr(frame, "height", 0) or 0
    ):
        return {
            "status": "not_applicable",
            "reason": "enriched_synthetic_columns_unavailable",
            "same_measurement": False,
        }
    usable = (
        frame.filter(frame.get_column("synth_usable").fill_null(False))
        if "synth_usable" in columns
        else frame
    )
    if not int(getattr(usable, "height", 0) or 0):
        return {
            "status": "unavailable",
            "reason": "no_usable_synthetic_row",
            "same_measurement": False,
        }
    line = format_influx_line(
        target.symbol,
        target.data_format,
        target.timeframe,
        usable.row(0),
        columns=usable.columns,
    )
    observed = "bidquote=" in line and "askquote=" in line
    synthetic_fields = all(
        f"{name}=" in line for name in SYNTHETIC_PLACEHOLDER_COLUMNS
    )
    training_fields = all(
        marker in line
        for marker in (
            ",row_id=",
            "mid=",
            "dq_issue_negative_spread=",
            "class_training_action_code=",
        )
    )
    same_measurement = (
        observed
        and synthetic_fields
        and training_fields
        and line.count(" ") == 2
    )
    return {
        "status": "valid" if same_measurement else "invalid",
        "reason": (
            None if same_measurement else "same_point_projection_missing_fields"
        ),
        "same_measurement": same_measurement,
        "observed_fields_present": observed,
        "synthetic_fields_present": synthetic_fields,
        "training_fields_present": training_fields,
        "line_count": 1,
        "raw_line_included": False,
    }


def _fingerprint_report(
    fingerprint: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
) -> QualityReport:
    finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=target,
        metadata={TIME_SERIES_FINGERPRINT_METADATA_KEY: dict(fingerprint)},
    )
    return QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=target,
                findings=(finding,),
            ),
        ),
    )


def _target_from_axis(axis: Mapping[str, JSONValue]) -> QualityTarget:
    return QualityTarget(
        path=(
            f"DAT_ASCII_{_text(axis.get('symbol'))}_T_"
            f"{_text(axis.get('period'))}.data"
        ),
        kind=QualityTargetKind.CACHE,
        data_format=_text(axis.get("data_format")) or "ascii",
        timeframe=_text(axis.get("timeframe")) or TICK,
        symbol=_text(axis.get("symbol")),
        period=_text(axis.get("period")),
    )


def _require_tick_axis(
    axis: Mapping[str, JSONValue], target: QualityTarget
) -> None:
    data_format = _text(axis.get("data_format")) or target.data_format
    timeframe = _text(axis.get("timeframe")) or target.timeframe
    if data_format.lower() != "ascii" or timeframe != TICK:
        raise ValueError("unsupported_base_grain")


def _require_reference_fingerprint(
    fingerprint: Mapping[str, JSONValue],
) -> None:
    if not fingerprint:
        raise ValueError("reference_fingerprint_required")
    if (
        fingerprint.get("schema_version")
        != TIME_SERIES_FINGERPRINT_SCHEMA_VERSION
    ):
        raise ValueError("unsupported_reference_fingerprint_schema")
    if not _text(fingerprint.get("fingerprint_id")):
        raise ValueError("reference_fingerprint_id_required")


def _axis_matches_target(
    axis: Mapping[str, JSONValue], target: QualityTarget
) -> bool:
    return bool(
        _text(axis.get("data_format")).lower() == target.data_format.lower()
        and _text(axis.get("timeframe")) == target.timeframe
        and _text(axis.get("symbol")).upper() == target.symbol.upper()
        and _text(axis.get("period")) == target.period
    )


def _fingerprint_sort_key(
    fingerprint: Mapping[str, JSONValue],
) -> tuple[str, str, str, str]:
    axis = _mapping(fingerprint.get("target_axis"))
    return (
        _text(axis.get("data_format")),
        _text(axis.get("timeframe")),
        _text(axis.get("symbol")),
        _text(axis.get("period")),
    )


def _has_existing_synthetic_values(frame: Any) -> bool:
    columns = set(getattr(frame, "columns", ()))
    return any(
        name in columns and frame.get_column(name).null_count() < frame.height
        for name in SYNTHETIC_PLACEHOLDER_COLUMNS
    )


def _confidence(
    *,
    valid_count: int,
    inspected_count: int,
    constraint_status: str,
    digits: int,
) -> float:
    validity = valid_count / inspected_count if inspected_count else 0.0
    constraint_weight = 1.0 if constraint_status == "ready" else 0.75
    return round(min(1.0, validity * constraint_weight), digits)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _bounded_positive(value: int, maximum: int, name: str) -> None:
    if value < 1 or value > maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")


def _mapping(value: Any) -> dict[str, JSONValue]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_rows(value: Any) -> list[dict[str, JSONValue]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_mapping(item) for item in value if isinstance(item, Mapping)]


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    parsed = _optional_float(value)
    return parsed if parsed is not None else 0.0


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


__all__ = [
    "DEFAULT_SYNTHETIC_TICK_BLOCK_SIZE",
    "DEFAULT_SYNTHETIC_TICK_DIAGNOSTIC_SAMPLE_LIMIT",
    "DEFAULT_SYNTHETIC_TICK_MAX_ABS_LOG_RETURN",
    "DEFAULT_SYNTHETIC_TICK_MAX_GENERATED_ROWS",
    "DEFAULT_SYNTHETIC_TICK_MAX_REFERENCE_ROWS",
    "DEFAULT_SYNTHETIC_TICK_MINIMUM_REFERENCE_ROWS",
    "DEFAULT_SYNTHETIC_TICK_ROUNDING_DIGITS",
    "DEFAULT_SYNTHETIC_TICK_SEED",
    "SYNTHETIC_TICK_GENERATION_CONFIGURATION_SCHEMA_VERSION",
    "SYNTHETIC_TICK_GENERATION_SCHEMA_VERSION",
    "SYNTHETIC_TICK_GENERATION_VALIDATION_SCHEMA_VERSION",
    "SYNTHETIC_TICK_METHOD_CODES",
    "SYNTHETIC_TICK_REASON_CODES",
    "SYNTHETIC_TICK_STATUS_CODES",
    "SyntheticTickGenerationProfile",
    "SyntheticTickGenerationResult",
    "format_synthetic_tick_generation",
    "generate_synthetic_ticks_from_reference",
    "reference_fingerprint_from_report",
    "validate_synthetic_tick_cache",
    "validate_synthetic_tick_frame",
]
