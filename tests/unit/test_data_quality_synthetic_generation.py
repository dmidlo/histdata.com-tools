"""Tests for deterministic reference-set synthetic tick generation."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from histdatacom.data_quality.contracts import (
    QualityReport,
    QualityRuleResult,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.fingerprints import (
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    HistDataSeriesFingerprintRule,
)
from histdatacom.data_quality.synthetic_generation import (
    SYNTHETIC_TICK_GENERATION_SCHEMA_VERSION,
    SyntheticTickGenerationProfile,
    format_synthetic_tick_generation,
    generate_synthetic_ticks_from_reference,
    reference_fingerprint_from_report,
)
from histdatacom.data_quality.training_features import (
    SYNTHETIC_PLACEHOLDER_COLUMNS,
)
from histdatacom.histdata_ascii import (
    TICK,
    format_influx_line,
    read_polars_cache,
    write_polars_cache,
)


def test_generation_is_deterministic_seeded_and_fingerprint_validated(
    tmp_path: Path,
) -> None:
    frame, fingerprint, target, _ = _reference(tmp_path, count=80)
    profile = _profile(seed=17)

    first = generate_synthetic_ticks_from_reference(
        frame, fingerprint, profile=profile, target=target
    )
    second = generate_synthetic_ticks_from_reference(
        frame, fingerprint, profile=profile, target=target
    )
    alternate = generate_synthetic_ticks_from_reference(
        frame,
        fingerprint,
        profile=_profile(seed=18),
        target=target,
    )

    assert first.diagnostics == second.diagnostics
    assert (
        first.frame.select(SYNTHETIC_PLACEHOLDER_COLUMNS).to_dicts()
        == second.frame.select(SYNTHETIC_PLACEHOLDER_COLUMNS).to_dicts()
    )
    assert (
        first.frame.get_column("synth_mid").to_list()
        != alternate.frame.get_column("synth_mid").to_list()
    )
    assert first.diagnostics["schema_version"] == (
        SYNTHETIC_TICK_GENERATION_SCHEMA_VERSION
    )
    assert first.diagnostics["status"] == "ready"
    application = _mapping(first.diagnostics["constraint_application"])
    assert _mapping(application["defects_to_avoid"])["count"] > 0
    assert _mapping(application["stylized_facts_to_preserve"])["count"] > 0
    assert (
        _mapping(application["source_artifacts_to_parameterize"])["count"] > 0
    )
    validation = _mapping(first.diagnostics["validation"])
    assert validation["same_fingerprint_path_used"] is True
    assert validation["candidate_fingerprint_id"]
    assert validation["hard_quality_gate"] is False
    influx = _mapping(validation["same_point_influx_projection"])
    assert influx["status"] == "valid"
    assert influx["same_measurement"] is True
    assert first.candidate_report is not None

    expected = json.dumps(first.diagnostics, indent=2, sort_keys=True) + "\n"
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "data_quality_reports"
        / "synthetic_tick_generation.json"
    )
    if os.environ.get("HISTDATACOM_UPDATE_QUALITY_GOLDENS") == "1":
        fixture.write_text(expected, encoding="utf-8")
    assert fixture.read_text(encoding="utf-8") == expected


def test_generation_preserves_rows_observed_values_cache_and_influx(
    tmp_path: Path,
) -> None:
    frame, fingerprint, target, _ = _reference(
        tmp_path, count=40, duplicate_timestamp=True
    )
    observed = frame.select("datetime", "bid", "ask").to_dicts()
    result = generate_synthetic_ticks_from_reference(
        frame,
        fingerprint,
        profile=_profile(),
        target=target,
    )
    generated = result.frame

    assert generated.select("datetime", "bid", "ask").to_dicts() == observed
    assert generated.height == frame.height
    assert generated.get_column("row_id").n_unique() == frame.height
    assert generated.head(2).get_column("datetime").n_unique() == 1
    assert generated.get_column("synth_usable").all()
    assert (
        generated.get_column("synth_ask") >= generated.get_column("synth_bid")
    ).all()
    assert (
        generated.get_column("synth_spread")
        == generated.get_column("synth_ask") - generated.get_column("synth_bid")
    ).all()

    output = tmp_path / "generated" / "DAT_ASCII_EURUSD_T_201202.data"
    output.parent.mkdir()
    write_polars_cache(generated, output)
    restored = read_polars_cache(output)
    assert restored.select(SYNTHETIC_PLACEHOLDER_COLUMNS).to_dicts() == (
        generated.select(SYNTHETIC_PLACEHOLDER_COLUMNS).to_dicts()
    )
    line = format_influx_line(
        "EURUSD",
        "ascii",
        TICK,
        restored.row(0),
        columns=restored.columns,
    )
    assert "synth_bid=" in line
    assert "synth_ask=" in line
    assert "synth_method_code=1i" in line
    assert "synth_usable=true" in line
    assert "observed bid/ask preserved: yes" in (
        format_synthetic_tick_generation(result.diagnostics)
    )


def test_resource_bounds_existing_values_and_insufficient_evidence(
    tmp_path: Path,
) -> None:
    frame, fingerprint, target, _ = _reference(tmp_path, count=40)
    limited = generate_synthetic_ticks_from_reference(
        frame,
        fingerprint,
        profile=_profile(max_generated_rows=10),
        target=target,
    )
    generation = _mapping(limited.diagnostics["generation"])

    assert limited.diagnostics["status"] == "limited"
    assert limited.diagnostics["reason"] == "generation_row_limit"
    assert generation["generated_row_count"] == 10
    assert generation["omitted_row_count"] == 30
    assert limited.frame.head(10).get_column("synth_usable").all()
    assert not limited.frame.tail(30).get_column("synth_usable").any()
    assert limited.frame.tail(30).get_column("synth_bid").null_count() == 30

    with pytest.raises(ValueError, match="existing_synthetic_values"):
        generate_synthetic_ticks_from_reference(
            limited.frame,
            fingerprint,
            profile=_profile(),
            target=target,
        )
    overwritten = generate_synthetic_ticks_from_reference(
        limited.frame,
        fingerprint,
        profile=_profile(
            overwrite_existing=True,
        ),
        target=target,
    )
    assert overwritten.diagnostics["status"] == "ready"

    insufficient = generate_synthetic_ticks_from_reference(
        frame.head(7),
        fingerprint,
        profile=_profile(
            minimum_reference_rows=10,
        ),
        target=target,
    )
    assert insufficient.diagnostics["status"] == "unavailable"
    assert insufficient.diagnostics["reason"] == ("insufficient_reference_rows")


def test_defective_reference_rows_are_filtered_not_reproduced(
    tmp_path: Path,
) -> None:
    frame = _raw_frame(60).with_columns(
        pl.when(pl.int_range(pl.len()) == 10)
        .then(pl.col("bid") - 0.01)
        .otherwise(pl.col("ask"))
        .alias("ask")
    )
    frame, fingerprint, target, _ = _reference(tmp_path, frame=frame)
    result = generate_synthetic_ticks_from_reference(
        frame,
        fingerprint,
        profile=_profile(),
        target=target,
    )
    evidence = _mapping(result.diagnostics["reference_evidence"])

    assert evidence["filtered_reference_row_count"] >= 1
    assert evidence["defective_rows_used"] is False
    assert (result.frame.get_column("synth_spread") >= 0).all()
    assert (result.frame.get_column("synth_bid") > 0).all()


def test_report_selection_profile_guards_and_axis_requirements(
    tmp_path: Path,
) -> None:
    frame, fingerprint, target, report = _reference(tmp_path, count=30)
    assert reference_fingerprint_from_report(report, target=target) == (
        fingerprint
    )
    with pytest.raises(ValueError, match="reference_fingerprint_required"):
        reference_fingerprint_from_report(QualityReport(), target=target)
    with pytest.raises(ValueError, match="block_size"):
        SyntheticTickGenerationProfile(block_size=0)
    with pytest.raises(ValueError, match="max_reference_rows"):
        SyntheticTickGenerationProfile(
            minimum_reference_rows=20,
            max_reference_rows=10,
        )
    with pytest.raises(
        ValueError, match="unsupported_reference_fingerprint_schema"
    ):
        generate_synthetic_ticks_from_reference(
            frame,
            {**fingerprint, "schema_version": "future.v2"},
            profile=_profile(),
            target=target,
        )
    with pytest.raises(ValueError, match="reference_target_axis_mismatch"):
        generate_synthetic_ticks_from_reference(
            frame,
            fingerprint,
            profile=_profile(),
            target=QualityTarget(
                path=target.path,
                kind=target.kind,
                data_format=target.data_format,
                timeframe=target.timeframe,
                symbol="GBPUSD",
                period=target.period,
            ),
        )
    with pytest.raises(ValueError, match="unsupported_base_grain"):
        generate_synthetic_ticks_from_reference(
            frame,
            {
                **fingerprint,
                "target_axis": {
                    **_mapping(fingerprint["target_axis"]),
                    "timeframe": "M1",
                },
            },
            profile=_profile(),
            target=target,
        )


def _reference(
    tmp_path: Path,
    *,
    count: int = 80,
    duplicate_timestamp: bool = False,
    frame: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, dict[str, Any], QualityTarget, QualityReport]:
    raw = (
        frame
        if frame is not None
        else _raw_frame(count, duplicate_timestamp=duplicate_timestamp)
    )
    cache = tmp_path / "reference" / "DAT_ASCII_EURUSD_T_201202.data"
    cache.parent.mkdir(exist_ok=True)
    write_polars_cache(raw, cache)
    target = QualityTarget(
        path=str(cache),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    [finding] = HistDataSeriesFingerprintRule().evaluate(target)
    fingerprint = _mapping(
        finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    )
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id=finding.rule_id,
                target=target,
                findings=(finding,),
            ),
        ),
    )
    return raw, fingerprint, target, report


def _raw_frame(
    count: int,
    *,
    duplicate_timestamp: bool = False,
) -> pl.DataFrame:
    timestamps = [1_325_376_000_000 + index * 1_000 for index in range(count)]
    if duplicate_timestamp and count > 1:
        timestamps[1] = timestamps[0]
    mids = [
        1.2 + index * 0.00001 + ((index * 17) % 11 - 5) * 0.000004
        for index in range(count)
    ]
    spreads = [0.0001 + (index % 4) * 0.00001 for index in range(count)]
    return pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": [mid - spread / 2 for mid, spread in zip(mids, spreads)],
            "ask": [mid + spread / 2 for mid, spread in zip(mids, spreads)],
            "vol": [0] * count,
        }
    )


def _profile(**overrides: Any) -> SyntheticTickGenerationProfile:
    values: dict[str, Any] = {
        "block_size": 4,
        "minimum_reference_rows": 8,
        "max_reference_rows": 1_000,
        "max_generated_rows": 1_000,
        "rounding_digits": 8,
        "diagnostic_sample_limit": 4,
    }
    values.update(overrides)
    return SyntheticTickGenerationProfile(**values)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value))
