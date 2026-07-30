"""Tests for feed-regime analytics."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from histdatacom.data_analytics import (
    ANALYTICS_REPORT_SCHEMA_VERSION,
    analyze_feed_regimes,
    discover_analytics_targets,
    format_feed_regime_console_summary,
)
from histdatacom.data_analytics.cli import main as analytics_main


def _write_tick_csv(
    root: Path,
    period: str,
    rows: tuple[tuple[str, float, float, int], ...],
) -> Path:
    path = root / f"DAT_ASCII_EURUSD_T_{period}.csv"
    path.write_text(
        "\n".join(
            f"{timestamp},{bid:.5f},{ask:.5f},{volume}"
            for timestamp, bid, ask, volume in rows
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _sampled_long_history_dataset(root: Path) -> tuple[Path, Path]:
    sparse = _write_tick_csv(
        root,
        "200101",
        (
            ("20010102 000000000", 1.00000, 1.00020, 0),
            ("20010102 001000000", 1.00000, 1.00020, 0),
            ("20010102 002000000", 1.00010, 1.00030, 0),
        ),
    )
    dense = _write_tick_csv(
        root,
        "202201",
        tuple(
            (
                f"20220103 00000{second}000",
                1.10000 + second * 0.00001,
                1.10020 + second * 0.00001,
                second,
            )
            for second in range(10)
        ),
    )
    return sparse, dense


def test_discovery_projects_canonical_data_quality_targets(
    tmp_path: Path,
) -> None:
    """Analytics discovery should reuse the canonical target scanner."""
    sparse, dense = _sampled_long_history_dataset(tmp_path)

    discovery = discover_analytics_targets((tmp_path,))

    assert {target.path for target in discovery.targets} == {
        str(sparse.resolve()),
        str(dense.resolve()),
    }
    assert all(target.is_supported_tick_target for target in discovery.targets)
    assert discovery.metadata["quality_semantics"] == (
        "analytics-only; no pass/fail status"
    )
    assert discovery.metadata["discovery_basis"] == (
        "canonical_quality_discovery"
    )


def test_feed_regime_analysis_segments_sparse_and_dense_periods(
    tmp_path: Path,
) -> None:
    """A sampled long-history dataset should produce regime boundaries."""
    _sampled_long_history_dataset(tmp_path)

    report = analyze_feed_regimes((tmp_path,), quiet_gap_ms=60_000)
    payload = report.to_dict()
    labels = {regime.label for regime in report.regimes}
    profile_by_period = {
        profile.period: profile for profile in report.period_profiles
    }

    assert payload["schema_version"] == ANALYTICS_REPORT_SCHEMA_VERSION
    assert payload["operation"] == "feed-regime-detection"
    assert payload["summary"]["symbols"] == ["EURUSD"]
    assert labels == {"epoch-001", "epoch-002"}
    assert payload["epoch_definition"]["boundaries"][0]["right_period"] == (
        "202201"
    )
    assert payload["metadata"]["fitting_basis"] == (
        "canonical_time_series_fingerprint"
    )
    assert profile_by_period["200101"].quiet_gap_count == 2
    assert profile_by_period["200101"].zero_change_run_count == 0
    assert profile_by_period["202201"].tick_rate_per_hour > (
        profile_by_period["200101"].tick_rate_per_hour
    )
    assert "pass/fail" in payload["metadata"]["quality_semantics"]


def test_feed_regime_report_console_summary(tmp_path: Path) -> None:
    """Console output should stay compact and descriptive."""
    _sampled_long_history_dataset(tmp_path)
    report = analyze_feed_regimes((tmp_path,))

    summary = format_feed_regime_console_summary(report)

    assert "Feed regime analytics" in summary
    assert "regimes: 2" in summary
    assert "EURUSD 200101-200101 epoch-001" in summary
    assert "EURUSD 202201-202201 epoch-002" in summary
    assert "Uncertain transitions" in summary


def test_year_bucket_aggregates_canonical_months_without_losing_lineage(
    tmp_path: Path,
) -> None:
    """Annual fitting should coarsen fingerprints, not rescan or relabel rows."""
    _sampled_long_history_dataset(tmp_path)

    report = analyze_feed_regimes((tmp_path,), bucket="year")
    definition = report.epoch_definition

    assert definition is not None
    assert {profile.period for profile in report.period_profiles} == {
        "2001",
        "2022",
    }
    assert report.metadata["requested_bucket"] == "year"
    preparation = report.metadata["evidence_preparation"]
    assert preparation["effective_bucket"] == "year"
    assert preparation["mixed_granularity_policy"] == (
        "coarsen_to_year_never_disaggregate"
    )
    assert definition.lineage["canonical_source_count"] == 2
    assert {
        source["period"] for source in definition.lineage["canonical_sources"]
    } == {
        "200101",
        "202201",
    }
    assert {
        source["source_hash_basis"] for source in definition.lineage["sources"]
    } == {"canonical_fingerprint_aggregate_id"}


def test_mixed_annual_monthly_evidence_uses_lossless_annual_grid(
    tmp_path: Path,
) -> None:
    """Annual evidence must force safe coarsening and resolve overlapping axes."""
    _write_tick_csv(
        tmp_path,
        "2001",
        (
            ("20010102 000000000", 1.00000, 1.00020, 0),
            ("20010102 001000000", 1.00010, 1.00030, 0),
        ),
    )
    _write_tick_csv(
        tmp_path,
        "2022",
        (
            ("20220103 000000000", 1.10000, 1.10020, 0),
            ("20220103 000001000", 1.10010, 1.10030, 0),
        ),
    )
    _write_tick_csv(
        tmp_path,
        "202201",
        (
            ("20220103 000000000", 1.10000, 1.10020, 0),
            ("20220103 000001000", 1.10010, 1.10030, 0),
        ),
    )

    report = analyze_feed_regimes((tmp_path,), bucket="month")
    preparation = report.metadata["evidence_preparation"]

    assert preparation["effective_bucket"] == "year"
    assert preparation["effective_bucket_reason"] == (
        "annual_evidence_cannot_be_safely_disaggregated"
    )
    assert preparation["annual_overlap_skip_count"] == 1
    assert {profile.period for profile in report.period_profiles} == {
        "2001",
        "2022",
    }


def test_feed_regime_cli_writes_machine_readable_report(
    tmp_path: Path,
    capsys,
) -> None:
    """The analytics subcommand should write structured report JSON."""
    _sampled_long_history_dataset(tmp_path)
    report_path = tmp_path / "reports" / "feed-regimes.json"
    epoch_path = tmp_path / "reports" / "feed-epochs.json"

    exit_code = analytics_main(
        [
            "feed-regimes",
            "--target",
            str(tmp_path),
            "--report",
            str(report_path),
            "--epoch-artifact",
            str(epoch_path),
            "--features",
            "log_tick_rate_per_hour",
            "log_median_interarrival_ms",
            "--min-evidence-periods",
            "2",
            "--min-segment-periods",
            "1",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Feed regime analytics" in output
    assert f"report: {report_path.resolve()}" in output
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["operation"] == "feed-regime-detection"
    assert payload["summary"]["regime_count"] == 2
    epoch_payload = json.loads(epoch_path.read_text(encoding="utf-8"))
    assert (
        epoch_payload["definition_id"]
        == payload["summary"]["epoch_definition_id"]
    )


def test_feed_regime_cli_reads_yaml_config(
    tmp_path: Path,
    capsys,
) -> None:
    """Issue #31: analytics commands should accept recurrent YAML defaults."""
    _sampled_long_history_dataset(tmp_path)
    config_path = tmp_path / "histdatacom.yaml"
    report_path = tmp_path / "reports" / "feed-regimes.json"
    epoch_path = tmp_path / "reports" / "feed-epochs.json"
    config_path.write_text(
        f"""
histdatacom:
  analytics:
    command: feed-regimes
    target: {tmp_path}
    report: {report_path}
    epoch_artifact: {epoch_path}
    features:
      - log_tick_rate_per_hour
      - log_median_interarrival_ms
    min_evidence_periods: 2
    min_segment_periods: 1
    max_sensitivity_runs: 3
    json: true
""",
        encoding="utf-8",
    )

    exit_code = analytics_main(["--config", str(config_path)])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == ANALYTICS_REPORT_SCHEMA_VERSION
    assert payload["epoch_definition"]["config"]["feature_names"] == [
        "log_tick_rate_per_hour",
        "log_median_interarrival_ms",
    ]
    assert report_path.exists()
    assert epoch_path.exists()


def test_top_level_main_dispatches_analytics_subcommand(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """histdatacom analytics should bypass the orchestration request parser."""
    import histdatacom.histdata_com as histdata_com

    _sampled_long_history_dataset(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "analytics",
            "feed-regimes",
            "--target",
            str(tmp_path),
            "--json",
        ],
    )

    assert histdata_com.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == ANALYTICS_REPORT_SCHEMA_VERSION
