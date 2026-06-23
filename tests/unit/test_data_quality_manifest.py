"""Tests for data-quality coverage manifests."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality import (
    QualitySeverity,
    QualityStatus,
    coverage_manifest_metadata,
    discover_quality_targets,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
    run_quality_assessment,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    write_ascii_case,
    write_zip_case,
)


def test_complete_coverage_manifest_is_serialized(
    tmp_path: Path,
) -> None:
    """Expected present files should produce a clean JSON manifest."""
    path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    discovery = discover_quality_targets((path,))

    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("inventory",)),
        run_rules=quality_run_rules_for_groups(("inventory",)),
        metadata={
            "coverage_manifest": coverage_manifest_metadata(
                expected_dimensions=(
                    _dimension(symbol="EURUSD", period="201202"),
                ),
            )
        },
    )

    manifest = report.metadata["coverage_manifest"]
    assert report.status is QualityStatus.CLEAN
    assert manifest["schema_version"] == "histdatacom.coverage-manifest.v1"
    assert manifest["expected_source"] == "metadata"
    assert manifest["expected_count"] == 1
    assert manifest["present_count"] == 1
    assert manifest["missing"] == []
    assert manifest["duplicates"] == []
    assert manifest["unexpected"] == []
    assert json.loads(json.dumps(report.to_dict()))["metadata"][
        "coverage_manifest"
    ]["present"][0]["dimension"] == _dimension(
        symbol="EURUSD",
        period="201202",
    )


def test_coverage_manifest_reports_missing_period(
    tmp_path: Path,
) -> None:
    """Expected dimensions without local targets should be hard failures."""
    path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    discovery = discover_quality_targets((path,))

    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("inventory",)),
        run_rules=quality_run_rules_for_groups(("inventory",)),
        metadata={
            "coverage_manifest": coverage_manifest_metadata(
                expected_dimensions=(
                    _dimension(symbol="EURUSD", period="201202"),
                    _dimension(symbol="EURUSD", period="201203"),
                ),
            )
        },
    )

    manifest = report.metadata["coverage_manifest"]
    finding = report.findings[0]
    assert report.status is QualityStatus.FAILED
    assert manifest["missing"] == [_dimension(symbol="EURUSD", period="201203")]
    assert finding.code == "COVERAGE_PERIOD_MISSING"
    assert finding.severity is QualitySeverity.ERROR
    assert finding.metadata["dimension"] == _dimension(
        symbol="EURUSD",
        period="201203",
    )


def test_coverage_manifest_reports_duplicate_same_kind_files(
    tmp_path: Path,
) -> None:
    """Same dataset dimension and artifact kind should be reported once."""
    write_ascii_case(tmp_path, CLEAN_M1_CASE)
    duplicate = tmp_path / "DAT_ASCII_EURUSD_M1_201202_COPY.csv"
    duplicate.write_text(CLEAN_M1_CASE.text, encoding="utf-8")
    discovery = discover_quality_targets((tmp_path,))

    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("inventory",)),
        run_rules=quality_run_rules_for_groups(("inventory",)),
        metadata={
            "coverage_manifest": coverage_manifest_metadata(
                expected_dimensions=(
                    _dimension(symbol="EURUSD", period="201202"),
                ),
            )
        },
    )

    manifest = report.metadata["coverage_manifest"]
    finding = report.findings[0]
    assert report.status is QualityStatus.WARNING
    assert manifest["duplicate_count"] == 1
    assert manifest["duplicates"][0]["artifact_kind"] == "csv"
    assert finding.code == "COVERAGE_DUPLICATE_FILE"
    assert finding.severity is QualitySeverity.WARNING


def test_coverage_manifest_reports_unexpected_extra_file(
    tmp_path: Path,
) -> None:
    """Observed dimensions outside the expected set should be warnings."""
    write_ascii_case(tmp_path, CLEAN_M1_CASE)
    extra = tmp_path / "DAT_ASCII_GBPUSD_M1_201202.csv"
    extra.write_text(CLEAN_M1_CASE.text, encoding="utf-8")
    discovery = discover_quality_targets((tmp_path,))

    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("inventory",)),
        run_rules=quality_run_rules_for_groups(("inventory",)),
        metadata={
            "coverage_manifest": coverage_manifest_metadata(
                expected_dimensions=(
                    _dimension(symbol="EURUSD", period="201202"),
                ),
            )
        },
    )

    manifest = report.metadata["coverage_manifest"]
    finding = report.findings[0]
    assert report.status is QualityStatus.WARNING
    assert manifest["unexpected_count"] == 1
    assert manifest["unexpected"][0]["dimension"] == _dimension(
        symbol="GBPUSD",
        period="201202",
    )
    assert finding.code == "COVERAGE_UNEXPECTED_FILE"
    assert finding.location.path == str(extra.resolve())


def test_coverage_manifest_uses_local_repo_ranges_offline(
    tmp_path: Path,
) -> None:
    """Local .repo metadata should expand expected periods without network."""
    write_ascii_case(tmp_path, CLEAN_M1_CASE)
    (tmp_path / ".repo").write_text(
        json.dumps({"eurusd": {"start": "201202", "end": "201203"}}),
        encoding="utf-8",
    )
    discovery = discover_quality_targets((tmp_path,))

    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("inventory",)),
        run_rules=quality_run_rules_for_groups(("inventory",)),
        metadata={
            "coverage_manifest": coverage_manifest_metadata(
                roots=discovery.roots,
            )
        },
    )

    manifest = report.metadata["coverage_manifest"]
    assert manifest["expected_source"] == "repo"
    assert manifest["expected_count"] == 2
    assert manifest["missing"] == [_dimension(symbol="EURUSD", period="201203")]
    assert report.findings[0].code == "COVERAGE_PERIOD_MISSING"


def test_archive_and_csv_for_same_dimension_are_not_duplicates(
    tmp_path: Path,
) -> None:
    """A retained ZIP plus extracted CSV should not look like duplicate files."""
    write_ascii_case(tmp_path, CLEAN_M1_CASE)
    write_zip_case(tmp_path, CLEAN_M1_CASE)
    discovery = discover_quality_targets((tmp_path,))

    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("inventory",)),
        run_rules=quality_run_rules_for_groups(("inventory",)),
        metadata={
            "coverage_manifest": coverage_manifest_metadata(
                expected_dimensions=(
                    _dimension(symbol="EURUSD", period="201202"),
                ),
            )
        },
    )

    manifest = report.metadata["coverage_manifest"]
    assert report.status is QualityStatus.CLEAN
    assert manifest["present"][0]["artifact_kinds"] == ["csv", "zip"]
    assert manifest["duplicate_count"] == 0


def _dimension(
    *,
    symbol: str,
    period: str,
    data_format: str = "ascii",
    timeframe: str = "M1",
) -> dict[str, str]:
    return {
        "data_format": data_format,
        "timeframe": timeframe,
        "symbol": symbol,
        "period": period,
    }
