"""Tests for inventory data-quality rules."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_TICK_CASE,
    case_by_name,
    write_corrupt_zip,
    write_zip_case,
)


def test_valid_histdata_zip_passes_and_exposes_metadata(tmp_path: Path) -> None:
    """A clean single-member HistData ZIP should pass inventory checks."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    assert report.status is QualityStatus.CLEAN
    assert report.findings == ()
    assert target.data_format == "ascii"
    assert target.symbol == "EURUSD"
    assert target.timeframe == "M1"
    assert target.period == "201202"
    assert target.metadata["filename"] == "DAT_ASCII_EURUSD_M1_201202.zip"


def test_corrupt_zip_fails_with_clear_error_finding(tmp_path: Path) -> None:
    """Corrupt ZIP archives should be hard failures."""
    archive = write_corrupt_zip(
        tmp_path,
        filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    assert report.status is QualityStatus.FAILED
    assert report.findings[0].severity is QualitySeverity.ERROR
    assert report.findings[0].code == "ZIP_CORRUPT"
    assert "could not be opened" in report.findings[0].message
    assert report.findings[0].location.path == str(archive.resolve())


def test_zip_missing_expected_member_is_reported(tmp_path: Path) -> None:
    """ZIPs without their expected CSV member should fail inventory checks."""
    archive = write_zip_case(
        tmp_path,
        case_by_name("m1_missing_file"),
        zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = report.findings[0]
    assert finding.code == "ZIP_MEMBER_MISSING"
    assert finding.metadata["expected_member"] == (
        "DAT_ASCII_EURUSD_M1_201202.csv"
    )
    assert finding.metadata["observed_members"] == []


def test_zip_extra_member_is_reported_as_warning(tmp_path: Path) -> None:
    """ZIPs with valid data plus extra files should be visible warnings."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_TICK_CASE,
        zip_filename="DAT_ASCII_EURUSD_T_201202.zip",
        extra_members=(("README.txt", "unexpected metadata"),),
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = report.findings[0]
    assert report.status is QualityStatus.WARNING
    assert finding.severity is QualitySeverity.WARNING
    assert finding.code == "ZIP_EXTRA_MEMBER"
    assert finding.metadata["extra_members"] == ["README.txt"]


def test_zip_member_period_mismatch_reports_expected_and_observed(
    tmp_path: Path,
) -> None:
    """Archive/member metadata mismatches should show both sides."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="DAT_ASCII_EURUSD_M1_201203.zip",
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = report.findings[0]
    assert finding.code == "ZIP_MEMBER_UNEXPECTED"
    assert finding.metadata["expected_member"] == (
        "DAT_ASCII_EURUSD_M1_201203.csv"
    )
    assert finding.metadata["observed_members"] == [
        "DAT_ASCII_EURUSD_M1_201202.csv"
    ]
    assert finding.metadata["observed_metadata"] == [
        {
            "member": "DAT_ASCII_EURUSD_M1_201202.csv",
            "data_format": "ascii",
            "symbol": "EURUSD",
            "timeframe": "M1",
            "period": "201202",
        }
    ]


def test_invalid_zip_filename_reports_expected_pattern(tmp_path: Path) -> None:
    """Invalid archive filenames should include expected versus observed."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="EURUSD_201202.zip",
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = report.findings[0]
    assert finding.code == "HISTDATA_ZIP_FILENAME_INVALID"
    assert finding.metadata["expected_pattern"] == (
        "DAT_ASCII_<SYMBOL>_<TIMEFRAME>_<YYYYMM>.zip"
    )
    assert finding.metadata["observed_filename"] == "EURUSD_201202.zip"


def test_suffixed_zip_filename_reports_expected_filename(
    tmp_path: Path,
) -> None:
    """Discovery may parse suffixes, but inventory enforces exact ZIP names."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="DAT_ASCII_EURUSD_M1_201202_DIRTY.zip",
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = report.findings[0]
    assert finding.code == "HISTDATA_ZIP_FILENAME_INVALID"
    assert finding.metadata["expected_filename"] == (
        "DAT_ASCII_EURUSD_M1_201202.zip"
    )
    assert finding.metadata["observed_filename"] == (
        "DAT_ASCII_EURUSD_M1_201202_DIRTY.zip"
    )
