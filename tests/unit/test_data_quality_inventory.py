"""Tests for inventory data-quality rules."""

from __future__ import annotations

from pathlib import Path
import zipfile

import pytest

from histdatacom.data_quality import (
    QualityFinding,
    QualityReport,
    QualitySeverity,
    QualityStatus,
    QualityTargetKind,
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


def _finding(report: QualityReport, code: str) -> QualityFinding:
    return next(finding for finding in report.findings if finding.code == code)


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
    assert target.metadata["quality_support"]["status"] == "deep-supported"


def test_live_histdata_download_zip_name_passes_inventory(
    tmp_path: Path,
) -> None:
    """Downloaded archives use HISTDATA_COM names around DAT_ASCII members."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="HISTDATA_COM_ASCII_EURUSD_M1201202.zip",
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
    assert target.metadata["filename"] == (
        "HISTDATA_COM_ASCII_EURUSD_M1201202.zip"
    )


def test_non_ascii_zip_passes_inventory_with_boundary_warning(
    tmp_path: Path,
) -> None:
    """Recognized non-ASCII archives should be inventory-only, not clean."""
    archive = tmp_path / "HISTDATA_COM_MT_EURUSD_M1201202.zip"
    with zipfile.ZipFile(archive, "w") as zip_file:
        zip_file.writestr("DAT_MT_EURUSD_M1_201202.csv", "rows")

    target = discover_quality_targets((archive,)).targets[0]
    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = _finding(report, "HISTDATA_FORMAT_INVENTORY_ONLY")
    assert report.status is QualityStatus.WARNING
    assert finding.severity is QualitySeverity.WARNING
    assert finding.rule_id == "inventory.format_support"
    assert finding.metadata["quality_support"]["data_format"] == "metatrader"
    assert finding.metadata["quality_support"]["status"] == "inventory-only"
    assert finding.metadata["quality_support"]["parser_supported"] is False
    assert not any(
        item.rule_id == "inventory.zip.integrity" and item.findings
        for item in report.rule_results
    )


@pytest.mark.parametrize(
    ("zip_filename", "member_filename", "data_format", "timeframe"),
    (
        (
            "HISTDATA_COM_MT_EURUSD_M1201202.zip",
            "DAT_MT_EURUSD_M1_201202.csv",
            "metatrader",
            "M1",
        ),
        (
            "HISTDATA_COM_NT_AUDCAD_T_LAST201212.zip",
            "DAT_NT_AUDCAD_T_LAST_201212.csv",
            "ninjatrader",
            "T_LAST",
        ),
        (
            "HISTDATA_COM_NT_AUDCAD_T_BID201212.zip",
            "DAT_NT_AUDCAD_T_BID_201212.csv",
            "ninjatrader",
            "T_BID",
        ),
        (
            "HISTDATA_COM_NT_AUDCAD_T_ASK201212.zip",
            "DAT_NT_AUDCAD_T_ASK_201212.csv",
            "ninjatrader",
            "T_ASK",
        ),
        (
            "HISTDATA_COM_MS_EURUSD_M1201202.zip",
            "DAT_MS_EURUSD_M1_201202.csv",
            "metastock",
            "M1",
        ),
        (
            "HISTDATA_COM_XLSX_EURUSD_M12022.zip",
            "DAT_XLSX_EURUSD_M1_2022.xlsx",
            "excel",
            "M1",
        ),
    ),
)
def test_advertised_non_ascii_formats_are_inventory_only(
    tmp_path: Path,
    zip_filename: str,
    member_filename: str,
    data_format: str,
    timeframe: str,
) -> None:
    """Every advertised non-ASCII format should have an explicit boundary."""
    archive = tmp_path / zip_filename
    with zipfile.ZipFile(archive, "w") as zip_file:
        zip_file.writestr(member_filename, "rows")

    target = discover_quality_targets((archive,)).targets[0]
    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    assert target.data_format == data_format
    assert target.timeframe == timeframe
    assert report.status is QualityStatus.WARNING
    assert report.findings == (
        _finding(report, "HISTDATA_FORMAT_INVENTORY_ONLY"),
    )


def test_extracted_excel_payload_is_inventory_only(tmp_path: Path) -> None:
    """Direct XLSX payloads should be discovered and bounded explicitly."""
    workbook = tmp_path / "DAT_XLSX_EURUSD_M1_2022.xlsx"
    workbook.write_bytes(b"spreadsheet")

    target = discover_quality_targets((workbook,)).targets[0]
    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = _finding(report, "HISTDATA_FORMAT_INVENTORY_ONLY")
    assert target.kind is QualityTargetKind.SPREADSHEET
    assert target.data_format == "excel"
    assert report.status is QualityStatus.WARNING
    assert finding.metadata["quality_support"]["payload_extension"] == "xlsx"


def test_unsupported_known_format_timeframe_fails_explicitly(
    tmp_path: Path,
) -> None:
    """Known formats used with unsupported timeframes should not look clean."""
    path = tmp_path / "DAT_MT_EURUSD_T_201202.csv"
    path.write_text("rows", encoding="utf-8")

    target = discover_quality_targets((path,)).targets[0]
    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    finding = _finding(report, "HISTDATA_FORMAT_UNSUPPORTED")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.metadata["quality_support"]["status"] == "unsupported"
    assert finding.metadata["quality_support"]["data_format"] == "metatrader"


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


def test_zip_vendor_txt_sidecar_is_allowed(tmp_path: Path) -> None:
    """Live HistData ZIPs include a same-stem TXT sidecar beside the CSV."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="HISTDATA_COM_ASCII_EURUSD_M1201202.zip",
        extra_members=(
            ("DAT_ASCII_EURUSD_M1_201202.txt", "HistData metadata"),
        ),
    )
    target = discover_quality_targets((archive,)).targets[0]

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("inventory",)),
    )

    assert report.status is QualityStatus.CLEAN
    assert report.findings == ()


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
        "DAT_<FORMAT>_<SYMBOL>_<TIMEFRAME>_<YYYY[MM]>.zip or "
        "HISTDATA_COM_<FORMAT>_<SYMBOL>_<TIMEFRAME><YYYY[MM]>.zip"
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
    assert finding.metadata["accepted_filenames"] == [
        "DAT_ASCII_EURUSD_M1_201202.zip",
        "HISTDATA_COM_ASCII_EURUSD_M1201202.zip",
    ]
    assert finding.metadata["observed_filename"] == (
        "DAT_ASCII_EURUSD_M1_201202_DIRTY.zip"
    )
