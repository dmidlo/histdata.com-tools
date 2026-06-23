"""Inventory data-quality rules for local HistData artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import zipfile

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.discovery import quality_metadata_from_filename
from histdatacom.runtime_contracts import JSONValue

ASCII_ZIP_FILENAME_PATTERN = "DAT_ASCII_<SYMBOL>_<TIMEFRAME>_<YYYYMM>.zip"
ASCII_CSV_MEMBER_PATTERN = "DAT_ASCII_<SYMBOL>_<TIMEFRAME>_<YYYYMM>.csv"
ZIP_INVENTORY_RULE_ID = "inventory.zip.integrity"


@dataclass(slots=True)
class HistDataZipInventoryRule:
    """Validate ZIP integrity and HistData archive/member naming."""

    rule_id: str = ZIP_INVENTORY_RULE_ID
    description: str = (
        "Validate ZIP CRC/decompression and expected HistData member names."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return ZIP inventory findings for one quality target."""
        if target.kind is not QualityTargetKind.ZIP:
            return ()

        findings: list[QualityFinding] = []
        path = Path(target.path)
        expected_filename = _expected_zip_filename(target)
        expected_member = _expected_zip_member(target)
        if expected_filename is None:
            findings.append(
                _finding(
                    target,
                    code="HISTDATA_ZIP_FILENAME_INVALID",
                    message=(
                        "ZIP filename does not match expected HistData ASCII "
                        "pattern."
                    ),
                    metadata={
                        "expected_pattern": ASCII_ZIP_FILENAME_PATTERN,
                        "observed_filename": path.name,
                    },
                )
            )
        elif path.name != expected_filename:
            findings.append(
                _finding(
                    target,
                    code="HISTDATA_ZIP_FILENAME_INVALID",
                    message=(
                        "ZIP filename does not match expected HistData ASCII "
                        "metadata."
                    ),
                    metadata={
                        "expected_pattern": ASCII_ZIP_FILENAME_PATTERN,
                        "expected_filename": expected_filename,
                        "observed_filename": path.name,
                    },
                )
            )

        try:
            with zipfile.ZipFile(path, "r") as archive:
                bad_member = archive.testzip()
                if bad_member is not None:
                    findings.append(
                        _finding(
                            target,
                            code="ZIP_CRC_ERROR",
                            message=(
                                "ZIP archive failed CRC/decompression check."
                            ),
                            metadata={"bad_member": bad_member},
                        )
                    )
                    return tuple(findings)
                members = _archive_file_members(archive)
        except zipfile.BadZipFile as exc:
            findings.append(
                _finding(
                    target,
                    code="ZIP_CORRUPT",
                    message="ZIP archive could not be opened.",
                    metadata={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
            )
            return tuple(findings)
        except OSError as exc:
            findings.append(
                _finding(
                    target,
                    code="ZIP_UNREADABLE",
                    message="ZIP archive could not be read.",
                    metadata={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
            )
            return tuple(findings)

        findings.extend(_member_findings(target, members, expected_member))
        return tuple(findings)


def inventory_quality_rules() -> tuple[QualityRule, ...]:
    """Return inventory quality rules in deterministic execution order."""
    rule: QualityRule = HistDataZipInventoryRule()
    return (rule,)


def _member_findings(
    target: QualityTarget,
    members: tuple[str, ...],
    expected_member: str | None,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = []
    if expected_member is None:
        for member in members:
            metadata = quality_metadata_from_filename(member)
            if not metadata:
                findings.append(
                    _finding(
                        target,
                        code="HISTDATA_ZIP_MEMBER_FILENAME_INVALID",
                        message=(
                            "ZIP member filename does not match expected "
                            "HistData ASCII pattern."
                        ),
                        metadata={
                            "expected_pattern": ASCII_CSV_MEMBER_PATTERN,
                            "observed_member": member,
                        },
                    )
                )
        return tuple(findings)

    if not members:
        return (
            _finding(
                target,
                code="ZIP_MEMBER_MISSING",
                message="ZIP archive does not contain the expected CSV member.",
                metadata={
                    "expected_member": expected_member,
                    "observed_members": [],
                },
            ),
        )

    if expected_member not in members:
        findings.append(
            _finding(
                target,
                code="ZIP_MEMBER_UNEXPECTED",
                message="ZIP archive does not contain the expected CSV member.",
                metadata={
                    "expected_member": expected_member,
                    "observed_members": list(members),
                    "observed_metadata": [
                        _member_metadata(member) for member in members
                    ],
                },
            )
        )
        return tuple(findings)

    extra_members = tuple(
        member for member in members if member != expected_member
    )
    if extra_members:
        findings.append(
            _finding(
                target,
                code="ZIP_EXTRA_MEMBER",
                message="ZIP archive contains unexpected extra members.",
                severity=QualitySeverity.WARNING,
                metadata={
                    "expected_member": expected_member,
                    "extra_members": list(extra_members),
                },
            )
        )
    return tuple(findings)


def _archive_file_members(archive: zipfile.ZipFile) -> tuple[str, ...]:
    return tuple(
        sorted(
            info.filename for info in archive.infolist() if not info.is_dir()
        )
    )


def _expected_zip_member(target: QualityTarget) -> str | None:
    if (
        target.data_format != "ascii"
        or not target.symbol
        or not target.timeframe
        or not target.period
    ):
        return None
    return f"DAT_ASCII_{target.symbol}_{target.timeframe}_{target.period}.csv"


def _expected_zip_filename(target: QualityTarget) -> str | None:
    if (
        target.data_format != "ascii"
        or not target.symbol
        or not target.timeframe
        or not target.period
    ):
        return None
    return f"DAT_ASCII_{target.symbol}_{target.timeframe}_{target.period}.zip"


def _member_metadata(member: str) -> dict[str, JSONValue]:
    parsed = quality_metadata_from_filename(member)
    return {
        "member": member,
        "data_format": str(parsed.get("data_format", "") or ""),
        "symbol": str(parsed.get("symbol", "") or ""),
        "timeframe": str(parsed.get("timeframe", "") or ""),
        "period": str(parsed.get("period", "") or ""),
    }


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=ZIP_INVENTORY_RULE_ID,
        target=target,
        location=QualityLocation(path=target.path),
        metadata=dict(metadata or {}),
    )
