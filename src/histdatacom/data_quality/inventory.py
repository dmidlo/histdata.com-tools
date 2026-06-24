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
from histdatacom.data_quality.format_support import (
    HISTDATA_FORMAT_SUPPORT_RULE_ID,
    format_code_for_data_format,
    payload_extension_for_format,
    quality_support_for_target,
)
from histdatacom.runtime_contracts import JSONValue

HISTDATA_ZIP_FILENAME_PATTERN = (
    "DAT_<FORMAT>_<SYMBOL>_<TIMEFRAME>_<YYYY[MM]>.zip or "
    "HISTDATA_COM_<FORMAT>_<SYMBOL>_<TIMEFRAME><YYYY[MM]>.zip"
)
HISTDATA_MEMBER_PATTERN = (
    "DAT_<FORMAT>_<SYMBOL>_<TIMEFRAME>_<YYYY[MM]>.<csv|xlsx>"
)
ZIP_INVENTORY_RULE_ID = "inventory.zip.integrity"


@dataclass(slots=True)
class HistDataFormatSupportRule:
    """Report the parser-support boundary for each discovered target."""

    rule_id: str = HISTDATA_FORMAT_SUPPORT_RULE_ID
    description: str = (
        "Report deep-parser, inventory-only, or unsupported format coverage."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return format-support boundary findings for one target."""
        support = quality_support_for_target(
            data_format=target.data_format,
            timeframe=target.timeframe,
            kind=target.kind.value,
        )
        if support.status == "inventory-only":
            return (
                _finding(
                    target,
                    code="HISTDATA_FORMAT_INVENTORY_ONLY",
                    message=(
                        "HistData format is recognized, but parser-level "
                        "quality checks are not implemented for this "
                        "format/timeframe."
                    ),
                    rule_id=self.rule_id,
                    severity=QualitySeverity.WARNING,
                    metadata={
                        "quality_support": support.to_metadata(),
                        "boundary": "inventory-only",
                    },
                ),
            )

        if support.status != "unsupported":
            return ()

        if target.kind is QualityTargetKind.ZIP and not target.data_format:
            return ()

        return (
            _finding(
                target,
                code="HISTDATA_FORMAT_UNSUPPORTED",
                message=(
                    "HistData target format metadata is unsupported for "
                    "data-quality checks."
                ),
                rule_id=self.rule_id,
                metadata={
                    "quality_support": support.to_metadata(),
                    "boundary": "unsupported",
                },
            ),
        )


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
        expected_filenames = _expected_zip_filenames(target)
        expected_member = _expected_zip_member(target)
        if not expected_filenames:
            findings.append(
                _finding(
                    target,
                    code="HISTDATA_ZIP_FILENAME_INVALID",
                    message="ZIP filename does not match expected HistData pattern.",
                    rule_id=self.rule_id,
                    metadata={
                        "expected_pattern": HISTDATA_ZIP_FILENAME_PATTERN,
                        "observed_filename": path.name,
                    },
                )
            )
        elif path.name not in expected_filenames:
            findings.append(
                _finding(
                    target,
                    code="HISTDATA_ZIP_FILENAME_INVALID",
                    message="ZIP filename does not match expected HistData metadata.",
                    rule_id=self.rule_id,
                    metadata={
                        "expected_pattern": HISTDATA_ZIP_FILENAME_PATTERN,
                        "expected_filename": expected_filenames[0],
                        "accepted_filenames": list(expected_filenames),
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
                            rule_id=self.rule_id,
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
                    rule_id=self.rule_id,
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
                    rule_id=self.rule_id,
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
    support_rule: QualityRule = HistDataFormatSupportRule()
    zip_rule: QualityRule = HistDataZipInventoryRule()
    return (support_rule, zip_rule)


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
                            "HistData pattern."
                        ),
                        rule_id=ZIP_INVENTORY_RULE_ID,
                        metadata={
                            "expected_pattern": HISTDATA_MEMBER_PATTERN,
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
                message="ZIP archive does not contain the expected data member.",
                rule_id=ZIP_INVENTORY_RULE_ID,
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
                message="ZIP archive does not contain the expected data member.",
                rule_id=ZIP_INVENTORY_RULE_ID,
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

    expected_sidecar = str(Path(expected_member).with_suffix(".txt"))
    extra_members = tuple(
        member
        for member in members
        if member not in {expected_member, expected_sidecar}
    )
    if extra_members:
        findings.append(
            _finding(
                target,
                code="ZIP_EXTRA_MEMBER",
                message="ZIP archive contains unexpected extra members.",
                rule_id=ZIP_INVENTORY_RULE_ID,
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
        not target.data_format
        or not target.symbol
        or not target.timeframe
        or not target.period
    ):
        return None
    format_code = format_code_for_data_format(target.data_format)
    if not format_code:
        return None
    extension = payload_extension_for_format(target.data_format)
    return (
        f"DAT_{format_code}_{target.symbol}_{target.timeframe}_"
        f"{target.period}.{extension}"
    )


def _expected_zip_filenames(target: QualityTarget) -> tuple[str, ...]:
    if (
        not target.data_format
        or not target.symbol
        or not target.timeframe
        or not target.period
    ):
        return ()
    format_code = format_code_for_data_format(target.data_format)
    if not format_code:
        return ()
    return (
        f"DAT_{format_code}_{target.symbol}_{target.timeframe}_{target.period}.zip",
        f"HISTDATA_COM_{format_code}_{target.symbol}_{target.timeframe}{target.period}.zip",
    )


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
    rule_id: str = ZIP_INVENTORY_RULE_ID,
    severity: QualitySeverity = QualitySeverity.ERROR,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=QualityLocation(path=target.path),
        metadata=dict(metadata or {}),
    )
