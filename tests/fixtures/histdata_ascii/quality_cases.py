"""Reusable HistData ASCII fixture cases for data-quality rule tests."""

from __future__ import annotations

import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from histdatacom.histdata_ascii import TICK

FIXTURE_ROOT = Path(__file__).resolve().parent
CLEAN_TICK_FIXTURE = FIXTURE_ROOT / "DAT_ASCII_EURUSD_T_201202.csv"


@dataclass(frozen=True, slots=True)
class HistDataAsciiCase:
    """One clean or intentionally dirty HistData ASCII fixture case."""

    name: str
    timeframe: str
    filename: str
    rows: tuple[str, ...] = ()
    anomalies: tuple[str, ...] = ()
    missing: bool = False

    @property
    def text(self) -> str:
        """Return the newline-terminated CSV payload for this case."""
        if not self.rows:
            return ""
        return "\n".join(self.rows) + "\n"

    @property
    def clean(self) -> bool:
        """Return whether this case is intended to parse without findings."""
        return not self.anomalies and not self.missing


@dataclass(frozen=True, slots=True)
class EstNoDstTimestampCase:
    """One source timestamp with its fixed EST-no-DST UTC millisecond value."""

    timeframe: str
    raw: str
    expected_utc_ms: int
    description: str


CLEAN_TICK_ROWS = (
    "20120201 000003660,1.306600,1.306770,0",
    "20120201 000003973,1.306580,1.306750,25",
    "20120201 000014990,1.306570,1.306740,2147483647",
)

CLEAN_TICK_CASE = HistDataAsciiCase(
    name="clean_tick",
    timeframe=TICK,
    filename="DAT_ASCII_EURUSD_T_201202.csv",
    rows=CLEAN_TICK_ROWS,
)

DIRTY_TICK_CASES = (
    HistDataAsciiCase(
        name="tick_malformed_row",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_MALFORMED.csv",
        rows=(
            CLEAN_TICK_ROWS[0],
            "20120201 000003973,1.306580,1.306750",
        ),
        anomalies=("malformed_row",),
    ),
    HistDataAsciiCase(
        name="tick_bad_timestamp",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_BAD_TIMESTAMP.csv",
        rows=(
            CLEAN_TICK_ROWS[0],
            "20120230 000003973,1.306580,1.306750,25",
        ),
        anomalies=("bad_timestamp",),
    ),
    HistDataAsciiCase(
        name="tick_bad_numeric",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_BAD_NUMERIC.csv",
        rows=(
            CLEAN_TICK_ROWS[0],
            "20120201 000003973,inf,1.306750,25",
        ),
        anomalies=("bad_numeric",),
    ),
    HistDataAsciiCase(
        name="tick_shifted_column",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_SHIFTED.csv",
        rows=(
            CLEAN_TICK_ROWS[0],
            "1.306580,20120201 000003973,1.306750,25",
        ),
        anomalies=("shifted_column",),
    ),
    HistDataAsciiCase(
        name="tick_bad_volume",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_BAD_VOLUME.csv",
        rows=(
            CLEAN_TICK_ROWS[0],
            "20120201 000003973,1.306580,1.306750,25.5",
        ),
        anomalies=("bad_volume",),
    ),
    HistDataAsciiCase(
        name="tick_duplicate_row",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_DUPLICATE.csv",
        rows=(
            CLEAN_TICK_ROWS[0],
            CLEAN_TICK_ROWS[0],
            CLEAN_TICK_ROWS[2],
        ),
        anomalies=("duplicate_tick", "duplicate_timestamp"),
    ),
    HistDataAsciiCase(
        name="tick_non_monotonic_timestamp",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_NON_MONOTONIC.csv",
        rows=(
            CLEAN_TICK_ROWS[1],
            CLEAN_TICK_ROWS[0],
            CLEAN_TICK_ROWS[2],
        ),
        anomalies=("non_monotonic_timestamp",),
    ),
    HistDataAsciiCase(
        name="tick_negative_spread",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_NEGATIVE_SPREAD.csv",
        rows=(
            CLEAN_TICK_ROWS[0],
            "20120201 000003973,1.306800,1.306750,25",
        ),
        anomalies=("negative_spread",),
    ),
    HistDataAsciiCase(
        name="tick_header_row",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_HEADER.csv",
        rows=(
            "datetime,bid,ask,vol",
            CLEAN_TICK_ROWS[0],
        ),
        anomalies=("header_row", "bad_timestamp"),
    ),
    HistDataAsciiCase(
        name="tick_bad_delimiter",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_BAD_DELIMITER.csv",
        rows=("20120201 000003660;1.306600;1.306770;0",),
        anomalies=("bad_delimiter", "malformed_row"),
    ),
    HistDataAsciiCase(
        name="tick_empty_file",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_EMPTY.csv",
        anomalies=("empty_file",),
    ),
    HistDataAsciiCase(
        name="tick_missing_file",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_MISSING.csv",
        anomalies=("missing_file",),
        missing=True,
    ),
)

ALL_CLEAN_CASES = (CLEAN_TICK_CASE,)
ALL_DIRTY_CASES = DIRTY_TICK_CASES
ALL_ASCII_CASES = (*ALL_CLEAN_CASES, *ALL_DIRTY_CASES)

EST_NO_DST_CALENDAR_CASES = (
    EstNoDstTimestampCase(
        timeframe=TICK,
        raw="20120229 235959999",
        expected_utc_ms=1330577999999,
        description="leap-day tick with millisecond precision",
    ),
    EstNoDstTimestampCase(
        timeframe=TICK,
        raw="20170101 000000000",
        expected_utc_ms=1483246800000,
        description="year boundary tick",
    ),
)


def case_by_name(name: str) -> HistDataAsciiCase:
    """Return a fixture case by its stable case name."""
    for case in ALL_ASCII_CASES:
        if case.name == name:
            return case
    raise KeyError(name)


def write_ascii_case(directory: Path, case: HistDataAsciiCase) -> Path:
    """Write one fixture case as a CSV file and return its target path."""
    target = directory / case.filename
    target.parent.mkdir(parents=True, exist_ok=True)
    if case.missing:
        if target.exists():
            target.unlink()
        return target
    target.write_text(case.text, encoding="utf-8")
    return target


def copy_clean_fixture(directory: Path, timeframe: str) -> Path:
    """Copy the static clean fixture for a supported timeframe."""
    source = {TICK: CLEAN_TICK_FIXTURE}[timeframe]
    target = directory / source.name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return target


def write_zip_case(
    directory: Path,
    case: HistDataAsciiCase,
    *,
    zip_filename: str | None = None,
    extra_members: Iterable[tuple[str, str | bytes]] = (),
) -> Path:
    """Write a ZIP archive for one fixture case."""
    target = directory / (
        zip_filename or Path(case.filename).with_suffix(".zip").name
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w") as archive:
        if not case.missing:
            archive.writestr(case.filename, case.text)
        for member_name, content in extra_members:
            archive.writestr(member_name, content)
    return target


def write_corrupt_zip(
    directory: Path,
    filename: str = "DAT_ASCII_EURUSD_T_201202_CORRUPT.zip",
) -> Path:
    """Write bytes that should fail ZIP integrity checks."""
    target = directory / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"not a zip archive")
    return target
