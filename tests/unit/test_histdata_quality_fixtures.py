"""Coverage for reusable HistData ASCII data-quality fixture cases."""

from __future__ import annotations

import csv
import math
import zipfile
from collections import Counter
from io import StringIO
from pathlib import Path

import pytest

from histdatacom.histdata_ascii import (
    M1,
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
    polars_datetime_to_utc_ms_expr,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    ALL_ASCII_CASES,
    ALL_CLEAN_CASES,
    ALL_DIRTY_CASES,
    CLEAN_M1_CASE,
    CLEAN_M1_FIXTURE,
    CLEAN_TICK_CASE,
    CLEAN_TICK_FIXTURE,
    DIRTY_M1_CASES,
    DIRTY_TICK_CASES,
    EST_NO_DST_CALENDAR_CASES,
    HistDataAsciiCase,
    case_by_name,
    copy_clean_fixture,
    write_ascii_case,
    write_corrupt_zip,
    write_zip_case,
)


def test_quality_case_index_covers_clean_and_dirty_m1_and_tick_cases() -> None:
    """The fixture suite should expose clean and dirty cases for both layouts."""
    assert ALL_CLEAN_CASES == (CLEAN_M1_CASE, CLEAN_TICK_CASE)
    assert {case.timeframe for case in ALL_CLEAN_CASES} == {M1, TICK}
    assert {case.timeframe for case in DIRTY_M1_CASES} == {M1}
    assert {case.timeframe for case in DIRTY_TICK_CASES} == {TICK}
    assert len(ALL_ASCII_CASES) == len(ALL_CLEAN_CASES) + len(ALL_DIRTY_CASES)

    m1_anomalies = _anomaly_names(DIRTY_M1_CASES)
    tick_anomalies = _anomaly_names(DIRTY_TICK_CASES)
    assert {
        "malformed_row",
        "bad_timestamp",
        "bad_numeric",
        "shifted_column",
        "bad_volume",
        "duplicate_timestamp",
        "non_monotonic_timestamp",
        "ohlc_violation",
        "header_row",
        "bad_delimiter",
        "empty_file",
        "missing_file",
    } <= m1_anomalies
    assert {
        "malformed_row",
        "bad_timestamp",
        "bad_numeric",
        "shifted_column",
        "bad_volume",
        "duplicate_tick",
        "duplicate_timestamp",
        "non_monotonic_timestamp",
        "negative_spread",
        "header_row",
        "bad_delimiter",
        "empty_file",
        "missing_file",
    } <= tick_anomalies


@pytest.mark.parametrize(
    ("case", "source"),
    (
        (CLEAN_M1_CASE, CLEAN_M1_FIXTURE),
        (CLEAN_TICK_CASE, CLEAN_TICK_FIXTURE),
    ),
    ids=lambda item: item.name if isinstance(item, HistDataAsciiCase) else None,
)
def test_clean_quality_cases_match_static_histdata_ascii_fixtures(
    case: HistDataAsciiCase,
    source: Path,
) -> None:
    """The reusable clean cases should not drift from the static fixtures."""
    assert case.text == source.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "case",
    ALL_ASCII_CASES,
    ids=lambda case: case.name,
)
def test_ascii_quality_case_builder_writes_expected_targets(
    tmp_path: Path,
    case: HistDataAsciiCase,
) -> None:
    """Fixture builders should support clean, dirty, empty, and missing files."""
    path = write_ascii_case(tmp_path, case)

    assert path == tmp_path / case.filename
    if case.missing:
        assert not path.exists()
        return

    assert path.read_text(encoding="utf-8") == case.text
    if "empty_file" in case.anomalies:
        assert path.stat().st_size == 0


@pytest.mark.parametrize(
    "timeframe",
    (M1, TICK),
)
def test_clean_fixture_copy_builder_reuses_static_fixture_files(
    tmp_path: Path,
    timeframe: str,
) -> None:
    """Quality tests can copy the existing clean CSV fixtures into temp dirs."""
    copied = copy_clean_fixture(tmp_path, timeframe)
    source = CLEAN_M1_FIXTURE if timeframe == M1 else CLEAN_TICK_FIXTURE

    assert copied.name == source.name
    assert copied.read_text(encoding="utf-8") == source.read_text(
        encoding="utf-8"
    )


def test_zip_quality_case_builder_writes_valid_single_member_archive(
    tmp_path: Path,
) -> None:
    """Future ZIP checks can start from a valid HistData archive fixture."""
    archive_path = write_zip_case(tmp_path, CLEAN_M1_CASE)

    with zipfile.ZipFile(archive_path, "r") as archive:
        assert archive.namelist() == [CLEAN_M1_CASE.filename]
        assert (
            archive.read(CLEAN_M1_CASE.filename).decode("utf-8")
            == CLEAN_M1_CASE.text
        )


def test_zip_quality_builders_cover_corrupt_and_extra_member_archives(
    tmp_path: Path,
) -> None:
    """Future archive checks need corrupt and ambiguous archive inputs."""
    corrupt_path = write_corrupt_zip(tmp_path)
    with pytest.raises(zipfile.BadZipFile):
        with zipfile.ZipFile(corrupt_path, "r") as archive:
            archive.testzip()

    extra_path = write_zip_case(
        tmp_path,
        CLEAN_TICK_CASE,
        extra_members=(("README.txt", "unexpected metadata"),),
    )
    with zipfile.ZipFile(extra_path, "r") as archive:
        assert set(archive.namelist()) == {
            CLEAN_TICK_CASE.filename,
            "README.txt",
        }


@pytest.mark.parametrize(
    "case",
    ALL_DIRTY_CASES,
    ids=lambda case: case.name,
)
def test_dirty_ascii_cases_contain_declared_anomalies(
    tmp_path: Path,
    case: HistDataAsciiCase,
) -> None:
    """Dirty fixture labels should correspond to observable raw-data defects."""
    path = write_ascii_case(tmp_path, case)

    for anomaly in case.anomalies:
        assert _has_declared_anomaly(case, path, anomaly), (
            case.name,
            anomaly,
        )


@pytest.mark.parametrize(
    "case_name",
    ("clean_m1", "m1_ohlc_violation", "tick_negative_spread"),
)
def test_quality_cases_can_be_looked_up_by_stable_case_name(
    case_name: str,
) -> None:
    """Future rule tests can select focused fixture cases by stable name."""
    assert case_by_name(case_name).name == case_name


def test_unknown_quality_case_name_fails_clearly() -> None:
    """Misspelled fixture names should fail at test setup time."""
    with pytest.raises(KeyError, match="missing_case"):
        case_by_name("missing_case")


@pytest.mark.parametrize(
    "case",
    EST_NO_DST_CALENDAR_CASES,
    ids=lambda case: f"{case.timeframe}-{case.description}",
)
def test_est_no_dst_calendar_fixture_cases_use_fixed_utc_offset(
    case,
) -> None:
    """Shared calendar fixtures should preserve HistData EST-no-DST semantics."""
    assert (
        parse_histdata_datetime_to_utc_ms(case.raw, case.timeframe)
        == case.expected_utc_ms
    )


@pytest.mark.parametrize(
    "timeframe",
    (M1, TICK),
)
def test_est_no_dst_calendar_fixture_cases_match_polars_expression(
    timeframe: str,
) -> None:
    """Vectorized timestamp checks should consume the same calendar fixtures."""
    import polars as pl

    cases = [
        case
        for case in EST_NO_DST_CALENDAR_CASES
        if case.timeframe == timeframe
    ]
    frame = pl.DataFrame({"datetime": [case.raw for case in cases]})

    assert frame.select(
        polars_datetime_to_utc_ms_expr(timeframe)
    ).to_series().to_list() == [case.expected_utc_ms for case in cases]


def _anomaly_names(
    cases: tuple[HistDataAsciiCase, ...],
) -> set[str]:
    return {anomaly for case in cases for anomaly in case.anomalies}


def _rows(case: HistDataAsciiCase) -> list[list[str]]:
    return list(
        csv.reader(
            StringIO(case.text),
            delimiter=delimiter_for_timeframe(case.timeframe),
        )
    )


def _has_declared_anomaly(
    case: HistDataAsciiCase,
    path: Path,
    anomaly: str,
) -> bool:
    match anomaly:
        case "malformed_row":
            return _has_malformed_row(case)
        case "bad_timestamp":
            return _has_bad_timestamp(case)
        case "bad_numeric":
            return _has_bad_numeric(case)
        case "shifted_column":
            return _has_shifted_column(case)
        case "bad_volume":
            return _has_bad_volume(case)
        case "duplicate_timestamp":
            return _has_duplicate_timestamp(case)
        case "duplicate_tick":
            return _has_duplicate_tick(case)
        case "non_monotonic_timestamp":
            return _has_non_monotonic_timestamp(case)
        case "ohlc_violation":
            return _has_m1_ohlc_violation(case)
        case "negative_spread":
            return _has_negative_spread(case)
        case "header_row":
            return _has_header_row(case)
        case "bad_delimiter":
            return _has_bad_delimiter(case)
        case "empty_file":
            return path.exists() and path.stat().st_size == 0
        case "missing_file":
            return not path.exists()
        case _:
            raise AssertionError(f"unknown anomaly marker: {anomaly}")


def _has_malformed_row(case: HistDataAsciiCase) -> bool:
    expected = len(columns_for_timeframe(case.timeframe))
    return any(len(row) != expected for row in _rows(case))


def _has_bad_timestamp(case: HistDataAsciiCase) -> bool:
    expected = len(columns_for_timeframe(case.timeframe))
    for row in _rows(case):
        if len(row) != expected:
            continue
        try:
            parse_histdata_datetime_to_utc_ms(row[0], case.timeframe)
        except ValueError:
            return True
    return False


def _has_bad_numeric(case: HistDataAsciiCase) -> bool:
    expected = len(columns_for_timeframe(case.timeframe))
    for row in _rows(case):
        if len(row) != expected:
            continue
        for raw_value in row[1:-1]:
            try:
                parsed = float(raw_value)
            except ValueError:
                return True
            if not math.isfinite(parsed):
                return True
    return False


def _has_shifted_column(case: HistDataAsciiCase) -> bool:
    expected = len(columns_for_timeframe(case.timeframe))
    for row in _rows(case):
        if len(row) != expected:
            continue
        try:
            parse_histdata_datetime_to_utc_ms(row[0], case.timeframe)
        except ValueError:
            return any(
                _is_valid_source_timestamp(value, case.timeframe)
                for value in row[1:]
            )
    return False


def _has_bad_volume(case: HistDataAsciiCase) -> bool:
    expected = len(columns_for_timeframe(case.timeframe))
    for row in _rows(case):
        if len(row) != expected:
            continue
        raw_value = row[-1].strip()
        if not raw_value.lstrip("+-").isdigit():
            return True
        parsed = int(raw_value)
        if parsed < 0 or parsed > 2**31 - 1:
            return True
    return False


def _is_valid_source_timestamp(value: str, timeframe: str) -> bool:
    try:
        parse_histdata_datetime_to_utc_ms(value, timeframe)
    except ValueError:
        return False
    return True


def _valid_timestamp_values(case: HistDataAsciiCase) -> list[int]:
    expected = len(columns_for_timeframe(case.timeframe))
    values: list[int] = []
    for row in _rows(case):
        if len(row) != expected:
            continue
        try:
            values.append(
                parse_histdata_datetime_to_utc_ms(row[0], case.timeframe)
            )
        except ValueError:
            continue
    return values


def _raw_valid_timestamp_values(case: HistDataAsciiCase) -> list[str]:
    expected = len(columns_for_timeframe(case.timeframe))
    return [row[0] for row in _rows(case) if len(row) == expected]


def _has_duplicate_timestamp(case: HistDataAsciiCase) -> bool:
    counts = Counter(_raw_valid_timestamp_values(case))
    return any(count > 1 for count in counts.values())


def _has_duplicate_tick(case: HistDataAsciiCase) -> bool:
    rows = [tuple(row) for row in _rows(case)]
    counts = Counter(rows)
    return any(count > 1 for count in counts.values())


def _has_non_monotonic_timestamp(case: HistDataAsciiCase) -> bool:
    values = _valid_timestamp_values(case)
    return any(
        current < previous for previous, current in zip(values, values[1:])
    )


def _has_m1_ohlc_violation(case: HistDataAsciiCase) -> bool:
    if case.timeframe != M1:
        return False
    for row in _rows(case):
        if len(row) != 6:
            continue
        try:
            open_bid, high_bid, low_bid, close_bid = map(float, row[1:5])
        except ValueError:
            continue
        if (
            high_bid < max(open_bid, close_bid)
            or low_bid > min(open_bid, close_bid)
            or high_bid < low_bid
        ):
            return True
    return False


def _has_negative_spread(case: HistDataAsciiCase) -> bool:
    if case.timeframe != TICK:
        return False
    for row in _rows(case):
        if len(row) != 4:
            continue
        try:
            bid, ask = float(row[1]), float(row[2])
        except ValueError:
            continue
        if ask < bid:
            return True
    return False


def _has_header_row(case: HistDataAsciiCase) -> bool:
    rows = _rows(case)
    return bool(rows and rows[0] == list(columns_for_timeframe(case.timeframe)))


def _has_bad_delimiter(case: HistDataAsciiCase) -> bool:
    expected_delimiter = delimiter_for_timeframe(case.timeframe)
    wrong_delimiter = "," if expected_delimiter == ";" else ";"
    return any(
        len(row) == 1 and wrong_delimiter in row[0] for row in _rows(case)
    )
