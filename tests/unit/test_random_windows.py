"""Deterministic random/session tick-window contract tests."""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from histdatacom.random_windows import (
    RANDOM_WINDOW_MODE_ALL_SESSIONS,
    RANDOM_WINDOW_SESSION_PROFILES,
    RandomWindowSelectionV1,
    RandomWindowSupportError,
    RandomWindowSyntaxError,
    filter_polars_frame_to_random_window,
    parse_random_window_expression,
    random_window_intervals_for_range,
    random_window_planning_yearmonths,
    random_window_requires_seed,
    random_window_selection_from_metadata,
    resolve_random_window_selection,
)

DOCUMENTED_EXPRESSIONS = (
    "1y",
    "1q",
    "1M",
    "2w",
    "2d",
    "6h",
    "90m",
    "ldn",
    "ldn-ny",
    "syd-syd",
    "hk-3d",
    "hk-3d-hk",
    "45m-auk",
    "1h-auk-1h",
    "30m-ldn-1w-syd-1h",
)


def _repo(**ranges: tuple[str, str]) -> dict[str, dict[str, str]]:
    return {
        pair: {"start": start, "end": end}
        for pair, (start, end) in ranges.items()
    }


def _ms(value: str) -> int:
    return int(
        datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()
        * 1000
    )


@pytest.mark.parametrize("expression", DOCUMENTED_EXPRESSIONS)
def test_parser_accepts_documented_expressions(expression: str) -> None:
    """Every issue-contract example should parse without normalization."""
    parsed = parse_random_window_expression(expression)

    assert parsed.expression == expression


@pytest.mark.parametrize(
    "expression",
    (
        "",
        " 1d",
        "0d",
        "-1d",
        "1D",
        "1d-2d",
        "1d-ldn",
        "ldn-1h-ny",
        "ldn-ny-1d",
        "ldn--ny",
        "ldn-ny-hk",
        "not-a-session",
    ),
)
def test_parser_rejects_ambiguous_or_unsupported_forms(
    expression: str,
) -> None:
    """Unsupported mixtures should fail closed at the parser boundary."""
    with pytest.raises(RandomWindowSyntaxError):
        parse_random_window_expression(expression)


def test_session_profiles_are_explicit_iana_sampling_windows() -> None:
    """Legacy session codes should expose reproducible clock semantics."""
    assert set(RANDOM_WINDOW_SESSION_PROFILES) == {
        "fra",
        "ldn",
        "ny",
        "chi",
        "la",
        "auk",
        "syd",
        "tyo",
        "hk",
    }
    for profile in RANDOM_WINDOW_SESSION_PROFILES.values():
        payload = profile.to_dict()
        assert payload["dst_policy"] == "iana_zone_rules"
        assert payload["semantics"] == "sampling_window_not_exchange_hours"


def test_seeded_selection_is_order_independent_and_round_trips() -> None:
    """Pair input order and Temporal serialization must not change selection."""
    repository = _repo(
        eurusd=("201001", "202412"),
        gbpusd=("201101", "202311"),
    )
    first = resolve_random_window_selection(
        "2d",
        seed=1729,
        pairs=("eurusd", "gbpusd"),
        repository_ranges=repository,
    )
    second = resolve_random_window_selection(
        "2d",
        seed=1729,
        pairs=("gbpusd", "eurusd"),
        repository_ranges=repository,
    )

    assert first == second
    assert RandomWindowSelectionV1.from_dict(first.to_dict()) == first
    assert first.support_start_utc_ms == _ms("2011-01-01T00:00:00")
    assert first.support_end_utc_ms == _ms("2023-12-01T00:00:00")


def test_multi_symbol_selection_refuses_empty_common_support() -> None:
    """Disjoint instrument inventories must not silently select one symbol."""
    with pytest.raises(RandomWindowSupportError, match="no common"):
        resolve_random_window_selection(
            "1d",
            seed=4,
            pairs=("eurusd", "gbpusd"),
            repository_ranges=_repo(
                eurusd=("201001", "201012"),
                gbpusd=("201101", "201112"),
            ),
        )


def test_multi_symbol_selection_refuses_missing_inventory_pair() -> None:
    """Present inventory must cover every requested instrument, even with bounds."""
    with pytest.raises(RandomWindowSupportError, match="gbpusd"):
        resolve_random_window_selection(
            "1d",
            seed=4,
            pairs=("eurusd", "gbpusd"),
            repository_ranges=_repo(eurusd=("201001", "202412")),
            start_yearmonth="202001",
            end_yearmonth="202012",
        )


@pytest.mark.parametrize(
    ("expression", "month_multiple"),
    (("1M", 1), ("1q", 3), ("1y", 12)),
)
def test_calendar_durations_are_calendar_aligned(
    expression: str,
    month_multiple: int,
) -> None:
    """Month/quarter/year selections should start at UTC calendar boundaries."""
    selection = resolve_random_window_selection(
        expression,
        seed=11,
        pairs=("eurusd",),
        repository_ranges=_repo(eurusd=("201001", "202412")),
    )
    start = datetime.fromtimestamp(
        selection.selected_start_utc_ms / 1000,  # type: ignore[operator]
        tz=timezone.utc,
    )
    end = datetime.fromtimestamp(
        selection.selected_end_utc_ms / 1000,  # type: ignore[operator]
        tz=timezone.utc,
    )

    assert (start.day, start.hour, start.minute) == (1, 0, 0)
    assert (end.day, end.hour, end.minute) == (1, 0, 0)
    assert (end.year * 12 + end.month) - (start.year * 12 + start.month) == (
        month_multiple
    )


def test_session_window_obeys_london_dst_and_padding() -> None:
    """IANA rules should shift UTC boundaries while preserving local clocks."""
    winter = resolve_random_window_selection(
        "1h-ldn-1h",
        seed=1,
        pairs=("eurusd",),
        repository_ranges=_repo(eurusd=("202401", "202401")),
    )
    summer = resolve_random_window_selection(
        "1h-ldn-1h",
        seed=1,
        pairs=("eurusd",),
        repository_ranges=_repo(eurusd=("202407", "202407")),
    )
    winter_start = datetime.fromtimestamp(
        winter.selected_start_utc_ms / 1000,  # type: ignore[operator]
        tz=timezone.utc,
    )
    summer_start = datetime.fromtimestamp(
        summer.selected_start_utc_ms / 1000,  # type: ignore[operator]
        tz=timezone.utc,
    )

    assert winter_start.hour == 7
    assert summer_start.hour == 6
    assert winter.selected_end_utc_ms - winter.selected_start_utc_ms == 11 * 3_600_000  # type: ignore[operator]


def test_ordered_and_same_session_windows_wrap_as_documented() -> None:
    """Ordered spans use the end close; same sessions advance one local day."""
    repository = _repo(eurusd=("202401", "202401"))
    ordered = resolve_random_window_selection(
        "ldn-ny",
        seed=3,
        pairs=("eurusd",),
        repository_ranges=repository,
    )
    same = resolve_random_window_selection(
        "syd-syd",
        seed=3,
        pairs=("eurusd",),
        repository_ranges=repository,
    )

    assert ordered.selected_end_utc_ms - ordered.selected_start_utc_ms == 14 * 3_600_000  # type: ignore[operator]
    assert same.selected_end_utc_ms - same.selected_start_utc_ms == 33 * 3_600_000  # type: ignore[operator]


def test_bounded_session_expression_selects_all_occurrences_without_seed() -> (
    None
):
    """Both user bounds switch session expressions to compact all-session mode."""
    assert not random_window_requires_seed(
        "ldn",
        start_yearmonth="202401",
        end_yearmonth="202401",
    )
    selection = resolve_random_window_selection(
        "ldn",
        seed=None,
        pairs=("eurusd",),
        repository_ranges=_repo(eurusd=("202001", "202412")),
        start_yearmonth="202401",
        end_yearmonth="202401",
    )

    assert selection.mode == RANDOM_WINDOW_MODE_ALL_SESSIONS
    assert selection.occurrence_count == 23
    assert selection.selected_start_utc_ms is None
    assert random_window_planning_yearmonths(selection) == ("202401", "202401")


def test_exact_half_open_filtering_handles_interval_union() -> None:
    """Projection should retain exact session rows and exclude end boundaries."""
    selection = resolve_random_window_selection(
        "ldn",
        seed=None,
        pairs=("eurusd",),
        repository_ranges=_repo(eurusd=("202401", "202401")),
        start_yearmonth="202401",
        end_yearmonth="202401",
    )
    timestamps = [
        _ms("2024-01-02T07:59:00"),
        _ms("2024-01-02T08:00:00"),
        _ms("2024-01-02T16:59:00"),
        _ms("2024-01-02T17:00:00"),
        _ms("2024-01-06T10:00:00"),
    ]
    frame = pl.DataFrame({"datetime": timestamps, "bid": range(5)})

    filtered = filter_polars_frame_to_random_window(frame, selection)

    assert filtered["datetime"].to_list() == timestamps[1:3]
    intervals = random_window_intervals_for_range(
        selection,
        range_start_utc_ms=timestamps[0],
        range_end_utc_ms=timestamps[-1] + 1,
    )
    assert intervals


def test_duration_larger_than_support_fails_boundedly() -> None:
    """An impossible requested duration should not substitute another window."""
    with pytest.raises(RandomWindowSupportError, match="exceeds common"):
        resolve_random_window_selection(
            "40d",
            seed=1,
            pairs=("eurusd",),
            repository_ranges=_repo(eurusd=("202401", "202401")),
            start_yearmonth="202401",
            end_yearmonth="202401",
        )


def test_malformed_persisted_selection_fails_closed() -> None:
    """Corrupt selection metadata must never be interpreted as full-cache mode."""
    with pytest.raises(ValueError, match="must be a mapping"):
        random_window_selection_from_metadata(
            {"random_window_selection": "not-a-contract"}
        )
