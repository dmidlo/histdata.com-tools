"""Pytest unit tests for histdatacom.fx_enums.py."""

from histdatacom.fx_enums import (
    PAIR_GROUPS,
    Pairs,
    TimePrecision,
    expand_pair_groups,
    expand_pair_selection,
    pair_group_names,
)


def test_fx_enums() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_time_precision_values_do_not_require_influxdb_client() -> None:
    """Influx precision metadata should stay importable without Influx."""
    assert TimePrecision.ASCII_M1.value == "s"
    assert TimePrecision.ASCII_T.value == "ms"
    assert TimePrecision.list_values() == {"s", "ms"}


def test_pair_groups_only_reference_supported_histdata_symbols() -> None:
    """Instrument groups should not drift from the public HistData enum."""
    supported_pairs = Pairs.list_keys()

    assert set(pair_group_names()) == set(PAIR_GROUPS)
    assert {
        pair
        for group_pairs in PAIR_GROUPS.values()
        for pair in group_pairs
        if pair not in supported_pairs
    } == set()


def test_pair_group_expansion_is_deterministic_and_supports_aliases() -> None:
    """Named groups should expand to sorted canonical pair keys."""
    assert expand_pair_groups(("major",)) == expand_pair_groups(("majors",))
    assert expand_pair_groups(("majors",)) == (
        "audusd",
        "eurusd",
        "gbpusd",
        "nzdusd",
        "usdcad",
        "usdchf",
        "usdjpy",
    )


def test_pair_group_selection_replaces_default_all_pair_selection() -> None:
    """Group-only requests should not accidentally preserve all pairs."""
    assert expand_pair_selection(Pairs.list_keys(), ("majors",)) == (
        "audusd",
        "eurusd",
        "gbpusd",
        "nzdusd",
        "usdcad",
        "usdchf",
        "usdjpy",
    )
    assert expand_pair_selection(("eurusd",), ("metals",)) == (
        "eurusd",
        "xagusd",
        "xauaud",
        "xauchf",
        "xaueur",
        "xaugbp",
        "xauusd",
    )
