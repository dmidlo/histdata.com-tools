"""Pytest unit tests for histdatacom.fx_enums.py."""

from histdatacom.fx_enums import (
    MAJOR_TRIANGLE_PAIR_GROUPS,
    MAJOR_TRIANGLE_RELATIONSHIPS,
    MAJOR_TRIANGLE_SYMBOLS,
    PAIR_GROUP_BASKETS,
    PAIR_GROUPS,
    Pairs,
    Timeframe,
    TimePrecision,
    expand_pair_groups,
    expand_pair_selection,
    normalize_pair_group,
    pair_group_basket_names,
    pair_group_names,
)


def test_fx_enums() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_time_precision_values_do_not_require_influxdb_client() -> None:
    """Influx precision metadata should stay importable without Influx."""
    assert TimePrecision.ASCII_T.value == "ms"
    assert TimePrecision.list_values() == {"ms"}


def test_ascii_m1_is_not_a_supported_raw_timeframe() -> None:
    """Tick is the only ASCII base timeframe accepted by the application."""
    assert "M1" not in Timeframe.list_keys()


def test_only_ascii_tick_raw_dimension_is_supported() -> None:
    """The raw substrate is constrained to ASCII tick only."""
    from histdatacom.fx_enums import Format

    assert Format.list_values() == {"ascii"}
    assert Format.list_keys() == {"ASCII"}
    assert Timeframe.list_values() == {"tick-data-quotes"}
    assert Timeframe.list_keys() == {"T"}


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
    assert expand_pair_groups(("major-triangles",)) == MAJOR_TRIANGLE_SYMBOLS
    assert expand_pair_groups(("major_triangles",)) == MAJOR_TRIANGLE_SYMBOLS
    assert expand_pair_groups(("majortriangles",)) == MAJOR_TRIANGLE_SYMBOLS
    assert expand_pair_groups(("major triangles",)) == MAJOR_TRIANGLE_SYMBOLS
    assert expand_pair_groups(("triangle",)) == MAJOR_TRIANGLE_SYMBOLS
    assert expand_pair_groups(("triangles",)) == MAJOR_TRIANGLE_SYMBOLS


def test_major_triangle_group_covers_complete_major_fx_triangle_set() -> None:
    """Major triangles should cover data-quality-oriented major FX triangles."""
    assert len(MAJOR_TRIANGLE_RELATIONSHIPS) == 56
    assert len(MAJOR_TRIANGLE_SYMBOLS) == 28
    assert PAIR_GROUPS["major-triangles"] == MAJOR_TRIANGLE_SYMBOLS
    assert ("eurgbp", "eurusd", "gbpusd") in MAJOR_TRIANGLE_RELATIONSHIPS
    assert ("eurusd", "eurjpy", "usdjpy") in MAJOR_TRIANGLE_RELATIONSHIPS
    assert ("cadchf", "cadjpy", "chfjpy") in MAJOR_TRIANGLE_RELATIONSHIPS
    assert ("audcad", "audchf", "cadchf") in MAJOR_TRIANGLE_RELATIONSHIPS


def test_individual_major_triangle_groups_are_user_selectable() -> None:
    """Each oriented major triangle should have a stable pair-group name."""
    assert len(MAJOR_TRIANGLE_PAIR_GROUPS) == len(MAJOR_TRIANGLE_RELATIONSHIPS)
    assert set(MAJOR_TRIANGLE_PAIR_GROUPS).issubset(PAIR_GROUPS)
    assert set(PAIR_GROUP_BASKETS).isdisjoint(MAJOR_TRIANGLE_PAIR_GROUPS)

    for relationship in MAJOR_TRIANGLE_RELATIONSHIPS:
        group = f"triangle-{'-'.join(relationship)}"
        assert MAJOR_TRIANGLE_PAIR_GROUPS[group] == relationship
        assert PAIR_GROUPS[group] == relationship
        assert expand_pair_groups((group,)) == tuple(sorted(relationship))

    group = "triangle-eurgbp-eurusd-gbpusd"
    assert normalize_pair_group("triangle eurgbp eurusd gbpusd") == group
    assert normalize_pair_group("triangle_eurgbp_eurusd_gbpusd") == group
    assert normalize_pair_group("triangleeurgbpeurusdgbpusd") == group


def test_pair_group_basket_names_exclude_individual_triangles() -> None:
    """Summary basket names should stay readable as triangle groups grow."""
    assert set(pair_group_basket_names()) == set(PAIR_GROUP_BASKETS)
    assert "major-triangles" in pair_group_basket_names()
    assert "triangle-eurgbp-eurusd-gbpusd" in pair_group_names()
    assert "triangle-eurgbp-eurusd-gbpusd" not in pair_group_basket_names()


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
