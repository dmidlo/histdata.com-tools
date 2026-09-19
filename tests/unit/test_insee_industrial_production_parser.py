"""Independent regressions for INSEE release-table interpretation."""

from __future__ import annotations

from html import escape
from itertools import permutations

import pytest

from histdatacom.market_context.insee_industrial_production_archive import (
    InseeIndustrialProductionAggregate,
    _parse_current_series,
    _parse_html,
    _release_values,
    _subtitle_period,
    build_insee_industrial_production_current_series_requests,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    load_packaged_official_source_registry,
)


def _table(headers: tuple[str, ...], rows: tuple[tuple[str, ...], ...]) -> str:
    header_html = "".join(f"<th>{escape(cell)}</th>" for cell in headers)
    row_html = "".join(
        "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return (
        '<table id="production"><caption>SA-WDA monthly changes</caption>'
        f"<tr>{header_html}</tr>{row_html}</table>"
    )


@pytest.mark.parametrize("order", tuple(permutations(range(3))))
def test_monthly_change_selection_follows_headers_when_columns_move(
    order: tuple[int, ...],
) -> None:
    headers = ("June / May", "July / June", "July / July")
    industry = ("7.1", "-0.4", "8.2")
    manufacturing = ("9.3", "-0.8", "10.4")
    html = _table(
        ("Aggregate", *(headers[index] for index in order)),
        (
            ("BE: Industry", *(industry[index] for index in order)),
            ("CZ: Manufacturing", *(manufacturing[index] for index in order)),
        ),
    )

    values = _release_values(_parse_html(html.encode()), "2026-07", 9_050_846)

    assert [(value.aggregate, value.value) for value in values] == [
        (InseeIndustrialProductionAggregate.INDUSTRY, -0.4),
        (InseeIndustrialProductionAggregate.MANUFACTURING, -0.8),
    ]
    expected_column = order.index(1) + 1
    assert [value.locator for value in values] == [
        f"table:0:production:row:1:column:{expected_column}",
        f"table:0:production:row:2:column:{expected_column}",
    ]


def test_legacy_header_without_aggregate_cell_keeps_data_columns_aligned() -> (
    None
):
    html = _table(
        ("July / June", "June / May"),
        (
            ("BE: Industry", "0.2", "14.0"),
            ("CZ: Manufacturing", "0.7", "15.0"),
        ),
    )

    values = _release_values(_parse_html(html.encode()), "2012-07", 1_567_000)

    assert [value.value for value in values] == [0.2, 0.7]
    assert [value.locator for value in values] == [
        "table:0:production:row:1:column:1",
        "table:0:production:row:2:column:1",
    ]


def test_same_month_names_do_not_select_a_prior_year_comparison() -> None:
    html = _table(
        ("Aggregate", "March 2021 / Feb. 2020", "March 2021 / Feb. 2021"),
        (
            ("BE: Industry", "27.1", "0.8"),
            ("CZ: Manufacturing", "31.4", "0.4"),
        ),
    )

    values = _release_values(_parse_html(html.encode()), "2021-03", 5_340_000)

    assert [value.value for value in values] == [0.8, 0.4]
    assert all(value.locator.endswith(":column:2") for value in values)


def test_january_change_uses_december_of_the_previous_year() -> None:
    html = _table(
        (
            "Aggregate",
            "January 2021 / December 2021",
            "January 2021 / December 2020",
        ),
        (
            ("BE: Industry", "26.1", "1.8"),
            ("CZ: Manufacturing", "29.4", "1.4"),
        ),
    )

    values = _release_values(_parse_html(html.encode()), "2021-01", 5_210_000)

    assert [value.value for value in values] == [1.8, 1.4]


def test_comma_decimals_and_unicode_minus_preserve_source_lexical_values() -> (
    None
):
    html = _table(
        ("Aggregate", "April / March"),
        (
            ("BE: Industry", "− 0,5 %"),
            ("CZ: Manufacturing", "+0,4"),
        ),
    )

    values = _release_values(_parse_html(html.encode()), "2019-04", 4_010_000)

    assert [(value.lexical_value, value.value) for value in values] == [
        ("− 0,5 %", -0.5),
        ("+0,4", 0.4),
    ]


def test_non_headline_aggregates_are_ignored_even_with_unparseable_values() -> (
    None
):
    html = _table(
        ("Aggregate", "April / March"),
        (
            ("C1: Manufacturing of food products", "unavailable"),
            ("DE: Mining and utilities", "n/a"),
            ("BE: Industry", "-0.5"),
            ("CZ: Manufacturing", "0.4"),
            ("C: Manufacturing", "malformed"),
            ("BE: Manufacturing", "malformed"),
            ("CZ: Industry", "malformed"),
        ),
    )

    values = _release_values(_parse_html(html.encode()), "2019-04", 4_010_000)

    assert [value.value for value in values] == [-0.5, 0.4]
    assert [value.locator for value in values] == [
        "table:0:production:row:3:column:1",
        "table:0:production:row:4:column:1",
    ]


@pytest.mark.parametrize("missing_label", ("BE: Industry", "CZ: Manufacturing"))
def test_subsector_rows_cannot_replace_a_missing_headline_aggregate(
    missing_label: str,
) -> None:
    rows = (
        ("BE: Industry", "-0.5"),
        ("CZ: Manufacturing", "0.4"),
        ("C: Manufacturing", "0.4"),
        ("DE: Industry", "-0.5"),
    )
    html = _table(
        ("Aggregate", "April / March"),
        tuple(row for row in rows if row[0] != missing_label),
    )

    with pytest.raises(ValueError, match="omits its BE/CZ monthly changes"):
        _release_values(_parse_html(html.encode()), "2019-04", 4_010_000)


@pytest.mark.parametrize(
    "lexical", ("n/a", "0.4 revised", "1,2,3", "NaN", "201")
)
def test_malformed_selected_numeric_value_is_rejected(lexical: str) -> None:
    html = _table(
        ("Aggregate", "April / March"),
        (("BE: Industry", lexical), ("CZ: Manufacturing", "0.4")),
    )

    with pytest.raises(ValueError, match="is not numeric|outside its bound"):
        _release_values(_parse_html(html.encode()), "2019-04", 4_010_000)


def test_known_misdated_subtitle_correction_is_document_specific() -> None:
    subtitle = "Industrial production index - December 2011"

    assert _subtitle_period(subtitle, 1_562_488) == "2010-12"
    assert _subtitle_period(subtitle, 1_570_000) == "2011-12"


@pytest.mark.parametrize(
    "subtitle",
    (
        "Industrial production index - December 2010",
        "Industrial production index - January 2011",
        "Consumer confidence - December 2011",
        "",
    ),
)
def test_known_document_id_does_not_override_unexpected_subtitles(
    subtitle: str,
) -> None:
    with pytest.raises(ValueError):
        _subtitle_period(subtitle, 1_562_488)


def test_known_april_2018_header_correction_selects_documented_column() -> None:
    html = _table(
        ("Aggregate", "April / April", "April / Feb."),
        (
            ("BE: Industry", "-0.5", "19.0"),
            ("CZ: Manufacturing", "0.4", "20.0"),
        ),
    )

    values = _release_values(_parse_html(html.encode()), "2018-04", 3_560_105)

    assert [value.value for value in values] == [-0.5, 0.4]
    assert all(value.locator.endswith(":column:1") for value in values)
    with pytest.raises(ValueError, match="omits its BE/CZ monthly changes"):
        _release_values(_parse_html(html.encode()), "2018-04", 3_560_106)


@pytest.mark.parametrize(
    ("period", "headers", "prefix"),
    (
        ("2019-04", ("Aggregate", "April / April", "April / Feb."), ""),
        ("2018-04", ("Aggregate", "April / Feb.", "April / April"), ""),
        ("2018-04", ("Aggregate", "April / April", "April / Jan."), ""),
        ("2018-04", ("Aggregate", "unrelated", "April / Feb."), ""),
        (
            "2018-04",
            ("Aggregate", "April / April", "April / Feb."),
            "<table><tr><th>Unrelated preceding table</th></tr></table>",
        ),
    ),
)
def test_known_document_id_does_not_override_unexpected_table_layout(
    period: str,
    headers: tuple[str, ...],
    prefix: str,
) -> None:
    html = prefix + _table(
        headers,
        (
            ("BE: Industry", "-0.5", "19.0"),
            ("CZ: Manufacturing", "0.4", "20.0"),
        ),
    )

    with pytest.raises(ValueError):
        _release_values(_parse_html(html.encode()), period, 3_560_105)


@pytest.mark.parametrize(
    ("period", "header"),
    (
        ("2021-03", "March 2021 / February"),
        ("2021-03", "March / February 2021"),
        ("2021-01", "January 2021 / December"),
        ("2021-01", "January / December 2020"),
    ),
)
def test_single_header_year_is_bound_to_the_named_month(
    period: str, header: str
) -> None:
    html = _table(
        ("Aggregate", header),
        (("BE: Industry", "0.8"), ("CZ: Manufacturing", "0.4")),
    )

    values = _release_values(_parse_html(html.encode()), period, 5_340_000)

    assert [value.value for value in values] == [0.8, 0.4]


@pytest.mark.parametrize(
    ("period", "header"),
    (
        ("2021-03", "March 2020 / February"),
        ("2021-03", "March / February 2020"),
        ("2021-01", "January 2020 / December"),
        ("2021-01", "January / December 2021"),
        ("2021-01", "January 2021 December"),
        ("2021-01", "January 2021 / December 2020 / 2019"),
        ("2021-03", "March 2021 / February 2021 / 2020"),
    ),
)
def test_inconsistent_or_ambiguous_header_years_are_rejected(
    period: str, header: str
) -> None:
    html = _table(
        ("Aggregate", header),
        (("BE: Industry", "27.1"), ("CZ: Manufacturing", "31.4")),
    )

    with pytest.raises(ValueError, match="omits its BE/CZ monthly changes"):
        _release_values(_parse_html(html.encode()), period, 5_340_000)


def _current_series_snapshot(unit_mult: str | None) -> OfficialRawSnapshotV1:
    request = build_insee_industrial_production_current_series_requests(
        load_packaged_official_source_registry()
    )[0]
    multiplier = (
        "" if unit_mult is None else f' UNIT_MULT="{escape(unit_mult)}"'
    )
    observations = "".join(
        f'<Obs TIME_PERIOD="{year}-{month:02d}" '
        'OBS_VALUE="100.00" OBS_STATUS="A" />'
        for year in range(2000, 2027)
        for month in range(1, 13)
        if (year, month) <= (2026, 7)
    )
    content = (
        '<StructureSpecificData><DataSet><Series IDBANK="010768261" '
        'FREQ="M" UNIT_MEASURE="SO" REF_AREA="FM" '
        'LAST_UPDATE="2026-09-09" '
        'TITLE_EN="SA-WDA industrial production index (base 100 in 2021) - '
        "Manufacturing, mining and quarrying and other industrial "
        'activities (NAF rev. 2, level A10, item BE)"'
        f"{multiplier}>{observations}</Series></DataSet></StructureSpecificData>"
    )
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1,
        completed_at_ns=2,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "application/xml"},
        content=content.encode(),
        content_type="application/xml",
    )


def test_sdmx_zero_multiplier_preserves_base_2021_index_levels() -> None:
    series = _parse_current_series(_current_series_snapshot("0"))

    assert series.aggregate is InseeIndustrialProductionAggregate.INDUSTRY
    assert series.unit == "base-2021-equals-100"
    assert len(series.observations) == 319
    assert series.observations[0].reference_period == "2000-01"
    assert series.observations[-1].reference_period == "2026-07"
    assert {item.value for item in series.observations} == {100.0}


@pytest.mark.parametrize("unit_mult", ("3", "-3", None))
def test_sdmx_scale_drift_cannot_be_labeled_as_unscaled_index_levels(
    unit_mult: str | None,
) -> None:
    with pytest.raises(ValueError, match="SDMX series semantics differ"):
        _parse_current_series(_current_series_snapshot(unit_mult))
