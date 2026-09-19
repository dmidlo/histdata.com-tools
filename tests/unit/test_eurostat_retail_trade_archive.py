"""Qualification tests for Eurostat monthly retail trade."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest
from pypdf.errors import PdfReadError

import histdatacom.market_context.eurostat_retail_trade_archive as archive
from histdatacom.market_context.eurostat_retail_trade_archive import (
    EUROSTAT_RETAIL_TRADE_AS_OF_DATE,
    EUROSTAT_RETAIL_TRADE_DATASET_URI,
    EUROSTAT_RETAIL_TRADE_FIRST_REFERENCE_PERIOD,
    EUROSTAT_RETAIL_TRADE_FIRST_SEARCH_RELEASE_DATE,
    EUROSTAT_RETAIL_TRADE_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_RETAIL_TRADE_LATEST_REFERENCE_PERIOD,
    EUROSTAT_RETAIL_TRADE_MEASURE_KEY,
    EUROSTAT_RETAIL_TRADE_PARSER_ID,
    EUROSTAT_RETAIL_TRADE_PARSER_VERSION,
    EUROSTAT_RETAIL_TRADE_PROGRAM_KEY,
    EUROSTAT_RETAIL_TRADE_RELEASE_COUNT,
    EUROSTAT_RETAIL_TRADE_SEARCH_PAGE_COUNT,
    EUROSTAT_RETAIL_TRADE_SEARCH_RESULT_COUNT,
    EUROSTAT_RETAIL_TRADE_SEARCH_UNIQUE_URI_COUNT,
    EUROSTAT_RETAIL_TRADE_SOURCE_KEY,
    EurostatRetailTradeArchiveManifestV1,
    EurostatRetailTradeArtifactRole,
    EurostatRetailTradeInventoryEntryV1,
    EurostatRetailTradeMeasure,
    EurostatRetailTradeMovement,
    EurostatRetailTradePublishedValueV1,
    _artifact_from_snapshot,
    _composition_from_publication,
    _ea_composition_for_period,
    _headline_value,
    _inventory_prefilter,
    _parse_release_landing,
    _product_release_date,
    _published_revision_counts,
    _published_values,
    _reference_period_from_publication,
    _retail_context_measure,
    _retail_pdf_page_values,
    _retail_pdf_table_values,
    _retail_period_cell,
    _retail_table_published_values,
    build_eurostat_retail_trade_dataset_request,
    build_eurostat_retail_trade_release_requests,
    build_eurostat_retail_trade_search_requests,
    load_packaged_eurostat_retail_trade_archive_manifest,
    packaged_eurostat_retail_trade_manifest_path,
    parse_eurostat_retail_trade_dataset,
    parse_eurostat_retail_trade_inventory,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    *,
    resolved_uri: str | None = None,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.ATOM: "application/atom+xml",
        OfficialSourceFormat.JSON_STAT: "application/json",
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.PDF: "application/pdf",
    }[request.source_format]
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=resolved_uri or request.uri,
        response_headers={"Content-Type": content_type},
        content=content,
        content_type=content_type,
    )


def _entry() -> EurostatRetailTradeInventoryEntryV1:
    return EurostatRetailTradeInventoryEntryV1(
        product_code="4-21012002-ap",
        release_date="2002-01-21",
        source_title="Volume of retail trade down by 0.8% in euro-zone",
        source_uri="https://ec.europa.eu/eurostat/product?code=4-21012002-ap",
        atom_published_at="2002-01-20T22:00:00Z",
        atom_updated_at="2002-01-20T22:00:00Z",
        summary="In November 2001, retail trade fell by 0.8%.",
        page_number=1,
    )


def _document_request() -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_RETAIL_TRADE_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=(
            "https://ec.europa.eu/eurostat/documents/2995521/5198130/"
            "4-21012002-AP-EN.HTML.html/source"
        ),
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def _release_request() -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_RETAIL_TRADE_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=_entry().source_uri,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def _dataset_payload(*, unsupported_coordinate: bool = False) -> bytes:
    periods = ("2001-10", "2001-11")
    dimensions = {
        "freq": ("M",),
        "indic_bt": ("VOL_SLS",),
        "nace_r2": ("G47",),
        "s_adj": ("CA", "SCA"),
        "unit": ("PCH_PRE", "PCH_SM"),
        "geo": ("EA21", "DE", "FR"),
        "time": periods,
    }
    # CA/PCH_SM occupies flat indexes 6..11; SCA/PCH_PRE occupies 12..17.
    indexes = (
        range(6) if unsupported_coordinate else (*range(6, 12), *range(12, 18))
    )
    values = {str(index): (index - 11.5) / 10 for index in indexes}
    return json.dumps(
        {
            "class": "dataset",
            "label": "Retail trade - monthly data",
            "updated": "2026-09-17T11:00:00+0200",
            "id": [
                "freq",
                "indic_bt",
                "nace_r2",
                "s_adj",
                "unit",
                "geo",
                "time",
            ],
            "size": [1, 1, 1, 2, 2, 3, 2],
            "dimension": {
                name: {
                    "category": {
                        "index": {
                            code: index for index, code in enumerate(codes)
                        }
                    }
                }
                for name, codes in dimensions.items()
            },
            "value": values,
            "status": {},
        },
        separators=(",", ":"),
    ).encode()


def _atom_entry(title: str, code: str) -> str:
    uri = f"https://ec.europa.eu/eurostat/product?code={code}"
    return f"""<entry>
      <title>{title}</title><link rel="alternate" href="{uri}"/>
      <published>2002-01-20T22:00:00Z</published>
      <updated>2002-01-20T22:00:00Z</updated>
      <summary>Official summary</summary>
    </entry>"""


def _manifest() -> EurostatRetailTradeArchiveManifestV1:
    return load_packaged_eurostat_retail_trade_archive_manifest()


def test_packaged_archive_quantifies_the_complete_corpus() -> None:
    manifest = _manifest()
    path = packaged_eurostat_retail_trade_manifest_path()

    assert EUROSTAT_RETAIL_TRADE_SEARCH_PAGE_COUNT == 5
    assert EUROSTAT_RETAIL_TRADE_SEARCH_RESULT_COUNT == 417
    assert EUROSTAT_RETAIL_TRADE_SEARCH_UNIQUE_URI_COUNT == 417
    assert EUROSTAT_RETAIL_TRADE_RELEASE_COUNT == 298
    assert archive.EUROSTAT_RETAIL_TRADE_DOCUMENT_COUNT == 267
    assert EUROSTAT_RETAIL_TRADE_FIRST_SEARCH_RELEASE_DATE == "2000-04-12"
    assert EUROSTAT_RETAIL_TRADE_LATEST_PACKAGED_RELEASE_DATE == "2026-09-04"
    assert EUROSTAT_RETAIL_TRADE_FIRST_REFERENCE_PERIOD == "2000-01"
    assert EUROSTAT_RETAIL_TRADE_LATEST_REFERENCE_PERIOD == "2026-07"
    assert (
        archive.EUROSTAT_RETAIL_TRADE_MONTHLY_HEADLINE_FIRST_REFERENCE_PERIOD
        == "2007-07"
    )
    assert manifest.as_of_date == EUROSTAT_RETAIL_TRADE_AS_OF_DATE
    assert manifest.inventory.query_result_count == 417
    assert manifest.inventory.unique_uri_count == 417
    assert manifest.inventory.title_match_count == 298
    assert manifest.inventory.prefilter_count == 298
    assert len(manifest.inventory.entries) == 298
    assert len(manifest.inventory.exclusions) == 119
    assert len(manifest.inventory.artifacts) == 5
    assert manifest.release_count == 298
    assert manifest.reference_period_inferred_count == 0
    assert manifest.composition_source_stated_count == 237
    assert manifest.exact_publication_time_count == 46
    assert manifest.page_date_offset_count == 34
    assert manifest.release_document_count == 267
    assert manifest.published_value_count == 9_393
    assert manifest.revision_comparison_count == 7_528
    assert manifest.changed_revision_count == 4_933
    assert manifest.current_dataset_comparison_count == 6_195
    assert manifest.changed_current_dataset_count == 5_842
    assert manifest.raw_artifact_count == 571
    assert manifest.unique_content_sha256_count == 571
    assert manifest.total_content_bytes == 156_617_385
    assert manifest.manifest_id == (
        "eurostat-retail-trade-archive-manifest:sha256:"
        "cf3474ef694cf0eb3f333d523aa6ebdcaae18dff6c9ede47c054e02e4e693bb1"
    )
    assert path.stat().st_size == 6_566_066
    assert sha256(path.read_bytes()).hexdigest() == (
        "27ca0891002e131388e2bb3e2b3371c6304b6a0936998eef21b685d6b9bd30cd"
    )


def test_packaged_selection_and_release_scope_stay_explicit() -> None:
    manifest = _manifest()

    assert Counter(item.reason for item in manifest.inventory.exclusions) == {
        "labour-market release, not retail sales volume": 77,
        "broad search-summary match outside the monthly headline lineage": 28,
        "production release, not retail sales volume": 14,
    }
    assert tuple(item.reference_period for item in manifest.releases) == (
        "2000-01",
        *archive._month_range("2001-10", "2003-11"),
        *archive._month_range("2004-01", "2026-07"),
    )
    assert (
        manifest.searchable_release_gap_reference_periods
        == archive._month_range("2000-02", "2001-09") + ("2003-12",)
    )
    assert Counter(
        item.headline_value.movement for item in manifest.releases
    ) == {
        EurostatRetailTradeMovement.UP: 150,
        EurostatRetailTradeMovement.DOWN: 129,
        EurostatRetailTradeMovement.STABLE: 19,
    }
    assert Counter(
        item.headline_value.measure for item in manifest.releases
    ) == {
        EurostatRetailTradeMeasure.YEAR_OVER_YEAR: 69,
        EurostatRetailTradeMeasure.MONTH_OVER_MONTH: 229,
    }
    assert Counter(
        item.euro_area_composition for item in manifest.releases
    ) == {
        "EA11": 1,
        "EA12": 62,
        "EA13": 12,
        "EA15": 12,
        "EA16": 24,
        "EA17": 36,
        "EA18": 12,
        "EA19": 96,
        "EA20": 36,
        "EA21": 7,
    }
    assert Counter(
        item.reference_period_source_stated for item in manifest.releases
    ) == {True: 298}
    assert Counter(
        item.composition_source_stated for item in manifest.releases
    ) == {
        True: 237,
        False: 61,
    }
    assert min(item.days_after_period_end for item in manifest.releases) == 31
    assert max(item.days_after_period_end for item in manifest.releases) == 72
    assert min(item.headline_value.value for item in manifest.releases) == -11.7
    assert max(item.headline_value.value for item in manifest.releases) == 17.8


def test_packaged_landings_documents_and_values_are_bounded() -> None:
    manifest = _manifest()

    assert Counter(
        item.page_release_date_offset_days for item in manifest.releases
    ) == {-6: 1, -1: 32, 0: 264, 3: 1}
    assert {
        item.inventory_entry.product_code: item.page_release_date_offset_days
        for item in manifest.releases
        if item.page_release_date_offset_days not in {-1, 0}
    } == {
        "4-04052007-ap": 3,
        "4-19032009-bp": -6,
    }
    assert all(
        item.reference_period_source_stated for item in manifest.releases
    )
    assert Counter(
        len(item.linked_document_uris) for item in manifest.releases
    ) == {
        0: 31,
        1: 267,
    }
    assert Counter(
        item.document_artifact.source_format
        for item in manifest.releases
        if item.document_artifact is not None
    ) == {
        OfficialSourceFormat.HTML: 32,
        OfficialSourceFormat.PDF: 235,
    }
    assert Counter(
        len(item.published_values) for item in manifest.releases
    ) == {
        1: 16,
        11: 1,
        13: 1,
        15: 1,
        17: 10,
        18: 5,
        19: 4,
        24: 26,
        25: 2,
        30: 2,
        32: 1,
        34: 4,
        36: 225,
    }
    assert Counter(
        item.measure
        for release in manifest.releases
        for item in release.published_values
    ) == {
        EurostatRetailTradeMeasure.MONTH_OVER_MONTH: 4_589,
        EurostatRetailTradeMeasure.YEAR_OVER_YEAR: 4_804,
    }


def test_packaged_dataset_and_requests_are_independent() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_eurostat_retail_trade_search_requests(registry)
    dataset = build_eurostat_retail_trade_dataset_request(registry)
    releases = build_eurostat_retail_trade_release_requests(
        registry, manifest.inventory
    )

    assert EUROSTAT_RETAIL_TRADE_SOURCE_KEY == ("ea.eurostat.retail-trade")
    assert EUROSTAT_RETAIL_TRADE_PROGRAM_KEY == (
        "ea.eurostat.monthly-retail-trade"
    )
    assert EUROSTAT_RETAIL_TRADE_MEASURE_KEY == "retail-trade-volume-total"
    assert manifest.current_dataset.dataset_id == "sts_trtu_m"
    assert manifest.current_dataset.label == (
        "Turnover and volume of sales in wholesale and retail trade - monthly data"
    )
    assert manifest.current_dataset.updated_at == "2026-09-16T11:00:00+0200"
    assert manifest.current_dataset.time_start == "2000-01"
    assert manifest.current_dataset.time_end == "2026-07"
    assert len(manifest.current_dataset.observations) == 1_901
    assert Counter(
        item.economy_code for item in manifest.current_dataset.observations
    ) == {"EA21": 625, "DE": 638, "FR": 638}
    assert [item.page_number for item in searches] == list(range(1, 6))
    assert len(releases) == 298
    assert len({item.uri for item in releases}) == 298
    assert {item.parser_id for item in (*searches, dataset, *releases)} == {
        EUROSTAT_RETAIL_TRADE_PARSER_ID
    }
    assert {
        item.parser_version for item in (*searches, dataset, *releases)
    } == {EUROSTAT_RETAIL_TRADE_PARSER_VERSION}


def test_packaged_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EurostatRetailTradeArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    payload = json.loads(manifest.to_json())
    payload["published_value_count"] += 1
    payload["manifest_id"] = ""
    with pytest.raises(ValueError, match="published-value count differs"):
        EurostatRetailTradeArchiveManifestV1.from_json(
            json.dumps(payload, separators=(",", ":"), sort_keys=True)
        )


def test_inventory_counts_raw_entries_but_deduplicates_identical_uris(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(archive, "EUROSTAT_RETAIL_TRADE_SEARCH_PAGE_COUNT", 1)
    monkeypatch.setattr(
        archive, "EUROSTAT_RETAIL_TRADE_SEARCH_RESULT_COUNT", 17
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_RETAIL_TRADE_SEARCH_UNIQUE_URI_COUNT", 2
    )
    monkeypatch.setattr(archive, "EUROSTAT_RETAIL_TRADE_TITLE_MATCH_COUNT", 1)
    monkeypatch.setattr(archive, "EUROSTAT_RETAIL_TRADE_PREFILTER_COUNT", 1)
    monkeypatch.setattr(archive, "EUROSTAT_RETAIL_TRADE_RELEASE_COUNT", 1)
    monkeypatch.setattr(
        archive,
        "EUROSTAT_RETAIL_TRADE_FIRST_SEARCH_RELEASE_DATE",
        "2002-01-21",
    )
    monkeypatch.setattr(
        archive,
        "EUROSTAT_RETAIL_TRADE_LATEST_PACKAGED_RELEASE_DATE",
        "2002-01-21",
    )
    request = build_eurostat_retail_trade_search_requests(
        load_packaged_official_source_registry(), page_count=1
    )[0]
    selected = _atom_entry(
        "Volume of retail trade down by 0.8% in euro-zone", "4-21012002-ap"
    )
    excluded = _atom_entry(
        "Industrial producer prices up by 0.2%", "4-22012002-ap"
    )
    content = (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        + selected
        + excluded * 16
        + "</feed>"
    ).encode()

    inventory = parse_eurostat_retail_trade_inventory(
        (_snapshot(request, content),), as_of_date="2002-01-31"
    )

    assert inventory.query_result_count == 17
    assert inventory.unique_uri_count == 2
    assert [item.product_code for item in inventory.entries] == [
        "4-21012002-ap"
    ]
    assert [item.product_code for item in inventory.exclusions] == [
        "4-22012002-ap"
    ]

    altered = content.replace(
        b"Official summary</summary>\n    </entry></feed>",
        b"Changed summary</summary>\n    </entry></feed>",
    )
    with pytest.raises(ValueError, match="duplicate URI differs"):
        parse_eurostat_retail_trade_inventory(
            (_snapshot(request, altered),), as_of_date="2002-01-31"
        )


@pytest.mark.parametrize(
    ("code", "release_date"),
    (
        ("4-040582004-ap", "2004-08-04"),
        (
            (
                "https://ec.europa.eu/eurostat/documents/2995521/5138486/"
                "4-040582004-AP-EN.HTML.html/source"
            ),
            "2004-08-04",
        ),
        ("4-17112004-de-bp", "2004-11-17"),
        (
            (
                "https://ec.europa.eu/eurostat/web/products-euro-indicators/-/"
                "4-17112004-de-bp"
            ),
            "2004-11-17",
        ),
        ("4-020102015-ap", "2015-01-02"),
    ),
)
def test_historical_product_codes_have_deterministic_release_dates(
    code: str, release_date: str
) -> None:
    assert _product_release_date(code) == release_date


@pytest.mark.parametrize(
    ("title", "selected"),
    (
        ("Volume of retail trade down by 0.8%", True),
        ("Volume of retail trade remained stable in the euro area", True),
        ("Industrial producer prices up by 0.2%", False),
        ("Services production increased by 1.0%", False),
    ),
)
def test_title_lineage_is_an_explicit_prefix(
    title: str, selected: bool
) -> None:
    assert _inventory_prefilter(title) is selected


def test_landing_binds_title_date_document_reference_and_composition() -> None:
    content = b"""<html><head>
    <meta property="og:title" content="Volume of retail trade down by 0.8% in euro-zone">
    </head><body>
    <p>Release date: 20 January 2002</p>
    <p>In November 2001, retail trade in the euro-zone (EA12)
    decreased by 0.8% compared with October 2001.</p>
    <a title="Download publication" href="/eurostat/documents/2995521/5198130/4-21012002-AP-EN.HTML.html/source">HTML</a>
    <a href="/eurostat/documents/2995521/5198130/4-21012002-AP-EN.HTML.html/source?download=true">Download HTML</a>
    </body></html>"""
    snapshot = _snapshot(
        _release_request(),
        content,
        resolved_uri=(
            "https://ec.europa.eu/eurostat/en/web/"
            "products-euro-indicators/w/4-21012002-ap"
        ),
    )

    title, page_date, published_at, links = _parse_release_landing(
        snapshot, _entry()
    )
    reference, lag = _reference_period_from_publication(
        _entry(), snapshot, None
    )
    composition = _composition_from_publication(reference, snapshot, None)

    assert title == _entry().source_title
    assert page_date == "2002-01-20"
    assert published_at is None
    assert links == (
        (
            "https://ec.europa.eu/eurostat/documents/2995521/5198130/"
            "4-21012002-AP-EN.HTML.html/source"
        ),
    )
    assert (reference, lag) == ("2001-11", 52)
    assert composition == ("EA12", True)


def test_landing_retains_document_for_the_malformed_legacy_product_code() -> (
    None
):
    entry = EurostatRetailTradeInventoryEntryV1(
        product_code="4-040582004-ap",
        release_date="2004-08-04",
        source_title="Volume of retail trade up by 1.2% in euro-zone",
        source_uri=(
            "https://ec.europa.eu/eurostat/product?code=4-040582004-ap"
        ),
        atom_published_at="2004-08-04T22:00:00Z",
        atom_updated_at="2004-08-04T22:00:00Z",
        summary="Official summary",
        page_number=1,
    )
    request = replace(_release_request(), uri=entry.source_uri)
    content = b"""<html><head>
    <meta property="og:title" content="Volume of retail trade up by 1.2% in euro-zone">
    </head><body><p>Release date: 4 August 2004</p>
    <a title="Download publication" href="/eurostat/documents/2995521/5138486/4-040582004-AP-EN.HTML.html/source">HTML</a>
    </body></html>"""
    snapshot = _snapshot(
        request,
        content,
        resolved_uri=(
            "https://ec.europa.eu/eurostat/web/products-euro-indicators/-/"
            "4-040582004-ap"
        ),
    )

    assert _parse_release_landing(snapshot, entry)[3] == (
        (
            "https://ec.europa.eu/eurostat/documents/2995521/5138486/"
            "4-040582004-AP-EN.HTML.html/source"
        ),
    )


def test_legacy_document_reference_allows_the_observed_72_day_lag() -> None:
    entry = replace(
        _entry(),
        product_code="4-12042000-ap",
        release_date="2000-04-12",
        source_title="Volume of retail trade up by 2.3% in the euro-zone",
        source_uri="https://ec.europa.eu/eurostat/product?code=4-12042000-ap",
        summary="Official summary without a reference month.",
        entry_id="",
    )
    document = _snapshot(
        _document_request(),
        (
            b"<html><body><p>12 April 2000</p>"
            b"<p>January 2000 compared with January 1999</p></body></html>"
        ),
    )

    assert _reference_period_from_publication(entry, document, document) == (
        "2000-01",
        72,
    )


def test_legacy_two_digit_period_headers_use_a_bounded_century_pivot() -> None:
    assert _retail_period_cell("Aug-99") == "1999-08"
    assert _retail_period_cell("Jan-00") == "2000-01"
    assert _retail_period_cell("Jul-26") == "2026-07"


def test_nearest_table_context_marker_controls_the_measure() -> None:
    annual_then_monthly = "same month of the previous year narrative; change compared with previous month"
    monthly_then_annual = "change compared with previous month narrative; same month of the previous year"

    assert _retail_context_measure(annual_then_monthly) is (
        EurostatRetailTradeMeasure.MONTH_OVER_MONTH
    )
    assert _retail_context_measure(monthly_then_annual) is (
        EurostatRetailTradeMeasure.YEAR_OVER_YEAR
    )


@pytest.mark.parametrize(
    ("reference_period", "composition"),
    (
        ("2000-01", "EA11"),
        ("2006-12", "EA12"),
        ("2007-01", "EA13"),
        ("2008-01", "EA15"),
        ("2009-01", "EA16"),
        ("2011-01", "EA17"),
        ("2014-01", "EA18"),
        ("2015-01", "EA19"),
        ("2023-01", "EA20"),
        ("2026-01", "EA21"),
    ),
)
def test_composition_follows_the_reference_month(
    reference_period: str, composition: str
) -> None:
    assert _ea_composition_for_period(reference_period) == composition


def test_dataset_parser_decodes_only_the_two_supported_measure_coordinates() -> (
    None
):
    request = build_eurostat_retail_trade_dataset_request(
        load_packaged_official_source_registry()
    )
    assert request.uri == EUROSTAT_RETAIL_TRADE_DATASET_URI

    parsed = parse_eurostat_retail_trade_dataset(
        _snapshot(request, _dataset_payload())
    )

    assert len(parsed.observations) == 12
    assert {
        (item.measure, item.economy_code) for item in parsed.observations
    } == {
        (measure, economy)
        for measure in EurostatRetailTradeMeasure
        for economy in ("EA21", "DE", "FR")
    }
    assert parsed == type(parsed).from_dict(parsed.to_dict())

    with pytest.raises(
        ValueError, match="unsupported adjustment and unit combination"
    ):
        parse_eurostat_retail_trade_dataset(
            _snapshot(request, _dataset_payload(unsupported_coordinate=True))
        )

    altered = replace(
        parsed.observations[0],
        flat_index=parsed.observations[0].flat_index + 1,
        observation_id="",
    )
    with pytest.raises(ValueError, match="observation coordinate differs"):
        replace(
            parsed,
            observations=(altered, *parsed.observations[1:]),
            dataset_receipt_id="",
        )


@pytest.mark.parametrize(
    ("title", "movement", "value"),
    (
        (
            "Volume of retail trade up by 1.2%",
            EurostatRetailTradeMovement.UP,
            1.2,
        ),
        (
            "Volume of retail trade down by 0.8%",
            EurostatRetailTradeMovement.DOWN,
            -0.8,
        ),
        (
            "Volume of retail trade remained stable",
            EurostatRetailTradeMovement.STABLE,
            0.0,
        ),
    ),
)
def test_headline_direction_is_preserved_as_a_signed_value(
    title: str, movement: EurostatRetailTradeMovement, value: float
) -> None:
    parsed = _headline_value(
        replace(_entry(), source_title=title, entry_id=""), "2001-11"
    )

    assert parsed.measure is EurostatRetailTradeMeasure.YEAR_OVER_YEAR
    assert parsed.movement is movement
    assert parsed.value == value
    assert parsed.value_lexical == (
        "0"
        if movement is EurostatRetailTradeMovement.STABLE
        else str(abs(value))
    )


def test_headline_measure_transition_is_source_era_specific() -> None:
    assert (
        _headline_value(_entry(), "2007-06").measure
        is EurostatRetailTradeMeasure.YEAR_OVER_YEAR
    )
    assert (
        _headline_value(_entry(), "2007-07").measure
        is EurostatRetailTradeMeasure.MONTH_OVER_MONTH
    )


def test_legacy_headline_reconciles_to_annual_instead_of_monthly_table() -> (
    None
):
    content = b"""<html><body>
    <h2>Monthly comparison</h2>
    <table><tr><th></th><th>Nov-01</th></tr>
    <tr><td>Euro-zone</td><td>-1.0</td></tr></table>
    <h2>Annual comparison</h2>
    <table><tr><th></th><th>Nov-01</th></tr>
    <tr><td>Euro-zone</td><td>-0.8</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_release_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatRetailTradeArtifactRole.RELEASE_HTML,
        product_code=_entry().product_code,
    )
    headline = _headline_value(_entry(), "2001-11")

    values = _published_values(
        snapshot,
        artifact,
        None,
        None,
        "2001-11",
        headline,
    )

    assert (
        next(
            item
            for item in values
            if item.economy_code == "EA"
            and item.reference_period == "2001-11"
            and item.measure is EurostatRetailTradeMeasure.YEAR_OVER_YEAR
        ).value
        == -0.8
    )


def test_html_tables_retain_monthly_and_annual_ea_de_fr_values() -> None:
    content = b"""<html><body>
    <table><tr><th>Percentage change compared with previous month</th></tr>
    <tr><th>Economy</th><th>Jun-01</th><th>Jul-01</th><th>Aug-01</th><th>Sep-01</th><th>Oct-01</th><th>Nov-01</th></tr>
    <tr><td>Euro-zone</td><td>0.2</td><td>-0.1</td><td>0.0</td><td>0.3</td><td>-0.2</td><td>-0.8</td></tr>
    <tr><td>Germany</td><td>0.1</td><td>-0.2</td><td>0.4</td><td>0.2</td><td>-0.3</td><td>-1.0</td></tr>
    <tr><td>France</td><td>0.3</td><td>0.2</td><td>-0.1</td><td>0.4</td><td>0.1</td><td>-0.4</td></tr></table>
    <table><tr><th>Percentage change compared with same month one year ago</th></tr>
    <tr><th>Economy</th><th>Jun-01</th><th>Jul-01</th><th>Aug-01</th><th>Sep-01</th><th>Oct-01</th><th>Nov-01</th></tr>
    <tr><td>Euro-zone</td><td>1.2</td><td>0.9</td><td>0.7</td><td>0.4</td><td>0.1</td><td>-0.5</td></tr>
    <tr><td>Germany</td><td>1.0</td><td>0.7</td><td>0.5</td><td>0.2</td><td>-0.1</td><td>-0.8</td></tr>
    <tr><td>France</td><td>1.4</td><td>1.1</td><td>0.9</td><td>0.6</td><td>0.3</td><td>-0.2</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatRetailTradeArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _retail_table_published_values(snapshot, artifact, "2001-11")

    assert len(values) == 36
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2001-11"
    } == {
        ("EA", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -0.8),
        ("DE", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -1.0),
        ("FR", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -0.4),
        ("EA", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, -0.5),
        ("DE", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, -0.8),
        ("FR", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, -0.2),
    }

    conflicting = replace(
        snapshot,
        content=content.replace(
            b"</body>",
            b"<table><tr><th>month-on-month</th></tr><tr><th></th><th>Nov-01</th></tr><tr><td>Euro-zone</td><td>9.9</td></tr></table></body>",
        ),
    )
    with pytest.raises(ValueError, match="HTML tables conflict"):
        _retail_table_published_values(conflicting, artifact, "2001-11")


def test_pdf_text_tables_retain_source_rows_and_reject_conflicts() -> None:
    snapshot = _snapshot(
        _document_request(), b"<html><body>evidence</body></html>"
    )
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatRetailTradeArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )
    monthly = """Retail trade - monthly variation
    % change compared with previous month
    Feb-07 Mar-07 Apr-07
    EA13
      Total retail trade4 0.5 0.5 -0.8
    Total retail trade4 Feb-07 Mar-07 Apr-07
    Euro-zone 0.5 0.5 -0.8
    Germany 0.3 0.7 -2.3
    France 1.3 0.2 -0.8
    """
    annual = """Retail trade - annual variation
    % change compared with same month of the previous year
    Feb-07 Mar-07 Apr-07
    EA13
      Total retail trade4 4.0 4.0 2.8
    Total retail trade4 Feb-07 Mar-07 Apr-07
    EA13 4.0 4.0 2.8
    Germany 6.1 7.3 4.0
    France 2.5 0.8 1.8
    """

    values = (
        *_retail_pdf_page_values(monthly, artifact, "2007-04", 3),
        *_retail_pdf_page_values(annual, artifact, "2007-04", 4),
    )

    assert len(values) == 18
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2007-04"
    } == {
        ("EA", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -0.8),
        ("DE", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -2.3),
        ("FR", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -0.8),
        ("EA", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, 2.8),
        ("DE", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, 4.0),
        ("FR", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, 1.8),
    }

    with pytest.raises(ValueError, match="PDF tables conflict"):
        _retail_pdf_page_values(
            monthly + "\nEA13 0.5 0.5 9.9\n", artifact, "2007-04", 3
        )


def test_html_heading_context_does_not_misclassify_later_index_tables() -> None:
    content = b"""<html><body>
    <h2>Retail trade - monthly variation</h2>
    <p>% change compared with previous month</p>
    <table><tr><th>Total retail trade</th><th>Jun-04</th></tr>
    <tr><td>Euro-zone</td><td>-0.4</td></tr>
    <tr><td>Germany</td><td>-2.0</td></tr>
    <tr><td>France</td><td>0.2</td></tr></table>
    <h2>Indices of the volume of retail trade</h2>
    <table><tr><th></th><th>06/04</th></tr>
    <tr><td>Euro-zone</td><td>102.1</td></tr>
    <tr><td>Germany</td><td>101.1</td></tr>
    <tr><td>France</td><td>101.2</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatRetailTradeArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _retail_table_published_values(snapshot, artifact, "2004-06")

    assert [(item.economy_code, item.value) for item in values] == [
        ("EA", -0.4),
        ("DE", -2.0),
        ("FR", 0.2),
    ]


def test_in_table_measure_headers_override_stale_landing_context() -> None:
    content = b"""<html><body>
    <h2>Annual comparison</h2>
    <table><tr><th>Retail trade % change compared with previous month</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>1.6</td><td>-3.2</td></tr>
    <tr><td>Germany</td><td>-1.8</td><td>0.6</td></tr>
    <tr><td>France</td><td>0.3</td><td>-1.0</td></tr></table>
    <table><tr><th>Retail trade % change compared with same month of the previous year</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>0.2</td><td>-6.7</td></tr>
    <tr><td>Germany</td><td>-4.3</td><td>-5.4</td></tr>
    <tr><td>France</td><td>0.5</td><td>1.0</td></tr></table>
    <p>Deflated turnover for total retail trade, seasonally adjusted (2015 = 100)</p>
    <table><tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>102.4</td><td>101.8</td></tr></table>
    <p>Deflated turnover for total retail trade, working-day adjusted (Base year 2015)</p>
    <table><tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>103.1</td><td>102.6</td></tr></table>
    <table><tr><th>Index of the volume of retail trade, calendar adjusted</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>95.1</td><td>94.0</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatRetailTradeArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _retail_table_published_values(snapshot, artifact, "2024-01")

    assert len(values) == 12
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2024-01"
    } == {
        ("EA", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -3.2),
        ("DE", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, 0.6),
        ("FR", EurostatRetailTradeMeasure.MONTH_OVER_MONTH, -1.0),
        ("EA", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, -6.7),
        ("DE", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, -5.4),
        ("FR", EurostatRetailTradeMeasure.YEAR_OVER_YEAR, 1.0),
    }


def test_parallel_euro_area_rows_select_the_reference_era_composition() -> None:
    content = b"""<html><body>
    <h2>Monthly comparison</h2>
    <table><tr><th>Total retail trade</th><th>Jan-26</th></tr>
    <tr><td>Euro area 20</td><td>0.5</td></tr>
    <tr><td>Euro area 21</td><td>0.4</td></tr>
    <tr><td>Germany</td><td>0.3</td></tr>
    <tr><td>France</td><td>0.2</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatRetailTradeArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _retail_table_published_values(snapshot, artifact, "2026-01")

    assert [(item.economy_code, item.value) for item in values] == [
        ("EA", 0.4),
        ("DE", 0.3),
        ("FR", 0.2),
    ]


def test_pdf_table_parser_tolerates_only_one_unreadable_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = replace(
        _document_request(),
        uri=_document_request().uri.replace("HTML.html", "PDF.pdf"),
        source_format=OfficialSourceFormat.PDF,
    )
    snapshot = _snapshot(request, b"%PDF-test")
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatRetailTradeArtifactRole.RELEASE_DOCUMENT_PDF,
        product_code="4-21012002-ap",
    )

    class _Page:
        def __init__(self, text: str | None) -> None:
            self.text = text

        def extract_text(self) -> str:
            if self.text is None:
                raise PdfReadError("test unreadable page")
            return self.text

    class _Reader:
        pages = (_Page(None), _Page("readable narrative"))

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

    monkeypatch.setattr(archive, "PdfReader", _Reader)
    assert _retail_pdf_table_values(snapshot, artifact, "2001-11") == ()

    _Reader.pages = (_Page(None), _Page(None))
    archive._retail_pdf_page_texts.cache_clear()
    with pytest.raises(ValueError, match="too many unreadable pages"):
        _retail_pdf_table_values(snapshot, artifact, "2001-11")


def test_revision_counts_do_not_cross_euro_area_compositions() -> None:
    digest = "0" * 64

    def value(
        economy: str, numeric: float
    ) -> EurostatRetailTradePublishedValueV1:
        lexical = str(numeric)
        return EurostatRetailTradePublishedValueV1(
            economy_code=economy,
            reference_period="2007-01",
            measure=EurostatRetailTradeMeasure.MONTH_OVER_MONTH,
            value_lexical=lexical,
            value=numeric,
            evidence_artifact_sha256=digest,
            evidence_locator="test table",
        )

    releases = (
        SimpleNamespace(
            euro_area_composition="EA12",
            published_values=(value("EA", 0.1), value("DE", 0.2)),
        ),
        SimpleNamespace(
            euro_area_composition="EA13",
            published_values=(value("EA", 0.3), value("DE", 0.4)),
        ),
    )

    assert _published_revision_counts(releases) == (1, 1)
