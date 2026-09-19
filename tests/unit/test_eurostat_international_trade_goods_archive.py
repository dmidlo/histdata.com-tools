"""Qualification tests for Eurostat monthly international trade in goods."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

import histdatacom.market_context.eurostat_international_trade_goods_archive as archive
from histdatacom import market_context
from histdatacom.market_context.eurostat_international_trade_goods_archive import (
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_AS_OF_DATE,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_SEARCH_RELEASE_DATE,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_ID,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_VERSION,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_RESULT_COUNT,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT,
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY,
    EurostatInternationalTradeGoodsArchiveManifestV1,
    EurostatInternationalTradeGoodsArtifactRole,
    EurostatInternationalTradeGoodsInventoryEntryV1,
    EurostatInternationalTradeGoodsMeasure,
    EurostatInternationalTradeGoodsMovement,
    EurostatInternationalTradeGoodsPublishedValueV1,
    _artifact_from_snapshot,
    _composition_from_publication,
    _current_dataset_comparison_counts,
    _ea_composition_for_period,
    _headline_value,
    _inventory_prefilter,
    _parse_release_landing,
    _published_revision_counts,
    _published_values,
    _reference_period_from_publication,
    build_eurostat_international_trade_goods_dataset_request,
    build_eurostat_international_trade_goods_release_requests,
    build_eurostat_international_trade_goods_search_requests,
    load_packaged_eurostat_international_trade_goods_archive_manifest,
    packaged_eurostat_international_trade_goods_manifest_path,
    parse_eurostat_international_trade_goods_dataset,
    parse_eurostat_international_trade_goods_inventory,
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


def _entry(
    *,
    code: str = "6-15062012-bp",
    release_date: str = "2012-06-15",
    title: str = "Euro area international trade in goods surplus 10.0 bn euro",
) -> EurostatInternationalTradeGoodsInventoryEntryV1:
    return EurostatInternationalTradeGoodsInventoryEntryV1(
        product_code=code,
        release_date=release_date,
        source_title=title,
        source_uri=f"https://ec.europa.eu/eurostat/product?code={code}",
        atom_published_at=f"{release_date}T09:00:00Z",
        atom_updated_at=f"{release_date}T09:00:00Z",
        summary="In April 2012, the euro area recorded an international trade surplus.",
        page_number=1,
    )


def _release_request(
    entry: EurostatInternationalTradeGoodsInventoryEntryV1,
) -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=entry.source_uri,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def _dataset_payload(*, omit_coordinate: bool = False) -> bytes:
    periods = ("2012-03", "2012-04")
    dimensions = {
        "freq": ("M",),
        "stk_flow": ("IMP", "EXP"),
        "indic_et": ("TRD_VAL", "TRD_VAL_SCA"),
        "partner": ("EA21", "EXT_EA21"),
        "sitc06": ("TOTAL",),
        "geo": ("EA21",),
        "time": periods,
    }
    values = {str(index): float(index + 1) for index in range(16)}
    if omit_coordinate:
        del values["6"]
    return json.dumps(
        {
            "version": "2.0",
            "class": "dataset",
            "label": "Euro area trade by SITC product group",
            "source": "ESTAT",
            "updated": "2026-09-15T11:00:00+0200",
            "id": [
                "freq",
                "stk_flow",
                "indic_et",
                "partner",
                "sitc06",
                "geo",
                "time",
            ],
            "size": [1, 2, 2, 2, 1, 1, 2],
            "dimension": {
                name: {
                    "category": {
                        "index": {
                            code: index for index, code in enumerate(codes)
                        },
                        **(
                            {
                                "label": {
                                    "TRD_VAL": "Trade value in million ECU/EURO",
                                    "TRD_VAL_SCA": (
                                        "Seasonally and calendar adjusted trade value "
                                        "in million ECU/EURO"
                                    ),
                                }
                            }
                            if name == "indic_et"
                            else {}
                        ),
                    }
                }
                for name, codes in dimensions.items()
            },
            "value": values,
            "status": {"7": "p"},
            "extension": {"id": "EXT_ST_EASITC", "agencyId": "ESTAT"},
        },
        separators=(",", ":"),
    ).encode()


def _atom_entry(title: str, code: str) -> str:
    uri = f"https://ec.europa.eu/eurostat/product?code={code}"
    date = archive._product_release_date(code)
    return f"""<entry>
      <title>{title}</title><link rel="alternate" href="{uri}"/>
      <published>{date}T09:00:00Z</published>
      <updated>{date}T09:00:00Z</updated>
      <summary>Official summary</summary>
    </entry>"""


def _trade_html() -> bytes:
    return b"""<html><body>
    <p>April 2012</p><p>Euro area 17 (EA17)</p>
    <h2>EA17 trade - non seasonally adjusted data (bn euro)</h2>
    <p>Extra-EA17 exports 100.0 110.0</p>
    <p>Extra-EA17 imports 95.0 100.0</p>
    <p>Balance 5.0 10.0</p>
    <p>Intra-EA17 trade 90.0 95.0</p>
    <h2>Annex - Seasonally adjusted data</h2>
    <p>previous three months</p>
    <h3>EA17 trade - seasonally adjusted data (bn euro)</h3>
    <p>Extra-EA17 exports 101.0 111.0</p>
    <p>Extra-EA17 imports 96.0 101.0</p>
    <p>Balance 5.0 10.0</p>
    <p>Intra-EA17 trade 91.0 96.0</p>
    </body></html>"""


def _manifest() -> EurostatInternationalTradeGoodsArchiveManifestV1:
    return load_packaged_eurostat_international_trade_goods_archive_manifest()


def test_packaged_archive_is_complete_and_registry_bound() -> None:
    manifest = _manifest()
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY)
    path = packaged_eurostat_international_trade_goods_manifest_path()

    assert EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT == 5
    assert EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_RESULT_COUNT == 414
    assert EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT == 414
    assert EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT == 172
    assert archive.EUROSTAT_INTERNATIONAL_TRADE_GOODS_DOCUMENT_COUNT == 141
    assert (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_SEARCH_RELEASE_DATE
        == "2012-06-15"
    )
    assert (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_PACKAGED_RELEASE_DATE
        == "2026-09-15"
    )
    assert (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD == "2012-04"
    )
    assert (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD == "2026-07"
    )
    assert manifest.as_of_date == EUROSTAT_INTERNATIONAL_TRADE_GOODS_AS_OF_DATE
    assert manifest.registry_id == registry.registry_id
    assert manifest.source_id == source.source_id
    assert manifest.inventory.query_result_count == 414
    assert manifest.inventory.unique_uri_count == 414
    assert manifest.inventory.title_match_count == 174
    assert manifest.inventory.prefilter_count == 172
    assert len(manifest.inventory.entries) == 172
    assert len(manifest.inventory.exclusions) == 242
    assert len(manifest.inventory.artifacts) == 5
    assert manifest.release_count == 172
    assert manifest.reference_period_inferred_count == 0
    assert manifest.composition_source_stated_count == 169
    assert manifest.exact_publication_time_count == 46
    assert manifest.page_date_offset_count == 0
    assert manifest.release_document_count == 141
    assert manifest.published_value_count == 2_734
    assert manifest.revision_comparison_count == 1_127
    assert manifest.changed_revision_count == 1_082
    assert manifest.current_dataset_comparison_count == 84
    assert manifest.changed_current_dataset_count == 67
    assert manifest.raw_artifact_count == 319
    assert manifest.unique_content_sha256_count == 319
    assert manifest.total_content_bytes == 104_536_218
    assert manifest.current_dataset.dataset_id == "ext_st_easitc"
    assert len(manifest.current_dataset.observations) == 1_122
    assert manifest.current_dataset.time_start == "2011-01"
    assert manifest.current_dataset.time_end == "2026-07"
    assert manifest.manifest_id == (
        "eurostat-international-trade-goods-archive-manifest:sha256:"
        "1c22ddce15a3dda58c1a862dac00d71403600460859e5a5972972f4a32c1de43"
    )
    assert path.stat().st_size == 2_816_352
    assert sha256(path.read_bytes()).hexdigest() == (
        "bed345acacc9f6c8f2b62aa31df6a874760354968abe1fc6a2fe3d8929d7a9a2"
    )


def test_packaged_selection_and_values_are_explicit() -> None:
    manifest = _manifest()

    assert Counter(item.reason for item in manifest.inventory.exclusions) == {
        "balance-of-payments release, not merchandise-trade headline": 144,
        "broad search result outside the monthly euro-area goods-trade headline": 45,
        "partner, product, or EU-only trade release outside the monthly euro-area headline": 28,
        "labour-market release returned by the broad text search": 13,
        "services-trade release, not merchandise trade in goods": 12,
    }
    assert tuple(
        item.reference_period for item in manifest.releases
    ) == archive._month_range(
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD,
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD,
    )
    assert Counter(
        len(item.published_values) for item in manifest.releases
    ) == {
        8: 2,
        14: 1,
        16: 169,
    }
    assert {item.headline_value.period_scope for item in manifest.releases} == {
        "month",
        "year-to-date",
    }
    assert Counter(
        item.headline_value.period_scope for item in manifest.releases
    ) == {"month": 158, "year-to-date": 14}
    assert Counter(
        item.headline_value.movement for item in manifest.releases
    ) == {
        EurostatInternationalTradeGoodsMovement.SURPLUS: 150,
        EurostatInternationalTradeGoodsMovement.DEFICIT: 22,
    }
    assert Counter(
        item.euro_area_composition for item in manifest.releases
    ) == {
        "EA17": 21,
        "EA18": 12,
        "EA19": 96,
        "EA20": 36,
        "EA21": 7,
    }
    assert [
        item.inventory_entry.product_code
        for item in manifest.releases
        if not item.composition_source_stated
    ] == ["6-15012026-bp", "6-13022026-bp", "6-20032026-ap"]
    assert Counter(
        len(item.linked_document_uris) for item in manifest.releases
    ) == {0: 31, 1: 141}
    assert {
        item.document_artifact.role
        for item in manifest.releases
        if item.document_artifact is not None
    } == {EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_PDF}
    assert min(item.days_after_period_end for item in manifest.releases) == 44
    assert max(item.days_after_period_end for item in manifest.releases) == 54
    assert min(item.headline_value.value for item in manifest.releases) == -50.9
    assert max(item.headline_value.value for item in manifest.releases) == 246.0
    assert (
        manifest.searchable_release_gap_reference_periods
        == archive._month_range("2011-01", "2012-03")
    )
    assert {
        value.measure
        for item in manifest.releases
        for value in item.published_values
    } == set(EurostatInternationalTradeGoodsMeasure)


def test_packaged_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EurostatInternationalTradeGoodsArchiveManifestV1.from_json(
            manifest.to_json()
        )
        == manifest
    )
    payload = json.loads(manifest.to_json())
    payload["published_value_count"] += 1
    payload["manifest_id"] = ""
    with pytest.raises(ValueError, match="published-value count differs"):
        EurostatInternationalTradeGoodsArchiveManifestV1.from_json(
            json.dumps(payload, separators=(",", ":"), sort_keys=True)
        )


def test_requests_keep_search_dataset_and_release_boundaries_separate() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_eurostat_international_trade_goods_search_requests(
        registry
    )
    dataset = build_eurostat_international_trade_goods_dataset_request(registry)
    releases = build_eurostat_international_trade_goods_release_requests(
        registry, manifest.inventory
    )

    assert [item.page_number for item in searches] == [1, 2, 3, 4, 5]
    assert all(
        item.source_format is OfficialSourceFormat.ATOM for item in searches
    )
    search_query = parse_qs(urlsplit(searches[0].uri).query)
    assert search_query[archive._SEARCH_PREFIX + "text"] == [
        "international trade in goods"
    ]
    assert search_query[archive._SEARCH_PREFIX + "collection"] == ["CAT_PREREL"]
    assert dataset.uri == EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI
    assert dataset.source_format is OfficialSourceFormat.JSON_STAT
    assert len(releases) == 172
    assert all(
        item.source_format is OfficialSourceFormat.HTML for item in releases
    )
    assert {item.parser_id for item in (*searches, dataset, *releases)} == {
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_ID
    }
    assert {
        item.parser_version for item in (*searches, dataset, *releases)
    } == {EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_VERSION}


def test_inventory_counts_raw_entries_but_deduplicates_identical_uris(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected_code = "6-15062012-bp"
    excluded_code = "4-14062012-ap"
    selected_title = (
        "Euro area international trade in goods surplus 10.0 bn euro"
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_RESULT_COUNT", 14
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT", 2
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INTERNATIONAL_TRADE_GOODS_TITLE_MATCH_COUNT", 2
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INTERNATIONAL_TRADE_GOODS_PREFILTER_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT", 1
    )
    monkeypatch.setattr(
        archive,
        "EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_SEARCH_RELEASE_DATE",
        "2012-06-15",
    )
    monkeypatch.setattr(
        archive,
        "EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_PACKAGED_RELEASE_DATE",
        "2012-06-15",
    )
    request = build_eurostat_international_trade_goods_search_requests(
        load_packaged_official_source_registry(), page_count=1
    )[0]
    content = (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        + _atom_entry(selected_title, selected_code)
        + _atom_entry(
            "EU international trade in goods by partner", excluded_code
        )
        * 13
        + "</feed>"
    ).encode()

    inventory = parse_eurostat_international_trade_goods_inventory(
        (_snapshot(request, content),), as_of_date="2012-06-30"
    )

    assert inventory.query_result_count == 14
    assert inventory.unique_uri_count == 2
    assert [item.product_code for item in inventory.entries] == [selected_code]
    assert [item.product_code for item in inventory.exclusions] == [
        excluded_code
    ]

    altered = content.replace(
        b"Official summary</summary>\n    </entry></feed>",
        b"Changed summary</summary>\n    </entry></feed>",
    )
    with pytest.raises(ValueError, match="duplicate URI differs"):
        parse_eurostat_international_trade_goods_inventory(
            (_snapshot(request, altered),), as_of_date="2012-06-30"
        )


@pytest.mark.parametrize(
    ("title", "selected"),
    (
        ("Euro area international trade in goods surplus 10.0 bn euro", True),
        ("Euro area international trade in goods deficit 2.1 bn euro", True),
        ("EU international trade in goods by partner", False),
        (
            "Euro area international trade in services surplus 1.0 bn euro",
            False,
        ),
    ),
)
def test_title_lineage_is_an_explicit_prefix(
    title: str, selected: bool
) -> None:
    assert _inventory_prefilter(title) is selected


def test_landing_binds_title_date_document_reference_and_composition() -> None:
    entry = _entry()
    content = f"""<html><head>
    <meta property="og:title" content="{entry.source_title}">
    <meta property="article:published_time" content="2012-06-15T09:00:00Z">
    </head><body>
    <p>Release date: 15 June 2012</p>
    <p>In April 2012, international trade in goods in the euro area 17 (EA17)
    recorded a surplus.</p>
    <a title="Download publication" href="/eurostat/documents/2995521/5149418/6-15062012-BP-EN.PDF.pdf/source">PDF</a>
    </body></html>""".encode()
    snapshot = _snapshot(
        _release_request(entry),
        content,
        resolved_uri=(
            "https://ec.europa.eu/eurostat/en/web/products-euro-indicators/w/"
            "6-15062012-bp"
        ),
    )

    title, page_date, published_at, links = _parse_release_landing(
        snapshot, entry
    )
    reference, lag = _reference_period_from_publication(entry, snapshot, None)
    composition = _composition_from_publication(reference, snapshot, None)

    assert title == entry.source_title
    assert page_date == "2012-06-15"
    assert published_at == "2012-06-15T09:00:00Z"
    assert links == (
        (
            "https://ec.europa.eu/eurostat/documents/2995521/5149418/"
            "6-15062012-BP-EN.PDF.pdf/source"
        ),
    )
    assert (reference, lag) == ("2012-04", 46)
    assert composition == ("EA17", True)


@pytest.mark.parametrize(
    ("code", "release_date", "human_date", "filename"),
    (
        (
            "6-16122021-ap",
            "2021-12-16",
            "16 December 2021",
            "6-16122021-+AP-EN.pdf",
        ),
        (
            "6-15112023-bp",
            "2023-11-15",
            "15 November 2023",
            "6-15112023-PB-EN.pdf",
        ),
    ),
)
def test_landing_keeps_observed_document_filename_aliases_bounded(
    code: str, release_date: str, human_date: str, filename: str
) -> None:
    entry = _entry(code=code, release_date=release_date)
    uri = f"https://ec.europa.eu/eurostat/documents/2995521/1/{filename}/id"
    content = f"""<html><head>
    <meta property="og:title" content="{entry.source_title}">
    </head><body>
    <p>Release date: {human_date}</p>
    <a title="Download publication" href="{uri}">PDF</a>
    </body></html>""".encode()

    _, _, _, links = _parse_release_landing(
        _snapshot(_release_request(entry), content), entry
    )

    assert archive._product_code(uri) == code
    assert links == (uri,)


def test_dataset_parser_decodes_only_source_supported_coordinates() -> None:
    request = build_eurostat_international_trade_goods_dataset_request(
        load_packaged_official_source_registry()
    )
    parsed = parse_eurostat_international_trade_goods_dataset(
        _snapshot(request, _dataset_payload())
    )

    assert len(parsed.observations) == 12
    assert {item.measure for item in parsed.observations} == {
        EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_EXPORTS,
        EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_IMPORTS,
        EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_INTRA_TRADE,
        EurostatInternationalTradeGoodsMeasure.SEASONAL_EXPORTS,
        EurostatInternationalTradeGoodsMeasure.SEASONAL_IMPORTS,
        EurostatInternationalTradeGoodsMeasure.SEASONAL_INTRA_TRADE,
    }
    assert parsed.observations[-1].status is None
    assert any(item.status == "p" for item in parsed.observations)
    assert {item.unit for item in parsed.observations} == {"million-euro"}
    assert parsed == type(parsed).from_dict(parsed.to_dict())

    with pytest.raises(ValueError, match="cube coverage differs"):
        parse_eurostat_international_trade_goods_dataset(
            _snapshot(request, _dataset_payload(omit_coordinate=True))
        )
    changed_unit = json.loads(_dataset_payload())
    changed_unit["dimension"]["indic_et"]["category"]["label"][
        "TRD_VAL"
    ] = "Trade value in euro"
    with pytest.raises(ValueError, match="labels or units differ"):
        parse_eurostat_international_trade_goods_dataset(
            _snapshot(
                request,
                json.dumps(changed_unit, separators=(",", ":")).encode(),
            )
        )
    with pytest.raises(ValueError, match="observation unit differs"):
        replace(
            parsed.observations[0],
            unit="billion-euro",
            observation_id="",
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
            "Euro area international trade in goods surplus 10.0 bn euro",
            EurostatInternationalTradeGoodsMovement.SURPLUS,
            10.0,
        ),
        (
            "Euro area international trade in goods deficit 2.1 bn euro",
            EurostatInternationalTradeGoodsMovement.DEFICIT,
            -2.1,
        ),
    ),
)
def test_headline_preserves_balance_sign(
    title: str, movement: EurostatInternationalTradeGoodsMovement, value: float
) -> None:
    parsed = _headline_value(_entry(title=title), "2012-04")

    assert (
        parsed.measure
        is EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_BALANCE
    )
    assert parsed.unit == "billion-euro"
    assert parsed.period_scope == "month"
    assert parsed.movement is movement
    assert parsed.value == value


def test_december_headline_keeps_year_to_date_scope() -> None:
    parsed = _headline_value(
        _entry(
            title="Euro area international trade in goods surplus 42.0 bn euro"
        ),
        "2012-12",
    )

    assert parsed.period_scope == "year-to-date"
    assert parsed.value == 42.0


def test_named_trade_rows_retain_all_eight_source_measures() -> None:
    entry = _entry()
    snapshot = _snapshot(_release_request(entry), _trade_html())
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML,
        product_code=entry.product_code,
    )
    headline = _headline_value(entry, "2012-04")

    values = _published_values(
        snapshot,
        artifact,
        None,
        None,
        "2012-04",
        headline,
    )

    assert len(values) == 16
    assert {item.unit for item in values} == {"billion-euro"}
    with pytest.raises(ValueError, match="published-value unit differs"):
        replace(values[0], unit="million-euro", published_value_id="")
    assert {item.measure for item in values} == set(
        EurostatInternationalTradeGoodsMeasure
    )
    assert {
        item.reference_period
        for item in values
        if item.measure.value.startswith("non-seasonally")
    } == {"2011-04", "2012-04"}
    assert {
        item.reference_period
        for item in values
        if item.measure.value.startswith("seasonally")
    } == {"2012-03", "2012-04"}
    assert (
        next(
            item
            for item in values
            if item.reference_period == "2012-04"
            and item.measure
            is EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_BALANCE
        ).value
        == 10.0
    )
    changed_unit_snapshot = _snapshot(
        _release_request(entry),
        _trade_html().replace(b"bn euro", b"million euro"),
    )
    changed_unit_artifact = _artifact_from_snapshot(
        changed_unit_snapshot,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML,
        product_code=entry.product_code,
    )
    with pytest.raises(ValueError, match="table unit differs"):
        _published_values(
            changed_unit_snapshot,
            changed_unit_artifact,
            None,
            None,
            "2012-04",
            headline,
        )


@pytest.mark.parametrize(
    ("code", "evidence", "missing_seasonal_annex", "expected_count"),
    (
        (
            "6-14022014-bp",
            "Seasonally adjusted data are not available in this News Release.",
            True,
            8,
        ),
        (
            "6-18032014-ap",
            (
                "Seasonally adjusted data for January 2014 for the EA18 are "
                "not available."
            ),
            False,
            14,
        ),
        (
            "6-18032015-ap",
            "Seasonally adjusted data were not available in time for this release.",
            True,
            8,
        ),
    ),
)
def test_source_stated_unavailable_trade_rows_remain_absent(
    code: str,
    evidence: str,
    missing_seasonal_annex: bool,
    expected_count: int,
) -> None:
    entry = _entry(code=code, release_date=archive._product_release_date(code))
    content = _trade_html()
    if missing_seasonal_annex:
        content = (
            content.split(b"<h2>Annex - Seasonally adjusted data</h2>")[0]
            + b"</body></html>"
        )
    else:
        content = content.replace(
            b"<p>Intra-EA17 trade 91.0 96.0</p>",
            b"<p>Intra-EA17 trade : :</p>",
        )
    content = content.replace(b"</body>", f"<p>{evidence}</p></body>".encode())
    snapshot = _snapshot(_release_request(entry), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML,
        product_code=entry.product_code,
    )

    values = archive._trade_source_published_values(
        snapshot, artifact, "2012-04"
    )

    assert len(values) == expected_count
    if missing_seasonal_annex:
        assert all(
            not item.measure.value.startswith("seasonally") for item in values
        )
    else:
        assert (
            EurostatInternationalTradeGoodsMeasure.SEASONAL_INTRA_TRADE
            not in {item.measure for item in values}
        )


def test_bare_ea_table_rows_stop_before_source_and_release_prose() -> None:
    entry = _entry()
    content = _trade_html().replace(
        b"previous three months",
        b"legacy twelve-month table",
    )
    content = content.replace(b"EA17", b"EA")
    content = content.replace(
        b"</body>",
        (
            b"<p>Source dataset: ext_st_ea19sitc -35 -30 2015</p>"
            b"<p>In February 2015, the euro area recorded a surplus.</p></body>"
        ),
    )
    snapshot = _snapshot(_release_request(entry), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML,
        product_code=entry.product_code,
    )

    values = archive._trade_source_published_values(
        snapshot, artifact, "2012-04"
    )

    assert [
        item.value
        for item in values
        if item.measure
        is EurostatInternationalTradeGoodsMeasure.SEASONAL_INTRA_TRADE
    ] == [91.0, 96.0]


@pytest.mark.parametrize(
    ("reference_period", "composition"),
    (
        ("2012-04", "EA17"),
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


def test_revision_counts_do_not_cross_euro_area_compositions() -> None:
    def value(
        numeric: float,
    ) -> EurostatInternationalTradeGoodsPublishedValueV1:
        return EurostatInternationalTradeGoodsPublishedValueV1(
            economy_code="EA",
            reference_period="2012-04",
            measure=EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_EXPORTS,
            unit="billion-euro",
            value_lexical=str(numeric),
            value=numeric,
            evidence_artifact_sha256="0" * 64,
            evidence_locator="test table",
        )

    releases = (
        SimpleNamespace(
            euro_area_composition="EA17", published_values=(value(1.0),)
        ),
        SimpleNamespace(
            euro_area_composition="EA17", published_values=(value(2.0),)
        ),
        SimpleNamespace(
            euro_area_composition="EA18", published_values=(value(3.0),)
        ),
    )

    assert _published_revision_counts(releases) == (1, 1)


def test_current_dataset_comparisons_scale_units_and_require_ea21() -> None:
    def published(
        measure: EurostatInternationalTradeGoodsMeasure, numeric: float
    ) -> EurostatInternationalTradeGoodsPublishedValueV1:
        return EurostatInternationalTradeGoodsPublishedValueV1(
            economy_code="EA",
            reference_period="2026-01",
            measure=measure,
            unit="billion-euro",
            value_lexical=str(numeric),
            value=numeric,
            evidence_artifact_sha256="0" * 64,
            evidence_locator="test table",
        )

    exports = EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_EXPORTS
    imports = EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_IMPORTS
    releases = (
        SimpleNamespace(
            euro_area_composition="EA20",
            published_values=(published(exports, 99.0),),
        ),
        SimpleNamespace(
            euro_area_composition="EA21",
            published_values=(
                published(exports, 10.0),
                published(imports, 9.0),
            ),
        ),
    )
    dataset = SimpleNamespace(
        observations=(
            SimpleNamespace(
                economy_code="EA21",
                reference_period="2026-01",
                measure=exports,
                value=10_000.0,
            ),
            SimpleNamespace(
                economy_code="EA21",
                reference_period="2026-01",
                measure=imports,
                value=10_000.0,
            ),
        )
    )

    assert _current_dataset_comparison_counts(releases, dataset) == (2, 1)


def test_public_package_exports_the_archive_contract() -> None:
    assert (
        market_context.EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY
        == EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY
    )
    assert (
        market_context.load_packaged_eurostat_international_trade_goods_archive_manifest
        is load_packaged_eurostat_international_trade_goods_archive_manifest
    )
