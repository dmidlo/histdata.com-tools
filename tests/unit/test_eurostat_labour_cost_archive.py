"""Qualification tests for the Eurostat quarterly labour-cost archive."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from hashlib import sha256

import pytest

import histdatacom.market_context.eurostat_labour_cost_archive as archive
from histdatacom.market_context.eurostat_labour_cost_archive import (
    EUROSTAT_LABOUR_COST_ACTIVITY_SCOPE_BREAK_REFERENCE_PERIOD,
    EUROSTAT_LABOUR_COST_AS_OF_DATE,
    EUROSTAT_LABOUR_COST_DATASET_URI,
    EUROSTAT_LABOUR_COST_DOCUMENT_COUNT,
    EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD,
    EUROSTAT_LABOUR_COST_FIRST_SEARCH_RELEASE_DATE,
    EUROSTAT_LABOUR_COST_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_LABOUR_COST_LATEST_REFERENCE_PERIOD,
    EUROSTAT_LABOUR_COST_MEASURE_KEY,
    EUROSTAT_LABOUR_COST_PAGE_DATE_OFFSET_EXCEPTIONS,
    EUROSTAT_LABOUR_COST_PARSER_ID,
    EUROSTAT_LABOUR_COST_PARSER_VERSION,
    EUROSTAT_LABOUR_COST_PROGRAM_KEY,
    EUROSTAT_LABOUR_COST_RELEASE_COUNT,
    EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT,
    EUROSTAT_LABOUR_COST_SEARCH_RESULT_COUNT,
    EUROSTAT_LABOUR_COST_SEARCH_UNIQUE_URI_COUNT,
    EUROSTAT_LABOUR_COST_SOURCE_KEY,
    EurostatLabourCostActivityScope,
    EurostatLabourCostArchiveManifestV1,
    EurostatLabourCostArtifactRole,
    EurostatLabourCostInventoryEntryV1,
    EurostatLabourCostMeasure,
    EurostatLabourCostMovement,
    _activity_scope_for_period,
    _artifact_from_snapshot,
    _composition_from_publication,
    _ea_composition_for_period,
    _headline_value,
    _inventory_prefilter,
    _labour_cost_html_table_values,
    _labour_cost_pdf_page_values,
    _labour_cost_source_table_values,
    _parse_release_landing,
    _product_release_date,
    _published_revision_counts,
    _quarter_range,
    _reference_period_from_publication,
    build_eurostat_labour_cost_dataset_request,
    build_eurostat_labour_cost_release_requests,
    build_eurostat_labour_cost_search_requests,
    load_packaged_eurostat_labour_cost_archive_manifest,
    packaged_eurostat_labour_cost_manifest_path,
    parse_eurostat_labour_cost_dataset,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)


def _request(
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_LABOUR_COST_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        page_number=page_number,
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
    product_code: str = "3-16092026-bp",
    release_date: str = "2026-09-16",
    title: str = "Annual increase in labour costs at 3.1% in euro area",
    summary: str = (
        "In the second quarter of 2026 hourly labour costs rose by 3.1% "
        "in the euro area (EA21)."
    ),
) -> EurostatLabourCostInventoryEntryV1:
    return EurostatLabourCostInventoryEntryV1(
        product_code=product_code,
        release_date=release_date,
        source_title=title,
        source_uri=(
            f"https://ec.europa.eu/eurostat/product?code={product_code}"
        ),
        atom_published_at=f"{release_date}T09:00:00Z",
        atom_updated_at=f"{release_date}T09:00:00Z",
        summary=summary,
        page_number=1,
    )


def _release_snapshot(content: bytes) -> OfficialRawSnapshotV1:
    entry = _entry()
    return _snapshot(
        _request(entry.source_uri, OfficialSourceFormat.HTML),
        content,
        resolved_uri=(
            "https://ec.europa.eu/eurostat/web/products-euro-indicators/w/"
            f"{entry.product_code}"
        ),
    )


def _release_artifact(content: bytes) -> archive.EurostatLabourCostArtifactV1:
    return _artifact_from_snapshot(
        _release_snapshot(content),
        EurostatLabourCostArtifactRole.RELEASE_HTML,
        product_code=_entry().product_code,
    )


def _dataset_payload(*, adjusted_code: str = "CA") -> bytes:
    dimensions = {
        "freq": ("Q",),
        "s_adj": (adjusted_code,),
        "unit": ("PCH_SM",),
        "nace_r2": ("B-S",),
        "lcstruct": ("D1_D4_MD5", "D11", "D12_D4_MD5"),
        "geo": ("EA21", "DE", "FR"),
        "time": ("2010-Q1",),
    }
    return json.dumps(
        {
            "class": "dataset",
            "label": "Labour cost index by NACE Rev. 2 activity - quarterly data",
            "updated": "2026-09-17T11:00:00+0200",
            "id": list(dimensions),
            "size": [len(codes) for codes in dimensions.values()],
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
            "value": {str(index): index / 10 for index in range(9)},
            "status": {"0": "p"},
        },
        separators=(",", ":"),
    ).encode()


def _modern_table_html() -> bytes:
    return b"""<html><head>
    <meta property="og:title" content="Annual increase in labour costs at 3.1% in euro area">
    <meta property="article:published_time" content="2026-09-16T11:00:00+02:00">
    </head><body><p>Release date: 16 September 2026</p>
    <table>
      <tr><th colspan="10">Nominal hourly labour costs of whole economy
      % change compared with same quarter of previous year - calendar adjusted</th></tr>
      <tr><th></th><th colspan="3">Q2 2025</th><th colspan="3">Q1 2026</th><th colspan="3">Q2 2026</th></tr>
      <tr><th></th><th>TOTAL</th><th>WAGES</th><th>OTHER</th><th>TOTAL</th><th>WAGES</th><th>OTHER</th><th>TOTAL</th><th>WAGES</th><th>OTHER</th></tr>
      <tr><th>Euro area 21</th><td>4.1</td><td>4.1</td><td>4.2</td><td>3.3</td><td>3.4</td><td>2.8</td><td>3.1</td><td>3.0</td><td>3.2</td></tr>
      <tr><th>Germany</th><td>4.3</td><td>4.5</td><td>3.6</td><td>2.9</td><td>3.3</td><td>1.6</td><td>3.0</td><td>2.9</td><td>3.3</td></tr>
      <tr><th>France</th><td>2.1</td><td>1.7</td><td>3.0</td><td>1.9</td><td>1.9</td><td>1.9</td><td>2.1</td><td>2.1</td><td>2.2</td></tr>
    </table>
    <table><tr><th colspan="10">Nominal hourly labour costs of business economy
    % change compared with same quarter of previous year</th></tr>
    <tr><th></th><th colspan="3">Q2 2025</th><th colspan="3">Q1 2026</th><th colspan="3">Q2 2026</th></tr>
    <tr><th></th><th>TOTAL</th><th>WAGES</th><th>OTHER</th><th>TOTAL</th><th>WAGES</th><th>OTHER</th><th>TOTAL</th><th>WAGES</th><th>OTHER</th></tr>
    <tr><th>Euro area 21</th><td>9.9</td><td>9.9</td><td>9.9</td><td>9.9</td><td>9.9</td><td>9.9</td><td>9.9</td><td>9.9</td><td>9.9</td></tr></table>
    </body></html>"""


def _manifest() -> EurostatLabourCostArchiveManifestV1:
    return load_packaged_eurostat_labour_cost_archive_manifest()


def test_packaged_archive_quantifies_the_complete_corpus() -> None:
    manifest = _manifest()
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_LABOUR_COST_SOURCE_KEY)
    path = packaged_eurostat_labour_cost_manifest_path()

    assert EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT == 2
    assert EUROSTAT_LABOUR_COST_SEARCH_RESULT_COUNT == 159
    assert EUROSTAT_LABOUR_COST_SEARCH_UNIQUE_URI_COUNT == 159
    assert EUROSTAT_LABOUR_COST_RELEASE_COUNT == 100
    assert EUROSTAT_LABOUR_COST_FIRST_SEARCH_RELEASE_DATE == "2002-01-09"
    assert EUROSTAT_LABOUR_COST_LATEST_PACKAGED_RELEASE_DATE == "2026-09-16"
    assert EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD == "2001-Q3"
    assert EUROSTAT_LABOUR_COST_LATEST_REFERENCE_PERIOD == "2026-Q2"
    assert manifest.as_of_date == EUROSTAT_LABOUR_COST_AS_OF_DATE
    assert manifest.registry_id == registry.registry_id
    assert manifest.source_id == source.source_id
    assert manifest.inventory.query_result_count == 159
    assert manifest.inventory.unique_uri_count == 159
    assert manifest.inventory.title_match_count == 111
    assert manifest.inventory.prefilter_count == 100
    assert len(manifest.inventory.entries) == 100
    assert len(manifest.inventory.exclusions) == 59
    assert len(manifest.inventory.artifacts) == 2
    assert manifest.release_count == 100
    assert manifest.reference_period_inferred_count == 0
    assert manifest.composition_source_stated_count == 79
    assert manifest.page_date_offset_count == 11
    assert manifest.exact_publication_time_count == 16
    assert (
        manifest.release_document_count == EUROSTAT_LABOUR_COST_DOCUMENT_COUNT
    )
    assert manifest.published_value_count == 4_399
    assert manifest.revision_comparison_count == 3_322
    assert manifest.changed_revision_count == 1_978
    assert manifest.current_dataset_comparison_count == 2_433
    assert manifest.changed_current_dataset_count == 2_236
    assert manifest.raw_artifact_count == 192
    assert manifest.unique_content_sha256_count == 192
    assert manifest.total_content_bytes == 51_057_206
    assert manifest.current_dataset.dataset_id == "lc_lci_r2_q"
    assert len(manifest.current_dataset.observations) == 726
    assert manifest.current_dataset.time_start == "2000-Q1"
    assert manifest.current_dataset.time_end == "2026-Q2"
    assert manifest.searchable_release_gap_reference_periods == (
        "2000-Q1",
        "2000-Q2",
        "2000-Q3",
        "2000-Q4",
        "2001-Q1",
        "2001-Q2",
    )
    assert manifest.manifest_id == (
        "eurostat-labour-cost-archive-manifest:sha256:"
        "b670ff832615e49c9c0c6f75aecf9e6713e7c8b202c08921200a3f55182e9df2"
    )
    assert path.stat().st_size == 2_961_196
    assert sha256(path.read_bytes()).hexdigest() == (
        "95fb1d2eeccdf0ee3fd7dece0ce61d28a5226abfc95fa1b283938699b2e025a7"
    )


def test_packaged_selection_and_release_scope_stay_explicit() -> None:
    manifest = _manifest()

    assert Counter(item.reason for item in manifest.inventory.exclusions) == {
        "agricultural-income release, not the quarterly labour-cost index": 16,
        "annual labour-cost-level article, not the quarterly index release": 10,
        "macro-imbalance summary, not the quarterly labour-cost index": 10,
        "broad search match outside the quarterly labour-cost-index lineage": 9,
        "tax-statistics release, not the quarterly labour-cost index": 9,
        "business-statistics article, not the quarterly labour-cost index": 4,
        "sector-level labour-cost article, not the quarterly index release": 1,
    }
    assert tuple(item.reference_period for item in manifest.releases) == (
        _quarter_range("2001-Q3", "2026-Q2")
    )
    assert all(
        item.headline_value.measure is EurostatLabourCostMeasure.TOTAL
        for item in manifest.releases
    )
    assert {item.euro_area_composition for item in manifest.releases} == {
        "EA12",
        "EA13",
        "EA15",
        "EA16",
        "EA17",
        "EA18",
        "EA19",
        "EA20",
        "EA21",
    }
    assert Counter(item.activity_scope for item in manifest.releases) == {
        EurostatLabourCostActivityScope.BUSINESS_ECONOMY: 43,
        EurostatLabourCostActivityScope.WHOLE_ECONOMY: 57,
    }
    assert Counter(
        len(item.published_values) for item in manifest.releases
    ) == {
        1: 1,
        24: 6,
        27: 11,
        42: 1,
        45: 71,
        72: 10,
    }
    headline_only = [
        item for item in manifest.releases if len(item.published_values) == 1
    ]
    assert [item.inventory_entry.product_code for item in headline_only] == [
        "3-12122008-ap"
    ]
    assert Counter(
        (
            item.document_artifact.source_format.value
            if item.document_artifact is not None
            else "self-contained"
        )
        for item in manifest.releases
    ) == {"pdf": 78, "html": 11, "self-contained": 11}


def test_packaged_page_date_offsets_are_exactly_scoped() -> None:
    manifest = _manifest()

    assert EUROSTAT_LABOUR_COST_PAGE_DATE_OFFSET_EXCEPTIONS == {
        "3-09012002-ap": -1,
        "3-22032002-ap": -1,
        "3-26062002-ap": -1,
        "3-25092002-ap": -1,
        "3-16122002-ap": -1,
        "3-19032003-bp": -1,
        "3-19062003-bp": -1,
        "3-19092003-ap": -1,
        "3-18122003-ap": -1,
        "3-18032004-ap": -1,
        "3-16062004-bp": -1,
    }
    assert {
        item.inventory_entry.product_code: item.page_release_date_offset_days
        for item in manifest.releases
        if item.page_release_date_offset_days
    } == EUROSTAT_LABOUR_COST_PAGE_DATE_OFFSET_EXCEPTIONS


def test_packaged_archive_round_trips_and_detects_tampering() -> None:
    manifest = _manifest()

    assert (
        EurostatLabourCostArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    with pytest.raises(ValueError, match="identity differs"):
        replace(
            manifest,
            manifest_id=(
                "eurostat-labour-cost-archive-manifest:sha256:" + "0" * 64
            ),
        )


def test_registry_contract_and_requests_are_frozen() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_LABOUR_COST_SOURCE_KEY)
    manifest = _manifest()

    assert source.parser_id == EUROSTAT_LABOUR_COST_PARSER_ID
    assert source.parser_version == EUROSTAT_LABOUR_COST_PARSER_VERSION
    assert (
        EUROSTAT_LABOUR_COST_PROGRAM_KEY == "ea.eurostat.quarterly-labour-cost"
    )
    assert EUROSTAT_LABOUR_COST_MEASURE_KEY == "labour-cost-index-annual-change"
    assert source.formats == (
        OfficialSourceFormat.ATOM,
        OfficialSourceFormat.JSON_STAT,
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    )
    assert len(build_eurostat_labour_cost_search_requests(registry)) == 2
    dataset_request = build_eurostat_labour_cost_dataset_request(registry)
    assert dataset_request.uri == EUROSTAT_LABOUR_COST_DATASET_URI
    assert dataset_request.source_format is OfficialSourceFormat.JSON_STAT
    release_requests = build_eurostat_labour_cost_release_requests(
        registry, manifest.inventory
    )
    assert len(release_requests) == 100
    assert release_requests[0].uri.endswith("code=3-09012002-ap")
    assert release_requests[-1].uri.endswith("code=3-16092026-bp")


@pytest.mark.parametrize(
    ("title", "selected"),
    (
        ("Euro-zone labour costs up by 3.3%", True),
        ("Euro area hourly labour costs rose by 2.2%", True),
        ("Annual growth in labour costs at 2.4% in euro area", True),
        ("Annual increase in labour costs at 3.1% in euro area", True),
        ("Hourly labour costs ranged from 8 to 50 euro", False),
        ("Agricultural income rose by 4%", False),
    ),
)
def test_inventory_prefilter_is_narrow(title: str, selected: bool) -> None:
    assert _inventory_prefilter(title) is selected


def test_product_dates_accept_historical_suffixes() -> None:
    assert _product_release_date("3-09012002-ap") == "2002-01-09"
    assert _product_release_date("3-16062006-ap1") == "2006-06-16"
    assert _product_release_date("3-16062017-cp") == "2017-06-16"
    with pytest.raises(ValueError, match="product code"):
        _product_release_date("3-2026-09-16")


def test_dataset_parser_preserves_sparse_coordinates_and_lexical_values() -> (
    None
):
    request = build_eurostat_labour_cost_dataset_request(
        load_packaged_official_source_registry()
    )
    dataset = parse_eurostat_labour_cost_dataset(
        _snapshot(request, _dataset_payload())
    )

    assert len(dataset.observations) == 9
    assert dataset.time_start == dataset.time_end == "2010-Q1"
    assert dataset.observations[0].economy_code == "EA21"
    assert dataset.observations[0].measure is EurostatLabourCostMeasure.TOTAL
    assert dataset.observations[0].value_lexical == "0.0"
    assert dataset.observations[0].status == "p"
    assert dataset.observations[-1].flat_index == 8


def test_dataset_parser_rejects_an_unqualified_coordinate() -> None:
    request = build_eurostat_labour_cost_dataset_request(
        load_packaged_official_source_registry()
    )
    with pytest.raises(ValueError, match="dimensions or codes differ"):
        parse_eurostat_labour_cost_dataset(
            _snapshot(request, _dataset_payload(adjusted_code="SA"))
        )


def test_release_landing_binds_atom_title_date_timestamp_and_document() -> None:
    content = b"""<html><head>
    <meta property="og:title" content="Annual increase in labour costs at 3.1% in euro area">
    <meta property="article:published_time" content="2026-09-16T11:00:00+02:00">
    </head><body><p>Release date: 16 September 2026</p>
    <a title="Download publication (EN)" href="/eurostat/documents/1/2/3-16092026-BP-EN.PDF.pdf/hash">EN</a>
    </body></html>"""

    title, page_date, published_at, links = _parse_release_landing(
        _release_snapshot(content), _entry()
    )

    assert title == _entry().source_title
    assert page_date == "2026-09-16"
    assert published_at == "2026-09-16T11:00:00+02:00"
    assert links == (
        "https://ec.europa.eu/eurostat/documents/1/2/3-16092026-BP-EN.PDF.pdf/hash",
    )


def test_reference_period_prefers_source_wording_and_checks_lag() -> None:
    snapshot = _release_snapshot(_modern_table_html())

    period, lag = _reference_period_from_publication(_entry(), snapshot, None)

    assert period == "2026-Q2"
    assert lag == 78


@pytest.mark.parametrize(
    ("period", "composition"),
    (
        ("2006-Q4", "EA12"),
        ("2007-Q1", "EA13"),
        ("2008-Q1", "EA15"),
        ("2009-Q1", "EA16"),
        ("2011-Q1", "EA17"),
        ("2014-Q1", "EA18"),
        ("2015-Q1", "EA19"),
        ("2023-Q1", "EA20"),
        ("2026-Q1", "EA21"),
    ),
)
def test_composition_follows_the_reference_quarter(
    period: str, composition: str
) -> None:
    assert _ea_composition_for_period(period) == composition


def test_activity_scope_preserves_the_2012_methodological_break() -> None:
    assert (
        EUROSTAT_LABOUR_COST_ACTIVITY_SCOPE_BREAK_REFERENCE_PERIOD == "2012-Q2"
    )
    assert (
        _activity_scope_for_period("2012-Q1")
        is EurostatLabourCostActivityScope.BUSINESS_ECONOMY
    )
    assert (
        _activity_scope_for_period("2012-Q2")
        is EurostatLabourCostActivityScope.WHOLE_ECONOMY
    )


def test_composition_requires_any_stated_scope_to_match() -> None:
    snapshot = _release_snapshot(
        _modern_table_html().replace(b"Euro area 21", b"Euro area 20")
    )
    with pytest.raises(ValueError, match="composition differs"):
        _composition_from_publication("2026-Q2", snapshot, None)


def test_headline_retains_true_decrease_sign_only() -> None:
    decrease = _entry(
        product_code="3-15092021-bp",
        release_date="2021-09-15",
        title="Annual decrease in labour costs at 0.1% in euro area",
        summary="In the second quarter of 2021 labour costs decreased.",
    )
    down = _headline_value(decrease, "2021-Q2")
    reported = _headline_value(_entry(), "2026-Q2")

    assert down.movement is EurostatLabourCostMovement.DOWN
    assert down.value == -0.1
    assert reported.movement is EurostatLabourCostMovement.REPORTED
    assert reported.value == 3.1


def test_html_parser_selects_only_the_whole_economy_table() -> None:
    content = _modern_table_html()
    values = _labour_cost_html_table_values(
        _release_snapshot(content), _release_artifact(content), "2026-Q2"
    )

    assert len(values) == 27
    assert {
        (item.economy_code, item.reference_period, item.measure, item.value)
        for item in values
        if item.reference_period == "2026-Q2"
    } == {
        ("EA", "2026-Q2", EurostatLabourCostMeasure.TOTAL, 3.1),
        ("EA", "2026-Q2", EurostatLabourCostMeasure.WAGES, 3.0),
        ("EA", "2026-Q2", EurostatLabourCostMeasure.OTHER, 3.2),
        ("DE", "2026-Q2", EurostatLabourCostMeasure.TOTAL, 3.0),
        ("DE", "2026-Q2", EurostatLabourCostMeasure.WAGES, 2.9),
        ("DE", "2026-Q2", EurostatLabourCostMeasure.OTHER, 3.3),
        ("FR", "2026-Q2", EurostatLabourCostMeasure.TOTAL, 2.1),
        ("FR", "2026-Q2", EurostatLabourCostMeasure.WAGES, 2.1),
        ("FR", "2026-Q2", EurostatLabourCostMeasure.OTHER, 2.2),
    }
    assert all(item.value != 9.9 for item in values)


def test_legacy_text_flow_tables_retain_three_components() -> None:
    content = b"""<html><head>
    <meta property="og:title" content="Annual increase in labour costs at 3.1% in euro area">
    </head><body><p>Table 1. Total nominal hourly labour costs, whole economy
    % change compared to same quarter a year earlier Q1-25 Q2-25 Q3-25 Q4-25 Q1-26 Q2-26
    Euro area 21 2.1 2.2 2.3 2.4 2.5 3.1 EU 2 2 2 2 2 2
    D 1.1 1.2 1.3 1.4 1.5 3.0 F 0.1 0.2 0.3 0.4 0.5 2.1
    Table 2. Total nominal hourly labour costs, industry
    Table 3: Breakdown of total nominal hourly labour costs, whole economy
    Q1-25 Q2-25 Q3-25 Q4-25 Q1-26 Q2-26
    Euro area 21 Wages 2.0 2.1 2.2 2.3 2.4 3.0 Other 2.2 2.3 2.4 2.5 2.6 3.2 Total 2.1 2.2 2.3 2.4 2.5 3.1
    D Wages 1.0 1.1 1.2 1.3 1.4 2.9 Other 1.2 1.3 1.4 1.5 1.6 3.3 Total 1.1 1.2 1.3 1.4 1.5 3.0
    F Wages 0.0 0.1 0.2 0.3 0.4 2.1 Other 0.2 0.3 0.4 0.5 0.6 2.2 Total 0.1 0.2 0.3 0.4 0.5 2.1
    Further information</p></body></html>"""

    values = _labour_cost_source_table_values(
        _release_snapshot(content), _release_artifact(content), "2026-Q2"
    )

    assert len(values) == 54
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2026-Q2"
    } == {
        ("EA", EurostatLabourCostMeasure.TOTAL, 3.1),
        ("EA", EurostatLabourCostMeasure.WAGES, 3.0),
        ("EA", EurostatLabourCostMeasure.OTHER, 3.2),
        ("DE", EurostatLabourCostMeasure.TOTAL, 3.0),
        ("DE", EurostatLabourCostMeasure.WAGES, 2.9),
        ("DE", EurostatLabourCostMeasure.OTHER, 3.3),
        ("FR", EurostatLabourCostMeasure.TOTAL, 2.1),
        ("FR", EurostatLabourCostMeasure.WAGES, 2.1),
        ("FR", EurostatLabourCostMeasure.OTHER, 2.2),
    }


def test_pdf_text_parser_preserves_period_component_and_economy_axes() -> None:
    artifact = replace(
        _release_artifact(_modern_table_html()),
        role=EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_PDF,
        source_format=OfficialSourceFormat.PDF,
    )
    page = """Nominal hourly labour costs of whole economy
    % change compared with same quarter of previous year - calendar adjusted
    Q2 2025 Q1 2026 Q2 2026
    TOTAL WAGES OTHER TOTAL WAGES OTHER TOTAL WAGES OTHER
    Euro area 21 4.1 4.1 4.2 3.3 3.4 2 .8 3.1 3.0 3.2
    Germany 4.3 4.5 3.6 2.9 3.3 1.6 3.0 2.9 3.3
    France 2.1 1.7 3.0 1.9 1.9 1.9 2.1 2.1 2.2
    Nominal hourly labour costs of business economy"""

    values = _labour_cost_pdf_page_values(page, artifact, "2026-Q2", 4)

    assert len(values) == 27
    assert (
        next(
            item
            for item in values
            if item.economy_code == "EA"
            and item.reference_period == "2026-Q2"
            and item.measure is EurostatLabourCostMeasure.TOTAL
        ).value
        == 3.1
    )
    assert all("PDF page 4" in item.evidence_locator for item in values)


def test_revision_counts_do_not_compare_different_euro_area_compositions() -> (
    None
):
    manifest = _manifest()
    releases = tuple(
        item
        for item in manifest.releases
        if item.reference_period in {"2022-Q4", "2023-Q1"}
    )

    comparisons, changes = _published_revision_counts(releases)

    assert 0 <= changes <= comparisons
    assert comparisons == sum(
        max(0, count - 1)
        for count in Counter(
            (
                value.reference_period,
                (
                    release.euro_area_composition
                    if value.economy_code == "EA"
                    else value.economy_code
                ),
                release.activity_scope,
                value.measure,
            )
            for release in releases
            for value in release.published_values
        ).values()
    )
