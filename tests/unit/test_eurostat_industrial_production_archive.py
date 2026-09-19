"""Qualification tests for Eurostat monthly industrial production."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest
from pypdf.errors import PdfReadError

import histdatacom.market_context.eurostat_industrial_production_archive as archive
from histdatacom.market_context.eurostat_industrial_production_archive import (
    EUROSTAT_INDUSTRIAL_PRODUCTION_AS_OF_DATE,
    EUROSTAT_INDUSTRIAL_PRODUCTION_DATASET_URI,
    EUROSTAT_INDUSTRIAL_PRODUCTION_DOCUMENT_COUNT,
    EUROSTAT_INDUSTRIAL_PRODUCTION_FIRST_REFERENCE_PERIOD,
    EUROSTAT_INDUSTRIAL_PRODUCTION_FIRST_SEARCH_RELEASE_DATE,
    EUROSTAT_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_INDUSTRIAL_PRODUCTION_LATEST_REFERENCE_PERIOD,
    EUROSTAT_INDUSTRIAL_PRODUCTION_MEASURE_KEY,
    EUROSTAT_INDUSTRIAL_PRODUCTION_PAGE_DATE_OFFSET_EXCEPTIONS,
    EUROSTAT_INDUSTRIAL_PRODUCTION_PARSER_ID,
    EUROSTAT_INDUSTRIAL_PRODUCTION_PARSER_VERSION,
    EUROSTAT_INDUSTRIAL_PRODUCTION_PERIOD_HEADER_CORRECTIONS,
    EUROSTAT_INDUSTRIAL_PRODUCTION_PROGRAM_KEY,
    EUROSTAT_INDUSTRIAL_PRODUCTION_REFERENCE_PERIOD_INFERENCES,
    EUROSTAT_INDUSTRIAL_PRODUCTION_RELEASE_COUNT,
    EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_PAGE_COUNT,
    EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_RESULT_COUNT,
    EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_UNIQUE_URI_COUNT,
    EUROSTAT_INDUSTRIAL_PRODUCTION_SOURCE_KEY,
    EurostatIndustrialProductionArchiveManifestV1,
    EurostatIndustrialProductionArtifactRole,
    EurostatIndustrialProductionInventoryEntryV1,
    EurostatIndustrialProductionMeasure,
    EurostatIndustrialProductionMovement,
    EurostatIndustrialProductionPublishedValueV1,
    _artifact_from_snapshot,
    _composition_from_publication,
    _ea_composition_for_period,
    _headline_value,
    _industrial_pdf_page_values,
    _industrial_pdf_table_values,
    _industrial_table_published_values,
    _inventory_prefilter,
    _parse_release_landing,
    _product_release_date,
    _published_revision_counts,
    _reference_period_from_publication,
    build_eurostat_industrial_production_dataset_request,
    build_eurostat_industrial_production_release_requests,
    build_eurostat_industrial_production_search_requests,
    load_packaged_eurostat_industrial_production_archive_manifest,
    packaged_eurostat_industrial_production_manifest_path,
    parse_eurostat_industrial_production_dataset,
    parse_eurostat_industrial_production_inventory,
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


def _entry() -> EurostatIndustrialProductionInventoryEntryV1:
    return EurostatIndustrialProductionInventoryEntryV1(
        product_code="4-21012002-ap",
        release_date="2002-01-21",
        source_title="Industrial production down by 0.8% in euro-zone",
        source_uri="https://ec.europa.eu/eurostat/product?code=4-21012002-ap",
        atom_published_at="2002-01-20T22:00:00Z",
        atom_updated_at="2002-01-20T22:00:00Z",
        summary="In November 2001, industrial production fell by 0.8%.",
        page_number=1,
    )


def _document_request() -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_INDUSTRIAL_PRODUCTION_SOURCE_KEY
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
        EUROSTAT_INDUSTRIAL_PRODUCTION_SOURCE_KEY
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
        "indic_bt": ("PRD",),
        "nace_r2": ("B-D",),
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
            "label": "Industrial production - monthly data",
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


def _manifest() -> EurostatIndustrialProductionArchiveManifestV1:
    return load_packaged_eurostat_industrial_production_archive_manifest()


def test_packaged_archive_quantifies_the_complete_corpus() -> None:
    manifest = _manifest()
    path = packaged_eurostat_industrial_production_manifest_path()

    assert EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_PAGE_COUNT == 13
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_RESULT_COUNT == 1_213
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_UNIQUE_URI_COUNT == 1_212
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_RELEASE_COUNT == 297
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_DOCUMENT_COUNT == 262
    assert (
        EUROSTAT_INDUSTRIAL_PRODUCTION_FIRST_SEARCH_RELEASE_DATE == "2002-01-21"
    )
    assert (
        EUROSTAT_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_RELEASE_DATE
        == "2026-09-16"
    )
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_FIRST_REFERENCE_PERIOD == "2001-11"
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_LATEST_REFERENCE_PERIOD == "2026-07"
    assert manifest.as_of_date == EUROSTAT_INDUSTRIAL_PRODUCTION_AS_OF_DATE
    assert manifest.inventory.query_result_count == 1_213
    assert manifest.inventory.unique_uri_count == 1_212
    assert manifest.inventory.title_match_count == 297
    assert manifest.inventory.prefilter_count == 297
    assert len(manifest.inventory.entries) == 297
    assert len(manifest.inventory.exclusions) == 915
    assert len(manifest.inventory.artifacts) == 13
    assert manifest.release_count == 297
    assert manifest.reference_period_inferred_count == 1
    assert manifest.composition_source_stated_count == 236
    assert manifest.exact_publication_time_count == 46
    assert manifest.page_date_offset_count == 31
    assert manifest.release_document_count == 262
    assert manifest.published_value_count == 10_278
    assert manifest.revision_comparison_count == 8_393
    assert manifest.changed_revision_count == 4_660
    assert manifest.current_dataset_comparison_count == 6_936
    assert manifest.changed_current_dataset_count == 6_473
    assert manifest.raw_artifact_count == 573
    assert manifest.unique_content_sha256_count == 573
    assert manifest.total_content_bytes == 151_722_249
    assert manifest.manifest_id == (
        "eurostat-industrial-production-archive-manifest:sha256:"
        "872d8c6ae052e559710623403d49f4f97fb4dd969332a3772cef22e7c8d87ea6"
    )
    assert path.stat().st_size == 7_767_781
    assert sha256(path.read_bytes()).hexdigest() == (
        "e2ab13f3a2a3ac3fb0a76a8304d6e459fe91e3851bcf9927728be1e3be43efce"
    )


def test_packaged_selection_and_release_scope_stay_explicit() -> None:
    manifest = _manifest()

    assert Counter(item.reason for item in manifest.inventory.exclusions) == {
        "broad search-summary match outside the monthly headline lineage": 379,
        "industrial producer-price release, not production volume": 266,
        "national-accounts or labour release, not industrial production": 167,
        "external-trade release, not industrial production": 73,
        "services-production release, not industrial production": 30,
    }
    assert tuple(item.reference_period for item in manifest.releases) == (
        archive._month_range("2001-11", "2026-07")
    )
    assert manifest.searchable_release_gap_reference_periods == (
        archive._month_range("2000-01", "2001-10")
    )
    assert Counter(
        item.headline_value.movement for item in manifest.releases
    ) == {
        EurostatIndustrialProductionMovement.UP: 147,
        EurostatIndustrialProductionMovement.DOWN: 141,
        EurostatIndustrialProductionMovement.STABLE: 9,
    }
    assert Counter(
        item.euro_area_composition for item in manifest.releases
    ) == {
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
    ) == {True: 296, False: 1}
    assert Counter(
        item.composition_source_stated for item in manifest.releases
    ) == {True: 236, False: 61}
    assert min(item.days_after_period_end for item in manifest.releases) == 40
    assert max(item.days_after_period_end for item in manifest.releases) == 63
    assert min(item.headline_value.value for item in manifest.releases) == -17.1
    assert max(item.headline_value.value for item in manifest.releases) == 12.4


def test_packaged_landings_documents_and_values_are_bounded() -> None:
    manifest = _manifest()

    assert Counter(
        item.page_release_date_offset_days for item in manifest.releases
    ) == {-1: 30, 0: 266, 4: 1}
    assert [
        item.inventory_entry.product_code
        for item in manifest.releases
        if item.page_release_date_offset_days == 4
    ] == ["4-1703204-bp"]
    assert [
        item.inventory_entry.product_code
        for item in manifest.releases
        if not item.reference_period_source_stated
    ] == ["4-1703204-bp"]
    assert Counter(
        len(item.linked_document_uris) for item in manifest.releases
    ) == {0: 35, 1: 262}
    assert Counter(
        item.document_artifact.source_format
        for item in manifest.releases
        if item.document_artifact is not None
    ) == {
        OfficialSourceFormat.HTML: 31,
        OfficialSourceFormat.PDF: 231,
    }
    assert Counter(
        len(item.published_values) for item in manifest.releases
    ) == {
        1: 11,
        18: 1,
        25: 1,
        36: 284,
    }
    assert Counter(
        item.measure
        for release in manifest.releases
        for item in release.published_values
    ) == {
        EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH: 5_154,
        EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR: 5_124,
    }


def test_packaged_dataset_and_requests_are_independent() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_eurostat_industrial_production_search_requests(registry)
    dataset = build_eurostat_industrial_production_dataset_request(registry)
    releases = build_eurostat_industrial_production_release_requests(
        registry, manifest.inventory
    )

    assert EUROSTAT_INDUSTRIAL_PRODUCTION_SOURCE_KEY == (
        "ea.eurostat.industrial-production"
    )
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_PROGRAM_KEY == (
        "ea.eurostat.monthly-industrial-production"
    )
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_MEASURE_KEY == (
        "industrial-production-total-industry"
    )
    assert manifest.current_dataset.dataset_id == "sts_inpr_m"
    assert manifest.current_dataset.updated_at == "2026-09-17T11:00:00+0200"
    assert manifest.current_dataset.time_start == "2000-01"
    assert manifest.current_dataset.time_end == "2026-07"
    assert len(manifest.current_dataset.observations) == 1_914
    assert Counter(
        item.economy_code for item in manifest.current_dataset.observations
    ) == {"EA21": 638, "DE": 638, "FR": 638}
    assert [item.page_number for item in searches] == list(range(1, 14))
    assert len(releases) == 297
    assert len({item.uri for item in releases}) == 297
    assert {item.parser_id for item in (*searches, dataset, *releases)} == {
        EUROSTAT_INDUSTRIAL_PRODUCTION_PARSER_ID
    }
    assert {
        item.parser_version for item in (*searches, dataset, *releases)
    } == {EUROSTAT_INDUSTRIAL_PRODUCTION_PARSER_VERSION}


def test_packaged_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EurostatIndustrialProductionArchiveManifestV1.from_json(
            manifest.to_json()
        )
        == manifest
    )
    payload = json.loads(manifest.to_json())
    payload["published_value_count"] += 1
    payload["manifest_id"] = ""
    with pytest.raises(ValueError, match="published-value count differs"):
        EurostatIndustrialProductionArchiveManifestV1.from_json(
            json.dumps(payload, separators=(",", ":"), sort_keys=True)
        )


def test_inventory_counts_raw_entries_but_deduplicates_identical_uris(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        archive, "EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_PAGE_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_RESULT_COUNT", 13
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INDUSTRIAL_PRODUCTION_SEARCH_UNIQUE_URI_COUNT", 2
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INDUSTRIAL_PRODUCTION_TITLE_MATCH_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INDUSTRIAL_PRODUCTION_PREFILTER_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_INDUSTRIAL_PRODUCTION_RELEASE_COUNT", 1
    )
    monkeypatch.setattr(
        archive,
        "EUROSTAT_INDUSTRIAL_PRODUCTION_FIRST_SEARCH_RELEASE_DATE",
        "2002-01-21",
    )
    monkeypatch.setattr(
        archive,
        "EUROSTAT_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_RELEASE_DATE",
        "2002-01-21",
    )
    request = build_eurostat_industrial_production_search_requests(
        load_packaged_official_source_registry(), page_count=1
    )[0]
    selected = _atom_entry(
        "Industrial production down by 0.8% in euro-zone", "4-21012002-ap"
    )
    excluded = _atom_entry(
        "Industrial producer prices up by 0.2%", "4-22012002-ap"
    )
    content = (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        + selected
        + excluded * 12
        + "</feed>"
    ).encode()

    inventory = parse_eurostat_industrial_production_inventory(
        (_snapshot(request, content),), as_of_date="2002-01-31"
    )

    assert inventory.query_result_count == 13
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
        parse_eurostat_industrial_production_inventory(
            (_snapshot(request, altered),), as_of_date="2002-01-31"
        )


@pytest.mark.parametrize(
    ("code", "release_date"),
    (
        ("4-1703204-bp", "2004-03-17"),
        (
            (
                "https://ec.europa.eu/eurostat/web/products-euro-indicators/-/"
                "4-1703204-bp"
            ),
            "2004-03-17",
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


def test_malformed_historical_code_retains_source_page_date_exception() -> None:
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_PAGE_DATE_OFFSET_EXCEPTIONS == {
        "4-1703204-bp": 4
    }
    assert EUROSTAT_INDUSTRIAL_PRODUCTION_REFERENCE_PERIOD_INFERENCES == {
        "4-1703204-bp": "2004-01"
    }

    entry = replace(
        _entry(),
        product_code="4-1703204-bp",
        release_date="2004-03-17",
        source_uri=("https://ec.europa.eu/eurostat/product?code=4-1703204-bp"),
        summary="No retained release body.",
        entry_id="",
    )
    snapshot = _snapshot(
        _release_request(),
        b"<html><body>No retained release body.</body></html>",
    )

    assert _reference_period_from_publication(entry, snapshot, None) == (
        "2004-01",
        46,
    )


@pytest.mark.parametrize(
    ("title", "selected"),
    (
        ("Industrial production down by 0.8%", True),
        ("Industrial production unchanged in the euro area", True),
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
    <meta property="og:title" content="Industrial production down by 0.8% in euro-zone">
    </head><body>
    <p>Release date: 20 January 2002</p>
    <p>In November 2001, industrial production in the euro-zone (EA12)
    decreased by 0.8% compared with October 2001.</p>
    <a title="Download publication" href="/eurostat/documents/2995521/5198130/4-21012002-AP-EN.HTML.html/source">HTML</a>
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


@pytest.mark.parametrize(
    ("reference_period", "composition"),
    (
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
    request = build_eurostat_industrial_production_dataset_request(
        load_packaged_official_source_registry()
    )
    assert request.uri == EUROSTAT_INDUSTRIAL_PRODUCTION_DATASET_URI

    parsed = parse_eurostat_industrial_production_dataset(
        _snapshot(request, _dataset_payload())
    )

    assert len(parsed.observations) == 12
    assert {
        (item.measure, item.economy_code) for item in parsed.observations
    } == {
        (measure, economy)
        for measure in EurostatIndustrialProductionMeasure
        for economy in ("EA21", "DE", "FR")
    }
    assert parsed == type(parsed).from_dict(parsed.to_dict())

    with pytest.raises(
        ValueError, match="unsupported adjustment and unit combination"
    ):
        parse_eurostat_industrial_production_dataset(
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
            "Industrial production up by 1.2%",
            EurostatIndustrialProductionMovement.UP,
            1.2,
        ),
        (
            "Industrial production down by 0.8%",
            EurostatIndustrialProductionMovement.DOWN,
            -0.8,
        ),
        (
            "Industrial production unchanged",
            EurostatIndustrialProductionMovement.STABLE,
            0.0,
        ),
    ),
)
def test_headline_direction_is_preserved_as_a_signed_value(
    title: str, movement: EurostatIndustrialProductionMovement, value: float
) -> None:
    parsed = _headline_value(
        replace(_entry(), source_title=title, entry_id=""), "2001-11"
    )

    assert (
        parsed.measure is EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH
    )
    assert parsed.movement is movement
    assert parsed.value == value
    assert parsed.value_lexical == (
        "0"
        if movement is EurostatIndustrialProductionMovement.STABLE
        else str(abs(value))
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
        EurostatIndustrialProductionArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _industrial_table_published_values(snapshot, artifact, "2001-11")

    assert len(values) == 36
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2001-11"
    } == {
        ("EA", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -0.8),
        ("DE", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -1.0),
        ("FR", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -0.4),
        ("EA", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, -0.5),
        ("DE", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, -0.8),
        ("FR", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, -0.2),
    }

    conflicting = replace(
        snapshot,
        content=content.replace(
            b"</body>",
            b"<table><tr><th>month-on-month</th></tr><tr><th></th><th>Nov-01</th></tr><tr><td>Euro-zone</td><td>9.9</td></tr></table></body>",
        ),
    )
    with pytest.raises(ValueError, match="HTML tables conflict"):
        _industrial_table_published_values(conflicting, artifact, "2001-11")


def test_pdf_text_tables_retain_source_rows_and_reject_conflicts() -> None:
    snapshot = _snapshot(
        _document_request(), b"<html><body>evidence</body></html>"
    )
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatIndustrialProductionArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )
    monthly = """Industrial production - monthly variation
    % change compared with previous month
    Feb-07 Mar-07 Apr-07
    EA13
      Total industry4 0.5 0.5 -0.8
    Total industry4 Feb-07 Mar-07 Apr-07
    Euro-zone 0.5 0.5 -0.8
    Germany 0.3 0.7 -2.3
    France 1.3 0.2 -0.8
    """
    annual = """Industrial production - annual variation
    % change compared with same month of the previous year
    Feb-07 Mar-07 Apr-07
    EA13
      Total industry4 4.0 4.0 2.8
    Total industry4 Feb-07 Mar-07 Apr-07
    EA13 4.0 4.0 2.8
    Germany 6.1 7.3 4.0
    France 2.5 0.8 1.8
    """

    values = (
        *_industrial_pdf_page_values(monthly, artifact, "2007-04", 3),
        *_industrial_pdf_page_values(annual, artifact, "2007-04", 4),
    )

    assert len(values) == 18
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2007-04"
    } == {
        ("EA", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -0.8),
        ("DE", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -2.3),
        ("FR", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -0.8),
        ("EA", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, 2.8),
        ("DE", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, 4.0),
        ("FR", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, 1.8),
    }

    with pytest.raises(ValueError, match="PDF tables conflict"):
        _industrial_pdf_page_values(
            monthly + "\nEA13 0.5 0.5 9.9\n", artifact, "2007-04", 3
        )


def test_pdf_header_correction_retains_the_source_typo_explicitly() -> None:
    request = replace(
        _document_request(),
        uri=(
            "https://ec.europa.eu/eurostat/documents/2995521/10612128/"
            "4-14092020-AP-EN.PDF.pdf/source"
        ),
        source_format=OfficialSourceFormat.PDF,
    )
    artifact = _artifact_from_snapshot(
        _snapshot(request, b"%PDF-test"),
        EurostatIndustrialProductionArtifactRole.RELEASE_DOCUMENT_PDF,
        product_code="4-14092020-ap",
    )
    page = """Industrial production
    % change compared with previous month
    Feb-20 Mar-20 Apr-20 June-20 Jun-20 Jul-20
    Euro area -0.1 -11.6 -18.0 12.2 9.5 4.1
    Germany 0.4 -10.7 -20.3 9.2 10.9 2.4
    France 1.0 -17.1 -20.8 20.3 13.2 3.8
    """

    values = _industrial_pdf_page_values(page, artifact, "2020-07", 4)

    assert EUROSTAT_INDUSTRIAL_PRODUCTION_PERIOD_HEADER_CORRECTIONS == {
        "4-14092020-ap": (
            (
                "2020-02",
                "2020-03",
                "2020-04",
                "2020-06",
                "2020-06",
                "2020-07",
            ),
            (
                "2020-02",
                "2020-03",
                "2020-04",
                "2020-05",
                "2020-06",
                "2020-07",
            ),
        )
    }
    assert len(values) == 18
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.economy_code == "EA"
    } == {
        ("EA", "2020-02", -0.1),
        ("EA", "2020-03", -11.6),
        ("EA", "2020-04", -18.0),
        ("EA", "2020-05", 12.2),
        ("EA", "2020-06", 9.5),
        ("EA", "2020-07", 4.1),
    }


def test_html_heading_context_does_not_misclassify_later_index_tables() -> None:
    content = b"""<html><body>
    <h2>Industrial production - monthly variation</h2>
    <p>% change compared with previous month</p>
    <table><tr><th>Total industry</th><th>Jun-04</th></tr>
    <tr><td>Euro-zone</td><td>-0.4</td></tr>
    <tr><td>Germany</td><td>-2.0</td></tr>
    <tr><td>France</td><td>0.2</td></tr></table>
    <h2>Production indices for total industry excluding construction</h2>
    <table><tr><th></th><th>06/04</th></tr>
    <tr><td>Euro-zone</td><td>102.1</td></tr>
    <tr><td>Germany</td><td>101.1</td></tr>
    <tr><td>France</td><td>101.2</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatIndustrialProductionArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _industrial_table_published_values(snapshot, artifact, "2004-06")

    assert [(item.economy_code, item.value) for item in values] == [
        ("EA", -0.4),
        ("DE", -2.0),
        ("FR", 0.2),
    ]


def test_in_table_measure_headers_override_stale_landing_context() -> None:
    content = b"""<html><body>
    <h2>Annual comparison</h2>
    <table><tr><th>Industrial production % change compared with previous month</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>1.6</td><td>-3.2</td></tr>
    <tr><td>Germany</td><td>-1.8</td><td>0.6</td></tr>
    <tr><td>France</td><td>0.3</td><td>-1.0</td></tr></table>
    <table><tr><th>Industrial production % change compared with same month of the previous year</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>0.2</td><td>-6.7</td></tr>
    <tr><td>Germany</td><td>-4.3</td><td>-5.4</td></tr>
    <tr><td>France</td><td>0.5</td><td>1.0</td></tr></table>
    <table><tr><th>Production indices for total industry, calendar adjusted</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>95.1</td><td>94.0</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatIndustrialProductionArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _industrial_table_published_values(snapshot, artifact, "2024-01")

    assert len(values) == 12
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2024-01"
    } == {
        ("EA", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -3.2),
        ("DE", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, 0.6),
        ("FR", EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH, -1.0),
        ("EA", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, -6.7),
        ("DE", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, -5.4),
        ("FR", EurostatIndustrialProductionMeasure.YEAR_OVER_YEAR, 1.0),
    }


def test_parallel_euro_area_rows_select_the_reference_era_composition() -> None:
    content = b"""<html><body>
    <h2>Monthly comparison</h2>
    <table><tr><th>Total industry</th><th>Jan-26</th></tr>
    <tr><td>Euro area 20</td><td>0.5</td></tr>
    <tr><td>Euro area 21</td><td>0.4</td></tr>
    <tr><td>Germany</td><td>0.3</td></tr>
    <tr><td>France</td><td>0.2</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatIndustrialProductionArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _industrial_table_published_values(snapshot, artifact, "2026-01")

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
        EurostatIndustrialProductionArtifactRole.RELEASE_DOCUMENT_PDF,
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
    assert _industrial_pdf_table_values(snapshot, artifact, "2001-11") == ()

    _Reader.pages = (_Page(None), _Page(None))
    with pytest.raises(ValueError, match="too many unreadable pages"):
        _industrial_pdf_table_values(snapshot, artifact, "2001-11")


def test_revision_counts_do_not_cross_euro_area_compositions() -> None:
    digest = "0" * 64

    def value(
        economy: str, numeric: float
    ) -> EurostatIndustrialProductionPublishedValueV1:
        lexical = str(numeric)
        return EurostatIndustrialProductionPublishedValueV1(
            economy_code=economy,
            reference_period="2007-01",
            measure=EurostatIndustrialProductionMeasure.MONTH_OVER_MONTH,
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
