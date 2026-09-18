"""Qualification tests for Eurostat mixed-frequency construction output."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest
from pypdf.errors import PdfReadError

import histdatacom.market_context.eurostat_construction_output_archive as archive
from histdatacom.market_context.eurostat_construction_output_archive import (
    EUROSTAT_CONSTRUCTION_OUTPUT_AS_OF_DATE,
    EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI,
    EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT,
    EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_RELEASE_DATE,
    EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_REFERENCE_PERIOD,
    EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_SEARCH_RELEASE_DATE,
    EUROSTAT_CONSTRUCTION_OUTPUT_INTERNAL_GAP_REFERENCE_PERIODS,
    EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_REFERENCE_PERIOD,
    EUROSTAT_CONSTRUCTION_OUTPUT_MEASURE_KEY,
    EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_ID,
    EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_VERSION,
    EUROSTAT_CONSTRUCTION_OUTPUT_PERIOD_HEADER_CORRECTIONS,
    EUROSTAT_CONSTRUCTION_OUTPUT_PROGRAM_KEY,
    EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT,
    EUROSTAT_CONSTRUCTION_OUTPUT_REPEATED_REFERENCE_PERIODS,
    EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT,
    EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT,
    EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT,
    EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY,
    EurostatConstructionOutputArchiveManifestV1,
    EurostatConstructionOutputArtifactRole,
    EurostatConstructionOutputInventoryEntryV1,
    EurostatConstructionOutputMeasure,
    EurostatConstructionOutputMovement,
    EurostatConstructionOutputPublishedValueV1,
    _artifact_from_snapshot,
    _composition_from_publication,
    _construction_pdf_page_values,
    _construction_pdf_table_values,
    _construction_table_published_values,
    _document_source_format,
    _ea_composition_for_period,
    _headline_value,
    _inventory_prefilter,
    _parse_release_landing,
    _product_release_date,
    _published_revision_counts,
    _reference_period_from_publication,
    build_eurostat_construction_output_dataset_request,
    build_eurostat_construction_output_release_requests,
    build_eurostat_construction_output_search_requests,
    load_packaged_eurostat_construction_output_archive_manifest,
    packaged_eurostat_construction_output_manifest_path,
    parse_eurostat_construction_output_dataset,
    parse_eurostat_construction_output_inventory,
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


def _entry() -> EurostatConstructionOutputInventoryEntryV1:
    return EurostatConstructionOutputInventoryEntryV1(
        product_code="4-21012002-ap",
        release_date="2002-01-21",
        source_title="Construction output down by 0.8% in euro-zone",
        source_uri="https://ec.europa.eu/eurostat/product?code=4-21012002-ap",
        atom_published_at="2002-01-20T22:00:00Z",
        atom_updated_at="2002-01-20T22:00:00Z",
        summary="In November 2001, construction output fell by 0.8%.",
        page_number=1,
    )


def _document_request() -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY
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
        EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY
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
        "nace_r2": ("F",),
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
            "label": "Construction output - monthly data",
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


def _manifest() -> EurostatConstructionOutputArchiveManifestV1:
    return load_packaged_eurostat_construction_output_archive_manifest()


def test_packaged_archive_quantifies_the_complete_corpus() -> None:
    manifest = _manifest()
    path = packaged_eurostat_construction_output_manifest_path()

    assert EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT == 10
    assert EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT == 916
    assert EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT == 915
    assert EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT == 257
    assert EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT == 225
    assert (
        EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_SEARCH_RELEASE_DATE == "2002-03-13"
    )
    assert (
        EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_PACKAGED_RELEASE_DATE
        == "2026-09-18"
    )
    assert EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_REFERENCE_PERIOD == "2001-Q4"
    assert EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_REFERENCE_PERIOD == "2026-07"
    assert manifest.as_of_date == EUROSTAT_CONSTRUCTION_OUTPUT_AS_OF_DATE
    assert manifest.inventory.query_result_count == 916
    assert manifest.inventory.unique_uri_count == 915
    assert manifest.inventory.title_match_count == 258
    assert manifest.inventory.prefilter_count == 257
    assert len(manifest.inventory.entries) == 257
    assert len(manifest.inventory.exclusions) == 658
    assert len(manifest.inventory.artifacts) == 10
    assert manifest.release_count == 257
    assert manifest.reference_period_inferred_count == 0
    assert manifest.composition_source_stated_count == 235
    assert manifest.exact_publication_time_count == 46
    assert manifest.page_date_offset_count == 11
    assert manifest.release_document_count == 225
    assert manifest.published_value_count == 8_884
    assert manifest.revision_comparison_count == 7_213
    assert manifest.changed_revision_count == 4_358
    assert manifest.current_dataset_comparison_count == 5_590
    assert manifest.changed_current_dataset_count == 5_384
    assert manifest.raw_artifact_count == 493
    assert manifest.unique_content_sha256_count == 493
    assert manifest.total_content_bytes == 149_658_751
    assert manifest.manifest_id == (
        "eurostat-construction-output-archive-manifest:sha256:"
        "dec261630d8c95727fc690db6dd9a1910ea3c63682479cf97160abf43376209f"
    )
    assert path.stat().st_size == 6_690_664
    assert sha256(path.read_bytes()).hexdigest() == (
        "37c1dce9861d03a01b91172acdea795cf206d3eae85c13bf80307ce056530935"
    )


def test_packaged_selection_and_release_scope_stay_explicit() -> None:
    manifest = _manifest()

    assert Counter(item.reason for item in manifest.inventory.exclusions) == {
        "broad search-summary match without construction in the title": 657,
        "education article, not a construction-output release": 1,
    }
    assert EUROSTAT_CONSTRUCTION_OUTPUT_REPEATED_REFERENCE_PERIODS == (
        "2002-Q4",
    )
    assert EUROSTAT_CONSTRUCTION_OUTPUT_INTERNAL_GAP_REFERENCE_PERIODS == (
        "2015-04",
    )
    quarterly = list(archive._period_range("2001-Q4", "2006-Q3"))
    quarterly.insert(
        quarterly.index(
            EUROSTAT_CONSTRUCTION_OUTPUT_REPEATED_REFERENCE_PERIODS[0]
        )
        + 1,
        EUROSTAT_CONSTRUCTION_OUTPUT_REPEATED_REFERENCE_PERIODS[0],
    )
    monthly = [
        period
        for period in archive._month_range("2006-11", "2026-07")
        if period != "2015-04"
    ]
    assert tuple(item.reference_period for item in manifest.releases) == (
        *quarterly,
        *monthly,
    )
    assert manifest.searchable_release_gap_reference_periods == (
        *archive._month_range("2000-01", "2006-10"),
        "2015-04",
    )
    assert Counter(
        item.headline_value.movement for item in manifest.releases
    ) == {
        EurostatConstructionOutputMovement.UP: 113,
        EurostatConstructionOutputMovement.DOWN: 133,
        EurostatConstructionOutputMovement.STABLE: 11,
    }
    assert Counter(
        item.euro_area_composition for item in manifest.releases
    ) == {
        "EA12": 23,
        "EA13": 12,
        "EA15": 12,
        "EA16": 24,
        "EA17": 36,
        "EA18": 12,
        "EA19": 95,
        "EA20": 36,
        "EA21": 7,
    }
    assert Counter(
        item.reference_period_source_stated for item in manifest.releases
    ) == {True: 257}
    assert Counter(
        item.composition_source_stated for item in manifest.releases
    ) == {
        True: 235,
        False: 22,
    }
    assert tuple(
        (item.inventory_entry.product_code, item.reference_period)
        for item in manifest.releases
        if not item.composition_source_stated
        and "-Q" not in item.reference_period
    ) == (("4-20042026-ap", "2026-02"),)
    assert min(item.days_after_period_end for item in manifest.releases) == 45
    assert max(item.days_after_period_end for item in manifest.releases) == 88
    assert min(item.headline_value.value for item in manifest.releases) == -14.6
    assert max(item.headline_value.value for item in manifest.releases) == 27.9


def test_packaged_landings_documents_and_values_are_bounded() -> None:
    manifest = _manifest()

    assert Counter(
        item.page_release_date_offset_days for item in manifest.releases
    ) == {-1: 11, 0: 246}
    assert Counter(
        len(item.linked_document_uris) for item in manifest.releases
    ) == {
        0: 32,
        1: 225,
    }
    assert Counter(
        item.document_artifact.source_format
        for item in manifest.releases
        if item.document_artifact is not None
    ) == {
        OfficialSourceFormat.HTML: 11,
        OfficialSourceFormat.PDF: 214,
    }
    assert Counter(
        len(item.published_values) for item in manifest.releases
    ) == {
        1: 6,
        12: 1,
        24: 1,
        30: 20,
        34: 1,
        36: 228,
    }
    assert tuple(
        (item.inventory_entry.product_code, item.reference_period)
        for item in manifest.releases
        if len(item.published_values) == 1
    ) == (
        ("4-18102024-ap", "2024-08"),
        ("4-20112024-ap", "2024-09"),
        ("4-18122024-bp", "2024-10"),
        ("4-20012025-ap", "2024-11"),
        ("4-20022025-ap", "2024-12"),
        ("4-20032025-ap", "2025-01"),
    )
    assert tuple(
        (item.inventory_entry.product_code, len(item.published_values))
        for item in manifest.releases
        if len(item.published_values) in {24, 34}
    ) == (
        ("4-17042020-bp", 34),
        ("4-19052020-ap", 24),
    )
    assert Counter(
        item.measure
        for release in manifest.releases
        for item in release.published_values
    ) == {
        EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER: 306,
        EurostatConstructionOutputMeasure.MONTH_OVER_MONTH: 4_139,
        EurostatConstructionOutputMeasure.YEAR_OVER_YEAR: 4_439,
    }


def test_packaged_dataset_and_requests_are_independent() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_eurostat_construction_output_search_requests(registry)
    dataset = build_eurostat_construction_output_dataset_request(registry)
    releases = build_eurostat_construction_output_release_requests(
        registry, manifest.inventory
    )

    assert EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY == (
        "ea.eurostat.construction-output"
    )
    assert EUROSTAT_CONSTRUCTION_OUTPUT_PROGRAM_KEY == (
        "ea.eurostat.construction-output"
    )
    assert EUROSTAT_CONSTRUCTION_OUTPUT_MEASURE_KEY == (
        "construction-output-total-construction"
    )
    assert manifest.current_dataset.dataset_id == "sts_copr_m"
    assert manifest.current_dataset.updated_at == "2026-09-18T11:00:00+0200"
    assert manifest.current_dataset.time_start == "2000-01"
    assert manifest.current_dataset.time_end == "2026-07"
    assert len(manifest.current_dataset.observations) == 1_914
    assert Counter(
        item.economy_code for item in manifest.current_dataset.observations
    ) == {"EA21": 638, "DE": 638, "FR": 638}
    assert [item.page_number for item in searches] == list(range(1, 11))
    assert len(releases) == 257
    assert len({item.uri for item in releases}) == 257
    assert {item.parser_id for item in (*searches, dataset, *releases)} == {
        EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_ID
    }
    assert {
        item.parser_version for item in (*searches, dataset, *releases)
    } == {EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_VERSION}


def test_packaged_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EurostatConstructionOutputArchiveManifestV1.from_json(
            manifest.to_json()
        )
        == manifest
    )
    payload = json.loads(manifest.to_json())
    payload["published_value_count"] += 1
    payload["manifest_id"] = ""
    with pytest.raises(ValueError, match="published-value count differs"):
        EurostatConstructionOutputArchiveManifestV1.from_json(
            json.dumps(payload, separators=(",", ":"), sort_keys=True)
        )


def test_inventory_counts_raw_entries_but_deduplicates_identical_uris(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        archive, "EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT", 13
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT", 2
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_CONSTRUCTION_OUTPUT_TITLE_MATCH_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_CONSTRUCTION_OUTPUT_PREFILTER_COUNT", 1
    )
    monkeypatch.setattr(
        archive, "EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT", 1
    )
    monkeypatch.setattr(
        archive,
        "EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_SEARCH_RELEASE_DATE",
        "2002-01-21",
    )
    monkeypatch.setattr(
        archive,
        "EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_PACKAGED_RELEASE_DATE",
        "2002-01-21",
    )
    request = build_eurostat_construction_output_search_requests(
        load_packaged_official_source_registry(), page_count=1
    )[0]
    selected = _atom_entry(
        "Construction output down by 0.8% in euro-zone", "4-21012002-ap"
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

    inventory = parse_eurostat_construction_output_inventory(
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
        parse_eurostat_construction_output_inventory(
            (_snapshot(request, altered),), as_of_date="2002-01-31"
        )


@pytest.mark.parametrize(
    ("code", "release_date"),
    (
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


def test_legacy_document_suffixes_bind_to_their_source_formats() -> None:
    assert (
        _document_source_format(
            "https://ec.europa.eu/eurostat/documents/2995521/5145790/"
            "4-10032004-AP-EN.HTM.htm/7170ac09?t=1414685299000"
        )
        is OfficialSourceFormat.HTML
    )
    assert (
        _document_source_format(
            "https://ec.europa.eu/eurostat/documents/2995521/5198130/"
            "4-18072007-BP-EN.PDF.pdf/source"
        )
        is OfficialSourceFormat.PDF
    )


def test_quarterly_reference_period_is_read_from_the_release_body() -> None:
    entry = replace(
        _entry(),
        product_code="4-13032002-ap",
        release_date="2002-03-13",
        source_uri=("https://ec.europa.eu/eurostat/product?code=4-13032002-ap"),
        summary="Production in the construction sector.",
        entry_id="",
    )
    snapshot = _snapshot(
        _release_request(),
        b"<html><body>In the fourth quarter of 2001, construction output "
        b"decreased by 0.6% compared with the previous quarter.</body></html>",
    )

    assert _reference_period_from_publication(entry, snapshot, None) == (
        "2001-Q4",
        72,
    )


@pytest.mark.parametrize(
    ("title", "selected"),
    (
        ("Production in construction down by 0.8%", True),
        ("Construction output down by 0.8%", True),
        ("Construction output unchanged in the euro area", True),
        ("Euro area production in construction up by 1.2%", True),
        ("Euro area and EU27 production in construction stable", True),
        ("Production in the construction sector up by 0.3%", True),
        (
            "Engineering, manufacturing and construction dominated education",
            False,
        ),
        ("Industrial producer prices up by 0.2%", False),
        ("Services production increased by 1.0%", False),
    ),
)
def test_title_lineage_is_an_explicit_prefix(
    title: str, selected: bool
) -> None:
    assert _inventory_prefilter(title) is selected


def test_landing_binds_title_date_document_reference_and_composition() -> None:
    entry = replace(
        _entry(),
        product_code="4-18012007-ap1",
        release_date="2007-01-18",
        source_title="Construction output up by 1.2% in the euro area",
        source_uri=(
            "https://ec.europa.eu/eurostat/product?code=4-18012007-ap1"
        ),
        summary="In November 2006, construction output increased by 1.2%.",
        entry_id="",
    )
    content = b"""<html><head>
    <meta property="og:title" content="Construction output up by 1.2% in the euro area">
    </head><body>
    <p>Release date: 18 January 2007</p>
    <p>In November 2006 compared with October 2006, construction output in
    the euro area (EA12) increased by 1.2%.</p>
    <a title="Download publication" href="/eurostat/documents/2995521/5198130/4-18012007-AP1-EN.HTML.html/source">HTML</a>
    </body></html>"""
    request = replace(_release_request(), uri=entry.source_uri)
    snapshot = _snapshot(
        request,
        content,
        resolved_uri=(
            "https://ec.europa.eu/eurostat/en/web/"
            "products-euro-indicators/-/4-18012007-ap1"
        ),
    )

    title, page_date, published_at, links = _parse_release_landing(
        snapshot, entry
    )
    reference, lag = _reference_period_from_publication(entry, snapshot, None)
    composition = _composition_from_publication(reference, snapshot, None)

    assert title == entry.source_title
    assert page_date == "2007-01-18"
    assert published_at is None
    assert links == (
        (
            "https://ec.europa.eu/eurostat/documents/2995521/5198130/"
            "4-18012007-AP1-EN.HTML.html/source"
        ),
    )
    assert (reference, lag) == ("2006-11", 49)
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


def test_release_body_precedes_an_unrelated_atom_summary() -> None:
    entry = replace(
        _entry(),
        product_code="4-19052016-ap",
        release_date="2016-05-19",
        source_title="Production in construction down by 0.9% in euro area",
        source_uri=("https://ec.europa.eu/eurostat/product?code=4-19052016-ap"),
        summary="Euro area annual inflation was -0.2% in April 2016.",
        entry_id="",
    )
    request = replace(
        _document_request(),
        uri=(
            "https://ec.europa.eu/eurostat/web/products-euro-indicators/"
            "-/4-19052016-ap"
        ),
    )
    landing = _snapshot(
        request,
        b"<html><body>March 2016 compared with February 2016</body></html>",
    )

    assert _reference_period_from_publication(entry, landing, None) == (
        "2016-03",
        49,
    )


def test_qualified_month_precedes_a_methodology_reference() -> None:
    entry = replace(
        _entry(),
        product_code="4-19022018-ap",
        release_date="2018-02-19",
        source_title="Production in construction up by 0.1% in euro area",
        source_uri=("https://ec.europa.eu/eurostat/product?code=4-19022018-ap"),
        summary=(
            "In December 2017 compared with November 2017, production in "
            "construction increased by 0.1%."
        ),
        entry_id="",
    )
    request = replace(
        _document_request(),
        uri=(
            "https://ec.europa.eu/eurostat/web/products-euro-indicators/"
            "-/4-19022018-ap"
        ),
    )
    landing = _snapshot(
        request,
        (
            b"<html><body>Data for the reference month January 2018 will be "
            b"published progressively. December 2017 compared with November "
            b"2017: production in construction up by 0.1%.</body></html>"
        ),
    )

    assert EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_RELEASE_DATE == (
        "2007-01-18"
    )
    assert _reference_period_from_publication(entry, landing, None) == (
        "2017-12",
        50,
    )


def test_dataset_parser_decodes_only_the_two_supported_measure_coordinates() -> (
    None
):
    request = build_eurostat_construction_output_dataset_request(
        load_packaged_official_source_registry()
    )
    assert request.uri == EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI

    parsed = parse_eurostat_construction_output_dataset(
        _snapshot(request, _dataset_payload())
    )

    assert len(parsed.observations) == 12
    assert {
        (item.measure, item.economy_code) for item in parsed.observations
    } == {
        (measure, economy)
        for measure in (
            EurostatConstructionOutputMeasure.MONTH_OVER_MONTH,
            EurostatConstructionOutputMeasure.YEAR_OVER_YEAR,
        )
        for economy in ("EA21", "DE", "FR")
    }
    assert parsed == type(parsed).from_dict(parsed.to_dict())

    with pytest.raises(
        ValueError, match="unsupported adjustment and unit combination"
    ):
        parse_eurostat_construction_output_dataset(
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
            "Construction output up by 1.2%",
            EurostatConstructionOutputMovement.UP,
            1.2,
        ),
        (
            "Construction output down by 0.8%",
            EurostatConstructionOutputMovement.DOWN,
            -0.8,
        ),
        (
            "Construction output unchanged",
            EurostatConstructionOutputMovement.STABLE,
            0.0,
        ),
        (
            "Production in construction up 2.1% in the euro area",
            EurostatConstructionOutputMovement.UP,
            2.1,
        ),
        (
            "Euro area and EU27 production in construction down by 1.4%",
            EurostatConstructionOutputMovement.DOWN,
            -1.4,
        ),
        (
            "Production in the construction sector steady in euro-zone",
            EurostatConstructionOutputMovement.STABLE,
            0.0,
        ),
    ),
)
def test_headline_direction_is_preserved_as_a_signed_value(
    title: str, movement: EurostatConstructionOutputMovement, value: float
) -> None:
    parsed = _headline_value(
        replace(_entry(), source_title=title, entry_id=""), "2001-11"
    )

    assert parsed.measure is EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
    assert parsed.movement is movement
    assert parsed.value == value
    assert parsed.value_lexical == (
        "0"
        if movement is EurostatConstructionOutputMovement.STABLE
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
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _construction_table_published_values(snapshot, artifact, "2001-11")

    assert len(values) == 36
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2001-11"
    } == {
        ("EA", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -0.8),
        ("DE", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -1.0),
        ("FR", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -0.4),
        ("EA", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, -0.5),
        ("DE", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, -0.8),
        ("FR", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, -0.2),
    }

    conflicting = replace(
        snapshot,
        content=content.replace(
            b"</body>",
            b"<table><tr><th>month-on-month</th></tr><tr><th></th><th>Nov-01</th></tr><tr><td>Euro-zone</td><td>9.9</td></tr></table></body>",
        ),
    )
    with pytest.raises(ValueError, match="HTML tables conflict"):
        _construction_table_published_values(conflicting, artifact, "2001-11")


def test_legacy_html_tables_retain_quarterly_and_annual_values() -> None:
    content = b"""<html><body>
    <h2>Construction output - quarterly variation</h2>
    <table><tr><th>Total construction</th><th>2nd quarter 2001</th><th>3rd quarter 2001</th><th>4th quarter 2001</th></tr>
    <tr><td>Euro-zone</td><td>0.2</td><td>-0.1</td><td>0.3</td></tr>
    <tr><td>Germany</td><td>0.1</td><td>-0.2</td><td>0.4</td></tr>
    <tr><td>France</td><td>0.3</td><td>0.2</td><td>-0.1</td></tr></table>
    <h2>Construction output - annual variation</h2>
    <table><tr><th>Total construction</th><th>2nd quarter 2001</th><th>3rd quarter 2001</th><th>4th quarter 2001</th></tr>
    <tr><td>Euro-zone</td><td>1.2</td><td>0.9</td><td>0.7</td></tr>
    <tr><td>Germany</td><td>1.0</td><td>0.7</td><td>0.5</td></tr>
    <tr><td>France</td><td>1.4</td><td>1.1</td><td>0.9</td></tr></table>
    </body></html>"""
    request = replace(
        _document_request(),
        uri=_document_request().uri.replace("4-21012002", "4-13032002"),
    )
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-13032002-ap",
    )

    values = _construction_table_published_values(snapshot, artifact, "2001-Q4")

    assert len(values) == 18
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2001-Q4"
    } == {
        ("EA", EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER, 0.3),
        ("DE", EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER, 0.4),
        ("FR", EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER, -0.1),
        ("EA", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, 0.7),
        ("DE", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, 0.5),
        ("FR", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, 0.9),
    }


def test_html_component_tables_cannot_overwrite_total_construction() -> None:
    content = b"""<html><body>
    <h2>Construction output - quarterly variation</h2>
    <table><tr><th>Total construction</th><th>Q4/01</th></tr>
    <tr><td>Euro-zone</td><td>0.3</td></tr></table>
    <table><tr><th>Building</th><th>Q4/01</th></tr>
    <tr><td>Euro-zone</td><td>-0.3</td></tr></table>
    <table><tr><th>Civil engineering</th><th>Q4/01</th></tr>
    <tr><td>Euro-zone</td><td>0.0</td></tr></table>
    </body></html>"""
    request = replace(
        _document_request(),
        uri=_document_request().uri.replace("4-21012002", "4-13032002"),
    )
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-13032002-ap",
    )

    values = _construction_table_published_values(snapshot, artifact, "2001-Q4")

    assert [(item.economy_code, item.value) for item in values] == [("EA", 0.3)]


def test_pdf_text_tables_retain_source_rows_and_reject_conflicts() -> None:
    snapshot = _snapshot(
        _document_request(), b"<html><body>evidence</body></html>"
    )
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )
    monthly = """Construction output - quarterly variation / monthly variation
    % change compared with previous quarter / previous month
    Q3-06 Q4-06 Feb-07 Mar-07 Apr-07
    EA134
      Total construction4 0.1 0.2 0.5 0.5 -0.8
    Total construction4 Q3-06 Q4-06 Feb-07 Mar-07 Apr-07
    Euro-zone 0.1 0.2 0.5 0.5 -0.8
    Germany 0.1 0.2 0.3 0.7 -2.3
    France 0.1 0.2 1.3 0.2 -0.8
    Building Q3-06 Q4-06 Feb-07 Mar-07 Apr-07
    Euro-zone 9.9 9.9 9.9 9.9 9.9
    Germany 9.9 9.9 9.9 9.9 9.9
    France 9.9 9.9 9.9 9.9 9.9
    """
    annual = """Construction output - annual variation
    % change compared with same quarter / same month of the previous  ye ar
    Q3-06 Q4-06 Feb-07 Mar-07 Apr-07
    EA13
      Total construction4 1.0 2.0 4.0 4.0 2.8
    Total construction4 Q3-06 Q4-06 Feb-07 Mar-07 Apr-07
    Euro area 18.0 2.0 4.0 4.0 2.8
    Germany 1.0 2.0 6.1 7.3 4.0
    France 1.0 2.0 2.5 0.8 1.8
    """

    values = (
        *_construction_pdf_page_values(monthly, artifact, "2007-04", 3),
        *_construction_pdf_page_values(annual, artifact, "2007-04", 4),
    )

    assert len(values) == 18
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2007-04"
    } == {
        ("EA", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -0.8),
        ("DE", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -2.3),
        ("FR", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -0.8),
        ("EA", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, 2.8),
        ("DE", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, 4.0),
        ("FR", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, 1.8),
    }

    with pytest.raises(ValueError, match="PDF tables conflict"):
        _construction_pdf_page_values(
            monthly.replace(
                "    Building Q3-06",
                "    EA13 0.1 0.2 0.5 0.5 9.9\n    Building Q3-06",
            ),
            artifact,
            "2007-04",
            3,
        )


def test_pdf_text_tables_retain_quarterly_source_rows() -> None:
    request = replace(
        _document_request(),
        uri=_document_request().uri.replace("4-21012002", "4-13032002"),
        source_format=OfficialSourceFormat.PDF,
    )
    artifact = _artifact_from_snapshot(
        _snapshot(request, b"%PDF-test"),
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF,
        product_code="4-13032002-ap",
    )
    page = """Production in the construction sector
    % change compared with previous quarter
    2001 Q2 2001 Q3 2001 Q4
    Euro-zone 0.2 -0.1 -0.6
    Germany 0.4 -0.3 -1.1
    France 0.1 0.2 -0.2
    """

    values = _construction_pdf_page_values(page, artifact, "2001-Q4", 3)

    assert len(values) == 9
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2001-Q4"
    } == {
        ("EA", EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER, -0.6),
        ("DE", EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER, -1.1),
        ("FR", EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER, -0.2),
    }


def test_pdf_period_header_correction_is_code_scoped() -> None:
    request = replace(
        _document_request(),
        uri=_document_request().uri.replace("4-21012002", "4-19092011"),
        source_format=OfficialSourceFormat.PDF,
    )
    artifact = _artifact_from_snapshot(
        _snapshot(request, b"%PDF-test"),
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF,
        product_code="4-19092011-ap",
    )
    page = """Construction output - quarterly variation / monthly variation
    % change compared with the previous quarter / previous month
    Q3-10 Q4-10 Q1-11 Q2-11 Feb-11 Mar-11 Apr-11 June-11 Jun-11 Jul-11
    EA17 -5.6 -1.7 1.5 0.2 -0.5 -0.2 0.8 0.1 -1.3 1.4
    Germany 0.0 -6.5 14.1 -0.9 3.0 4.3 -3.2 -0.2 -3.7 3.2
    France 0.3 -4.0 6.0 -1.5 -0.5 -1.1 0.0 -0.4 -0.8 -0.1
    """

    values = _construction_pdf_page_values(page, artifact, "2011-07", 3)

    assert EUROSTAT_CONSTRUCTION_OUTPUT_PERIOD_HEADER_CORRECTIONS == {
        "4-19092011-ap": (
            (
                "2010-Q3",
                "2010-Q4",
                "2011-Q1",
                "2011-Q2",
                "2011-02",
                "2011-03",
                "2011-04",
                "2011-06",
                "2011-06",
                "2011-07",
            ),
            (
                "2010-Q3",
                "2010-Q4",
                "2011-Q1",
                "2011-Q2",
                "2011-02",
                "2011-03",
                "2011-04",
                "2011-05",
                "2011-06",
                "2011-07",
            ),
        )
    }
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.economy_code == "EA"
    } == {
        ("EA", "2011-02", -0.5),
        ("EA", "2011-03", -0.2),
        ("EA", "2011-04", 0.8),
        ("EA", "2011-05", 0.1),
        ("EA", "2011-06", -1.3),
        ("EA", "2011-07", 1.4),
    }


def test_html_heading_context_does_not_misclassify_later_index_tables() -> None:
    content = b"""<html><body>
    <h2>Construction output - monthly variation</h2>
    <p>% change compared with previous month</p>
    <table><tr><th>Total construction</th><th>Jun-04</th></tr>
    <tr><td>Euro-zone</td><td>-0.4</td></tr>
    <tr><td>Germany</td><td>-2.0</td></tr>
    <tr><td>France</td><td>0.2</td></tr></table>
    <h2>Production indices for total construction</h2>
    <table><tr><th></th><th>06/04</th></tr>
    <tr><td>Euro-zone</td><td>102.1</td></tr>
    <tr><td>Germany</td><td>101.1</td></tr>
    <tr><td>France</td><td>101.2</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _construction_table_published_values(snapshot, artifact, "2004-06")

    assert [(item.economy_code, item.value) for item in values] == [
        ("EA", -0.4),
        ("DE", -2.0),
        ("FR", 0.2),
    ]


def test_in_table_measure_headers_override_stale_landing_context() -> None:
    content = b"""<html><body>
    <h2>Annual comparison</h2>
    <table><tr><th>Construction output % change compared with previous month</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>1.6</td><td>-3.2</td></tr>
    <tr><td>Germany</td><td>-1.8</td><td>0.6</td></tr>
    <tr><td>France</td><td>0.3</td><td>-1.0</td></tr></table>
    <table><tr><th>Construction output % change compared with same month of the previous year</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>0.2</td><td>-6.7</td></tr>
    <tr><td>Germany</td><td>-4.3</td><td>-5.4</td></tr>
    <tr><td>France</td><td>0.5</td><td>1.0</td></tr></table>
    <table><tr><th>Monthly indices for production in construction, calendar adjusted</th></tr>
    <tr><th></th><th>Dec-23</th><th>Jan-24</th></tr>
    <tr><td>Euro area</td><td>95.1</td><td>94.0</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _construction_table_published_values(snapshot, artifact, "2024-01")

    assert len(values) == 12
    assert {
        (item.economy_code, item.measure, item.value)
        for item in values
        if item.reference_period == "2024-01"
    } == {
        ("EA", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -3.2),
        ("DE", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, 0.6),
        ("FR", EurostatConstructionOutputMeasure.MONTH_OVER_MONTH, -1.0),
        ("EA", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, -6.7),
        ("DE", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, -5.4),
        ("FR", EurostatConstructionOutputMeasure.YEAR_OVER_YEAR, 1.0),
    }


def test_parallel_euro_area_rows_select_the_reference_era_composition() -> None:
    content = b"""<html><body>
    <h2>Monthly comparison</h2>
    <table><tr><th>Total construction</th><th>Jan-26</th></tr>
    <tr><td>Euro area 20</td><td>0.5</td></tr>
    <tr><td>Euro area 21</td><td>0.4</td></tr>
    <tr><td>Germany</td><td>0.3</td></tr>
    <tr><td>France</td><td>0.2</td></tr></table>
    </body></html>"""
    snapshot = _snapshot(_document_request(), content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="4-21012002-ap",
    )

    values = _construction_table_published_values(snapshot, artifact, "2026-01")

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
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF,
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
    assert _construction_pdf_table_values(snapshot, artifact, "2001-11") == ()

    _Reader.pages = (_Page(None), _Page(None))
    with pytest.raises(ValueError, match="too many unreadable pages"):
        _construction_pdf_table_values(snapshot, artifact, "2001-11")


def test_revision_counts_do_not_cross_euro_area_compositions() -> None:
    digest = "0" * 64

    def value(
        economy: str, numeric: float
    ) -> EurostatConstructionOutputPublishedValueV1:
        lexical = str(numeric)
        return EurostatConstructionOutputPublishedValueV1(
            economy_code=economy,
            reference_period="2007-01",
            measure=EurostatConstructionOutputMeasure.MONTH_OVER_MONTH,
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
