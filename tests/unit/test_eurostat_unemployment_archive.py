"""Qualification tests for the Eurostat monthly unemployment archive."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest
from pypdf.errors import PdfReadError

import histdatacom.market_context.eurostat_unemployment_archive as unemployment_archive
from histdatacom.market_context import (
    EUROSTAT_UNEMPLOYMENT_AS_OF_DATE,
    EUROSTAT_UNEMPLOYMENT_DOCUMENT_COUNT,
    EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD,
    EUROSTAT_UNEMPLOYMENT_FIRST_SEARCH_RELEASE_DATE,
    EUROSTAT_UNEMPLOYMENT_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_UNEMPLOYMENT_LATEST_REFERENCE_PERIOD,
    EUROSTAT_UNEMPLOYMENT_MEASURE_KEY,
    EUROSTAT_UNEMPLOYMENT_PAGE_DATE_OFFSET_EXCEPTIONS,
    EUROSTAT_UNEMPLOYMENT_PROGRAM_KEY,
    EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT,
    EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT,
    EUROSTAT_UNEMPLOYMENT_SEARCH_RESULT_COUNT,
    EUROSTAT_UNEMPLOYMENT_SOURCE_KEY,
    EurostatUnemploymentArchiveManifestV1,
    EurostatUnemploymentDatasetV1,
    EurostatUnemploymentMeasure,
    EurostatUnemploymentMovement,
    build_eurostat_unemployment_dataset_request,
    build_eurostat_unemployment_release_requests,
    build_eurostat_unemployment_search_requests,
    load_packaged_eurostat_unemployment_archive_manifest,
    load_packaged_official_source_registry,
    packaged_eurostat_unemployment_manifest_path,
    parse_eurostat_unemployment_dataset,
)
from histdatacom.market_context.eurostat_unemployment_archive import (
    EurostatUnemploymentArtifactRole,
    EurostatUnemploymentInventoryEntryV1,
    _artifact_from_snapshot,
    _composition_from_publication,
    _ea_composition_for_period,
    _headline_value,
    _inventory_prefilter,
    _parse_release_landing,
    _published_revision_counts,
    _reference_period_from_publication,
    _table_published_values,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
)


def _month_range(start: str, end: str) -> tuple[str, ...]:
    year, month = (int(item) for item in start.split("-"))
    result: list[str] = []
    while True:
        value = f"{year:04d}-{month:02d}"
        result.append(value)
        if value == end:
            return tuple(result)
        month += 1
        if month == 13:
            year += 1
            month = 1


def _release_entry() -> EurostatUnemploymentInventoryEntryV1:
    return EurostatUnemploymentInventoryEntryV1(
        product_code="3-01092026-bp",
        release_date="2026-09-01",
        source_title="Euro area unemployment at 6.4%",
        source_uri=("https://ec.europa.eu/eurostat/product?code=3-01092026-bp"),
        atom_published_at="2026-09-01T09:00:00Z",
        atom_updated_at="2026-09-01T09:00:00Z",
        summary="In July 2026, the euro area unemployment rate was 6.4%.",
        page_number=1,
    )


def _release_request() -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_UNEMPLOYMENT_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=_release_entry().source_uri,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    *,
    resolved_uri: str | None = None,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.JSON_STAT: "application/json",
        OfficialSourceFormat.HTML: "text/html",
    }.get(request.source_format, "application/octet-stream")
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


def _dataset_payload() -> bytes:
    periods = _month_range("2000-01", "2026-08")
    geos = ("EA21", "DE", "FR")
    dimensions = {
        "freq": ("M",),
        "s_adj": ("SA",),
        "age": ("TOTAL",),
        "unit": ("PC_ACT",),
        "sex": ("T",),
        "geo": geos,
        "time": periods,
    }
    values = {
        str(geo_index * len(periods) + period_index): (
            6.0 + geo_index + (period_index % 10) / 10
        )
        for geo_index in range(len(geos))
        for period_index in range(len(periods) - 1)
    }
    return json.dumps(
        {
            "class": "dataset",
            "label": "Unemployment by sex and age - monthly data",
            "updated": "2026-09-17T23:00:00+0200",
            "id": ["freq", "s_adj", "age", "unit", "sex", "geo", "time"],
            "size": [1, 1, 1, 1, 1, 3, len(periods)],
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


def _manifest() -> EurostatUnemploymentArchiveManifestV1:
    return load_packaged_eurostat_unemployment_archive_manifest()


def test_packaged_unemployment_archive_quantifies_the_complete_corpus() -> None:
    manifest = _manifest()
    path = packaged_eurostat_unemployment_manifest_path()

    assert EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT == 4
    assert EUROSTAT_UNEMPLOYMENT_SEARCH_RESULT_COUNT == 383
    assert EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT == 296
    assert EUROSTAT_UNEMPLOYMENT_DOCUMENT_COUNT == 264
    assert EUROSTAT_UNEMPLOYMENT_FIRST_SEARCH_RELEASE_DATE == "2002-01-04"
    assert EUROSTAT_UNEMPLOYMENT_LATEST_PACKAGED_RELEASE_DATE == "2026-09-01"
    assert EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD == "2001-11"
    assert EUROSTAT_UNEMPLOYMENT_LATEST_REFERENCE_PERIOD == "2026-07"
    assert manifest.as_of_date == EUROSTAT_UNEMPLOYMENT_AS_OF_DATE
    assert manifest.inventory.query_result_count == 383
    assert manifest.inventory.title_match_count == 317
    assert manifest.inventory.prefilter_count == 296
    assert len(manifest.inventory.entries) == 296
    assert len(manifest.inventory.exclusions) == 87
    assert len(manifest.inventory.artifacts) == 4
    assert manifest.release_count == 296
    assert manifest.release_document_count == 264
    assert manifest.composition_source_stated_count == 237
    assert manifest.exact_publication_time_count == 46
    assert manifest.page_date_offset_count == 31
    assert manifest.published_value_count == 4_036
    assert manifest.revision_comparison_count == 3_146
    assert manifest.changed_revision_count == 1_144
    assert manifest.current_dataset_comparison_count == 2_683
    assert manifest.changed_current_dataset_count == 2_248
    assert manifest.raw_artifact_count == 565
    assert manifest.unique_content_sha256_count == 565
    assert manifest.total_content_bytes == 134_483_606
    assert manifest.manifest_id == (
        "eurostat-unemployment-archive-manifest:sha256:"
        "009de7ae5058c195fb9b003445fe9a3d18d920145df7840d9dc5559814d22c48"
    )
    assert path.stat().st_size == 3_577_040
    assert sha256(path.read_bytes()).hexdigest() == (
        "57b018b3679dbd86ebe8b4028992b6cf70eba00decbc9ff50ded42bfc108bc0a"
    )


def test_packaged_unemployment_selection_and_release_scope_stay_explicit() -> (
    None
):
    manifest = _manifest()
    periods = tuple(
        item for item in _month_range("2001-11", "2026-07") if item != "2003-02"
    )

    assert Counter(item.reason for item in manifest.inventory.exclusions) == {
        "search-summary match without unemployment in the title": 63,
        "regional unemployment article, not the monthly headline release": 21,
        "thematic labour article, not the monthly headline release": 3,
    }
    assert tuple(item.reference_period for item in manifest.releases) == periods
    assert manifest.searchable_release_gap_reference_periods == (
        *_month_range("2000-01", "2001-10"),
        "2003-02",
    )
    assert Counter(
        item.headline_value.movement for item in manifest.releases
    ) == {
        EurostatUnemploymentMovement.REPORTED: 195,
        EurostatUnemploymentMovement.STABLE: 57,
        EurostatUnemploymentMovement.UP: 26,
        EurostatUnemploymentMovement.DOWN: 18,
    }
    assert Counter(
        item.euro_area_composition for item in manifest.releases
    ) == {
        "EA12": 61,
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
        item.composition_source_stated for item in manifest.releases
    ) == {
        True: 237,
        False: 59,
    }
    assert min(item.days_after_period_end for item in manifest.releases) == 27
    assert max(item.days_after_period_end for item in manifest.releases) == 41


def test_packaged_unemployment_landings_documents_and_values_are_bounded() -> (
    None
):
    manifest = _manifest()

    assert Counter(
        item.page_release_date_offset_days for item in manifest.releases
    ) == {-1: 30, 0: 265, 3: 1}
    assert [
        item.inventory_entry.product_code
        for item in manifest.releases
        if item.page_release_date_offset_days == 3
    ] == ["3-30032007-bp"]
    assert Counter(
        len(item.linked_document_uris) for item in manifest.releases
    ) == {
        0: 32,
        1: 264,
    }
    assert Counter(
        item.document_artifact.source_format
        for item in manifest.releases
        if item.document_artifact is not None
    ) == {
        OfficialSourceFormat.HTML: 30,
        OfficialSourceFormat.PDF: 234,
    }
    assert Counter(
        len(item.published_values) for item in manifest.releases
    ) == {
        1: 64,
        15: 160,
        18: 26,
        24: 46,
    }


def test_packaged_unemployment_dataset_and_requests_are_independent() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_eurostat_unemployment_search_requests(registry)
    dataset = build_eurostat_unemployment_dataset_request(registry)
    releases = build_eurostat_unemployment_release_requests(
        registry, manifest.inventory
    )

    assert manifest.current_dataset.dataset_id == "une_rt_m"
    assert manifest.current_dataset.updated_at == "2026-09-17T23:00:00+0200"
    assert manifest.current_dataset.time_start == "2000-01"
    assert manifest.current_dataset.time_end == "2026-08"
    assert len(manifest.current_dataset.observations) == 957
    assert Counter(
        item.economy_code for item in manifest.current_dataset.observations
    ) == {"EA21": 319, "DE": 319, "FR": 319}
    assert [item.page_number for item in searches] == [1, 2, 3, 4]
    assert len(releases) == 296
    assert len({item.uri for item in releases}) == 296
    assert {item.parser_id for item in (*searches, dataset, *releases)} == {
        "official.eurostat-unemployment.v1"
    }


def test_packaged_unemployment_manifest_round_trip_and_tamper_detection() -> (
    None
):
    manifest = _manifest()

    assert (
        EurostatUnemploymentArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    payload = json.loads(manifest.to_json())
    payload["published_value_count"] += 1
    payload["manifest_id"] = ""
    with pytest.raises(ValueError, match="published-value count differs"):
        EurostatUnemploymentArchiveManifestV1.from_json(
            json.dumps(payload, separators=(",", ":"), sort_keys=True)
        )


def test_unemployment_identity_and_bounded_requests() -> None:
    registry = load_packaged_official_source_registry()
    searches = build_eurostat_unemployment_search_requests(registry)
    dataset = build_eurostat_unemployment_dataset_request(registry)

    assert EUROSTAT_UNEMPLOYMENT_SOURCE_KEY == "ea.eurostat.unemployment"
    assert EUROSTAT_UNEMPLOYMENT_PROGRAM_KEY == (
        "ea.eurostat.monthly-unemployment"
    )
    assert EUROSTAT_UNEMPLOYMENT_MEASURE_KEY == "unemployment-rate-total-sa"
    assert EUROSTAT_UNEMPLOYMENT_AS_OF_DATE == "2026-09-17"
    assert EUROSTAT_UNEMPLOYMENT_PAGE_DATE_OFFSET_EXCEPTIONS == {
        "3-30032007-bp": 3
    }
    assert len(searches) == EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT == 4
    assert [item.page_number for item in searches] == [1, 2, 3, 4]
    assert all("text=unemployment" in item.uri for item in searches)
    assert dataset.source_format is OfficialSourceFormat.JSON_STAT
    assert "une_rt_m" in dataset.uri
    assert "geo=EA21&geo=DE&geo=FR" in dataset.uri
    assert {item.parser_id for item in (*searches, dataset)} == {
        "official.eurostat-unemployment.v1"
    }

    with pytest.raises(ValueError, match="source URI route differs"):
        replace(
            _release_entry(),
            source_uri=_release_entry().source_uri + "&ignored=",
            entry_id="",
        )


@pytest.mark.parametrize(
    ("title", "selected"),
    (
        ("Euro-zone unemployment stable at 8.5%", True),
        ("Euro area unemployment rate at 10.0%", True),
        ("Euro area unemployment at 6.4%", True),
        ("Regional unemployment rates ranged from 2% to 20%", False),
        ("Euro area and EU27 government deficit at 6.2%", False),
    ),
)
def test_unemployment_title_lineage_is_explicit(
    title: str, selected: bool
) -> None:
    assert _inventory_prefilter(title) is selected


def test_unemployment_dataset_parser_preserves_sparse_current_cube() -> None:
    request = build_eurostat_unemployment_dataset_request(
        load_packaged_official_source_registry()
    )
    parsed = parse_eurostat_unemployment_dataset(
        _snapshot(request, _dataset_payload())
    )

    assert isinstance(parsed, EurostatUnemploymentDatasetV1)
    assert parsed.dataset_id == "une_rt_m"
    assert parsed.time_start == "2000-01"
    assert parsed.time_end == "2026-08"
    assert len(parsed.observations) == 957
    assert {
        (item.measure, item.economy_code) for item in parsed.observations
    } == {
        (EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE, "EA21"),
        (EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE, "DE"),
        (EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE, "FR"),
    }
    assert parsed == EurostatUnemploymentDatasetV1.from_dict(parsed.to_dict())
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


def test_unemployment_dataset_parser_rejects_request_identity_drift() -> None:
    request = build_eurostat_unemployment_dataset_request(
        load_packaged_official_source_registry()
    )
    with pytest.raises(ValueError, match="dataset request identity differs"):
        parse_eurostat_unemployment_dataset(
            _snapshot(replace(request, uri=request.uri + "&geo=ES"), b"{}")
        )


def test_unemployment_landing_binds_title_clock_document_and_reference() -> (
    None
):
    entry = _release_entry()
    request = _release_request()
    content = b"""<html><head>
    <meta property="og:title" content="Euro area unemployment at 6.4%">
    <meta property="article:published_time" content="2026-09-01T09:00:00Z">
    </head><body>
    <script>const staleMetadata = "June 2026";</script>
    <p>In July 2026, the euro area (EA21) seasonally adjusted unemployment
    rate was 6.4%.</p><p>Release date: 1 September 2026</p>
    <a title="Download publication" href="/eurostat/documents/2995521/1/3-01092026-BP-EN.PDF.pdf/x">PDF</a>
    </body></html>"""
    snapshot = _snapshot(
        request,
        content,
        resolved_uri=(
            "https://ec.europa.eu/eurostat/en/web/"
            "products-euro-indicators/w/3-01092026-bp"
        ),
    )

    title, page_date, published_at, links = _parse_release_landing(
        snapshot, entry
    )
    stale_document = replace(
        snapshot,
        request=replace(
            request,
            uri=(
                "https://ec.europa.eu/eurostat/documents/2995521/1/"
                "3-01092026-BP-EN.HTML.html/source"
            ),
        ),
        content=b"<html><body>June 2026 table note</body></html>",
    )
    reference_entry = replace(
        entry, summary="Headline-only Atom summary", entry_id=""
    )
    reference, lag = _reference_period_from_publication(
        reference_entry, snapshot, stale_document
    )
    composition = _composition_from_publication(reference, snapshot, None)

    assert title == entry.source_title
    assert page_date == "2026-09-01"
    assert published_at == "2026-09-01T09:00:00Z"
    assert links == (
        "https://ec.europa.eu/eurostat/documents/2995521/1/3-01092026-BP-EN.PDF.pdf/x",
    )
    assert (reference, lag) == ("2026-07", 32)
    assert composition == ("EA21", True)


@pytest.mark.parametrize(
    ("title", "movement", "value"),
    (
        (
            "Euro area unemployment at 6.4%",
            EurostatUnemploymentMovement.REPORTED,
            6.4,
        ),
        (
            "Euro area unemployment stable at 7.1%",
            EurostatUnemploymentMovement.STABLE,
            7.1,
        ),
        (
            "Euro-zone unemployment up to 8.6%",
            EurostatUnemploymentMovement.UP,
            8.6,
        ),
        (
            "Euro area unemployment down to 7.2%",
            EurostatUnemploymentMovement.DOWN,
            7.2,
        ),
    ),
)
def test_unemployment_headline_preserves_rate_and_direction_wording(
    title: str,
    movement: EurostatUnemploymentMovement,
    value: float,
) -> None:
    release = replace(_release_entry(), source_title=title, entry_id="")
    parsed = _headline_value(release, "2026-07")

    assert parsed.measure is EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE
    assert parsed.movement is movement
    assert parsed.value == value
    assert parsed.value_lexical == str(value)


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
def test_unemployment_composition_follows_the_reference_month(
    reference_period: str, composition: str
) -> None:
    assert _ea_composition_for_period(reference_period) == composition


def test_unemployment_totals_table_retains_months_and_ea_de_fr_rates() -> None:
    request = _release_request()
    content = b"""<html><body>
    <h2>Seasonally adjusted unemployment, totals</h2>
    Rates (%) Number of persons (in thousands)
    Jul-25 Apr-26 May-26 Jun-26 Jul-26
    Jul-25 Apr-26 May-26 Jun-26 Jul-26
    Euro area (EA21) 6.3 6.4 6.3 6.4 6.4 11089 11245 11199 11264 11264
    Germany 3.8 3.9 3.9 4.0 4.0 1656 1706 1716 1729 1740
    France 7.7 8.3 8.4 8.3 8.3 2460 2673 2687 2657 2647
    Seasonally adjusted youth unemployment
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2026-07")

    assert len(values) == 15
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2026-07"
    } == {
        ("EA", "2026-07", 6.4),
        ("DE", "2026-07", 4.0),
        ("FR", "2026-07", 8.3),
    }
    assert all(
        item.measure is EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE
        for item in values
    )

    conflicting = replace(
        snapshot,
        content=content.replace(
            b"Seasonally adjusted youth unemployment",
            (
                b"France 7.7 8.3 8.4 8.3 8.4 "
                b"2460 2673 2687 2657 2647 "
                b"Seasonally adjusted youth unemployment"
            ),
        ),
    )
    with pytest.raises(ValueError, match="conflicting totals-table row"):
        _table_published_values(conflicting, artifact, "2026-07")


def test_unemployment_split_year_table_header_retains_monthly_rates() -> None:
    request = _release_request()
    content = b"""<html><body>
    Seasonally adjusted unemployment, totals
    Rates (%) Number of persons (in thousands)
    2023 2024 2023 2024
    Jun Mar Apr May June Jun Mar Apr May June
    Euro area 6.4 6.3 6.4 6.2 6.3 10998 10874 11007 10776 10830
    Germany 3.4 3.6 3.6 3.7 3.7 1491 1580 1599 1609 1611
    France 7.4 7.4 7.4 7.1 7.1 2326 2345 2339 2247 2224
    Seasonally adjusted youth unemployment
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2024-06")

    assert len(values) == 15
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2024-06"
    } == {
        ("EA", "2024-06", 6.3),
        ("DE", "2024-06", 3.7),
        ("FR", "2024-06", 7.1),
    }

    malformed = replace(
        snapshot,
        content=content.replace(
            b"Jun Mar Apr May June Jun Mar Apr May June",
            b"Jun Feb Apr May June Jun Feb Apr May June",
        ),
    )
    with pytest.raises(ValueError, match="totals-table header differs"):
        _table_published_values(malformed, artifact, "2024-06")

    year_boundary = replace(
        snapshot,
        content=content.replace(
            b"2023 2024 2023 2024",
            b"2024 2024 2025 2024 2024 2025",
        ).replace(
            b"Jun Mar Apr May June Jun Mar Apr May June",
            b"Jan Oct Nov Dec Jan Jan Oct Nov Dec Jan",
        ),
    )
    assert (
        len(_table_published_values(year_boundary, artifact, "2025-01")) == 15
    )


def test_unemployment_parallel_composition_rows_select_source_era_ea() -> None:
    request = _release_request()
    content = b"""<html><body>
    Seasonally adjusted unemployment, totals
    Rates (%) Number of persons (in thousands)
    Jan-25 Oct-25 Nov-25 Dec-25 Jan-26
    Jan-25 Oct-25 Nov-25 Dec-25 Jan-26
    Euro area 20 6.3 6.4 6.3 6.3 6.2 10932 11037 10927 10858 10676
    Euro area 21 6.3 6.3 6.3 6.2 6.1 11043 11140 11026 10954 10770
    Germany 3.5 3.9 3.9 4.0 4.0 1553 1711 1729 1743 1749
    France 7.3 7.9 7.9 7.8 7.7 2288 2521 2503 2486 2461
    Seasonally adjusted youth unemployment
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2026-01")
    composition = _composition_from_publication("2026-01", snapshot, None)

    assert len(values) == 15
    assert (
        next(
            item.value
            for item in values
            if item.economy_code == "EA" and item.reference_period == "2026-01"
        )
        == 6.1
    )
    assert composition == ("EA21", True)


def test_unemployment_modern_pdf_table_repairs_split_month() -> None:
    request = _release_request()
    content = b"""<html><body>
    Seasonally adjusted unemployment Totals
    Rates (%) Number of persons (in thousands)
    Apr 12 Jan 13 Feb 13 Ma r 13 Apr 13
    Apr 12 Jan 13 Feb 13 Ma r 13 Apr 13
    EA17 11.2 12.0 12.1 12.1 12.2 17731 19129 19215 19280 19375
    DE6 5.5 5.4 5.4 5.4 5.4 2323 2293 2294 2291 2286
    FR 10.1 10.7 10.8 11.0 11.0 2949 3180 3212 3250 3276
    Seasonally adjusted youth unemployment
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2013-04")

    assert len(values) == 15
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2013-04"
    } == {
        ("EA", "2013-04", 12.2),
        ("DE", "2013-04", 5.4),
        ("FR", "2013-04", 11.0),
    }


def test_unemployment_legacy_totals_table_accepts_member_state_codes() -> None:
    request = _release_request()
    content = b"""<html><body>
    Seasonally adjusted unemployment, totals
    Sep-01 Oct-01 Nov-01 Sep-01 Oct-01 Nov-01
    Euro-zone 8.4 8.5 8.5 11800 11900 12000
    B 6.6 6.7 6.7 400 410 410
    D 7.8 7.9 8.0 3200 3250 3300
    E 10.5 10.4 10.3 2100 2080 2060
    F 8.6 8.7 8.8 2400 2450 2500
    Seasonally adjusted youth unemployment
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2001-11")

    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2001-11"
    } == {
        ("EA", "2001-11", 8.5),
        ("DE", "2001-11", 8.0),
        ("FR", "2001-11", 8.8),
    }


def test_unemployment_legacy_wide_matrix_retains_recent_country_rates() -> None:
    request = _release_request()
    content = b"""<html><body>
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) TOTAL MALES AND FEMALES
    EU15 Euro-zone B DK D GR E F IRL I L NL A P FIN S UK USA JAP
    1999.11 8.8 9.5 8.1 4.9 8.4 : 15.1 10.6 5.0 11.1 2.4 2.8 3.9 4.2 10.0 6.7 5.9 4.1 4.6
    2000.11 7.9 8.5 6.8 4.8 7.7 : 13.6 9.1 3.9 10.0 2.4 2.8 3.6 4.0 9.4 5.4 5.4 4.0 4.8
    2001.10 7.8 8.5 7.0 4.4 8.0 : 13.0 9.1 3.9 9.3 2.5 2.2 4.0 4.0 9.2 5.1 : 5.4 5.4
    2001.11 7.8 8.5 7.0 : 8.0 : 13.0 9.2 4.1 : 2.5 : 4.0 4.2 9.2 5.0 : 5.7 5.4
    MALES EU15 Euro-zone B DK D GR E F IRL I L NL A P FIN S UK USA JAP
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2001-11")

    assert len(values) == 9
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2001-11"
    } == {
        ("EA", "2001-11", 8.5),
        ("DE", "2001-11", 8.0),
        ("FR", "2001-11", 9.2),
    }
    assert {item.reference_period for item in values} == {
        "2000-11",
        "2001-10",
        "2001-11",
    }


@pytest.mark.parametrize(
    "header",
    (
        "EU15 Euro- zone B DK D EL E F IRL I L NL A P FIN S UK USA JAP",
        "EU15 Euro-zone BE DK DE EL ES FR IE IT LU NL AT PT FI SE UK USA JAP",
    ),
)
def test_unemployment_legacy_wide_matrix_accepts_header_transitions(
    header: str,
) -> None:
    request = _release_request()
    content = f"""<html><body>
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) TOTAL MALES AND FEMALES
    {header}
    2002.09 7.8 8.5 7.3 4.6 8.7 9.9 11.5 8.9 4.4 8.9 2.9 2.9 4.3 5.4 9.1 5.0 5.2 5.7 5.5
    2003.09 8.0 8.8 8.0 5.5 9.4 : 11.2 9.5 4.7 : 3.8 : 4.5 7.4 8.9 5.5 : 6.1 5.1
    MALES {header}
    2003.09 7.0 7.7 7.1 4.9 9.5 : 8.3 7.8 4.8 : 2.9 : 4.0 6.1 8.1 5.4 : 6.3 5.2
    </body></html>""".encode()
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2003-09")

    assert len(values) == 6
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2003-09"
    } == {
        ("EA", "2003-09", 8.8),
        ("DE", "2003-09", 9.4),
        ("FR", "2003-09", 9.5),
    }


@pytest.mark.parametrize(
    ("periods", "reference_period"),
    (
        (
            "2003.04 2003.10 2003.11 2003.12 2004.01 2004.02 2004.03 2004.04",
            "2004-04",
        ),
        (
            "Apr 2003 Oct 2003 Nov 2003 Dec 2003 Jan 2004 Feb 2004 Mar 2004 Apr 2004",
            "2004-04",
        ),
        (
            "June 2003 Dec 2003 Jan 2004 Feb 2004 Mar 2004 Apr 2 004 May 2004 June 2004",
            "2004-06",
        ),
        (
            "Aug 2004 Feb 2005 Mar 2005 Apr 2005 May 2005 Jun 20 05 Jul 2005 Aug 2005",
            "2005-08",
        ),
    ),
)
def test_unemployment_legacy_row_table_accepts_month_header_transition(
    periods: str, reference_period: str
) -> None:
    request = _release_request()
    content = f"""<html><body>
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) TOTAL MALES AND FEMALES
    {periods}
    Euro-zone 5 8.9 8.9 8.9 8.9 8.9 8.9 9.0 9.0
    DE 5 9.7 9.7 9.7 9.6 9.6 9.7 9.7 9.8
    FR 9.3 9.5 9.5 9.5 9.5 9.5 9.4 9.4
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) MALES
    {periods}
    Euro-zone 7.2 7.2 7.2 7.2 7.2 7.2 7.3 7.3
    DE 8.1 8.1 8.1 8.0 8.0 8.1 8.1 8.2
    FR 7.5 7.7 7.7 7.7 7.7 7.7 7.6 7.6
    </body></html>""".encode()
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, reference_period)
    composition = _composition_from_publication(
        reference_period, snapshot, None
    )

    assert len(values) == 24
    assert composition == ("EA12", False)
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == reference_period
    } == {
        ("EA", reference_period, 9.0),
        ("DE", reference_period, 9.8),
        ("FR", reference_period, 9.4),
    }


def test_unemployment_legacy_row_table_accepts_pdf_footnotes() -> None:
    request = _release_request()
    content = b"""<html><body>
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) TOTAL MALES AND FEMALES
    Jan 2006 Feb 2006
    Euro-zone5 8.3 8.2
    DE5 9.1 8. 9
    FR 9.2 9.1
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) MALES
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2006-02")
    composition = _composition_from_publication("2006-02", snapshot, None)

    assert len(values) == 6
    assert composition == ("EA12", False)
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2006-02"
    } == {
        ("EA", "2006-02", 8.2),
        ("DE", "2006-02", 8.9),
        ("FR", "2006-02", 9.1),
    }


def test_unemployment_legacy_row_table_selects_compact_composition() -> None:
    request = _release_request()
    content = b"""<html><body>
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) TOTAL MALES AND FEMALES
    Oct 2006 Nov 2006
    EA121 7.7 7.6
    EA131 7.6 7.5
    DE4 8.1 8.0
    FR 8.7 8.6
    SEASONALLY ADJUSTED UNEMPLOYMENT RATES (%) MALES
    </body></html>"""
    snapshot = _snapshot(request, content)
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="3-01092026-bp",
    )

    values = _table_published_values(snapshot, artifact, "2006-11")
    composition = _composition_from_publication("2006-11", snapshot, None)

    assert len(values) == 6
    assert composition == ("EA12", True)
    assert {
        (item.economy_code, item.reference_period, item.value)
        for item in values
        if item.reference_period == "2006-11"
    } == {
        ("EA", "2006-11", 7.6),
        ("DE", "2006-11", 8.0),
        ("FR", "2006-11", 8.6),
    }


def test_unemployment_revision_counts_do_not_cross_ea_compositions() -> None:
    def release(composition: str, value: float) -> SimpleNamespace:
        return SimpleNamespace(
            euro_area_composition=composition,
            published_values=(
                SimpleNamespace(
                    reference_period="2006-12",
                    economy_code="EA",
                    measure=EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE,
                    value=value,
                ),
            ),
        )

    counts = _published_revision_counts(  # type: ignore[arg-type]
        (release("EA12", 7.8), release("EA13", 7.7), release("EA13", 7.6))
    )

    assert counts == (1, 1)


def test_unemployment_pdf_text_tolerates_one_unreadable_page_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_UNEMPLOYMENT_SOURCE_KEY
    )
    request = OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=(
            "https://ec.europa.eu/eurostat/documents/2995521/1/"
            "3-04012002-AP-EN.PDF.pdf/source"
        ),
        source_format=OfficialSourceFormat.PDF,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )
    snapshot = OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "application/pdf"},
        content=b"%PDF-1.4 retained test fixture",
        content_type="application/pdf",
    )

    class _Page:
        def __init__(self, text: str | None) -> None:
            self.text = text

        def extract_text(self) -> str:
            if self.text is None:
                raise PdfReadError("unreadable content stream")
            return self.text

    pages = [_Page("First page"), _Page(None), _Page("Unemployment table")]

    class _Reader:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.pages = pages

    monkeypatch.setattr(unemployment_archive, "PdfReader", _Reader)

    assert unemployment_archive._release_source_text(snapshot) == (
        "First page Unemployment table"
    )

    pages[:] = [_Page(None), _Page(None), _Page("Unemployment table")]
    with pytest.raises(ValueError, match="PDF text extraction failed"):
        unemployment_archive._release_source_text(snapshot)
