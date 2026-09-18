"""Complete-archive qualification tests for Eurostat quarterly GDP."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

import pytest
from pypdf.errors import PdfReadError

import histdatacom.market_context.eurostat_gdp_archive as gdp_archive

from histdatacom.market_context import (
    EUROSTAT_GDP_AS_OF_DATE,
    EUROSTAT_GDP_DOCUMENT_COUNT,
    EUROSTAT_GDP_FIRST_SEARCH_RELEASE_DATE,
    EUROSTAT_GDP_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_GDP_MEASURE_KEY,
    EUROSTAT_GDP_PROGRAM_KEY,
    EUROSTAT_GDP_RELEASE_COUNT,
    EUROSTAT_GDP_SEARCH_PAGE_COUNT,
    EUROSTAT_GDP_SEARCH_RESULT_COUNT,
    EUROSTAT_GDP_SOURCE_KEY,
    EurostatGdpArchiveManifestV1,
    EurostatGdpArtifactRole,
    EurostatGdpGrowthMeasure,
    EurostatGdpInventoryEntryV1,
    EurostatGdpMovement,
    EurostatGdpReleaseStage,
    build_eurostat_gdp_dataset_request,
    build_eurostat_gdp_release_requests,
    build_eurostat_gdp_search_requests,
    load_packaged_eurostat_gdp_archive_manifest,
    load_packaged_official_source_registry,
    packaged_eurostat_gdp_manifest_path,
    parse_eurostat_gdp_dataset,
)
from histdatacom.market_context.eurostat_gdp_archive import (
    _artifact_from_snapshot,
    _parse_release_landing,
    _table_published_values,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
)


def _manifest() -> EurostatGdpArchiveManifestV1:
    return load_packaged_eurostat_gdp_archive_manifest()


def _quarter_range(start: str, end: str) -> tuple[str, ...]:
    year = int(start[:4])
    quarter = int(start[-1])
    result: list[str] = []
    while True:
        value = f"{year:04d}-Q{quarter}"
        result.append(value)
        if value == end:
            return tuple(result)
        quarter += 1
        if quarter == 5:
            year += 1
            quarter = 1


def _raw_snapshot(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "application/json"},
        content=b"{}",
        content_type="application/json",
    )


def test_packaged_gdp_archive_quantifies_inventory_dataset_and_releases() -> (
    None
):
    manifest = _manifest()

    assert EUROSTAT_GDP_SOURCE_KEY == "ea.eurostat.gdp"
    assert EUROSTAT_GDP_PROGRAM_KEY == "ea.eurostat.quarterly-gdp"
    assert EUROSTAT_GDP_MEASURE_KEY == "real-gdp-growth"
    assert EUROSTAT_GDP_SEARCH_PAGE_COUNT == 10
    assert EUROSTAT_GDP_SEARCH_RESULT_COUNT == 932
    assert EUROSTAT_GDP_RELEASE_COUNT == 279
    assert EUROSTAT_GDP_DOCUMENT_COUNT == 248
    assert EUROSTAT_GDP_FIRST_SEARCH_RELEASE_DATE == "2002-01-10"
    assert EUROSTAT_GDP_LATEST_PACKAGED_RELEASE_DATE == "2026-09-07"
    assert manifest.as_of_date == EUROSTAT_GDP_AS_OF_DATE == "2026-09-15"
    assert manifest.inventory.query_result_count == 932
    assert manifest.inventory.title_match_count == 521
    assert manifest.inventory.prefilter_count == 280
    assert len(manifest.inventory.entries) == 279
    assert len(manifest.inventory.exclusions) == 1
    assert len(manifest.inventory.artifacts) == 10
    assert manifest.current_dataset.dataset_id == "namq_10_gdp"
    assert manifest.current_dataset.time_start == "2000-Q1"
    assert manifest.current_dataset.time_end == "2026-Q2"
    assert len(manifest.current_dataset.observations) == 636
    assert manifest.release_count == 279
    assert manifest.release_document_count == 248
    assert manifest.exact_publication_time_count == 46
    assert manifest.published_value_count == 1_582
    assert manifest.revision_comparison_count == 982
    assert manifest.changed_revision_count == 247
    assert manifest.raw_artifact_count == 538
    assert manifest.unique_content_sha256_count == 538
    assert manifest.total_content_bytes == 118_956_446
    assert manifest.manifest_id == (
        "eurostat-gdp-archive-manifest:sha256:"
        "1d7182518601561dfd6b04cd10f1209f9cb64aa3bbdd6632c005af66f15ef1a2"
    )
    assert packaged_eurostat_gdp_manifest_path().is_file()


def test_gdp_search_selection_keeps_explicit_false_positive_and_suffixes() -> (
    None
):
    manifest = _manifest()
    exclusion = manifest.inventory.exclusions[0]
    codes = {item.product_code for item in manifest.inventory.entries}

    assert exclusion.product_code == "2-17102014-bp"
    assert exclusion.reason == (
        "ESA 2010 level-shift methodology item is not a quarterly GDP release"
    )
    assert exclusion.product_code not in codes
    assert {
        "2-14072005-ap2",
        "2-14082007-bp1",
        "2-04122008-ap_0",
    } <= codes


def test_gdp_release_stages_and_reference_quarters_are_not_collapsed() -> None:
    manifest = _manifest()
    stages = Counter(item.stage for item in manifest.releases)
    periods = {item.reference_period for item in manifest.releases}

    assert stages == {
        EurostatGdpReleaseStage.PRELIMINARY_FLASH: 42,
        EurostatGdpReleaseStage.FLASH: 94,
        EurostatGdpReleaseStage.FIRST_ESTIMATE: 35,
        EurostatGdpReleaseStage.SECOND_ESTIMATE: 37,
        EurostatGdpReleaseStage.THIRD_ESTIMATE: 8,
        EurostatGdpReleaseStage.REGULAR_ESTIMATE: 63,
    }
    assert periods == set(_quarter_range("2001-Q3", "2026-Q2"))
    assert manifest.searchable_release_gap_reference_periods == _quarter_range(
        "2000-Q1", "2001-Q2"
    )
    first_period_releases = [
        item for item in manifest.releases if item.reference_period == "2001-Q3"
    ]
    assert [item.stage for item in first_period_releases] == [
        EurostatGdpReleaseStage.SECOND_ESTIMATE,
        EurostatGdpReleaseStage.THIRD_ESTIMATE,
    ]
    assert len({item.occurrence_key for item in manifest.releases}) == 279
    assert all(
        20 <= item.days_after_period_end <= 130 for item in manifest.releases
    )
    third_estimates = [
        item
        for item in manifest.releases
        if item.stage is EurostatGdpReleaseStage.THIRD_ESTIMATE
    ]
    assert [item.inventory_entry.product_code for item in third_estimates] == [
        "2-07022002-ap",
        "2-07052002-ap",
        "2-08082002-ap",
        "2-07112002-ap",
        "2-06022003-ap",
        "2-08052003-ap",
        "2-06082003-ap",
        "2-06112003-ap",
    ]
    assert third_estimates[1].reference_period == "2001-Q4"


def test_gdp_headlines_preserve_direction_magnitude_and_primary_scope() -> None:
    manifest = _manifest()
    values = [item.headline_value for item in manifest.releases]

    assert Counter(item.movement for item in values) == {
        EurostatGdpMovement.UP: 219,
        EurostatGdpMovement.DOWN: 47,
        EurostatGdpMovement.STABLE: 13,
    }
    assert all(item.economy_code == "EA" for item in values)
    assert all(
        item.measure is EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER
        for item in values
    )
    assert all(
        item.reference_period == release.reference_period
        and item.evidence_id == release.inventory_entry.entry_id
        for release, item in zip(manifest.releases, values, strict=True)
    )


def test_gdp_current_dataset_is_an_independent_revised_cross_check() -> None:
    manifest = _manifest()
    observations = manifest.current_dataset.observations

    assert Counter(
        (item.measure, item.economy_code) for item in observations
    ) == {
        (EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER, "EA20"): 106,
        (EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER, "DE"): 106,
        (EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER, "FR"): 106,
        (EurostatGdpGrowthMeasure.YEAR_OVER_YEAR, "EA20"): 106,
        (EurostatGdpGrowthMeasure.YEAR_OVER_YEAR, "DE"): 106,
        (EurostatGdpGrowthMeasure.YEAR_OVER_YEAR, "FR"): 106,
    }
    assert all(
        observation.observation_id
        not in {
            release.headline_value.evidence_id for release in manifest.releases
        }
        for observation in observations
    )


def test_gdp_release_landings_bind_titles_dates_and_document_discovery() -> (
    None
):
    manifest = _manifest()

    assert Counter(
        len(item.linked_document_uris) for item in manifest.releases
    ) == {
        0: 31,
        1: 248,
    }
    assert (
        sum(item.document_artifact is not None for item in manifest.releases)
        == 248
    )
    assert Counter(
        item.document_artifact.source_format
        for item in manifest.releases
        if item.document_artifact is not None
    ) == {
        OfficialSourceFormat.HTML: 32,
        OfficialSourceFormat.PDF: 216,
    }
    assert all(
        item.artifact.product_code == item.inventory_entry.product_code
        for item in manifest.releases
    )
    assert Counter(
        item.page_release_date_offset_days for item in manifest.releases
    ) == {-1: 33, 0: 245, 1: 1}
    assert manifest.page_date_offset_count == 34
    assert [
        item.inventory_entry.product_code
        for item in manifest.releases
        if item.page_release_date_offset_days == 1
    ] == ["2-12042007-ap1"]


def test_gdp_requests_bind_the_complete_retained_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_eurostat_gdp_search_requests(registry)
    dataset = build_eurostat_gdp_dataset_request(registry)
    releases = build_eurostat_gdp_release_requests(registry, manifest.inventory)

    assert len(searches) == 10
    assert [item.page_number for item in searches] == list(range(1, 11))
    assert {item.source_format for item in searches} == {
        OfficialSourceFormat.ATOM
    }
    assert dataset.source_format is OfficialSourceFormat.JSON_STAT
    assert len(releases) == 279
    assert len({item.uri for item in releases}) == 279
    assert {item.source_format for item in releases} == {
        OfficialSourceFormat.HTML
    }
    assert {item.parser_id for item in (*searches, dataset, *releases)} == {
        "official.eurostat-gdp.v1"
    }


def test_gdp_dataset_parser_refuses_request_identity_drift() -> None:
    registry = load_packaged_official_source_registry()
    request = build_eurostat_gdp_dataset_request(registry)
    mutations = (
        (
            replace(request, source_key="ea.eurostat.hicp"),
            "source or parser differs",
        ),
        (
            replace(request, parser_id="official.eurostat-hicp.v1"),
            "source or parser differs",
        ),
        (
            replace(request, parser_version="2"),
            "source or parser differs",
        ),
        (
            replace(request, method=OfficialRequestMethod.POST),
            "snapshot method differs",
        ),
        (
            replace(
                request,
                uri="https://ec.europa.eu/eurostat/not-the-gdp-dataset",
            ),
            "dataset request identity differs",
        ),
        (
            replace(request, page_number=1),
            "dataset request identity differs",
        ),
    )

    for mutated, message in mutations:
        with pytest.raises(ValueError, match=message):
            parse_eurostat_gdp_dataset(_raw_snapshot(mutated))


def test_gdp_release_landing_requires_one_unique_page_date() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    entry = EurostatGdpInventoryEntryV1(
        product_code="2-10012002-ap",
        release_date="2002-01-10",
        source_title="Euro-zone GDP up by 0.3%",
        source_uri=("https://ec.europa.eu/eurostat/product?code=2-10012002-ap"),
        atom_published_at="2002-01-10T00:00:00+00:00",
        atom_updated_at="2002-01-10T00:00:00+00:00",
        summary="Quarterly GDP release",
        page_number=10,
    )
    with pytest.raises(ValueError, match="no unique product code"):
        replace(
            entry,
            source_uri=(
                "https://ec.europa.eu/eurostat/product?"
                "code=2-10012002-ap&code=2-07092026-ap"
            ),
            entry_id="",
        )
    request = OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=entry.source_uri,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )

    def snapshot(dates: str) -> OfficialRawSnapshotV1:
        content = (
            '<html><head><meta property="og:title" '
            f'content="{entry.source_title}"></head><body>{dates}'
            '<a title="Download publication" '
            'href="/eurostat/documents/2-10012002-ap-en.pdf">PDF</a>'
            "</body></html>"
        ).encode()
        return OfficialRawSnapshotV1(
            request=request,
            retrieved_at_ns=1,
            completed_at_ns=1,
            status_code=200,
            resolved_uri=(
                "https://ec.europa.eu/eurostat/en/web/"
                "products-euro-indicators/-/2-10012002-ap"
            ),
            response_headers={"Content-Type": "text/html"},
            content=content,
            content_type="text/html",
        )

    _, page_date, published_at, links = _parse_release_landing(
        snapshot("Release date: 10 January 2002"), entry
    )
    assert page_date == "2002-01-10"
    assert published_at is None
    assert links == (
        "https://ec.europa.eu/eurostat/documents/2-10012002-ap-en.pdf",
    )

    with pytest.raises(ValueError, match="no unique release date"):
        _parse_release_landing(
            snapshot(
                "Release date: 10 January 2002 " "Release date: 9 January 2002"
            ),
            entry,
        )

    exact_content = (
        '<html><head><meta property="og:title" '
        f'content="{entry.source_title}">'
        '<meta property="article:published_time" '
        'content="2002-01-10T10:00:00Z"></head><body>'
        '<a title="Download publication" '
        'href="/eurostat/documents/2-10012002-ap-en.pdf">PDF</a>'
        '<a title="Download publication" '
        'href="/eurostat/documents/2-10012002-ap-de.pdf">PDF</a>'
        "</body></html>"
    ).encode()
    exact = replace(
        snapshot("Release date: 10 January 2002"), content=exact_content
    )
    _, exact_date, exact_published_at, exact_links = _parse_release_landing(
        exact, entry
    )
    assert exact_date == "2002-01-10"
    assert exact_published_at == "2002-01-10T10:00:00Z"
    assert exact_links == (
        "https://ec.europa.eu/eurostat/documents/2-10012002-ap-en.pdf",
    )


def test_gdp_legacy_growth_table_preserves_ea_de_fr_qoq_and_yoy() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    request = OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=(
            "https://ec.europa.eu/eurostat/documents/2995521/5198690/"
            "2-10012002-AP-EN.HTML.html/source"
        ),
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )
    content = """<html><body>
    Legacy release table – retained in its source encoding
    Growth rates of GDP in volume
    Percentage change compared to the previous quarter
    Percentage change compared to the same quarter of the previous year
    2000 2001 Previously released value 2000 2001 Previously released value
    Q4 Q1 Q2 Q3 2001 Q3 Q4 Q1 Q2 Q3 2001 Q3
    Euro-zone 0.6 0.6 0.1 0.1 (0.1) 2.8 2.5 1.6 1.4 (1.4)
    EU15 0.6 0.6 0.1 0.2 2.8 2.5 1.7 1.4
    EU Member States
    B 0.9 0.4 -0.5 0.0 2.9 3.1 1.6 0.7
    D 0.2 0.4 0.0 -0.1 (-0.1) 2.5 1.8 0.6 0.4 (0.4)
    E 0.8 1.3 0.4 0.3 3.4 3.4 2.7 2.8
    F 0.9 0.4 0.2 0.5 (0.5) 3.2 2.9 2.2 2.0 (2.0)
    : Data not available
    </body></html>""".encode("cp1252")
    snapshot = OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "text/html"},
        content=content,
        content_type="text/html",
    )
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatGdpArtifactRole.RELEASE_DOCUMENT_HTML,
        product_code="2-10012002-ap",
    )

    values = _table_published_values(snapshot, artifact, "2001-Q3")

    assert {
        (item.economy_code, item.measure.value): item.value for item in values
    } == {
        ("EA", "quarter-over-quarter"): 0.1,
        ("EA", "year-over-year"): 1.4,
        ("DE", "quarter-over-quarter"): -0.1,
        ("DE", "year-over-year"): 0.4,
        ("FR", "quarter-over-quarter"): 0.5,
        ("FR", "year-over-year"): 2.0,
    }


def test_gdp_modern_growth_table_preserves_ea_de_fr_qoq_and_yoy() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    request = OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=(
            "https://ec.europa.eu/eurostat/en/web/products-euro-indicators/"
            "w/2-30072024-ap"
        ),
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )
    content = b"""<html><body>
    Growth rates of GDP in volume
    Percentage change compared with the previous quarter
    Percentage change compared with the same quarter of the previous year
    2023 2024 2023 2024 Q3 Q4 Q1 Q2 Q3 Q4 Q1 Q2
    Euro area -0.1 0.0 0.3 0.0 0.0 0.2 0.5 0.1
    Germany 0.1 -0.5 0.2 -0.3 -0.3 -0.2 -0.1 -0.2
    France 0.1 0.4 0.2 0.1-0.8 1.1 1.4 0.7
    Growth rates of employment
    </body></html>"""
    snapshot = OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "text/html"},
        content=content,
        content_type="text/html",
    )
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatGdpArtifactRole.RELEASE_HTML,
        product_code="2-30072024-ap",
    )

    values = _table_published_values(snapshot, artifact, "2024-Q2")

    assert {
        (item.economy_code, item.measure.value): item.value for item in values
    } == {
        ("EA", "quarter-over-quarter"): 0.0,
        ("EA", "year-over-year"): 0.1,
        ("DE", "quarter-over-quarter"): -0.3,
        ("DE", "year-over-year"): -0.2,
        ("FR", "quarter-over-quarter"): 0.1,
        ("FR", "year-over-year"): 0.7,
    }

    duplicated_table = b"""
    Growth rates of GDP in volume
    2023 2024 2023 2024 Q3 Q4 Q1 Q2 Q3 Q4 Q1 Q2
    Euro area -0.1 0.0 0.3 0.0 0.0 0.2 0.5 0.1
    Germany 0.1 -0.5 0.2 -0.3 -0.3 -0.2 -0.1 -0.2
    France 0.1 0.4 0.2 0.1-0.8 1.1 1.4 0.7
    """
    duplicated = replace(
        snapshot,
        content=content.replace(
            b"Growth rates of employment",
            duplicated_table + b"Growth rates of employment",
        ),
    )
    assert _table_published_values(duplicated, artifact, "2024-Q2") == values

    conflicting = replace(
        duplicated,
        content=duplicated.content.replace(
            b"Euro area -0.1 0.0 0.3 0.0 0.0 0.2 0.5 0.1",
            b"Euro area -0.1 0.0 0.4 0.0 0.0 0.2 0.5 0.1",
            1,
        ),
    )
    with pytest.raises(ValueError, match="repeats a conflicting GDP table row"):
        _table_published_values(conflicting, artifact, "2024-Q2")


@pytest.mark.parametrize(
    ("reference_period", "rows", "expected_qoq"),
    (
        (
            "2022-Q4",
            "EA19 0.0 0.1 1.0 1.1 EA20 0.0 0.9 1.0 1.9",
            0.1,
        ),
        (
            "2013-Q4",
            "EA171 0.0 0.2 1.0 1.2 EA18 0.0 0.8 1.0 1.8",
            0.2,
        ),
    ),
)
def test_gdp_table_selects_the_source_period_euro_area_composition(
    reference_period: str,
    rows: str,
    expected_qoq: float,
) -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    request = OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri="https://ec.europa.eu/eurostat/product?code=2-30072024-ap",
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )
    year = reference_period[:4]
    quarter = f"Q{reference_period[-1]}"
    snapshot = OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "text/html"},
        content=(
            f"<html><body>Growth rates of GDP in volume {year} {quarter} "
            f"{rows} Growth rates of employment</body></html>"
        ).encode(),
        content_type="text/html",
    )
    artifact = _artifact_from_snapshot(
        snapshot,
        EurostatGdpArtifactRole.RELEASE_HTML,
        product_code="2-30072024-ap",
    )

    values = _table_published_values(snapshot, artifact, reference_period)

    assert {
        item.measure: item.value for item in values if item.economy_code == "EA"
    }[EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER] == expected_qoq


def test_gdp_pdf_text_tolerates_one_unreadable_page_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    request = OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri="https://ec.europa.eu/eurostat/documents/2-06032003-ap-en.pdf",
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

    pages = [_Page("First page"), _Page(None), _Page("GDP table")]

    class _Reader:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.pages = pages

    monkeypatch.setattr(gdp_archive, "PdfReader", _Reader)

    assert gdp_archive._release_source_text(snapshot) == "First page GDP table"

    pages[:] = [_Page(None), _Page(None), _Page("GDP table")]
    with pytest.raises(ValueError, match="PDF text extraction failed"):
        gdp_archive._release_source_text(snapshot)


def test_gdp_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EurostatGdpArchiveManifestV1.from_json(manifest.to_json()) == manifest
    )
    with pytest.raises(ValueError, match="release-stage counts differ"):
        replace(manifest, flash_count=manifest.flash_count - 1, manifest_id="")
    with pytest.raises(ValueError, match="archive boundary differs"):
        replace(manifest, as_of_date="2026-09-16", manifest_id="")
    with pytest.raises(ValueError, match="dataset cube coverage differs"):
        replace(
            manifest.current_dataset,
            observations=manifest.current_dataset.observations[:-1],
            dataset_receipt_id="",
        )

    first = manifest.releases[0]
    assert first.stage is EurostatGdpReleaseStage.SECOND_ESTIMATE
    altered_stage = replace(
        first,
        stage=EurostatGdpReleaseStage.PRELIMINARY_FLASH,
        release_id="",
    )
    with pytest.raises(ValueError, match="release-stage evidence differs"):
        replace(
            manifest,
            releases=(altered_stage, *manifest.releases[1:]),
            preliminary_flash_count=manifest.preliminary_flash_count + 1,
            second_estimate_count=manifest.second_estimate_count - 1,
            manifest_id="",
        )

    magnitude = float(first.headline_value.magnitude_lexical) + 0.1
    signed = (
        -magnitude
        if first.headline_value.movement is EurostatGdpMovement.DOWN
        else magnitude
    )
    altered_value = replace(
        first.headline_value,
        magnitude_lexical=f"{magnitude:g}",
        value=signed,
        release_value_id="",
    )
    published_values = tuple(
        (
            replace(
                item,
                value_lexical=f"{signed:g}",
                value=signed,
                published_value_id="",
            )
            if item.economy_code == "EA"
            and item.measure is EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER
            else item
        )
        for item in first.published_values
    )
    altered_headline = replace(
        first,
        headline_value=altered_value,
        published_values=published_values,
        release_id="",
    )
    with pytest.raises(ValueError, match="release headline evidence differs"):
        replace(
            manifest,
            releases=(altered_headline, *manifest.releases[1:]),
            manifest_id="",
        )

    payload = json.loads(manifest.to_json())
    payload["searchable_release_gap_reference_periods"] = payload[
        "searchable_release_gap_reference_periods"
    ][1:]
    payload["manifest_id"] = ""
    with pytest.raises(ValueError, match="gap inventory differs"):
        EurostatGdpArchiveManifestV1.from_json(json.dumps(payload))


def test_gdp_manifest_binds_the_reviewed_registry_source() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    manifest = _manifest()

    assert manifest.registry_id == registry.registry_id
    assert manifest.source_id == source.source_id
    assert source.availability_time_precision.value == "date-only"
    assert source.source_table_ids == ("namq_10_gdp",)
    assert "quarterly GDP third estimate" in source.source_release_ids
    assert source.parser_id == "official.eurostat-gdp.v1"
