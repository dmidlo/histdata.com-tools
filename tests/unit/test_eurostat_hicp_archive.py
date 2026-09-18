"""Complete-archive qualification tests for Eurostat HICP releases."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    EUROSTAT_HICP_AS_OF_DATE,
    EUROSTAT_HICP_FINAL_START_REFERENCE_PERIOD,
    EUROSTAT_HICP_FIRST_FLASH_REFERENCE_PERIOD,
    EUROSTAT_HICP_LATEST_PACKAGED_RELEASE_DATE,
    EUROSTAT_HICP_MEASURE_KEY,
    EUROSTAT_HICP_MIGRATED_INDEX_START_DATE,
    EUROSTAT_HICP_PROGRAM_KEY,
    EUROSTAT_HICP_SOURCE_KEY,
    EurostatHicpArchiveManifestV1,
    EurostatHicpArtifactRole,
    EurostatHicpPublicationV1,
    EurostatHicpReleaseStage,
    EurostatHicpValueProvenance,
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    build_eurostat_hicp_dataset_requests,
    build_eurostat_hicp_document_requests,
    build_eurostat_hicp_index_requests,
    build_eurostat_hicp_legacy_release_requests,
    build_eurostat_hicp_release_requests,
    load_packaged_eurostat_hicp_archive_manifest,
    load_packaged_official_source_registry,
    packaged_eurostat_hicp_manifest_path,
)


def _manifest() -> EurostatHicpArchiveManifestV1:
    return load_packaged_eurostat_hicp_archive_manifest()


def _month_range(start: str, end: str) -> tuple[str, ...]:
    year, month = (int(item) for item in start.split("-"))
    last_year, last_month = (int(item) for item in end.split("-"))
    values: list[str] = []
    while (year, month) <= (last_year, last_month):
        values.append(f"{year:04d}-{month:02d}")
        year, month = divmod(year * 12 + month, 12)
        month += 1
    return tuple(values)


def test_packaged_hicp_archive_quantifies_both_first_published_tables() -> None:
    manifest = _manifest()

    assert EUROSTAT_HICP_PROGRAM_KEY == "ea.eurostat.hicp-first-releases"
    assert EUROSTAT_HICP_MEASURE_KEY == "hicp-all-items-annual-rate"
    assert EUROSTAT_HICP_FINAL_START_REFERENCE_PERIOD == "2000-01"
    assert EUROSTAT_HICP_FIRST_FLASH_REFERENCE_PERIOD == "2001-10"
    assert EUROSTAT_HICP_MIGRATED_INDEX_START_DATE == "2004-01-05"
    assert EUROSTAT_HICP_LATEST_PACKAGED_RELEASE_DATE == "2026-09-01"
    assert manifest.as_of_date == EUROSTAT_HICP_AS_OF_DATE == "2026-09-15"
    assert manifest.legacy_dataset.dataset_id == "prc_hicp_fp"
    assert manifest.legacy_dataset.taxonomy == "ecoicop-v1"
    assert manifest.legacy_dataset.time_start == "2000-01"
    assert manifest.legacy_dataset.time_end == "2025-12"
    assert len(manifest.legacy_dataset.observations) == 1_238
    assert manifest.current_dataset.dataset_id == "prc_hicp_fpd"
    assert manifest.current_dataset.taxonomy == "ecoicop-v2"
    assert manifest.current_dataset.time_start == "2000-01"
    assert manifest.current_dataset.time_end == "2026-08"
    assert len(manifest.current_dataset.observations) == 981
    assert len(manifest.index_artifacts) == 180
    assert manifest.migrated_publication_count == 539
    assert len(manifest.index_exclusions) == 1
    assert manifest.raw_artifact_count == 829
    assert manifest.unique_content_sha256_count == 829
    assert manifest.total_content_bytes == 189_367_122
    assert manifest.manifest_id == (
        "eurostat-hicp-archive-manifest:sha256:"
        "11c7636605dad4e792126b1b04625f323638993bbd35f61a0f5d5280ca33c7f6"
    )
    exclusion = manifest.index_exclusions[0]
    assert exclusion.index_page_number == 103
    assert exclusion.reason == "non-euro-area-scope"
    assert exclusion.source_title == (
        "G20 annual inflation slows to 3.0% in August 2013"
    )
    assert packaged_eurostat_hicp_manifest_path().is_file()


def test_hicp_dataset_scope_stage_and_overlap_differences_are_explicit() -> (
    None
):
    manifest = _manifest()
    legacy_counts = Counter(
        (item.stage.value, item.economy_code)
        for item in manifest.legacy_dataset.observations
    )
    current_counts = Counter(
        (item.stage.value, item.economy_code)
        for item in manifest.current_dataset.observations
    )

    assert legacy_counts == {
        ("final", "EA"): 312,
        ("final", "DE"): 312,
        ("final", "FR"): 312,
        ("flash", "EA"): 101,
        ("flash", "DE"): 100,
        ("flash", "FR"): 101,
    }
    assert current_counts == {
        ("final", "EA"): 319,
        ("final", "DE"): 319,
        ("final", "FR"): 319,
        ("flash", "EA"): 8,
        ("flash", "DE"): 8,
        ("flash", "FR"): 8,
    }
    assert manifest.overlap_count == 936
    assert len(manifest.overlap_differences) == 365
    assert manifest.numeric_overlap_difference_count == 173
    assert manifest.status_overlap_difference_count == 204
    assert Counter(
        item.economy_code
        for item in manifest.overlap_differences
        if item.numeric_difference
    ) == {"EA": 34, "DE": 124, "FR": 15}
    assert Counter(
        item.economy_code
        for item in manifest.overlap_differences
        if item.status_difference
    ) == {"FR": 204}


def test_hicp_publications_and_explicit_artifact_gaps_cover_each_month() -> (
    None
):
    manifest = _manifest()
    final_published = {
        item.reference_period
        for item in manifest.publications
        if item.stage is EurostatHicpReleaseStage.FINAL
    }
    flash_published = {
        item.reference_period
        for item in manifest.publications
        if item.stage is EurostatHicpReleaseStage.FLASH
    }

    assert final_published | set(
        manifest.final_artifact_gap_reference_periods
    ) == set(_month_range("2000-01", "2026-07"))
    assert flash_published | set(
        manifest.flash_artifact_gap_reference_periods
    ) == set(_month_range("2001-10", "2026-08"))
    assert final_published.isdisjoint(
        manifest.final_artifact_gap_reference_periods
    )
    assert flash_published.isdisjoint(
        manifest.flash_artifact_gap_reference_periods
    )
    assert manifest.final_release_count + manifest.flash_release_count == (
        manifest.release_count
    )
    assert manifest.final_release_count == 289
    assert manifest.flash_release_count == 290
    assert manifest.page_date_offset_count == 40
    assert manifest.card_date_offset_count == 15
    assert (
        manifest.migrated_publication_count + manifest.legacy_unindexed_count
        == (manifest.release_count)
    )
    assert manifest.publications[0].release_date < "2004-01-05"
    assert manifest.publications[-1].release_date == "2026-09-01"
    by_release_date = {
        item.release_date: item for item in manifest.publications
    }
    assert by_release_date["2013-02-28"].stage is EurostatHicpReleaseStage.FINAL
    assert by_release_date["2013-02-28"].reference_period == "2013-01"
    assert by_release_date["2026-01-07"].stage is EurostatHicpReleaseStage.FLASH
    assert by_release_date["2026-01-07"].reference_period == "2025-12"


def test_hicp_release_artifacts_preserve_source_era_formats_and_dates() -> None:
    manifest = _manifest()
    legacy = [
        item for item in manifest.releases if item.publication.legacy_unindexed
    ]

    assert len(legacy) == 40
    assert Counter(item.publication.stage.value for item in legacy) == {
        "final": 22,
        "flash": 18,
    }
    assert all(len(item.artifacts) == 2 for item in legacy)
    assert Counter(item.artifacts[1].role for item in legacy) == {
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML: 38,
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF: 2,
    }
    assert Counter(item.artifacts[0].role for item in manifest.releases) == {
        EurostatHicpArtifactRole.RELEASE_LANDING_HTML: 130,
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML: 16,
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF: 433,
    }
    assert Counter(
        artifact.role
        for item in manifest.releases
        for artifact in item.artifacts[1:]
    ) == {
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML: 38,
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF: 30,
    }
    assert Counter(len(item.artifacts) for item in manifest.releases) == {
        1: 511,
        2: 68,
    }
    assert all(
        item.publication.page_release_date_offset_days == -1
        and item.publication.card_date is None
        for item in legacy
    )
    assert manifest.raw_artifact_count == (
        2
        + len(manifest.index_artifacts)
        + sum(len(item.artifacts) for item in manifest.releases)
    )
    corrected = [
        item for item in manifest.publications if not item.release_date_from_uri
    ]
    assert len(corrected) == 1
    assert corrected[0].release_date == "2011-02-28"
    assert "2-2802211-AP-EN.PDF.pdf" in corrected[0].source_uri


def test_hicp_release_values_keep_stage_scope_and_vintage_provenance() -> None:
    manifest = _manifest()
    releases = manifest.releases

    assert all(item.values[0].economy_code == "EA" for item in releases)
    assert all(
        item.values[0].provenance
        is EurostatHicpValueProvenance.RELEASE_HEADLINE
        for item in releases
    )
    assert all(
        value.stage is item.publication.stage
        and value.reference_period == item.publication.reference_period
        for item in releases
        for value in item.values
    )
    compared = [item for item in releases if item.dataset_ea_value is not None]
    different = [
        item for item in compared if item.headline_dataset_numeric_difference
    ]
    assert len(compared) == manifest.headline_dataset_comparison_count
    assert len(different) == manifest.headline_dataset_difference_count
    assert manifest.headline_dataset_comparison_count == 398
    assert manifest.headline_dataset_difference_count == 38
    assert manifest.headline_only_release_count == 181
    assert all(
        item.dataset_ea_value is not None
        and item.dataset_ea_value.economy_code == "EA"
        and item.dataset_ea_value.provenance
        is not EurostatHicpValueProvenance.RELEASE_HEADLINE
        for item in compared
    )
    assert all(
        value.provenance is EurostatHicpValueProvenance.LEGACY_DATASET
        for item in releases
        if item.publication.reference_period <= "2025-12"
        for value in item.values[1:]
    )
    assert all(
        value.provenance is EurostatHicpValueProvenance.CURRENT_DATASET
        for item in releases
        if item.publication.reference_period >= "2026-01"
        for value in item.values[1:]
    )


def test_hicp_requests_bind_the_complete_retained_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    datasets = build_eurostat_hicp_dataset_requests(registry)
    indexes = build_eurostat_hicp_index_requests(
        registry, page_count=len(manifest.index_artifacts)
    )
    releases = build_eurostat_hicp_release_requests(
        registry, manifest.publications
    )

    assert len(datasets) == 2
    assert {item.source_format for item in datasets} == {
        OfficialSourceFormat.JSON_STAT
    }
    assert len(indexes) == 180
    assert {item.page_number for item in indexes} == set(range(1, 181))
    assert len(releases) == manifest.release_count
    assert len({item.uri for item in releases}) == manifest.release_count
    assert Counter(item.source_format for item in releases) == {
        OfficialSourceFormat.HTML: 146,
        OfficialSourceFormat.PDF: 433,
    }
    assert {item.source_format for item in (*indexes, *releases)} == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert {item.parser_id for item in (*datasets, *indexes, *releases)} == {
        "official.eurostat-hicp.v1"
    }


def test_hicp_legacy_landing_discovers_its_exact_english_document() -> None:
    registry = load_packaged_official_source_registry()
    landing = build_eurostat_hicp_legacy_release_requests(
        registry,
        (
            (
                "https://ec.europa.eu/eurostat/en/web/"
                "products-euro-indicators/-/2-02042002-ap"
            ),
        ),
    )[0]
    document_uri = (
        "https://ec.europa.eu/eurostat/documents/2995521/5196002/"
        "2-02042002-AP-EN.HTML.html/"
        "ff9928db-d596-4440-8d45-7b2d8979a349?t=1414686548000"
    )
    snapshot = OfficialRawSnapshotV1(
        request=landing,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=landing.uri,
        response_headers={"Content-Type": "text/html"},
        content=(
            '<html><a title="Download publication" href="'
            + document_uri
            + '">Download publication (EN)</a></html>'
        ).encode(),
        content_type="text/html",
    )

    requests = build_eurostat_hicp_document_requests(registry, (snapshot,))

    assert len(requests) == 1
    assert requests[0].uri == document_uri
    assert requests[0].source_format is OfficialSourceFormat.HTML
    assert requests[0].parser_id == "official.eurostat-hicp.v1"


def test_hicp_modern_landing_ignores_cross_release_document_links() -> None:
    registry = load_packaged_official_source_registry()
    landing = build_eurostat_hicp_release_requests(
        registry,
        (
            EurostatHicpPublicationV1(
                release_date="2024-02-22",
                release_date_from_uri=True,
                reference_period="2024-01",
                stage=EurostatHicpReleaseStage.FINAL,
                source_title="Annual inflation down to 2.8% in the euro area",
                source_uri=(
                    "https://ec.europa.eu/eurostat/en/web/"
                    "products-euro-indicators/w/2-22022024-ap"
                ),
                headline_value_lexical="2.8",
                headline_value=2.8,
                page_release_date=None,
                page_release_date_offset_days=None,
                card_date="2024-02-22",
                card_date_offset_days=0,
                index_page_number=32,
                legacy_unindexed=False,
            ),
        ),
    )[0]
    previous_release_document = (
        "https://ec.europa.eu/eurostat/documents/2995521/18426685/"
        "2-01022024-AP-EN.pdf/5193daed-bd77-c9e9-07ff-71b28061152e"
        "?version=1.0&t=1706712136852&download=true"
    )
    snapshot = OfficialRawSnapshotV1(
        request=landing,
        retrieved_at_ns=1,
        completed_at_ns=1,
        status_code=200,
        resolved_uri=landing.uri,
        response_headers={"Content-Type": "text/html"},
        content=(
            '<html><a href="'
            + previous_release_document
            + '">Previous release</a></html>'
        ).encode(),
        content_type="text/html",
    )

    assert build_eurostat_hicp_document_requests(registry, (snapshot,)) == ()


def test_hicp_direct_ap1_document_keeps_its_exact_request_identity() -> None:
    registry = load_packaged_official_source_registry()
    uri = (
        "https://ec.europa.eu/eurostat/documents/2995521/5106630/"
        "2-30062008-AP1-EN.PDF.pdf/"
        "06e1c057-094f-43a0-9fca-06f74fef7126?t=1414684512000"
    )
    publication = EurostatHicpPublicationV1(
        release_date="2008-06-30",
        release_date_from_uri=True,
        reference_period="2008-06",
        stage=EurostatHicpReleaseStage.FLASH,
        source_title="Euro area inflation estimated at 4.0%",
        source_uri=uri,
        headline_value_lexical="4.0",
        headline_value=4.0,
        page_release_date=None,
        page_release_date_offset_days=None,
        card_date="2008-06-30",
        card_date_offset_days=0,
        index_page_number=145,
        legacy_unindexed=False,
    )

    request = build_eurostat_hicp_release_requests(registry, (publication,))[0]

    assert request.uri == uri
    assert request.source_format is OfficialSourceFormat.PDF


def test_hicp_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EurostatHicpArchiveManifestV1.from_json(manifest.to_json()) == manifest
    )
    with pytest.raises(
        ValueError, match="final artifact gap inventory differs"
    ):
        replace(
            manifest,
            final_artifact_gap_reference_periods=(
                manifest.final_artifact_gap_reference_periods[1:]
            ),
        )

    payload = json.loads(manifest.to_json())
    payload["overlap_differences"] = payload["overlap_differences"][1:]
    payload["manifest_id"] = ""
    with pytest.raises(ValueError, match="overlap audit differs from datasets"):
        EurostatHicpArchiveManifestV1.from_json(json.dumps(payload))


def test_hicp_manifest_binds_the_reviewed_registry_source() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_HICP_SOURCE_KEY)
    manifest = _manifest()

    assert manifest.registry_id == registry.registry_id
    assert manifest.source_id == source.source_id
    assert source.availability_time_precision.value == "date-only"
    assert source.source_table_ids == ("prc_hicp_fp", "prc_hicp_fpd")
