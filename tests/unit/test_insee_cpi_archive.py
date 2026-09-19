"""Qualification tests for the official INSEE CPI archive."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from histdatacom.market_context import (
    INSEE_CPI_CURRENT_START_PERIOD,
    INSEE_CPI_FINAL_START_PERIOD,
    INSEE_CPI_LATEST_PACKAGED_PERIOD,
    INSEE_CPI_PARSER_ID,
    INSEE_CPI_PROGRAM_KEY,
    INSEE_CPI_PROVISIONAL_START_PERIOD,
    INSEE_CPI_SOURCE_KEY,
    InseeCpiArchiveManifestV1,
    InseeCpiReleaseStage,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRole,
    OfficialSourceVerificationStatus,
    build_insee_cpi_catalog_requests,
    build_insee_cpi_current_series_requests,
    build_insee_cpi_release_requests,
    build_insee_cpi_search_request,
    load_packaged_insee_cpi_archive_manifest,
    load_packaged_official_source_registry,
    packaged_insee_cpi_manifest_path,
)


def _manifest() -> InseeCpiArchiveManifestV1:
    return load_packaged_insee_cpi_archive_manifest()


def _values(
    manifest: InseeCpiArchiveManifestV1,
    period: str,
    stage: InseeCpiReleaseStage,
) -> dict[tuple[str, str], float]:
    release = next(
        item
        for item in manifest.releases
        if item.reference_period == period and item.stage is stage
    )
    return {
        (item.measure, item.change_basis): item.value for item in release.values
    }


def test_packaged_insee_cpi_archive_quantifies_complete_lineage() -> None:
    manifest = _manifest()

    assert INSEE_CPI_PROGRAM_KEY == "fr.insee.consumer-price-index"
    assert INSEE_CPI_SOURCE_KEY == "fr.insee.cpi"
    assert INSEE_CPI_FINAL_START_PERIOD == "2009-05"
    assert INSEE_CPI_PROVISIONAL_START_PERIOD == "2016-01"
    assert INSEE_CPI_CURRENT_START_PERIOD == "2000-01"
    assert INSEE_CPI_LATEST_PACKAGED_PERIOD == "2026-08"
    assert manifest.search.as_of_date == "2026-09-18"
    assert manifest.search.total_result_count == 583
    assert len(manifest.search.exclusions) == 247
    assert len(manifest.releases) == 336
    assert manifest.raw_artifact_count == 343
    assert manifest.unique_content_sha256_count == 343
    assert manifest.total_content_bytes == 29_835_873
    assert manifest.released_value_count == 1_468
    assert len(manifest.revisions) == 384
    assert manifest.nonzero_revision_count == 106
    assert manifest.manifest_id == (
        "insee-cpi-archive-manifest:sha256:"
        "27547e62d679aa01e154bc4b2df817b7a222cef9faf62574ed0b4b9aeec8bb99"
    )
    assert packaged_insee_cpi_manifest_path().is_file()


def test_insee_cpi_registry_entry_is_empirically_verified_and_non_primary() -> (
    None
):
    registry = load_packaged_official_source_registry()
    source = registry.source(INSEE_CPI_SOURCE_KEY)

    assert source.source_id == (
        "official-source:sha256:"
        "07aab456e5ed68c6975e26ccc0786504f6aad9e89e9344260f781ca49335cd5a"
    )
    assert source.parser_id == INSEE_CPI_PARSER_ID
    assert source.verification_status is (
        OfficialSourceVerificationStatus.EMPIRICALLY_VERIFIED
    )
    assert source.reviewed_on == "2026-09-18"
    assert source.roles == (
        OfficialSourceRole.OFFICIAL_ARCHIVE,
        OfficialSourceRole.RELEASE_CALENDAR,
    )
    assert OfficialSourceRole.PRIMARY_PRODUCER not in source.roles
    assert source.formats == (
        OfficialSourceFormat.JSON,
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.SDMX_21,
    )
    assert source.source_series_ids == (
        "011812232",
        "011814145",
        "011814631",
        "011814632",
    )


def test_insee_cpi_inventory_is_gap_free_and_accounts_for_every_result() -> (
    None
):
    manifest = _manifest()

    assert Counter(item.stage for item in manifest.releases) == {
        InseeCpiReleaseStage.FINAL: 208,
        InseeCpiReleaseStage.PROVISIONAL: 128,
    }
    assert Counter(item.reason for item in manifest.search.exclusions) == {
        "search-result-outside-release-lineage": 210,
        "missing-subtitle": 35,
        "other-consumer-price-publication": 2,
    }
    assert (
        len(manifest.search.selected_document_ids)
        + len(manifest.search.exclusions)
        == manifest.search.total_result_count
    )
    assert tuple(item.source_document_id for item in manifest.releases) == (
        manifest.search.selected_document_ids
    )
    assert manifest.releases[0].reference_period == "2009-05"
    assert manifest.releases[-1].reference_period == "2026-08"
    assert manifest.releases[-1].stage is InseeCpiReleaseStage.FINAL


def test_insee_cpi_release_values_cover_legacy_and_current_table_eras() -> None:
    manifest = _manifest()
    legacy = _values(manifest, "2009-05", InseeCpiReleaseStage.FINAL)
    latest = _values(manifest, "2026-08", InseeCpiReleaseStage.FINAL)

    assert legacy == {
        ("core-inflation", "month-on-month"): 0.1,
        ("core-inflation", "year-on-year"): 1.6,
        ("cpi-all-items", "month-on-month"): 0.2,
        ("cpi-all-items", "year-on-year"): -0.3,
        ("hicp-all-items", "month-on-month"): 0.1,
        ("hicp-all-items", "year-on-year"): -0.3,
    }
    assert latest == {
        ("core-inflation", "month-on-month"): -0.2,
        ("core-inflation", "year-on-year"): 1.1,
        ("cpi-all-items", "month-on-month"): 0.7,
        ("cpi-all-items", "year-on-year"): 2.4,
        ("hicp-all-items", "month-on-month"): 0.7,
        ("hicp-all-items", "year-on-year"): 2.6,
    }
    assert Counter(
        (value.measure, value.change_basis)
        for release in manifest.releases
        for value in release.values
    ) == {
        ("cpi-all-items", "month-on-month"): 336,
        ("cpi-all-items", "year-on-year"): 336,
        ("hicp-all-items", "year-on-year"): 295,
        ("core-inflation", "month-on-month"): 167,
        ("core-inflation", "year-on-year"): 167,
        ("hicp-all-items", "month-on-month"): 167,
    }


def test_insee_cpi_revisions_keep_provisional_and_final_values_distinct() -> (
    None
):
    manifest = _manifest()
    january = {
        (item.measure, item.change_basis): (
            item.provisional_value,
            item.final_value,
            item.delta,
        )
        for item in manifest.revisions
        if item.reference_period == "2025-01"
    }

    assert january == {
        ("cpi-all-items", "month-on-month"): (-0.1, 0.2, 0.3),
        ("cpi-all-items", "year-on-year"): (1.4, 1.7, 0.3),
        ("hicp-all-items", "year-on-year"): (1.8, 1.8, 0.0),
    }
    assert max(abs(item.delta) for item in manifest.revisions) <= 0.3


def test_insee_cpi_current_series_are_explicit_latest_revised_cross_checks() -> (
    None
):
    manifest = _manifest()
    latest = {
        item.series_id: item.observations[-1].value
        for item in manifest.current_series
    }

    assert latest == {
        "011814631": 0.7,
        "011814632": 2.4,
        "011814145": 1.1,
        "011812232": 2.6,
    }
    assert all(
        len(item.observations) == 320 for item in manifest.current_series
    )
    assert {
        (
            item.observations[0].reference_period,
            item.observations[-1].reference_period,
        )
        for item in manifest.current_series
    } == {("2000-01", "2026-08")}
    assert {item.last_update for item in manifest.current_series} == {
        "2026-09-15"
    }


def test_insee_cpi_requests_bind_exact_methods_formats_and_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    search = build_insee_cpi_search_request(registry)
    catalogs = build_insee_cpi_catalog_requests(registry)
    series = build_insee_cpi_current_series_requests(registry)
    releases = build_insee_cpi_release_requests(
        registry,
        ({"id": item.source_document_id} for item in manifest.releases),
    )

    assert search.method is OfficialRequestMethod.POST
    assert search.source_format is OfficialSourceFormat.JSON
    assert json.loads(search.body_text or "") == {
        "facetsQuery": [],
        "filters": [
            {"field": "diffusion", "values": [True]},
            {"field": "rubrique", "values": ["statistiques"]},
        ],
        "q": "consumer price index",
        "rows": 1000,
        "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
        "start": 0,
    }
    assert len(catalogs) == 2
    assert {item.method for item in catalogs} == {OfficialRequestMethod.POST}
    assert len(series) == 4
    assert {item.source_format for item in series} == {
        OfficialSourceFormat.SDMX_21
    }
    assert len(releases) == 336
    assert len({item.uri for item in releases}) == 336
    assert {item.source_format for item in releases} == {
        OfficialSourceFormat.HTML
    }
    assert {
        item.parser_id for item in (search, *catalogs, *series, *releases)
    } == {INSEE_CPI_PARSER_ID}


def test_insee_cpi_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert InseeCpiArchiveManifestV1.from_json(manifest.to_json()) == manifest

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["values"][0]["value"] = 99.0
    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        InseeCpiArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["manifest_id"] = "insee-cpi-archive-manifest:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="manifest identity differs"):
        InseeCpiArchiveManifestV1.from_json(json.dumps(payload))
