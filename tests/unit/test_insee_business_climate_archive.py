"""Qualification tests for the official INSEE business-climate archive."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from histdatacom.market_context import (
    INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID,
    INSEE_BUSINESS_CLIMATE_CURRENT_START_PERIOD,
    INSEE_BUSINESS_CLIMATE_LATEST_PACKAGED_PERIOD,
    INSEE_BUSINESS_CLIMATE_PARSER_ID,
    INSEE_BUSINESS_CLIMATE_PROGRAM_KEY,
    INSEE_BUSINESS_CLIMATE_RELEASE_GAPS,
    INSEE_BUSINESS_CLIMATE_RELEASE_START_PERIOD,
    INSEE_BUSINESS_CLIMATE_SOURCE_KEY,
    InseeBusinessClimateArchiveManifestV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRole,
    OfficialSourceVerificationStatus,
    build_insee_business_climate_catalog_request,
    build_insee_business_climate_current_series_request,
    build_insee_business_climate_release_requests,
    build_insee_business_climate_search_requests,
    load_packaged_insee_business_climate_archive_manifest,
    load_packaged_official_source_registry,
    packaged_insee_business_climate_manifest_path,
)


def _manifest() -> InseeBusinessClimateArchiveManifestV1:
    return load_packaged_insee_business_climate_archive_manifest()


def _release(manifest: InseeBusinessClimateArchiveManifestV1, period: str):
    return next(
        item for item in manifest.releases if item.reference_period == period
    )


def test_packaged_insee_business_climate_archive_quantifies_lineage() -> None:
    manifest = _manifest()

    assert INSEE_BUSINESS_CLIMATE_PROGRAM_KEY == ("fr.insee.business-climate")
    assert INSEE_BUSINESS_CLIMATE_SOURCE_KEY == ("fr.insee.business-climate")
    assert INSEE_BUSINESS_CLIMATE_RELEASE_START_PERIOD == "2009-07"
    assert INSEE_BUSINESS_CLIMATE_CURRENT_START_PERIOD == "2000-01"
    assert INSEE_BUSINESS_CLIMATE_LATEST_PACKAGED_PERIOD == "2026-08"
    assert INSEE_BUSINESS_CLIMATE_RELEASE_GAPS == (
        "2009-08",
        "2010-08",
        "2011-08",
    )
    assert manifest.search.as_of_date == "2026-09-19"
    assert manifest.search.total_result_count == 203
    assert len(manifest.search.exclusions) == 0
    assert len(manifest.releases) == 203
    assert manifest.raw_artifact_count == 207
    assert manifest.unique_content_sha256_count == 207
    assert manifest.total_content_bytes == 23_389_828
    assert manifest.released_value_count == 203
    assert manifest.compatible_comparison_count == 203
    assert packaged_insee_business_climate_manifest_path().is_file()


def test_insee_business_climate_registry_entry_is_verified() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(INSEE_BUSINESS_CLIMATE_SOURCE_KEY)

    assert source.parser_id == INSEE_BUSINESS_CLIMATE_PARSER_ID
    assert source.verification_status is (
        OfficialSourceVerificationStatus.EMPIRICALLY_VERIFIED
    )
    assert source.reviewed_on == "2026-09-19"
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
        INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID,
    )
    assert source.source_table_ids == ("103047029",)


def test_inventory_accounts_for_gaps_and_every_search_result() -> None:
    manifest = _manifest()
    gaps = set(INSEE_BUSINESS_CLIMATE_RELEASE_GAPS)
    expected = [
        f"{year}-{month:02d}"
        for year in range(2009, 2027)
        for month in range(1, 13)
        if "2009-07" <= f"{year}-{month:02d}" <= "2026-08"
        and f"{year}-{month:02d}" not in gaps
    ]

    assert Counter(item.reason for item in manifest.search.exclusions) == {}
    assert (
        len(manifest.search.selected_document_ids)
        + len(manifest.search.exclusions)
        == manifest.search.total_result_count
    )
    assert [item.reference_period for item in manifest.releases] == expected
    assert tuple(item.source_document_id for item in manifest.releases) == (
        manifest.search.selected_document_ids
    )
    assert manifest.coverage_gaps == INSEE_BUSINESS_CLIMATE_RELEASE_GAPS
    assert manifest.releases[0].source_document_id == 1_561_614
    assert manifest.releases[-1].source_document_id == 9_038_023


def test_values_cover_source_layouts_and_controlled_text_fallbacks() -> None:
    manifest = _manifest()
    legacy = _release(manifest, "2009-07")
    early_text = _release(manifest, "2011-01")
    modern_table = _release(manifest, "2017-07")
    pandemic_text = _release(manifest, "2020-05")
    current = _release(manifest, "2026-08")

    assert (legacy.value.value, legacy.value.methodology) == (
        75.0,
        "source-normalized-composite",
    )
    assert legacy.value.locator.startswith("table:")
    assert (early_text.value.value, early_text.value.locator) == (
        106.0,
        "paragraph:meta-description:business-climate-summary",
    )
    assert (modern_table.value.value, modern_table.value.methodology) == (
        108.0,
        "source-normalized-composite",
    )
    assert (pandemic_text.value.value, pandemic_text.value.locator) == (
        59.0,
        "paragraph:page-introduction:business-climate-summary",
    )
    assert (current.value.value, current.value.methodology) == (
        98.1,
        "source-normalized-composite",
    )
    values = tuple(item.value for item in manifest.releases)
    assert {item.metric for item in values} == {
        "business-climate-summary-indicator"
    }
    assert {item.unit for item in values} == {
        "normalized-index-long-term-average-100"
    }
    assert {item.adjustment for item in values} == {
        "source-defined-composite-adjustment"
    }
    assert {item.geography_scope for item in values} == {"metropolitan-france"}


def test_current_series_and_comparisons_keep_latest_state_explicit() -> None:
    manifest = _manifest()
    series = manifest.current_series

    assert series.series_id == INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID
    assert series.title == (
        "Business climate summary indicator - All sectors - Metropolitan France"
    )
    assert series.semantics == "latest-revised-cross-check"
    assert series.methodology == "source-normalized-composite"
    assert len(series.observations) == 320
    assert (
        series.observations[0].reference_period,
        series.observations[0].value,
    ) == ("2000-01", 115.9)
    assert (
        series.observations[-1].reference_period,
        series.observations[-1].value,
    ) == ("2026-08", 98.1)
    assert series.last_update == "2026-08-21"
    assert manifest.comparisons[0].to_dict() == {
        "schema_version": ("histdatacom.insee-business-climate-comparison.v1"),
        "reference_period": "2009-07",
        "methodology": "source-normalized-composite",
        "release_value": 75.0,
        "current_value": 79.7,
        "delta": 4.7,
    }
    assert manifest.comparisons[-1].reference_period == "2026-08"
    assert manifest.comparisons[-1].delta == 0.0


def test_requests_bind_exact_methods_formats_and_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_insee_business_climate_search_requests(registry)
    catalog = build_insee_business_climate_catalog_request(registry)
    series = build_insee_business_climate_current_series_request(registry)
    releases = build_insee_business_climate_release_requests(
        registry,
        ({"id": item.source_document_id} for item in manifest.releases),
    )

    assert {search.method for search in searches} == {
        OfficialRequestMethod.POST
    }
    assert {search.source_format for search in searches} == {
        OfficialSourceFormat.JSON
    }
    assert tuple(
        json.loads(search.body_text or "")["q"] for search in searches
    ) == (
        '"Business climate indicator and turning point indicator"',
        '"Business indicator"',
    )
    assert catalog.method is OfficialRequestMethod.POST
    assert catalog.source_format is OfficialSourceFormat.JSON
    assert json.loads(catalog.body_text or "") == {
        "facetsField": [],
        "filters": [{"field": "bdm_idFamille", "values": ["103047029"]}],
        "q": "French business climate composite indicator",
        "rows": 1000,
        "sortFields": [],
        "start": 0,
    }
    assert series.source_format is OfficialSourceFormat.SDMX_21
    assert len(releases) == 203
    assert len({item.uri for item in releases}) == 203
    assert {item.source_format for item in releases} == {
        OfficialSourceFormat.HTML
    }
    assert {
        item.parser_id for item in (*searches, catalog, series, *releases)
    } == {INSEE_BUSINESS_CLIMATE_PARSER_ID}


def test_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        InseeBusinessClimateArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["value"]["value"] = 99.0
    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        InseeBusinessClimateArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["search"]["artifacts"].pop()
    with pytest.raises(ValueError, match="artifact inventory differs"):
        InseeBusinessClimateArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["manifest_id"] = (
        "insee-business-climate-archive-manifest:sha256:" + "0" * 64
    )
    with pytest.raises(ValueError, match="manifest identity differs"):
        InseeBusinessClimateArchiveManifestV1.from_json(json.dumps(payload))
