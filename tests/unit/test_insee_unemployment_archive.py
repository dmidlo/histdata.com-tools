"""Qualification tests for the official INSEE unemployment archive."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from histdatacom.market_context import (
    INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID,
    INSEE_UNEMPLOYMENT_CURRENT_START_PERIOD,
    INSEE_UNEMPLOYMENT_LATEST_PACKAGED_PERIOD,
    INSEE_UNEMPLOYMENT_PARSER_ID,
    INSEE_UNEMPLOYMENT_PROGRAM_KEY,
    INSEE_UNEMPLOYMENT_RELEASE_GAPS,
    INSEE_UNEMPLOYMENT_RELEASE_START_PERIOD,
    INSEE_UNEMPLOYMENT_SOURCE_KEY,
    InseeUnemploymentArchiveManifestV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRole,
    OfficialSourceVerificationStatus,
    build_insee_unemployment_catalog_request,
    build_insee_unemployment_current_series_request,
    build_insee_unemployment_release_requests,
    build_insee_unemployment_search_request,
    load_packaged_insee_unemployment_archive_manifest,
    load_packaged_official_source_registry,
    packaged_insee_unemployment_manifest_path,
)


def _manifest() -> InseeUnemploymentArchiveManifestV1:
    return load_packaged_insee_unemployment_archive_manifest()


def _release(manifest: InseeUnemploymentArchiveManifestV1, period: str):
    return next(
        item for item in manifest.releases if item.reference_period == period
    )


def test_packaged_insee_unemployment_archive_quantifies_lineage() -> None:
    manifest = _manifest()

    assert INSEE_UNEMPLOYMENT_PROGRAM_KEY == "fr.insee.ilo-unemployment"
    assert INSEE_UNEMPLOYMENT_SOURCE_KEY == "fr.insee.ilo-unemployment"
    assert INSEE_UNEMPLOYMENT_RELEASE_START_PERIOD == "2009-Q1"
    assert INSEE_UNEMPLOYMENT_CURRENT_START_PERIOD == "2003-Q1"
    assert INSEE_UNEMPLOYMENT_LATEST_PACKAGED_PERIOD == "2026-Q2"
    assert INSEE_UNEMPLOYMENT_RELEASE_GAPS == ("2013-Q1",)
    assert manifest.search.as_of_date == "2026-09-18"
    assert manifest.search.total_result_count == 246
    assert len(manifest.search.exclusions) == 177
    assert len(manifest.releases) == 69
    assert manifest.raw_artifact_count == 72
    assert manifest.unique_content_sha256_count == 72
    assert manifest.total_content_bytes == 10_889_565
    assert manifest.released_value_count == 69
    assert manifest.compatible_comparison_count == 1
    assert manifest.manifest_id == (
        "insee-unemployment-archive-manifest:sha256:"
        "4872670d7e80e9fab2cafc2b2642f54b44e8b2a5d15e9061735abf3717ab693e"
    )
    assert packaged_insee_unemployment_manifest_path().is_file()


def test_insee_unemployment_registry_entry_is_empirically_verified() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(INSEE_UNEMPLOYMENT_SOURCE_KEY)

    assert registry.registry_id == (
        "official-source-registry:sha256:"
        "544cae6dba51e3f428b719b38787975c595cd18f4bc21c13377c456766319829"
    )
    assert source.source_id == (
        "official-source:sha256:"
        "6cf422f52344b61791458ef6d164055d2d400eb5987cf324884cf6985ce2294e"
    )
    assert source.parser_id == INSEE_UNEMPLOYMENT_PARSER_ID
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
    assert source.source_series_ids == (INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID,)


def test_insee_unemployment_inventory_accounts_for_gap_and_every_result() -> (
    None
):
    manifest = _manifest()
    expected = [
        f"{year}-Q{quarter}"
        for year in range(2009, 2027)
        for quarter in range(1, 5)
        if "2009-Q1" <= f"{year}-Q{quarter}" <= "2026-Q2"
        and f"{year}-Q{quarter}" != "2013-Q1"
    ]

    assert Counter(item.reason for item in manifest.search.exclusions) == {
        "missing-subtitle": 133,
        "other-unemployment-publication": 26,
        "search-result-outside-release-lineage": 18,
    }
    assert (
        len(manifest.search.selected_document_ids)
        + len(manifest.search.exclusions)
        == manifest.search.total_result_count
    )
    assert [item.reference_period for item in manifest.releases] == expected
    assert tuple(item.source_document_id for item in manifest.releases) == (
        manifest.search.selected_document_ids
    )
    assert manifest.coverage_gaps == ("2013-Q1",)
    assert manifest.releases[0].source_document_id == 1_561_535
    assert manifest.releases[-1].source_document_id == 9_033_057


def test_insee_unemployment_values_cover_all_source_layouts_and_scopes() -> (
    None
):
    manifest = _manifest()

    oldest = _release(manifest, "2009-Q1")
    table_fallback = _release(manifest, "2017-Q1")
    chart = _release(manifest, "2026-Q1")
    whole_france = _release(manifest, "2026-Q2")

    assert (oldest.value.value, oldest.value.geography_scope) == (
        9.1,
        "metropolitan-france-plus-overseas-departments",
    )
    assert oldest.value.locator == "meta:description"
    assert (
        table_fallback.value.value,
        table_fallback.value.geography_scope,
    ) == (
        9.3,
        "metropolitan-france",
    )
    assert table_fallback.value.locator.startswith("table:")
    assert (chart.value.value, chart.value.geography_scope) == (
        8.1,
        "france-excluding-mayotte",
    )
    assert (whole_france.value.value, whole_france.value.geography_scope) == (
        8.3,
        "france",
    )
    assert {item.value.metric for item in manifest.releases} == {
        "ilo-unemployment-rate"
    }
    assert {item.value.unit for item in manifest.releases} == {
        "percent-of-labour-force"
    }
    assert {item.value.sex for item in manifest.releases} == {"all"}
    assert {item.value.age_group for item in manifest.releases} == {
        "15-and-over"
    }


def test_current_series_and_comparison_keep_latest_state_explicit() -> None:
    manifest = _manifest()
    series = manifest.current_series

    assert series.series_id == INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID
    assert series.title == "ILO unemployment rate - Total - France - SA data"
    assert series.semantics == "latest-revised-cross-check"
    assert series.geography_scope == "france"
    assert len(series.observations) == 94
    assert (
        series.observations[0].reference_period,
        series.observations[0].value,
    ) == (
        "2003-Q1",
        8.4,
    )
    assert (
        series.observations[-1].reference_period,
        series.observations[-1].value,
    ) == ("2026-Q2", 8.3)
    assert series.last_update == "2026-08-07"
    assert [item.to_dict() for item in manifest.comparisons] == [
        {
            "schema_version": "histdatacom.insee-unemployment-comparison.v1",
            "reference_period": "2026-Q2",
            "geography_scope": "france",
            "release_value": 8.3,
            "current_value": 8.3,
            "delta": 0.0,
        }
    ]


def test_insee_unemployment_requests_bind_exact_methods_and_formats() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    search = build_insee_unemployment_search_request(registry)
    catalog = build_insee_unemployment_catalog_request(registry)
    series = build_insee_unemployment_current_series_request(registry)
    releases = build_insee_unemployment_release_requests(
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
        "q": "unemployment rate",
        "rows": 1000,
        "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
        "start": 0,
    }
    assert catalog.method is OfficialRequestMethod.POST
    assert catalog.source_format is OfficialSourceFormat.JSON
    assert json.loads(catalog.body_text or "")["q"] == (
        '"ILO unemployment rate - Total - France - SA data"'
    )
    assert series.source_format is OfficialSourceFormat.SDMX_21
    assert len(releases) == 69
    assert len({item.uri for item in releases}) == 69
    assert {item.source_format for item in releases} == {
        OfficialSourceFormat.HTML
    }
    assert {
        item.parser_id for item in (search, catalog, series, *releases)
    } == {INSEE_UNEMPLOYMENT_PARSER_ID}


def test_insee_unemployment_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        InseeUnemploymentArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["value"]["value"] = 99.0
    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        InseeUnemploymentArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["manifest_id"] = (
        "insee-unemployment-archive-manifest:sha256:" + "0" * 64
    )
    with pytest.raises(ValueError, match="manifest identity differs"):
        InseeUnemploymentArchiveManifestV1.from_json(json.dumps(payload))
