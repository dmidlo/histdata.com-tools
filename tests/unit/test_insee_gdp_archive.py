"""Qualification tests for the official INSEE quarterly GDP archive."""

from __future__ import annotations

import json
from collections import Counter
from typing import cast

import pytest

from histdatacom.market_context import (
    INSEE_GDP_CURRENT_SERIES_ID,
    INSEE_GDP_CURRENT_START_PERIOD,
    INSEE_GDP_LATEST_PACKAGED_PERIOD,
    INSEE_GDP_PARSER_ID,
    INSEE_GDP_PROGRAM_KEY,
    INSEE_GDP_RELEASE_START_PERIOD,
    INSEE_GDP_SOURCE_KEY,
    InseeGdpArchiveManifestV1,
    InseeGdpReleaseStage,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRole,
    OfficialSourceVerificationStatus,
    build_insee_gdp_current_series_request,
    build_insee_gdp_release_requests,
    build_insee_gdp_search_request,
    load_packaged_insee_gdp_archive_manifest,
    load_packaged_official_source_registry,
    packaged_insee_gdp_manifest_path,
)


def _manifest() -> InseeGdpArchiveManifestV1:
    return load_packaged_insee_gdp_archive_manifest()


def _release_value(
    manifest: InseeGdpArchiveManifestV1,
    period: str,
    stage: InseeGdpReleaseStage,
) -> float:
    return cast(
        float,
        next(
            item.value.value
            for item in manifest.releases
            if item.reference_period == period and item.stage is stage
        ),
    )


def test_packaged_insee_gdp_archive_quantifies_paired_lineage() -> None:
    manifest = _manifest()

    assert INSEE_GDP_PROGRAM_KEY == "fr.insee.quarterly-national-accounts"
    assert INSEE_GDP_SOURCE_KEY == "fr.insee.quarterly-gdp"
    assert INSEE_GDP_RELEASE_START_PERIOD == "2016-Q3"
    assert INSEE_GDP_CURRENT_START_PERIOD == "2000-Q1"
    assert INSEE_GDP_LATEST_PACKAGED_PERIOD == "2026-Q2"
    assert manifest.search.as_of_date == "2026-09-18"
    assert manifest.search.total_result_count == 336
    assert len(manifest.search.exclusions) == 256
    assert len(manifest.releases) == 80
    assert manifest.raw_artifact_count == 82
    assert manifest.unique_content_sha256_count == 82
    assert manifest.total_content_bytes == 10_434_107
    assert manifest.released_value_count == 80
    assert len(manifest.revisions) == 40
    assert manifest.nonzero_revision_count == 17
    assert manifest.manifest_id == (
        "insee-gdp-archive-manifest:sha256:"
        "c706b35041df30795bbdbdb6549096bb4f924e4dd0ce0ca73b92e771b5844be2"
    )
    assert packaged_insee_gdp_manifest_path().is_file()


def test_insee_gdp_registry_entry_is_empirically_verified_and_non_primary() -> (
    None
):
    registry = load_packaged_official_source_registry()
    source = registry.source(INSEE_GDP_SOURCE_KEY)

    assert registry.registry_id == (
        "official-source-registry:sha256:"
        "544cae6dba51e3f428b719b38787975c595cd18f4bc21c13377c456766319829"
    )
    assert source.source_id == (
        "official-source:sha256:"
        "f6b026ebbd54044a3dab249426037a732335e1f331130c2038f473a68c0a7d77"
    )
    assert source.parser_id == INSEE_GDP_PARSER_ID
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
    assert source.source_series_ids == (INSEE_GDP_CURRENT_SERIES_ID,)


def test_insee_gdp_inventory_is_gap_free_and_accounts_for_every_result() -> (
    None
):
    manifest = _manifest()

    assert Counter(item.stage for item in manifest.releases) == {
        InseeGdpReleaseStage.FIRST_ESTIMATE: 40,
        InseeGdpReleaseStage.DETAILED_FIGURES: 40,
    }
    assert Counter(item.reason for item in manifest.search.exclusions) == {
        "search-result-outside-release-lineage": 171,
        "other-quarterly-national-accounts-publication": 70,
        "missing-subtitle": 12,
        "outside-packaged-coverage": 3,
    }
    assert (
        len(manifest.search.selected_document_ids)
        + len(manifest.search.exclusions)
        == manifest.search.total_result_count
    )
    assert tuple(item.source_document_id for item in manifest.releases) == (
        manifest.search.selected_document_ids
    )
    assert manifest.releases[0].reference_period == "2016-Q3"
    assert manifest.releases[0].stage is InseeGdpReleaseStage.FIRST_ESTIMATE
    assert manifest.releases[-1].reference_period == "2026-Q2"
    assert manifest.releases[-1].stage is InseeGdpReleaseStage.DETAILED_FIGURES


def test_insee_gdp_values_cover_legacy_and_current_table_layouts() -> None:
    manifest = _manifest()

    assert (
        _release_value(manifest, "2016-Q3", InseeGdpReleaseStage.FIRST_ESTIMATE)
        == 0.2
    )
    assert (
        _release_value(
            manifest, "2016-Q3", InseeGdpReleaseStage.DETAILED_FIGURES
        )
        == 0.2
    )
    assert (
        _release_value(manifest, "2026-Q2", InseeGdpReleaseStage.FIRST_ESTIMATE)
        == 0.2
    )
    assert (
        _release_value(
            manifest, "2026-Q2", InseeGdpReleaseStage.DETAILED_FIGURES
        )
        == 0.0
    )
    assert {item.value.metric for item in manifest.releases} == {"gdp-volume"}
    assert {item.value.change_basis for item in manifest.releases} == {
        "quarter-on-quarter"
    }
    assert all(
        item.value.locator.startswith("table:") for item in manifest.releases
    )


def test_insee_gdp_revisions_keep_stages_distinct() -> None:
    manifest = _manifest()
    revisions = {item.reference_period: item for item in manifest.revisions}

    assert revisions["2020-Q1"].first_estimate_value == -5.8
    assert revisions["2020-Q1"].detailed_figures_value == -5.3
    assert revisions["2020-Q1"].delta == 0.5
    assert revisions["2021-Q1"].first_estimate_value == 0.4
    assert revisions["2021-Q1"].detailed_figures_value == -0.1
    assert revisions["2021-Q1"].delta == -0.5
    assert max(abs(item.delta) for item in manifest.revisions) == 0.5


def test_insee_gdp_current_series_is_explicit_latest_revised_cross_check() -> (
    None
):
    series = _manifest().current_series

    assert series.series_id == INSEE_GDP_CURRENT_SERIES_ID
    assert series.semantics == "latest-revised-cross-check"
    assert len(series.observations) == 106
    assert series.observations[0].reference_period == "2000-Q1"
    assert series.observations[0].value == 0.9
    assert series.observations[-1].reference_period == "2026-Q2"
    assert series.observations[-1].value == 0.0
    assert series.last_update == "2026-08-28"


def test_insee_gdp_requests_bind_exact_methods_formats_and_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    search = build_insee_gdp_search_request(registry)
    series = build_insee_gdp_current_series_request(registry)
    releases = build_insee_gdp_release_requests(
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
        "q": "quarterly national accounts",
        "rows": 1000,
        "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
        "start": 0,
    }
    assert series.source_format is OfficialSourceFormat.SDMX_21
    assert len(releases) == 80
    assert len({item.uri for item in releases}) == 80
    assert {item.source_format for item in releases} == {
        OfficialSourceFormat.HTML
    }
    assert {item.parser_id for item in (search, series, *releases)} == {
        INSEE_GDP_PARSER_ID
    }


def test_insee_gdp_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert InseeGdpArchiveManifestV1.from_json(manifest.to_json()) == manifest

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["value"]["value"] = 99.0
    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        InseeGdpArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["manifest_id"] = "insee-gdp-archive-manifest:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="manifest identity differs"):
        InseeGdpArchiveManifestV1.from_json(json.dumps(payload))
