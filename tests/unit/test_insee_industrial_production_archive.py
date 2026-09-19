"""Qualification tests for the official INSEE industrial-production archive."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_IDS,
    INSEE_INDUSTRIAL_PRODUCTION_CURRENT_START_PERIOD,
    INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID,
    INSEE_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_PERIOD,
    INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID,
    INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID,
    INSEE_INDUSTRIAL_PRODUCTION_PROGRAM_KEY,
    INSEE_INDUSTRIAL_PRODUCTION_RELEASE_GAPS,
    INSEE_INDUSTRIAL_PRODUCTION_RELEASE_START_PERIOD,
    INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY,
    InseeIndustrialProductionAggregate,
    InseeIndustrialProductionArchiveManifestV1,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    OfficialSourceRole,
    OfficialSourceVerificationStatus,
    build_insee_industrial_production_archive_manifest,
    build_insee_industrial_production_catalog_requests,
    build_insee_industrial_production_current_series_requests,
    build_insee_industrial_production_release_requests,
    build_insee_industrial_production_search_requests,
    load_packaged_insee_industrial_production_archive_manifest,
    load_packaged_official_source_registry,
    packaged_insee_industrial_production_manifest_path,
)


def _manifest() -> InseeIndustrialProductionArchiveManifestV1:
    return load_packaged_insee_industrial_production_archive_manifest()


def _release(manifest: InseeIndustrialProductionArchiveManifestV1, period: str):
    return next(
        item for item in manifest.releases if item.reference_period == period
    )


def test_packaged_archive_quantifies_complete_lineage() -> None:
    manifest = _manifest()

    assert INSEE_INDUSTRIAL_PRODUCTION_PROGRAM_KEY == (
        "fr.insee.industrial-production"
    )
    assert INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY == (
        "fr.insee.industrial-production"
    )
    assert INSEE_INDUSTRIAL_PRODUCTION_RELEASE_START_PERIOD == "2009-04"
    assert INSEE_INDUSTRIAL_PRODUCTION_CURRENT_START_PERIOD == "2000-01"
    assert INSEE_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_PERIOD == "2026-07"
    assert INSEE_INDUSTRIAL_PRODUCTION_RELEASE_GAPS == ()
    assert manifest.search.as_of_date == "2026-09-19"
    assert manifest.search.total_result_count == 218
    assert len(manifest.search.exclusions) == 10
    assert len(manifest.releases) == 208
    assert manifest.raw_artifact_count == 213
    assert manifest.unique_content_sha256_count == 213
    assert manifest.total_content_bytes == 23_135_372
    assert manifest.released_value_count == 416
    assert manifest.compatible_comparison_count == 416
    assert packaged_insee_industrial_production_manifest_path().is_file()


def test_registry_entry_is_verified() -> None:
    source = load_packaged_official_source_registry().source(
        INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY
    )

    assert source.parser_id == INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID
    assert source.verification_status is (
        OfficialSourceVerificationStatus.EMPIRICALLY_VERIFIED
    )
    assert source.reviewed_on == "2026-09-19"
    assert source.roles == (
        OfficialSourceRole.OFFICIAL_ARCHIVE,
        OfficialSourceRole.RELEASE_CALENDAR,
    )
    assert source.formats == (
        OfficialSourceFormat.JSON,
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.SDMX_21,
    )
    assert source.source_series_ids == (
        INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_IDS
    )
    assert source.source_table_ids == ("117606968",)


def test_inventory_accounts_for_every_search_result() -> None:
    manifest = _manifest()
    expected = [
        f"{year}-{month:02d}"
        for year in range(2009, 2027)
        for month in range(1, 13)
        if "2009-04" <= f"{year}-{month:02d}" <= "2026-07"
    ]

    assert Counter(item.reason for item in manifest.search.exclusions) == {
        "missing-subtitle": 5,
        "other-industrial-production-product": 4,
        "search-result-outside-release-lineage": 1,
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
    assert manifest.coverage_gaps == ()
    assert manifest.releases[0].source_document_id == 1_561_508
    assert manifest.releases[-1].source_document_id == 9_050_846


def test_values_cover_layouts_and_explicit_source_corrections() -> None:
    manifest = _manifest()
    legacy = _release(manifest, "2009-04")
    corrected_period = _release(manifest, "2010-12")
    malformed_header = _release(manifest, "2018-04")
    ambiguous_year = _release(manifest, "2021-03")
    current = _release(manifest, "2026-07")

    assert [(item.aggregate, item.value) for item in legacy.values] == [
        (InseeIndustrialProductionAggregate.INDUSTRY, -1.4),
        (InseeIndustrialProductionAggregate.MANUFACTURING, -0.5),
    ]
    assert corrected_period.source_document_id == 1_562_488
    assert [item.value for item in corrected_period.values] == [0.3, -0.1]
    assert [item.value for item in malformed_header.values] == [-0.5, 0.4]
    assert [item.value for item in ambiguous_year.values] == [0.8, 0.4]
    assert [item.value for item in current.values] == [-0.4, -0.8]
    values = tuple(
        value for release in manifest.releases for value in release.values
    )
    assert {item.metric for item in values} == {
        "industrial-production-month-over-month-change"
    }
    assert {item.unit for item in values} == {"percent"}
    assert {item.adjustment for item in values} == {
        "seasonally-and-working-day-adjusted"
    }
    assert {item.geography_scope for item in values} == {"metropolitan-france"}


def test_current_series_and_comparisons_keep_latest_state_explicit() -> None:
    manifest = _manifest()
    industry, manufacturing = manifest.current_series

    assert industry.series_id == INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID
    assert manufacturing.series_id == (
        INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID
    )
    assert industry.aggregate is InseeIndustrialProductionAggregate.INDUSTRY
    assert manufacturing.aggregate is (
        InseeIndustrialProductionAggregate.MANUFACTURING
    )
    assert industry.semantics == "latest-revised-cross-check"
    assert industry.methodology == "current-revised-sa-wda-index-level"
    assert len(industry.observations) == 319
    assert (
        industry.observations[0].reference_period,
        industry.observations[0].value,
    ) == ("2000-01", 109.84)
    assert (
        manufacturing.observations[-1].reference_period,
        manufacturing.observations[-1].value,
    ) == ("2026-07", 102.59)
    assert industry.last_update == "2026-09-09"
    assert manifest.comparisons[0].to_dict() == {
        "schema_version": (
            "histdatacom.insee-industrial-production-comparison.v1"
        ),
        "aggregate": "industry-be",
        "reference_period": "2009-04",
        "methodology": "derived-current-growth-versus-release-vintage",
        "release_value": -1.4,
        "current_value": -0.020951183742,
        "delta": 1.379048816258,
    }
    assert manifest.comparisons[-1].reference_period == "2026-07"
    assert manifest.comparisons[-1].delta == 0.00703993811


def test_requests_bind_exact_methods_formats_and_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    searches = build_insee_industrial_production_search_requests(registry)
    catalogs = build_insee_industrial_production_catalog_requests(registry)
    series = build_insee_industrial_production_current_series_requests(registry)
    releases = build_insee_industrial_production_release_requests(
        registry,
        ({"id": item.source_document_id} for item in manifest.releases),
    )

    assert {item.method for item in searches} == {OfficialRequestMethod.POST}
    assert tuple(
        json.loads(item.body_text or "")["q"] for item in searches
    ) == ('"Industrial production index"',)
    assert len(catalogs) == 2
    assert {item.method for item in catalogs} == {OfficialRequestMethod.POST}
    assert [json.loads(item.body_text or "")["start"] for item in catalogs] == [
        0,
        1000,
    ]
    assert len(series) == 2
    assert {item.source_format for item in series} == {
        OfficialSourceFormat.SDMX_21
    }
    assert len(releases) == len({item.uri for item in releases}) == 208
    assert {item.source_format for item in releases} == {
        OfficialSourceFormat.HTML
    }
    assert {
        item.parser_id for item in (*searches, *catalogs, *series, *releases)
    } == {INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID}


def test_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        InseeIndustrialProductionArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["values"][0]["value"] = 99.0
    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        InseeIndustrialProductionArchiveManifestV1.from_json(
            json.dumps(payload)
        )


def test_manifest_rejects_missing_artifacts_and_incorrect_identity() -> None:
    manifest = _manifest()
    payload = json.loads(manifest.to_json())
    payload["search"]["artifacts"].pop()
    with pytest.raises(ValueError, match="artifact inventory differs"):
        InseeIndustrialProductionArchiveManifestV1.from_json(
            json.dumps(payload)
        )

    payload = json.loads(manifest.to_json())
    payload["manifest_id"] = (
        "insee-industrial-production-archive-manifest:sha256:" + "0" * 64
    )
    with pytest.raises(ValueError, match="manifest identity differs"):
        InseeIndustrialProductionArchiveManifestV1.from_json(
            json.dumps(payload)
        )


@pytest.mark.parametrize("field", ("release_value", "current_value"))
def test_manifest_binds_comparison_inputs_to_retained_evidence(
    field: str,
) -> None:
    manifest = _manifest()
    comparison = manifest.comparisons[0]
    changed_value = getattr(comparison, field) + 1.0
    changed = replace(
        comparison,
        **{field: changed_value},
        delta=round(
            (
                changed_value
                if field == "current_value"
                else comparison.current_value
            )
            - (
                changed_value
                if field == "release_value"
                else comparison.release_value
            ),
            12,
        ),
    )

    with pytest.raises(ValueError, match="comparison inputs differ"):
        replace(manifest, comparisons=(changed, *manifest.comparisons[1:]))


@pytest.mark.parametrize("value", (float("nan"), float("inf"), -float("inf")))
def test_non_finite_values_cannot_enter_evidence_or_comparisons(
    value: float,
) -> None:
    manifest = _manifest()

    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        replace(manifest.releases[0].values[0], value=value)
    with pytest.raises(ValueError, match="observation lexical differs"):
        replace(manifest.current_series[0].observations[0], value=value)
    for field in ("release_value", "current_value", "delta"):
        with pytest.raises(ValueError, match="finite"):
            replace(manifest.comparisons[0], **{field: value})


def test_manifest_rejects_evidence_newer_than_its_as_of_date() -> None:
    manifest = _manifest()

    with pytest.raises(ValueError, match="evidence exceeds as-of date"):
        replace(
            manifest, search=replace(manifest.search, as_of_date="2026-09-08")
        )


def test_release_local_timestamp_requires_the_source_timezone_offset() -> None:
    release = _manifest().releases[0]

    with pytest.raises(ValueError, match="local publication timestamp differs"):
        replace(
            release,
            published_at_local=release.published_at.replace("Z", "+00:00"),
        )


def test_current_series_rejects_evidence_from_the_other_series_endpoint() -> (
    None
):
    industry, manufacturing = _manifest().current_series

    with pytest.raises(ValueError, match="current series artifact URI differs"):
        replace(industry, artifact=manufacturing.artifact)
    with pytest.raises(ValueError, match="current series artifact URI differs"):
        replace(manufacturing, artifact=industry.artifact)


@pytest.mark.parametrize("drift", ("request", "resolved-uri", "search-request"))
def test_builder_rejects_snapshot_request_drift_before_parsing(
    drift: str,
) -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    documents = tuple(
        {"id": item.source_document_id} for item in manifest.releases
    )

    def snapshot(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
        return OfficialRawSnapshotV1(
            request=request,
            retrieved_at_ns=1,
            completed_at_ns=1,
            status_code=200,
            resolved_uri=request.uri,
            response_headers={},
            content=b"Not parsed because request validation must fail first",
            content_type="application/octet-stream",
        )

    requests = build_insee_industrial_production_release_requests(
        registry, documents
    )
    releases = {
        document["id"]: snapshot(request)
        for document, request in zip(documents, requests, strict=True)
    }
    catalogs = tuple(
        snapshot(request)
        for request in build_insee_industrial_production_catalog_requests(
            registry
        )
    )
    series = tuple(
        snapshot(request)
        for request in build_insee_industrial_production_current_series_requests(
            registry
        )
    )
    search = manifest.search
    first_id = documents[0]["id"]
    if drift == "request":
        releases[first_id] = replace(releases[first_id], request=requests[1])
    elif drift == "resolved-uri":
        releases[first_id] = replace(
            releases[first_id], resolved_uri=requests[1].uri
        )
    else:
        search = replace(
            search,
            artifacts=(
                replace(search.artifacts[0], request_id=requests[0].request_id),
            ),
        )

    with pytest.raises(ValueError, match="request (identity )?differs"):
        build_insee_industrial_production_archive_manifest(
            registry, search, documents, releases, catalogs, series
        )
