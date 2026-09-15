"""Real-archive qualification tests for Census advance retail sales."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    CENSUS_RETAIL_BENCHMARK_INDEX_URI,
    CENSUS_RETAIL_BENCHMARK_URIS,
    CENSUS_RETAIL_CONTROL_GROUP,
    CENSUS_RETAIL_CURRENT_PDF_URI,
    CENSUS_RETAIL_EX_GAS,
    CENSUS_RETAIL_MEASURE_KEYS,
    CENSUS_RETAIL_NAICS_BOUNDARY,
    CENSUS_RETAIL_PREDECESSOR_URI,
    CENSUS_RETAIL_PROGRAM_KEY,
    CENSUS_RETAIL_SOURCE_KEY,
    CensusRetailArchiveManifestV1,
    CensusRetailBenchmarkDisposition,
    CensusRetailLevelBasis,
    EconomicTimePrecision,
    OfficialSourceFormat,
    build_census_retail_archive_requests,
    build_census_retail_benchmark_index_request,
    build_census_retail_benchmark_requests,
    build_census_retail_index_request,
    built_in_united_states_backfill_profile,
    census_retail_coverage_from_manifest,
    load_packaged_census_retail_archive_manifest,
    load_packaged_census_retail_benchmark_index,
    load_packaged_census_retail_benchmark_index_snapshot,
    load_packaged_census_retail_index,
    load_packaged_census_retail_index_snapshot,
    load_packaged_census_retail_ocr_corpus,
    load_packaged_official_source_registry,
    parse_census_retail_release_index,
)


def test_packaged_retail_archive_quantifies_every_measure_occurrence() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(CENSUS_RETAIL_SOURCE_KEY)
    manifest = load_packaged_census_retail_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert (
        source.availability_time_precision is EconomicTimePrecision.EXACT_MINUTE
    )
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.XLS,
        OfficialSourceFormat.XLSX,
    }
    assert isinstance(manifest, CensusRetailArchiveManifestV1)
    assert len(manifest.publications) == 320
    assert manifest.raw_artifact_count == 512
    assert manifest.total_content_bytes == 174_591_370
    assert manifest.measure_occurrence_count == 1_280
    assert manifest.comparable_revision_count == 1_067
    assert manifest.derived_aggregate_occurrence_count == 541
    assert manifest.support_artifact_count == 163
    assert manifest.corrected_support_link_count == 6
    assert manifest.ocr_publication_count == 37
    assert manifest.benchmark_revision_notice_count == 39
    assert manifest.benchmark_state_override_count == 24
    assert manifest.shutdown_delayed_count == 9
    assert manifest.reported_rate_cross_check_count == 850
    assert manifest.exact_minute_count == 320
    assert (
        CensusRetailArchiveManifestV1.from_json(manifest.to_json()) == manifest
    )


def test_packaged_monthly_and_annual_indexes_replay_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_census_retail_archive_manifest()
    monthly_snapshot = load_packaged_census_retail_index_snapshot()
    annual_snapshot = load_packaged_census_retail_benchmark_index_snapshot()
    monthly = parse_census_retail_release_index(
        monthly_snapshot, as_of_date=manifest.release_index.as_of_date
    )
    annual = load_packaged_census_retail_benchmark_index()

    assert (
        build_census_retail_index_request(registry, as_of_date="2026-09-14").uri
        == monthly_snapshot.request.uri
    )
    assert (
        build_census_retail_benchmark_index_request(
            registry, as_of_date="2026-09-14"
        ).uri
        == CENSUS_RETAIL_BENCHMARK_INDEX_URI
    )
    assert monthly == manifest.release_index
    assert monthly == load_packaged_census_retail_index()
    assert len(monthly.releases) == 320
    assert monthly.releases[-1].artifact_uri == CENSUS_RETAIL_CURRENT_PDF_URI
    assert len(build_census_retail_archive_requests(registry, monthly)) == 484
    assert (
        len(build_census_retail_benchmark_requests(registry, annual_snapshot))
        == 26
    )
    assert len(annual.entries) == 26
    assert tuple(item.report_year for item in annual.entries) == tuple(
        range(2000, 2026)
    )
    assert tuple(item.artifact_uri for item in annual.entries) == tuple(
        CENSUS_RETAIL_BENCHMARK_URIS.values()
    )


def test_annual_benchmarks_replace_only_overlapping_prior_states() -> None:
    manifest = load_packaged_census_retail_archive_manifest()
    annual = manifest.benchmark_index
    april_2018 = annual.by_application_period["2018-05"]
    may_release = manifest.by_period["2018-05"]
    total = may_release.by_measure[CENSUS_RETAIL_MEASURE_KEYS[0]]

    assert [item.disposition for item in annual.entries[:2]] == [
        CensusRetailBenchmarkDisposition.RETAINED_NON_OVERLAPPING,
        CensusRetailBenchmarkDisposition.RETAINED_NON_OVERLAPPING,
    ]
    assert all(not item.measures for item in annual.entries[:2])
    assert all(
        item.disposition
        is CensusRetailBenchmarkDisposition.PRIOR_STATE_OVERRIDE
        for item in annual.entries[2:]
    )
    assert april_2018.report_period_end == "2018-04"
    assert total.previous_as_known_lexical == "0.2"
    assert total.previous_artifact_uri == april_2018.artifact_uri
    assert (
        total.previous_as_known_value
        == april_2018.by_measure[total.measure_key].value
    )
    assert total.revised_previous_lexical == "0.4"


def test_classification_derivation_and_predecessor_boundaries_are_explicit() -> (
    None
):
    manifest = load_packaged_census_retail_archive_manifest()
    first = manifest.publications[0]
    classification_boundary = manifest.by_period[CENSUS_RETAIL_NAICS_BOUNDARY]
    april_2018 = manifest.by_period["2018-04"]
    may_2018 = manifest.by_period["2018-05"]

    assert first.reference_period == "1999-12"
    assert set(first.by_measure) == set(CENSUS_RETAIL_MEASURE_KEYS)
    assert all(
        item.previous_artifact_uri == CENSUS_RETAIL_PREDECESSOR_URI
        for item in first.measures
    )
    assert all(
        not item.revision_comparable
        for item in classification_boundary.measures
    )
    assert {
        item.comparison_basis for item in classification_boundary.measures
    } == {"sic-to-naics-boundary"}
    assert (
        april_2018.by_measure[CENSUS_RETAIL_EX_GAS].level_basis
        is CensusRetailLevelBasis.DERIVED_AGGREGATE
    )
    assert (
        may_2018.by_measure[CENSUS_RETAIL_EX_GAS].level_basis
        is CensusRetailLevelBasis.SOURCE_PUBLISHED
    )
    assert all(
        item.by_measure[CENSUS_RETAIL_CONTROL_GROUP].level_basis
        is CensusRetailLevelBasis.DERIVED_AGGREGATE
        for item in manifest.publications
    )


def test_ocr_spreadsheets_shutdowns_and_zones_are_closed() -> None:
    manifest = load_packaged_census_retail_archive_manifest()
    ocr = load_packaged_census_retail_ocr_corpus()

    assert len(ocr.entries) == 38
    assert {item.parser_era for item in manifest.publications} == {
        "ocr-pdf-table",
        "native-pdf-table",
        "xls-table",
        "xlsx-table",
    }
    assert [
        item.reference_period
        for item in manifest.publications
        if item.shutdown_delayed
    ] == [
        "2018-12",
        "2019-01",
        "2019-02",
        "2025-09",
        "2025-10",
        "2025-11",
        "2025-12",
        "2026-01",
        "2026-02",
    ]
    assert all(item.zone_consistent for item in manifest.publications)
    assert {item.reported_zone for item in manifest.publications} == {
        "ET",
        "EST",
        "EDT",
    }


def test_retail_coverage_is_complete_for_the_builtin_profile() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_census_retail_archive_manifest()
    coverage = census_retail_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert coverage.expected_occurrence_count == 320
    assert coverage.schedule_count == 320
    assert coverage.initial_actual_count == 320
    assert coverage.previous_as_known_count == 320
    assert coverage.revision_count == 1_067
    assert coverage.exact_minute_count == 320
    assert len(coverage.artifact_sha256s) == 512
    assert coverage.is_complete_for(profile.by_key[CENSUS_RETAIL_PROGRAM_KEY])


def test_manifest_rejects_identity_tampering() -> None:
    manifest = load_packaged_census_retail_archive_manifest()

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, profile_id=manifest.profile_id + "-tampered")
