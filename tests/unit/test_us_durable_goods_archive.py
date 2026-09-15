"""Real-archive qualification tests for Census advance durable goods."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    DURABLE_CORE_CAPITAL_GOODS,
    DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS,
    DURABLE_HEADLINE_ORDERS,
    DURABLE_INDEX_URI,
    DURABLE_JULY_2005_ARTIFACT_URI,
    DURABLE_JULY_2005_LISTED_URI,
    DURABLE_OFFICIALLY_UNAVAILABLE_PERIOD,
    DURABLE_PREDECESSOR_URI,
    DURABLE_PROGRAM_KEY,
    DURABLE_SHUTDOWN_DELAYED_PERIODS,
    DURABLE_SOURCE_KEY,
    CensusDurableArchiveManifestV1,
    EconomicTimePrecision,
    OfficialSourceFormat,
    build_census_durable_archive_requests,
    build_census_durable_index_request,
    built_in_united_states_backfill_profile,
    census_durable_coverage_from_manifest,
    load_packaged_census_durable_archive_manifest,
    load_packaged_census_durable_index,
    load_packaged_census_durable_index_snapshot,
    load_packaged_census_durable_ocr_corpus,
    load_packaged_official_source_registry,
    parse_census_durable_release_index,
)


def test_packaged_durable_archive_quantifies_every_occurrence() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(DURABLE_SOURCE_KEY)
    manifest = load_packaged_census_durable_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.parser_id == "official.census-m3.v1"
    assert (
        source.availability_time_precision is EconomicTimePrecision.EXACT_MINUTE
    )
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert isinstance(manifest, CensusDurableArchiveManifestV1)
    assert len(manifest.publications) == 318
    assert manifest.raw_artifact_count == 320
    assert manifest.total_content_bytes == 95_545_357
    assert manifest.measure_occurrence_count == 621
    assert manifest.comparable_revision_count == 580
    assert manifest.officially_unavailable_count == 1
    assert manifest.core_source_unavailable_count == 13
    assert manifest.corrected_link_count == 1
    assert manifest.ocr_publication_count == 36
    assert manifest.historical_revision_notice_count == 97
    assert manifest.shutdown_delayed_count == 9
    assert manifest.source_zone_error_count == 9
    assert manifest.nonstandard_release_time_count == 3
    assert manifest.reported_rate_cross_check_count == 1_242
    assert manifest.exact_minute_count == 318
    assert (
        CensusDurableArchiveManifestV1.from_json(manifest.to_json()) == manifest
    )


def test_packaged_durable_index_replays_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_census_durable_archive_manifest()
    snapshot = load_packaged_census_durable_index_snapshot()
    index = parse_census_durable_release_index(
        snapshot, as_of_date=manifest.release_index.as_of_date
    )

    assert (
        build_census_durable_index_request(
            registry, as_of_date="2026-09-15"
        ).uri
        == DURABLE_INDEX_URI
    )
    assert index == manifest.release_index
    assert index == load_packaged_census_durable_index()
    assert len(index.releases) == 318
    assert index.releases[0].reference_period == "2000-01"
    assert index.releases[-1].reference_period == "2026-06"
    assert len(build_census_durable_archive_requests(registry, index)) == 319
    july = index.by_period["2005-07"]
    assert july.listed_uri == DURABLE_JULY_2005_LISTED_URI
    assert july.artifact_uri == DURABLE_JULY_2005_ARTIFACT_URI
    assert july.link_corrected


def test_predecessor_core_gap_and_unavailable_notice_are_explicit() -> None:
    manifest = load_packaged_census_durable_archive_manifest()
    first = manifest.publications[0]
    may_2002 = manifest.by_period["2002-05"]
    unavailable = manifest.by_period[DURABLE_OFFICIALLY_UNAVAILABLE_PERIOD]
    december = manifest.by_period["2009-12"]

    assert first.reference_period == "2000-01"
    assert set(first.by_measure) == {
        DURABLE_HEADLINE_ORDERS,
        DURABLE_CORE_CAPITAL_GOODS,
    }
    assert all(
        item.previous_artifact_uri == DURABLE_PREDECESSOR_URI
        for item in first.measures
    )
    assert [
        item.reference_period
        for item in manifest.publications
        if not item.officially_unavailable
        and DURABLE_CORE_CAPITAL_GOODS not in item.by_measure
    ] == list(DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS)
    returned_core = may_2002.by_measure[DURABLE_CORE_CAPITAL_GOODS]
    assert returned_core.previous_as_known_value is None
    assert not returned_core.revision_comparable
    assert returned_core.comparison_basis == "prior-core-measure-unavailable"
    assert unavailable.officially_unavailable
    assert unavailable.measures == ()
    assert unavailable.released_lexical == "2009-12-24T08:30:00"
    assert all(
        item.comparison_basis == "intervening-historical-correction"
        and not item.revision_comparable
        and item.previous_as_known_value == item.revised_previous_value
        for item in december.measures
    )


def test_ocr_shutdown_times_and_source_zone_errors_are_closed() -> None:
    manifest = load_packaged_census_durable_archive_manifest()
    ocr = load_packaged_census_durable_ocr_corpus()

    assert len(ocr.entries) == 37
    assert {item.parser_era for item in manifest.publications} == {
        "ocr-pdf-table",
        "native-pdf-table",
        "official-unavailable-notice",
    }
    assert [
        item.reference_period
        for item in manifest.publications
        if item.shutdown_delayed
    ] == list(DURABLE_SHUTDOWN_DELAYED_PERIODS)
    assert [
        (item.reference_period, item.released_lexical)
        for item in manifest.publications
        if "T10:00:00" in item.released_lexical
    ] == [
        ("2000-09", "2000-10-27T10:00:00"),
        ("2001-04", "2001-05-25T10:00:00"),
        ("2002-10", "2002-11-27T10:00:00"),
    ]
    assert [
        item.reference_period
        for item in manifest.publications
        if not item.zone_consistent
    ] == [
        "2000-01",
        "2000-02",
        "2000-10",
        "2001-03",
        "2001-10",
        "2001-11",
        "2001-12",
        "2002-01",
        "2002-02",
    ]


def test_durable_coverage_is_complete_for_the_builtin_profile() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_census_durable_archive_manifest()
    coverage = census_durable_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert coverage.expected_occurrence_count == 318
    assert coverage.schedule_count == 318
    assert coverage.initial_actual_count == 317
    assert coverage.previous_as_known_count == 317
    assert coverage.revision_count == 580
    assert coverage.exact_minute_count == 318
    assert coverage.officially_unavailable_count == 1
    assert len(coverage.artifact_sha256s) == 320
    assert coverage.is_complete_for(profile.by_key[DURABLE_PROGRAM_KEY])


def test_durable_manifest_rejects_identity_tampering() -> None:
    manifest = load_packaged_census_durable_archive_manifest()

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, profile_id=manifest.profile_id + "-tampered")
