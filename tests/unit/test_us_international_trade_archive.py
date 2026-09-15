"""Real-archive qualification tests for Census/BEA FT-900 releases."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    INTERNATIONAL_TRADE_CORRECTED_PERIODS,
    INTERNATIONAL_TRADE_INDEX_URI,
    INTERNATIONAL_TRADE_PREDECESSOR_URI,
    INTERNATIONAL_TRADE_PROGRAM_KEY,
    INTERNATIONAL_TRADE_RETIRED_INDEX_URI,
    INTERNATIONAL_TRADE_SPECIAL_REFERENCE_PERIODS,
    CensusInternationalTradeArchiveManifestV1,
    EconomicTimePrecision,
    OfficialSourceFormat,
    build_census_international_trade_archive_requests,
    build_census_international_trade_index_request,
    built_in_united_states_backfill_profile,
    census_international_trade_coverage_from_manifest,
    load_packaged_census_international_trade_archive_manifest,
    load_packaged_census_international_trade_index,
    load_packaged_census_international_trade_index_snapshot,
    load_packaged_official_source_registry,
    parse_census_international_trade_release_index,
)


def test_packaged_trade_archive_quantifies_every_release() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.bea-census.international-trade")
    manifest = load_packaged_census_international_trade_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.parser_id == "official.census-ft900.v1"
    assert (
        source.availability_time_precision is EconomicTimePrecision.EXACT_MINUTE
    )
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert isinstance(manifest, CensusInternationalTradeArchiveManifestV1)
    assert len(manifest.publications) == 319
    assert manifest.raw_artifact_count == 321
    assert manifest.unique_content_sha256_count == 321
    assert manifest.total_content_bytes == 349_754_556
    assert manifest.measure_occurrence_count == 319
    assert manifest.previous_as_known_measure_count == 319
    assert manifest.changed_revision_count == 296
    assert manifest.corrected_link_count == 6
    assert manifest.native_pdf_publication_count == 315
    assert manifest.hash_bound_publication_count == 4
    assert manifest.inferred_zone_count == 61
    assert manifest.source_zone_token_count == 258
    assert manifest.zone_mismatch_count == 5
    assert manifest.annual_revision_count == 27
    assert manifest.federal_disruption_notice_count == 8
    assert manifest.exact_minute_count == 319
    assert (
        CensusInternationalTradeArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )


def test_packaged_trade_index_replays_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_census_international_trade_archive_manifest()
    snapshot = load_packaged_census_international_trade_index_snapshot()
    index = parse_census_international_trade_release_index(
        snapshot, as_of_date=manifest.release_index.as_of_date
    )

    assert (
        build_census_international_trade_index_request(
            registry, as_of_date="2026-09-15"
        ).uri
        == INTERNATIONAL_TRADE_INDEX_URI
    )
    assert (
        INTERNATIONAL_TRADE_RETIRED_INDEX_URI != INTERNATIONAL_TRADE_INDEX_URI
    )
    assert index == manifest.release_index
    assert index == load_packaged_census_international_trade_index()
    assert len(index.releases) == 319
    assert index.releases[0].reference_period == "2000-01"
    assert index.releases[-1].reference_period == "2026-07"
    assert index.by_period["2019-02"].source_title == "Ferbruary 2019"
    assert (
        len(build_census_international_trade_archive_requests(registry, index))
        == 320
    )


def test_wrong_target_index_links_are_bounded() -> None:
    index = load_packaged_census_international_trade_index()
    corrected = [item for item in index.releases if item.link_corrected]

    assert [item.reference_period for item in corrected] == list(
        INTERNATIONAL_TRADE_CORRECTED_PERIODS
    )
    assert all(
        item.listed_uri != item.artifact_uri
        and item.correction_kind == "wrong-target"
        for item in corrected
    )
    with pytest.raises(ValueError, match="link correction differs"):
        replace(corrected[0], artifact_uri=corrected[0].listed_uri)


def test_hash_bound_pdf_fallbacks_are_exactly_scoped() -> None:
    manifest = load_packaged_census_international_trade_archive_manifest()
    special = [
        item.reference_period
        for item in manifest.publications
        if item.parser_era == "hash-bound-pdf"
    ]

    assert special == list(INTERNATIONAL_TRADE_SPECIAL_REFERENCE_PERIODS)
    assert manifest.by_period["2000-05"].measure.actual_lexical == "31.0"
    assert manifest.by_period["2000-08"].measure.actual_lexical == "29.4"
    assert manifest.by_period["2002-04"].measure.actual_lexical == "35.9"
    assert manifest.by_period["2015-07"].released_lexical == (
        "2015-09-03T08:30:00"
    )


def test_annual_revisions_do_not_rewrite_previous_vintages() -> None:
    manifest = load_packaged_census_international_trade_archive_manifest()
    april = manifest.by_period["2026-04"].measure

    assert april.actual_deficit_billions == 55.9
    assert april.previous_reference_period == "2026-03"
    assert april.previous_as_known_deficit_billions == 60.3
    assert april.revised_previous_deficit_billions == 56.6
    assert april.previous_was_revised
    assert april.annual_revision_context
    assert april.comparison_basis == "concurrent-annual-revision"
    assert (
        sum(
            item.measure.annual_revision_context
            for item in manifest.publications
        )
        == 27
    )


def test_source_time_zone_and_disruption_facts_remain_auditable() -> None:
    manifest = load_packaged_census_international_trade_archive_manifest()

    assert [
        item.reference_period
        for item in manifest.publications
        if item.zone_consistent is False
    ] == ["2005-02", "2005-03", "2005-04", "2007-02", "2007-03"]
    assert [
        (item.reference_period, item.release_date)
        for item in manifest.publications
        if item.federal_disruption_notice
    ] == [
        ("2013-08", "2013-10-24"),
        ("2018-11", "2019-02-06"),
        ("2019-01", "2019-03-27"),
        ("2025-08", "2025-11-19"),
        ("2025-09", "2025-12-11"),
        ("2025-10", "2026-01-08"),
        ("2025-11", "2026-01-29"),
        ("2025-12", "2026-02-19"),
    ]
    assert manifest.by_period["2000-01"].reported_zone is None
    assert manifest.by_period["2000-01"].zone_basis == (
        "producer-local-inference"
    )
    assert manifest.by_period["2026-07"].reported_zone == "EDT"


def test_trade_coverage_is_complete_for_builtin_profile() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_census_international_trade_archive_manifest()
    coverage = census_international_trade_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert coverage.expected_occurrence_count == 319
    assert coverage.schedule_count == 319
    assert coverage.initial_actual_count == 319
    assert coverage.previous_as_known_count == 319
    assert coverage.revision_count == 296
    assert coverage.exact_minute_count == 319
    assert len(coverage.artifact_sha256s) == 321
    assert coverage.is_complete_for(
        profile.by_key[INTERNATIONAL_TRADE_PROGRAM_KEY]
    )


def test_trade_manifest_rejects_identity_and_lineage_tampering() -> None:
    manifest = load_packaged_census_international_trade_archive_manifest()

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, profile_id=manifest.profile_id + "-tampered")

    with pytest.raises(ValueError, match="release lag is outside bounds"):
        replace(
            manifest.by_period["2026-07"],
            release_date="2027-01-08",
            publication_id="",
        )

    stale_index = replace(
        manifest.release_index,
        as_of_date="2026-08-01",
        index_id="",
    )
    with pytest.raises(ValueError, match="index coverage differs"):
        replace(manifest, release_index=stale_index, manifest_id="")

    last = manifest.by_period["2026-07"]
    repeated_artifact = replace(
        last.artifact,
        content_sha256=manifest.predecessor.content_sha256,
        content_length=manifest.predecessor.content_length,
        artifact_id="",
    )
    repeated_publication = replace(
        last,
        artifact=repeated_artifact,
        publication_id="",
    )
    repeated_publications = tuple(
        repeated_publication if item is last else item
        for item in manifest.publications
    )
    with pytest.raises(ValueError, match="repeats source bytes"):
        replace(
            manifest,
            publications=repeated_publications,
            manifest_id="",
        )

    publication = manifest.by_period["2026-07"]
    measure = publication.measure
    tampered_measure = replace(
        measure,
        previous_as_known_deficit_billions=73.4,
        previous_as_known_lexical="73.4",
        measure_id="",
    )
    tampered_publication = replace(
        publication, measure=tampered_measure, publication_id=""
    )
    publications = tuple(
        tampered_publication if item is publication else item
        for item in manifest.publications
    )
    with pytest.raises(ValueError, match="prior lineage differs"):
        replace(manifest, publications=publications, manifest_id="")


def test_predecessor_is_used_only_for_first_previous_as_known_value() -> None:
    manifest = load_packaged_census_international_trade_archive_manifest()
    first = manifest.by_period["2000-01"].measure

    assert (
        manifest.predecessor.artifact_uri == INTERNATIONAL_TRADE_PREDECESSOR_URI
    )
    assert manifest.predecessor_actual_deficit_billions == 25.5
    assert first.previous_as_known_reference_period == "1999-12"
    assert first.previous_as_known_deficit_billions == 25.5
    assert first.previous_artifact_uri == INTERNATIONAL_TRADE_PREDECESSOR_URI
