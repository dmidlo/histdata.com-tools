"""Real-archive qualification tests for Census housing construction."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    HOUSING_APRIL_2009_ARTIFACT_URI,
    HOUSING_APRIL_2009_LISTED_URI,
    HOUSING_AUGUST_2012_ARTIFACT_URI,
    HOUSING_AUGUST_2012_LISTED_URI,
    HOUSING_BUILDING_PERMITS,
    HOUSING_COMPLETIONS,
    HOUSING_INDEX_URI,
    HOUSING_MEASURE_KEYS,
    HOUSING_NOVEMBER_2000_ARTIFACT_URI,
    HOUSING_NOVEMBER_2000_LISTED_URI,
    HOUSING_PROGRAM_KEY,
    HOUSING_SPECIAL_PRIMARY_PERIODS,
    HOUSING_STARTS,
    CensusHousingArchiveManifestV1,
    EconomicTimePrecision,
    OfficialSourceFormat,
    build_census_housing_archive_requests,
    build_census_housing_index_request,
    built_in_united_states_backfill_profile,
    census_housing_coverage_from_manifest,
    load_packaged_census_housing_archive_manifest,
    load_packaged_census_housing_index,
    load_packaged_census_housing_index_snapshot,
    load_packaged_official_source_registry,
    parse_census_housing_release_index,
)


def test_packaged_housing_archive_quantifies_every_measure() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.census.new-residential-construction")
    manifest = load_packaged_census_housing_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.parser_id == "official.census-nrc.v1"
    assert (
        source.availability_time_precision is EconomicTimePrecision.EXACT_MINUTE
    )
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
    }
    assert isinstance(manifest, CensusHousingArchiveManifestV1)
    assert len(manifest.publications) == 330
    assert manifest.raw_artifact_count == 334
    assert manifest.unique_content_sha256_count == 334
    assert manifest.total_content_bytes == 47_485_575
    assert manifest.measure_occurrence_count == 957
    assert manifest.previous_as_known_measure_count == 943
    assert manifest.comparable_measure_count == 943
    assert manifest.changed_revision_count == 916
    assert manifest.corrected_link_count == 3
    assert manifest.split_text_publication_count == 30
    assert manifest.layout_pdf_publication_count == 264
    assert manifest.standard_pdf_fallback_count == 31
    assert manifest.hash_bound_special_publication_count == 5
    assert manifest.catch_up_publication_count == 5
    assert manifest.disruption_publication_count == 8
    assert manifest.historical_revision_notice_count == 25
    assert manifest.ten_am_publication_count == 15
    assert manifest.exact_minute_count == 330
    assert manifest.zone_mismatch_count == 0
    assert (
        CensusHousingArchiveManifestV1.from_json(manifest.to_json()) == manifest
    )


def test_packaged_housing_index_replays_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_census_housing_archive_manifest()
    snapshot = load_packaged_census_housing_index_snapshot()
    index = parse_census_housing_release_index(
        snapshot, as_of_date=manifest.release_index.as_of_date
    )

    assert (
        build_census_housing_index_request(
            registry, as_of_date="2026-09-15"
        ).uri
        == HOUSING_INDEX_URI
    )
    assert index == manifest.release_index
    assert index == load_packaged_census_housing_index()
    assert len(index.releases) == 331
    assert [
        (item.reference_period, item.publication_kind)
        for item in index.releases[:2]
    ] == [
        ("2000-01", "starts-permits-text"),
        ("2000-01", "completions-text"),
    ]
    assert index.releases[-1].reference_period == "2026-07"
    assert len(build_census_housing_archive_requests(registry, index)) == 333


def test_all_three_broken_index_links_are_bounded_corrections() -> None:
    index = load_packaged_census_housing_index()
    corrected = [item for item in index.releases if item.link_corrected]

    assert [
        (item.listed_uri, item.artifact_uri, item.correction_kind)
        for item in corrected
    ] == [
        (
            HOUSING_NOVEMBER_2000_LISTED_URI,
            HOUSING_NOVEMBER_2000_ARTIFACT_URI,
            "wrong-target",
        ),
        (
            HOUSING_APRIL_2009_LISTED_URI,
            HOUSING_APRIL_2009_ARTIFACT_URI,
            "malformed-scheme",
        ),
        (
            HOUSING_AUGUST_2012_LISTED_URI,
            HOUSING_AUGUST_2012_ARTIFACT_URI,
            "wrong-target",
        ),
    ]


def test_split_releases_and_2013_alias_preserve_publication_identity() -> None:
    manifest = load_packaged_census_housing_archive_manifest()
    january = manifest.by_primary_period["2000-01"]
    october = manifest.by_primary_period["2013-10"]
    november = manifest.by_primary_period["2013-11"]

    assert len(january) == 2
    assert [item.released_lexical for item in january] == [
        "2000-02-16T08:30:00",
        "2000-03-06T10:00:00",
    ]
    assert [set(item.by_reference_measure) for item in january] == [
        {
            ("2000-01", HOUSING_BUILDING_PERMITS),
            ("2000-01", HOUSING_STARTS),
        },
        {("2000-01", HOUSING_COMPLETIONS)},
    ]
    assert len(october) == 1
    assert len(october[0].artifacts) == 2
    assert october[0].released_lexical == "2013-11-26T08:30:00"
    assert len(october[0].measures) == 2
    assert len(november[0].measures) == 7


def test_catch_up_reports_do_not_invent_missed_publication_times() -> None:
    manifest = load_packaged_census_housing_archive_manifest()
    catch_ups = [
        item for item in manifest.publications if item.catch_up_publication
    ]
    noncomparable = [
        measure
        for publication in manifest.publications
        for measure in publication.measures
        if not measure.revision_comparable
    ]

    assert [item.primary_reference_period for item in catch_ups] == list(
        HOUSING_SPECIAL_PRIMARY_PERIODS
    )
    assert len(noncomparable) == 14
    assert all(
        item.comparison_basis == "same-publication-catch-up"
        and item.previous_as_known_thousands is None
        and item.previous_artifact_uri is None
        for item in noncomparable
    )
    september_2025 = manifest.by_reference_measure[
        ("2025-09", HOUSING_BUILDING_PERMITS)
    ]
    assert september_2025.actual_thousands == 1415
    assert september_2025.previous_as_known_thousands == 1312
    assert september_2025.revised_previous_thousands == 1330
    assert set(HOUSING_MEASURE_KEYS) == {
        HOUSING_BUILDING_PERMITS,
        HOUSING_STARTS,
        HOUSING_COMPLETIONS,
    }


def test_housing_coverage_is_complete_for_the_builtin_profile() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_census_housing_archive_manifest()
    coverage = census_housing_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert coverage.expected_occurrence_count == 330
    assert coverage.schedule_count == 330
    assert coverage.initial_actual_count == 330
    assert coverage.previous_as_known_count == 330
    assert coverage.revision_count == 916
    assert coverage.exact_minute_count == 330
    assert len(coverage.artifact_sha256s) == 334
    assert coverage.is_complete_for(profile.by_key[HOUSING_PROGRAM_KEY])


def test_housing_manifest_rejects_identity_tampering() -> None:
    manifest = load_packaged_census_housing_archive_manifest()

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, profile_id=manifest.profile_id + "-tampered")


def test_housing_manifest_rejects_semantic_lineage_and_special_hash_drift() -> (
    None
):
    manifest = load_packaged_census_housing_archive_manifest()
    publication = manifest.by_primary_period["2001-04"][0]
    measure = publication.measures[0]
    assert measure.previous_as_known_thousands is not None
    changed = measure.previous_as_known_thousands + 1
    changed_measure = replace(
        measure,
        previous_as_known_thousands=changed,
        previous_as_known_lexical=f"{changed:,},000",
        measure_id="",
    )
    changed_publication = replace(
        publication,
        measures=(changed_measure, *publication.measures[1:]),
        publication_id="",
    )
    publications = tuple(
        changed_publication if item == publication else item
        for item in manifest.publications
    )
    with pytest.raises(ValueError, match="previous-as-known lineage differs"):
        replace(manifest, publications=publications, manifest_id="")

    catch_up = manifest.by_primary_period["2025-10"][0]
    changed_artifact = replace(
        catch_up.artifacts[0], content_sha256="0" * 64, artifact_id=""
    )
    with pytest.raises(ValueError, match="catch-up artifact binding differs"):
        replace(
            catch_up,
            artifacts=(changed_artifact,),
            publication_id="",
        )
