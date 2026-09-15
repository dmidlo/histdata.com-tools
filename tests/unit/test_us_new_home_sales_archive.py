"""Real-archive qualification tests for Census New Residential Sales."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    NEW_HOME_SALES_DECEMBER_2018_ARTIFACT_URI,
    NEW_HOME_SALES_DECEMBER_2018_LISTED_URI,
    NEW_HOME_SALES_INDEX_URI,
    NEW_HOME_SALES_PREDECESSOR_URI,
    NEW_HOME_SALES_PROGRAM_KEY,
    NEW_HOME_SALES_RETIRED_INDEX_URI,
    NEW_HOME_SALES_SPECIAL_PRIMARY_PERIODS,
    CensusNewHomeSalesArchiveManifestV1,
    EconomicTimePrecision,
    OfficialSourceFormat,
    build_census_new_home_sales_archive_requests,
    build_census_new_home_sales_index_request,
    built_in_united_states_backfill_profile,
    census_new_home_sales_coverage_from_manifest,
    load_packaged_census_new_home_sales_archive_manifest,
    load_packaged_census_new_home_sales_index,
    load_packaged_census_new_home_sales_index_snapshot,
    load_packaged_official_source_registry,
    parse_census_new_home_sales_release_index,
)


def test_packaged_new_home_sales_archive_quantifies_every_measure() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.census.new-residential-sales")
    manifest = load_packaged_census_new_home_sales_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.parser_id == "official.census-nrs.v1"
    assert (
        source.availability_time_precision is EconomicTimePrecision.EXACT_MINUTE
    )
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
    }
    assert isinstance(manifest, CensusNewHomeSalesArchiveManifestV1)
    assert len(manifest.publications) == 315
    assert manifest.raw_artifact_count == 318
    assert manifest.unique_content_sha256_count == 318
    assert manifest.total_content_bytes == 41_950_804
    assert manifest.measure_occurrence_count == 319
    assert manifest.previous_as_known_measure_count == 315
    assert manifest.comparable_measure_count == 315
    assert manifest.changed_revision_count == 306
    assert manifest.corrected_link_count == 1
    assert manifest.legacy_text_publication_count == 15
    assert manifest.layout_pdf_publication_count == 294
    assert manifest.standard_pdf_fallback_count == 2
    assert manifest.hash_bound_special_publication_count == 4
    assert manifest.catch_up_publication_count == 4
    assert manifest.disruption_publication_count == 7
    assert manifest.historical_revision_notice_count == 14
    assert manifest.exact_minute_count == 315
    assert manifest.zone_mismatch_count == 2
    assert manifest.source_previous_period_mismatch_count == 1
    assert (
        CensusNewHomeSalesArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )


def test_packaged_new_home_sales_index_replays_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_census_new_home_sales_archive_manifest()
    snapshot = load_packaged_census_new_home_sales_index_snapshot()
    index = parse_census_new_home_sales_release_index(
        snapshot, as_of_date=manifest.release_index.as_of_date
    )

    assert (
        build_census_new_home_sales_index_request(
            registry, as_of_date="2026-09-15"
        ).uri
        == NEW_HOME_SALES_INDEX_URI
    )
    assert NEW_HOME_SALES_RETIRED_INDEX_URI != NEW_HOME_SALES_INDEX_URI
    assert index == manifest.release_index
    assert index == load_packaged_census_new_home_sales_index()
    assert len(index.releases) == 316
    assert index.releases[0].reference_period == "2000-01"
    assert index.releases[-1].reference_period == "2026-07"
    assert (
        len(build_census_new_home_sales_archive_requests(registry, index))
        == 317
    )


def test_broken_december_2018_index_link_is_bounded() -> None:
    index = load_packaged_census_new_home_sales_index()
    corrected = [item for item in index.releases if item.link_corrected]

    assert [
        (item.listed_uri, item.artifact_uri, item.correction_kind)
        for item in corrected
    ] == [
        (
            NEW_HOME_SALES_DECEMBER_2018_LISTED_URI,
            NEW_HOME_SALES_DECEMBER_2018_ARTIFACT_URI,
            "wrong-target",
        )
    ]


def test_2013_aliases_preserve_one_real_catch_up_publication() -> None:
    manifest = load_packaged_census_new_home_sales_archive_manifest()
    publication = manifest.by_primary_period["2013-10"]

    assert len(publication.index_entry_ids) == 2
    assert len(publication.artifacts) == 2
    assert publication.released_lexical == "2013-12-04T10:00:00"
    assert [item.reference_period for item in publication.measures] == [
        "2013-09",
        "2013-10",
    ]
    assert [item.actual_thousands for item in publication.measures] == [
        354,
        444,
    ]
    assert publication.measures[0].revision_comparable
    assert not publication.measures[1].revision_comparable


def test_later_catch_up_reports_do_not_invent_release_times() -> None:
    manifest = load_packaged_census_new_home_sales_archive_manifest()
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
        NEW_HOME_SALES_SPECIAL_PRIMARY_PERIODS
    )
    assert [item.reference_period for item in noncomparable] == [
        "2013-10",
        "2025-10",
        "2025-12",
        "2026-03",
    ]
    assert all(
        item.comparison_basis == "same-publication-catch-up"
        and item.previous_as_known_thousands is None
        and item.previous_artifact_uri is None
        for item in noncomparable
    )
    assert manifest.by_reference_period["2025-09"].actual_thousands == 738
    assert (
        manifest.by_reference_period["2025-09"].previous_as_known_thousands
        == 800
    )
    assert (
        manifest.by_reference_period["2025-09"].revised_previous_thousands
        == 711
    )


def test_source_authored_date_labels_remain_auditable() -> None:
    manifest = load_packaged_census_new_home_sales_archive_manifest()
    mismatch = manifest.by_reference_period["2001-02"]
    bad_zones = [
        item.primary_reference_period
        for item in manifest.publications
        if not item.zone_consistent
    ]

    assert mismatch.previous_reference_period == "2001-01"
    assert mismatch.source_previous_reference_period == "2000-01"
    assert mismatch.previous_artifact_uri.endswith("c25_0101.txt")
    assert bad_zones == ["2004-03", "2019-01"]


def test_new_home_sales_coverage_is_complete_for_builtin_profile() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_census_new_home_sales_archive_manifest()
    coverage = census_new_home_sales_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert coverage.expected_occurrence_count == 315
    assert coverage.schedule_count == 315
    assert coverage.initial_actual_count == 315
    assert coverage.previous_as_known_count == 315
    assert coverage.revision_count == 306
    assert coverage.exact_minute_count == 315
    assert len(coverage.artifact_sha256s) == 318
    assert coverage.is_complete_for(profile.by_key[NEW_HOME_SALES_PROGRAM_KEY])


def test_new_home_sales_manifest_rejects_identity_and_lineage_tampering() -> (
    None
):
    manifest = load_packaged_census_new_home_sales_archive_manifest()

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, profile_id=manifest.profile_id + "-tampered")

    publication = manifest.by_primary_period["2001-04"]
    measure = publication.measures[0]
    changed = measure.previous_as_known_thousands
    assert changed is not None
    changed += 1
    changed_measure = replace(
        measure,
        previous_as_known_thousands=changed,
        previous_as_known_lexical=f"{changed:,},000",
        measure_id="",
    )
    changed_publication = replace(
        publication,
        measures=(changed_measure,),
        publication_id="",
    )
    publications = tuple(
        changed_publication if item == publication else item
        for item in manifest.publications
    )
    with pytest.raises(ValueError, match="prior lineage differs"):
        replace(manifest, publications=publications, manifest_id="")

    catch_up = manifest.by_primary_period["2025-10"]
    changed_artifact = replace(
        catch_up.artifacts[0], content_sha256="0" * 64, artifact_id=""
    )
    with pytest.raises(ValueError, match="artifact binding differs"):
        replace(
            catch_up,
            artifacts=(changed_artifact,),
            publication_id="",
        )


def test_predecessor_is_explicit_external_lineage_evidence() -> None:
    manifest = load_packaged_census_new_home_sales_archive_manifest()
    january = manifest.by_reference_period["2000-01"]

    assert manifest.predecessor.artifact_uri == NEW_HOME_SALES_PREDECESSOR_URI
    assert january.actual_thousands == 882
    assert january.previous_as_known_thousands == 900
    assert january.revised_previous_thousands == 921
