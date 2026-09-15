"""Real-archive qualification tests for Census MTIS releases."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    MTIS_BRIEFING_URI,
    MTIS_CORRECTED_PERIODS,
    MTIS_DIRECTORY_URI,
    MTIS_DISRUPTION_PERIODS,
    MTIS_INDEX_URI,
    MTIS_MEASURE_KEYS,
    MTIS_PREDECESSOR_URI,
    MTIS_PROGRAM_KEY,
    MTIS_RELEASE_METADATA_FALLBACK_PERIODS,
    MTIS_RETIRED_INDEX_URI,
    CensusMTISArchiveManifestV1,
    EconomicTimePrecision,
    OfficialSourceFormat,
    build_census_mtis_archive_requests,
    build_census_mtis_index_requests,
    built_in_united_states_backfill_profile,
    census_mtis_coverage_from_manifest,
    load_packaged_census_mtis_archive_manifest,
    load_packaged_census_mtis_index,
    load_packaged_census_mtis_index_evidence,
    load_packaged_official_source_registry,
    parse_census_mtis_release_index,
)


def test_packaged_mtis_archive_quantifies_every_concept() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.census.mtis")
    manifest = load_packaged_census_mtis_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.parser_id == "official.census-mtis.v1"
    assert (
        source.availability_time_precision is EconomicTimePrecision.EXACT_MINUTE
    )
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
        OfficialSourceFormat.XLS,
        OfficialSourceFormat.XLSX,
    }
    assert isinstance(manifest, CensusMTISArchiveManifestV1)
    assert len(manifest.publications) == 318
    assert manifest.raw_artifact_count == 635
    assert manifest.unique_content_sha256_count == 635
    assert manifest.total_content_bytes == 91_178_773
    assert manifest.measure_occurrence_count == 954
    assert manifest.previous_as_known_measure_count == 954
    assert manifest.changed_revision_count == 723
    assert manifest.corrected_index_entry_count == 3
    assert manifest.legacy_text_publication_count == 157
    assert manifest.legacy_xls_publication_count == 109
    assert manifest.modern_xlsx_publication_count == 52
    assert manifest.release_document_count == 306
    assert manifest.archived_page_metadata_count == 12
    assert manifest.previous_schedule_metadata_count == 3
    assert manifest.native_pdf_metadata_count == 303
    assert manifest.disruption_publication_count == 9
    assert manifest.exact_minute_count == 318
    assert manifest.zone_mismatch_count == 0
    assert CensusMTISArchiveManifestV1.from_json(manifest.to_json()) == manifest


def test_packaged_mtis_index_replays_all_evidence() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_census_mtis_archive_manifest()
    snapshots, witnesses = load_packaged_census_mtis_index_evidence()
    index = parse_census_mtis_release_index(
        snapshots,
        witnesses,
        as_of_date=manifest.release_index.as_of_date,
    )

    requests = build_census_mtis_index_requests(
        registry, as_of_date="2026-09-15"
    )
    assert {item.uri for item in requests} == {
        MTIS_INDEX_URI,
        MTIS_DIRECTORY_URI,
        MTIS_BRIEFING_URI,
    }
    assert MTIS_RETIRED_INDEX_URI != MTIS_INDEX_URI
    assert index == manifest.release_index
    assert index == load_packaged_census_mtis_index()
    assert len(index.releases) == 318
    assert index.releases[0].reference_period == "2000-01"
    assert index.releases[-1].reference_period == "2026-06"
    assert len(index.evidence) == 10
    assert len(build_census_mtis_archive_requests(registry, index)) == 625


def test_mtis_archive_corrections_are_exactly_bounded() -> None:
    index = load_packaged_census_mtis_index()
    corrected = [item for item in index.releases if item.link_corrected]

    assert tuple(item.reference_period for item in corrected) == (
        MTIS_CORRECTED_PERIODS
    )
    assert [item.correction_kind for item in corrected] == [
        "unlisted-pdf",
        "broken-spreadsheet-text-fallback",
        "current-not-yet-indexed",
    ]
    july_2001, may_2014, june_2026 = corrected
    assert july_2001.listed_pdf_uri is None
    assert july_2001.pdf_uri and july_2001.pdf_uri.endswith("mtis0107.pdf")
    assert may_2014.listed_data_uri and may_2014.listed_data_uri.endswith(
        "mtis1405.xls"
    )
    assert may_2014.data_uri.endswith("mtis1405.txt")
    assert june_2026.listed_data_uri is None
    assert june_2026.listed_pdf_uri is None
    assert june_2026.data_uri.endswith("mtis2606.xlsx")


def test_mtis_three_concepts_and_revision_lineage_remain_distinct() -> None:
    manifest = load_packaged_census_mtis_archive_manifest()
    first = manifest.by_period["2000-01"]
    last = manifest.by_period["2026-06"]

    assert (
        tuple(item.measure_key for item in first.measures) == MTIS_MEASURE_KEYS
    )
    assert [item.unit for item in first.measures] == [
        "millions-of-us-dollars",
        "millions-of-us-dollars",
        "ratio",
    ]
    assert [
        (
            item.actual_value,
            item.previous_as_known_value,
            item.revised_previous_value,
        )
        for item in first.measures
    ] == [
        (876_615, 868_827, 869_946),
        (1_150_442, 1_145_115, 1_144_898),
        (1.31, 1.32, 1.32),
    ]
    assert all(
        item.previous_artifact_uri == MTIS_PREDECESSOR_URI
        for item in first.measures
    )
    assert [item.actual_value for item in last.measures] == [
        2_111_298,
        2_740_239,
        1.30,
    ]
    assert [item.revised_previous_value for item in last.measures] == [
        2_135_487,
        2_739_202,
        1.28,
    ]


def test_mtis_release_time_fallbacks_do_not_invent_timestamps() -> None:
    manifest = load_packaged_census_mtis_archive_manifest()
    fallback = [
        item
        for item in manifest.publications
        if item.release_metadata_basis == "previous-release-schedule"
    ]

    assert tuple(item.reference_period for item in fallback) == (
        MTIS_RELEASE_METADATA_FALLBACK_PERIODS
    )
    assert [item.released_lexical for item in fallback] == [
        "2001-06-14T08:30:00",
        "2003-06-12T10:00:00",
        "2010-01-14T10:00:00",
    ]
    assert fallback[0].release_metadata_artifact_uri.endswith("mtis0103.pdf")
    assert fallback[1].release_metadata_artifact_uri.endswith("mtis0303.pdf")
    assert fallback[2].release_metadata_artifact_uri.endswith("mtis0910.pdf")
    assert all("scheduled" in item.source_header_lexical for item in fallback)


def test_mtis_legacy_and_disruption_dates_are_source_backed() -> None:
    manifest = load_packaged_census_mtis_archive_manifest()
    legacy = manifest.publications[:12]
    disruptions = [
        item.reference_period
        for item in manifest.publications
        if item.federal_disruption
    ]

    assert all(
        item.release_metadata_basis == "archived-census-page"
        and item.released_lexical.endswith("T08:30:00")
        for item in legacy
    )
    assert legacy[0].released_lexical == "2000-03-15T08:30:00"
    assert legacy[-1].released_lexical == "2001-02-14T08:30:00"
    assert tuple(disruptions) == MTIS_DISRUPTION_PERIODS


def test_mtis_coverage_is_complete_for_builtin_profile() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_census_mtis_archive_manifest()
    coverage = census_mtis_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert coverage.expected_occurrence_count == 318
    assert coverage.schedule_count == 318
    assert coverage.initial_actual_count == 318
    assert coverage.previous_as_known_count == 318
    assert coverage.revision_count == 723
    assert coverage.exact_minute_count == 318
    assert len(coverage.artifact_sha256s) == 635
    assert coverage.is_complete_for(profile.by_key[MTIS_PROGRAM_KEY])


def test_mtis_manifest_rejects_identity_and_lineage_tampering() -> None:
    manifest = load_packaged_census_mtis_archive_manifest()

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, profile_id=manifest.profile_id + "-tampered")

    publication = manifest.by_period["2000-01"]
    measure = publication.measures[0]
    changed = int(measure.previous_as_known_value) + 1
    changed_measure = replace(
        measure,
        previous_as_known_value=changed,
        previous_as_known_lexical=f"{changed:,}",
        measure_id="",
    )
    changed_publication = replace(
        publication,
        measures=(changed_measure, *publication.measures[1:]),
        publication_id="",
    )
    changed_publications = tuple(
        changed_publication if item == publication else item
        for item in manifest.publications
    )
    with pytest.raises(ValueError, match="prior lineage differs"):
        replace(
            manifest,
            publications=changed_publications,
            manifest_id="",
        )

    with pytest.raises(ValueError, match="changed_revision_count differs"):
        replace(
            manifest,
            changed_revision_count=manifest.changed_revision_count - 1,
        )
