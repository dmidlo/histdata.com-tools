"""Complete-archive qualification tests for Federal Reserve H.6."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    H6_FINAL_WEEKLY_RELEASE_DATE,
    H6_FIRST_MONTHLY_RELEASE_DATE,
    H6_INDEX_URI,
    H6_LATEST_PACKAGED_RELEASE_DATE,
    H6_PARSER_ID,
    H6_PROGRAM_KEY,
    H6_RELEASE_DATES_URI,
    H6_SOURCE_KEY,
    EconomicEventFamily,
    EconomicTimePrecision,
    FederalReserveH6ArchiveManifestV1,
    H6PublicationFrequency,
    OfficialFederalReserveH6ParserV1,
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    build_federal_reserve_h6_index_requests,
    build_federal_reserve_h6_release_requests,
    built_in_united_states_backfill_profile,
    federal_reserve_h6_coverage_from_manifest,
    load_packaged_federal_reserve_h6_archive_manifest,
    load_packaged_federal_reserve_h6_index_pages,
    load_packaged_official_source_registry,
    parse_federal_reserve_h6_release_index,
    resolve_official_source_parser,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.JSON: "application/json",
    }[request.source_format]
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=CAPTURED_AT_NS,
        completed_at_ns=CAPTURED_AT_NS + 1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": content_type},
        content=content,
        content_type=content_type,
    )


def test_h6_source_parser_profile_and_manifest_are_bound() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    source = registry.source(H6_SOURCE_KEY)
    manifest = load_packaged_federal_reserve_h6_archive_manifest()

    assert H6_PROGRAM_KEY == H6_SOURCE_KEY == "us.frb.money-stock"
    assert source.parser_id == H6_PARSER_ID
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.JSON,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
    }
    assert source.event_families == (EconomicEventFamily.MONEY_CREDIT,)
    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "scheduled-only"
    assert isinstance(
        resolve_official_source_parser(source),
        OfficialFederalReserveH6ParserV1,
    )
    assert profile.by_key[H6_PROGRAM_KEY].source_key == H6_SOURCE_KEY
    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id


def test_packaged_h6_archive_quantifies_complete_native_frequency_eras() -> (
    None
):
    manifest = load_packaged_federal_reserve_h6_archive_manifest()

    assert isinstance(manifest, FederalReserveH6ArchiveManifestV1)
    assert manifest.release_index.as_of_date == "2026-09-15"
    assert len(manifest.release_index.entries) == 1_170
    assert len(manifest.publications) == 1_169
    assert manifest.publications[0].release_date == "2000-01-06"
    assert (
        manifest.publications[-1].release_date
        == H6_LATEST_PACKAGED_RELEASE_DATE
    )
    assert manifest.raw_artifact_count == 2_247
    assert manifest.total_content_bytes == 174_852_559
    assert manifest.release_artifact_bytes == 127_582_888
    assert manifest.timing_pdf_bytes == 47_121_598
    assert manifest.weekly_count == 1_102
    assert manifest.monthly_count == 67
    assert manifest.exact_minute_count == 1_091
    assert manifest.scheduled_only_count == 78
    assert manifest.revision_occurrence_count == 973
    assert Counter(item.parser_era for item in manifest.publications) == {
        "preformatted-m3-weekly": 324,
        "preformatted-m2-weekly": 396,
        "legacy-table-weekly": 178,
        "modern-table-weekly": 204,
        "modern-table-monthly": 67,
    }
    assert Counter(
        item.release_artifact.source_format for item in manifest.publications
    ) == {"html": 1_168, "text": 1}


def test_h6_triplets_preserve_previous_as_known_and_transition_semantics() -> (
    None
):
    manifest = load_packaged_federal_reserve_h6_archive_manifest()
    final_weekly = manifest.by_release_date[H6_FINAL_WEEKLY_RELEASE_DATE]
    first_monthly = manifest.by_release_date[H6_FIRST_MONTHLY_RELEASE_DATE]
    latest = manifest.publications[-1]

    assert final_weekly.frequency is H6PublicationFrequency.WEEKLY
    assert final_weekly.reference_period == "2021-02-01"
    assert final_weekly.actual_lexical == "19414.9"
    assert final_weekly.previous_reference_period == "2021-01-25"
    assert final_weekly.previous_as_known_lexical == "19514.3"
    assert final_weekly.revised_previous_lexical == "19411.9"
    assert first_monthly.frequency is H6PublicationFrequency.MONTHLY
    assert first_monthly.first_monthly_release
    assert first_monthly.reference_period == "2021-01"
    assert first_monthly.actual_lexical == "19394.6"
    assert first_monthly.previous_reference_period == "2020-12"
    assert first_monthly.previous_as_known_lexical == "19088.8"
    assert first_monthly.revised_previous_lexical == "19088.8"
    assert first_monthly.time_precision is EconomicTimePrecision.EXACT_MINUTE
    assert latest.reference_period == "2026-07"
    assert latest.actual_lexical == "23218.0"
    assert latest.previous_as_known_lexical == "23155.2"
    assert latest.revised_previous_lexical == "23115.2"
    assert latest.time_precision is EconomicTimePrecision.SCHEDULED_ONLY
    assert latest.released_at_ns is None


def test_h6_index_retains_all_occurrence_scoped_archive_corrections() -> None:
    manifest = load_packaged_federal_reserve_h6_archive_manifest()
    index = manifest.release_index
    corrections = {
        item.indexed_date: item
        for item in index.entries
        if item.indexed_date != item.release_date
    }

    assert {
        indexed: item.release_date for indexed, item in corrections.items()
    } == {
        "2005-03-05": "2005-03-03",
        "2013-04-05": "2013-04-04",
        "2016-11-18": "2016-11-17",
        "2017-11-23": "2017-11-24",
    }
    assert corrections["2005-03-05"].release_uri.endswith("/20050305/")
    assert corrections["2013-04-05"].release_uri.endswith("/20130404/")
    assert corrections["2016-11-18"].release_uri.endswith("/20161118/")
    assert corrections["2017-11-23"].release_uri.endswith("/20171123/")
    text_fallback = index.by_release_date["2002-06-13"]
    assert text_fallback.release_format is OfficialSourceFormat.TEXT
    assert text_fallback.release_uri.endswith("/20020613/h6.txt")
    assert (
        manifest.by_release_date["2002-06-13"].release_artifact.source_format
        == "text"
    )
    assert sum(item.annual_seasonal_review for item in index.entries) == 22
    assert sum(item.first_monthly_release for item in index.entries) == 1


def test_packaged_h6_index_pages_are_hash_bound_and_reparse_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_federal_reserve_h6_archive_manifest()
    pages = load_packaged_federal_reserve_h6_index_pages()
    requests = build_federal_reserve_h6_index_requests(
        registry, as_of_date=manifest.release_index.as_of_date
    )

    assert set(pages) == {H6_INDEX_URI, H6_RELEASE_DATES_URI}
    assert set(pages) == {item.uri for item in requests}
    assert (
        len(pages[H6_INDEX_URI])
        == manifest.release_index.index_page.content_length
    )
    assert (
        len(pages[H6_RELEASE_DATES_URI])
        == manifest.release_index.release_dates.content_length
    )
    rebuilt = parse_federal_reserve_h6_release_index(
        tuple(_snapshot(request, pages[request.uri]) for request in requests),
        as_of_date=manifest.release_index.as_of_date,
    )
    assert rebuilt == manifest.release_index

    releases = build_federal_reserve_h6_release_requests(registry, rebuilt)
    assert len(releases) == 2_245
    assert Counter(item.source_format for item in releases) == {
        OfficialSourceFormat.HTML: 1_169,
        OfficialSourceFormat.TEXT: 1,
        OfficialSourceFormat.PDF: 1_075,
    }
    assert {item.parser_id for item in releases} == {H6_PARSER_ID}


def test_h6_manifest_round_trip_coverage_and_fail_closed_invariants() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_federal_reserve_h6_archive_manifest()
    coverage = federal_reserve_h6_coverage_from_manifest(profile, manifest)

    assert (
        FederalReserveH6ArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    assert coverage.is_complete_for(profile.by_key[H6_PROGRAM_KEY])
    assert coverage.expected_occurrence_count == 1_169
    assert coverage.schedule_count == 1_169
    assert coverage.initial_actual_count == 1_169
    assert coverage.previous_as_known_count == 1_169
    assert coverage.revision_count == 973
    assert coverage.exact_minute_count == 1_091
    assert len(coverage.artifact_sha256s) == 1_169

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, registry_id=registry.registry_id + "0")
    with pytest.raises(ValueError, match="summary counts differ"):
        replace(manifest, revision_occurrence_count=972, manifest_id="")
    with pytest.raises(ValueError, match="lexical/numeric values differ"):
        replace(
            manifest.publications[0],
            actual_value=0.0,
            publication_id="",
        )
