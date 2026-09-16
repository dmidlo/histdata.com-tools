"""Tests for the retained DOL/ETA Initial Claims archive qualification."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    INITIAL_CLAIMS_ARCHIVE_URI,
    INITIAL_CLAIMS_LATEST_PACKAGED_RELEASE_DATE,
    INITIAL_CLAIMS_PARSER_ID,
    INITIAL_CLAIMS_PROGRAM_KEY,
    DolInitialClaimsArchiveManifestV1,
    DolInitialClaimsPublicationV1,
    InitialClaimsComparisonBasis,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    build_dol_initial_claims_index_requests,
    build_dol_initial_claims_release_requests,
    built_in_united_states_backfill_profile,
    dol_initial_claims_coverage_from_manifest,
    load_packaged_dol_initial_claims_archive_manifest,
    load_packaged_dol_initial_claims_index_pages,
    load_packaged_official_source_registry,
    packaged_dol_initial_claims_indexes_path,
    packaged_dol_initial_claims_manifest_path,
    parse_dol_initial_claims_release_index,
    parse_with_built_in_official_adapter,
)

AS_OF_DATE = "2026-09-16"
CAPTURED_AT_NS = 1_789_530_000_000_000_000


def _index_snapshots() -> tuple[OfficialRawSnapshotV1, ...]:
    registry = load_packaged_official_source_registry()
    pages = load_packaged_dol_initial_claims_index_pages()
    requests = build_dol_initial_claims_index_requests(
        registry, as_of_date=AS_OF_DATE
    )
    snapshots = []
    for request in requests:
        key = (
            "landing"
            if request.page_number is None
            else f"year:{request.window_start[:4]}"
        )
        snapshots.append(
            OfficialRawSnapshotV1(
                request=request,
                retrieved_at_ns=CAPTURED_AT_NS,
                completed_at_ns=CAPTURED_AT_NS + 1,
                status_code=200,
                resolved_uri=request.uri,
                response_headers={"Content-Type": "text/html"},
                content=pages[key],
                content_type="text/html",
            )
        )
    return tuple(snapshots)


def test_registry_profile_and_request_contracts_are_exact() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    source = registry.source("us.dol.eta-unemployment-insurance")
    program = profile.by_key[INITIAL_CLAIMS_PROGRAM_KEY]

    assert source.parser_id == INITIAL_CLAIMS_PARSER_ID
    assert source.archive_uri == INITIAL_CLAIMS_ARCHIVE_URI
    assert source.verification_status.value == "empirically-verified"
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert program.archive_start_date == "2002-10-17"
    assert program.scheduled_time_local == "08:30"
    assert program.source_key == source.source_key

    requests = build_dol_initial_claims_index_requests(
        registry, as_of_date=AS_OF_DATE
    )
    assert len(requests) == 26
    assert requests[0].method is OfficialRequestMethod.GET
    assert requests[0].uri == INITIAL_CLAIMS_ARCHIVE_URI
    assert requests[1].method is OfficialRequestMethod.POST
    assert requests[1].body_text == "report=press&year=2002&submit=Submit"
    assert requests[-1].body_text == "report=press&year=2026&submit=Submit"


def test_packaged_index_pages_reparse_to_the_manifest_index() -> None:
    manifest = load_packaged_dol_initial_claims_archive_manifest()
    pages = load_packaged_dol_initial_claims_index_pages()
    observed = parse_dol_initial_claims_release_index(
        _index_snapshots(), as_of_date=AS_OF_DATE
    )

    assert packaged_dol_initial_claims_indexes_path().is_file()
    assert len(pages) == 26
    assert observed == manifest.release_index
    assert len(observed.entries) == 1_240
    assert observed.entries[0].release_date == "2002-10-17"
    assert observed.entries[-1].release_date == (
        INITIAL_CLAIMS_LATEST_PACKAGED_RELEASE_DATE
    )
    assert observed.unindexed_release_dates == ("2019-10-17",)
    assert observed.unpublished_release_dates == (
        "2025-10-02",
        "2025-10-09",
        "2025-10-16",
        "2025-10-23",
        "2025-10-30",
        "2025-11-06",
        "2025-11-13",
    )
    assert set(observed.excluded_uris) == {
        "https://oui.doleta.gov/press/2012/010313.asp",
        "https://oui.doleta.gov/press/2012/080113.asp",
        "https://oui.doleta.gov/press/2014/031514.pdf",
    }
    corrected = next(
        item
        for item in observed.entries
        if item.artifact_uri.endswith("/2019/010318.pdf")
    )
    assert corrected.release_date == "2019-01-03"


def test_release_requests_include_canonical_and_excluded_artifacts() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_dol_initial_claims_archive_manifest()
    requests = build_dol_initial_claims_release_requests(
        registry, manifest.release_index
    )

    assert len(requests) == 1_243
    assert all(item.method is OfficialRequestMethod.GET for item in requests)
    assert {item.source_format for item in requests} == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    corrected = next(
        item for item in requests if item.uri.endswith("010318.pdf")
    )
    assert corrected.window_start == corrected.window_end == "2019-01-03"


def test_manifest_quantifies_every_release_and_archive_anomaly() -> None:
    manifest = load_packaged_dol_initial_claims_archive_manifest()

    assert packaged_dol_initial_claims_manifest_path().is_file()
    assert manifest.manifest_id == (
        "initial-claims-archive-manifest:sha256:"
        "414b9870e1f9cd1903831ce70a7628060d9ead0d06b33af29124113583e75f41"
    )
    assert len(manifest.publications) == 1_240
    assert manifest.raw_artifact_count == 1_269
    assert manifest.unique_content_sha256_count == 1_267
    assert manifest.total_content_bytes == 358_921_608
    assert manifest.html_publication_count == 599
    assert manifest.pdf_publication_count == 641
    assert manifest.qualified_triplet_count == 1_238
    assert manifest.revision_occurrence_count == 1_019
    assert manifest.noncomparable_revision_count == 2
    assert manifest.current_revision_fallback_count == 1
    assert manifest.source_prior_mismatch_count == 1
    assert len(manifest.excluded_links) == 3
    assert (
        DolInitialClaimsArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )


def test_occurrence_values_preserve_previous_as_known_semantics() -> None:
    manifest = load_packaged_dol_initial_claims_archive_manifest()
    by_date = manifest.by_release_date

    first = by_date["2002-10-17"]
    assert first.actual_value == 411_000
    assert first.revised_previous_value == 389_000
    assert first.comparison_basis is InitialClaimsComparisonBasis.UNAVAILABLE
    assert not first.revision_comparable

    second = by_date["2002-10-24"]
    assert (
        second.actual_value,
        second.previous_as_known_value,
        second.revised_previous_value,
    ) == (389_000, 411_000, 414_000)
    assert second.revision_delta == 3_000

    rebenchmark = by_date["2016-03-24"]
    assert rebenchmark.previous_as_known_value == 265_000
    assert rebenchmark.source_reported_prior_value == 260_000
    assert rebenchmark.revised_previous_value == 259_000
    assert rebenchmark.revision_delta == -6_000

    recovered = by_date["2019-10-24"]
    assert recovered.comparison_basis is (
        InitialClaimsComparisonBasis.CURRENT_REVISION_SENTENCE
    )
    assert (
        recovered.actual_value,
        recovered.previous_as_known_value,
        recovered.revised_previous_value,
    ) == (212_000, 214_000, 218_000)

    restart = by_date["2025-11-20"]
    assert restart.actual_value == 220_000
    assert restart.previous_as_known_value is None
    assert restart.revised_previous_value == 228_000
    assert not restart.revision_comparable

    post_restart = by_date["2025-11-26"]
    assert (
        post_restart.actual_value,
        post_restart.previous_as_known_value,
        post_restart.revised_previous_value,
    ) == (216_000, 220_000, 222_000)

    latest = by_date["2026-09-10"]
    assert latest.reference_week_ending == "2026-09-05"
    assert (
        latest.actual_value,
        latest.previous_as_known_value,
        latest.revised_previous_value,
    ) == (206_000, 206_000, 207_000)
    assert DolInitialClaimsPublicationV1.from_dict(latest.to_dict()) == latest


def test_coverage_qualifies_only_comparable_triplets() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_dol_initial_claims_archive_manifest()
    coverage = dol_initial_claims_coverage_from_manifest(profile, manifest)

    assert coverage.expected_occurrence_count == 1_238
    assert coverage.initial_actual_count == 1_238
    assert coverage.previous_as_known_count == 1_238
    assert coverage.exact_minute_count == 1_238
    assert coverage.revision_count == 1_019
    assert coverage.is_complete_for(profile.by_key[INITIAL_CLAIMS_PROGRAM_KEY])
    assert not coverage.blocking_gap_reasons


def test_dedicated_adapter_parses_the_retained_landing_page() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.dol.eta-unemployment-insurance")
    snapshot = _index_snapshots()[0]
    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=100
    )

    assert records
    assert records[0]["parser_id"] == INITIAL_CLAIMS_PARSER_ID
    assert any(
        item["fields"].get("uri") == "https://www.dol.gov/ui/data.pdf"
        for item in records
    )


def test_manifest_and_publication_invariants_fail_closed() -> None:
    manifest = load_packaged_dol_initial_claims_archive_manifest()
    latest = manifest.publications[-1]

    with pytest.raises(ValueError, match="lexical and numeric"):
        replace(latest, actual_value=latest.actual_value + 1, publication_id="")
    with pytest.raises(ValueError, match="qualified_triplet_count"):
        replace(
            manifest,
            qualified_triplet_count=manifest.qualified_triplet_count - 1,
            manifest_id="",
        )
