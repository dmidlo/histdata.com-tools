"""Real-archive qualification tests for the United States G.17 program."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

from histdatacom.market_context import (
    FEDERAL_RESERVE_G17_INDEX_URI,
    G17_2002_12_17_NORMALIZED_SHA256,
    FederalReserveG17ArchiveManifestV1,
    OfficialRawSnapshotV1,
    OfficialSourceRequestV1,
    UnitedStatesBackfillProfileV1,
    build_federal_reserve_g17_archive_manifest,
    build_federal_reserve_g17_archive_request,
    build_federal_reserve_g17_archive_requests,
    build_federal_reserve_g17_index_request,
    built_in_united_states_backfill_profile,
    federal_reserve_g17_coverage_from_manifest,
    load_packaged_federal_reserve_g17_2002,
    load_packaged_federal_reserve_g17_archive_manifest,
    load_packaged_federal_reserve_g17_index,
    load_packaged_official_source_registry,
    parse_federal_reserve_g17_release,
    parse_federal_reserve_g17_release_index,
    replay_federal_reserve_g17_archive,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    *,
    content_type: str,
) -> OfficialRawSnapshotV1:
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


def _profile() -> UnitedStatesBackfillProfileV1:
    registry = load_packaged_official_source_registry()
    return built_in_united_states_backfill_profile(registry)


def _g17_fragment(
    *,
    release_date: str,
    zone: str,
    title: str,
    month_header: str,
    previous: str,
    encoding: str = "utf-8",
) -> bytes:
    text = f"""FEDERAL RESERVE STATISTICAL RELEASE
G.17 (419) For release at 9:15 a.m. ({zone})
{release_date}
{title}
Seasonally adjusted
Industrial Production | {month_header} | {month_header}
Total index | 100.0 100.1 100.2 100.3 | -.2 .1 -.4 .5 | 1.0
Previous estimates | 100.0 100.1 100.2 | -.2 .1 {previous} |
Narrative encoding marker: –
"""
    return text.encode(encoding)


def test_packaged_archive_quantifies_every_real_release() -> None:
    source = load_packaged_official_source_registry().source(
        "us.frb.industrial-production"
    )
    manifest = load_packaged_federal_reserve_g17_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "exact-minute"
    assert source.source_release_ids == ("G.17",)
    assert isinstance(manifest, FederalReserveG17ArchiveManifestV1)
    assert len(manifest.entries) == 319
    assert manifest.entries[0].archive_date == "2000-01-14"
    assert manifest.entries[0].reference_period == "1999-12"
    assert manifest.entries[-1].archive_date == "2026-08-18"
    assert manifest.entries[-1].reference_period == "2026-07"
    assert manifest.total_content_bytes == 49_791_597
    assert manifest.revision_occurrence_count == 261
    assert len({item.content_sha256 for item in manifest.entries}) == 319
    assert len({item.normalized_sha256 for item in manifest.entries}) == 319
    assert manifest.release_index.unavailable_schedule_months == (
        "2025-10",
        "2025-11",
    )
    assert manifest.release_index.future_release_dates[0] == "2026-09-18"
    assert manifest.release_index.future_release_dates[-1] == "2027-12-16"

    mismatches = tuple(
        item
        for item in manifest.entries
        if item.archive_date != item.release_date
    )
    assert [(item.archive_date, item.release_date) for item in mismatches] == [
        ("2000-02-15", "2000-02-16")
    ]


def test_retained_release_index_replays_to_packaged_inventory() -> None:
    registry = load_packaged_official_source_registry()
    request = build_federal_reserve_g17_index_request(
        registry, as_of_date="2026-09-14"
    )
    content = load_packaged_federal_reserve_g17_index()
    snapshot = _snapshot(request, content, content_type="text/html")
    observed = parse_federal_reserve_g17_release_index(
        snapshot, as_of_date="2026-09-14"
    )
    manifest = load_packaged_federal_reserve_g17_archive_manifest()

    assert request.uri == FEDERAL_RESERVE_G17_INDEX_URI
    assert observed == manifest.release_index
    assert observed.content_sha256 == sha256(content).hexdigest()
    archive_requests = build_federal_reserve_g17_archive_requests(
        registry, observed
    )
    assert len(archive_requests) == 319
    assert archive_requests[0].uri.endswith("/20000114/g17.txt")
    assert archive_requests[-1].uri.endswith("/20260818/g17.txt")


def test_packaged_archive_derives_a_complete_g17_coverage_slice() -> None:
    profile = _profile()
    manifest = load_packaged_federal_reserve_g17_archive_manifest()
    coverage = federal_reserve_g17_coverage_from_manifest(
        profile, manifest, window_end_date="2026-09-14"
    )

    assert coverage.expected_occurrence_count == 319
    assert coverage.schedule_count == 319
    assert coverage.initial_actual_count == 319
    assert coverage.previous_as_known_count == 319
    assert coverage.revision_count == 261
    assert coverage.exact_minute_count == 319
    assert len(coverage.artifact_sha256s) == 319
    assert coverage.is_complete_for(profile.by_key[coverage.program_key])


def test_retained_2002_release_matches_the_complete_manifest_entry() -> None:
    registry = load_packaged_official_source_registry()
    request = build_federal_reserve_g17_archive_request(registry, "2002-12-17")
    content = load_packaged_federal_reserve_g17_2002()
    snapshot = _snapshot(request, content, content_type="text/plain")
    triplet = parse_federal_reserve_g17_release(snapshot)
    expected = (
        load_packaged_federal_reserve_g17_archive_manifest().by_archive_date[
            "2002-12-17"
        ]
    )

    assert expected.content_sha256 == sha256(content).hexdigest()
    assert expected.normalized_sha256 == G17_2002_12_17_NORMALIZED_SHA256
    assert expected.actual_value == triplet.actual_value
    assert expected.previous_as_known_value == triplet.previous_as_known_value
    assert expected.revised_previous_value == triplet.revised_previous_value


def test_one_release_manifest_build_and_replay_are_exact() -> None:
    profile = _profile()
    packaged = load_packaged_federal_reserve_g17_archive_manifest()
    index = replace(
        packaged.release_index,
        published_release_dates=("2002-12-17",),
        future_release_dates=(),
        unavailable_schedule_months=(),
        index_id="",
    )
    registry = load_packaged_official_source_registry()
    request = build_federal_reserve_g17_archive_request(registry, "2002-12-17")
    snapshot = _snapshot(
        request,
        load_packaged_federal_reserve_g17_2002(),
        content_type="text/plain",
    )
    manifest = build_federal_reserve_g17_archive_manifest(
        profile, index, (snapshot,)
    )

    assert len(manifest.entries) == 1
    assert replay_federal_reserve_g17_archive(manifest, (snapshot,))[
        0
    ].triplet_id
    with pytest.raises(ValueError, match="differ from the manifest"):
        replay_federal_reserve_g17_archive(manifest, ())


@pytest.mark.parametrize(
    (
        "archive_date",
        "release_date",
        "zone",
        "title",
        "months",
        "previous",
        "encoding",
    ),
    (
        (
            "2000-01-14",
            "January 14, 2000",
            "EST",
            "INDUSTRIAL PRODUCTION AND CAPACITY UTILIZATION:  SUMMARY",
            "Sept. Oct. Nov. Dec.",
            "-.4",
            "utf-8",
        ),
        (
            "2011-06-15",
            "June 15, 2011",
            "EDT",
            "Industrial Production and Capacity Utilization:  Summary",
            "Dec.[r] Jan.[r] Feb.[r] Mar.[r] Apr.[r] May[p]",
            "-.4",
            "utf-8",
        ),
        (
            "2005-11-17",
            "November 17, 2005",
            "EST",
            "INDUSTRIAL PRODUCTION AND CAPACITY UTILIZATION:  SUMMARY",
            "July Aug. Sept. Oct.",
            "-.3",
            "windows-1252",
        ),
        (
            "2026-06-15",
            "June 15, 2026",
            "AM",
            "Industrial Production and Capacity Utilization:  Summary",
            "Dec.[r] Jan.[r] Feb.[r] Mar.[r] Apr.[r] May[p]",
            "-.3",
            "utf-8",
        ),
    ),
)
def test_parser_handles_every_observed_historical_format_era(
    archive_date: str,
    release_date: str,
    zone: str,
    title: str,
    months: str,
    previous: str,
    encoding: str,
) -> None:
    registry = load_packaged_official_source_registry()
    request = build_federal_reserve_g17_archive_request(registry, archive_date)
    content = _g17_fragment(
        release_date=release_date,
        zone=zone,
        title=title,
        month_header=months,
        previous=previous,
        encoding=encoding,
    )
    triplet = parse_federal_reserve_g17_release(
        _snapshot(request, content, content_type="text/plain")
    )

    assert triplet.actual_value == 0.5
    assert triplet.previous_as_known_value == float(previous)
    assert triplet.revised_previous_value == -0.4
    if encoding == "windows-1252":
        assert any("windows-1252" in item for item in triplet.limitations)
    if zone in {"AM", "PM"}:
        assert any(
            "not a timezone abbreviation" in item
            for item in triplet.limitations
        )


def test_archive_contracts_reject_tampering() -> None:
    manifest = load_packaged_federal_reserve_g17_archive_manifest()

    with pytest.raises(ValueError, match="differs from actual_value"):
        replace(
            manifest.entries[0],
            actual_lexical=".9",
            entry_id="",
        )
    with pytest.raises(ValueError, match="total byte count differs"):
        replace(manifest, total_content_bytes=manifest.total_content_bytes + 1)
    with pytest.raises(ValueError, match="identity differs"):
        FederalReserveG17ArchiveManifestV1.from_dict(
            {
                **manifest.to_dict(),
                "profile_id": manifest.profile_id + "-tampered",
            }
        )
