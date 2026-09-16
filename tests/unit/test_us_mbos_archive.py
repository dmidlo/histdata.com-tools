"""Complete-archive qualification tests for Philadelphia Fed MBOS."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from io import BytesIO
from zipfile import ZipFile

import pytest

from histdatacom.market_context import (
    MBOS_BULK_ARCHIVE_URI,
    MBOS_PROGRAM_KEY,
    MBOS_SOURCE_KEY,
    OfficialPhiladelphiaFedMbosParserV1,
    OfficialRawSnapshotV1,
    OfficialSourceRequestV1,
    PhiladelphiaFedMbosArchiveManifestV1,
    build_philadelphia_fed_mbos_release_requests,
    build_philadelphia_fed_mbos_support_requests,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    load_packaged_philadelphia_fed_mbos_archive_manifest,
    load_packaged_philadelphia_fed_mbos_ocr_corpus,
    load_packaged_philadelphia_fed_mbos_release_index,
    parse_official_snapshot,
    parse_philadelphia_fed_mbos_release_index,
    philadelphia_fed_mbos_coverage_from_manifest,
    resolve_official_source_parser,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def test_mbos_registry_uses_archive_capable_parser() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(MBOS_SOURCE_KEY)
    parser = resolve_official_source_parser(source)

    assert isinstance(parser, OfficialPhiladelphiaFedMbosParserV1)
    assert set(source.formats) <= set(parser.supported_formats)


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


def _legacy_zip() -> bytes:
    result = BytesIO()
    with ZipFile(result, "w") as archive:
        for year in range(1999, 2008):
            first_month = 12 if year == 1999 else 1
            for month in range(first_month, 13):
                archive.writestr(
                    f"{year}/bos{month:02d}{year % 100:02d}.pdf",
                    b"%PDF-fixture",
                )
    return result.getvalue()


def test_mbos_parser_enumerates_the_legacy_archive() -> None:
    registry = load_packaged_official_source_registry()
    parser = resolve_official_source_parser(registry.source(MBOS_SOURCE_KEY))
    request = build_philadelphia_fed_mbos_support_requests(
        registry, as_of_date="2008-01-31"
    )[1]

    records = parse_official_snapshot(
        _snapshot(request, _legacy_zip(), content_type="application/zip"),
        parser,
        max_events=100,
    )

    assert len(records) == 97
    assert records[0]["kind"] == "archive-member"
    assert records[0]["fields"]["name"] == "1999/bos1299.pdf"


def test_packaged_mbos_archive_quantifies_every_monthly_vintage() -> None:
    manifest = load_packaged_philadelphia_fed_mbos_archive_manifest()

    assert isinstance(manifest, PhiladelphiaFedMbosArchiveManifestV1)
    assert manifest.predecessor_current == 8.6
    assert manifest.predecessor_future == 29.8
    assert len(manifest.publications) == 320
    assert manifest.publications[0].reference_period == "2000-01"
    assert manifest.publications[-1].reference_period == "2026-08"
    assert manifest.raw_artifact_count == 234
    assert manifest.total_content_bytes == 114_235_398
    assert manifest.publication_pdf_bytes == 94_818_605
    assert manifest.exact_minute_count == 254
    assert manifest.inferred_bounded_count == 66
    assert manifest.forecast_occurrence_count == 320
    assert manifest.revision_record_count == 54
    assert manifest.revision_publication_count == 27
    assert manifest.ocr_publication_count == 24
    assert (
        len(
            {
                manifest.predecessor.content_sha256,
                *(
                    item.release_pdf.content_sha256
                    for item in manifest.publications
                ),
            }
        )
        == 321
    )


def test_mbos_preserves_eras_values_times_and_revisions() -> None:
    manifest = load_packaged_philadelphia_fed_mbos_archive_manifest()
    january_2000 = manifest.by_period["2000-01"]
    june_2005 = manifest.by_period["2005-06"]
    january_2010 = manifest.by_period["2010-01"]
    october_2019 = manifest.by_period["2019-10"]
    april_2021 = manifest.by_period["2021-04"]
    january_2022 = manifest.by_period["2022-01"]
    latest = manifest.by_period["2026-08"]

    assert january_2000.parser_era == "ocr-pdf-eight-column"
    assert january_2000.current_index_lexical == "9.1"
    assert january_2000.future_index_lexical == "19.7"
    assert january_2000.previous_current_lexical == "8.6"
    assert january_2000.revised_previous_current_lexical == "15.1"
    assert january_2000.revised_previous_future_lexical == "26.7"
    assert january_2000.revision_record_count == 2
    assert january_2000.release_time_local is None
    assert january_2000.released_at_ns is None
    assert january_2000.time_precision.value == "inferred-bounded"

    assert june_2005.release_time_local == "12:00"
    assert june_2005.time_precision.value == "exact-minute"
    assert june_2005.released_at_ns is not None
    assert january_2010.release_time_local == "10:00"
    assert january_2010.released_at_ns is None
    assert january_2010.time_precision.value == "inferred-bounded"
    assert october_2019.release_date == "2019-10-17"
    assert october_2019.release_time_local == "08:30"

    assert april_2021.revision_notice
    assert april_2021.current_revision_delta == -7.3
    assert april_2021.future_revision_delta == -2.5
    assert january_2022.revision_notice
    assert january_2022.revision_record_count == 0
    assert latest.current_index_lexical == "47.4"
    assert latest.future_index_lexical == "73.6"
    assert latest.release_page is not None


def test_mbos_source_profile_ocr_and_coverage_are_qualified() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    source = registry.source(MBOS_SOURCE_KEY)
    program = profile.by_key[MBOS_PROGRAM_KEY]
    manifest = load_packaged_philadelphia_fed_mbos_archive_manifest()
    ocr = load_packaged_philadelphia_fed_mbos_ocr_corpus()
    coverage = philadelphia_fed_mbos_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert ocr.corpus_id == manifest.ocr_corpus_id
    assert len(ocr.entries) == 25
    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "inferred-bounded"
    assert "archive" in {item.value for item in source.formats}
    assert program.source_key == MBOS_SOURCE_KEY
    assert program.scheduled_time_local is None
    assert program.time_precision.value == "inferred-bounded"
    assert coverage.expected_occurrence_count == 320
    assert coverage.schedule_count == 320
    assert coverage.initial_actual_count == 320
    assert coverage.previous_as_known_count == 320
    assert coverage.revision_count == 54
    assert coverage.exact_minute_count == 254
    assert coverage.forecast_count == 320
    assert len(coverage.artifact_sha256s) == 321
    assert coverage.is_complete_for(program)


def test_packaged_mbos_archive_index_is_hash_bound() -> None:
    manifest = load_packaged_philadelphia_fed_mbos_archive_manifest()
    content = load_packaged_philadelphia_fed_mbos_release_index()

    assert len(content) == 277_638
    assert sha256(content).hexdigest() == (
        manifest.release_index.archive_index.content_sha256
    )
    assert b"data-periodic-release-archive-filter" in content
    assert b"August 2026" in content


def test_mbos_release_index_joins_legacy_zip_and_live_archive() -> None:
    registry = load_packaged_official_source_registry()
    requests = build_philadelphia_fed_mbos_support_requests(
        registry, as_of_date="2008-01-31"
    )
    archive = b"""
    <script>components.push({name: 'data-periodic-release-archive-filter',
    data: {"results":[{"results":[{"attributes":{"title":"January 2008",
    "year":"2008","files":[{"url":"/-/media/FRBP/Assets/Surveys-And-Data/MBOS/2008/bos0108.pdf?sc_lang=en","icon":{"text":"PDF"}}]}}]}]}});</script>
    """
    release_index = parse_philadelphia_fed_mbos_release_index(
        _snapshot(requests[0], archive, content_type="text/html"),
        _snapshot(requests[1], _legacy_zip(), content_type="application/zip"),
        as_of_date="2008-01-31",
    )

    assert len(release_index.entries) == 98
    assert release_index.entries[0].reference_period == "1999-12"
    assert release_index.entries[0].archive_member == "1999/bos1299.pdf"
    assert release_index.entries[-1].reference_period == "2008-01"
    assert release_index.entries[-1].archive_member is None
    release_requests = build_philadelphia_fed_mbos_release_requests(
        registry, release_index
    )
    assert len(release_requests) == 1
    assert release_requests[0].source_key == MBOS_SOURCE_KEY

    with pytest.raises(ValueError, match="no modern releases"):
        parse_philadelphia_fed_mbos_release_index(
            _snapshot(requests[0], archive, content_type="text/html"),
            _snapshot(
                requests[1], _legacy_zip(), content_type="application/zip"
            ),
            as_of_date="2007-12-31",
        )


def test_mbos_manifest_fails_closed_on_tampering() -> None:
    manifest = load_packaged_philadelphia_fed_mbos_archive_manifest()
    with pytest.raises(ValueError, match="summary counts differ"):
        replace(manifest, revision_record_count=53, manifest_id="")
    with pytest.raises(ValueError, match="identity differs"):
        replace(
            manifest,
            manifest_id="mbos-archive-manifest:sha256:" + "0" * 64,
        )
    with pytest.raises(ValueError, match="member and container evidence"):
        replace(manifest.predecessor, container_sha256=None, artifact_id="")
    with pytest.raises(ValueError, match="legacy MBOS entry"):
        replace(
            manifest.release_index.entries[0],
            artifact_uri="https://www.philadelphiafed.org/wrong.pdf",
            entry_id="",
        )


def test_mbos_bulk_archive_identity_is_pinned() -> None:
    manifest = load_packaged_philadelphia_fed_mbos_archive_manifest()
    assert (
        manifest.release_index.bulk_archive.source_uri == MBOS_BULK_ARCHIVE_URI
    )
    assert manifest.release_index.bulk_archive.content_length == 38_035_521
    assert manifest.release_index.bulk_archive.content_sha256 == (
        "c2fde2f82a3c4663730142a737e72b72afb24c38c3123d604d48e5c5fbd9de90"
    )
