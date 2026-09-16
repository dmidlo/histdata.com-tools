"""Complete-archive qualification tests for Philadelphia Fed SPF."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

from histdatacom.market_context import (
    SPF_PROGRAM_KEY,
    SPF_SHUTDOWN_DELAYED_PERIODS,
    SPF_SOURCE_KEY,
    OfficialPhiladelphiaFedSpfParserV1,
    OfficialRawSnapshotV1,
    OfficialSourceRequestV1,
    PhiladelphiaFedSpfArchiveManifestV1,
    build_philadelphia_fed_spf_release_requests,
    build_philadelphia_fed_spf_support_requests,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    load_packaged_philadelphia_fed_spf_archive_manifest,
    load_packaged_philadelphia_fed_spf_release_dates,
    parse_philadelphia_fed_spf_release_index,
    philadelphia_fed_spf_coverage_from_manifest,
    resolve_official_source_parser,
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


def test_packaged_spf_archive_quantifies_every_quarterly_vintage() -> None:
    manifest = load_packaged_philadelphia_fed_spf_archive_manifest()

    assert isinstance(manifest, PhiladelphiaFedSpfArchiveManifestV1)
    assert len(manifest.publications) == 107
    assert manifest.publications[0].survey_period == "2000-Q1"
    assert manifest.publications[0].true_deadline_date == "2000-02-12"
    assert manifest.publications[0].release_date == "2000-02-22"
    assert manifest.publications[-1].survey_period == "2026-Q3"
    assert manifest.publications[-1].true_deadline_date == "2026-08-11"
    assert manifest.publications[-1].release_date == "2026-08-14"
    assert manifest.raw_artifact_count == 113
    assert manifest.total_content_bytes == 93_449_761
    assert manifest.forecast_occurrence_count == 428
    assert manifest.shutdown_delayed_count == 4
    assert manifest.date_only_count == 107
    assert (
        tuple(
            item.survey_period
            for item in manifest.publications
            if item.shutdown_delayed
        )
        == SPF_SHUTDOWN_DELAYED_PERIODS
    )
    assert (
        len({item.release_pdf.content_sha256 for item in manifest.publications})
        == 107
    )


def test_packaged_spf_values_keep_workbook_coordinates_and_precision() -> None:
    manifest = load_packaged_philadelphia_fed_spf_archive_manifest()
    first = manifest.by_period["2000-Q1"].forecasts
    latest = manifest.by_period["2026-Q3"].forecasts

    assert [(item.worksheet, item.column, item.value) for item in first] == [
        ("RGDP", "drgdp2", 3.1608),
        ("UNEMP", "UNEMP2", 4.0),
        ("CPI", "CPI2", 2.5),
        ("TBILL", "TBILL2", 5.5),
    ]
    assert [item.value for item in latest] == [2.4624, 4.2, 2.2758, 3.73]
    assert [item.lexical_value for item in latest] == [
        "2.4624",
        "4.2",
        "2.2758",
        "3.73",
    ]


def test_spf_source_profile_parser_and_coverage_are_qualified() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    source = registry.source(SPF_SOURCE_KEY)
    program = profile.by_key[SPF_PROGRAM_KEY]
    manifest = load_packaged_philadelphia_fed_spf_archive_manifest()
    coverage = philadelphia_fed_spf_coverage_from_manifest(manifest)

    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id
    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "date-only"
    assert isinstance(
        resolve_official_source_parser(source),
        OfficialPhiladelphiaFedSpfParserV1,
    )
    assert program.source_key == SPF_SOURCE_KEY
    assert not program.requires_previous_as_known
    assert not program.requires_revision_history
    assert [item.value for item in program.release_stages] == ["initial"]
    assert coverage.expected_occurrence_count == 107
    assert coverage.schedule_count == 107
    assert coverage.initial_actual_count == 107
    assert coverage.previous_as_known_count == 0
    assert coverage.revision_count == 0
    assert coverage.exact_minute_count == 0
    assert coverage.forecast_count == 428
    assert len(coverage.artifact_sha256s) == 113
    assert coverage.is_complete_for(program)


def test_packaged_release_ledger_is_hash_bound() -> None:
    manifest = load_packaged_philadelphia_fed_spf_archive_manifest()
    content = load_packaged_philadelphia_fed_spf_release_dates()

    assert len(content) == 8_298
    assert sha256(content).hexdigest() == (
        manifest.release_index.release_date_ledger.content_sha256
    )
    assert b"2026 Q1" in content
    assert b"3/6/26" in content


def test_spf_release_index_parser_joins_archive_and_ledger() -> None:
    registry = load_packaged_official_source_registry()
    requests = build_philadelphia_fed_spf_support_requests(
        registry, as_of_date="2000-05-22"
    )
    archive = b"""
    <script>data: {"results":[
    {"attributes":{"year":"2000","url":"/-/media/FRBP/Assets/Surveys-And-Data/survey-of-professional-forecasters/2000/spfq200.pdf?sc_lang=en","title":"Second Quarter 2000 Survey of Professional Forecasters","pdf":true}},
    {"attributes":{"year":"2000","url":"/-/media/FRBP/Assets/Surveys-And-Data/survey-of-professional-forecasters/2000/spfq100.pdf?sc_lang=en","title":"First Quarter 2000 Survey of Professional Forecasters","pdf":true}}
    ]}</script>
    """
    ledger = b"""Deadline and Release Dates for the Survey of Professional Forecasters
Survey         True Deadline Date       News Release Date
2000 Q1             2/12/00             2/22/00
     Q2             5/13/00             5/22/00
"""
    index = parse_philadelphia_fed_spf_release_index(
        _snapshot(requests[0], archive, content_type="text/html"),
        _snapshot(requests[2], ledger, content_type="text/plain"),
        as_of_date="2000-05-22",
    )

    assert [item.survey_period for item in index.entries] == [
        "2000-Q1",
        "2000-Q2",
    ]
    assert index.entries[0].release_pdf_uri.endswith("/2000/spfq100.pdf")
    assert index.entries[1].release_date == "2000-05-22"
    release_requests = build_philadelphia_fed_spf_release_requests(
        registry, index
    )
    assert len(release_requests) == 2
    assert all(item.source_key == SPF_SOURCE_KEY for item in release_requests)

    with pytest.raises(ValueError, match="no released surveys"):
        parse_philadelphia_fed_spf_release_index(
            _snapshot(requests[0], archive, content_type="text/html"),
            _snapshot(requests[2], ledger, content_type="text/plain"),
            as_of_date="1999-12-31",
        )


def test_spf_manifest_fails_closed_on_tampering() -> None:
    manifest = load_packaged_philadelphia_fed_spf_archive_manifest()
    with pytest.raises(ValueError, match="summary counts differ"):
        replace(manifest, forecast_occurrence_count=427, manifest_id="")
    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, manifest_id="spf-archive-manifest:sha256:" + "0" * 64)
    with pytest.raises(ValueError, match="support-artifact URI differs"):
        replace(
            manifest.support_artifacts[0],
            source_uri="https://www.philadelphiafed.org/wrong",
            artifact_id="",
        )
