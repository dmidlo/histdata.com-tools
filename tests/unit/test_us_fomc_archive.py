"""Complete-archive qualification tests for the Federal Reserve FOMC."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    FOMC_DECISION_PROGRAM_KEY,
    FOMC_MINUTES_PROGRAM_KEY,
    FOMC_SEP_PROGRAM_KEY,
    FOMC_SOURCE_KEY,
    EconomicEventFamily,
    FederalReserveFomcArchiveManifestV1,
    OfficialFederalReserveFomcParserV1,
    OfficialRawSnapshotV1,
    OfficialSourceRequestV1,
    build_federal_reserve_fomc_document_requests,
    build_federal_reserve_fomc_index_requests,
    built_in_united_states_backfill_profile,
    federal_reserve_fomc_coverages_from_manifest,
    load_packaged_federal_reserve_fomc_archive_manifest,
    load_packaged_federal_reserve_fomc_index_pages,
    load_packaged_official_source_registry,
    parse_federal_reserve_fomc_release_index,
    parse_federal_reserve_fomc_statement,
    resolve_official_source_parser,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    *,
    content_type: str = "text/html",
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


def test_fomc_source_parser_profile_and_h6_identity_are_separate() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    source = registry.source(FOMC_SOURCE_KEY)
    money_stock = registry.source("us.frb.money-stock")
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()

    assert source.source_release_ids == (
        "FOMC minutes",
        "FOMC statement",
        "Summary of Economic Projections",
    )
    assert source.event_families == (EconomicEventFamily.MONETARY_POLICY,)
    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "inferred-bounded"
    assert isinstance(
        resolve_official_source_parser(source),
        OfficialFederalReserveFomcParserV1,
    )
    assert money_stock.verification_status.value == "empirically-verified"
    assert money_stock.source_release_ids == ("H.6 Money Stock Measures",)
    assert profile.by_key["us.frb.money-stock"].source_key == (
        "us.frb.money-stock"
    )
    assert manifest.registry_id == registry.registry_id
    assert manifest.profile_id == profile.profile_id


def test_packaged_fomc_archive_quantifies_all_three_programs() -> None:
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()

    assert isinstance(manifest, FederalReserveFomcArchiveManifestV1)
    assert manifest.release_index.as_of_date == "2026-09-15"
    assert len(manifest.release_index.index_artifacts) == 22
    assert len(manifest.release_index.entries) == 249
    assert len(manifest.decisions) == 225
    assert len(manifest.minutes) == 213
    assert len(manifest.projections) == 75
    assert manifest.raw_artifact_count == 521
    assert manifest.total_content_bytes == 49_272_968
    assert manifest.exact_minute_decision_count == 90
    assert manifest.unscheduled_decision_count == 13
    assert manifest.decisions[0].release_date == "2000-02-02"
    assert manifest.decisions[-1].release_date == "2026-07-29"


def test_fomc_decisions_preserve_setting_lineage_and_native_shapes() -> None:
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()
    first = manifest.decisions[0]
    transition = next(
        item for item in manifest.decisions if item.release_date == "2008-12-16"
    )

    assert first.previous_setting.scalar_value == 5.5
    assert first.new_setting.scalar_value == 5.75
    assert first.direction.value == "tighten"
    assert transition.previous_setting.kind.value == "scalar-rate"
    assert transition.previous_setting.scalar_value == 1.0
    assert transition.new_setting.kind.value == "target-range"
    assert transition.new_setting.lower_bound == 0.0
    assert transition.new_setting.upper_bound == 0.25
    assert all(
        current.previous_setting.semantic_id == previous.new_setting.semantic_id
        for previous, current in zip(
            manifest.decisions[:-1], manifest.decisions[1:], strict=True
        )
    )
    assert Counter(item.direction.value for item in manifest.decisions) == {
        "hold": 148,
        "tighten": 40,
        "ease": 34,
        "guidance-only": 1,
        "operational": 2,
    }
    assert Counter(
        item.new_setting.kind.value for item in manifest.decisions
    ) == {
        "scalar-rate": 79,
        "target-range": 146,
    }
    assert [
        item.release_date
        for item in manifest.decisions
        if item.direction.value == "guidance-only"
    ] == ["2007-08-17"]
    assert [
        item.release_date
        for item in manifest.decisions
        if item.direction.value == "operational"
    ] == ["2008-03-11", "2010-05-09"]
    assert not any(
        item.release_date == "2025-08-22" for item in manifest.decisions
    )


def test_fomc_minutes_and_sep_retain_publication_eras() -> None:
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()
    first_sep = manifest.projections[0]
    last_sep = manifest.projections[-1]

    assert manifest.minutes[0].meeting_end_date == "2000-02-02"
    assert manifest.minutes[0].release_date == "2000-03-23"
    assert manifest.minutes[-1].meeting_end_date == "2026-07-29"
    assert manifest.minutes[-1].release_date == "2026-08-19"
    assert max(len(item.meeting_keys) for item in manifest.minutes) == 5
    assert first_sep.meeting_end_date == "2007-10-31"
    assert first_sep.release_date == "2007-11-20"
    assert first_sep.artifact.source_uri.endswith("fomcminutes20071031.htm")
    assert last_sep.meeting_end_date == "2026-06-17"
    assert last_sep.release_date == "2026-06-17"
    assert last_sep.artifact.source_uri.endswith("fomcprojtabl20260617.htm")
    assert (
        sum(
            item.release_date > item.meeting_end_date
            for item in manifest.projections
        )
        == 14
    )
    assert Counter(
        (
            (
                "delayed"
                if item.release_date > item.meeting_end_date
                else "same-day"
            ),
            item.artifact.media_type,
        )
        for item in manifest.projections
    ) == {
        ("delayed", "text/html"): 13,
        ("delayed", "application/pdf"): 1,
        ("same-day", "application/pdf"): 39,
        ("same-day", "text/html"): 22,
    }


def test_packaged_fomc_index_pages_are_hash_bound_and_reparse_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()
    requests = build_federal_reserve_fomc_index_requests(
        registry, as_of_date=manifest.release_index.as_of_date
    )
    pages = load_packaged_federal_reserve_fomc_index_pages()

    assert len(requests) == len(pages) == 22
    assert set(pages) == {item.uri for item in requests}
    assert sum(len(content) for content in pages.values()) == 2_172_157
    rebuilt = parse_federal_reserve_fomc_release_index(
        tuple(_snapshot(request, pages[request.uri]) for request in requests),
        as_of_date=manifest.release_index.as_of_date,
    )
    assert rebuilt == manifest.release_index
    documents = build_federal_reserve_fomc_document_requests(registry, rebuilt)
    assert len(documents) == 499
    assert Counter(item.source_format.value for item in documents) == {
        "html": 459,
        "pdf": 40,
    }
    assert {item.parser_id for item in documents} == {
        "official.federal-reserve-fomc.v1"
    }

    bounded_requests = build_federal_reserve_fomc_index_requests(
        registry, as_of_date="2026-07-29"
    )
    bounded = parse_federal_reserve_fomc_release_index(
        tuple(
            _snapshot(request, pages[request.uri])
            for request in bounded_requests
        ),
        as_of_date="2026-07-29",
    )
    july = bounded.by_meeting_key["fomc.2026-07-29.july-28-29"]
    assert july.statement_uri is not None
    assert july.minutes_uri is None
    assert july.minutes_release_date is None
    assert all(
        item.minutes_release_date is None
        or item.minutes_release_date <= bounded.as_of_date
        for item in bounded.entries
    )


def test_fomc_statement_parser_handles_fraction_and_authored_clock() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()
    entry = manifest.release_index.by_meeting_key[
        "fomc.2000-02-02.february-1-2-meeting-2000"
    ]
    request = next(
        item
        for item in build_federal_reserve_fomc_document_requests(
            registry, manifest.release_index
        )
        if item.uri == entry.statement_uri
    )
    content = (
        b"<html><body>Federal Reserve FOMC. For release at 2:15 p.m. EDT. "
        b"The Committee voted to raise its target for the federal funds "
        b"rate by 25 basis points to 5-3/4 percent.</body></html>"
    )

    decision = parse_federal_reserve_fomc_statement(
        _snapshot(request, content), entry, previous_setting=None
    )

    assert decision.release_date == "2000-02-02"
    assert decision.release_time_local == "14:15"
    assert decision.time_zone_abbreviation == "EDT"
    assert decision.previous_setting.scalar_value == 5.5
    assert decision.new_setting.scalar_value == 5.75
    assert decision.direction.value == "tighten"

    with pytest.raises(ValueError, match="no supported target setting"):
        parse_federal_reserve_fomc_statement(
            _snapshot(
                request,
                b"<html><body>Federal Reserve FOMC operational "
                b"announcement without a rate setting.</body></html>",
            ),
            entry,
            previous_setting=decision.new_setting,
        )


def test_fomc_coverages_close_each_program_without_consensus_substitution() -> (
    None
):
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()
    coverages = {
        item.program_key: item
        for item in federal_reserve_fomc_coverages_from_manifest(manifest)
    }

    assert set(coverages) == {
        FOMC_DECISION_PROGRAM_KEY,
        FOMC_MINUTES_PROGRAM_KEY,
        FOMC_SEP_PROGRAM_KEY,
    }
    decision = coverages[FOMC_DECISION_PROGRAM_KEY]
    assert decision.expected_occurrence_count == 225
    assert decision.previous_as_known_count == 225
    assert decision.exact_minute_count == 90
    assert decision.forecast_count == 0
    assert [item.value for item in decision.gap_reasons] == [
        "no-event-forecast"
    ]
    assert coverages[FOMC_MINUTES_PROGRAM_KEY].expected_occurrence_count == 213
    assert coverages[FOMC_SEP_PROGRAM_KEY].forecast_count == 75
    assert all(
        coverage.is_complete_for(profile.by_key[program_key])
        for program_key, coverage in coverages.items()
    )


def test_fomc_manifest_fails_closed_on_tampering() -> None:
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()

    with pytest.raises(ValueError, match="summary counts differ"):
        replace(manifest, raw_artifact_count=520, manifest_id="")
    with pytest.raises(ValueError, match="identity differs"):
        replace(
            manifest,
            manifest_id="fomc-archive-manifest:sha256:" + "0" * 64,
        )
    with pytest.raises(ValueError, match="identity differs"):
        replace(
            manifest.decisions[0].statement,
            content_sha256="0" * 64,
        )
    with pytest.raises(ValueError, match="decision inventory differs"):
        replace(
            manifest,
            decisions=manifest.decisions[:-1],
            manifest_id="",
        )
