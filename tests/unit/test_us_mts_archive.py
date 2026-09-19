"""Tests for the retained Monthly Treasury Statement qualification."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    MTS_API_TABLE_ID,
    MTS_API_URI,
    MTS_CATALOG_URI,
    MTS_DATASET_ID,
    MTS_FIRST_REFERENCE_PERIOD,
    MTS_LATEST_PACKAGED_REFERENCE_PERIOD,
    MTS_PARSER_ID,
    MTS_PREDECESSOR_REFERENCE_PERIOD,
    MTS_PROGRAM_KEY,
    MtsTimingBasis,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    TreasuryMtsArchiveManifestV1,
    TreasuryMtsPublicationV1,
    build_treasury_mts_catalog_request,
    build_treasury_mts_report_requests,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    load_packaged_treasury_mts_archive_manifest,
    load_packaged_treasury_mts_catalog,
    load_packaged_treasury_mts_reviewed_extracts,
    packaged_treasury_mts_catalog_path,
    packaged_treasury_mts_manifest_path,
    packaged_treasury_mts_reviewed_extracts_path,
    parse_treasury_mts_release_index,
    parse_with_built_in_official_adapter,
    treasury_mts_coverage_from_manifest,
)
from histdatacom.market_context.economic_calendar import EconomicTimePrecision

AS_OF_DATE = "2026-09-16"
CAPTURED_AT_NS = 1_789_530_000_000_000_000


def _catalog_snapshot() -> OfficialRawSnapshotV1:
    registry = load_packaged_official_source_registry()
    request = build_treasury_mts_catalog_request(
        registry, as_of_date=AS_OF_DATE
    )
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=CAPTURED_AT_NS,
        completed_at_ns=CAPTURED_AT_NS + 1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "application/json"},
        content=load_packaged_treasury_mts_catalog(),
        content_type="application/json",
    )


def test_registry_profile_and_requests_are_exact() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    source = registry.source("us.treasury.fiscal-data")
    program = profile.by_key[MTS_PROGRAM_KEY]

    assert source.parser_id == MTS_PARSER_ID
    assert source.verification_status.value == "empirically-verified"
    assert source.source_series_ids == (MTS_DATASET_ID,)
    assert source.source_table_ids == (MTS_API_TABLE_ID,)
    assert source.endpoint_uri_template == MTS_API_URI
    assert set(source.formats) == {
        OfficialSourceFormat.JSON,
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert program.source_key == source.source_key
    assert program.time_precision is EconomicTimePrecision.DATE_ONLY

    catalog = build_treasury_mts_catalog_request(
        registry, as_of_date=AS_OF_DATE
    )
    assert catalog.method is OfficialRequestMethod.GET
    assert catalog.uri == MTS_CATALOG_URI
    index = load_packaged_treasury_mts_archive_manifest().release_index
    reports = build_treasury_mts_report_requests(registry, index)
    assert len(reports) == 321
    assert reports[0].window_start == "1999-12-01"
    assert reports[-1].window_start == "2026-08-01"


def test_packaged_catalog_replays_the_selected_continuous_index() -> None:
    manifest = load_packaged_treasury_mts_archive_manifest()
    observed = parse_treasury_mts_release_index(
        _catalog_snapshot(), as_of_date=AS_OF_DATE
    )

    assert packaged_treasury_mts_catalog_path().is_file()
    assert observed == manifest.release_index
    assert len(observed.entries) == 321
    assert observed.entries[0].reference_period == (
        MTS_PREDECESSOR_REFERENCE_PERIOD
    )
    assert observed.entries[1].reference_period == MTS_FIRST_REFERENCE_PERIOD
    assert observed.entries[-1].reference_period == (
        MTS_LATEST_PACKAGED_REFERENCE_PERIOD
    )


def test_manifest_quantifies_the_complete_retained_corpus() -> None:
    manifest = load_packaged_treasury_mts_archive_manifest()
    reviewed = load_packaged_treasury_mts_reviewed_extracts()

    assert packaged_treasury_mts_manifest_path().is_file()
    assert packaged_treasury_mts_reviewed_extracts_path().is_file()
    assert manifest.manifest_id == (
        "mts-archive-manifest:sha256:"
        "358bf797c0b67adbcc1d6c1deadda5ebd24ca31b734d164a110c259e4bac3a37"
    )
    assert reviewed.corpus_id == manifest.reviewed_extracts_id
    assert len(reviewed.timings) == 320
    assert len(reviewed.value_overrides) == 4
    assert len(manifest.publications) == 320
    assert len(manifest.year_end_artifacts) == 25
    assert manifest.raw_artifact_count == 347
    assert manifest.unique_content_sha256_count == 347
    assert manifest.total_content_bytes == 417_602_025
    assert manifest.exact_minute_count == 288
    assert manifest.inferred_bounded_count == 6
    assert manifest.date_only_count == 26
    assert manifest.revision_occurrence_count == 88
    assert manifest.reviewed_value_count == 4
    assert TreasuryMtsArchiveManifestV1.from_json(manifest.to_json()) == (
        manifest
    )


def test_vintage_triplets_preserve_budget_balance_semantics() -> None:
    manifest = load_packaged_treasury_mts_archive_manifest()
    by_period = manifest.by_period

    first = by_period["2000-01"]
    assert (
        first.actual_balance,
        first.previous_as_known,
        first.revised_previous,
    ) == (62_152, 33_081, 33_081)
    assert first.actual_balance_lexical == "+62,152"

    financial_crisis = by_period["2009-09"]
    assert (
        financial_crisis.previous_as_known,
        financial_crisis.revised_previous,
        financial_crisis.revision_delta,
    ) == (-111_403, -103_555, 7_848)

    pandemic = by_period["2020-06"]
    assert pandemic.actual_balance == -864_074
    assert pandemic.actual_receipts == 240_829
    assert pandemic.actual_outlays == 1_104_903

    reviewed = by_period["2024-04"]
    assert reviewed.value_basis == "reviewed-ocr-table-1"
    assert (
        reviewed.actual_balance,
        reviewed.previous_as_known,
        reviewed.revised_previous,
    ) == (209_529, -236_457, -236_556)

    latest = by_period["2026-08"]
    assert (
        latest.actual_balance,
        latest.previous_as_known,
        latest.revised_previous,
    ) == (-166_797, -432_308, -432_286)
    assert TreasuryMtsPublicationV1.from_dict(latest.to_dict()) == latest


def test_release_timing_keeps_source_anomalies_explicit() -> None:
    by_period = load_packaged_treasury_mts_archive_manifest().by_period

    early_annual = by_period["2001-12"]
    assert early_annual.release_date == "2002-01-21"
    assert early_annual.release_time_local is None
    assert early_annual.time_precision is (
        EconomicTimePrecision.INFERRED_BOUNDED
    )
    assert early_annual.timing_basis is MtsTimingBasis.ANNUAL_TABLE

    corrected = by_period["2007-01"]
    assert corrected.release_date == "2007-02-12"
    assert corrected.release_time_local == "14:00"
    assert corrected.timing_basis is (MtsTimingBasis.REVIEWED_SOURCE_CORRECTION)

    annual_fallback = by_period["2023-05"]
    assert annual_fallback.release_date == "2023-06-12"
    assert annual_fallback.timing_basis is MtsTimingBasis.ANNUAL_TABLE

    year_end = by_period["2025-09"]
    assert year_end.release_date == "2025-10-16"
    assert year_end.time_precision is EconomicTimePrecision.DATE_ONLY
    assert year_end.released_at_ns is None
    assert year_end.timing_basis is (MtsTimingBasis.RESPONSE_LAST_MODIFIED)


def test_coverage_completes_the_final_us_program() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_treasury_mts_archive_manifest()
    coverage = treasury_mts_coverage_from_manifest(profile, manifest)

    assert coverage.expected_occurrence_count == 320
    assert coverage.schedule_count == 320
    assert coverage.initial_actual_count == 320
    assert coverage.previous_as_known_count == 320
    assert coverage.revision_count == 88
    assert coverage.exact_minute_count == 288
    assert coverage.is_complete_for(profile.by_key[MTS_PROGRAM_KEY])
    assert not coverage.blocking_gap_reasons


def test_dedicated_adapter_parses_the_catalog_receipt() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.treasury.fiscal-data")
    records = parse_with_built_in_official_adapter(
        _catalog_snapshot(), source, max_events=10_000
    )

    assert records
    assert records[0]["parser_id"] == MTS_PARSER_ID


def test_manifest_and_publication_invariants_fail_closed() -> None:
    manifest = load_packaged_treasury_mts_archive_manifest()
    latest = manifest.publications[-1]

    with pytest.raises(ValueError, match="lexical value differs"):
        replace(
            latest,
            actual_balance_lexical="+1",
            publication_id="",
        )
    with pytest.raises(ValueError, match="revision count"):
        replace(
            manifest,
            revision_occurrence_count=(manifest.revision_occurrence_count - 1),
            manifest_id="",
        )
