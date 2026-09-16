"""Tests for the first-party United States macro-calendar backfill."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    G17_2002_12_17_NORMALIZED_SHA256,
    G17_2002_12_17_SHA256,
    G17_2002_12_17_URI,
    US_BACKFILL_START_DATE,
    EconomicEventFamily,
    EconomicReleaseStage,
    EconomicTimePrecision,
    OfficialAdapterRecordKind,
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    UnitedStatesBackfillAuditV1,
    UnitedStatesBackfillProfileV1,
    UnitedStatesCoverageGapReason,
    UnitedStatesForecastStrategy,
    UnitedStatesProgramCoverageV1,
    UnitedStatesReleaseTripletV1,
    audit_united_states_backfill,
    build_federal_reserve_g17_archive_request,
    built_in_united_states_backfill_profile,
    economic_calendar_release_from_g17_triplet,
    federal_reserve_g17_2002_artifact_lock,
    load_packaged_federal_reserve_g17_2002,
    load_packaged_official_source_registry,
    packaged_federal_reserve_g17_2002_path,
    parse_federal_reserve_g17_release,
    parse_with_built_in_official_adapter,
)

RETRIEVED_AT_NS = 1_789_400_000_000_000_000


def _snapshot(content: bytes | None = None) -> OfficialRawSnapshotV1:
    registry = load_packaged_official_source_registry()
    request = build_federal_reserve_g17_archive_request(registry, "2002-12-17")
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=RETRIEVED_AT_NS,
        completed_at_ns=RETRIEVED_AT_NS + 1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "text/plain"},
        content=(
            load_packaged_federal_reserve_g17_2002()
            if content is None
            else content
        ),
        content_type="text/plain",
    )


def _coverage(
    profile: UnitedStatesBackfillProfileV1,
    program_key: str,
) -> UnitedStatesProgramCoverageV1:
    program = profile.by_key[program_key]
    gaps = []
    if program.forecast_strategy is UnitedStatesForecastStrategy.UNAVAILABLE:
        gaps.append(UnitedStatesCoverageGapReason.NO_EVENT_FORECAST)
    if program.archive_start_date > US_BACKFILL_START_DATE:
        gaps.append(UnitedStatesCoverageGapReason.PROGRAM_NOT_YET_PUBLISHED)
    return UnitedStatesProgramCoverageV1(
        program_key=program_key,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date="2026-09-14",
        expected_occurrence_count=10,
        schedule_count=10,
        initial_actual_count=10,
        previous_as_known_count=(
            10 if program.requires_previous_as_known else 0
        ),
        revision_count=1 if program.requires_revision_history else 0,
        exact_minute_count=(
            10
            if program.time_precision
            in {
                EconomicTimePrecision.EXACT_MINUTE,
                EconomicTimePrecision.EXACT_SECOND,
            }
            else 0
        ),
        forecast_count=(
            0
            if program.forecast_strategy
            is UnitedStatesForecastStrategy.UNAVAILABLE
            else 1
        ),
        artifact_sha256s=tuple(
            hashlib.sha256(f"{program.program_id}:{index}".encode()).hexdigest()
            for index in range(
                1
                if program.archive_strategy.value == "official-vintage-table"
                else 10
            )
        ),
        gap_reasons=tuple(gaps),
        notes=("Fixture counts exercise the fail-closed coverage audit.",),
    )


def test_registry_represents_us_specific_legal_producers_and_text() -> None:
    registry = load_packaged_official_source_registry()
    assert registry.reviewed_on == "2026-09-16"
    assert len(registry.sources) == 69

    dol = registry.source("us.dol.eta-unemployment-insurance")
    assert dol.institution.startswith("U.S. Department of Labor")
    assert "ETA-538" in dol.source_series_ids
    assert EconomicEventFamily.LABOUR_MARKET in dol.event_families

    philadelphia = registry.source("us.frb.philadelphia-surveys")
    assert "SPF" not in philadelphia.source_series_ids
    assert philadelphia.source_release_ids == (
        "manufacturing-business-outlook-survey",
    )
    assert EconomicEventFamily.CONFIDENCE_SURVEY in philadelphia.event_families
    assert OfficialSourceFormat.ARCHIVE in philadelphia.formats
    assert philadelphia.availability_time_precision.value == "inferred-bounded"
    assert philadelphia.verification_status.value == "empirically-verified"

    spf = registry.source("us.frb.philadelphia-spf")
    assert spf.verification_status.value == "empirically-verified"
    assert spf.parser_id == "official.philadelphia-spf.v1"
    assert len(spf.source_series_ids) == 4
    assert spf.source_release_ids == ("survey-of-professional-forecasters",)

    g17 = registry.source("us.frb.industrial-production")
    assert OfficialSourceFormat.TEXT in g17.formats
    assert "text/plain" in g17.expected_media_types

    cpi = registry.source("us.bls.cpi")
    assert cpi.source_release_ids == ("CPI",)
    assert OfficialSourceFormat.PDF in cpi.formats
    assert cpi.verification_status.value == "empirically-verified"

    ppi = registry.source("us.bls.ppi")
    assert ppi.source_release_ids == ("PPI",)
    assert set(ppi.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }
    assert ppi.verification_status.value == "empirically-verified"

    employment = registry.source("us.bls.employment-situation")
    assert employment.source_release_ids == ("Employment Situation",)
    assert set(employment.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }
    assert employment.verification_status.value == "empirically-verified"

    jolts = registry.source("us.bls.jolts")
    assert jolts.source_release_ids == ("Job Openings and Labor Turnover",)
    assert set(jolts.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }
    assert jolts.verification_status.value == "empirically-verified"

    productivity = registry.source("us.bls.productivity-costs")
    assert productivity.source_release_ids == ("Productivity and Costs",)
    assert set(productivity.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }
    assert productivity.verification_status.value == "empirically-verified"

    gdp = registry.source("us.bea.gdp")
    assert gdp.source_release_ids == ("Gross Domestic Product",)
    assert set(gdp.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert gdp.availability_time_precision.value == "exact-minute"
    assert gdp.verification_status.value == "empirically-verified"

    retail = registry.source("us.census.retail-sales")
    assert retail.source_release_ids == (
        "Advance Monthly Sales for Retail and Food Services",
    )
    assert set(retail.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.XLS,
        OfficialSourceFormat.XLSX,
    }
    assert retail.availability_time_precision.value == "exact-minute"
    assert retail.verification_status.value == "empirically-verified"

    housing = registry.source("us.census.new-residential-construction")
    assert housing.source_release_ids == ("New Residential Construction",)
    assert set(housing.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
    }
    assert housing.availability_time_precision.value == "exact-minute"
    assert housing.verification_status.value == "empirically-verified"

    trade = registry.source("us.bea-census.international-trade")
    assert trade.source_release_ids == (
        "FT-900 U.S. International Trade in Goods and Services",
    )
    assert set(trade.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert trade.availability_time_precision.value == "exact-minute"
    assert trade.verification_status.value == "empirically-verified"

    mtis = registry.source("us.census.mtis")
    assert mtis.source_release_ids == (
        "Manufacturing and Trade Inventories and Sales",
    )
    assert set(mtis.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
        OfficialSourceFormat.XLS,
        OfficialSourceFormat.XLSX,
    }
    assert mtis.availability_time_precision.value == "exact-minute"
    assert mtis.verification_status.value == "empirically-verified"

    fomc = registry.source("us.frb.fomc")
    assert fomc.source_release_ids == (
        "FOMC minutes",
        "FOMC statement",
        "Summary of Economic Projections",
    )
    assert fomc.parser_id == "official.federal-reserve-fomc.v1"
    assert fomc.verification_status.value == "empirically-verified"

    money_stock = registry.source("us.frb.money-stock")
    assert money_stock.source_release_ids == ("H.6 Money Stock Measures",)
    assert money_stock.parser_id == "official.federal-reserve-h6.v1"
    assert money_stock.verification_status.value == "empirically-verified"


def test_builtin_us_profile_covers_every_required_family_and_program() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)

    assert profile.registry_id == registry.registry_id
    assert len(profile.programs) == 22
    assert {item.event_family for item in profile.programs} == set(
        EconomicEventFamily
    )
    assert set(profile.required_program_keys) == set(profile.by_key)
    assert profile.by_key["us.dol.initial-claims"].source_key == (
        "us.dol.eta-unemployment-insurance"
    )
    assert profile.by_key["us.philadelphia-fed.mbos"].forecast_strategy is (
        UnitedStatesForecastStrategy.PRODUCER_OUTLOOK_SURVEY
    )
    assert profile.by_key["us.fomc.sep"].archive_start_date == "2007-10-31"
    assert profile.by_key["us.bls.ppi"].source_key == "us.bls.ppi"
    assert profile.by_key["us.bls.employment-situation"].source_key == (
        "us.bls.employment-situation"
    )
    assert profile.by_key["us.bls.jolts"].source_key == "us.bls.jolts"
    assert profile.by_key["us.bls.productivity-costs"].source_key == (
        "us.bls.productivity-costs"
    )
    assert profile.by_key["us.bea.gdp"].source_key == "us.bea.gdp"
    assert profile.by_key["us.census.retail-sales"].source_key == (
        "us.census.retail-sales"
    )
    assert profile.by_key["us.bea-census.international-trade"].source_key == (
        "us.bea-census.international-trade"
    )
    assert profile.by_key["us.census.housing-starts-permits"].source_key == (
        "us.census.new-residential-construction"
    )
    assert (
        profile.by_key["us.census.manufacturing-trade-inventories"].source_key
        == "us.census.mtis"
    )
    assert profile.by_key["us.frb.money-stock"].source_key == (
        "us.frb.money-stock"
    )
    assert UnitedStatesBackfillProfileV1.from_json(profile.to_json()) == profile


def test_profile_rejects_registry_and_required_program_drift() -> None:
    profile = built_in_united_states_backfill_profile(
        load_packaged_official_source_registry()
    )
    payload = profile.to_dict()
    payload["registry_id"] = "not-a-registry"
    with pytest.raises(ValueError, match="official registry identity"):
        UnitedStatesBackfillProfileV1.from_dict(payload)

    payload = profile.to_dict()
    payload["required_program_keys"] = list(profile.required_program_keys[:-1])
    with pytest.raises(ValueError, match="differ from the inventory"):
        UnitedStatesBackfillProfileV1.from_dict(payload)


def test_packaged_g17_bytes_and_request_are_exact() -> None:
    path = packaged_federal_reserve_g17_2002_path()
    content = load_packaged_federal_reserve_g17_2002()
    request = build_federal_reserve_g17_archive_request(
        load_packaged_official_source_registry(), "2002-12-17"
    )

    assert path.is_file()
    assert len(content) == 150_109
    assert request.uri == G17_2002_12_17_URI
    assert request.source_format is OfficialSourceFormat.TEXT
    assert request.window_start == request.window_end == "2002-12-17"
    assert _snapshot().content_sha256 == G17_2002_12_17_SHA256


def test_generic_adapter_retains_plain_text_as_one_bounded_record() -> None:
    snapshot = _snapshot()
    source = load_packaged_official_source_registry().source(
        "us.frb.industrial-production"
    )
    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=1
    )

    assert len(records) == 1
    assert records[0]["kind"] == OfficialAdapterRecordKind.TEXT_DOCUMENT.value
    assert records[0]["fields"]["line_count"] > 1_000
    assert "INDUSTRIAL PRODUCTION" in records[0]["fields"]["text"]


def test_g17_replay_recomputes_ex_ante_triplet_and_calendar_release() -> None:
    snapshot = _snapshot()
    triplet = parse_federal_reserve_g17_release(snapshot)

    assert triplet.reference_period == "2002-11"
    assert triplet.previous_reference_period == "2002-10"
    assert triplet.actual_value == 0.1
    assert triplet.actual_lexical == ".1"
    assert triplet.previous_as_known_value == -0.8
    assert triplet.previous_as_known_lexical == "-.8"
    assert triplet.revised_previous_value == -0.6
    assert triplet.revised_previous_lexical == "-.6"
    assert triplet.released_lexical == "2002-12-17T09:15:00"
    assert triplet.normalized_sha256 == G17_2002_12_17_NORMALIZED_SHA256
    assert UnitedStatesReleaseTripletV1.from_dict(triplet.to_dict()) == triplet
    federal_reserve_g17_2002_artifact_lock().verify(triplet)

    release = economic_calendar_release_from_g17_triplet(triplet, snapshot)
    assert release.actual_value == 0.1
    assert release.actual_lexical == ".1"
    assert release.stage is EconomicReleaseStage.INITIAL
    assert release.reference_period == "2002-11"
    assert release.released_lexical == "2002-12-17T09:15:00-05:00"
    assert release.available_at_ns == triplet.released_at_ns
    assert release.first_observed_at_ns == RETRIEVED_AT_NS
    assert release.source.metadata["previous_as_known_value"] == -0.8
    assert release.source.metadata["revised_previous_value"] == -0.6


def test_g17_parser_and_lock_fail_closed_on_drift() -> None:
    malformed = (
        b"FEDERAL RESERVE STATISTICAL RELEASE\n"
        b"For release at 9:15 a.m. (EST) December 17, 2002\n"
    )
    with pytest.raises(ValueError, match="summary table"):
        parse_federal_reserve_g17_release(_snapshot(malformed))

    triplet = parse_federal_reserve_g17_release(_snapshot())
    changed = replace(triplet, actual_value=0.2, triplet_id="")
    with pytest.raises(ValueError, match="differs from replayed triplet"):
        federal_reserve_g17_2002_artifact_lock().verify(changed)


def test_us_coverage_audit_passes_only_complete_raw_backed_slices() -> None:
    profile = built_in_united_states_backfill_profile(
        load_packaged_official_source_registry()
    )
    coverages = tuple(
        _coverage(profile, key) for key in profile.required_program_keys
    )
    audit = audit_united_states_backfill(profile, coverages)

    assert audit.passed
    assert audit.program_count == 22
    assert len(audit.family_counts) == 12
    assert audit.expected_occurrence_count == 220
    assert audit.schedule_count == audit.initial_actual_count == 220
    assert not audit.incomplete_program_keys
    assert not audit.blocking_gap_program_keys
    assert "us.bls.cpi" in audit.explicit_no_forecast_program_keys
    assert UnitedStatesBackfillAuditV1.from_json(audit.to_json()) == audit
    assert (
        UnitedStatesProgramCoverageV1.from_dict(coverages[0].to_dict())
        == coverages[0]
    )


def test_us_coverage_audit_exposes_missing_actual_and_blocking_gap() -> None:
    profile = built_in_united_states_backfill_profile(
        load_packaged_official_source_registry()
    )
    coverages = [
        _coverage(profile, key) for key in profile.required_program_keys
    ]
    index = next(
        index
        for index, item in enumerate(coverages)
        if item.program_key == "us.bls.cpi"
    )
    coverages[index] = replace(
        coverages[index],
        initial_actual_count=9,
        gap_reasons=(
            UnitedStatesCoverageGapReason.MISSING_INITIAL_VALUE,
            UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,
        ),
        coverage_id="",
    )
    audit = audit_united_states_backfill(profile, coverages)

    assert not audit.passed
    assert audit.incomplete_program_keys == ("us.bls.cpi",)
    assert audit.blocking_gap_program_keys == ("us.bls.cpi",)


def test_us_coverage_rejects_overclaiming_and_tampered_ids() -> None:
    profile = built_in_united_states_backfill_profile(
        load_packaged_official_source_registry()
    )
    coverage = _coverage(profile, "us.bls.cpi")
    with pytest.raises(ValueError, match="exceeds expected"):
        replace(coverage, initial_actual_count=11, coverage_id="")

    payload = coverage.to_dict()
    payload["coverage_id"] = "us-program-coverage:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="deterministic identity"):
        UnitedStatesProgramCoverageV1.from_dict(payload)
