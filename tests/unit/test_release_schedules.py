"""Tests for historical as-known economic release schedules."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from histdatacom.market_context import (
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicExpectedOccurrenceStatus,
    EconomicExpectedOccurrenceV1,
    EconomicIndicatorCatalogV1,
    EconomicIndicatorFrequency,
    EconomicReleaseAvailabilityBasis,
    EconomicReleaseScheduleAuditV1,
    EconomicReleaseScheduleChainV1,
    EconomicReleaseScheduleCorpusV1,
    EconomicReleaseScheduleEvidenceV1,
    EconomicReleaseScheduleQueryV1,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicReleaseTimeEvidenceV1,
    EconomicReleaseVintageMutationV1,
    EconomicScheduleExceptionKind,
    EconomicTimePrecision,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    apply_economic_release_vintage_mutation,
    audit_economic_release_schedules,
    build_economic_release_schedule_corpus,
    build_economic_release_vintage_chain,
    build_official_indicator_catalog,
    load_packaged_official_source_registry,
    query_economic_release_schedule_as_known,
    read_economic_release_schedule_corpus,
    write_economic_release_schedule_corpus,
)

SECOND_NS = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND_NS
COVERAGE_START = BASE_NS
COVERAGE_END = BASE_NS + 1_000 * SECOND_NS


def _qualified_catalog() -> EconomicIndicatorCatalogV1:
    catalog = build_official_indicator_catalog(
        load_packaged_official_source_registry()
    )
    original = catalog.entry("us.consumer-and-producer-prices")
    era = replace(
        original.methodology_eras[0],
        source_series_ids=("CUUR0000SA0",),
        source_table_ids=("CPI-TABLE-1",),
        source_release_ids=("CPI-NEWS-RELEASE",),
        era_id="",
    )
    rule = replace(
        original.expected_release_rules[0],
        frequency=EconomicIndicatorFrequency.MONTHLY,
        qualified=True,
        qualification_evidence=("Official calendar fixture.",),
        limitations=("Fixture qualifies only schedule semantics.",),
        rule_id="",
    )
    entry = replace(
        original,
        frequency=EconomicIndicatorFrequency.MONTHLY,
        methodology_eras=(era,),
        expected_release_rules=(rule,),
        entry_id="",
    )
    return replace(
        catalog,
        entries=tuple(
            entry if item.indicator_key == entry.indicator_key else item
            for item in catalog.entries
        ),
        catalog_id="",
    )


def _source(known: int, digest: str) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name="Official release calendar fixture",
        source_version="2026-09-14",
        retrieved_at_ns=known,
        content_sha256=digest,
        adapter_name="official-schedule-fixture",
        adapter_version="1",
        license_name="Public official fixture",
        redistribution_allowed=True,
        redistribution_constraints=("Attribute the producer.",),
        limitations=("Fixture proves schedule semantics only.",),
        source_uri="https://example.invalid/calendar",
    )


def _time(
    scheduled: int,
    known: int,
    *,
    published: int | None = None,
    source_timezone: str = "UTC",
    time_precision: EconomicTimePrecision = EconomicTimePrecision.EXACT_SECOND,
    precision: MarketContextPrecision = MarketContextPrecision.EXACT,
) -> EconomicReleaseTimeEvidenceV1:
    zone = ZoneInfo(source_timezone)
    return EconomicReleaseTimeEvidenceV1(
        scheduled_for_ns=scheduled,
        scheduled_lexical=datetime.fromtimestamp(
            scheduled / SECOND_NS, tz=zone
        ).isoformat(),
        actual_published_at_ns=published,
        actual_published_lexical=(
            None
            if published is None
            else datetime.fromtimestamp(
                published / SECOND_NS, tz=zone
            ).isoformat()
        ),
        first_observed_at_ns=known,
        source_updated_at_ns=None,
        source_updated_lexical=None,
        available_at_ns=known,
        publication_not_before_ns=None,
        publication_not_after_ns=None,
        source_timezone=source_timezone,
        timezone_evidence="Official UTC fixture.",
        time_precision=time_precision,
        precision=precision,
        availability_basis=(
            EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION
            if published is not None
            else EconomicReleaseAvailabilityBasis.FIRST_OBSERVED
        ),
    )


def _initial(
    logical_event_key: str,
    evidence: EconomicReleaseTimeEvidenceV1,
    digest: str,
    status: EconomicReleaseStatus,
    *,
    actual: float | None = None,
) -> EconomicCalendarReleaseV1:
    actual_status = status in {
        EconomicReleaseStatus.RELEASED,
        EconomicReleaseStatus.UNSCHEDULED,
    }
    return EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key="us.consumer-and-producer-prices",
        series_version="v1",
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="U.S. Bureau of Labor Statistics",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="consumer-and-producer-prices",
        source_series_id="cuur0000sa0",
        source_table_id="cpi-table-1",
        source_release_id=f"release.{digest[:8]}",
        source_request_id=f"request.{digest[:8]}",
        title="Consumer Price Index",
        reference_period="2026-07",
        reference_period_end_ns=BASE_NS + 50 * SECOND_NS,
        frequency="monthly",
        seasonality="not-seasonally-adjusted",
        unit="index",
        scale=1.0,
        base="1982-84=100",
        stage=EconomicReleaseStage.INITIAL,
        status=status,
        scheduled_for_ns=evidence.scheduled_for_ns,
        scheduled_lexical=evidence.scheduled_lexical,
        released_at_ns=evidence.actual_published_at_ns,
        released_lexical=evidence.actual_published_lexical,
        first_observed_at_ns=evidence.first_observed_at_ns,
        available_at_ns=evidence.available_at_ns,
        source_timezone=evidence.source_timezone,
        timezone_evidence=evidence.timezone_evidence,
        time_precision=evidence.time_precision,
        precision=evidence.precision,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=_source(evidence.first_observed_at_ns, digest),
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        limitations=("Fixture proves schedule semantics only.",),
        actual_value=actual,
        actual_lexical=None if not actual_status else str(actual),
        content_sha256=digest,
    )


def _mutation(
    previous: EconomicCalendarReleaseV1,
    evidence: EconomicReleaseTimeEvidenceV1,
    digest: str,
    status: EconomicReleaseStatus,
    *,
    reason: str | None = None,
    actual: float | None = None,
) -> EconomicReleaseVintageMutationV1:
    return EconomicReleaseVintageMutationV1(
        previous_release_id=previous.release_id,
        logical_event_key=previous.logical_event_key,
        series_id=previous.series_id,
        source_release_id=f"release.{digest[:8]}",
        source_request_id=f"request.{digest[:8]}",
        stage=EconomicReleaseStage.INITIAL,
        status=status,
        time_evidence=evidence,
        source=_source(evidence.first_observed_at_ns, digest),
        limitations=("Fixture proves schedule semantics only.",),
        schedule_change_reason=reason,
        actual_value=actual,
        actual_lexical=None if actual is None else str(actual),
        content_sha256=digest,
    )


def _occurrence(
    catalog: EconomicIndicatorCatalogV1,
    logical_event_key: str,
    scheduled: int,
    status: EconomicExpectedOccurrenceStatus,
    *,
    status_known_at_ns: int,
    reason: str | None = None,
) -> EconomicExpectedOccurrenceV1:
    entry = catalog.entry("us.consumer-and-producer-prices")
    return EconomicExpectedOccurrenceV1(
        logical_event_key=logical_event_key,
        indicator_key=entry.indicator_key,
        rule_id=entry.expected_release_rules[0].rule_id,
        reference_period="2026-07",
        reference_period_end_ns=BASE_NS + 50 * SECOND_NS,
        expected_for_ns=scheduled,
        status=status,
        status_known_at_ns=status_known_at_ns,
        source_key=entry.legal_producer_source_key,
        source_occurrence_id=logical_event_key,
        evidence_sha256="a" * 64,
        reason=reason,
    )


def _schedule_evidence(
    release: EconomicCalendarReleaseV1,
    kind: EconomicScheduleExceptionKind,
    *,
    reason: str | None = None,
    fold: int | None = None,
) -> EconomicReleaseScheduleEvidenceV1:
    return EconomicReleaseScheduleEvidenceV1(
        release_id=release.release_id,
        source_key="us.bls.public-data",
        known_at_ns=release.available_at_ns,
        timezone_database_version="tzdata-2026b",
        holiday_calendar_version="us-federal-holidays.v1",
        dst_fold=fold,
        exception_kind=kind,
        exception_reason=reason,
        evidence_sha256=release.content_sha256 or "f" * 64,
    )


def _rescheduled_chain(
    catalog: EconomicIndicatorCatalogV1,
) -> EconomicReleaseScheduleChainV1:
    original_time = BASE_NS + 100 * SECOND_NS
    moved_time = BASE_NS + 120 * SECOND_NS
    initial_evidence = _time(original_time, BASE_NS + 10 * SECOND_NS)
    initial = _initial(
        "us.cpi.2026-07",
        initial_evidence,
        "1" * 64,
        EconomicReleaseStatus.SCHEDULED,
    )
    reschedule_evidence = _time(moved_time, BASE_NS + 20 * SECOND_NS)
    reschedule = _mutation(
        initial,
        reschedule_evidence,
        "2" * 64,
        EconomicReleaseStatus.RESCHEDULED,
        reason="Official holiday calendar shifted publication.",
    )
    moved = apply_economic_release_vintage_mutation(initial, reschedule)
    publication_evidence = _time(
        moved_time,
        BASE_NS + 125 * SECOND_NS,
        published=BASE_NS + 125 * SECOND_NS,
    )
    publication = _mutation(
        moved,
        publication_evidence,
        "3" * 64,
        EconomicReleaseStatus.RELEASED,
        actual=308.1,
    )
    vintage_chain = build_economic_release_vintage_chain(
        initial,
        initial_time_evidence=initial_evidence,
        mutations=(reschedule, publication),
        limitations=("Fixture chain.",),
    )
    releases = vintage_chain.releases
    return EconomicReleaseScheduleChainV1(
        vintage_chain=vintage_chain,
        expected_occurrence=_occurrence(
            catalog,
            initial.logical_event_key,
            original_time,
            EconomicExpectedOccurrenceStatus.RELEASED,
            status_known_at_ns=BASE_NS + 125 * SECOND_NS,
        ),
        schedule_evidence=(
            _schedule_evidence(
                releases[0], EconomicScheduleExceptionKind.ROUTINE
            ),
            _schedule_evidence(
                releases[1],
                EconomicScheduleExceptionKind.HOLIDAY_SHIFT,
                reason="Independence Day schedule shift.",
            ),
            _schedule_evidence(
                releases[2], EconomicScheduleExceptionKind.ROUTINE
            ),
        ),
        limitations=("Fixture chain.",),
    )


def _corpus(
    catalog: EconomicIndicatorCatalogV1,
    *chains: EconomicReleaseScheduleChainV1,
) -> EconomicReleaseScheduleCorpusV1:
    return build_economic_release_schedule_corpus(
        catalog,
        chains,
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        limitations=("Fixture corpus.",),
    )


def test_as_known_schedule_moves_only_when_reschedule_becomes_public() -> None:
    catalog = _qualified_catalog()
    chain = _rescheduled_chain(catalog)
    corpus = _corpus(catalog, chain)

    before_initial = query_economic_release_schedule_as_known(
        corpus,
        start_ns=COVERAGE_START,
        end_ns=COVERAGE_END,
        decision_at_ns=BASE_NS + 9 * SECOND_NS,
    )
    assert before_initial.states == ()

    initial = query_economic_release_schedule_as_known(
        corpus,
        start_ns=COVERAGE_START,
        end_ns=COVERAGE_END,
        decision_at_ns=BASE_NS + 19 * SECOND_NS,
    ).states[0]
    assert initial.scheduled_for_ns == BASE_NS + 100 * SECOND_NS
    assert initial.release.status is EconomicReleaseStatus.SCHEDULED
    assert initial.expected_occurrence_id is None

    moved = query_economic_release_schedule_as_known(
        corpus,
        start_ns=COVERAGE_START,
        end_ns=COVERAGE_END,
        decision_at_ns=BASE_NS + 20 * SECOND_NS,
    ).states[0]
    assert moved.scheduled_for_ns == BASE_NS + 120 * SECOND_NS
    assert moved.release.status is EconomicReleaseStatus.RESCHEDULED
    assert moved.visible_release_ids == tuple(
        item.release_id for item in chain.vintage_chain.releases[:2]
    )


def test_actual_publication_does_not_replace_the_schedule_time_surface() -> (
    None
):
    catalog = _qualified_catalog()
    chain = _rescheduled_chain(catalog)
    corpus = _corpus(catalog, chain)
    query = query_economic_release_schedule_as_known(
        corpus,
        start_ns=COVERAGE_START,
        end_ns=COVERAGE_END,
        decision_at_ns=BASE_NS + 130 * SECOND_NS,
    )
    state = query.states[0]
    assert state.scheduled_for_ns == BASE_NS + 120 * SECOND_NS
    assert state.release.released_at_ns == BASE_NS + 125 * SECOND_NS
    assert state.advance_notice_ns == -5 * SECOND_NS
    assert (
        state.expected_occurrence_id == chain.expected_occurrence.occurrence_id
    )
    assert EconomicReleaseScheduleQueryV1.from_json(query.to_json()) == query


def test_schedule_audit_counts_holiday_reschedule_and_no_future_replay() -> (
    None
):
    catalog = _qualified_catalog()
    corpus = _corpus(catalog, _rescheduled_chain(catalog))
    audit = audit_economic_release_schedules(corpus)
    assert audit.passed
    assert audit.rescheduled_event_keys == ("us.cpi.2026-07",)
    assert audit.cancelled_event_keys == ()
    assert audit.unscheduled_event_keys == ()
    assert dict(audit.exception_counts) == {"holiday-shift": 1, "routine": 2}
    assert EconomicReleaseScheduleAuditV1.from_json(audit.to_json()) == audit


def test_unscheduled_emergency_appears_only_at_publication() -> None:
    catalog = _qualified_catalog()
    published = BASE_NS + 300 * SECOND_NS
    time = _time(published, published, published=published)
    release = _initial(
        "us.cpi.emergency",
        time,
        "4" * 64,
        EconomicReleaseStatus.UNSCHEDULED,
        actual=309.0,
    )
    vintage = build_economic_release_vintage_chain(
        release,
        initial_time_evidence=time,
        limitations=("Emergency fixture.",),
    )
    chain = EconomicReleaseScheduleChainV1(
        vintage_chain=vintage,
        expected_occurrence=None,
        schedule_evidence=(
            _schedule_evidence(
                release,
                EconomicScheduleExceptionKind.UNSCHEDULED_EMERGENCY,
                reason="Emergency action had no advance schedule.",
            ),
        ),
        limitations=("Emergency fixture.",),
    )
    corpus = _corpus(catalog, chain)
    assert (
        query_economic_release_schedule_as_known(
            corpus,
            start_ns=COVERAGE_START,
            end_ns=COVERAGE_END,
            decision_at_ns=published - 1,
        ).states
        == ()
    )
    assert (
        query_economic_release_schedule_as_known(
            corpus,
            start_ns=COVERAGE_START,
            end_ns=COVERAGE_END,
            decision_at_ns=published,
        )
        .states[0]
        .release.status
        is EconomicReleaseStatus.UNSCHEDULED
    )
    assert audit_economic_release_schedules(corpus).unscheduled_event_keys == (
        "us.cpi.emergency",
    )


def test_cancellation_is_retained_and_can_be_filtered() -> None:
    catalog = _qualified_catalog()
    scheduled = BASE_NS + 400 * SECOND_NS
    initial_time = _time(scheduled, BASE_NS + 310 * SECOND_NS)
    initial = _initial(
        "us.cpi.cancelled",
        initial_time,
        "5" * 64,
        EconomicReleaseStatus.SCHEDULED,
    )
    cancel_time = _time(scheduled, BASE_NS + 320 * SECOND_NS)
    mutation = _mutation(
        initial,
        cancel_time,
        "6" * 64,
        EconomicReleaseStatus.CANCELLED,
        reason="Official cancellation.",
    )
    vintage = build_economic_release_vintage_chain(
        initial,
        initial_time_evidence=initial_time,
        mutations=(mutation,),
        limitations=("Cancellation fixture.",),
    )
    chain = EconomicReleaseScheduleChainV1(
        vintage_chain=vintage,
        expected_occurrence=_occurrence(
            catalog,
            initial.logical_event_key,
            scheduled,
            EconomicExpectedOccurrenceStatus.CANCELLED,
            status_known_at_ns=BASE_NS + 320 * SECOND_NS,
            reason="Official cancellation.",
        ),
        schedule_evidence=(
            _schedule_evidence(
                vintage.releases[0], EconomicScheduleExceptionKind.ROUTINE
            ),
            _schedule_evidence(
                vintage.releases[1],
                EconomicScheduleExceptionKind.CANCELLATION,
                reason="Official cancellation.",
            ),
        ),
        limitations=("Cancellation fixture.",),
    )
    corpus = _corpus(catalog, chain)
    assert (
        query_economic_release_schedule_as_known(
            corpus,
            start_ns=COVERAGE_START,
            end_ns=COVERAGE_END,
            decision_at_ns=BASE_NS + 330 * SECOND_NS,
        )
        .states[0]
        .release.status
        is EconomicReleaseStatus.CANCELLED
    )
    assert (
        query_economic_release_schedule_as_known(
            corpus,
            start_ns=COVERAGE_START,
            end_ns=COVERAGE_END,
            decision_at_ns=BASE_NS + 330 * SECOND_NS,
            include_cancelled=False,
        ).states
        == ()
    )
    assert audit_economic_release_schedules(corpus).cancelled_event_keys == (
        "us.cpi.cancelled",
    )


def test_dst_reschedule_requires_fold_and_status_evidence() -> None:
    catalog = _qualified_catalog()
    chain = _rescheduled_chain(catalog)
    rescheduled = chain.vintage_chain.releases[1]
    with pytest.raises(ValueError, match="explicit fold"):
        replace(
            chain,
            schedule_evidence=(
                chain.schedule_evidence[0],
                _schedule_evidence(
                    rescheduled,
                    EconomicScheduleExceptionKind.DST_TRANSITION,
                    reason="DST fallback.",
                ),
                chain.schedule_evidence[2],
            ),
            chain_id="",
        )
    with pytest.raises(ValueError, match="matching exception"):
        replace(
            chain,
            schedule_evidence=(
                chain.schedule_evidence[0],
                _schedule_evidence(
                    rescheduled,
                    EconomicScheduleExceptionKind.ROUTINE,
                ),
                chain.schedule_evidence[2],
            ),
            chain_id="",
        )


def test_tentative_date_only_schedule_retains_local_time_evidence() -> None:
    catalog = _qualified_catalog()
    scheduled = BASE_NS + 500 * SECOND_NS
    evidence = _time(
        scheduled,
        BASE_NS + 410 * SECOND_NS,
        source_timezone="America/New_York",
        time_precision=EconomicTimePrecision.DATE_ONLY,
        precision=MarketContextPrecision.APPROXIMATE,
    )
    release = _initial(
        "us.cpi.tentative", evidence, "7" * 64, EconomicReleaseStatus.TENTATIVE
    )
    vintage = build_economic_release_vintage_chain(
        release,
        initial_time_evidence=evidence,
        limitations=("Tentative date-only fixture.",),
    )
    chain = EconomicReleaseScheduleChainV1(
        vintage_chain=vintage,
        expected_occurrence=_occurrence(
            catalog,
            release.logical_event_key,
            scheduled,
            EconomicExpectedOccurrenceStatus.SCHEDULED,
            status_known_at_ns=BASE_NS + 410 * SECOND_NS,
        ),
        schedule_evidence=(
            _schedule_evidence(release, EconomicScheduleExceptionKind.ROUTINE),
        ),
        limitations=("Tentative date-only fixture.",),
    )
    state = query_economic_release_schedule_as_known(
        _corpus(catalog, chain),
        start_ns=COVERAGE_START,
        end_ns=COVERAGE_END,
        decision_at_ns=BASE_NS + 420 * SECOND_NS,
    ).states[0]
    assert state.release.status is EconomicReleaseStatus.TENTATIVE
    assert state.release.time_precision is EconomicTimePrecision.DATE_ONLY
    assert state.release.source_timezone == "America/New_York"
    assert state.release.scheduled_lexical.endswith("-05:00")
    assert state.evidence.timezone_database_version == "tzdata-2026b"


def test_delayed_discontinued_and_source_outage_states_are_preserved() -> None:
    catalog = _qualified_catalog()
    cases = (
        (
            "us.cpi.delayed",
            BASE_NS + 600 * SECOND_NS,
            EconomicReleaseStatus.DELAYED,
            EconomicExpectedOccurrenceStatus.RESCHEDULED,
            EconomicScheduleExceptionKind.PRODUCER_DELAY,
            "Official producer delay.",
            10 * SECOND_NS,
            ("8", "9"),
        ),
        (
            "us.cpi.discontinued",
            BASE_NS + 700 * SECOND_NS,
            EconomicReleaseStatus.DISCONTINUED,
            EconomicExpectedOccurrenceStatus.DISCONTINUED,
            EconomicScheduleExceptionKind.ROUTINE,
            "Official series discontinuation.",
            0,
            ("b", "c"),
        ),
        (
            "us.cpi.source-outage",
            BASE_NS + 800 * SECOND_NS,
            EconomicReleaseStatus.SOURCE_CALENDAR_UNAVAILABLE,
            EconomicExpectedOccurrenceStatus.SOURCE_UNAVAILABLE,
            EconomicScheduleExceptionKind.SOURCE_CALENDAR_OUTAGE,
            "Official calendar archive unavailable.",
            0,
            ("d", "e"),
        ),
    )
    chains = []
    for index, (
        event_key,
        scheduled,
        release_status,
        occurrence_status,
        exception_kind,
        reason,
        schedule_shift,
        digest_characters,
    ) in enumerate(cases):
        initial_evidence = _time(
            scheduled, BASE_NS + (510 + index * 20) * SECOND_NS
        )
        initial = _initial(
            event_key,
            initial_evidence,
            digest_characters[0] * 64,
            EconomicReleaseStatus.SCHEDULED,
        )
        changed_evidence = _time(
            scheduled + schedule_shift,
            BASE_NS + (520 + index * 20) * SECOND_NS,
        )
        mutation = _mutation(
            initial,
            changed_evidence,
            digest_characters[1] * 64,
            release_status,
            reason=(
                reason
                if release_status is EconomicReleaseStatus.DELAYED
                else None
            ),
        )
        vintage = build_economic_release_vintage_chain(
            initial,
            initial_time_evidence=initial_evidence,
            mutations=(mutation,),
            limitations=("Status fixture.",),
        )
        chains.append(
            EconomicReleaseScheduleChainV1(
                vintage_chain=vintage,
                expected_occurrence=_occurrence(
                    catalog,
                    event_key,
                    scheduled,
                    occurrence_status,
                    status_known_at_ns=(
                        BASE_NS + (520 + index * 20) * SECOND_NS
                    ),
                    reason=(
                        reason
                        if occurrence_status
                        in {
                            EconomicExpectedOccurrenceStatus.DISCONTINUED,
                            EconomicExpectedOccurrenceStatus.SOURCE_UNAVAILABLE,
                        }
                        else None
                    ),
                ),
                schedule_evidence=(
                    _schedule_evidence(
                        vintage.releases[0],
                        EconomicScheduleExceptionKind.ROUTINE,
                    ),
                    _schedule_evidence(
                        vintage.releases[1],
                        exception_kind,
                        reason=(
                            None
                            if exception_kind
                            is EconomicScheduleExceptionKind.ROUTINE
                            else reason
                        ),
                    ),
                ),
                limitations=("Status fixture.",),
            )
        )
    query = query_economic_release_schedule_as_known(
        _corpus(catalog, *chains),
        start_ns=COVERAGE_START,
        end_ns=COVERAGE_END,
        decision_at_ns=BASE_NS + 900 * SECOND_NS,
    )
    assert {state.release.status for state in query.states} == {
        EconomicReleaseStatus.DELAYED,
        EconomicReleaseStatus.DISCONTINUED,
        EconomicReleaseStatus.SOURCE_CALENDAR_UNAVAILABLE,
    }
    assert audit_economic_release_schedules(_corpus(catalog, *chains)).passed


def test_unqualified_catalog_rule_and_status_mismatch_fail_closed() -> None:
    qualified = _qualified_catalog()
    chain = _rescheduled_chain(qualified)
    unqualified = build_official_indicator_catalog(
        load_packaged_official_source_registry()
    )
    unqualified_rule = unqualified.entry(
        "us.consumer-and-producer-prices"
    ).expected_release_rules[0]
    unqualified_chain = replace(
        chain,
        expected_occurrence=replace(
            chain.expected_occurrence,
            rule_id=unqualified_rule.rule_id,
            occurrence_id="",
        ),
        chain_id="",
    )
    with pytest.raises(ValueError, match="not evidence-qualified"):
        _corpus(unqualified, unqualified_chain)

    mismatched_occurrence = replace(
        chain.expected_occurrence,
        status=EconomicExpectedOccurrenceStatus.RESCHEDULED,
        occurrence_id="",
    )
    mismatched_chain = replace(
        chain,
        expected_occurrence=mismatched_occurrence,
        chain_id="",
    )
    audit = audit_economic_release_schedules(
        _corpus(qualified, mismatched_chain)
    )
    assert not audit.passed
    assert audit.occurrence_status_mismatches == ("us.cpi.2026-07",)


def test_schedule_corpus_round_trip_persistence_and_tamper_rejection(
    tmp_path: Path,
) -> None:
    catalog = _qualified_catalog()
    corpus = _corpus(catalog, _rescheduled_chain(catalog))
    assert EconomicReleaseScheduleCorpusV1.from_json(corpus.to_json()) == corpus
    path = write_economic_release_schedule_corpus(
        corpus, tmp_path / "schedule.json"
    )
    assert read_economic_release_schedule_corpus(path) == corpus

    payload = corpus.to_dict()
    payload["complete"] = False
    with pytest.raises(ValueError, match="corpus_id"):
        EconomicReleaseScheduleCorpusV1.from_dict(payload)
