"""Tests for provider-neutral economic release vintages and queries."""

from __future__ import annotations

import inspect
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

import histdatacom.market_context.economic_calendar as calendar_module
from histdatacom.market_context import (
    EconomicCalendarAsKnownReaderV1,
    EconomicCalendarCorpusV1,
    EconomicCalendarEventStateV1,
    EconomicCalendarForecastV1,
    EconomicCalendarQueryV1,
    EconomicCalendarReleaseV1,
    EconomicCalendarSurpriseV1,
    EconomicEventFamily,
    EconomicForecastKind,
    EconomicForecastScope,
    EconomicForecastStatistic,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicSurprisePolicyV1,
    EconomicTimePrecision,
    EconomicUnitConversionV1,
    MarketContextEventV1,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    StaticOfficialEconomicCalendarAdapterV1,
    build_economic_calendar_corpus,
    compute_economic_calendar_surprises,
    economic_calendar_corpus_from_arrow,
    economic_calendar_corpus_to_arrow,
    economic_release_from_market_context,
    project_economic_calendar_state,
    query_economic_calendar_as_known,
    read_economic_calendar_corpus,
    replay_economic_calendar_corpus,
    write_economic_calendar_corpus,
)

SECOND_NS = 1_000_000_000
DAY_NS = 86_400 * SECOND_NS
PRIOR_TIME = 1_700_000_000 * SECOND_NS
TARGET_TIME = PRIOR_TIME + 30 * DAY_NS
COVERAGE_START = PRIOR_TIME - DAY_NS
COVERAGE_END = TARGET_TIME + DAY_NS
RETRIEVED_AT = COVERAGE_END


def _utc_text(timestamp_ns: int) -> str:
    seconds, remainder = divmod(timestamp_ns, SECOND_NS)
    value = datetime.fromtimestamp(seconds, tz=timezone.utc)
    if remainder:
        return (
            f"{value.strftime('%Y-%m-%dT%H:%M:%S')}."
            f"{remainder // 1_000:06d}+00:00"
        )
    return value.isoformat()


def _source(
    adapter_name: str,
    *,
    digest: str,
    source_name: str = "Official statistical producer fixture",
) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name=source_name,
        source_version=f"fixture-{digest[0]}",
        retrieved_at_ns=RETRIEVED_AT,
        content_sha256=digest,
        adapter_name=adapter_name,
        adapter_version="1.0",
        license_name="Public official-source fixture terms",
        redistribution_allowed=True,
        redistribution_constraints=("Attribute the statistical producer.",),
        limitations=("Fixture proves contracts, not production coverage.",),
        source_uri=f"https://example.invalid/{adapter_name}/{digest[0]}",
        metadata={"source_kind": "official_fixture"},
    )


def _release(
    *,
    logical_event_key: str,
    reference_period: str,
    reference_period_end_ns: int,
    event_time_ns: int,
    available_at_ns: int,
    stage: EconomicReleaseStage,
    revision_sequence: int,
    supersedes_release_id: str | None = None,
    actual_value: float | None = None,
    adapter_name: str = "bls-public-release",
    digest: str = "a" * 64,
    currency: str = "USD",
    status: EconomicReleaseStatus | None = None,
    event_family: EconomicEventFamily = EconomicEventFamily.INFLATION_PRICES,
    series_version: str = "v1",
    comparability_bridge_id: str | None = None,
    scheduled_for_ns: int | None = None,
    source_timezone: str = "UTC",
    scheduled_lexical: str | None = None,
    released_lexical: str | None = None,
    schedule_change_reason: str | None = None,
    value_conversion: EconomicUnitConversionV1 | None = None,
    actual_lexical: str | None = None,
) -> EconomicCalendarReleaseV1:
    selected_status = status or (
        EconomicReleaseStatus.RELEASED
        if actual_value is not None
        else EconomicReleaseStatus.SCHEDULED
    )
    scheduled = event_time_ns if scheduled_for_ns is None else scheduled_for_ns
    actual_status = selected_status in {
        EconomicReleaseStatus.RELEASED,
        EconomicReleaseStatus.UNSCHEDULED,
    }
    return EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key="us.cpi.all-items.sa.mom",
        series_version=series_version,
        comparability_bridge_id=comparability_bridge_id,
        economy="United States",
        economy_code="US" if currency == "USD" else "GB",
        currency=currency,
        institution="U.S. Bureau of Labor Statistics",
        event_family=event_family,
        indicator_id="cpi-all-items",
        source_series_id="cuur0000sa0",
        source_table_id="cpi-table-1",
        source_release_id=logical_event_key,
        source_request_id=f"request.{digest[:12]}",
        title="Consumer Price Index",
        reference_period=reference_period,
        reference_period_end_ns=reference_period_end_ns,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="percent_mom",
        scale=1.0,
        base="1982-84=100",
        stage=stage,
        status=selected_status,
        scheduled_for_ns=scheduled,
        scheduled_lexical=scheduled_lexical or _utc_text(scheduled),
        released_at_ns=event_time_ns if actual_status else None,
        released_lexical=(
            (released_lexical or _utc_text(event_time_ns))
            if actual_status
            else None
        ),
        first_observed_at_ns=available_at_ns,
        available_at_ns=available_at_ns,
        source_timezone=source_timezone,
        timezone_evidence="IANA timezone retained from official fixture.",
        time_precision=EconomicTimePrecision.EXACT_SECOND,
        precision=MarketContextPrecision.EXACT,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=_source(adapter_name, digest=digest),
        affected_currencies=(currency,),
        affected_symbols=("EURUSD", "GBPUSD"),
        limitations=("Official fixture omits intraday publication latency.",),
        schedule_change_reason=schedule_change_reason,
        value_conversion=value_conversion,
        actual_value=actual_value,
        actual_lexical=(
            None
            if actual_value is None
            else (
                actual_lexical
                if actual_lexical is not None
                else f"{actual_value:.1f}%"
            )
        ),
        content_sha256=digest,
        revision_sequence=revision_sequence,
        supersedes_release_id=supersedes_release_id,
    )


def _forecast(
    *,
    kind: EconomicForecastKind,
    available_at_ns: int,
    value: float | None,
    digest: str,
    logical_event_key: str = "us.cpi.2023-12",
    scope: EconomicForecastScope | None = None,
    statistic: EconomicForecastStatistic = EconomicForecastStatistic.MEDIAN,
) -> EconomicCalendarForecastV1:
    adapter = (
        "public-consensus-observation"
        if kind is EconomicForecastKind.OBSERVED_CONSENSUS
        else "point-in-time-machine-projection"
    )
    return EconomicCalendarForecastV1(
        logical_event_key=logical_event_key,
        kind=kind,
        scope=(
            scope
            if scope is not None
            else (
                EconomicForecastScope.EVENT_CONSENSUS
                if kind is EconomicForecastKind.OBSERVED_CONSENSUS
                else EconomicForecastScope.MACHINE_EVENT_TARGET
            )
        ),
        statistic=statistic,
        collection_started_at_ns=available_at_ns - 2 * SECOND_NS,
        collection_ended_at_ns=available_at_ns - SECOND_NS,
        produced_at_ns=available_at_ns - SECOND_NS,
        available_at_ns=available_at_ns,
        value=value,
        lexical_value=None if value is None else f"{value:.1f}%",
        unit="percent_mom",
        scale=1.0,
        base="1982-84=100",
        source=_source(adapter, digest=digest),
        limitations=("Fixture expectation has no production coverage claim.",),
    )


def _corpus() -> EconomicCalendarCorpusV1:
    prior_initial = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="a" * 64,
    )
    prior_revision = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME - DAY_NS,
        scheduled_for_ns=PRIOR_TIME,
        available_at_ns=TARGET_TIME - DAY_NS,
        stage=EconomicReleaseStage.REVISION,
        revision_sequence=1,
        supersedes_release_id=prior_initial.release_id,
        actual_value=1.1,
        digest="b" * 64,
    )
    late_prior_revision = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME + SECOND_NS,
        scheduled_for_ns=PRIOR_TIME,
        available_at_ns=TARGET_TIME + SECOND_NS,
        stage=EconomicReleaseStage.REVISION,
        revision_sequence=2,
        supersedes_release_id=prior_revision.release_id,
        actual_value=1.2,
        digest="c" * 64,
    )
    schedule = _release(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME - 20 * DAY_NS,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.SCHEDULED,
        revision_sequence=0,
        digest="d" * 64,
    )
    initial = _release(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=1,
        supersedes_release_id=schedule.release_id,
        actual_value=2.0,
        digest="e" * 64,
    )
    revision = _release(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME + 2 * SECOND_NS,
        scheduled_for_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME + 2 * SECOND_NS,
        stage=EconomicReleaseStage.REVISION,
        revision_sequence=2,
        supersedes_release_id=initial.release_id,
        actual_value=2.1,
        digest="f" * 64,
    )
    forecasts = (
        _forecast(
            kind=EconomicForecastKind.OBSERVED_CONSENSUS,
            available_at_ns=TARGET_TIME - 2 * DAY_NS,
            value=1.9,
            digest="1" * 64,
        ),
        _forecast(
            kind=EconomicForecastKind.MACHINE_PROJECTION,
            available_at_ns=TARGET_TIME - DAY_NS,
            value=1.8,
            digest="2" * 64,
        ),
        _forecast(
            kind=EconomicForecastKind.OBSERVED_CONSENSUS,
            available_at_ns=TARGET_TIME + SECOND_NS,
            value=2.0,
            digest="3" * 64,
        ),
    )
    return EconomicCalendarCorpusV1(
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        releases=(
            prior_initial,
            prior_revision,
            late_prior_revision,
            schedule,
            initial,
            revision,
        ),
        forecasts=forecasts,
        limitations=(
            "Completeness applies only to configured official-source evidence.",
        ),
    )


def _query(
    corpus: EconomicCalendarCorpusV1, decision_at_ns: int
) -> EconomicCalendarQueryV1:
    return query_economic_calendar_as_known(
        corpus,
        start_ns=TARGET_TIME,
        end_ns=TARGET_TIME + SECOND_NS,
        decision_at_ns=decision_at_ns,
        currencies=("usd",),
        symbols=("eurusd",),
        event_families=(EconomicEventFamily.INFLATION_PRICES,),
    )


def test_as_known_query_keeps_schedule_actual_and_revision_distinct() -> None:
    corpus = _corpus()

    scheduled = _query(corpus, TARGET_TIME - SECOND_NS).events[0]
    initial = _query(corpus, TARGET_TIME).events[0]
    revised = _query(corpus, TARGET_TIME + 3 * SECOND_NS).events[0]

    assert scheduled.release.stage is EconomicReleaseStage.INITIAL
    assert scheduled.release.status is EconomicReleaseStatus.SCHEDULED
    assert scheduled.actual_initial is None
    assert scheduled.previous_value == 1.1
    assert scheduled.observed_consensus is not None
    assert scheduled.observed_consensus.value == 1.9
    assert scheduled.machine_projection is not None
    assert scheduled.machine_projection.value == 1.8
    assert initial.release.stage is EconomicReleaseStage.INITIAL
    assert initial.actual_initial == 2.0
    assert initial.actual_latest == 2.0
    assert initial.previous_initial == 1.0
    assert initial.previous_latest == 1.1
    assert initial.observed_surprise == pytest.approx(0.1)
    assert initial.machine_surprise == pytest.approx(0.2)
    assert revised.release.stage is EconomicReleaseStage.REVISION
    assert revised.release.status is EconomicReleaseStatus.RELEASED
    assert revised.actual_initial == 2.0
    assert revised.actual_latest == 2.1


def test_previous_as_known_excludes_revision_published_after_release() -> None:
    state = _query(_corpus(), TARGET_TIME + 3 * SECOND_NS).events[0]

    assert state.previous_as_known is not None
    assert state.previous_as_known.revision_sequence == 1
    assert state.previous_as_known.actual_value == 1.1
    assert state.previous_as_known.available_at_ns < TARGET_TIME
    assert state.previous_initial == 1.0
    assert state.previous_latest == 1.2


def test_previous_as_known_never_backshifts_to_an_older_period() -> None:
    old = _release(
        logical_event_key="us.cpi.old-prior",
        reference_period="2023-10",
        reference_period_end_ns=PRIOR_TIME - 20 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=0.8,
        digest="c" * 64,
    )
    late_preceding = _release(
        logical_event_key="us.cpi.late-prior",
        reference_period="2023-11",
        reference_period_end_ns=TARGET_TIME - 20 * DAY_NS,
        event_time_ns=TARGET_TIME + 3_600 * SECOND_NS,
        available_at_ns=TARGET_TIME + 3_600 * SECOND_NS,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="d" * 64,
    )
    current = _release(
        logical_event_key="us.cpi.current",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.2,
        digest="e" * 64,
    )
    corpus = EconomicCalendarCorpusV1(
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=False,
        releases=(old, late_preceding, current),
        forecasts=(),
        limitations=("Immediate preceding period was not yet available.",),
    )

    state = corpus.as_known_at(
        start_ns=TARGET_TIME,
        end_ns=TARGET_TIME + SECOND_NS,
        decision_at_ns=TARGET_TIME,
    ).events[0]
    assert state.previous_as_known is None
    assert state.previous_initial is None
    assert state.previous_latest is None


def test_post_release_consensus_never_replaces_pre_release_vintage() -> None:
    state = _query(_corpus(), TARGET_TIME + 3 * SECOND_NS).events[0]

    assert state.observed_consensus is not None
    assert state.observed_consensus.value == 1.9
    assert state.observed_consensus.available_at_ns < TARGET_TIME


def test_forecast_available_at_release_boundary_is_not_admitted() -> None:
    corpus = _corpus()
    boundary = replace(
        corpus.forecasts[0],
        produced_at_ns=TARGET_TIME - SECOND_NS,
        available_at_ns=TARGET_TIME,
        value=2.5,
        source=_source("public-consensus-observation", digest="7" * 64),
        forecast_id="",
    )
    bounded = replace(
        corpus,
        forecasts=(*corpus.forecasts, boundary),
        corpus_id="",
    )

    state = _query(bounded, TARGET_TIME).events[0]

    assert state.observed_consensus is not None
    assert state.observed_consensus.value == 1.9


def test_query_rejects_intervals_outside_declared_coverage() -> None:
    corpus = _corpus()

    with pytest.raises(ValueError, match="outside corpus coverage"):
        query_economic_calendar_as_known(
            corpus,
            start_ns=COVERAGE_START - SECOND_NS,
            end_ns=COVERAGE_START + SECOND_NS,
            decision_at_ns=TARGET_TIME,
        )


def test_query_round_trip_and_provider_neutral_reader_protocol() -> None:
    corpus = _corpus()
    reader: EconomicCalendarAsKnownReaderV1 = corpus

    assert isinstance(reader, EconomicCalendarAsKnownReaderV1)
    query = reader.as_known_at(
        start_ns=TARGET_TIME,
        end_ns=TARGET_TIME + SECOND_NS,
        decision_at_ns=TARGET_TIME,
    )
    assert EconomicCalendarQueryV1.from_json(query.to_json()) == query
    assert query.coverage_start_ns == corpus.coverage_start_ns
    assert query.coverage_end_ns == corpus.coverage_end_ns
    assert query.corpus_complete is True


def test_two_official_adapters_build_the_same_neutral_contract() -> None:
    bls_release = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="4" * 64,
    )
    ons_release = replace(
        _release(
            logical_event_key="uk.cpi.2023-11",
            reference_period="2023-11",
            reference_period_end_ns=PRIOR_TIME - 9 * DAY_NS,
            event_time_ns=PRIOR_TIME + SECOND_NS,
            available_at_ns=PRIOR_TIME + SECOND_NS,
            stage=EconomicReleaseStage.INITIAL,
            revision_sequence=0,
            actual_value=0.4,
            adapter_name="ons-public-release",
            digest="5" * 64,
        ),
        series_key="uk.cpi.all-items.mom",
        economy="United Kingdom",
        currency="GBP",
        institution="Office for National Statistics",
        indicator_id="uk-cpi-all-items",
        source_series_id="ons-cpi-d7bt",
        affected_currencies=("GBP",),
        affected_symbols=("EURGBP", "GBPUSD"),
        release_id="",
    )
    corpus = build_economic_calendar_corpus(
        (
            StaticOfficialEconomicCalendarAdapterV1(
                adapter_name="bls-public-release",
                adapter_version="1.0",
                releases=(bls_release,),
            ),
            StaticOfficialEconomicCalendarAdapterV1(
                adapter_name="ons-public-release",
                adapter_version="1.0",
                releases=(ons_release,),
            ),
        ),
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        limitations=("Only two official fixture adapters are configured.",),
    )

    query = corpus.as_known_at(
        start_ns=PRIOR_TIME,
        end_ns=PRIOR_TIME + 2 * SECOND_NS,
        decision_at_ns=PRIOR_TIME + 2 * SECOND_NS,
    )

    assert len(query.events) == 2
    assert {item.release.source.adapter_name for item in query.events} == {
        "bls-public-release",
        "ons-public-release",
    }
    assert all(isinstance(item, type(query.events[0])) for item in query.events)


def test_market_context_projection_preserves_projection_provenance() -> None:
    state = _query(_corpus(), TARGET_TIME).events[0]

    projected = project_economic_calendar_state(
        state, pre_event_ns=SECOND_NS, post_event_ns=SECOND_NS
    )

    assert isinstance(projected, MarketContextEventV1)
    assert projected.actual_value == 2.0
    assert projected.previous_value == 1.1
    assert projected.expected_value == 1.9
    assert "projection:observed_consensus" in projected.tags


def test_official_market_context_event_bridges_without_provider_fields() -> (
    None
):
    source = _source("official-market-context", digest="6" * 64)
    event = MarketContextEventV1(
        canonical_key="official.cpi.2023-12",
        kind=MarketContextKind.MACRO_RELEASE,
        title="Official CPI release",
        source=source,
        source_event_time="2023-12-14T13:30:00+00:00",
        source_timezone="UTC",
        event_time_ns=1_702_560_600_000_000_000,
        first_known_at_ns=1_702_560_600_000_000_000,
        available_at_ns=1_702_560_600_000_000_000,
        pre_event_ns=0,
        post_event_ns=1,
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        confidence=1.0,
        precision=MarketContextPrecision.EXACT,
        limitations=("Official fixture only.",),
        vintage_id="official-initial",
        actual_value=2.0,
        value_unit="percent_mom",
    )

    release = economic_release_from_market_context(
        event,
        logical_event_key="us.cpi.2023-12",
        series_key="us.cpi.all-items.sa.mom",
        series_version="v1",
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="U.S. Bureau of Labor Statistics",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="cpi-all-items",
        source_series_id="cuur0000sa0",
        source_table_id="cpi-table-1",
        source_release_id="us.cpi.2023-12",
        source_request_id="official.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="percent_mom",
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.RELEASED,
        time_precision=EconomicTimePrecision.EXACT_SECOND,
        timezone_evidence="Official release timestamp uses UTC.",
        actual_lexical="2.0%",
    )

    assert release.stage is EconomicReleaseStage.INITIAL
    assert release.source is source
    assert release.actual_value == 2.0


def test_content_addressed_corpus_round_trip_and_tamper_rejection(
    tmp_path: Path,
) -> None:
    corpus = _corpus()
    artifact = write_economic_calendar_corpus(corpus, tmp_path)

    assert read_economic_calendar_corpus(artifact.path) == corpus
    assert replay_economic_calendar_corpus(artifact.path) == corpus
    path = Path(artifact.path)
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash differs"):
        read_economic_calendar_corpus(path)


def test_invalid_revision_chain_is_rejected() -> None:
    corpus = _corpus()
    broken = replace(
        corpus.releases[-1],
        supersedes_release_id="economic-calendar-release:sha256:" + "0" * 64,
        release_id="",
    )

    with pytest.raises(ValueError, match="does not supersede"):
        replace(
            corpus,
            releases=(*corpus.releases[:-1], broken),
            corpus_id="",
        )


def test_release_cannot_be_available_before_publication() -> None:
    corpus = _corpus()

    with pytest.raises(ValueError, match="publication follows"):
        replace(
            corpus.releases[1],
            first_observed_at_ns=PRIOR_TIME - SECOND_NS,
            available_at_ns=PRIOR_TIME - SECOND_NS,
            release_id="",
        )


def test_series_units_cannot_drift_between_reference_periods() -> None:
    first = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="8" * 64,
    )
    second = replace(
        _release(
            logical_event_key="us.cpi.2023-12",
            reference_period="2023-12",
            reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
            event_time_ns=TARGET_TIME,
            available_at_ns=TARGET_TIME,
            stage=EconomicReleaseStage.INITIAL,
            revision_sequence=0,
            actual_value=2.0,
            digest="9" * 64,
        ),
        unit="index_points",
        release_id="",
    )

    with pytest.raises(ValueError, match="series semantic identity drifts"):
        EconomicCalendarCorpusV1(
            coverage_start_ns=COVERAGE_START,
            coverage_end_ns=COVERAGE_END,
            complete=True,
            releases=(first, second),
            forecasts=(),
            limitations=("Unit-drift rejection fixture.",),
        )


def test_forecasts_require_matching_scale_and_unambiguous_slots() -> None:
    corpus = _corpus()
    wrong_scale = replace(corpus.forecasts[0], scale=100.0, forecast_id="")
    with pytest.raises(ValueError, match="scale/base differs"):
        replace(
            corpus,
            forecasts=(wrong_scale, *corpus.forecasts[1:]),
            corpus_id="",
        )

    duplicate_slot = replace(
        corpus.forecasts[0],
        value=1.85,
        source=_source("public-consensus-observation", digest="0" * 64),
        forecast_id="",
    )
    with pytest.raises(ValueError, match="ambiguous forecast"):
        replace(
            corpus,
            forecasts=(*corpus.forecasts, duplicate_slot),
            corpus_id="",
        )


def test_canonical_module_has_no_commercial_calendar_dependency() -> None:
    source = inspect.getsource(calendar_module).lower()

    assert "trading_economics" not in source
    assert "trading economics" not in source
    assert "api_key" not in source
    assert "import requests" not in source


def test_foundation_enums_cover_required_neutral_taxonomy() -> None:
    assert {
        "flash",
        "advance",
        "preliminary",
        "second",
        "final",
        "revision",
    } <= {item.value for item in EconomicReleaseStage}
    assert {
        "event_consensus",
        "official_professional_survey_event_target",
        "official_professional_survey_period_target",
        "central_bank_projection",
        "government_projection",
        "market_implied",
        "unavailable",
    } <= {item.value for item in EconomicForecastScope}
    assert {
        EconomicEventFamily.MONETARY_POLICY,
        EconomicEventFamily.INFLATION_PRICES,
        EconomicEventFamily.LABOUR_MARKET,
        EconomicEventFamily.GDP_NATIONAL_ACCOUNTS,
        EconomicEventFamily.RETAIL_CONSUMPTION,
        EconomicEventFamily.INDUSTRIAL_PRODUCTION,
        EconomicEventFamily.TRADE_EXTERNAL,
        EconomicEventFamily.HOUSING,
        EconomicEventFamily.CONFIDENCE_SURVEY,
    } <= set(EconomicEventFamily)
    assert EconomicForecastScope.EVENT_CONSENSUS.calendar_style_eligible
    assert (
        EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET.calendar_style_eligible
    )
    assert not EconomicForecastScope.MARKET_IMPLIED.calendar_style_eligible
    assert (
        not EconomicForecastScope.MACHINE_EVENT_TARGET.calendar_style_eligible
    )


def test_release_json_and_versioned_series_identity_round_trip() -> None:
    initial = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="4" * 64,
    )
    rebased = replace(
        _release(
            logical_event_key="us.cpi.2023-12",
            reference_period="2023-12",
            reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
            event_time_ns=TARGET_TIME,
            available_at_ns=TARGET_TIME,
            stage=EconomicReleaseStage.INITIAL,
            revision_sequence=0,
            actual_value=2.0,
            digest="5" * 64,
            series_version="v2",
            comparability_bridge_id="cpi-rebase-bridge-v1",
        ),
        source_series_id="cuur0000sa0-2024-base",
        base="2024=100",
        release_id="",
    )

    assert EconomicCalendarReleaseV1.from_json(initial.to_json()) == initial
    assert rebased.series_id != initial.series_id
    corpus = EconomicCalendarCorpusV1(
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        releases=(initial, rebased),
        forecasts=(),
        limitations=("Versioned rebase fixture.",),
    )
    assert corpus.releases[1].comparability_bridge_id == "cpi-rebase-bridge-v1"

    tampered = initial.to_dict()
    tampered["series_id"] = "economic-calendar-series:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="series_id"):
        EconomicCalendarReleaseV1.from_dict(tampered)


def test_unit_conversion_retains_raw_lexical_evidence() -> None:
    conversion = EconomicUnitConversionV1(
        source_value=0.02,
        source_unit="decimal_ratio",
        source_scale=1.0,
        source_base="1982-84=100",
        target_unit="percent_mom",
        target_scale=1.0,
        target_base="1982-84=100",
        multiplier=100.0,
        offset=0.0,
        raw_lexical="0.02",
        policy_version="decimal-to-percent-v1",
    )
    release = _release(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=2.0,
        actual_lexical="0.02",
        value_conversion=conversion,
        digest="6" * 64,
    )

    assert conversion.normalized_value == pytest.approx(2.0)
    assert (
        EconomicUnitConversionV1.from_json(conversion.to_json()) == conversion
    )
    assert EconomicCalendarReleaseV1.from_json(release.to_json()) == release
    with pytest.raises(ValueError, match="does not reproduce"):
        replace(
            release,
            actual_value=2.1,
            release_id="",
        )


def test_dst_fold_offsets_and_timezone_evidence_change_identity() -> None:
    first_text = "2023-11-05T01:30:00-04:00"
    second_text = "2023-11-05T01:30:00-05:00"
    first_ns = int(datetime.fromisoformat(first_text).timestamp()) * SECOND_NS
    second_ns = int(datetime.fromisoformat(second_text).timestamp()) * SECOND_NS
    first = _release(
        logical_event_key="us.cpi.dst-fold",
        reference_period="2023-10",
        reference_period_end_ns=first_ns - DAY_NS,
        event_time_ns=first_ns,
        available_at_ns=first_ns - DAY_NS,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.SCHEDULED,
        revision_sequence=0,
        source_timezone="America/New_York",
        scheduled_lexical=first_text,
        digest="7" * 64,
    )
    second = _release(
        logical_event_key="us.cpi.dst-fold",
        reference_period="2023-10",
        reference_period_end_ns=first_ns - DAY_NS,
        event_time_ns=second_ns,
        available_at_ns=first_ns - DAY_NS,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.SCHEDULED,
        revision_sequence=0,
        source_timezone="America/New_York",
        scheduled_lexical=second_text,
        digest="8" * 64,
    )

    assert second_ns - first_ns == 3_600 * SECOND_NS
    assert first.release_id != second.release_id
    with pytest.raises(ValueError, match="offset differs"):
        replace(first, source_timezone="UTC", release_id="")


def test_reschedule_is_an_immutable_visible_vintage() -> None:
    scheduled = _release(
        logical_event_key="us.cpi.rescheduled",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME - 20 * DAY_NS,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.SCHEDULED,
        revision_sequence=0,
        digest="9" * 64,
    )
    moved_time = TARGET_TIME + 3_600 * SECOND_NS
    moved = _release(
        logical_event_key="us.cpi.rescheduled",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=moved_time,
        scheduled_for_ns=moved_time,
        available_at_ns=TARGET_TIME - 10 * DAY_NS,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.RESCHEDULED,
        revision_sequence=1,
        supersedes_release_id=scheduled.release_id,
        schedule_change_reason="Official producer moved the publication time.",
        digest="a" * 64,
    )
    corpus = EconomicCalendarCorpusV1(
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        releases=(scheduled, moved),
        forecasts=(),
        limitations=("Reschedule fixture only.",),
    )

    before = corpus.as_known_at(
        start_ns=TARGET_TIME,
        end_ns=TARGET_TIME + 2 * 3_600 * SECOND_NS,
        decision_at_ns=TARGET_TIME - 15 * DAY_NS,
    )
    after = corpus.as_known_at(
        start_ns=TARGET_TIME,
        end_ns=TARGET_TIME + 2 * 3_600 * SECOND_NS,
        decision_at_ns=TARGET_TIME - 5 * DAY_NS,
    )
    assert before.events[0].release == scheduled
    assert after.events[0].release == moved
    assert after.events[0].release.status is EconomicReleaseStatus.RESCHEDULED
    with pytest.raises(ValueError, match="without reschedule status"):
        replace(
            corpus,
            releases=(
                scheduled,
                replace(
                    moved,
                    status=EconomicReleaseStatus.SCHEDULED,
                    schedule_change_reason=None,
                    release_id="",
                ),
            ),
            corpus_id="",
        )


def test_forecast_scope_unavailable_and_proxy_states_are_explicit() -> None:
    corpus = _corpus()
    unavailable = _forecast(
        kind=EconomicForecastKind.UNAVAILABLE,
        scope=EconomicForecastScope.UNAVAILABLE,
        statistic=EconomicForecastStatistic.UNAVAILABLE,
        available_at_ns=TARGET_TIME - 3 * DAY_NS,
        value=None,
        digest="b" * 64,
    )
    extended = replace(
        corpus,
        forecasts=(*corpus.forecasts, unavailable),
        corpus_id="",
    )

    assert (
        EconomicCalendarForecastV1.from_json(unavailable.to_json())
        == unavailable
    )
    assert (
        _query(extended, TARGET_TIME).events[0].observed_consensus is not None
    )
    with pytest.raises(ValueError, match="machine event scope"):
        replace(
            corpus.forecasts[0],
            scope=EconomicForecastScope.MACHINE_EVENT_TARGET,
            forecast_id="",
        )


def test_arrow_round_trip_rejects_projection_and_schema_tampering() -> None:
    corpus = _corpus()
    table = economic_calendar_corpus_to_arrow(corpus)

    assert economic_calendar_corpus_from_arrow(table) == corpus
    assert table.num_rows == len(corpus.releases) + len(corpus.forecasts)
    record_ids = table.column("record_id").to_pylist()
    record_ids[0] = "tampered-record-id"
    tampered = table.set_column(
        table.schema.get_field_index("record_id"),
        table.schema.field("record_id"),
        pa.array(record_ids, type=pa.string()),
    )
    with pytest.raises(ValueError, match="release projection differs"):
        economic_calendar_corpus_from_arrow(tampered)
    with pytest.raises(ValueError, match="field schema differs"):
        economic_calendar_corpus_from_arrow(
            table.rename_columns(
                [
                    "record_kind",
                    *table.column_names[1:],
                ]
            )
        )


def test_surprise_policy_uses_only_strictly_prior_releases() -> None:
    releases: list[EconomicCalendarReleaseV1] = []
    forecasts: list[EconomicCalendarForecastV1] = []
    actual_values = (1.0, 3.0, 5.0, 100.0, 200.0)
    forecast_values = (0.0, 2.0, 4.0, 0.0, 0.0)
    for index, (actual, forecast) in enumerate(
        zip(actual_values, forecast_values, strict=True)
    ):
        event_time = PRIOR_TIME + min(index, 3) * DAY_NS
        logical_key = f"us.cpi.robust-{index}"
        releases.append(
            _release(
                logical_event_key=logical_key,
                reference_period=f"2023-{index + 1:02d}",
                reference_period_end_ns=(
                    PRIOR_TIME - 10 * DAY_NS + index * DAY_NS
                ),
                event_time_ns=event_time,
                available_at_ns=event_time,
                stage=EconomicReleaseStage.INITIAL,
                revision_sequence=0,
                actual_value=actual,
                digest=str(index) * 64,
            )
        )
        forecasts.append(
            _forecast(
                logical_event_key=logical_key,
                kind=EconomicForecastKind.OBSERVED_CONSENSUS,
                available_at_ns=event_time - SECOND_NS,
                value=forecast,
                digest=str(index + 4) * 64,
            )
        )
    corpus = EconomicCalendarCorpusV1(
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        releases=tuple(releases),
        forecasts=tuple(forecasts),
        limitations=("Robust surprise fixture.",),
    )
    query = corpus.as_known_at(
        start_ns=PRIOR_TIME,
        end_ns=PRIOR_TIME + 4 * DAY_NS,
        decision_at_ns=TARGET_TIME,
    )
    policy = EconomicSurprisePolicyV1(
        policy_version="cpi-direction-v1",
        direction_by_series=(("us.cpi.all-items.sa.mom", -1),),
        scale_window=3,
        minimum_observations=3,
        epsilon=0.5,
    )

    surprises = compute_economic_calendar_surprises(
        query.events,
        policy=policy,
    )
    assert [item.prior_support for item in surprises] == [0, 1, 2, 3, 3]
    assert [item.robust_z for item in surprises[:3]] == [None, None, None]
    assert surprises[-2].raw_surprise == 100.0
    assert surprises[-2].robust_z == pytest.approx(200.0)
    assert surprises[-1].raw_surprise == 200.0
    assert surprises[-1].directional_surprise == -200.0
    assert surprises[-1].robust_scale == pytest.approx(0.5)
    assert surprises[-1].robust_z == pytest.approx(400.0)
    assert EconomicSurprisePolicyV1.from_json(policy.to_json()) == policy
    assert (
        EconomicCalendarSurpriseV1.from_json(surprises[-1].to_json())
        == surprises[-1]
    )
    assert (
        EconomicCalendarEventStateV1.from_json(query.events[-1].to_json())
        == query.events[-1]
    )
