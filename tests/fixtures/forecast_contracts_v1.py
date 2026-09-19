"""Offline, synthetic, provenance-complete forecast contract fixtures."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from histdatacom.forecasting import (
    ConsensusReferenceV1,
    ForecastCutoffV1,
    ForecastDistributionV1,
    ForecastHorizon,
    ForecastModelIdentityV1,
    ForecastSnapshotV1,
    ForecastTargetKind,
    ForecastTargetV1,
    capture_forecast_inputs,
)
from histdatacom.forecasting.contracts import DAY_NS, SECOND_NS
from histdatacom.market_context.contracts import (
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
)
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
    EconomicCalendarForecastV1,
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicForecastKind,
    EconomicForecastScope,
    EconomicForecastStatistic,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicTimePrecision,
)
from histdatacom.runtime_contracts import JSONValue

RELEASE_TIME = 1_700_000_000 * SECOND_NS


def nested_forecast_state(depth: int) -> dict[str, JSONValue]:
    """Return a JSON object whose scalar leaf is exactly depth levels down."""
    value: JSONValue = 0
    for _ in range(depth):
        value = {"nested": value}
    assert isinstance(value, dict)
    return value


def utc_text(value: int) -> str:
    return datetime.fromtimestamp(value / SECOND_NS, timezone.utc).isoformat()


def source(digest: str, *, consensus: bool = False) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name=(
            "Institutional consensus fixture"
            if consensus
            else "Official release fixture"
        ),
        source_version="synthetic-v1",
        retrieved_at_ns=RELEASE_TIME + 500 * DAY_NS,
        content_sha256=digest * 64,
        adapter_name=(
            "forecast-consensus-fixture"
            if consensus
            else "forecast-release-fixture"
        ),
        adapter_version="1.0",
        license_name="Synthetic test data",
        redistribution_allowed=True,
        redistribution_constraints=(),
        limitations=("Synthetic evidence, not production coverage.",),
        metadata={"fixture": {"mutable_at_origin": True}},
    )


def calendar_fixture(
    *,
    index: int = 0,
    actual: float = 3.0,
    revision: float = 8.0,
    early_consensus: float = 2.0,
    late_consensus: float = 9.0,
    availability_delay_ns: int = 0,
) -> EconomicCalendarCorpusV1:
    event_time = RELEASE_TIME + index * 40 * DAY_NS
    key = f"fixture.cpi.event-{index}"
    schedule = EconomicCalendarReleaseV1(
        logical_event_key=key,
        series_key="fixture.cpi.mom",
        series_version="v1",
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="Official fixture agency",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="cpi-mom",
        source_series_id="cpi-mom",
        source_table_id="table-1",
        source_release_id=key,
        source_request_id=f"request-{index}",
        title="Synthetic CPI",
        reference_period=f"period-{index}",
        reference_period_end_ns=event_time - 10 * DAY_NS,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="percent_mom",
        scale=1.0,
        base=None,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.SCHEDULED,
        scheduled_for_ns=event_time,
        scheduled_lexical=utc_text(event_time),
        released_at_ns=None,
        released_lexical=None,
        first_observed_at_ns=event_time - 40 * DAY_NS,
        available_at_ns=event_time - 40 * DAY_NS,
        source_timezone="UTC",
        timezone_evidence="Synthetic UTC clock.",
        time_precision=EconomicTimePrecision.EXACT_SECOND,
        precision=MarketContextPrecision.EXACT,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=source("a"),
        affected_currencies=("USD",),
        affected_symbols=(),
        limitations=("Synthetic fixture only.",),
    )
    initial = replace(
        schedule,
        status=EconomicReleaseStatus.RELEASED,
        released_at_ns=event_time,
        released_lexical=utc_text(event_time),
        first_observed_at_ns=event_time + availability_delay_ns,
        available_at_ns=event_time + availability_delay_ns,
        actual_value=actual,
        actual_lexical=str(actual),
        revision_sequence=1,
        supersedes_release_id=schedule.release_id,
        source=source("b"),
        release_id="",
    )
    revised = replace(
        initial,
        stage=EconomicReleaseStage.REVISION,
        released_at_ns=event_time + 30 * DAY_NS,
        released_lexical=utc_text(event_time + 30 * DAY_NS),
        first_observed_at_ns=event_time + 30 * DAY_NS,
        available_at_ns=event_time + 30 * DAY_NS,
        actual_value=revision,
        actual_lexical=str(revision),
        revision_sequence=2,
        supersedes_release_id=initial.release_id,
        source=source("c"),
        release_id="",
    )

    def consensus(
        value: float, days: int, digest: str
    ) -> EconomicCalendarForecastV1:
        available = event_time - days * DAY_NS
        return EconomicCalendarForecastV1(
            logical_event_key=key,
            kind=EconomicForecastKind.OBSERVED_CONSENSUS,
            scope=EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET,
            statistic=EconomicForecastStatistic.MEDIAN,
            collection_started_at_ns=available - DAY_NS,
            collection_ended_at_ns=available - SECOND_NS,
            produced_at_ns=available - SECOND_NS,
            available_at_ns=available,
            value=value,
            lexical_value=str(value),
            unit="percent_mom",
            scale=1.0,
            base=None,
            source=source(digest, consensus=True),
            limitations=("Synthetic consensus with a visible vintage clock.",),
        )

    return EconomicCalendarCorpusV1(
        coverage_start_ns=event_time - 41 * DAY_NS,
        coverage_end_ns=event_time + 40 * DAY_NS,
        complete=False,
        releases=(schedule, initial, revised),
        forecasts=(
            consensus(early_consensus, 8, "d"),
            consensus(late_consensus, 2, "e"),
        ),
        limitations=("Synthetic point-in-time fixture only.",),
    )


def reference_fixture(cutoff: int) -> ConsensusReferenceV1:
    return ConsensusReferenceV1(
        source_name="Institutional consensus fixture",
        adapter_name="forecast-consensus-fixture",
        scope=EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET,
        statistic=EconomicForecastStatistic.MEDIAN,
        target_at_ns=cutoff,
    )


def snapshot_fixture(
    corpus: EconomicCalendarCorpusV1 | None = None,
    *,
    point: float = 2.5,
    kind: ForecastTargetKind = ForecastTargetKind.FIRST_RELEASE_ACTUAL,
    with_reference: bool = True,
) -> ForecastSnapshotV1:
    corpus = corpus or calendar_fixture()
    schedule = corpus.releases[0]
    event_time = schedule.scheduled_for_ns
    cutoff = event_time - 7 * DAY_NS
    inputs = capture_forecast_inputs(corpus, cutoff_at_ns=cutoff)
    training = capture_forecast_inputs(
        corpus, cutoff_at_ns=event_time - 10 * DAY_NS
    )
    target = ForecastTargetV1(
        kind=kind,
        logical_event_key=schedule.logical_event_key,
        series_id=schedule.series_id,
        indicator_id=schedule.indicator_id,
        reference_period=schedule.reference_period,
        unit=schedule.unit,
        scale=schedule.scale,
        base=schedule.base,
        consensus_reference=(
            reference_fixture(cutoff) if with_reference else None
        ),
    )
    return ForecastSnapshotV1(
        cutoff=ForecastCutoffV1(
            ForecastHorizon.T7D, cutoff, event_time, schedule.release_id
        ),
        target=target,
        inputs=inputs,
        model=ForecastModelIdentityV1(
            "fixture-model",
            "1.0.0",
            "f" * 64,
            '{"algorithm":"constant","parameters":{"value":2.5}}',
            training,
            event_time - 9 * DAY_NS,
        ),
        distribution=ForecastDistributionV1(((point, 1.0),)),
        generated_at_ns=cutoff,
    )
