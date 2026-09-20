"""Installed synthetic evidence factory, independent of the test package."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone

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

from .benchmark_contracts import (
    BenchmarkScenario,
    ForecastBenchmarkCaseV1,
    ForecastBenchmarkSuiteV1,
)
from .contracts import (
    DAY_NS,
    HOUR_NS,
    SECOND_NS,
    ConsensusReferenceV1,
    ConsensusTiming,
    ForecastCutoffV1,
    ForecastHorizon,
    ForecastTargetKind,
    ForecastTargetV1,
    RevisionMeasure,
    SurpriseReference,
)
from .feature_contracts import (
    FeatureColumnV1,
    FeatureDefinitionV1,
    FeatureEvidenceV1,
    FeatureKind,
    FeatureObservationV1,
    FeaturePeriodV1,
    FeatureRequestV1,
    FeatureSourceMode,
)
from .feature_forecasts import (
    ForecastFeatureInputsV1,
    capture_forecast_feature_inputs,
)
from .feature_store import VintageFeatureStoreV1
from .math_verification import math_json, math_load

BENCHMARK_RELEASE_NS = 1_700_000_000 * SECOND_NS
CONSENSUS_NAME = "Synthetic institutional benchmark"
CONSENSUS_ADAPTER = "synthetic-benchmark-consensus"


def _utc(value: int) -> str:
    return datetime.fromtimestamp(value / SECOND_NS, timezone.utc).isoformat()


def _source(
    case_id: str, tag: str, *, consensus: bool = False
) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name=CONSENSUS_NAME if consensus else "Synthetic release benchmark",
        source_version="benchmark-fixture-v1",
        retrieved_at_ns=BENCHMARK_RELEASE_NS + 500 * DAY_NS,
        content_sha256=hashlib.sha256(
            f"synthetic:{case_id}:{tag}".encode()
        ).hexdigest(),
        adapter_name=(
            CONSENSUS_ADAPTER if consensus else "synthetic-benchmark-release"
        ),
        adapter_version="1.0.0",
        license_name="Synthetic benchmark data",
        redistribution_allowed=True,
        redistribution_constraints=(),
        limitations=(
            "Invented fixture, not an official source or coverage claim.",
        ),
        metadata={"synthetic_case": case_id, "fixture_record": tag},
    )


@dataclass(frozen=True, slots=True)
class ForecastBenchmarkEvidenceV1:
    """Typed input data, not a certificate or caller-supplied success flag."""

    corpus: EconomicCalendarCorpusV1
    observations: tuple[FeatureObservationV1, ...]
    periods: tuple[FeaturePeriodV1, ...]
    cutoff: ForecastCutoffV1
    target: ForecastTargetV1

    def inputs(self, cutoff_at_ns: int) -> ForecastFeatureInputsV1:
        matrix = VintageFeatureStoreV1(self.observations).snapshot(
            FeatureRequestV1(
                cutoff_at_ns,
                self.periods,
                (FeatureColumnV1("history", "benchmark.history"),),
            )
        )
        return capture_forecast_feature_inputs(
            self.corpus,
            matrix,
            calendar_event_keys=(self.target.logical_event_key,),
            calendar_coverage_start_ns=BENCHMARK_RELEASE_NS - 150 * DAY_NS,
            calendar_coverage_end_ns=BENCHMARK_RELEASE_NS + 40 * DAY_NS,
        )


def build_forecast_benchmark_evidence(
    suite: ForecastBenchmarkSuiteV1, case: ForecastBenchmarkCaseV1
) -> ForecastBenchmarkEvidenceV1:
    """Construct all source vintages; capture later excludes unavailable ones."""
    if type(suite) is not ForecastBenchmarkSuiteV1 or case not in suite.cases:
        raise ValueError("case is not in the exact benchmark suite")
    data = math_load(suite.source_json)
    event = BENCHMARK_RELEASE_NS
    leads = {
        ForecastHorizon.T30D: 30 * DAY_NS,
        ForecastHorizon.T7D: 7 * DAY_NS,
        ForecastHorizon.T1D: DAY_NS,
        ForecastHorizon.T1H: HOUR_NS,
        ForecastHorizon.FINAL_PRE_RELEASE: SECOND_NS,
    }
    cutoff_at = event - leads[case.horizon]
    key = f"benchmark.{case.case_id}"
    schedule_known = (
        event
        - (5 if case.scenario is BenchmarkScenario.LATE_SCHEDULE else 140)
        * DAY_NS
    )
    schedule = EconomicCalendarReleaseV1(
        logical_event_key=key,
        series_key="benchmark.cpi.mom",
        series_version="v1",
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="Synthetic benchmark institution",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="benchmark-cpi-mom",
        source_series_id="benchmark-cpi-mom",
        source_table_id="fixture-table",
        source_release_id=key,
        source_request_id=key,
        title="Invented benchmark release",
        reference_period="benchmark-period",
        reference_period_end_ns=event - 10 * DAY_NS,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="percent_mom",
        scale=1.0,
        base=None,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.SCHEDULED,
        scheduled_for_ns=event,
        scheduled_lexical=_utc(event),
        released_at_ns=None,
        released_lexical=None,
        first_observed_at_ns=schedule_known,
        available_at_ns=schedule_known,
        source_timezone="UTC",
        timezone_evidence="Invented exact UTC clock.",
        time_precision=EconomicTimePrecision.EXACT_SECOND,
        precision=MarketContextPrecision.EXACT,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=_source(case.case_id, "schedule"),
        affected_currencies=("USD",),
        affected_symbols=(),
        limitations=("Synthetic benchmark only.",),
    )
    published = (
        event - 2 * HOUR_NS
        if case.scenario is BenchmarkScenario.DELAYED_ACTUAL
        else event
    )
    available = (
        event + HOUR_NS
        if case.scenario is BenchmarkScenario.DELAYED_ACTUAL
        else event
    )
    initial = replace(
        schedule,
        status=EconomicReleaseStatus.RELEASED,
        released_at_ns=published,
        released_lexical=_utc(published),
        first_observed_at_ns=available,
        available_at_ns=available,
        actual_value=data["actual"],
        actual_lexical=str(data["actual"]),
        revision_sequence=1,
        supersedes_release_id=schedule.release_id,
        source=_source(case.case_id, "initial"),
        release_id="",
    )
    revised = replace(
        initial,
        stage=EconomicReleaseStage.REVISION,
        released_at_ns=event + 30 * DAY_NS,
        released_lexical=_utc(event + 30 * DAY_NS),
        first_observed_at_ns=event + 30 * DAY_NS,
        available_at_ns=event + 30 * DAY_NS,
        actual_value=data["revision"],
        actual_lexical=str(data["revision"]),
        revision_sequence=2,
        supersedes_release_id=initial.release_id,
        source=_source(case.case_id, "revised"),
        release_id="",
    )

    def survey(value: float, days: int, tag: str) -> EconomicCalendarForecastV1:
        known = event - days * DAY_NS
        return EconomicCalendarForecastV1(
            logical_event_key=key,
            kind=EconomicForecastKind.OBSERVED_CONSENSUS,
            scope=EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET,
            statistic=EconomicForecastStatistic.MEDIAN,
            collection_started_at_ns=known - DAY_NS,
            collection_ended_at_ns=known - SECOND_NS,
            produced_at_ns=known - SECOND_NS,
            available_at_ns=known,
            value=value,
            lexical_value=str(value),
            unit="percent_mom",
            scale=1.0,
            base=None,
            source=_source(case.case_id, tag, consensus=True),
            limitations=(
                "Invented survey, not an independently qualified consensus.",
            ),
        )

    forecasts = (
        ()
        if case.scenario is BenchmarkScenario.MISSING_CONSENSUS
        else (
            survey(data["early_consensus"], 35, "early"),
            survey(data["late_consensus"], 2, "late"),
        )
    )
    corpus = EconomicCalendarCorpusV1(
        event - 150 * DAY_NS,
        event + 40 * DAY_NS,
        False,
        (schedule, initial, revised),
        forecasts,
        ("Synthetic contract benchmark, not empirical data.",),
    )
    periods = tuple(
        FeaturePeriodV1(
            f"history-{i}",
            event - (130 - 30 * i) * DAY_NS,
            event - (100 - 30 * i) * DAY_NS,
        )
        for i in range(3)
    )
    history_key = (
        "consensus_history"
        if case.target_kind is ForecastTargetKind.CONSENSUS
        else "actual_history"
    )
    observations = []
    for index, period in enumerate(periods):
        if (
            case.scenario is BenchmarkScenario.INSUFFICIENT_SUPPORT
            and index == 0
        ):
            continue
        known = period.end_ns + DAY_NS
        record = math_json(
            {
                "case": case.case_id,
                "kind": history_key,
                "period": period.label,
                "value": data[history_key][index],
            }
        )
        definition = FeatureDefinitionV1(
            "benchmark.history",
            (
                FeatureKind.MACRO
                if history_key == "actual_history"
                else FeatureKind.CONSENSUS
            ),
            "percent_mom",
            1.0,
            "seasonally-adjusted",
            "synthetic-v1",
            "monthly",
            period.start_ns,
        )
        evidence = FeatureEvidenceV1(
            "Synthetic benchmark history",
            "https://example.invalid/benchmark",
            hashlib.sha256(record.encode()).hexdigest(),
            f"{key}:{index}",
            "benchmark-fixture-history",
            "1.0.0",
            "Invented first availability.",
            event + 500 * DAY_NS,
            FeatureSourceMode.HISTORICAL_VINTAGE,
            record,
        )
        observations.append(
            FeatureObservationV1(
                definition,
                period,
                (
                    None
                    if case.scenario is BenchmarkScenario.MISSING_FEATURE
                    and index == 0
                    else data[history_key][index]
                ),
                known,
                period.end_ns,
                0,
                None,
                evidence,
            )
        )
    reference = ConsensusReferenceV1(
        CONSENSUS_NAME,
        CONSENSUS_ADAPTER,
        EconomicForecastScope.OFFICIAL_PROFESSIONAL_SURVEY_EVENT_TARGET,
        EconomicForecastStatistic.MEDIAN,
        (
            event - DAY_NS
            if case.scenario is BenchmarkScenario.FUTURE_CONSENSUS
            else cutoff_at
        ),
        (
            ConsensusTiming.FUTURE_EVOLUTION
            if case.scenario is BenchmarkScenario.FUTURE_CONSENSUS
            else ConsensusTiming.AT_CUTOFF
        ),
    )
    target = ForecastTargetV1(
        case.target_kind,
        key,
        schedule.series_id,
        schedule.indicator_id,
        schedule.reference_period,
        schedule.unit,
        2.0 if case.scenario is BenchmarkScenario.SEMANTIC_MISMATCH else 1.0,
        None,
        None if case.target_kind is ForecastTargetKind.REVISION else reference,
        2 if case.target_kind is ForecastTargetKind.REVISION else None,
        (
            (
                RevisionMeasure.CHANGE_FROM_FIRST
                if case.scenario is BenchmarkScenario.REVISION_CHANGE
                else RevisionMeasure.LEVEL
            )
            if case.target_kind is ForecastTargetKind.REVISION
            else None
        ),
        (
            SurpriseReference.OBSERVED_CONSENSUS
            if case.target_kind is ForecastTargetKind.SURPRISE
            else None
        ),
    )
    cutoff = ForecastCutoffV1(
        case.horizon,
        cutoff_at,
        event,
        schedule.release_id,
        (
            SECOND_NS
            if case.horizon is ForecastHorizon.FINAL_PRE_RELEASE
            else None
        ),
    )
    return ForecastBenchmarkEvidenceV1(
        corpus, tuple(observations), periods, cutoff, target
    )
