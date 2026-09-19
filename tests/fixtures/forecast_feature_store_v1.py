"""Offline feature-store evidence, including genuine normalized calendar input."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

from histdatacom.forecasting import (
    FeatureColumnV1,
    FeatureDefinitionV1,
    FeatureEvidenceV1,
    FeatureKind,
    FeatureObservationV1,
    FeaturePeriodV1,
    FeatureRequestV1,
    FeatureSourceMode,
    ForecastFeatureInputsV1,
    ForecastFeatureSnapshotV1,
    VintageFeatureStoreV1,
    capture_forecast_feature_inputs,
    forecast_feature_baseline,
    train_feature_baseline,
)
from histdatacom.forecasting.contracts import DAY_NS
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
)
from histdatacom.synthetic.bars import ActivitySliceScope, DerivedBarV1
from tests.fixtures.forecast_contracts_v1 import (
    RELEASE_TIME,
    calendar_fixture,
    snapshot_fixture,
)

CUTOFF = RELEASE_TIME - 7 * DAY_NS
PERIODS = tuple(
    FeaturePeriodV1(
        f"month-{index}",
        RELEASE_TIME - (40 - index * 10) * DAY_NS,
        RELEASE_TIME - (30 - index * 10) * DAY_NS,
    )
    for index in range(3)
)


def evidence(
    identity: str,
    *,
    mode: FeatureSourceMode = FeatureSourceMode.HISTORICAL_VINTAGE,
    record_json: str | None = None,
) -> FeatureEvidenceV1:
    return FeatureEvidenceV1(
        "Synthetic macro provider",
        "https://example.invalid/source",
        hashlib.sha256(identity.encode()).hexdigest(),
        identity,
        "synthetic-feature-fixture",
        "1.0",
        "Synthetic exact first-observed publication clock.",
        RELEASE_TIME + 100 * DAY_NS,
        mode,
        record_json or json.dumps({"record": identity}),
    )


def observation(
    index: int,
    *,
    key: str = "macro.cpi",
    value: float | None = None,
    kind: FeatureKind = FeatureKind.MACRO,
    available_at_ns: int | None = None,
) -> FeatureObservationV1:
    period = PERIODS[index]
    available = (
        period.end_ns + DAY_NS if available_at_ns is None else available_at_ns
    )
    definition = FeatureDefinitionV1(
        key,
        kind,
        "percent_mom",
        1.0,
        "seasonally-adjusted",
        "synthetic-v1",
        "monthly",
        period.start_ns,
    )
    return FeatureObservationV1(
        definition,
        period,
        float(index + 1) if value is None else value,
        available,
        period.end_ns,
        0,
        None,
        evidence(f"{key}-{index}"),
    )


def revision(
    previous: FeatureObservationV1,
    *,
    value: float = 99.0,
    available_at_ns: int = CUTOFF + DAY_NS,
) -> FeatureObservationV1:
    return replace(
        previous,
        value=value,
        available_at_ns=available_at_ns,
        published_at_ns=available_at_ns,
        vintage_sequence=previous.vintage_sequence + 1,
        supersedes_id=previous.observation_id,
        evidence=evidence(f"revision-{previous.observation_id}-{value}"),
    )


def store_fixture() -> VintageFeatureStoreV1:
    return VintageFeatureStoreV1(
        tuple(observation(index) for index in range(3))
    )


def inputs_fixture(
    *,
    cutoff: int = CUTOFF,
    store: VintageFeatureStoreV1 | None = None,
    corpus: EconomicCalendarCorpusV1 | None = None,
) -> ForecastFeatureInputsV1:
    store = store or store_fixture()
    corpus = corpus or calendar_fixture()
    matrix = store.snapshot(
        FeatureRequestV1(
            cutoff, PERIODS, (FeatureColumnV1("cpi", "macro.cpi"),)
        )
    )
    return capture_forecast_feature_inputs(
        corpus,
        matrix,
        calendar_event_keys=("fixture.cpi.event-0",),
        calendar_coverage_start_ns=RELEASE_TIME - 50 * DAY_NS,
        calendar_coverage_end_ns=RELEASE_TIME + 50 * DAY_NS,
    )


def feature_forecast_fixture() -> ForecastFeatureSnapshotV1:
    calendar_only = snapshot_fixture()
    model = train_feature_baseline(
        inputs_fixture(cutoff=CUTOFF - DAY_NS),
        column="cpi",
        trained_at_ns=CUTOFF - DAY_NS,
    )
    return forecast_feature_baseline(
        model,
        inputs_fixture(),
        cutoff=calendar_only.cutoff,
        target=calendar_only.target,
        generated_at_ns=CUTOFF,
    )


def observed_bar_fixture() -> DerivedBarV1:
    minute = 60 * 1_000_000_000
    start = (CUTOFF // minute - 1) * minute
    return DerivedBarV1(
        source_product_manifest_id="fixture-product",
        policy_id="fixture-policy",
        rounding_digits=8,
        run_id="fixture-run",
        ensemble_member_id="fixture-member",
        symbol="EURUSD",
        scope=ActivitySliceScope.OBSERVED,
        interval_code="1m",
        interval_ns=minute,
        bar_start_ns=start,
        bar_end_ns=start + minute,
        first_event_id="event-1",
        last_event_id="event-1",
        first_event_time_ns=start + 1,
        last_event_time_ns=start + 1,
        event_count=1,
        observed_event_count=1,
        synthetic_event_count=0,
        quote_update_count=1,
        transition_count=0,
        bid_open=1.1,
        bid_high=1.1,
        bid_low=1.1,
        bid_close=1.1,
        ask_open=1.2,
        ask_high=1.2,
        ask_low=1.2,
        ask_close=1.2,
        mid_open=1.15,
        mid_high=1.15,
        mid_low=1.15,
        mid_close=1.15,
        spread_open=0.1,
        spread_high=0.1,
        spread_low=0.1,
        spread_close=0.1,
        mean_spread=0.1,
        activity_duration_ns=0,
        tick_intensity_per_second=None,
        price_change_count=0,
        stale_quote_count=0,
        stale_quote_rate=None,
        mean_event_confidence=None,
        confidence_support_count=0,
        is_partial_start=False,
        is_partial_end=False,
        source_version_ids=("observed-source-v1",),
        event_content_sha256="a" * 64,
    )
