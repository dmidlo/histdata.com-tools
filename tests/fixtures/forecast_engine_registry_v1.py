"""Distinct deterministic reference cases, never historical skill evidence."""

from __future__ import annotations

from histdatacom.forecasting.engine_runner import (
    ForecastEngineComparisonV1,
    ForecastEngineRunnerV1,
    REFERENCE_BLEND,
    default_forecast_registry,
)
from histdatacom.forecasting.contracts import DAY_NS
from tests.fixtures.forecast_contracts_v1 import RELEASE_TIME, calendar_fixture
from tests.fixtures.forecast_feature_store_v1 import feature_forecast_fixture


def engine_comparison_fixture() -> ForecastEngineComparisonV1:
    snapshot = feature_forecast_fixture()
    return ForecastEngineRunnerV1(default_forecast_registry()).compare(
        REFERENCE_BLEND,
        snapshot.model.training_inputs,
        snapshot.inputs,
        column="cpi",
        cutoff=snapshot.cutoff,
        target=snapshot.target,
        trained_at_ns=snapshot.model.trained_at_ns,
        generated_at_ns=snapshot.generated_at_ns,
        outcomes=calendar_fixture(),
        scored_at_ns=RELEASE_TIME + 40 * DAY_NS,
    )
