"""Test selection only; the complete benchmark factory is installed code."""

from histdatacom.forecasting import (
    build_forecast_benchmark_evidence,
    default_forecast_benchmark_suite,
)


def benchmark_case(case_id="actual-2"):
    suite = default_forecast_benchmark_suite()
    case = next(c for c in suite.cases if c.case_id == case_id)
    return suite, case, build_forecast_benchmark_evidence(suite, case)
