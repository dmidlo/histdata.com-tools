"""Frozen suite inventory, semantic succession and hostile wire boundaries."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.forecasting import (
    ForecastBenchmarkCaseV1,
    ForecastBenchmarkSuiteV1,
    ForecastHorizon,
    ForecastTargetKind,
    default_forecast_benchmark_suite,
)
from histdatacom.forecasting import benchmark_contracts as contracts
from histdatacom.forecasting.math_verification import math_json


@pytest.mark.parametrize("field", ["comparators", "expected_points"])
def test_case_factory_refuses_oversized_arrays_before_decoding_members(field):
    case = default_forecast_benchmark_suite().cases[0].to_dict()
    definition = {
        key: value
        for key, value in case.items()
        if key not in {"id", "schema_version", "contract_type"}
    }
    definition[field] = ["invalid, deliberately not decoded"] * 7
    with pytest.raises(ValueError, match="collections exceed bounds"):
        ForecastBenchmarkCaseV1.from_definition(definition)


def test_frozen_coverage_has_every_horizon_objective_and_precise_comparators():
    suite = default_forecast_benchmark_suite()
    assert len(suite.cases) == 20
    ordinary = [c for c in suite.cases if c.scenario.value == "ordinary"]
    assert {(c.horizon, c.target_kind) for c in ordinary} == {
        (h, k)
        for h in ForecastHorizon
        for k in (
            ForecastTargetKind.CONSENSUS,
            ForecastTargetKind.FIRST_RELEASE_ACTUAL,
        )
    }
    assert all(
        {
            "historical-mean",
            "historical-median",
            "equal-forecast-mean",
            "forecast-median",
        }
        <= {k.value for k in c.comparators}
        for c in ordinary
    )
    assert ForecastBenchmarkSuiteV1.from_json(suite.to_json()) == suite
    assert suite.source_json == math_json(json.loads(suite.source_json))


@pytest.mark.parametrize(
    "mutation",
    [
        "remove",
        "duplicate",
        "reorder",
        "new-root",
        "changed-point",
        "extra-wire",
        "unknown-case-field",
    ],
)
def test_root_definition_and_exact_wire_cannot_silently_change(mutation):
    suite = default_forecast_benchmark_suite()
    with pytest.raises(ValueError):
        if mutation == "remove":
            replace(suite, cases=suite.cases[:-1])
        elif mutation == "duplicate":
            replace(suite, cases=suite.cases + (suite.cases[-1],))
        elif mutation == "reorder":
            replace(suite, cases=tuple(reversed(suite.cases)))
        elif mutation == "new-root":
            replace(suite, version="1.1.0")
        elif mutation == "changed-point":
            changed = replace(suite.cases[0], expected_target=999.0)
            replace(suite, cases=(changed,) + suite.cases[1:])
        else:
            wire = suite.to_dict()
            if mutation == "extra-wire":
                wire["unknown"] = "not ignored"
            else:
                wire["cases"][0]["unknown"] = "not ignored"
            ForecastBenchmarkSuiteV1.from_dict(wire)


def test_successor_retains_exact_ancestry_and_requires_semantic_version_bumps():
    suite = default_forecast_benchmark_suite()
    extra = replace(suite.cases[0], case_id="zz-additional-case")
    with pytest.raises(ValueError, match="SemVer"):
        suite.successor(version="1.0.1", cases=suite.cases + (extra,))
    successor = suite.successor(version="1.1.0", cases=suite.cases + (extra,))
    assert json.loads(successor.predecessor_json)["id"] == suite.suite_id
    assert ForecastBenchmarkSuiteV1.from_json(successor.to_json()) == successor
    changed = replace(suite.cases[0], expected_target=99.0)
    with pytest.raises(ValueError, match="SemVer"):
        suite.successor(version="1.1.0", cases=(changed,) + suite.cases[1:])
    breaking = suite.successor(
        version="2.0.0", cases=(changed,) + suite.cases[1:]
    )
    assert breaking.suite_id != suite.suite_id
    with pytest.raises(ValueError, match="permanent"):
        suite.successor(version="2.0.0", cases=suite.cases[:-1])
    with pytest.raises(ValueError, match="SemVer"):
        suite.successor(version="1.0.0")


@pytest.mark.parametrize(
    "version",
    ["01.0.0", "1", "1.0", "1.0.0-alpha", True, "1." + "1" * 40 + ".0"],
)
def test_strict_version_refuses_nonrelease_or_unbounded_values(version):
    with pytest.raises(ValueError):
        default_forecast_benchmark_suite().successor(version=version)


def test_asset_mutation_cannot_republish_version_one_under_new_content(
    monkeypatch,
):
    original = Path.read_bytes

    def changed(path):
        payload = original(path)
        return payload + b" " if path.name == contracts.ASSET_NAME else payload

    monkeypatch.setattr(Path, "read_bytes", changed)
    with pytest.raises(ValueError, match="asset bytes"):
        default_forecast_benchmark_suite()


def test_invalid_numeric_pairs_and_nonfinite_or_duplicate_wire_refuse():
    case = default_forecast_benchmark_suite().cases[0]
    with pytest.raises(ValueError):
        replace(case, expected_points=((case.comparators[0], 1.0, 2.0),))
    with pytest.raises(ValueError):
        replace(case, expected_target=float("inf"))
    wire = case.to_dict()
    wire["expected_points"][0].append(2)
    with pytest.raises(ValueError):
        ForecastBenchmarkCaseV1.from_dict(wire)
    with pytest.raises(ValueError):
        ForecastBenchmarkSuiteV1.from_json(
            '{"version":"1.0.0","version":"2.0.0"}'
        )


def test_oversized_case_inventory_refuses_before_serializing_children(
    monkeypatch,
):
    suite = default_forecast_benchmark_suite()
    monkeypatch.setattr(
        ForecastBenchmarkCaseV1,
        "to_dict",
        lambda self: pytest.fail("serialized before count check"),
    )
    with pytest.raises(ValueError, match="bounded"):
        replace(suite, cases=suite.cases * 2)
