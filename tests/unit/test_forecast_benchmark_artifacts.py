"""Real complete-suite replay plus atomic and bounded public artifact paths."""

import hashlib
import os
from dataclasses import replace

import pytest

from histdatacom.forecasting import (
    ForecastBenchmarkRunV1,
    ForecastBenchmarkSuiteV1,
    default_forecast_benchmark_suite,
    read_benchmark_artifact,
    write_benchmark_artifact,
)
from histdatacom.forecasting import benchmark_artifacts as artifacts
from histdatacom.forecasting import benchmark_runner as runner
from histdatacom.forecasting import math_verification


@pytest.fixture(scope="module")
def executed():
    return ForecastBenchmarkRunV1(default_forecast_benchmark_suite())


def test_complete_executed_coverage_and_real_write_read_reexecution(
    executed, tmp_path
):
    wire = executed.to_dict()
    assert wire["scored_count"] == 55
    assert wire["refused_count"] == 5
    assert (
        len(wire["coverage"]) == len({tuple(v) for v in wire["coverage"]}) == 60
    )
    assert (
        wire["qualification"]
        == "synthetic-contract-benchmark-not-empirical-or-production"
    )
    assert all(
        r["checked_report"]["verification"]["passed"]
        for r in wire["results"]
        if r["status"] == "scored"
    )
    combinations = [
        r for r in wire["results"] if r["combination_check"] is not None
    ]
    assert len(combinations) == 20
    assert all(len(r["members"]) == 3 for r in combinations)
    path = write_benchmark_artifact(executed, tmp_path)
    restored = read_benchmark_artifact(path)
    assert restored.to_json() == executed.to_json()
    assert restored.run_id == executed.run_id
    # Returned dictionaries are detached from the cached immutable artifact.
    wire["results"].clear()
    assert len(executed.to_dict()["results"]) == 60


@pytest.mark.parametrize(
    "change", ["missing", "duplicate", "extra", "reorder", "code"]
)
def test_incomplete_or_changed_code_refuses_before_execution(
    executed, monkeypatch, change
):
    wire = executed.to_dict()
    if change == "missing":
        wire["results"].pop()
    elif change == "duplicate":
        wire["results"][-1] = wire["results"][0]
    elif change == "extra":
        wire["results"].append(wire["results"][0])
    elif change == "reorder":
        wire["results"].reverse()
    else:
        wire["code_bindings"]["benchmark_runner.py"] = "f" * 64
    monkeypatch.setattr(
        runner,
        "_execute",
        lambda *args: pytest.fail("expensive execution before preflight"),
    )
    with pytest.raises(ValueError, match="coverage|binding"):
        ForecastBenchmarkRunV1.from_dict(wire)


@pytest.mark.parametrize("mutation", ["combination-value", "refusal-reason"])
def test_resealed_false_result_still_fails_actual_reexecution(
    executed, monkeypatch, mutation
):
    wire = executed.to_dict()
    if mutation == "combination-value":
        wire["results"][0]["combination_check"]["actual_point"] = 123.0
    else:
        refused = next(r for r in wire["results"] if r["status"] == "refused")
        refused["reason"] = "forged-source-supported-refusal"
    wire = math_verification.math_seal(
        "forecast-benchmark-run",
        {
            key: value
            for key, value in wire.items()
            if key not in {"id", "schema_version", "contract_type"}
        },
    )
    # A conventional outer hash check would accept this false result.
    digest = hashlib.sha256(
        math_verification.math_json(
            {key: value for key, value in wire.items() if key != "id"}
        ).encode()
    ).hexdigest()
    assert wire["id"] == "forecast-benchmark-run:" + digest
    assert wire["id"] != executed.run_id
    real_execute = runner._execute
    calls = []

    def tracked_execute(suite):
        calls.append(suite.suite_id)
        return real_execute(suite)

    monkeypatch.setattr(runner, "_execute", tracked_execute)
    with pytest.raises(ValueError, match="identity or payload"):
        ForecastBenchmarkRunV1.from_dict(wire)
    assert calls == [executed.suite.suite_id]


def test_suite_persistence_is_atomic_canonical_no_clobber_and_bounded(
    tmp_path, monkeypatch
):
    suite = default_forecast_benchmark_suite()
    target = write_benchmark_artifact(suite, tmp_path)
    assert read_benchmark_artifact(target) == suite
    assert write_benchmark_artifact(suite, tmp_path) == target
    original = target.read_bytes()
    target.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="differs"):
        write_benchmark_artifact(suite, tmp_path)
    assert target.read_bytes() == b"corrupt"
    target.write_bytes(original)

    def interrupted(*args, **kwargs):
        raise OSError("simulated publication interruption")

    monkeypatch.setattr(artifacts.os, "link", interrupted)
    with pytest.raises(OSError, match="interruption"):
        write_benchmark_artifact(suite, tmp_path)
    assert target.read_bytes() == original
    assert not list(tmp_path.glob(".benchmark-*"))
    with pytest.raises(ValueError, match="regular"):
        read_benchmark_artifact(tmp_path)
    alternate = tmp_path / "wrong.json"
    alternate.write_bytes(original)
    with pytest.raises(ValueError, match="filename"):
        read_benchmark_artifact(alternate)


def test_symlink_refusal_when_platform_permits_creation(tmp_path):
    target = write_benchmark_artifact(
        default_forecast_benchmark_suite(), tmp_path
    )
    link = tmp_path / "symlink"
    try:
        link.symlink_to(target)
    except NotImplementedError:
        pytest.skip("symlink creation unavailable")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege unavailable")
        raise
    with pytest.raises(ValueError, match="regular"):
        read_benchmark_artifact(link)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO requires os.mkfifo")
def test_fifo_refuses_without_open_or_block(tmp_path):
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular"):
        read_benchmark_artifact(fifo)


def test_final_suite_bytes_include_ancestry_and_identity_overhead(monkeypatch):
    suite = default_forecast_benchmark_suite().successor(version="1.0.1")
    size = len(suite.to_json())
    monkeypatch.setattr(math_verification, "MAX_MATH_ARTIFACT_BYTES", size)
    assert ForecastBenchmarkSuiteV1.from_json(suite.to_json()) == suite
    monkeypatch.setattr(math_verification, "MAX_MATH_ARTIFACT_BYTES", size - 1)
    with pytest.raises(ValueError, match="byte"):
        replace(suite)


def test_changed_expected_case_cannot_authorize_false_execution():
    suite = default_forecast_benchmark_suite()
    first = suite.cases[0]
    changed = replace(
        first,
        expected_points=tuple(
            (key, value + 1) for key, value in first.expected_points
        ),
    )
    successor = suite.successor(
        version="2.0.0", cases=(changed,) + suite.cases[1:]
    )
    with pytest.raises(ValueError, match="frozen point"):
        ForecastBenchmarkRunV1(successor)
