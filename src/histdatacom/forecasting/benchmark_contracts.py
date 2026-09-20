"""Closed, versioned benchmark definitions; fixture evidence is not skill."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from histdatacom.runtime_contracts import JSONValue

from .contracts import ForecastHorizon, ForecastTargetKind, _verify
from .math_reference import number
from .math_verification import math_json, math_load, math_seal

MAX_BENCHMARK_CASES = 32
MAX_BENCHMARK_RESULTS = 128
ASSET_NAME = "forecast_benchmark_suite_v1.json"
BUILTIN_SUITE_SHA256 = (
    "059395376b648619c9f19ccddfec3990b41c85f0bd2ae20f4d5641150ad84b76"
)
QUALIFICATION = "synthetic-contract-benchmark-not-empirical-or-production"


class BenchmarkComparator(str, Enum):
    HISTORICAL_MEAN = "historical-mean"
    HISTORICAL_MEDIAN = "historical-median"
    LAST_VALUE = "last-value"
    CONSENSUS_PERSISTENCE = "consensus-persistence"
    FORECAST_MEAN = "equal-forecast-mean"
    FORECAST_MEDIAN = "forecast-median"


class BenchmarkScenario(str, Enum):
    ORDINARY = "ordinary"
    FUTURE_CONSENSUS = "future-consensus"
    REVISION_LEVEL = "revision-level"
    REVISION_CHANGE = "revision-change"
    OBSERVED_SURPRISE = "observed-surprise"
    MISSING_CONSENSUS = "missing-consensus"
    MISSING_FEATURE = "missing-feature"
    INSUFFICIENT_SUPPORT = "insufficient-support"
    SEMANTIC_MISMATCH = "semantic-mismatch"
    LATE_SCHEDULE = "late-schedule"
    DELAYED_ACTUAL = "delayed-actual"


def _text(value: str) -> None:
    if (
        type(value) is not str
        or re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,95}", value) is None
    ):
        raise ValueError("invalid benchmark name")


def _version(value: str) -> tuple[int, int, int]:
    if (
        type(value) is not str
        or re.fullmatch(
            r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value
        )
        is None
    ):
        raise ValueError("benchmark version requires release SemVer")
    if len(value) > 32:
        raise ValueError("benchmark version exceeds bound")
    a, b, c = value.split(".")
    return int(a), int(b), int(c)


def builtin_benchmark_definition() -> dict[str, Any]:
    path = Path(__file__).with_name("assets") / ASSET_NAME
    raw = path.read_bytes()
    if len(raw) > 256_000:
        raise ValueError("benchmark asset exceeds bound")
    if hashlib.sha256(raw).hexdigest() != BUILTIN_SUITE_SHA256:
        raise ValueError("installed frozen benchmark asset bytes differ")
    data = math_load(raw.decode("utf-8"))
    if set(data) != {"version", "source", "cases"}:
        raise ValueError("benchmark asset fields differ")
    return dict(data)


@dataclass(frozen=True, slots=True)
class ForecastBenchmarkCaseV1:
    case_id: str
    horizon: ForecastHorizon
    target_kind: ForecastTargetKind
    scenario: BenchmarkScenario
    comparators: tuple[BenchmarkComparator, ...]
    expected_target: float | None
    expected_points: tuple[tuple[BenchmarkComparator, float], ...]
    expected_refusal: str | None = None

    def __post_init__(self) -> None:
        _text(self.case_id)
        if (
            not isinstance(self.horizon, ForecastHorizon)
            or not isinstance(self.target_kind, ForecastTargetKind)
            or not isinstance(self.scenario, BenchmarkScenario)
        ):
            raise TypeError("benchmark cases require typed task fields")
        if (
            type(self.comparators) is not tuple
            or not 1 <= len(self.comparators) <= 6
            or any(type(x) is not BenchmarkComparator for x in self.comparators)
        ):
            raise ValueError("benchmark comparator set is invalid")
        if self.comparators != tuple(
            sorted(set(self.comparators), key=lambda x: x.value)
        ):
            raise ValueError("benchmark comparators must be sorted and unique")
        if (
            type(self.expected_points) is not tuple
            or len(self.expected_points) > 6
        ):
            raise ValueError("benchmark expected points exceed bounds")
        for pair in self.expected_points:
            if (
                type(pair) is not tuple
                or len(pair) != 2
                or type(pair[0]) is not BenchmarkComparator
            ):
                raise ValueError("invalid expected benchmark point")
            number(pair[1])
        if self.expected_refusal is None:
            if (
                self.expected_target is None
                or tuple(p[0] for p in self.expected_points) != self.comparators
            ):
                raise ValueError("positive case must cover every comparator")
            number(self.expected_target)
        elif (
            self.expected_refusal
            not in {
                "consensus-unavailable",
                "missing-feature",
                "insufficient-support",
                "semantic-mismatch",
                "schedule-unavailable",
            }
            or self.expected_points
            or self.expected_target is not None
        ):
            raise ValueError("invalid closed refusal expectation")
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return math_seal(
            "forecast-benchmark-case",
            {
                "case_id": self.case_id,
                "horizon": self.horizon.value,
                "target_kind": self.target_kind.value,
                "scenario": self.scenario.value,
                "comparators": [x.value for x in self.comparators],
                "expected_target": self.expected_target,
                "expected_points": [
                    [x.value, value] for x, value in self.expected_points
                ],
                "expected_refusal": self.expected_refusal,
            },
        )

    @classmethod
    def from_definition(
        cls, data: Mapping[str, Any]
    ) -> ForecastBenchmarkCaseV1:
        if set(data) != {
            "case_id",
            "horizon",
            "target_kind",
            "scenario",
            "comparators",
            "expected_target",
            "expected_points",
            "expected_refusal",
        }:
            raise ValueError("benchmark definition fields differ")
        if (
            type(data["comparators"]) is not list
            or type(data["expected_points"]) is not list
        ):
            raise ValueError("benchmark collections require arrays")
        if len(data["comparators"]) > 6 or len(data["expected_points"]) > 6:
            raise ValueError("benchmark collections exceed bounds")
        if any(
            type(p) is not list or len(p) != 2 for p in data["expected_points"]
        ):
            raise ValueError(
                "expected benchmark pairs require exactly two fields"
            )
        return cls(
            data["case_id"],
            ForecastHorizon(data["horizon"]),
            ForecastTargetKind(data["target_kind"]),
            BenchmarkScenario(data["scenario"]),
            tuple(BenchmarkComparator(x) for x in data["comparators"]),
            data["expected_target"],
            tuple(
                (BenchmarkComparator(p[0]), p[1])
                for p in data["expected_points"]
            ),
            data["expected_refusal"],
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastBenchmarkCaseV1:
        math_json(data)
        result = cls.from_definition(
            {
                k: v
                for k, v in data.items()
                if k not in {"schema_version", "contract_type", "id"}
            }
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastBenchmarkSuiteV1:
    version: str
    source_json: str
    cases: tuple[ForecastBenchmarkCaseV1, ...]
    predecessor_json: str | None = None

    def __post_init__(self) -> None:
        version = _version(self.version)
        if (
            type(self.cases) is not tuple
            or not 1 <= len(self.cases) <= MAX_BENCHMARK_CASES
            or any(type(c) is not ForecastBenchmarkCaseV1 for c in self.cases)
        ):
            raise ValueError("suite requires bounded immutable cases")
        if tuple(c.case_id for c in self.cases) != tuple(
            sorted({c.case_id for c in self.cases})
        ):
            raise ValueError("suite cases must be sorted and unique")
        if sum(len(c.comparators) for c in self.cases) > MAX_BENCHMARK_RESULTS:
            raise ValueError("suite execution count exceeds bounds")
        source = math_load(self.source_json)
        if set(source) != {
            "actual_history",
            "consensus_history",
            "actual",
            "revision",
            "early_consensus",
            "late_consensus",
        }:
            raise ValueError("benchmark source parameters differ")
        for name, values in source.items():
            if name.endswith("history"):
                if type(values) is not list or len(values) != 3:
                    raise ValueError("reference history requires three values")
                for item in values:
                    number(item)
            else:
                number(values)
        object.__setattr__(self, "source_json", math_json(source))
        # Validate final expanded envelope before recursively restoring ancestry.
        self.to_dict()
        if self.predecessor_json is None:
            definition = builtin_benchmark_definition()
            canonical = tuple(
                ForecastBenchmarkCaseV1.from_definition(c)
                for c in definition["cases"]
            )
            if (
                self.version != definition["version"]
                or self.source_json != math_json(definition["source"])
                or self.cases != canonical
            ):
                raise ValueError(
                    "unversioned root suite differs from frozen installed definition"
                )
        else:
            predecessor = self.from_json(self.predecessor_json)
            prior = _version(predecessor.version)
            old = {c.case_id: c for c in predecessor.cases}
            new = {c.case_id: c for c in self.cases}
            # Permanent cases/comparators survive even a breaking successor.
            if not set(old) <= set(new) or any(
                not set(c.comparators) <= set(new[k].comparators)
                for k, c in old.items()
            ):
                raise ValueError(
                    "successor cannot remove permanent benchmark coverage"
                )
            incompatible = self.source_json != predecessor.source_json or any(
                new[k] != c for k, c in old.items()
            )
            additive = set(new) != set(old)
            if (
                version <= prior
                or (incompatible and version[0] <= prior[0])
                or (additive and not incompatible and version[:2] <= prior[:2])
            ):
                raise ValueError(
                    "benchmark successor requires appropriate SemVer increase"
                )

    @property
    def suite_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return math_seal(
            "forecast-benchmark-suite",
            {
                "version": self.version,
                "source": math_load(self.source_json),
                "cases": [c.to_dict() for c in self.cases],
                "predecessor": (
                    None
                    if self.predecessor_json is None
                    else math_load(self.predecessor_json)
                ),
                "qualification": QUALIFICATION,
            },
        )

    def to_json(self) -> str:
        return math_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastBenchmarkSuiteV1:
        math_json(data)
        if (
            type(data.get("cases")) is not list
            or len(data["cases"]) > MAX_BENCHMARK_CASES
        ):
            raise ValueError("invalid suite case array")
        result = cls(
            data["version"],
            math_json(data["source"]),
            tuple(ForecastBenchmarkCaseV1.from_dict(c) for c in data["cases"]),
            (
                None
                if data["predecessor"] is None
                else math_json(data["predecessor"])
            ),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastBenchmarkSuiteV1:
        return cls.from_dict(math_load(text))

    def successor(
        self,
        *,
        version: str,
        cases: tuple[ForecastBenchmarkCaseV1, ...] | None = None,
        source_json: str | None = None,
    ) -> ForecastBenchmarkSuiteV1:
        return ForecastBenchmarkSuiteV1(
            version,
            self.source_json if source_json is None else source_json,
            self.cases if cases is None else cases,
            self.to_json(),
        )


def default_forecast_benchmark_suite() -> ForecastBenchmarkSuiteV1:
    definition = builtin_benchmark_definition()
    return ForecastBenchmarkSuiteV1(
        definition["version"],
        math_json(definition["source"]),
        tuple(
            ForecastBenchmarkCaseV1.from_definition(c)
            for c in definition["cases"]
        ),
    )


def benchmark_code_bindings() -> dict[str, str]:
    files = (
        "benchmark_contracts.py",
        "benchmark_cases.py",
        "benchmark_runner.py",
        "benchmark_artifacts.py",
        "feature_store.py",
        "feature_contracts.py",
        "feature_forecasts.py",
        "contracts.py",
        "scoring.py",
    )
    root = Path(__file__).parent
    return {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in (*files, f"assets/{ASSET_NAME}")
    }
