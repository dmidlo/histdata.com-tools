"""Versioned scientific classification, not a count of independent opinions."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from importlib.resources import files
from typing import Any

from histdatacom.runtime_contracts import JSONValue

from .contracts import (
    ForecastHorizon,
    ForecastTargetKind,
    _array,
    _json,
    _load,
    _mapping,
    _seal,
    _text,
    _verify,
)


def _version(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 64
        or re.fullmatch(
            r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value
        )
        is None
    ):
        raise ValueError("registry versions require major.minor.patch SemVer")
    return value


def _names(values: tuple[str, ...], name: str) -> tuple[str, ...]:
    _bounded(values, name, 256)
    result = tuple(sorted(_text(value, name) for value in values))
    if not result or len(result) > 256 or len(set(result)) != len(result):
        raise ValueError(f"{name} must be nonempty, unique and bounded")
    return result


def _bounded(values: object, name: str, maximum: int) -> None:
    if not isinstance(values, tuple) or not 1 <= len(values) <= maximum:
        raise ValueError(f"{name} requires a bounded nonempty immutable tuple")


def _count(value: int, name: str, maximum: int) -> None:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} outside positive integer bound")


def _items(value: Any, maximum: int = 256) -> list[Any]:
    items = _array(value)
    if not 1 <= len(items) <= maximum:
        raise ValueError("wire collection exceeds bounds")
    return items


class EngineInformation(str, Enum):
    MACRO = "macro-vintages"
    CONSENSUS = "event-consensus"
    MARKET = "observed-market"
    TEXT = "point-in-time-documents"
    ALTERNATIVE = "qualified-alternative"
    PANEL = "cross-sectional-panel"
    COMPONENT = "accounting-components"
    CALENDAR = "known-release-calendar"


class EngineFunction(str, Enum):
    PRIMARY = "primary-forecaster"
    COMPONENT = "component-forecaster"
    REVISION = "revision-forecaster"
    REGIME = "regime-classifier"
    RELIABILITY = "reliability-model"
    RESIDUAL = "residual-corrector"
    CALIBRATOR = "calibrator"
    WEIGHTER = "ensemble-weighter"
    ANOMALY = "anomaly-detector"
    FALLBACK = "fallback-model"


class EngineEnsembleRole(str, Enum):
    BENCHMARK = "permanent-benchmark"
    CANDIDATE = "candidate"
    CHALLENGER = "challenger"
    MEMBER = "member-subject-to-diversity-qualification"
    CONTROL = "ablation-control"
    FALLBACK = "fallback"
    CONTROLLER = "combination-controller"


class EngineOutput(str, Enum):
    FORECAST = "forecast-feature-snapshot-v1"
    COMPONENT = "component-forecast"
    CLASSIFICATION = "regime-probabilities"
    RELIABILITY = "reliability-probability"
    RESIDUAL = "residual-distribution"
    CALIBRATION = "calibration-map"
    WEIGHTS = "ensemble-weights"
    ANOMALY = "anomaly-score"


class EngineProbability(str, Enum):
    POINT = "point-only"
    FINITE_SUPPORT = "finite-support-distribution"
    NONE = "not-applicable"


class EngineReadiness(str, Enum):
    DECLARED = "declarative-only"
    REFERENCE = "executable-reference-unqualified"


@dataclass(frozen=True, slots=True)
class ForecastTechniqueV1:
    technique_id: str
    label: str
    parent_id: str | None
    requirement_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("technique_id", "label"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        if self.parent_id is not None:
            object.__setattr__(
                self, "parent_id", _text(self.parent_id, "parent")
            )
        object.__setattr__(
            self,
            "requirement_refs",
            _names(self.requirement_refs, "requirements"),
        )
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-technique",
            {
                "technique_id": self.technique_id,
                "label": self.label,
                "parent_id": self.parent_id,
                "requirement_refs": list(self.requirement_refs),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastTechniqueV1:
        result = cls(
            data["technique_id"],
            data["label"],
            data["parent_id"],
            tuple(_items(data["requirement_refs"])),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastTaxonomyV1:
    version: str
    techniques: tuple[ForecastTechniqueV1, ...]
    archive_source: str | None = None
    family_roots: tuple[tuple[str, str], ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _version(self.version)
        _bounded(self.techniques, "techniques", 4096)
        if any(
            not isinstance(item, ForecastTechniqueV1)
            for item in self.techniques
        ):
            raise TypeError("taxonomy requires typed technique nodes")
        if self.archive_source is not None:
            object.__setattr__(
                self,
                "archive_source",
                _text(self.archive_source, "archive source pointer"),
            )
        items = tuple(
            sorted(self.techniques, key=lambda item: item.technique_id)
        )
        _count(len(items), "techniques", 4096)
        by_id = {item.technique_id: item for item in items}
        if len(by_id) != len(items):
            raise ValueError("duplicate technique identity")
        roots: dict[str, str] = {}
        for item in items:
            seen = {item.technique_id}
            parent = item.parent_id
            root = item.technique_id
            while parent is not None:
                if parent in seen or parent not in by_id or len(seen) > 32:
                    raise ValueError("cyclic or missing technique parent")
                seen.add(parent)
                root = parent
                parent = by_id[parent].parent_id
            roots[item.technique_id] = root
        object.__setattr__(self, "techniques", items)
        object.__setattr__(self, "family_roots", tuple(sorted(roots.items())))
        self.to_dict()

    @property
    def taxonomy_id(self) -> str:
        return str(self.to_dict()["id"])

    def family(self, technique_id: str) -> str:
        roots = dict(self.family_roots)
        if technique_id not in roots:
            raise ValueError("technique not present in exact taxonomy")
        return roots[technique_id]

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-taxonomy",
            {
                "version": self.version,
                "archive_coverage": (
                    "unverified"
                    if self.archive_source is None
                    else "source-pointer-retained-coverage-unverified"
                ),
                "archive_source": self.archive_source,
                "techniques": [item.to_dict() for item in self.techniques],
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastTaxonomyV1:
        result = cls(
            data["version"],
            tuple(
                ForecastTechniqueV1.from_dict(_mapping(item))
                for item in _items(data["techniques"], 4096)
            ),
            data["archive_source"],
        )
        _verify(data, result.to_dict())
        return result


def default_forecast_taxonomy() -> ForecastTaxonomyV1:
    """Issue-derived planning taxonomy; never evidence of archive completeness."""
    raw = _load(
        files("histdatacom.forecasting")
        .joinpath("assets/forecast_taxonomy_v1.json")
        .read_text(encoding="utf-8")
    )
    if (
        set(raw) != {"version", "source_status", "families"}
        or raw["source_status"] != "issue-derived; original archive unavailable"
    ):
        raise ValueError("unexpected taxonomy asset schema/source status")
    nodes: list[ForecastTechniqueV1] = []
    for family in _items(raw["families"], 4096):
        row = _mapping(family)
        if set(row) != {"id", "label", "children", "refs"}:
            raise ValueError("unexpected taxonomy family schema")
        refs = tuple(_items(row["refs"]))
        nodes.append(ForecastTechniqueV1(row["id"], row["label"], None, refs))
        for child in _array(row["children"]):
            if len(nodes) >= 4096:
                raise ValueError("taxonomy asset node bound exceeded")
            name = _text(child, "child")
            nodes.append(
                ForecastTechniqueV1(
                    f'{row["id"]}/{name}', name, row["id"], refs
                )
            )
    return ForecastTaxonomyV1(raw["version"], tuple(nodes))


@dataclass(frozen=True, slots=True)
class ForecastEngineDescriptorV1:
    engine_key: str
    version: str
    model_name: str
    model_version: str
    implementation_module: str
    implementation_sha256: str
    techniques: tuple[str, ...]
    information: tuple[EngineInformation, ...]
    horizons: tuple[ForecastHorizon, ...]
    targets: tuple[ForecastTargetKind, ...]
    frequencies: tuple[str, ...]
    function: EngineFunction
    ensemble_role: EngineEnsembleRole
    output: EngineOutput
    probability: EngineProbability
    readiness: EngineReadiness
    minimum_sample: int
    maximum_sample: int
    maximum_columns: int
    fit_determinism: str
    generate_determinism: str
    training_cost: str
    refusal_modes: tuple[str, ...]
    leakage_constraints: tuple[str, ...]
    scientific_role: str
    ablation_key: str
    minimum_benchmarks: tuple[str, ...]

    def __post_init__(self) -> None:
        _version(self.version)
        for name in (
            "engine_key",
            "model_name",
            "model_version",
            "implementation_module",
            "fit_determinism",
            "generate_determinism",
            "training_cost",
            "scientific_role",
            "ablation_key",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        if (
            not isinstance(self.implementation_sha256, str)
            or re.fullmatch("[0-9a-f]{64}", self.implementation_sha256) is None
        ):
            raise ValueError("implementation requires exact code SHA256")
        for name in (
            "techniques",
            "frequencies",
            "refusal_modes",
            "leakage_constraints",
            "minimum_benchmarks",
        ):
            object.__setattr__(self, name, _names(getattr(self, name), name))
        for name, kind in (
            ("information", EngineInformation),
            ("horizons", ForecastHorizon),
            ("targets", ForecastTargetKind),
        ):
            object.__setattr__(
                self,
                name,
                tuple(kind(item) for item in _names(getattr(self, name), name)),
            )
        for name, scalar_kind in (
            ("function", EngineFunction),
            ("ensemble_role", EngineEnsembleRole),
            ("output", EngineOutput),
            ("probability", EngineProbability),
            ("readiness", EngineReadiness),
        ):
            object.__setattr__(self, name, scalar_kind(getattr(self, name)))
        _count(self.minimum_sample, "minimum_sample", 10_000)
        _count(self.maximum_sample, "maximum_sample", 10_000)
        _count(self.maximum_columns, "maximum_columns", 256)
        if self.minimum_sample > self.maximum_sample:
            raise ValueError("sample limits reversed")
        expected = {
            EngineFunction.PRIMARY: EngineOutput.FORECAST,
            EngineFunction.REVISION: EngineOutput.FORECAST,
            EngineFunction.FALLBACK: EngineOutput.FORECAST,
            EngineFunction.COMPONENT: EngineOutput.COMPONENT,
            EngineFunction.REGIME: EngineOutput.CLASSIFICATION,
            EngineFunction.RELIABILITY: EngineOutput.RELIABILITY,
            EngineFunction.RESIDUAL: EngineOutput.RESIDUAL,
            EngineFunction.CALIBRATOR: EngineOutput.CALIBRATION,
            EngineFunction.WEIGHTER: EngineOutput.WEIGHTS,
            EngineFunction.ANOMALY: EngineOutput.ANOMALY,
        }
        if self.output != expected[self.function]:
            raise ValueError("function/output schema mismatch")
        if self.ablation_key == self.engine_key:
            raise ValueError("ablation must identify a different engine")
        self.to_dict()

    @property
    def descriptor_id(self) -> str:
        return str(self.to_dict()["id"])

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-engine-descriptor",
            {
                "engine_key": self.engine_key,
                "version": self.version,
                "model_name": self.model_name,
                "model_version": self.model_version,
                "implementation_module": self.implementation_module,
                "implementation_sha256": self.implementation_sha256,
                "techniques": list(self.techniques),
                "information": [item.value for item in self.information],
                "horizons": [item.value for item in self.horizons],
                "targets": [item.value for item in self.targets],
                "frequencies": list(self.frequencies),
                "function": self.function.value,
                "ensemble_role": self.ensemble_role.value,
                "output": self.output.value,
                "probability": self.probability.value,
                "readiness": self.readiness.value,
                "minimum_sample": self.minimum_sample,
                "maximum_sample": self.maximum_sample,
                "maximum_columns": self.maximum_columns,
                "fit_determinism": self.fit_determinism,
                "generate_determinism": self.generate_determinism,
                "training_cost": self.training_cost,
                "refusal_modes": list(self.refusal_modes),
                "leakage_constraints": list(self.leakage_constraints),
                "scientific_role": self.scientific_role,
                "ablation_key": self.ablation_key,
                "minimum_benchmarks": list(self.minimum_benchmarks),
            },
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastEngineDescriptorV1:
        result = cls(
            data["engine_key"],
            data["version"],
            data["model_name"],
            data["model_version"],
            data["implementation_module"],
            data["implementation_sha256"],
            tuple(_items(data["techniques"])),
            tuple(
                EngineInformation(item) for item in _items(data["information"])
            ),
            tuple(ForecastHorizon(item) for item in _items(data["horizons"])),
            tuple(ForecastTargetKind(item) for item in _items(data["targets"])),
            tuple(_items(data["frequencies"])),
            EngineFunction(data["function"]),
            EngineEnsembleRole(data["ensemble_role"]),
            EngineOutput(data["output"]),
            EngineProbability(data["probability"]),
            EngineReadiness(data["readiness"]),
            data["minimum_sample"],
            data["maximum_sample"],
            data["maximum_columns"],
            data["fit_determinism"],
            data["generate_determinism"],
            data["training_cost"],
            tuple(_items(data["refusal_modes"])),
            tuple(_items(data["leakage_constraints"])),
            data["scientific_role"],
            data["ablation_key"],
            tuple(_items(data["minimum_benchmarks"])),
        )
        _verify(data, result.to_dict())
        return result


@dataclass(frozen=True, slots=True)
class ForecastEngineRegistryV1:
    version: str
    taxonomy: ForecastTaxonomyV1
    engines: tuple[ForecastEngineDescriptorV1, ...]

    def __post_init__(self) -> None:
        _version(self.version)
        if not isinstance(self.taxonomy, ForecastTaxonomyV1):
            raise TypeError("registry requires typed taxonomy")
        _bounded(self.engines, "engines", 4096)
        if any(
            not isinstance(item, ForecastEngineDescriptorV1)
            for item in self.engines
        ):
            raise TypeError("registry requires typed descriptors")
        if sum(len(item.techniques) for item in self.engines) > 16_384:
            raise ValueError("registry technique-edge bound exceeded")
        engines = tuple(sorted(self.engines, key=lambda item: item.engine_key))
        _count(len(engines), "engines", 4096)
        keys = {item.engine_key for item in engines}
        if len(keys) != len(engines):
            raise ValueError("duplicate engine key")
        roots = dict(self.taxonomy.family_roots)
        for item in engines:
            for technique in item.techniques:
                if technique not in roots:
                    raise ValueError("technique not present in exact taxonomy")
            if (
                item.ablation_key not in keys
                or not set(item.minimum_benchmarks) <= keys
            ):
                raise ValueError(
                    "ablation/benchmark is absent from exact registry"
                )
        object.__setattr__(self, "engines", engines)
        self.to_dict()

    @property
    def registry_id(self) -> str:
        return str(self.to_dict()["id"])

    def engine(self, key: str) -> ForecastEngineDescriptorV1:
        for item in self.engines:
            if item.engine_key == key:
                return item
        raise ValueError("engine is absent from exact registry")

    def family_groups(self) -> tuple[tuple[str, tuple[str, ...]], ...]:
        groups: dict[str, list[str]] = {}
        roots = dict(self.taxonomy.family_roots)
        for item in self.engines:
            for family in sorted({roots[key] for key in item.techniques}):
                groups.setdefault(family, []).append(item.engine_key)
        return tuple(
            (key, tuple(value)) for key, value in sorted(groups.items())
        )

    def require_production(self, key: str) -> None:
        self.engine(key)
        raise ValueError(
            "no production admission: chronological qualification, protected evaluation and diversity evidence are not implemented"
        )

    def successor(
        self,
        version: str,
        *,
        taxonomy: ForecastTaxonomyV1 | None = None,
        engines: tuple[ForecastEngineDescriptorV1, ...] | None = None,
    ) -> ForecastRegistryChangeV1:
        return ForecastRegistryChangeV1(
            self,
            ForecastEngineRegistryV1(
                version,
                self.taxonomy if taxonomy is None else taxonomy,
                self.engines if engines is None else engines,
            ),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-engine-registry",
            {
                "version": self.version,
                "taxonomy": self.taxonomy.to_dict(),
                "engines": [item.to_dict() for item in self.engines],
                "independence_claim": "none-family-groups-are-not-independent-votes",
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastEngineRegistryV1:
        result = cls(
            data["version"],
            ForecastTaxonomyV1.from_dict(_mapping(data["taxonomy"])),
            tuple(
                ForecastEngineDescriptorV1.from_dict(_mapping(item))
                for item in _items(data["engines"], 4096)
            ),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastEngineRegistryV1:
        return cls.from_dict(_load(text))


def _delta(old: Mapping[str, object], new: Mapping[str, object]) -> int:
    if any(key not in new or value != new[key] for key, value in old.items()):
        return 0
    return 1 if old.keys() != new.keys() else 2


def _advance(previous: str, current: str, level: int) -> None:
    _version(previous)
    _version(current)
    old = tuple(map(int, previous.split(".")))
    new = tuple(map(int, current.split(".")))
    if new <= old or new[: level + 1] <= old[: level + 1]:
        raise ValueError(
            "change requires an advancing major/minor/patch SemVer at its classified level"
        )


@dataclass(frozen=True, slots=True)
class ForecastRegistryChangeV1:
    """Retained predecessor and executable conservative semantic delta policy."""

    previous: ForecastEngineRegistryV1
    current: ForecastEngineRegistryV1

    def __post_init__(self) -> None:
        if not isinstance(
            self.previous, ForecastEngineRegistryV1
        ) or not isinstance(self.current, ForecastEngineRegistryV1):
            raise TypeError(
                "registry change requires both exact registry snapshots"
            )
        old_tax = self.previous.taxonomy
        new_tax = self.current.taxonomy
        if old_tax != new_tax:
            tax_level = _delta(
                {item.technique_id: item for item in old_tax.techniques},
                {item.technique_id: item for item in new_tax.techniques},
            )
            if old_tax.archive_source != new_tax.archive_source:
                tax_level = min(tax_level, 1)
            _advance(old_tax.version, new_tax.version, tax_level)
        new_engines = {item.engine_key: item for item in self.current.engines}
        for old in self.previous.engines:
            new = new_engines.get(old.engine_key)
            if new is not None and new != old:
                _advance(old.version, new.version, 0)
        _advance(self.previous.version, self.current.version, self.change_level)
        self.to_dict()

    @property
    def change_level(self) -> int:
        tax = _delta(
            {
                item.technique_id: item
                for item in self.previous.taxonomy.techniques
            },
            {
                item.technique_id: item
                for item in self.current.taxonomy.techniques
            },
        )
        engines = _delta(
            {item.engine_key: item for item in self.previous.engines},
            {item.engine_key: item for item in self.current.engines},
        )
        if (
            self.previous.taxonomy.archive_source
            != self.current.taxonomy.archive_source
        ):
            tax = min(tax, 1)
        return min(tax, engines)

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "forecast-registry-change",
            {
                "previous": self.previous.to_dict(),
                "current": self.current.to_dict(),
                "change_class": ("major", "minor", "patch")[self.change_level],
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ForecastRegistryChangeV1:
        result = cls(
            ForecastEngineRegistryV1.from_dict(_mapping(data["previous"])),
            ForecastEngineRegistryV1.from_dict(_mapping(data["current"])),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> ForecastRegistryChangeV1:
        return cls.from_dict(_load(text))
