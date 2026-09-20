"""Independent axes, honest catalog scope and enforceable registry changes."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from histdatacom.forecasting.engine_contracts import (
    EngineFunction,
    EngineInformation,
    EngineOutput,
    EngineReadiness,
    ForecastEngineRegistryV1,
    ForecastRegistryChangeV1,
    ForecastTaxonomyV1,
    ForecastTechniqueV1,
    default_forecast_taxonomy,
)
from histdatacom.forecasting.engine_runner import (
    REFERENCE_BLEND,
    default_forecast_registry,
)


def test_catalog_retains_all_issue_roots_without_claiming_archive_completeness() -> (
    None
):
    taxonomy = default_forecast_taxonomy()
    assert {
        item.technique_id
        for item in taxonomy.techniques
        if item.parent_id is None
    } == {
        "benchmark",
        "univariate-state-space",
        "multivariate",
        "latent-factor",
        "mixed-frequency",
        "regularized-linear",
        "nonlinear",
        "trees",
        "neural-temporal",
        "regime",
        "structural",
        "semi-structural",
        "analogue",
        "market-implied",
        "textual",
        "alternative",
        "panel",
        "density",
    }
    assert taxonomy.family("mixed-frequency/U-MIDAS") == "mixed-frequency"
    assert taxonomy.family("neural-temporal/N-HiTS") == "neural-temporal"
    assert taxonomy.to_dict()["archive_coverage"] == "unverified"
    assert taxonomy.archive_source is None
    assert ForecastTaxonomyV1.from_dict(taxonomy.to_dict()) == taxonomy


@pytest.mark.parametrize(
    "function,output",
    list(
        zip(
            EngineFunction,
            (
                EngineOutput.FORECAST,
                EngineOutput.COMPONENT,
                EngineOutput.FORECAST,
                EngineOutput.CLASSIFICATION,
                EngineOutput.RELIABILITY,
                EngineOutput.RESIDUAL,
                EngineOutput.CALIBRATION,
                EngineOutput.WEIGHTS,
                EngineOutput.ANOMALY,
                EngineOutput.FORECAST,
            ),
        )
    ),
)
def test_every_function_is_independent_of_technique_information_and_ensemble_axes(
    function: EngineFunction, output: EngineOutput
) -> None:
    registry = default_forecast_registry()
    base = registry.engine(REFERENCE_BLEND)
    other = replace(
        base,
        engine_key=f"declared-{function.value}",
        techniques=("neural-temporal/transformer", "panel/multi-task"),
        information=(EngineInformation.TEXT, EngineInformation.PANEL),
        function=function,
        output=output,
        readiness=EngineReadiness.DECLARED,
    )
    expanded = replace(registry, engines=(*registry.engines, other))
    assert ForecastEngineRegistryV1.from_json(expanded.to_json()) == expanded
    assert other.ensemble_role == base.ensemble_role
    with pytest.raises(ValueError, match="no production admission"):
        expanded.require_production(other.engine_key)


def test_four_executed_variants_are_one_family_not_four_independent_votes() -> (
    None
):
    registry = default_forecast_registry()
    assert registry.family_groups() == (
        (
            "benchmark",
            (
                "historical-mean",
                "historical-median",
                "last-value",
                "reference-blend",
            ),
        ),
    )
    assert "not-independent-votes" in str(
        registry.to_dict()["independence_claim"]
    )
    assert registry.engine(REFERENCE_BLEND).model_version == "1.0"
    assert registry.engine(REFERENCE_BLEND).version == "1.0.0"


@pytest.mark.parametrize(
    "version", ["1.0", "v1.0.0", "01.0.0", "1.0.-1", "1.0.0-extra", "9" * 1000]
)
def test_semver_is_strict_and_bounded(version: str) -> None:
    with pytest.raises(ValueError, match="SemVer"):
        replace(default_forecast_registry(), version=version)


def test_successor_requires_taxonomy_minor_and_retains_predecessor() -> None:
    previous = default_forecast_registry()
    added = ForecastTechniqueV1(
        "benchmark/new-declared", "New declaration", "benchmark", ("#562",)
    )
    unversioned = replace(
        previous.taxonomy, techniques=(*previous.taxonomy.techniques, added)
    )
    with pytest.raises(ValueError, match="SemVer"):
        previous.successor("1.1.0", taxonomy=unversioned)
    changed = replace(unversioned, version="1.1.0")
    for version in ("1.0.0", "1.0.1"):
        with pytest.raises(ValueError, match="SemVer"):
            previous.successor(version, taxonomy=changed)
    receipt = previous.successor("1.1.0", taxonomy=changed)
    assert receipt.previous.registry_id == previous.registry_id
    assert receipt.change_level == 1
    assert ForecastRegistryChangeV1.from_json(receipt.to_json()) == receipt
    assert previous.registry_id != receipt.current.registry_id


def test_changed_existing_meaning_requires_explicit_major_versions() -> None:
    previous = default_forecast_registry()
    nodes = tuple(
        (
            replace(item, label="Changed scientific meaning")
            if item.technique_id == "benchmark"
            else item
        )
        for item in previous.taxonomy.techniques
    )
    with pytest.raises(ValueError, match="SemVer"):
        previous.successor(
            "2.0.0",
            taxonomy=replace(
                previous.taxonomy, version="1.1.0", techniques=nodes
            ),
        )
    taxonomy = replace(previous.taxonomy, version="2.0.0", techniques=nodes)
    assert previous.successor("2.0.0", taxonomy=taxonomy).change_level == 0
    engine = replace(
        previous.engine(REFERENCE_BLEND),
        scientific_role="Different scientific role",
    )
    engines = tuple(
        engine if item.engine_key == engine.engine_key else item
        for item in previous.engines
    )
    with pytest.raises(ValueError, match="SemVer"):
        previous.successor("2.0.0", engines=engines)
    major = replace(engine, version="2.0.0")
    assert (
        previous.successor(
            "2.0.0",
            engines=tuple(
                major if item.engine_key == major.engine_key else item
                for item in engines
            ),
        ).change_level
        == 0
    )


def test_source_pointer_successor_is_not_a_coverage_certificate() -> None:
    previous = default_forecast_registry()
    taxonomy = replace(
        previous.taxonomy,
        version="1.1.0",
        archive_source="operator-supplied-archive:sha256:" + "a" * 64,
    )
    receipt = previous.successor("1.1.0", taxonomy=taxonomy)
    assert (
        receipt.current.taxonomy.to_dict()["archive_coverage"]
        == "source-pointer-retained-coverage-unverified"
    )


@pytest.mark.parametrize(
    "mutation", ["unknown", "duplicate", "orphan", "cycle", "role"]
)
def test_malformed_registry_and_taxonomy_refuse(mutation: str) -> None:
    registry = default_forecast_registry()
    with pytest.raises(ValueError):
        if mutation == "unknown":
            replace(
                registry,
                engines=(
                    replace(registry.engines[0], techniques=("unknown",)),
                    *registry.engines[1:],
                ),
            )
        elif mutation == "duplicate":
            replace(registry, engines=(*registry.engines, registry.engines[0]))
        elif mutation == "role":
            replace(registry.engines[0], function=EngineFunction.REGIME)
        else:
            ForecastTaxonomyV1(
                "1.0.0",
                (
                    ForecastTechniqueV1("a", "a", "b", ("#562",)),
                    ForecastTechniqueV1(
                        "b",
                        "b",
                        "a" if mutation == "cycle" else "missing",
                        ("#562",),
                    ),
                ),
            )


def test_collection_limits_refuse_before_visiting_or_sorting_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import histdatacom.forecasting.engine_contracts as contracts

    registry = default_forecast_registry()

    def fail(*args: object) -> str:
        raise AssertionError("oversized collection was visited")

    monkeypatch.setattr(contracts, "_text", fail)
    with pytest.raises(ValueError, match="bounded"):
        contracts._names(("x",) * 257, "too-many")
    with pytest.raises(ValueError, match="bounded"):
        replace(
            registry.taxonomy, techniques=registry.taxonomy.techniques * 100
        )
    with pytest.raises(ValueError, match="bounded"):
        replace(registry, engines=registry.engines * 2000)


def test_registry_reuses_family_roots_instead_of_rebuilding_per_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = default_forecast_registry()

    def fail(*args: object) -> str:
        raise AssertionError("per-edge family lookup")

    monkeypatch.setattr(ForecastTaxonomyV1, "family", fail)
    assert replace(registry).family_groups() == registry.family_groups()


def test_wire_checks_collection_size_before_decoding_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = default_forecast_taxonomy().to_dict()
    payload["techniques"] = [None] * 4097

    def fail(*args: object) -> None:
        raise AssertionError("oversized wire decoded members")

    monkeypatch.setattr(ForecastTechniqueV1, "from_dict", fail)
    with pytest.raises(ValueError, match="wire collection"):
        ForecastTaxonomyV1.from_dict(payload)


def test_registry_wire_rejects_unknown_keys_and_duplicate_json_keys() -> None:
    registry = default_forecast_registry()
    payload = registry.to_dict()
    payload["independent_votes"] = 4
    with pytest.raises(ValueError, match="identity or payload"):
        ForecastEngineRegistryV1.from_json(json.dumps(payload))
    with pytest.raises(ValueError, match="duplicate"):
        ForecastEngineRegistryV1.from_json(
            '{"version":"1.0.0","version":"1.1.0"}'
        )
