"""Registry publication must not confuse metadata with semantic proof."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import pytest

from histdatacom.schema_compatibility import CompatibilityRegistryV1
from histdatacom.schema_compatibility import inventory
from tests.unit.test_schema_compatibility import _edge, _registry, _schema


def _generator() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[2]
        / "scripts/generate_schema_compatibility.py"
    )
    spec = importlib.util.spec_from_file_location(
        "semantic_gate_generator", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # The developer script inserts src for standalone use. Do not let that
    # alter an installed-wheel test session's import boundary.
    previous_path = list(sys.path)
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous_path
    return module


@pytest.mark.parametrize("mode", ("--write", "--check"))
def test_generator_refuses_unproven_lossless_edge_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    from histdatacom import schema_semantics

    generator = _generator()
    registry = _registry((_schema("a"), _schema("b")), (_edge("ab", "a", "b"),))
    # The unchanged v1 graph accepts these as metadata declarations. The
    # publication boundary must additionally reject missing executable proof.
    assert registry.exact_semantics("a", "b")
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["generate", mode])
    monkeypatch.setattr(inventory, "build_registry", lambda root: registry)
    actual_validate = schema_semantics.validate_lossless_evidence
    checked: list[str] = []

    def validate(candidate: CompatibilityRegistryV1) -> object:
        checked.append(candidate.registry_id)
        return actual_validate(candidate)

    def unexpected_render(candidate: CompatibilityRegistryV1) -> str:
        pytest.fail("unproven registry reached documentation publication")

    monkeypatch.setattr(
        schema_semantics, "validate_lossless_evidence", validate
    )
    monkeypatch.setattr(inventory, "render_documentation", unexpected_render)
    sentinel = tmp_path / "retained-evidence.txt"
    sentinel.write_bytes(b"unchanged")
    with pytest.raises(ValueError):
        generator.main()
    assert checked == [registry.registry_id]
    assert tuple(tmp_path.iterdir()) == (sentinel,)
    assert sentinel.read_bytes() == b"unchanged"


def test_generator_checks_semantic_gate_in_both_successful_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from histdatacom import schema_semantics

    generator = _generator()
    registry = _registry((_schema("a"),))
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    monkeypatch.setattr(inventory, "build_registry", lambda root: registry)
    monkeypatch.setattr(
        inventory,
        "render_documentation",
        lambda candidate: "Fixture registry\n",
    )
    actual_validate = schema_semantics.validate_lossless_evidence
    checked: list[str] = []

    def validate(candidate: CompatibilityRegistryV1) -> object:
        checked.append(candidate.registry_id)
        return actual_validate(candidate)

    monkeypatch.setattr(
        schema_semantics, "validate_lossless_evidence", validate
    )
    monkeypatch.setattr(sys, "argv", ["generate", "--write"])
    assert generator.main() == 0
    asset = (
        tmp_path
        / "src/histdatacom/schema_compatibility/assets/registry_v1.json"
    )
    assert asset.read_text("ascii") == registry.to_json()
    assert (tmp_path / "docs/schema-compatibility.md").read_text("ascii") == (
        "Fixture registry\n"
    )
    monkeypatch.setattr(sys, "argv", ["generate", "--check"])
    assert generator.main() == 0
    assert checked == [registry.registry_id, registry.registry_id]


@pytest.mark.parametrize("second_qualified", (True, False))
def test_semantic_subjects_preserve_distinct_parallel_edge_identity(
    second_qualified: bool,
) -> None:
    from histdatacom.schema_semantics import edge_subject_id

    first = _edge("first", "a", "b")
    second = replace(
        first, edge_id="second", priority=1, qualified=second_qualified
    )
    registry = _registry((_schema("a"), _schema("b")), (first, second))
    assert registry.migration_path("a", "b")[0].edge_id == "first"
    assert edge_subject_id(registry, "first") != edge_subject_id(
        registry, "second"
    )


def test_new_contracts_have_concrete_inventory_and_embedded_readers() -> None:
    root = Path(__file__).resolve().parents[2]
    registry = inventory.build_registry(root)
    schemas = {schema.family: schema for schema in registry.schemas}
    exemptions = {item.qualified_name for item in registry.exemptions}
    for family in (
        "histdatacom.data_quality.training_join_contracts.TrainingJoinBatchV1",
        "histdatacom.schema_semantics.contracts.SemanticMigrationProofV1",
        "histdatacom.experiments.lineage.ExperimentRegistryV1",
    ):
        assert schemas[family].readers
        assert schemas[family].writers
        assert not schemas[family].wire_schema.startswith("unversioned:")
    embedded = schemas["histdatacom.experiments.contracts.ArtifactReferenceV1"]
    implementations = {
        item.implementation_id: item for item in registry.implementations
    }
    assert {
        implementations[key].qualified_name for key in embedded.readers
    } == {"histdatacom.experiments.contracts.ArtifactReferenceV1.from_payload"}
    # A real embedded shape has a reader but no invented standalone wire label.
    assert embedded.wire_schema.startswith("unversioned:")
    envelope = schemas["histdatacom.experiments.lineage.ExperimentRegistryV1"]
    assert not any(
        implementations[key].qualified_name.endswith(
            (".from_payload", ".to_payload")
        )
        for key in (*envelope.readers, *envelope.writers)
    )
    for template in (
        "histdatacom.experiments._wire.Record",
        "histdatacom.experiments._wire.Artifact",
        "histdatacom.experiments._wire.Artifact.artifact_id",
    ):
        assert template in exemptions
        assert template not in schemas
