"""Real capture and legacy-experiment linkage without changing admission."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import runpy

import pytest

from histdatacom.broker_capture import BrokerCaptureSessionV1
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginExperimentInventoryV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistryError,
    bind_plugin_inventory_to_experiment,
    capture_plugin_inventory_metadata,
    read_plugin_inventory,
    verify_capture_plugin_inventory,
    verify_plugin_experiment_inventory,
    write_plugin_inventory,
)
from histdatacom.reconstruction_experiment import (
    ReconstructionExperimentArtifactBindingV1,
    ReconstructionExperimentError,
    ReconstructionExperimentRole,
    read_reconstruction_experiment,
)

ROOT = Path(__file__).resolve().parents[2]


def _inventory() -> BrokerPluginInventoryV1:
    fixture = runpy.run_path(
        str(ROOT / "tests/fixtures/broker_plugin_wheel.py")
    )
    return BrokerPluginInventoryV1(
        (
            BrokerPluginCandidateV1(
                fixture["fixture_registration"](), "a" * 64, "b" * 64
            ),
        )
    )


def _session(inventory: BrokerPluginInventoryV1) -> BrokerCaptureSessionV1:
    return BrokerCaptureSessionV1(
        adapter_id="example.adapter",
        adapter_version="1.0.0",
        adapter_config_sha256="0" * 64,
        protocol="fixture",
        environment_id="fixture",
        server_id="fixture",
        started_at_utc_ns=1,
        started_at_monotonic_ns=1,
        public_metadata=capture_plugin_inventory_metadata(
            inventory,
            selected_candidate_ids=(inventory.candidates[0].artifact_id,),
        ),
    )


def test_capture_inventory_binds_exact_session_and_selected_declaration(
    tmp_path: Path,
) -> None:
    inventory = _inventory()
    session = _session(inventory)
    reference = write_plugin_inventory(inventory, tmp_path / "inventory.json")
    restored = BrokerCaptureSessionV1.from_json(session.to_json())
    verify_capture_plugin_inventory(restored, read_plugin_inventory(reference))
    link = restored.public_metadata["broker_plugin_inventory"]
    assert link["discovered_candidate_count"] == 1
    assert link["selected_candidate_ids"] == [
        inventory.candidates[0].artifact_id
    ]
    assert not link["activation_or_scientific_admission"]
    with pytest.raises(BrokerPluginRegistryError):
        verify_capture_plugin_inventory(restored, BrokerPluginInventoryV1())
    restored.public_metadata["broker_plugin_inventory"][
        "selected_candidate_ids"
    ] = []
    with pytest.raises(BrokerPluginRegistryError):
        verify_capture_plugin_inventory(restored, inventory)


def test_binding_cannot_bypass_duplicate_selection_or_incompatible_sdk() -> (
    None
):
    inventory = _inventory()
    candidate = inventory.candidates[0]
    duplicate = replace(
        inventory,
        candidates=(
            candidate,
            replace(candidate, implementation_sha256="c" * 64),
        ),
    )
    with pytest.raises(BrokerPluginRegistryError, match="ambiguous_selection"):
        capture_plugin_inventory_metadata(
            duplicate, selected_candidate_ids=(candidate.artifact_id,)
        )
    future = replace(
        candidate,
        registration=replace(
            candidate.registration,
            sdk_min_version="2.0.0",
            sdk_max_version="3.0.0",
        ),
    )
    with pytest.raises(BrokerPluginRegistryError):
        capture_plugin_inventory_metadata(
            BrokerPluginInventoryV1((future,)),
            selected_candidate_ids=(future.artifact_id,),
        )
    unselected = capture_plugin_inventory_metadata(duplicate)
    assert unselected["broker_plugin_inventory"]["selected_candidate_ids"] == []


def test_binding_canonical_comparison_distinguishes_boolean_and_integer() -> (
    None
):
    inventory = _inventory()
    session = _session(inventory)
    metadata = capture_plugin_inventory_metadata(inventory)
    metadata["broker_plugin_inventory"][
        "activation_or_scientific_admission"
    ] = 0
    session = replace(session, public_metadata=metadata, session_id="")
    with pytest.raises(BrokerPluginRegistryError):
        verify_capture_plugin_inventory(session, inventory)
    envelope = BrokerPluginExperimentInventoryV1(
        "reconstruction-experiment:sha256:" + "a" * 64,
        inventory,
    )
    payload = envelope.to_dict()
    payload["activation_or_scientific_admission"] = 0
    with pytest.raises(BrokerPluginRegistryError):
        BrokerPluginExperimentInventoryV1.from_dict(payload)


def test_real_experiment_envelope_roundtrip_external_verification_and_legacy_guard(
    tmp_path: Path,
) -> None:
    fixture = runpy.run_path(
        str(ROOT / "tests/unit/test_reconstruction_experiment.py")
    )
    experiment, experiment_ref, _, _ = fixture["_freeze"](
        tmp_path / "experiment"
    )
    old_bytes = Path(experiment_ref.path).read_bytes()
    inventory = _inventory()
    envelope = bind_plugin_inventory_to_experiment(
        experiment,
        inventory,
        selected_candidate_ids=(inventory.candidates[0].artifact_id,),
    )
    reference = write_plugin_inventory(envelope, tmp_path / "envelope.json")
    restored = read_plugin_inventory(
        reference, artifact_type=BrokerPluginExperimentInventoryV1
    )
    authoritative = read_reconstruction_experiment(experiment_ref.path)
    verify_plugin_experiment_inventory(restored, authoritative)
    assert restored.inventory == inventory
    assert not restored.to_dict()["activation_or_scientific_admission"]
    assert Path(experiment_ref.path).read_bytes() == old_bytes
    successor = replace(
        experiment,
        limitations=(*experiment.limitations, "different experiment"),
        experiment_id="",
    )
    with pytest.raises(BrokerPluginRegistryError):
        verify_plugin_experiment_inventory(restored, successor)
    payload = restored.to_dict()
    payload["inventory"]["candidates"][0]["implementation_sha256"] = "d" * 64
    with pytest.raises(BrokerPluginRegistryError):
        BrokerPluginExperimentInventoryV1.from_dict(payload)
    with pytest.raises(ReconstructionExperimentError, match="outside v2.4"):
        ReconstructionExperimentArtifactBindingV1(
            name="broker-plugin-inventory",
            domain="broker-plugin",
            artifact=reference,
            artifact_id=envelope.artifact_id,
            artifact_identity_field="artifact_id",
            dataset_roles=(ReconstructionExperimentRole.PRODUCT_INPUT,),
        )
