"""Software inventory linkage, without admitting broker data or activation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import TYPE_CHECKING, ClassVar, cast

from histdatacom.runtime_contracts import JSONValue

from .contracts import (
    BrokerPluginInventoryV1,
    BrokerPluginRegistryError,
    BrokerPluginRegistryReason,
    canonical_plugin_registry_json,
    _Artifact,
    _dict,
    _tuple,
)
from .selection import select_broker_plugin

if TYPE_CHECKING:
    from histdatacom.broker_capture import BrokerCaptureSessionV1
    from histdatacom.reconstruction_experiment import (
        ReconstructionExperimentManifestV1,
    )


def _selected(
    inventory: BrokerPluginInventoryV1, selected: tuple[str, ...]
) -> None:
    if (
        type(inventory) is not BrokerPluginInventoryV1
        or type(selected) is not tuple
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        )
    if len(selected) > len(inventory.candidates) or any(
        type(item) is not str for item in selected
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        )
    if tuple(sorted(set(selected))) != selected:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        )
    available = {
        candidate.artifact_id: candidate for candidate in inventory.candidates
    }
    if any(item not in available for item in selected):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        )
    if selected and inventory.diagnostics:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INCOMPLETE_INVENTORY
        )
    plugins = [available[item].registration.plugin_id for item in selected]
    if len(set(plugins)) != len(plugins) or any(
        not available[item].registration.supports_sdk(inventory.sdk_version)
        for item in selected
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        )
    for item in selected:
        registration = available[item].registration
        resolved = select_broker_plugin(
            inventory,
            plugin_id=registration.plugin_id,
            version_constraint="==" + registration.plugin_version,
        )
        if resolved.artifact_id != item:
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVALID_BINDING
            )


def capture_plugin_inventory_metadata(
    inventory: BrokerPluginInventoryV1,
    *,
    selected_candidate_ids: tuple[str, ...] = (),
) -> dict[str, JSONValue]:
    """Public capture-session metadata; persist the full inventory separately."""
    _selected(inventory, selected_candidate_ids)
    return {
        "broker_plugin_inventory": {
            "schema_version": "histdatacom.broker-plugin-capture-inventory.v1",
            "inventory_id": inventory.artifact_id,
            "inventory_sha256": hashlib.sha256(
                inventory.to_json().encode("ascii")
            ).hexdigest(),
            "discovered_candidate_count": len(inventory.candidates),
            "selected_candidate_ids": list(selected_candidate_ids),
            "activation_or_scientific_admission": False,
        }
    }


def verify_capture_plugin_inventory(
    session: BrokerCaptureSessionV1,
    inventory: BrokerPluginInventoryV1,
) -> None:
    """Verify against the authoritative session, including its original ID."""
    from histdatacom.broker_capture import BrokerCaptureSessionV1

    try:
        if type(session) is not BrokerCaptureSessionV1:
            raise ValueError
        restored = BrokerCaptureSessionV1.from_dict(session.to_dict())
        link = _dict(restored.public_metadata["broker_plugin_inventory"])
        selected = _tuple(link["selected_candidate_ids"])
        expected = capture_plugin_inventory_metadata(
            inventory, selected_candidate_ids=selected
        )
        if canonical_plugin_registry_json(
            expected["broker_plugin_inventory"]
        ) != canonical_plugin_registry_json(link):
            raise ValueError
    except (KeyError, ValueError, TypeError):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        ) from None


def _experiment_identity(experiment: ReconstructionExperimentManifestV1) -> str:
    from histdatacom.reconstruction_experiment import (
        ReconstructionExperimentManifestV1,
    )

    try:
        if type(experiment) is not ReconstructionExperimentManifestV1:
            raise ValueError
        identity = ReconstructionExperimentManifestV1.from_dict(
            experiment.to_dict()
        ).experiment_id
        if not isinstance(identity, str):
            raise ValueError
        return identity
    except (ValueError, TypeError, KeyError):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        ) from None


@dataclass(frozen=True, slots=True)
class BrokerPluginExperimentInventoryV1(_Artifact):
    """Separate software-environment envelope, not a v2.4 provider binding.

    Reading proves the envelope's own identity. Call verify against the
    authoritative experiment manifest to establish the external linkage.
    """

    experiment_id: str
    inventory: BrokerPluginInventoryV1
    selected_candidate_ids: tuple[str, ...] = ()
    KIND: ClassVar[str] = "experiment-inventory"

    def __post_init__(self) -> None:
        if type(self.experiment_id) is not str or not re.fullmatch(
            r"reconstruction-experiment:sha256:[a-f0-9]{64}", self.experiment_id
        ):
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVALID_BINDING
            )
        _selected(self.inventory, self.selected_candidate_ids)
        self._bound()

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "experiment_id": self.experiment_id,
            "inventory": self.inventory.to_dict(),
            "selected_candidate_ids": list(self.selected_candidate_ids),
            "activation_or_scientific_admission": False,
        }

    @classmethod
    def from_dict(
        cls, value: dict[str, object]
    ) -> BrokerPluginExperimentInventoryV1:
        try:
            obj = cls(
                cast(str, value["experiment_id"]),
                BrokerPluginInventoryV1.from_dict(_dict(value["inventory"])),
                _tuple(value["selected_candidate_ids"]),
            )
            obj._check_payload(value)
            return obj
        except (KeyError, ValueError, TypeError):
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVALID_BINDING
            ) from None


def bind_plugin_inventory_to_experiment(
    experiment: ReconstructionExperimentManifestV1,
    inventory: BrokerPluginInventoryV1,
    *,
    selected_candidate_ids: tuple[str, ...] = (),
) -> BrokerPluginExperimentInventoryV1:
    return BrokerPluginExperimentInventoryV1(
        _experiment_identity(experiment), inventory, selected_candidate_ids
    )


def verify_plugin_experiment_inventory(
    envelope: BrokerPluginExperimentInventoryV1,
    experiment: ReconstructionExperimentManifestV1,
) -> None:
    if type(
        envelope
    ) is not BrokerPluginExperimentInventoryV1 or envelope.experiment_id != _experiment_identity(
        experiment
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_BINDING
        )
    BrokerPluginExperimentInventoryV1.from_json(envelope.to_json())
