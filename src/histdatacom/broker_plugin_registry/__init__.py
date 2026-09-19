"""Public host-side, metadata-only installed broker plugin registry."""

from .bindings import (
    BrokerPluginExperimentInventoryV1,
    bind_plugin_inventory_to_experiment,
    capture_plugin_inventory_metadata,
    verify_capture_plugin_inventory,
    verify_plugin_experiment_inventory,
)
from .contracts import (
    BROKER_PLUGIN_ENTRY_POINT_GROUP,
    MAX_INVENTORY_BYTES,
    MAX_PLUGIN_REGISTRATIONS,
    MAX_REGISTRATION_BYTES,
    BrokerPluginCandidateV1,
    BrokerPluginDiscoveryDiagnosticV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistrationV1,
    BrokerPluginRegistryError,
    BrokerPluginRegistryReason,
    canonical_plugin_registry_json,
    normalized_distribution_name,
)
from .discovery import discover_broker_plugins, registration_resource_path
from .selection import inspect_broker_plugins, select_broker_plugin
from .storage import read_plugin_inventory, write_plugin_inventory

__all__ = [
    "BROKER_PLUGIN_ENTRY_POINT_GROUP",
    "MAX_INVENTORY_BYTES",
    "MAX_PLUGIN_REGISTRATIONS",
    "MAX_REGISTRATION_BYTES",
    "BrokerPluginCandidateV1",
    "BrokerPluginDiscoveryDiagnosticV1",
    "BrokerPluginExperimentInventoryV1",
    "BrokerPluginInventoryV1",
    "BrokerPluginRegistrationV1",
    "BrokerPluginRegistryError",
    "BrokerPluginRegistryReason",
    "bind_plugin_inventory_to_experiment",
    "canonical_plugin_registry_json",
    "capture_plugin_inventory_metadata",
    "discover_broker_plugins",
    "inspect_broker_plugins",
    "normalized_distribution_name",
    "read_plugin_inventory",
    "registration_resource_path",
    "select_broker_plugin",
    "verify_capture_plugin_inventory",
    "verify_plugin_experiment_inventory",
    "write_plugin_inventory",
]
