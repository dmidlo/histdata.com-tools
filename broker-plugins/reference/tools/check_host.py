"""Refuse absent/unsupported public APIs before development or qualification."""

import importlib
import json
from importlib import metadata

CATALOG_VERSION = "2.0.0"
CATALOG_ID = (
    "broker-conformance-catalog:sha256:"
    "7c4965dad269a282167e08b1b04a7caaa6feaf4fc78da4d87b6229aef008b705"
)


def check_host():
    required = {
        "histdatacom.broker_plugins": (
            "BrokerPluginV1",
            "BrokerEventV1",
            "BrokerHostResourcesV1",
        ),
        "histdatacom.broker_plugin_registry": (
            "discover_broker_plugins",
            "BrokerPluginRegistrationV1",
            "BrokerPluginCandidateV1",
            "registration_resource_path",
        ),
        "histdatacom.broker_plugin_permissions": (
            "BrokerPermissionManifestV1",
            "read_installed_broker_permissions",
            "permission_resource_path",
            "BrokerPermissionAuthorityV1",
            "BrokerPermissionBindingV1",
            "BrokerPermissionGrantV1",
            "BrokerPermissionContextV1",
            "BrokerPermissionResourcesV1",
            "broker_permission_scope",
        ),
        "histdatacom.broker_plugin_capabilities": (
            "BrokerCapabilityWorkflowV1",
            "negotiate_broker_capabilities",
        ),
        "histdatacom.broker_plugin_policy": (
            "BrokerSDKInvocationV1",
            "BrokerProviderConfigurationV1",
            "BrokerProviderOutputContractV1",
            "provider_policy_scope",
        ),
        "histdatacom.broker_plugin_conformance": (
            "broker_conformance_catalog",
            "BrokerConformanceDriverV1",
            "BrokerConformanceScenarioV1",
            "plan_broker_conformance",
            "run_broker_conformance",
            "verify_broker_conformance",
        ),
        "histdatacom.broker_plugin_security": ("run_secure_broker_plugin",),
        "histdatacom.broker_plugin_lifecycle": (
            "replay_broker_lifecycle",
            "inspect_broker_lifecycle",
        ),
        "histdatacom.broker_plugin_provenance": (
            "read_lifecycle_capture_provenance",
            "BrokerProvenanceSealV1",
        ),
    }
    try:
        sdk = importlib.import_module("histdatacom.broker_plugins")
        if sdk.BROKER_PLUGIN_SDK_VERSION != "1.0.0":
            raise RuntimeError("unsupported public broker SDK; expected 1.0.0")
        for module_name, names in required.items():
            module = importlib.import_module(module_name)
            if any(not callable(getattr(module, name, None)) for name in names):
                raise RuntimeError(
                    "required public broker API unavailable: " + module_name
                )
        kit = importlib.import_module("histdatacom.broker_plugin_conformance")
        catalog = kit.broker_conformance_catalog()
        if catalog.version != CATALOG_VERSION or catalog.artifact_id != CATALOG_ID:
            raise RuntimeError("review the changed public fixture ABI/catalog")
        return {
            "host_distribution_version": metadata.version("histdatacom"),
            "sdk_version": sdk.BROKER_PLUGIN_SDK_VERSION,
            "catalog_version": catalog.version,
            "catalog_id": catalog.artifact_id,
            "public_api_preflight": "passed",
            "provider_qualification": "not_claimed",
        }
    except (
        ImportError,
        AttributeError,
        metadata.PackageNotFoundError,
    ) as error:
        raise RuntimeError(
            "reviewed development host with public broker APIs required"
        ) from error


if __name__ == "__main__":
    print(json.dumps(check_host(), sort_keys=True))
