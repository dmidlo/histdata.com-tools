"""Fail early on unsupported SDK or missing unreleased public host surfaces."""

from __future__ import annotations

import importlib
import importlib.metadata
import json

CATALOG_VERSION = "2.0.0"
CATALOG_ID = (
    "broker-conformance-catalog:sha256:"
    "7c4965dad269a282167e08b1b04a7caaa6feaf4fc78da4d87b6229aef008b705"
)


def check_host() -> dict[str, str]:
    try:
        sdk = importlib.import_module("histdatacom.broker_plugins")
        if sdk.BROKER_PLUGIN_SDK_VERSION != "1.0.0":
            raise RuntimeError("unsupported SDK version")
        required = {
            "broker_plugin_registry": (
                "BrokerPluginCandidateV1",
                "registration_resource_path",
            ),
            "broker_plugin_permissions": (
                "BrokerPermissionManifestV1",
                "permission_resource_path",
            ),
            "broker_plugin_conformance": (
                "broker_conformance_catalog",
                "BrokerConformanceDriverV1",
                "plan_broker_conformance",
                "verify_broker_conformance",
            ),
            "broker_plugin_provenance": ("read_lifecycle_capture_provenance",),
        }
        for package, names in required.items():
            module = importlib.import_module("histdatacom." + package)
            for name in names:
                if getattr(module, name, None) is None:
                    raise RuntimeError("missing required public host API")
        kit = importlib.import_module("histdatacom.broker_plugin_conformance")
        catalog = kit.broker_conformance_catalog()
        if catalog.version != CATALOG_VERSION or catalog.artifact_id != CATALOG_ID:
            raise RuntimeError("review the changed public fixture ABI/catalog")
        return {
            "host_distribution_version": importlib.metadata.version("histdatacom"),
            "sdk_version": sdk.BROKER_PLUGIN_SDK_VERSION,
            "catalog_version": catalog.version,
            "catalog_id": catalog.artifact_id,
            "compatibility": (
                "public API presence only; not conformance or qualification"
            ),
        }
    except (ImportError, AttributeError, RuntimeError):
        raise SystemExit(
            "Install the reviewed unreleased-v3 host wheel; supported public "
            "SDK 1.0.0, required host APIs or reviewed conformance catalog differ."
        ) from None


if __name__ == "__main__":
    print(json.dumps(check_host(), sort_keys=True))
