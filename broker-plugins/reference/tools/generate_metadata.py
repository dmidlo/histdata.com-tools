"""Regenerate public artifacts explicitly; normal builds only verify the lock.

The build-time checker is stdlib-only. Artifact generation uses documented
public APIs, never the host's generated-wheel builder or internal hooks.
"""

import argparse
import hashlib
import json
import runpy
from pathlib import Path

PLUGIN = "org.histdatacom.reference-broker"
ENTRY = "histdatacom_reference_broker.plugin:factory"
PACKAGE = "histdatacom_reference_broker"
CATALOG_ID = (
    "broker-conformance-catalog:sha256:"
    "7c4965dad269a282167e08b1b04a7caaa6feaf4fc78da4d87b6229aef008b705"
)
INPUTS = (
    "pyproject.toml",
    "setup.py",
    "MANIFEST.in",
    "README.md",
    "DEVELOPER_GUIDE.md",
    "OPERATOR.md",
    "CONFORMANCE_ABI.md",
    "INTEGRATED_QUALIFICATION.md",
    ".github/workflows/ci.yml",
    "tools/check_host.py",
    "tools/capture_example.py",
    "tools/generate_metadata.py",
    "tests/test_reference.py",
    "tests/test_capture_example.py",
    "src/histdatacom_reference_broker/__init__.py",
    "src/histdatacom_reference_broker/plugin.py",
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_metadata(root):
    try:
        lock = json.loads((root / "metadata-lock.json").read_text("ascii"))
        if set(lock) != {"format", "files"} or lock["format"] != 1:
            raise ValueError("invalid lock")
        expected_outputs = {
            f"src/{PACKAGE}/_histdatacom_broker_plugins/{PLUGIN}.json",
            f"src/{PACKAGE}/_histdatacom_broker_permissions/{PLUGIN}.json",
            "conformance/driver.json",
        }
        if set(lock["files"]) != set(INPUTS) | expected_outputs:
            raise ValueError("incomplete lock")
        if any(digest(root / name) != sha for name, sha in lock["files"].items()):
            raise ValueError("source or resource changed")
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise RuntimeError(
            "stale metadata: run python tools/generate_metadata.py with the reviewed host"
        ) from error


def generate(root):
    runpy.run_path(str(root / "tools" / "check_host.py"))["check_host"]()
    from histdatacom.broker_plugin_conformance import (
        BrokerConformanceDriverV1,
        BrokerConformanceScenarioV1,
        broker_conformance_catalog,
    )
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionManifestV1,
        permission_resource_path,
    )
    from histdatacom.broker_plugin_registry import (
        BrokerPluginCandidateV1,
        BrokerPluginRegistrationV1,
        registration_resource_path,
    )
    from histdatacom.broker_plugins import (
        BrokerConfigurationFieldV1,
        BrokerConfigurationSchemaV1,
        BrokerConfigurationType,
    )

    catalog = broker_conformance_catalog()
    if catalog.version != "2.0.0" or catalog.artifact_id != CATALOG_ID:
        raise RuntimeError(
            "conformance catalog changed; review CONFORMANCE_ABI.md before regenerating"
        )
    capabilities = (
        "connection.v1",
        "events.v1",
        "gaps.v1",
        "health.v1",
        "heartbeat.v1",
        "instruments.price-increment.v1",
        "instruments.v1",
        "quotes.v1",
        "raw-hashes.v1",
        "session.v1",
        "sizes.quoted.v1",
        "timestamps.broker-event.v1",
        "timestamps.receive.v1",
    )
    registration = BrokerPluginRegistrationV1(
        PLUGIN,
        "0.1.0",
        "Deterministic reference broker",
        "histdatacom-reference-broker",
        "0.1.0",
        ENTRY,
        "1.0.0",
        "2.0.0",
        ("generated-reference",),
        tuple(sorted(capabilities)),
    )
    raw = registration.to_json().encode("ascii")
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(raw).hexdigest(),
        digest(root / "src" / PACKAGE / "plugin.py"),
    )
    permissions = BrokerPermissionManifestV1(
        candidate.artifact_id,
        "histdatacom-reference-broker",
        "0.1.0",
        "1.0.0",
        ("generated-reference",),
        ("emit:health", "emit:quotes"),
        ("emit:sizes", "raw_payload:emit"),
        resource_abi="host_resources_v1",
    )
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "mode",
                BrokerConfigurationType.STRING,
                "Canonical generated scenario",
            ),
        )
    )
    scenarios = tuple(
        BrokerConformanceScenarioV1(
            name,
            json.dumps({"mode": name}, sort_keys=True, separators=(",", ":")),
        )
        for name in sorted({case.scenario_id for case in catalog.cases})
    )
    driver = BrokerConformanceDriverV1(
        candidate.artifact_id,
        permissions.artifact_id,
        "generated-reference",
        schema.to_json(),
        ("EURUSD",),
        scenarios,
    )
    outputs = {
        "src/" + registration_resource_path(PLUGIN, ENTRY): raw,
        "src/"
        + permission_resource_path(PLUGIN, ENTRY): permissions.to_json().encode(
            "ascii"
        ),
        "conformance/driver.json": driver.to_json().encode("ascii"),
    }
    for name, data in outputs.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    lock = {
        "format": 1,
        "files": {
            name: digest(root / name) for name in sorted(set(INPUTS) | outputs.keys())
        },
    }
    (root / "metadata-lock.json").write_text(
        json.dumps(lock, sort_keys=True, indent=2) + "\n", encoding="ascii"
    )
    check_metadata(root)
    print(
        json.dumps(
            {
                "candidate_id": candidate.artifact_id,
                "permissions_id": permissions.artifact_id,
                "driver_id": driver.artifact_id,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    if arguments.check:
        check_metadata(project)
    else:
        generate(project)
