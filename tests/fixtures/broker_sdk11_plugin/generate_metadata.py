"""Generate canonical registration/permission/driver resources before building.

Run with an installed development host, from this fixture project. This build
step adds only this distribution's own source, never host checkout paths. Use
--check against the frozen outputs. Actual acceptance uses the built wheel.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))


def documents():
    from histdatacom_generated_sdk11.plugin import (
        CAPABILITIES,
        PROVIDER,
        SCHEMA,
        VARIANTS,
        VERSION,
    )

    from histdatacom.broker_plugin_conformance import (
        BrokerConformanceDriverV1,
        BrokerConformanceScenarioV1,
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

    digest = hashlib.sha256(
        (ROOT / "src/histdatacom_generated_sdk11/plugin.py").read_bytes()
    ).hexdigest()
    outputs = {}
    for plugin_id, factory in VARIANTS.items():
        entry = "histdatacom_generated_sdk11.plugin:" + factory
        registration = BrokerPluginRegistrationV1(
            plugin_id,
            VERSION,
            "Generated independent SDK 1.1 fixture",
            "histdatacom-generated-sdk11",
            VERSION,
            entry,
            "1.1.0",
            "1.2.0",
            (PROVIDER,),
            CAPABILITIES,
        )
        candidate = BrokerPluginCandidateV1(
            registration,
            hashlib.sha256(registration.to_json().encode("ascii")).hexdigest(),
            digest,
        )
        permission = BrokerPermissionManifestV1(
            candidate.artifact_id,
            "histdatacom-generated-sdk11",
            VERSION,
            "1.1.0",
            (PROVIDER,),
            ("emit:health", "emit:quotes", "raw_payload:emit"),
            resource_abi="none",
        )
        driver = BrokerConformanceDriverV1(
            candidate.artifact_id,
            permission.artifact_id,
            PROVIDER,
            SCHEMA.to_json(),
            ("EURUSD",),
            (
                BrokerConformanceScenarioV1(
                    "honest-health", '{"mode":"honest-health"}'
                ),
            ),
        )
        outputs[ROOT / "src" / registration_resource_path(plugin_id, entry)] = (
            registration.to_json()
        )
        outputs[ROOT / "src" / permission_resource_path(plugin_id, entry)] = (
            permission.to_json()
        )
        outputs[ROOT / "conformance" / (plugin_id + ".json")] = driver.to_json()
    return outputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    for path, text in documents().items():
        if args.check:
            assert path.read_text("ascii") == text, str(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="ascii")


if __name__ == "__main__":
    main()
