"""Generate canonical wheel resources from PUBLIC contracts; no host factory.

Run from this project after changing plugin.py/version/schema. --check is read-only.
No timestamps, local paths, private revision URLs or credentials enter artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def documents() -> dict[Path, str]:
    from check_host import check_host

    check_host()
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

    from histdatacom_broker_starter.plugin import (
        CAPABILITIES,
        CONFIGURATION_SCHEMA,
        DISPLAY_NAME,
        PLUGIN_ID,
        PLUGIN_VERSION,
        PROVIDER_ID,
        SCENARIOS,
    )

    entry = "histdatacom_broker_starter.plugin:factory"
    registration = BrokerPluginRegistrationV1(
        PLUGIN_ID,
        PLUGIN_VERSION,
        DISPLAY_NAME,
        "histdatacom-broker-starter",
        PLUGIN_VERSION,
        entry,
        "1.0.0",
        "2.0.0",
        (PROVIDER_ID,),
        CAPABILITIES,
    )
    registration_text = registration.to_json()
    implementation = ROOT / "src/histdatacom_broker_starter/plugin.py"
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration_text.encode("ascii")).hexdigest(),
        hashlib.sha256(implementation.read_bytes()).hexdigest(),
    )
    permission = BrokerPermissionManifestV1(
        candidate.artifact_id,
        "histdatacom-broker-starter",
        PLUGIN_VERSION,
        "1.0.0",
        (PROVIDER_ID,),
        ("emit:health", "emit:quotes"),
        resource_abi="none",
    )
    driver = BrokerConformanceDriverV1(
        candidate.artifact_id,
        permission.artifact_id,
        PROVIDER_ID,
        CONFIGURATION_SCHEMA.to_json(),
        ("EURUSD",),
        tuple(
            BrokerConformanceScenarioV1(
                mode, json.dumps({"mode": mode}, sort_keys=True, separators=(",", ":"))
            )
            for mode in SCENARIOS
        ),
    )
    return {
        ROOT / "src" / registration_resource_path(PLUGIN_ID, entry): registration_text,
        ROOT / "src" / permission_resource_path(PLUGIN_ID, entry): permission.to_json(),
        ROOT / "conformance/driver.json": driver.to_json(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    outputs = documents()
    hashes = {
        path.relative_to(ROOT)
        .as_posix(): hashlib.sha256(text.encode("ascii"))
        .hexdigest()
        for path, text in outputs.items()
    }
    for relative in (
        "src/histdatacom_broker_starter/plugin.py",
        "src/histdatacom_broker_starter/__init__.py",
        "pyproject.toml",
        "setup.py",
        "README.md",
        "docs/CONFORMANCE_ABI.md",
        "tools/generate_metadata.py",
        "tools/check_host.py",
    ):
        hashes[relative] = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
    outputs[ROOT / "metadata-lock.json"] = (
        json.dumps(hashes, sort_keys=True, indent=2) + "\n"
    )
    for path, text in outputs.items():
        if args.check:
            if not path.is_file() or path.read_text("ascii") != text:
                raise SystemExit(
                    "stale metadata: run python tools/generate_metadata.py"
                )
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="ascii")
    print("metadata verified" if args.check else "metadata generated")


if __name__ == "__main__":
    main()
