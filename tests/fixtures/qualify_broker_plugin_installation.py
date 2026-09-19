"""Run inside a clean installed-host venv; pip never contacts a registry."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from histdatacom.broker_plugin_registry import (
    BrokerPluginInventoryV1,
    discover_broker_plugins,
    select_broker_plugin,
)


def main() -> None:
    wheel = Path(sys.argv[1]).resolve()
    before = discover_broker_plugins()
    assert not before.candidates and not before.diagnostics
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            str(wheel),
        ],
        check=True,
    )
    installed = discover_broker_plugins()
    selected = select_broker_plugin(installed, plugin_id="org.example.fixture")
    assert (
        selected.registration.distribution_name == "histdatacom-fixture-broker"
    )
    assert BrokerPluginInventoryV1.from_json(installed.to_json()) == installed
    for arguments in (
        ("list",),
        ("inspect", "--provider", "example"),
        (
            "select",
            "--plugin-id",
            "org.example.fixture",
            "--version",
            "==1.0.0",
        ),
    ):
        process = subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "histdatacom.broker_plugin_registry",
                *arguments,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert "PLUGIN_IMPORT_CANARY" not in process.stdout + process.stderr
        json.loads(process.stdout)
    assert not any(
        name == "fixture_broker" or name.startswith("fixture_broker.")
        for name in sys.modules
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "uninstall",
            "--yes",
            "histdatacom-fixture-broker",
        ],
        check=True,
    )
    assert discover_broker_plugins().to_json() == before.to_json()
    print(
        json.dumps(
            {
                "status": "passed",
                "installed_candidate_id": selected.artifact_id,
                "same_process_refresh": True,
                "plugin_imported": False,
                "uninstall_restored_inventory": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
