"""Exercise metadata-only public discovery, without importing the entry module."""

import subprocess
import sys
import unittest

from histdatacom.broker_plugin_permissions import read_installed_broker_permissions
from histdatacom.broker_plugin_registry import (
    discover_broker_plugins,
    select_broker_plugin,
)


class InstalledMetadataTests(unittest.TestCase):
    def test_discovery_does_not_import_plugin_in_fresh_interpreter(self):
        code = (
            "import sys; "
            "from histdatacom.broker_plugin_registry import "
            "discover_broker_plugins, select_broker_plugin; "
            "from histdatacom.broker_plugin_permissions import "
            "read_installed_broker_permissions; "
            "candidate=select_broker_plugin(discover_broker_plugins(), "
            "plugin_id='org.example.starter'); "
            "read_installed_broker_permissions(candidate); "
            "assert 'histdatacom_broker_starter.plugin' not in sys.modules"
        )
        subprocess.run([sys.executable, "-I", "-B", "-c", code], check=True, timeout=10)

    def test_exact_installed_candidate_and_permission_manifest(self):
        before = "histdatacom_broker_starter.plugin" in sys.modules
        inventory = discover_broker_plugins()
        selected = select_broker_plugin(
            inventory, plugin_id="org.example.starter", version_constraint="==0.1.0"
        )
        declaration = read_installed_broker_permissions(selected)
        self.assertEqual(declaration.candidate_id, selected.artifact_id)
        self.assertEqual(declaration.resource_abi, "none")
        self.assertEqual(declaration.required_atoms, ("emit:health", "emit:quotes"))
        self.assertEqual(declaration.optional_atoms, ())
        self.assertEqual(declaration.endpoints, ())
        self.assertEqual(declaration.secret_profiles, ())
        self.assertEqual(declaration.caches, ())
        self.assertEqual(before, "histdatacom_broker_starter.plugin" in sys.modules)


if __name__ == "__main__":
    unittest.main()
