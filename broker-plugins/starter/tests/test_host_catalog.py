"""Pure compatibility checks; these do not execute conformance cases."""

import runpy
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from histdatacom import broker_plugin_conformance as kit


class HostCatalogTests(unittest.TestCase):
    def test_exact_catalog_version_and_identity_are_required(self):
        project = Path(__file__).resolve().parents[1]
        check = runpy.run_path(str(project / "tools/check_host.py"))["check_host"]
        actual = kit.broker_conformance_catalog()
        self.assertEqual(check()["catalog_version"], "2.0.0")
        self.assertEqual(check()["catalog_id"], actual.artifact_id)
        for version, identity in (
            ("1.0.0", actual.artifact_id),
            (actual.version, "broker-conformance-catalog:sha256:" + "0" * 64),
        ):
            with (
                self.subTest(version=version, identity=identity),
                patch.object(
                    kit,
                    "broker_conformance_catalog",
                    return_value=SimpleNamespace(version=version, artifact_id=identity),
                ),
                self.assertRaisesRegex(SystemExit, "reviewed conformance catalog"),
            ):
                check()
