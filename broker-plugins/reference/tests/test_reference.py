"""Standalone stdlib tests against the independently installed reference wheel."""

import ast
import hashlib
import runpy
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerEventV1,
    BrokerReasonCode,
)

from histdatacom_reference_broker import plugin

PROJECT = Path(__file__).resolve().parents[1]


class NoEffects:
    def __getattr__(self, name):
        raise AssertionError("reference scenario attempted a host resource")


def vector(mode):
    instance = plugin.factory(NoEffects())
    session = instance.open_session({"mode": mode})
    return instance, session, tuple(instance.iter_events(session))


def semantic_vector(events):
    # CONFORMANCE_ABI.md excludes physical session/clock identities from the
    # golden market values while retaining each actual event's own binding.
    return tuple(
        (
            event.kind,
            event.instrument,
            event.quote,
            event.source_time,
            event.gap,
            event.diagnostic,
            event.raw_provenance,
        )
        for event in events
    )


class ReferenceTests(unittest.TestCase):
    def test_installed_runtime_import_boundary(self):
        path = Path(plugin.__file__).resolve()
        self.assertIn("site-packages", path.parts)
        tree = ast.parse(path.read_text("utf-8"))
        allowed = {
            "__future__",
            "hashlib",
            "time",
            "collections.abc",
            "typing",
            "uuid",
            "histdatacom.broker_plugins",
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                self.assertTrue(all(alias.name in allowed for alias in node.names))
            elif isinstance(node, ast.ImportFrom):
                self.assertIn(node.module, allowed)

    def test_finite_vectors_are_deterministic_and_roundtrip(self):
        instance, session, first = vector("finite")
        _, other_session, second = vector("finite")
        self.assertNotEqual(session.instance_nonce, other_session.instance_nonce)
        self.assertNotEqual(session.artifact_id, other_session.artifact_id)
        self.assertEqual(semantic_vector(first), semantic_vector(second))
        self.assertEqual(
            [event.kind for event in first],
            [BrokerEventKind.HEALTH] + [BrokerEventKind.QUOTE] * 3,
        )
        self.assertEqual([event.sequence for event in first], list(range(4)))
        for event in first:
            self.assertEqual(BrokerEventV1.from_json(event.to_json()), event)
        self.assertEqual(
            [(event.quote.bid, event.quote.ask) for event in first[1:]],
            [("1.10000", "1.20000")] * 3,
        )
        instance.subscribe(session, ("EURUSD",))
        instance.unsubscribe(session, ("EURUSD",))
        instance.close_session(session)
        self.assertTrue(instance.closed)

    def test_reopening_creates_a_fresh_session_with_bound_events(self):
        instance = plugin.factory(NoEffects())
        first_session = instance.open_session({"mode": "finite"})
        first = tuple(instance.iter_events(first_session))
        instance.close_session(first_session)
        second_session = instance.open_session({"mode": "finite"})
        self.assertFalse(instance.closed)
        second = tuple(instance.iter_events(second_session))
        self.assertNotEqual(first_session.instance_nonce, second_session.instance_nonce)
        self.assertNotEqual(first_session.artifact_id, second_session.artifact_id)
        self.assertEqual(first_session.metadata_id, second_session.metadata_id)
        for session, events in ((first_session, first), (second_session, second)):
            self.assertRegex(session.instance_nonce, r"\A[a-f0-9]{32}\Z")
            self.assertTrue(
                all(event.session_id == session.artifact_id for event in events)
            )
            self.assertEqual([event.sequence for event in events], list(range(4)))
        self.assertEqual(semantic_vector(first), semantic_vector(second))
        instance.close_session(second_session)
        self.assertTrue(instance.closed)

    def test_unsupported_configuration_and_instrument_refuse(self):
        for configuration in (
            {},
            {"mode": "unknown"},
            {"mode": 1},
            {"mode": "finite", "secret": "never"},
        ):
            with (
                self.subTest(configuration=configuration),
                self.assertRaisesRegex(ValueError, "unsupported reference scenario"),
            ):
                plugin.factory(NoEffects()).open_session(configuration)
        instance, session, _ = vector("finite")
        with self.assertRaises(ValueError):
            instance.subscribe(session, ("GBPUSD",))

    def test_optional_values_are_explicit_only_in_their_scenarios(self):
        _, _, finite = vector("finite")
        self.assertTrue(
            all(
                event.quote.bid_size is None and event.raw_provenance is None
                for event in finite[1:]
            )
        )
        _, _, sizes = vector("sizes")
        self.assertTrue(all(event.quote.bid_size.value == "2" for event in sizes))
        _, _, raw = vector("raw")
        self.assertTrue(
            all(
                event.raw_provenance.sha256 == hashlib.sha256(b"generated").hexdigest()
                for event in raw
            )
        )

    def test_gap_reconnect_heartbeat_shapes(self):
        _, _, gaps = vector("gap")
        self.assertEqual(gaps[0].gap.missing_message_count, 5)
        _, _, reconnect = vector("reconnect")
        self.assertEqual(
            [event.kind for event in reconnect],
            [BrokerEventKind.QUOTE] * 3 + [BrokerEventKind.DISCONNECTED],
        )
        _, _, heartbeat = vector("heartbeat")
        self.assertEqual(
            [event.kind for event in heartbeat],
            [BrokerEventKind.HEARTBEAT, BrokerEventKind.QUOTE] * 3,
        )

    def test_honest_health_has_no_source_clock_claim(self):
        with (
            patch.object(plugin.time, "time_ns", return_value=1000),
            patch.object(plugin.time, "monotonic_ns", return_value=2000),
        ):
            _, _, events = vector("honest-health")
        self.assertTrue(all(event.source_time is None for event in events[1:]))
        self.assertTrue(all(event.receive_time.utc_ns == 1000 for event in events[1:]))

    def test_advisory_summaries_are_closed_reason_values(self):
        # Free-form diagnostic prose is conservatively RAW_PAYLOAD, even for
        # otherwise generated health/gap events. Do not widen provider rights.
        for mode, reason in (
            ("finite", BrokerReasonCode.HEALTHY),
            ("honest-health", BrokerReasonCode.HEALTHY),
            ("gap", BrokerReasonCode.SOURCE_GAP),
            ("healthy-gap", BrokerReasonCode.SOURCE_GAP),
        ):
            with self.subTest(mode=mode):
                _, _, events = vector(mode)
                self.assertIs(events[0].diagnostic.code, reason)
                self.assertEqual(events[0].diagnostic.summary, reason.value)

    def test_adversarial_scenarios_remain_explicit(self):
        _, _, events = vector("duplicate")
        self.assertEqual(events[0].source_time, events[1].source_time)
        _, _, malformed = vector("malformed")
        self.assertIs(type(malformed[0]), dict)
        _, _, spread = vector("invalid-spread")
        self.assertGreater(float(spread[0].quote.bid), float(spread[0].quote.ask))
        for mode in ("exception", "secret-leak"):
            with self.assertRaises(RuntimeError):
                plugin.factory(NoEffects()).open_session({"mode": mode})

    def test_blocking_modes_are_tested_without_sleeping(self):
        with patch.object(plugin.time, "sleep") as sleep:
            vector("next-block")
            instance, session, _ = vector("close-block")
            instance.close_session(session)
        self.assertEqual([call.args for call in sleep.call_args_list], [(300,), (300,)])

    def test_public_metadata_discovery_is_candidate_bound(self):
        from histdatacom.broker_plugin_conformance import (
            BrokerConformanceDriverV1,
            inspect_broker_conformance_driver,
        )
        from histdatacom.broker_plugin_permissions import (
            read_installed_broker_permissions,
        )
        from histdatacom.broker_plugin_registry import discover_broker_plugins

        inventory = discover_broker_plugins()
        self.assertFalse(inventory.diagnostics)
        candidate = next(
            item
            for item in inventory.candidates
            if item.registration.plugin_id == "org.histdatacom.reference-broker"
        )
        permissions = read_installed_broker_permissions(candidate)
        self.assertEqual(permissions.candidate_id, candidate.artifact_id)
        self.assertEqual(permissions.required_atoms, ("emit:health", "emit:quotes"))
        self.assertFalse(permissions.endpoints)
        driver = BrokerConformanceDriverV1.from_json(
            (PROJECT / "conformance/driver.json").read_text("ascii")
        )
        self.assertEqual(inspect_broker_conformance_driver(inventory, driver), driver)

    def test_host_preflight_refuses_missing_and_unsupported_sdk(self):
        check = runpy.run_path(str(PROJECT / "tools/check_host.py"))["check_host"]
        self.assertEqual(check()["sdk_version"], "1.0.0")
        with (
            patch("importlib.import_module", side_effect=ModuleNotFoundError),
            self.assertRaisesRegex(RuntimeError, "reviewed development host"),
        ):
            check()

        from histdatacom import broker_plugins

        with (
            patch.object(broker_plugins, "BROKER_PLUGIN_SDK_VERSION", "9.0.0"),
            self.assertRaisesRegex(RuntimeError, "unsupported public broker SDK"),
        ):
            check()

    def test_host_preflight_refuses_missing_public_api(self):
        from histdatacom import broker_plugin_provenance

        check = runpy.run_path(str(PROJECT / "tools/check_host.py"))["check_host"]
        with (
            patch.object(broker_plugin_provenance, "BrokerProvenanceSealV1", None),
            self.assertRaisesRegex(
                RuntimeError, "required public broker API unavailable"
            ),
        ):
            check()

    def test_host_preflight_pins_catalog_semantics_not_only_sdk(self):
        from histdatacom import broker_plugin_conformance as kit

        check = runpy.run_path(str(PROJECT / "tools/check_host.py"))["check_host"]
        actual = kit.broker_conformance_catalog()
        self.assertEqual(check()["catalog_version"], "2.0.0")
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
                self.assertRaisesRegex(RuntimeError, "fixture ABI/catalog"),
            ):
                check()

    def test_metadata_lock_refuses_changed_source_and_resources(self):
        functions = runpy.run_path(str(PROJECT / "tools/generate_metadata.py"))
        check = functions["check_metadata"]
        check(PROJECT)
        with tempfile.TemporaryDirectory() as temporary:
            copied = Path(temporary) / "project"
            shutil.copytree(
                PROJECT,
                copied,
                ignore=shutil.ignore_patterns(
                    "build", "dist", "__pycache__", "*.egg-info", "*.log"
                ),
            )
            for relative in (
                "src/histdatacom_reference_broker/plugin.py",
                "conformance/driver.json",
            ):
                target = copied / relative
                original = target.read_bytes()
                target.write_bytes(original + b"\n")
                with self.assertRaisesRegex(RuntimeError, "stale metadata"):
                    check(copied)
                target.write_bytes(original)
                check(copied)


if __name__ == "__main__":
    unittest.main()
