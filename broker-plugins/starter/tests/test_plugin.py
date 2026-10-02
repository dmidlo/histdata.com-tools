"""Standalone SDK tests. Install this project's wheel before running."""

import ast
import importlib.metadata
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerEventV1,
    BrokerPluginError,
    BrokerPluginV1,
    validate_broker_event_stream,
)

from histdatacom_broker_starter import plugin


class PluginTests(unittest.TestCase):
    def opened(self, mode="finite"):
        instance = plugin.factory()
        session = instance.open_session({"mode": mode})
        instance.subscribe(session, ("EURUSD",))
        self.addCleanup(instance.close_session, session)
        return instance, session

    def test_structural_protocol_and_installed_version(self):
        self.assertIsInstance(plugin.factory(), BrokerPluginV1)
        self.assertEqual(
            importlib.metadata.version("histdatacom-broker-starter"), "0.1.0"
        )

    def test_finite_canonical_lifecycle(self):
        instance, session = self.opened()
        self.assertEqual(instance.instruments(session), (plugin.INSTRUMENT,))
        events = tuple(
            validate_broker_event_stream(instance.iter_events(session), session)
        )
        self.assertEqual([e.sequence for e in events], list(range(4)))
        self.assertEqual(events[0].kind, BrokerEventKind.HEALTH)
        quotes = [e for e in events if e.quote is not None]
        self.assertEqual(len(quotes), 3)
        self.assertEqual(quotes[0].quote.bid, "1.10000")
        self.assertIsNone(quotes[0].quote.bid_size)
        self.assertIsNone(quotes[0].quote.activity)
        for event in events:
            self.assertEqual(BrokerEventV1.from_json(event.to_json()), event)
            self.assertIsNone(event.raw_provenance)
            self.assertEqual(event.extensions, ())
        instance.unsubscribe(session, ("EURUSD",))
        tail = tuple(
            validate_broker_event_stream(
                instance.iter_events(session), session, starting_sequence=4
            )
        )
        self.assertEqual(tail, ())
        instance.close_session(session)
        instance.close_session(session)

    def test_configuration_is_closed_and_does_not_echo_values(self):
        for config in (
            {},
            {"mode": True},
            {"mode": "unknown"},
            {"mode": "finite", "secret": "private-test-marker"},
        ):
            with (
                self.subTest(keys=tuple(config)),
                self.assertRaises(BrokerPluginError) as caught,
            ):
                plugin.factory().open_session(config)
            self.assertNotIn("private-test-marker", str(caught.exception))
            self.assertNotIn("unknown", str(caught.exception))

    def test_exact_symbol_and_session_required(self):
        instance, session = self.opened()
        for symbols in (("EUR/USD",), ("USDJPY",), ("EURUSD", "EURUSD")):
            with self.assertRaises(BrokerPluginError):
                instance.subscribe(session, symbols)
        with self.assertRaises(BrokerPluginError):
            instance.instruments(replace(session, instance_nonce="a" * 32))

    def test_duplicate_is_not_unchanged_price(self):
        values = {}
        for mode in ("duplicate", "unchanged"):
            instance, session = self.opened(mode)
            quotes = [event for event in instance.iter_events(session) if event.quote]
            self.assertNotEqual(quotes[0].sequence, quotes[1].sequence)
            self.assertEqual(quotes[0].quote, quotes[1].quote)
            values[mode] = [event.source_time for event in quotes]
        self.assertEqual(values["duplicate"][0], values["duplicate"][1])
        self.assertNotEqual(values["unchanged"][0], values["unchanged"][1])

    def test_negative_scenarios_are_rejected_by_public_sdk(self):
        for mode in ("bad-sequence", "bad-clock", "invalid-spread", "malformed"):
            with self.subTest(mode=mode):
                instance, session = self.opened(mode)
                with self.assertRaises((ValueError, TypeError)):
                    tuple(
                        BrokerEventV1.from_json(event.to_json())
                        for event in validate_broker_event_stream(
                            instance.iter_events(session), session
                        )
                    )

    def test_open_exceptions_are_deliberate_synthetic_stimuli(self):
        for mode, expected in (
            ("exception", "generated open failure"),
            ("secret-leak", "generated-conformance-secret-not-for-retention"),
        ):
            with self.subTest(mode=mode), self.assertRaises(RuntimeError) as caught:
                plugin.factory().open_session({"mode": mode})
            self.assertEqual(str(caught.exception), expected)

    def test_each_nonblocking_positive_scenario_has_exact_shape(self):
        expected = {
            "burst": 4,
            "reconnect": 4,
            "stale": 3,
            "healthy-duplicate": 3,
            "healthy-reorder": 3,
            "heartbeat": 6,
            "healthy-delay": 6,
            "honest-health": 4,
            "gap": 4,
            "healthy-gap": 4,
        }
        for mode, count in expected.items():
            with self.subTest(mode=mode):
                instance, session = self.opened(mode)
                events = tuple(
                    validate_broker_event_stream(instance.iter_events(session), session)
                )
                self.assertEqual(len(events), count)
                if mode.startswith("healthy-"):
                    self.assertFalse(
                        any(e.kind is BrokerEventKind.HEALTH for e in events)
                    )
                if mode == "honest-health":
                    self.assertTrue(all(e.source_time is None for e in events))
                if mode == "reconnect":
                    self.assertEqual(events[-1].kind, BrokerEventKind.DISCONNECTED)

    def test_reopening_has_new_identity(self):
        instance, first = self.opened()
        instance.close_session(first)
        second = instance.open_session({"mode": "finite"})
        self.assertNotEqual(first.instance_nonce, second.instance_nonce)
        self.assertNotEqual(first.artifact_id, second.artifact_id)
        instance.close_session(second)

    def test_independent_factories_use_fresh_nonces_at_the_same_open_time(self):
        _, first = self.opened()
        _, second = self.opened()
        self.assertNotEqual(first.instance_nonce, second.instance_nonce)
        self.assertEqual(first.opened_at_utc_ns, second.opened_at_utc_ns)

    def test_blocking_stimuli_are_tested_without_real_waits(self):
        with patch("histdatacom_broker_starter.plugin.time.sleep") as sleep:
            instance, session = self.opened("next-block")
            self.assertEqual(tuple(instance.iter_events(session)), ())
            sleep.assert_called_once_with(300)
        with patch("histdatacom_broker_starter.plugin.time.sleep") as sleep:
            instance, session = self.opened("close-block")
            self.assertEqual(len(tuple(instance.iter_events(session))), 3)
            instance.close_session(session)
            instance.close_session(session)
            sleep.assert_called_once_with(300)

    def test_runtime_imports_only_public_sdk_and_standard_library(self):
        source = Path(plugin.__file__).read_text("utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                self.assertIn(
                    node.module,
                    {
                        "__future__",
                        "collections.abc",
                        "dataclasses",
                        "typing",
                        "histdatacom.broker_plugins",
                    },
                )
            elif isinstance(node, ast.Import):
                self.assertTrue(
                    all(alias.name in {"uuid", "time"} for alias in node.names)
                )


if __name__ == "__main__":
    unittest.main()
