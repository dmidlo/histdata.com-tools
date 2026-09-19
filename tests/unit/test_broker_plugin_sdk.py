"""Public SDK acceptance: immutable wire evidence and external-only plugins."""

from __future__ import annotations

import ast
import hashlib
import json
import runpy
import subprocess
import sys
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from histdatacom.broker_plugins import (
    BROKER_PLUGIN_SDK_VERSION,
    BrokerActivitySemantics,
    BrokerActivityV1,
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerExtensionV1,
    BrokerGapScope,
    BrokerGapV1,
    BrokerInstrumentV1,
    BrokerPluginError,
    BrokerPluginMetadataV1,
    BrokerQuoteV1,
    BrokerRawProvenanceV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSizeSemantics,
    BrokerSizeV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
    canonical_broker_sdk_json,
    normalize_broker_instrument,
    validate_broker_event_stream,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/broker_sdk_external.py"


def _metadata() -> BrokerPluginMetadataV1:
    return BrokerPluginMetadataV1("org.example.fx", "2.4.1", "Fixture FX")


def _session() -> BrokerSessionV1:
    return BrokerSessionV1(_metadata().artifact_id, "a" * 32, 100, "process-1")


def _diagnostic(
    code: BrokerReasonCode = BrokerReasonCode.SOURCE_GAP,
) -> BrokerDiagnosticV1:
    return BrokerDiagnosticV1(
        code, BrokerDiagnosticSeverity.WARNING, "Public fixture reason"
    )


def _quote() -> BrokerQuoteV1:
    return BrokerQuoteV1(
        symbol="EURUSD",
        bid="1.10000",
        ask="1.10015",
        bid_size=BrokerSizeV1("100000", BrokerSizeSemantics.QUOTED_SIZE, "EUR"),
        activity=BrokerActivityV1(
            "2", BrokerActivitySemantics.MESSAGE_COUNT, "message"
        ),
    )


def _event(sequence: int = 0) -> BrokerEventV1:
    return BrokerEventV1(
        session_id=_session().artifact_id,
        sequence=sequence,
        kind=BrokerEventKind.QUOTE,
        connection_id="connection-1",
        instrument="EURUSD",
        quote=_quote(),
        source_time=BrokerSourceTimeV1(
            1_000, 100, BrokerSourceTimeSemantics.BROKER_EVENT
        ),
        receive_time=BrokerReceiveTimeV1(
            2_000 + sequence, 100 + sequence, "process-1"
        ),
        raw_provenance=BrokerRawProvenanceV1(
            "c" * 64, 50, "application/json", "policy-fixture-v1"
        ),
        extensions=(
            BrokerExtensionV1(
                "org.example.telemetry", '{"flags":[1,true,null]}'
            ),
        ),
    )


def test_every_public_artifact_round_trips_and_binds_every_field() -> None:
    event = _event()
    field = BrokerConfigurationFieldV1(
        "endpoint",
        BrokerConfigurationType.STRING,
        "Public environment",
        choices=("demo", "production"),
    )
    artifacts = (
        _metadata(),
        _session(),
        field,
        BrokerConfigurationSchemaV1((field,)),
        BrokerInstrumentV1("EURUSD", "EUR/USD.demo", "EUR", "USD", "0.00001"),
        event,
        event.quote,
        event.quote.bid_size,
        event.quote.activity,
        event.source_time,
        event.receive_time,
        event.raw_provenance,
        event.extensions[0],
        _diagnostic(),
        BrokerGapV1(BrokerGapScope.TRANSPORT, 100, 200, 3),
    )
    for artifact in artifacts:
        assert artifact is not None
        payload = artifact.to_dict()
        restored = type(artifact).from_json(artifact.to_json())
        assert restored == artifact
        assert restored.to_json() == artifact.to_json()
        identity_payload = dict(payload)
        identity_payload.pop("artifact_id")
        expected = hashlib.sha256(
            canonical_broker_sdk_json(identity_payload).encode()
        ).hexdigest()
        assert artifact.artifact_id.endswith(expected)
        payload["artifact_id"] = "changed"
        with pytest.raises(ValueError, match="identity mismatch"):
            type(artifact).from_dict(payload)


def test_sdk_and_plugin_versions_are_independent() -> None:
    assert BROKER_PLUGIN_SDK_VERSION == "1.0.0"
    assert _metadata().plugin_version == "2.4.1"
    assert _metadata().sdk_version == "1.0.0"
    assert (
        replace(_metadata(), plugin_version="3.0.0-rc.1+build.2").artifact_id
        != _metadata().artifact_id
    )
    for version in ("01.0.0", "1.0", "v1.0.0", "1.0.0-01"):
        with pytest.raises(ValueError, match="SemVer"):
            replace(_metadata(), plugin_version=version)
    with pytest.raises(ValueError, match="SDK version"):
        replace(_metadata(), sdk_version="2.0.0")


def test_nested_contracts_are_immutable_and_wire_copies_are_detached() -> None:
    event = _event()
    original = event.artifact_id
    with pytest.raises(FrozenInstanceError):
        event.sequence = 4
    with pytest.raises(FrozenInstanceError):
        event.quote.bid = "9"
    payload = event.to_dict()
    payload["quote"]["bid"] = "7"
    event.extensions[0].payload()["flags"].append("changed")
    assert event.artifact_id == original
    assert event.quote.bid == "1.10000"
    assert event.extensions[0].payload() == {"flags": [1, True, None]}


def test_constructor_bounds_collections_before_recursing_or_copying() -> None:
    # Invalid children prove cardinality is checked before inspecting items.
    with pytest.raises(ValueError, match="collection exceeds"):
        replace(_metadata(), extensions=[object()] * 129)
    cycle: list[object] = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match="nesting exceeds"):
        replace(_metadata(), extensions=cycle)


@pytest.mark.parametrize(
    "changed",
    [
        {"unknown": 1},
        {"sequence": True},
        {"sequence": 1.5},
        {"kind": "unknown"},
        {"schema_version": "histdatacom.broker-plugin.event.v2"},
        {"session_id": "bad"},
    ],
)
def test_wire_rejects_unknown_fields_types_versions_and_identity(
    changed: dict[str, object],
) -> None:
    payload = _event().to_dict()
    payload.update(changed)
    with pytest.raises(ValueError):
        BrokerEventV1.from_dict(payload)


def test_missing_even_optional_wire_fields_are_rejected() -> None:
    payload = _event().to_dict()
    payload.pop("gap")
    with pytest.raises(ValueError, match="missing or unknown"):
        BrokerEventV1.from_dict(payload)


@pytest.mark.parametrize(
    "text",
    [
        '{"same":1,"same":2}',
        '{"nested":{"x":1,"x":2}}',
        '{"x":NaN}',
        '{"x":Infinity}',
        '{"x":9223372036854775808}',
        '{"x":' + "[" * 18 + "0" + "]" * 18 + "}",
        '{"x":[' + ",".join("0" for _ in range(129)) + "]}",
    ],
)
def test_extension_json_is_strict_bounded_and_duplicate_free(text: str) -> None:
    with pytest.raises(ValueError):
        BrokerExtensionV1("org.example.data", text)


def test_extensions_are_canonical_namespaced_and_cannot_replace_core_fields() -> (
    None
):
    extension = BrokerExtensionV1(
        "org.example.fields", '{ "sequence":999, "kind":"untrusted" }'
    )
    event = replace(_event(), extensions=(extension,))
    assert event.sequence == 0
    assert event.kind is BrokerEventKind.QUOTE
    assert extension.payload_json == '{"kind":"untrusted","sequence":999}'
    assert BrokerEventV1.from_json(event.to_json()).extensions == (extension,)
    for namespace in (
        "sequence",
        "histdatacom.override",
        "histdatacom-private",
        "broker-plugin",
        "broker-plugin.fields",
        "broker.plugin.fields",
        "broker_plugin.fields",
    ):
        with pytest.raises(ValueError):
            BrokerExtensionV1(namespace, "{}")
    with pytest.raises(ValueError, match="unique"):
        replace(event, extensions=(extension, extension))
    with pytest.raises(ValueError, match="byte limit"):
        BrokerExtensionV1("org.example.big", '{"x":"' + "a" * 8192 + '"}')


@pytest.mark.parametrize(
    "payload",
    [
        {"nested": [{"api_key": "not-a-public-value"}]},
        {"Authorization": "not-a-public-value"},
        {"nested": {"access-token": "not-a-public-value"}},
        {"description": "Bearer not-a-public-value"},
        {"description": "https://user:not-a-public-value@example.test"},
        {"description": "-----BEGIN RSA PRIVATE KEY-----"},
    ],
)
def test_extensions_reject_known_credential_patterns_without_echoing_values(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="credential-like") as exc:
        BrokerExtensionV1("org.example.fields", json.dumps(payload))
    assert "not-a-public-value" not in str(exc.value)


@pytest.mark.parametrize(
    ("value", "has_credentials"),
    [
        ("https://user:pass@example.test", True),
        ("123http://user:pass@example.test", True),
        ("9+.HTTP://user:pass@example.test", True),
        ("embedded_prefix/http://user:pass@example.test", True),
        ("123_a://user:pass@example.test", True),
        ("\u0130\u0131\u017f\u212a://user:pass@example.test", True),
        ("a://user:pass@example.test a://user:pass@example.test", True),
        ("123+.-://user:pass@example.test", False),
        ("a_123://user:pass@example.test", False),
        ("://user:pass@example.test", False),
        ("a://user:@example.test", False),
        ("a://user:pass/example.test@host", False),
        ("a://user:pass word@example.test", False),
    ],
)
def test_url_credential_detection_preserves_embedded_scheme_semantics(
    value: str, has_credentials: bool
) -> None:
    payload = json.dumps({"description": value})
    if has_credentials:
        with pytest.raises(ValueError, match="credential-like"):
            BrokerExtensionV1("org.example.fields", payload)
    else:
        extension = BrokerExtensionV1("org.example.fields", payload)
        assert extension.payload() == {"description": value}
        assert BrokerExtensionV1.from_json(extension.to_json()) == extension


def test_maximum_depth_extension_round_trips_inside_parent_artifacts() -> None:
    # The payload's depth budget is independent of its encoded-string wrapper.
    payload = '{"x":' + "[" * 15 + "0" + "]" * 15 + "}"
    extension = BrokerExtensionV1("org.example.depth", payload)
    metadata = replace(_metadata(), extensions=(extension,))
    session = replace(_session(), metadata_id=metadata.artifact_id)
    event = replace(
        _event(), session_id=session.artifact_id, extensions=(extension,)
    )
    for artifact in (extension, metadata, session, event):
        assert type(artifact).from_json(artifact.to_json()) == artifact
    with pytest.raises(ValueError, match="nesting exceeds"):
        BrokerExtensionV1(
            "org.example.depth", '{"x":' + "[" * 16 + "0" + "]" * 16 + "}"
        )


def test_maximum_alphabetic_extensions_have_bounded_composition_runtime() -> (
    None
):
    # A process timeout is a generous safety bound, not a machine-speed score.
    # Repeated child validation formerly rescanned every alphabetic suffix.
    script = """
import json
import sys
sys.path.insert(0, sys.argv[1])
from histdatacom.broker_plugins import (
    BrokerEventKind, BrokerEventV1, BrokerExtensionV1,
    BrokerPluginMetadataV1, BrokerSessionV1,
)
payload = json.dumps({"x": "a" * 8184}, separators=(",", ":"))
assert len(payload.encode()) == 8192
extensions = tuple(
    BrokerExtensionV1(f"org.example.data{index}", payload)
    for index in range(7)
)
metadata = BrokerPluginMetadataV1(
    "org.example.fx", "1.0.0", "FX", extensions=extensions
)
session = BrokerSessionV1(metadata.artifact_id, "a" * 32, 0, "clock")
event = BrokerEventV1(
    session.artifact_id, 0, BrokerEventKind.HEARTBEAT, "connection",
    extensions=extensions,
)
for artifact in extensions + (metadata, session, event):
    assert type(artifact).from_json(artifact.to_json()) == artifact
print("maximum-payload composition round-trips")
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(ROOT / "src")],
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "maximum-payload composition round-trips"


def test_configuration_schema_is_public_but_configuration_values_are_ephemeral() -> (
    None
):
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "token",
                BrokerConfigurationType.STRING,
                "Provider credential",
                secret=True,
            ),
            BrokerConfigurationFieldV1(
                "retry_limit",
                BrokerConfigurationType.INTEGER,
                "Attempt limit",
                required=False,
            ),
            BrokerConfigurationFieldV1(
                "environment",
                BrokerConfigurationType.STRING,
                "Provider environment",
                choices=("demo", "live"),
            ),
        )
    )
    values = {
        "token": "not-serialized-secret",
        "environment": "demo",
        "retry_limit": 3,
    }
    schema.validate_configuration(values)
    assert "not-serialized-secret" not in schema.to_json()
    for invalid in (
        {},
        {**values, "other": 1},
        {**values, "retry_limit": True},
        {**values, "environment": "unknown"},
    ):
        with pytest.raises(ValueError) as exc:
            schema.validate_configuration(invalid)
        assert "not-serialized-secret" not in str(exc.value)
    with pytest.raises(ValueError, match="unique"):
        BrokerConfigurationSchemaV1((schema.fields[0], schema.fields[0]))
    with pytest.raises(ValueError, match="non-secret"):
        replace(schema.fields[0], choices=("forbidden",))


@pytest.mark.parametrize(
    "value", [10**1000, -(10**1000), float("inf"), float("nan")]
)
def test_configuration_numeric_bounds_refuse_without_overflow(
    value: object,
) -> None:
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "number", BrokerConfigurationType.NUMBER, "Bounded number"
            ),
        )
    )
    with pytest.raises(ValueError, match="outside SDK bounds"):
        schema.validate_configuration({"number": value})


def test_instrument_normalization_requires_unambiguous_discovery() -> None:
    instrument = BrokerInstrumentV1(
        "EURUSD", "EUR/USD.raw", "EUR", "USD", "0.00001"
    )
    assert (
        normalize_broker_instrument("EUR/USD.raw", (instrument,)) == instrument
    )
    for wrong in ("eur/usd.raw", "EURUSD", "EUR/USD"):
        with pytest.raises(BrokerPluginError) as exc:
            normalize_broker_instrument(wrong, (instrument,))
        assert (
            exc.value.diagnostic.code is BrokerReasonCode.UNSUPPORTED_INSTRUMENT
        )
    with pytest.raises(ValueError, match="ambiguous"):
        normalize_broker_instrument("EUR/USD.raw", (instrument, instrument))
    with pytest.raises(ValueError, match="bounds"):
        normalize_broker_instrument("EUR/USD.raw", (instrument,) * 129)
    with pytest.raises(TypeError, match="immutable SDK artifacts"):
        normalize_broker_instrument("EUR/USD.raw", [instrument])
    with pytest.raises(TypeError, match="immutable SDK artifacts"):
        normalize_broker_instrument("EUR/USD.raw", ("EURUSD",))
    with pytest.raises(ValueError):
        replace(instrument, symbol="USDEUR")


def test_quotes_preserve_decimal_lexemes_and_explicit_units() -> None:
    quote = _quote()
    assert quote.bid == "1.10000"
    assert quote.bid_size.unit == "EUR"
    assert quote.ask_size is None
    assert quote.activity.semantics is BrokerActivitySemantics.MESSAGE_COUNT
    for bid, ask in (
        ("NaN", "2"),
        ("-1", "2"),
        ("2", "1"),
        ("0", "1"),
        ("1e0", "2"),
    ):
        with pytest.raises(ValueError):
            replace(quote, bid=bid, ask=ask)
    with pytest.raises(ValueError):
        BrokerActivityV1(
            "2.5", BrokerActivitySemantics.MESSAGE_COUNT, "message"
        )
    with pytest.raises(ValueError):
        BrokerActivityV1("2", BrokerActivitySemantics.MESSAGE_COUNT, "trade")


@pytest.mark.parametrize("kind", list(BrokerEventKind))
def test_every_event_kind_has_a_canonical_round_trip(
    kind: BrokerEventKind,
) -> None:
    event = BrokerEventV1(
        session_id=_session().artifact_id,
        sequence=0,
        kind=kind,
        connection_id="fixture-connection",
        receive_time=BrokerReceiveTimeV1(1000, 500, "process-1"),
        instrument=(
            "EURUSD"
            if kind
            in (
                BrokerEventKind.QUOTE,
                BrokerEventKind.SUBSCRIBED,
                BrokerEventKind.UNSUBSCRIBED,
            )
            else None
        ),
        quote=_quote() if kind is BrokerEventKind.QUOTE else None,
        gap=(
            BrokerGapV1(BrokerGapScope.TRANSPORT, 100, 200)
            if kind is BrokerEventKind.GAP
            else None
        ),
        diagnostic=_diagnostic(
            BrokerReasonCode.CLOCK_DISCONTINUITY
            if kind is BrokerEventKind.CLOCK_CORRECTION
            else BrokerReasonCode.SOURCE_GAP
        ),
    )
    assert BrokerEventV1.from_json(event.to_json()) == event


def test_inconsistent_event_fields_fail_instead_of_being_dropped() -> None:
    event = _event()
    for changes in (
        {"kind": BrokerEventKind.HEARTBEAT},
        {"quote": None},
        {"instrument": "GBPUSD"},
        {"instrument": "USDUSD"},
        {"gap": BrokerGapV1(BrokerGapScope.SOURCE, 100)},
    ):
        with pytest.raises(ValueError):
            replace(event, **changes)
    with pytest.raises(ValueError, match="diagnostics"):
        BrokerEventV1(
            _session().artifact_id,
            0,
            BrokerEventKind.RECONNECTING,
            "connection",
        )
    with pytest.raises(ValueError, match="clock evidence"):
        BrokerEventV1(
            _session().artifact_id,
            0,
            BrokerEventKind.CLOCK_CORRECTION,
            "connection",
            diagnostic=_diagnostic(),
        )


def test_stream_order_is_session_sequence_not_source_clock_order() -> None:
    first = _event(0)
    second = replace(
        _event(1),
        source_time=BrokerSourceTimeV1(
            900, 100, BrokerSourceTimeSemantics.BROKER_EVENT
        ),
    )
    assert tuple(validate_broker_event_stream((first, second), _session())) == (
        first,
        second,
    )
    assert tuple(
        validate_broker_event_stream((second,), _session(), starting_sequence=1)
    ) == (second,)
    for broken in (
        (second, first),
        (first, first),
        (first, replace(second, sequence=2)),
        (
            replace(
                first,
                session_id=replace(
                    _session(), instance_nonce="b" * 32
                ).artifact_id,
            ),
        ),
    ):
        with pytest.raises(ValueError, match="session or contiguous"):
            tuple(validate_broker_event_stream(broken, _session()))


def test_receive_clock_identity_steps_and_process_boundaries_are_explicit() -> (
    None
):
    first, second = _event(0), _event(1)
    for timing in (
        BrokerReceiveTimeV1(2001, 101, "another-process"),
        BrokerReceiveTimeV1(2001, 99, "process-1"),
        BrokerReceiveTimeV1(1999, 101, "process-1"),
    ):
        with pytest.raises(ValueError):
            tuple(
                validate_broker_event_stream(
                    (first, replace(second, receive_time=timing)), _session()
                )
            )
    correction = BrokerEventV1(
        _session().artifact_id,
        1,
        BrokerEventKind.CLOCK_CORRECTION,
        "connection-1",
        receive_time=BrokerReceiveTimeV1(1999, 101, "process-1"),
        diagnostic=_diagnostic(BrokerReasonCode.CLOCK_DISCONTINUITY),
    )
    assert (
        len(
            tuple(validate_broker_event_stream((first, correction), _session()))
        )
        == 2
    )


def test_diagnostics_are_bounded_and_raw_provenance_is_hash_only() -> None:
    diagnostic = _diagnostic()
    assert len(str(BrokerPluginError(diagnostic))) < 600
    for text in (
        "x" * 513,
        "Bearer abcdefghijk",
        "https://user:password@example.org",
        "bad\nmessage",
    ):
        with pytest.raises(ValueError):
            replace(diagnostic, summary=text)
    assert "raw_message" not in _event().to_dict()
    with pytest.raises(ValueError):
        replace(_event().raw_provenance, sha256="unknown")


def test_external_plugin_uses_only_public_sdk_and_exercises_lifecycle() -> None:
    fixture = runpy.run_path(str(FIXTURE))
    events = fixture["exercise_external_plugin"]()
    assert len(events) == 6
    assert events[0].kind is BrokerEventKind.CONNECTED
    assert events[-1].kind is BrokerEventKind.DISCONNECTED


def test_external_plugin_refuses_bad_configuration_and_inactive_sessions() -> (
    None
):
    fixture = runpy.run_path(str(FIXTURE))
    plugin = fixture["ExternalFixturePlugin"]()
    with pytest.raises(BrokerPluginError) as invalid:
        plugin.open_session({"unknown": "fixture-only-secret"})
    assert (
        invalid.value.diagnostic.code is BrokerReasonCode.INVALID_CONFIGURATION
    )
    assert "fixture-only-secret" not in str(invalid.value)
    session = plugin.open_session({"credential": "fixture-only-secret"})
    with pytest.raises(BrokerPluginError):
        plugin.instruments(replace(session, instance_nonce="2" * 32))
    with pytest.raises(BrokerPluginError):
        plugin.subscribe(session, ("GBPUSD",))
    plugin.close_session(session)
    with pytest.raises(BrokerPluginError):
        plugin.subscribe(session, ("EURUSD",))


def test_sdk_dependency_graph_contains_only_stdlib_and_its_own_namespace() -> (
    None
):
    for path in (ROOT / "src/histdatacom/broker_plugins").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                assert all(
                    item.name.split(".")[0] in sys.stdlib_module_names
                    for item in node.names
                )
            elif isinstance(node, ast.ImportFrom) and not node.level:
                assert node.module.split(".")[0] in sys.stdlib_module_names
    for node in ast.walk(ast.parse(FIXTURE.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module.startswith(
            "histdatacom"
        ):
            assert node.module == "histdatacom.broker_plugins"


def test_public_sdk_includes_its_scoped_pep561_typing_marker() -> None:
    from importlib.resources import files

    assert files("histdatacom.broker_plugins").joinpath("py.typed").is_file()


def test_external_plugin_runs_without_any_private_host_module() -> None:
    script = """
import importlib.abc
import runpy
import sys
class RefusePrivateHost(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('histdatacom.') and not (
            fullname.startswith('histdatacom.broker_plugins')
            or fullname in {'histdatacom.options', 'histdatacom.fx_enums'}
        ):
            raise AssertionError('Private host dependency: ' + fullname)
        return None
sys.meta_path.insert(0, RefusePrivateHost())
sys.path.insert(0, sys.argv[1])
runpy.run_path(sys.argv[2], run_name='__main__')
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(ROOT / "src"), str(FIXTURE)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"event_count": 6}


def test_sdk_contract_is_documented_independently_of_fixture_source() -> None:
    text = (ROOT / "docs/broker-plugin-sdk.md").read_text()
    for phrase in (
        "1.0.0",
        "open_session",
        "unsubscribe",
        "close_session",
        "host boundary",
        "namespaced",
        "first release",
        "strict",
        "metadata_id",
    ):
        assert phrase in text
