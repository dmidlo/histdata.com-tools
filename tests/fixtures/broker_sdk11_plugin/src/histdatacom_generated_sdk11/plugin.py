"""Independent real SDK 1.1 behavior using only public SDK imports.

Successful open_session proves dispatch occurred: provider_protocol mutates
private call state and open_session refuses without it. Negative factories
independently exercise absent, wrong-type and substituted protocol methods.
No network, credentials, files, process creation or invented source timestamps.
"""

from __future__ import annotations

import time
import uuid

from histdatacom.broker_plugins import (
    BROKER_PROVIDER_PROTOCOL_CAPABILITY,
    BROKER_PROVIDER_PROTOCOL_NAMESPACE,
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerExtensionV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerProviderProtocolV1,
    BrokerQuoteV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
)

VERSION = "0.1.0"
PROVIDER = "generated-sdk11"
INFO = BrokerProviderProtocolV1(PROVIDER, "generated-finite-quotes", "1.0.0")
CAPABILITIES = tuple(
    sorted(
        (
            "events.v1",
            "health.v1",
            "instruments.v1",
            "quotes.v1",
            "session.v1",
            "timestamps.receive.v1",
            BROKER_PROVIDER_PROTOCOL_CAPABILITY,
        )
    )
)
SCHEMA = BrokerConfigurationSchemaV1(
    (
        BrokerConfigurationFieldV1(
            "mode",
            BrokerConfigurationType.STRING,
            "Generated finite test scenario",
            choices=("honest-health",),
        ),
    )
)
VARIANTS = {
    "org.example.sdk11": "factory",
    "org.example.sdk11-missing": "missing_factory",
    "org.example.sdk11-wrong": "wrong_factory",
    "org.example.sdk11-mismatch": "mismatch_factory",
}


class GeneratedPlugin:
    configuration_schema = SCHEMA

    def __init__(self, plugin_id="org.example.sdk11"):
        self.metadata = BrokerPluginMetadataV1(
            plugin_id,
            VERSION,
            "Generated independent SDK 1.1 fixture",
            sdk_version="1.1.0",
            extensions=(
                BrokerExtensionV1(
                    BROKER_PROVIDER_PROTOCOL_NAMESPACE, INFO.to_json()
                ),
            ),
        )
        self.protocol_calls = 0
        self.session = None
        self.subscribed = False
        self.closed = False

    def provider_protocol(self):
        self.protocol_calls += 1
        return INFO

    def open_session(self, configuration):
        self.configuration_schema.validate_configuration(configuration)
        if self.protocol_calls < 1:
            raise RuntimeError("SDK 1.1 provider method was not dispatched")
        if self.session is not None:
            raise RuntimeError("one session per fresh fixture")
        self.session = BrokerSessionV1(
            self.metadata.artifact_id,
            uuid.uuid4().hex,
            time.time_ns(),
            "generated-sdk11-clock",
        )
        return self.session

    def _session(self, session):
        if session != self.session or self.closed:
            raise RuntimeError("inactive fixture session")

    def instruments(self, session):
        self._session(session)
        return (
            BrokerInstrumentV1("EURUSD", "EUR/USD.generated", "EUR", "USD"),
        )

    def subscribe(self, session, symbols):
        self._session(session)
        if symbols != ("EURUSD",):
            raise ValueError("only generated EURUSD exists")
        self.subscribed = True

    def iter_events(self, session):
        self._session(session)
        if not self.subscribed:
            raise RuntimeError("not subscribed")
        for sequence in range(4):
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                (
                    BrokerEventKind.HEALTH
                    if sequence == 0
                    else BrokerEventKind.QUOTE
                ),
                "generated-sdk11-connection",
                receive_time=BrokerReceiveTimeV1(
                    time.time_ns(),
                    time.monotonic_ns(),
                    session.receive_clock_id,
                ),
                instrument=None if sequence == 0 else "EURUSD",
                quote=(
                    None
                    if sequence == 0
                    else BrokerQuoteV1(
                        "EURUSD",
                        "1.10000",
                        "1.20000",
                    )
                ),
                diagnostic=(
                    BrokerDiagnosticV1(
                        BrokerReasonCode.HEALTHY,
                        BrokerDiagnosticSeverity.INFO,
                        BrokerReasonCode.HEALTHY.value,
                    )
                    if sequence == 0
                    else None
                ),
            )

    def unsubscribe(self, session, symbols):
        self._session(session)
        if symbols != ("EURUSD",):
            raise ValueError("only generated EURUSD exists")
        self.subscribed = False

    def close_session(self, session):
        if session != self.session:
            raise RuntimeError("unknown fixture session")
        self.closed = True


class MissingMethod(GeneratedPlugin):
    provider_protocol = None


class WrongType(GeneratedPlugin):
    def provider_protocol(self):
        self.protocol_calls += 1
        return INFO.to_json()


class MismatchedProtocol(GeneratedPlugin):
    def provider_protocol(self):
        self.protocol_calls += 1
        return BrokerProviderProtocolV1(
            PROVIDER, "different-generated-protocol", "1.0.0"
        )


def factory():
    return GeneratedPlugin()


def missing_factory():
    return MissingMethod("org.example.sdk11-missing")


def wrong_factory():
    return WrongType("org.example.sdk11-wrong")


def mismatch_factory():
    return MismatchedProtocol("org.example.sdk11-mismatch")
