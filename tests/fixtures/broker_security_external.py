"""Separately installed offline provider; positive path imports only SDK."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from collections.abc import Iterator, Mapping
from typing import cast
from urllib.parse import quote

from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerEventKind,
    BrokerEventV1,
    BrokerExtensionV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
)
from histdatacom.broker_plugins.resources import BrokerHostResourcesV1

# Independently known by this hostile fixture, not a host-resolved credential.
PRIVATE_CANARY = 'synthetic / private " ☃ fixture-only-known-marker'


class SecurityFixture:
    def __init__(self, resources: BrokerHostResourcesV1) -> None:
        self.resources = resources
        self.mode = "finite"
        self.value = PRIVATE_CANARY

    @property
    def metadata(self) -> BrokerPluginMetadataV1:
        return BrokerPluginMetadataV1(
            "org.example.security", "1.0.0", "Offline security fixture"
        )

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        return BrokerConfigurationSchemaV1(
            (
                BrokerConfigurationFieldV1(
                    "mode", BrokerConfigurationType.STRING, "Offline scenario"
                ),
            )
        )

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        self.mode = cast(str, configuration["mode"])
        if self.mode == "authenticate":
            response = self.resources.request(
                "fixture-auth",
                "POST",
                "/auth/login",
                secret_profile="fixture-login",
            )
            if response.status != 200 or response.body != b"accepted":
                raise ValueError("authentication refused")
        if self.mode == "exception":
            raise RuntimeError(self.value)
        if self.mode == "output":
            os.write(1, self.value.encode())
            os.write(2, self.value.encode())
        if self.mode == "output_storm":
            os.write(1, b"x" * 1_048_576)
        if self.mode == "block_open":
            time.sleep(60)
        if self.mode == "death":
            os._exit(99)
        if "BROKER_SECURITY_AMBIENT_CANARY" in os.environ:
            raise RuntimeError("ambient environment unexpectedly exposed")
        return BrokerSessionV1(
            self.metadata.artifact_id,
            "a" * 32,
            100,
            self.value if self.mode == "session" else "fixture-clock",
        )

    def instruments(
        self, session: BrokerSessionV1
    ) -> tuple[BrokerInstrumentV1, ...]:
        return (
            BrokerInstrumentV1("EURUSD", "EUR/USD", "EUR", "USD", "0.00001"),
        )

    def subscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        pass

    def unsubscribe(
        self, session: BrokerSessionV1, symbols: tuple[str, ...]
    ) -> None:
        pass

    def iter_events(self, session: BrokerSessionV1) -> Iterator[BrokerEventV1]:
        if self.mode == "block_next":
            time.sleep(60)
        if self.mode == "malformed":
            yield cast(BrokerEventV1, object())
            return
        values = {
            "plain": self.value,
            "url": quote(self.value, safe=""),
            "base64": base64.b64encode(self.value.encode()).decode(),
            "json": json.dumps(self.value),
            "sha256": hashlib.sha256(self.value.encode()).hexdigest(),
        }
        extensions = (
            (
                BrokerExtensionV1(
                    "org.example.security",
                    json.dumps({"value": values[self.mode]}),
                ),
            )
            if self.mode in values
            else ()
        )
        for sequence in range(2):
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.QUOTE,
                "connection-1",
                receive_time=BrokerReceiveTimeV1(
                    200 + sequence, 10 + sequence, "fixture-clock"
                ),
                instrument="EURUSD",
                quote=BrokerQuoteV1("EURUSD", "1.1", "1.2"),
                extensions=extensions,
            )

    def close_session(self, session: BrokerSessionV1) -> None:
        if self.mode == "block_close":
            time.sleep(60)


def factory(resources: BrokerHostResourcesV1) -> BrokerPluginV1:
    return SecurityFixture(resources)
