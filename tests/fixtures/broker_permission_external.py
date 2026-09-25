"""Generated third-party resource adversary; imports only the public SDK."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import os
import socket
import sys
from typing import cast

from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerHostResourcesV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
    BrokerPluginV1,
    BrokerQuoteV1,
    BrokerRawProvenanceV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSizeSemantics,
    BrokerSizeV1,
)


class PermissionFixture:
    def __init__(self, resources: BrokerHostResourcesV1) -> None:
        self.resources = resources
        self.mode = "finite"
        self.clock = "generated-clock"

    @property
    def metadata(self) -> BrokerPluginMetadataV1:
        return BrokerPluginMetadataV1(
            "org.example.permissions", "1.0.0", "Generated permission fixture"
        )

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        return BrokerConfigurationSchemaV1(
            (
                BrokerConfigurationFieldV1(
                    "mode", BrokerConfigurationType.STRING, "Generated scenario"
                ),
                BrokerConfigurationFieldV1(
                    "target",
                    BrokerConfigurationType.STRING,
                    "Generated protected target",
                    required=False,
                ),
                BrokerConfigurationFieldV1(
                    "port",
                    BrokerConfigurationType.INTEGER,
                    "Generated localhost port",
                    required=False,
                ),
            )
        )

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        self.mode = cast(str, configuration["mode"])
        if "credential" in configuration:
            raise ValueError(
                "plaintext credentials must not enter resource plugins"
            )
        if self.mode.startswith("network") or self.mode.startswith("secret"):
            response = self.resources.request(
                "undeclared" if self.mode == "network_undeclared" else "quotes",
                "DELETE" if self.mode == "network_method" else "GET",
                "/outside/" if self.mode == "network_path" else "/v1/ticks",
                secret_profile=(
                    "undeclared"
                    if self.mode == "secret_undeclared"
                    else (
                        "paper"
                        if self.mode in ("secret", "secret_undeclared")
                        else None
                    )
                ),
            )
            if response.status != 200 or response.body != b"generated-response":
                raise ValueError("generated transport response differs")
        if self.mode.startswith("cache"):
            cache_id = (
                "undeclared" if self.mode == "cache_undeclared" else "prices"
            )
            key = "../scientific-store" if self.mode == "cache_path" else "one"
            value = b"x" * 65 if self.mode == "cache_size" else b"generated"
            self.resources.put_cache(cache_id, key, value)
            if self.resources.get_cache(cache_id, key) != value:
                raise ValueError("generated cache round-trip differs")
        if self.mode == "subprocess":
            self.resources.request_subprocess("generated-operation")
        if self.mode in {"worker_bypass_sizes", "worker_bypass_raw"}:
            # Deliberately hostile isolated-worker mutation: cooperative worker
            # checks are not an authority boundary. The parent must independently
            # refuse the resulting native IPC event before durable admission.
            scope = sys.modules["histdatacom.broker_plugin_permissions.scope"]
            setattr(scope, "require_event_permissions", lambda event: None)
        if self.mode.startswith("direct_"):
            try:
                if self.mode == "direct_fork":
                    child = os.fork()
                    if child == 0:
                        os._exit(0)
                    os.waitpid(child, 0)
                elif self.mode == "direct_spawn":
                    child = os.posix_spawn(
                        sys.executable,
                        [sys.executable, "-I", "-S", "-c", "pass"],
                        {"LANG": "C"},
                    )
                    os.waitpid(child, 0)
                elif self.mode == "direct_exec":
                    os.execve(
                        sys.executable,
                        [
                            sys.executable,
                            "-I",
                            "-S",
                            "-c",
                            "raise SystemExit(81)",
                        ],
                        {"LANG": "C"},
                    )
                elif self.mode == "direct_write":
                    with open(
                        cast(str, configuration["target"]), "wb"
                    ) as stream:
                        stream.write(b"generated-forbidden-write")
                elif self.mode == "direct_network":
                    with socket.create_connection(
                        ("127.0.0.1", cast(int, configuration["port"])), 0.2
                    ):
                        pass
                else:
                    raise ValueError("unknown generated direct probe")
            except PermissionError:
                self.clock = "generated-kernel-denied"
            else:
                raise ValueError(
                    "undeclared direct operation unexpectedly succeeded"
                )
        return BrokerSessionV1(
            self.metadata.artifact_id, "d" * 32, 100, self.clock
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
        if self.mode == "health":
            yield BrokerEventV1(
                session.artifact_id,
                0,
                BrokerEventKind.HEALTH,
                "generated-connection",
                diagnostic=BrokerDiagnosticV1(
                    BrokerReasonCode.HEALTHY,
                    BrokerDiagnosticSeverity.INFO,
                    "Generated healthy claim",
                ),
            )
            return
        for sequence in range(2):
            yield BrokerEventV1(
                session.artifact_id,
                sequence,
                BrokerEventKind.QUOTE,
                "generated-connection",
                receive_time=BrokerReceiveTimeV1(
                    200 + sequence, 10 + sequence, self.clock
                ),
                instrument="EURUSD",
                quote=BrokerQuoteV1(
                    "EURUSD",
                    "1.1",
                    "1.2",
                    bid_size=(
                        BrokerSizeV1(
                            "2", BrokerSizeSemantics.QUOTED_SIZE, "unit"
                        )
                        if self.mode in {"sizes", "worker_bypass_sizes"}
                        else None
                    ),
                ),
                raw_provenance=(
                    BrokerRawProvenanceV1(
                        "e" * 64, 12, "application/json", "generated-policy"
                    )
                    if self.mode in {"raw", "worker_bypass_raw"}
                    else None
                ),
            )

    def close_session(self, session: BrokerSessionV1) -> None:
        pass


def factory(resources: BrokerHostResourcesV1) -> BrokerPluginV1:
    return PermissionFixture(resources)
