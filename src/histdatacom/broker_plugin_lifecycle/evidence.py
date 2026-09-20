"""Constant-space replay checks, shared by live ingestion and durable replay."""

from __future__ import annotations

from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    validate_broker_metadata,
    validate_broker_instrument,
    verify_broker_admitted_event,
)
from histdatacom.broker_plugins import (
    BrokerDiagnosticSeverity,
    BrokerEventKind,
    BrokerReasonCode,
    validate_broker_event_stream,
)

from .contracts import (
    BrokerLifecycleHeaderV1,
    BrokerLifecycleRecordV1,
    BrokerLifecycleIdentityV1,
    BrokerLifecycleSessionV1,
    BrokerLifecycleTransitionV1,
    BrokerLifecycleState as State,
    BrokerLifecycleError,
    BrokerLifecycleReason as Reason,
)


class Evidence:
    def __init__(self, header: BrokerLifecycleHeaderV1) -> None:
        self.header = header
        self.state = State.DISCOVERED
        self.sequence = 0
        self.epoch = 0
        self.identity: BrokerLifecycleIdentityV1 | None = None
        self.session: BrokerLifecycleSessionV1 | None = None
        self.event_sequence = 0
        self.host_clock = 0
        self.plugin_monotonic: int | None = None
        self.plugin_utc: int | None = None
        self.event_count = 0
        self.health_count = 0
        self.unknown_loss = False
        self.error_diagnostics = 0

    def accept(self, record: BrokerLifecycleRecordV1) -> None:
        try:
            self._accept(record)
        except Exception:
            raise BrokerLifecycleError(Reason.INTEGRITY) from None

    def _accept(self, record: BrokerLifecycleRecordV1) -> None:
        if (
            record.run_id != self.header.artifact_id
            or record.capture_sequence != self.sequence
            or record.receive_monotonic_ns < self.host_clock
            or record.epoch > len(self.header.policy.retry_delays_ms)
        ):
            raise ValueError
        if record.kind == "transition":
            change = BrokerLifecycleTransitionV1.from_json(record.payload_json)
            if (
                change.previous is not self.state
                or change.epoch != record.epoch
            ):
                raise ValueError
            if change.current is State.STARTING:
                expected = self.epoch + (self.state is State.RECONNECTING)
                if record.epoch != expected:
                    raise ValueError
                self.epoch = expected
                self.identity = None
                self.session = None
                self.event_sequence = 0
                self.plugin_monotonic = self.plugin_utc = None
            if record.epoch != self.epoch:
                raise ValueError
            if change.current is State.ACTIVE and self.session is None:
                raise ValueError
            self.state = change.current
            if (
                change.current is State.RECONNECTING
                or (
                    change.current is State.STOPPING
                    and change.reason is not Reason.EOF
                )
                or change.current is State.FAILED
            ):
                self.unknown_loss = True
            self.health_count += 1
            if self.health_count > self.header.policy.max_health_records:
                raise ValueError
        elif record.epoch != self.epoch:
            raise ValueError
        elif record.kind == "identity":
            identity = BrokerLifecycleIdentityV1.from_json(record.payload_json)
            plan = self.header.plan
            if (
                self.state is not State.STARTING
                or self.identity is not None
                or identity.binding.plan_id != plan.artifact_id
                or identity.binding.candidate_id != plan.candidate.artifact_id
                or identity.binding.module_sha256
                != plan.candidate.implementation_sha256
                or validate_broker_metadata(plan, identity.metadata.metadata)
                != identity.metadata
            ):
                raise ValueError
            self.identity = identity
        elif record.kind == "session":
            session = BrokerLifecycleSessionV1.from_json(record.payload_json)
            if (
                self.state is not State.STARTING
                or self.session is not None
                or self.identity is None
                or session.session.metadata_id
                != self.identity.metadata.metadata.artifact_id
            ):
                raise ValueError
            for instrument in session.instruments:
                if (
                    validate_broker_instrument(
                        self.header.plan, instrument.instrument
                    )
                    != instrument
                ):
                    raise ValueError
            if not set(self.header.symbols) <= {
                item.instrument.symbol for item in session.instruments
            }:
                raise ValueError
            self.session = session
        elif record.kind == "event":
            admitted = BrokerAdmittedEventV1.from_json(record.payload_json)
            if (
                self.state not in (State.ACTIVE, State.DEGRADED)
                or self.session is None
                or record.delivery_sequence != self.event_sequence
            ):
                raise ValueError
            verify_broker_admitted_event(admitted, self.header.plan)
            event = admitted.event
            if event.diagnostic is not None:
                self.error_diagnostics += (
                    event.diagnostic.severity is BrokerDiagnosticSeverity.ERROR
                )
                if event.diagnostic.code is BrokerReasonCode.SOURCE_GAP:
                    self.unknown_loss = True
            if event.kind in (
                BrokerEventKind.GAP,
                BrokerEventKind.DISCONNECTED,
                BrokerEventKind.RECONNECTING,
            ):
                self.unknown_loss = True
            next(
                validate_broker_event_stream(
                    (event,),
                    self.session.session,
                    starting_sequence=self.event_sequence,
                )
            )
            if (
                event.kind is BrokerEventKind.QUOTE
                and event.instrument not in self.header.symbols
            ):
                raise ValueError
            if event.receive_time is not None:
                clock = event.receive_time
                if (
                    self.plugin_monotonic is not None
                    and clock.monotonic_ns < self.plugin_monotonic
                ):
                    raise ValueError
                if (
                    self.plugin_utc is not None
                    and clock.utc_ns < self.plugin_utc
                    and event.kind is not BrokerEventKind.CLOCK_CORRECTION
                ):
                    raise ValueError
                self.plugin_monotonic, self.plugin_utc = (
                    clock.monotonic_ns,
                    clock.utc_ns,
                )
            self.event_sequence += 1
            self.event_count += 1
            if self.event_count > self.header.policy.max_events:
                raise ValueError
        else:
            raise ValueError
        self.sequence += 1
        self.host_clock = record.receive_monotonic_ns
