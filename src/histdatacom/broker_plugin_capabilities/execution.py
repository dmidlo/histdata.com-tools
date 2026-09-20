"""Caller-authorized synchronous execution with immutable capability evidence.

No timeouts, automatic retries, background queues or capture completion claims:
those require the separate lifecycle/isolation/permission policies.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
import threading
from typing import TypeVar

from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugins import (
    BrokerConfigurationSchemaV1,
    BrokerEventV1,
    BrokerPluginV1,
    BrokerSessionV1,
    normalize_broker_instrument,
    validate_broker_event_stream,
)

from .contracts import (
    MAX_CAPABILITY_ITEMS,
    BrokerAdmittedEventV1,
    BrokerAdmittedInstrumentV1,
    BrokerAdmittedMetadataV1,
    BrokerCapabilityError,
    BrokerCapabilityPlanV1,
    BrokerCapabilityReason,
    BrokerInvocationAssociation,
    BrokerInvocationBindingV1,
)
from .negotiation import verify_broker_capability_plan
from .validation import (
    _operation,
    validate_broker_capability_event,
    validate_broker_instrument,
    validate_broker_metadata,
)

MAX_INVOCATION_EVENTS = 1_000_000
_Result = TypeVar("_Result")
_CONSTRUCTION_KEY = object()


def _call(operation: Callable[[], _Result]) -> _Result:
    try:
        return operation()
    except (Exception, SystemExit):
        # Plugin-supplied exceptions cannot impersonate host refusal receipts.
        raise BrokerCapabilityError(
            BrokerCapabilityReason.PLUGIN_FAILURE
        ) from None


def _authorize(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    authorize: Callable[[BrokerCapabilityPlanV1], bool],
) -> None:
    verify_broker_capability_plan(plan, inventory)
    plan.require_admitted()
    try:
        if authorize(plan) is not True:
            raise ValueError
    except (Exception, SystemExit):
        raise BrokerCapabilityError(
            BrokerCapabilityReason.AUTHORIZATION_REQUIRED
        ) from None


class GatedBrokerPluginV1:
    """One authorized, single-threaded invocation, not a permission authority.

    Obtain through invoke_authorized_broker_factory or the installed loader.
    Only a finite instrument snapshot and one last clock/sequence are retained.
    Plugin calls can block: process/deadline containment is not implemented here.
    """

    __slots__ = (
        "_plugin",
        "_plan",
        "_thread",
        "_metadata",
        "_binding",
        "_session",
        "_instruments",
        "_subscriptions",
        "_streaming",
        "_closed",
        "_next_sequence",
        "_last_monotonic",
        "_last_utc",
    )

    def __init__(
        self,
        plugin: BrokerPluginV1,
        plan: BrokerCapabilityPlanV1,
        *,
        association: BrokerInvocationAssociation,
        module_sha256: str | None = None,
        _key: object = None,
    ) -> None:
        if _key is not _CONSTRUCTION_KEY:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.AUTHORIZATION_REQUIRED
            )
        self._plugin = plugin
        self._plan = plan
        self._thread = threading.get_ident()
        self._metadata = validate_broker_metadata(
            plan, _call(lambda: plugin.metadata)
        )
        self._binding = BrokerInvocationBindingV1(
            plan.artifact_id,
            plan.candidate.artifact_id,
            self._metadata.metadata.artifact_id,
            association,
            module_sha256,
        )
        self._session: BrokerSessionV1 | None = None
        self._instruments: tuple[BrokerAdmittedInstrumentV1, ...] | None = None
        self._subscriptions: set[str] = set()
        self._streaming = False
        self._closed = False
        self._next_sequence = 0
        self._last_monotonic: int | None = None
        self._last_utc: int | None = None

    @property
    def plan(self) -> BrokerCapabilityPlanV1:
        return self._plan

    @property
    def binding(self) -> BrokerInvocationBindingV1:
        return self._binding

    def _guard(self, operation: str) -> None:
        if threading.get_ident() != self._thread:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        _operation(self.plan, operation)

    def _active(self, operation: str) -> BrokerSessionV1:
        self._guard(operation)
        if self._session is None or self._closed:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        return self._session

    @property
    def metadata(self) -> BrokerAdmittedMetadataV1:
        self._guard("metadata")
        return self._metadata

    def _schema(self) -> BrokerConfigurationSchemaV1:
        value = _call(lambda: self._plugin.configuration_schema)
        try:
            if type(value) is not BrokerConfigurationSchemaV1:
                raise ValueError
            return BrokerConfigurationSchemaV1.from_json(value.to_json())
        except Exception:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            ) from None

    @property
    def configuration_schema(self) -> BrokerConfigurationSchemaV1:
        self._guard("configuration_schema")
        return self._schema()

    def open_session(
        self, configuration: Mapping[str, object]
    ) -> BrokerSessionV1:
        self._guard("open_session")
        if self._session is not None or self._closed:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        try:
            if (
                not isinstance(configuration, Mapping)
                or len(configuration) > MAX_CAPABILITY_ITEMS
            ):
                raise ValueError
            ephemeral = dict(configuration)
            self._schema().validate_configuration(ephemeral)
        except BrokerCapabilityError:
            raise
        except Exception:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            ) from None
        session = _call(lambda: self._plugin.open_session(ephemeral))
        try:
            if type(session) is not BrokerSessionV1:
                raise ValueError
            restored = BrokerSessionV1.from_json(session.to_json())
            if restored.metadata_id != self._metadata.metadata.artifact_id:
                raise ValueError
        except Exception:
            if type(session) is BrokerSessionV1:
                try:
                    _call(lambda: self._plugin.close_session(session))
                except BrokerCapabilityError:
                    pass
            raise BrokerCapabilityError(
                BrokerCapabilityReason.RUNTIME_IDENTITY
            ) from None
        self._session = restored
        return restored

    def instruments(self) -> tuple[BrokerAdmittedInstrumentV1, ...]:
        session = self._active("instruments")
        values = _call(lambda: self._plugin.instruments(session))
        if type(values) is not tuple or len(values) > MAX_CAPABILITY_ITEMS:
            raise BrokerCapabilityError(BrokerCapabilityReason.RESOURCE_LIMIT)
        admitted = tuple(
            validate_broker_instrument(self.plan, item) for item in values
        )
        raw = tuple(item.instrument for item in admitted)
        try:
            if raw:
                normalize_broker_instrument(raw[0].provider_symbol, raw)
        except Exception:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            ) from None
        if self._subscriptions - {item.symbol for item in raw}:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            )
        self._instruments = admitted
        return admitted

    def _symbols(self, symbols: tuple[str, ...]) -> set[str]:
        if (
            type(symbols) is not tuple
            or not 1 <= len(symbols) <= MAX_CAPABILITY_ITEMS
            or any(type(item) is not str for item in symbols)
        ):
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            )
        if (
            self._instruments is None
            or len(set(symbols)) != len(symbols)
            or set(symbols)
            - {item.instrument.symbol for item in self._instruments}
        ):
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            )
        return set(symbols)

    def subscribe(self, symbols: tuple[str, ...]) -> None:
        session = self._active("subscribe")
        selected = self._symbols(symbols)
        if selected & self._subscriptions:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        result = _call(lambda: self._plugin.subscribe(session, symbols))
        if result is not None:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            )
        self._subscriptions.update(selected)

    def unsubscribe(self, symbols: tuple[str, ...]) -> None:
        session = self._active("unsubscribe")
        selected = self._symbols(symbols)
        if not selected <= self._subscriptions:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        result = _call(lambda: self._plugin.unsubscribe(session, symbols))
        if result is not None:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.CAPABILITY_VIOLATION
            )
        self._subscriptions -= selected

    def iter_events(
        self, *, max_events: int = 1024
    ) -> Iterator[BrokerAdmittedEventV1]:
        session = self._active("iter_events")
        if (
            type(max_events) is not int
            or not 1 <= max_events <= MAX_INVOCATION_EVENTS
        ):
            raise BrokerCapabilityError(BrokerCapabilityReason.RESOURCE_LIMIT)
        if self._streaming:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        # No provider call occurs until advancement acquires the single-owner
        # guard. Multiple unadvanced generators cannot start provider streams.
        return self._events(session, max_events)

    def _events(
        self, session: BrokerSessionV1, maximum: int
    ) -> Iterator[BrokerAdmittedEventV1]:
        if self._streaming:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        if self._active("iter_events") != session:
            raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_STATE)
        self._streaming = True
        events: Iterator[BrokerEventV1] | None = None
        try:
            events = _call(lambda: iter(self._plugin.iter_events(session)))
            validated = validate_broker_event_stream(
                events, session, starting_sequence=self._next_sequence
            )
            for _ in range(maximum):
                if self._active("iter_events") != session:
                    raise BrokerCapabilityError(
                        BrokerCapabilityReason.INVALID_STATE
                    )
                try:
                    event = next(validated)
                except StopIteration:
                    return
                except (Exception, SystemExit):
                    raise BrokerCapabilityError(
                        BrokerCapabilityReason.CAPABILITY_VIOLATION
                    ) from None
                admitted = validate_broker_capability_event(self.plan, event)
                if event.instrument is not None and (
                    self._instruments is None
                    or event.instrument
                    not in {
                        item.instrument.symbol for item in self._instruments
                    }
                ):
                    raise BrokerCapabilityError(
                        BrokerCapabilityReason.CAPABILITY_VIOLATION
                    )
                from histdatacom.broker_plugins import BrokerEventKind

                if (
                    event.kind is BrokerEventKind.QUOTE
                    and event.instrument not in self._subscriptions
                ):
                    raise BrokerCapabilityError(
                        BrokerCapabilityReason.CAPABILITY_VIOLATION
                    )
                if event.receive_time is not None:
                    timing = event.receive_time
                    if (
                        self._last_monotonic is not None
                        and timing.monotonic_ns < self._last_monotonic
                    ) or (
                        self._last_utc is not None
                        and timing.utc_ns < self._last_utc
                        and event.kind is not BrokerEventKind.CLOCK_CORRECTION
                    ):
                        raise BrokerCapabilityError(
                            BrokerCapabilityReason.CAPABILITY_VIOLATION
                        )
                    self._last_monotonic = timing.monotonic_ns
                    self._last_utc = timing.utc_ns
                self._next_sequence = event.sequence + 1
                yield admitted
            # No extra source message is consumed to probe completeness.
            raise BrokerCapabilityError(BrokerCapabilityReason.RESOURCE_LIMIT)
        finally:
            self._streaming = False
            try:
                close = getattr(events, "close", None)
                if callable(close):
                    close()
            except (Exception, SystemExit):
                pass

    def close_session(self) -> None:
        self._guard("close_session")
        session, self._session = self._session, None
        self._closed = True
        self._subscriptions.clear()
        self._instruments = None
        if session is not None:
            result = _call(lambda: self._plugin.close_session(session))
            if result is not None:
                raise BrokerCapabilityError(
                    BrokerCapabilityReason.CAPABILITY_VIOLATION
                )


def invoke_authorized_broker_factory(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    *,
    authorize: Callable[[BrokerCapabilityPlanV1], bool],
    factory: Callable[[], BrokerPluginV1],
) -> GatedBrokerPluginV1:
    """Explicit caller-owned factory; does NOT verify installed association."""
    _authorize(inventory, plan, authorize)
    plugin = _call(factory)
    return GatedBrokerPluginV1(
        plugin,
        plan,
        association=BrokerInvocationAssociation.CALLER_FACTORY,
        _key=_CONSTRUCTION_KEY,
    )
