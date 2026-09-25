"""Independent native-bound observation replay, without files or provider calls.

The caller supplies actual canonical native artifacts plus retained authority
evidence, not metric flags or an opaque verified boolean. This is tamper
detection against those inputs, not authentication of the recording host.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .contracts import (
    BrokerHostHealthAuditV1,
    BrokerHostHealthBucketV1,
    BrokerHostHealthHeaderV1,
    BrokerHostHealthObservationV1,
    BrokerHostHealthPolicyV1,
    BrokerHostHealthUnavailableV1,
)
from .contracts import (
    BrokerHostHealthFailure as Failure,
)
from .contracts import (
    BrokerHostHealthNativeFamily as Family,
)
from .contracts import (
    BrokerHostHealthObservationKind as Kind,
)
from .contracts import (
    BrokerHostHealthReason as Reason,
)
from .contracts import (
    BrokerHostHealthState as State,
)
from .metrics import health_rate, little_law_diagnostic, summarize_health_times

_DEFAULT_HEALTH_POLICY = BrokerHostHealthPolicyV1()

if TYPE_CHECKING:
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureEventV1,
        BrokerCaptureSessionManifestV1,
        BrokerCaptureSessionV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
        BrokerLifecycleManifestV1,
        BrokerLifecycleRecordV1,
    )
    from histdatacom.broker_plugin_permissions.contracts import (
        BrokerPermissionContextV1,
        BrokerPermissionDecisionV1,
        BrokerPermissionManifestV1,
    )
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerLegacyCaptureV1,
        BrokerSDKInvocationV1,
    )
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyDecisionV1,
    )


def historical_host_health_status(
    manifest: BrokerLifecycleManifestV1 | BrokerCaptureSessionManifestV1,
) -> BrokerHostHealthUnavailableV1:
    """Report absence, not an inferred audit or a replay-verification receipt."""
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureSessionManifestV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )

    if type(manifest) is BrokerLifecycleManifestV1:
        restored = BrokerLifecycleManifestV1.from_json(manifest.to_json())
        return BrokerHostHealthUnavailableV1(
            Family.LIFECYCLE_V1,
            restored.header.artifact_id,
            restored.artifact_id,
        )
    if type(manifest) is BrokerCaptureSessionManifestV1:
        legacy = BrokerCaptureSessionManifestV1.from_json(manifest.to_json())
        return BrokerHostHealthUnavailableV1(
            Family.LEGACY_CAPTURE_V1,
            legacy.session.session_id,
            legacy.manifest_id,
        )
    raise ValueError(
        "exact native manifest required for historical health status"
    )


def _provider_decision(
    native: object, decision: BrokerPolicyDecisionV1
) -> None:
    from histdatacom.broker_plugin_policy.bindings import (
        resolve_provider_subject,
    )
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyDecisionV1,
        BrokerPolicyOperation,
    )

    if type(decision) is not BrokerPolicyDecisionV1:
        raise ValueError("exact retained provider decision required")
    decision = BrokerPolicyDecisionV1.from_json(decision.to_json())
    if (
        not decision.allowed
        or decision.request.operation is not BrokerPolicyOperation.CAPTURE
        or decision.request.subject != resolve_provider_subject(native)
    ):
        raise ValueError(
            "health header differs from native capture policy decision"
        )


def make_lifecycle_health_header(
    native_header: BrokerLifecycleHeaderV1,
    invocation: BrokerSDKInvocationV1,
    permission_manifest: BrokerPermissionManifestV1,
    permission_context: BrokerPermissionContextV1,
    permission_decision: BrokerPermissionDecisionV1,
    provider_decision: BrokerPolicyDecisionV1,
    *,
    started_at_utc_ns: int,
    started_at_monotonic_ns: int,
    policy: BrokerHostHealthPolicyV1 = _DEFAULT_HEALTH_POLICY,
) -> BrokerHostHealthHeaderV1:
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
    )
    from histdatacom.broker_plugin_permissions.contracts import (
        BrokerPermissionBindingV1,
        BrokerPermissionContextV1,
        BrokerPermissionDecisionV1,
        BrokerPermissionManifestV1,
    )
    from histdatacom.broker_plugin_permissions.decisions import (
        decide_broker_permissions,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

    for value, expected in (
        (native_header, BrokerLifecycleHeaderV1),
        (invocation, BrokerSDKInvocationV1),
        (permission_manifest, BrokerPermissionManifestV1),
        (permission_context, BrokerPermissionContextV1),
        (permission_decision, BrokerPermissionDecisionV1),
    ):
        if type(value) is not expected:
            raise ValueError("exact native health binding types required")
    native_header = BrokerLifecycleHeaderV1.from_json(native_header.to_json())
    invocation = BrokerSDKInvocationV1.from_json(invocation.to_json())
    if native_header.plan != invocation.plan:
        raise ValueError("native health invocation plan substitution")
    selected = native_header.plan.candidate.registration
    binding = BrokerPermissionBindingV1(
        native_header.plan.candidate.artifact_id,
        permission_manifest.artifact_id,
        native_header.inventory.sdk_version,
        invocation.configuration_profile.provider_id,
        invocation.configuration_profile.artifact_id,
    )
    replayed = decide_broker_permissions(
        permission_manifest,
        binding,
        permission_decision.grant_id,
        permission_context,
        permission_decision.at_ns,
    )
    if (
        replayed != permission_decision
        or not replayed.admitted
        or (
            permission_manifest.distribution_name,
            permission_manifest.distribution_version,
        )
        != (selected.distribution_name, selected.distribution_version)
    ):
        raise ValueError("native health permission identity substitution")
    _provider_decision(invocation, provider_decision)
    return BrokerHostHealthHeaderV1(
        Family.LIFECYCLE_V1,
        native_header.artifact_id,
        selected.plugin_id,
        invocation.configuration_profile.provider_id,
        invocation.configuration_profile.artifact_id,
        permission_manifest.artifact_id,
        permission_context.artifact_id,
        permission_decision.artifact_id,
        provider_decision.artifact_id,
        native_header.symbols,
        started_at_utc_ns,
        started_at_monotonic_ns,
        native_header.policy.queue_items,
        policy,
    )


def make_legacy_health_header(
    session: BrokerCaptureSessionV1,
    provider_decision: BrokerPolicyDecisionV1,
    *,
    provider_request: BrokerLegacyCaptureV1,
    symbols: tuple[str, ...],
    queue_capacity: int,
    policy: BrokerHostHealthPolicyV1 = _DEFAULT_HEALTH_POLICY,
) -> BrokerHostHealthHeaderV1:
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1
    from histdatacom.broker_plugin_policy.bindings import BrokerLegacyCaptureV1

    if (
        type(session) is not BrokerCaptureSessionV1
        or type(provider_request) is not BrokerLegacyCaptureV1
    ):
        raise ValueError("exact legacy session and capture request required")
    session = BrokerCaptureSessionV1.from_json(session.to_json())
    if provider_request.session.to_json() != session.to_json():
        raise ValueError("legacy health request session substitution")
    _provider_decision(provider_request, provider_decision)
    return BrokerHostHealthHeaderV1(
        Family.LEGACY_CAPTURE_V1,
        session.session_id,
        session.adapter_id,
        session.adapter_id,
        session.adapter_config_sha256,
        None,
        None,
        None,
        provider_decision.artifact_id,
        symbols,
        session.started_at_utc_ns,
        session.started_at_monotonic_ns,
        queue_capacity,
        policy,
    )


@dataclass(frozen=True, slots=True)
class _Native:
    record_id: str
    event_id: str | None
    epoch: int
    utc_ns: int
    monotonic_ns: int
    symbol: str | None = None
    source_ns: int | None = None
    quote: tuple[str, str] | None = None
    source_identity: str | None = None
    connection_id: str | None = None
    heartbeat: bool = False
    gap: bool = False
    reconnect: bool = False
    clock_correction: bool = False
    healthy_claim: bool = False


MAX_NATIVE_HEALTH_BYTES = 64 * 1024 * 1024


def _bounded_native(values: Iterable[object], maximum: int) -> Iterator[object]:
    for index, value in enumerate(values):
        if index >= maximum:
            raise ValueError("native health inventory exceeds frozen bound")
        yield value


def _lifecycle_records(
    manifest: BrokerLifecycleManifestV1,
    records: Iterable[BrokerLifecycleRecordV1],
    maximum: int,
) -> tuple[list[_Native], str, bool]:
    from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleCompletion,
        BrokerLifecycleManifestV1,
        BrokerLifecycleRecordV1,
        BrokerLifecycleState,
        BrokerLifecycleTransitionV1,
    )
    from histdatacom.broker_plugin_lifecycle.evidence import Evidence
    from histdatacom.broker_plugins import BrokerEventKind, BrokerReasonCode

    if type(manifest) is not BrokerLifecycleManifestV1:
        raise ValueError("exact native lifecycle manifest required")
    manifest = BrokerLifecycleManifestV1.from_json(manifest.to_json())
    evidence = Evidence(manifest.header)
    lines: list[bytes] = []
    output: list[_Native] = []
    native_bytes = 0
    for item in _bounded_native(records, maximum):
        if type(item) is not BrokerLifecycleRecordV1:
            raise ValueError("exact native lifecycle record required")
        record = BrokerLifecycleRecordV1.from_json(item.to_json())
        evidence.accept(record)
        line = record.to_json().encode("ascii") + b"\n"
        native_bytes += len(line)
        if native_bytes > MAX_NATIVE_HEALTH_BYTES:
            raise ValueError("native health replay byte bound exceeded")
        lines.append(line)
        if record.kind != "event":
            reconnect = False
            if record.kind == "transition":
                reconnect = (
                    BrokerLifecycleTransitionV1.from_json(
                        record.payload_json
                    ).current
                    is BrokerLifecycleState.RECONNECTING
                )
            output.append(
                _Native(
                    record.artifact_id,
                    None,
                    record.epoch,
                    record.receive_utc_ns,
                    record.receive_monotonic_ns,
                    reconnect=reconnect,
                )
            )
            continue
        admitted = BrokerAdmittedEventV1.from_json(record.payload_json)
        event = admitted.event
        output.append(
            _Native(
                record.artifact_id,
                admitted.artifact_id,
                record.epoch,
                record.receive_utc_ns,
                record.receive_monotonic_ns,
                event.instrument,
                (
                    None
                    if event.source_time is None
                    else event.source_time.timestamp_ns
                ),
                (
                    None
                    if event.quote is None
                    else (event.quote.bid, event.quote.ask)
                ),
                None,
                event.connection_id,
                event.kind is BrokerEventKind.HEARTBEAT,
                event.kind is BrokerEventKind.GAP
                or (
                    event.diagnostic is not None
                    and event.diagnostic.code is BrokerReasonCode.SOURCE_GAP
                ),
                event.kind is BrokerEventKind.RECONNECTING,
                event.kind is BrokerEventKind.CLOCK_CORRECTION,
                event.diagnostic is not None
                and event.diagnostic.code is BrokerReasonCode.HEALTHY,
            )
        )
    offset = 0
    partitions = manifest.partitions + (
        ()
        if manifest.partial_partition is None
        else (manifest.partial_partition,)
    )
    for partition in partitions:
        part = lines[offset : offset + partition.record_count]
        payload = b"".join(part)
        selected = output[offset : offset + partition.record_count]
        if (
            len(part) != partition.record_count
            or len(payload) != partition.byte_count
            or hashlib.sha256(payload).hexdigest() != partition.sha256
            or sum(item.event_id is not None for item in selected)
            != partition.event_count
        ):
            raise ValueError("native lifecycle partition inventory differs")
        offset += partition.record_count
    if (
        offset != len(output)
        or evidence.state is not manifest.state
        or evidence.event_count > manifest.appended_events
        or (evidence.unknown_loss and not manifest.unknown_loss)
        or evidence.error_diagnostics > manifest.error_diagnostics
    ):
        raise ValueError(
            "native lifecycle inventory is incomplete or substituted"
        )
    complete = (
        manifest.completion is BrokerLifecycleCompletion.COMPLETE
        and len(output) == manifest.appended_records
    )
    return output, hashlib.sha256(b"".join(lines)).hexdigest(), complete


def _legacy_records(
    manifest: BrokerCaptureSessionManifestV1,
    records: Iterable[BrokerCaptureEventV1],
    maximum: int,
) -> tuple[list[_Native], str, bool]:
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureEventKind,
        BrokerCaptureEventV1,
        BrokerCaptureSessionManifestV1,
        BrokerCaptureSessionState,
    )

    if type(manifest) is not BrokerCaptureSessionManifestV1:
        raise ValueError("exact legacy manifest required")
    manifest = BrokerCaptureSessionManifestV1.from_json(manifest.to_json())
    lines: list[bytes] = []
    output: list[_Native] = []
    kinds: list[str] = []
    epoch = 0
    clock = manifest.session.started_at_monotonic_ns
    native_bytes = 0
    for item in _bounded_native(records, maximum):
        if type(item) is not BrokerCaptureEventV1:
            raise ValueError("exact legacy record required")
        event = BrokerCaptureEventV1.from_json(item.to_json())
        if (
            event.session_id != manifest.session.session_id
            or event.capture_sequence != len(output)
            or event.receive_time_monotonic_ns < clock
        ):
            raise ValueError(
                "legacy capture record sequence/session/clock differs"
            )
        clock = event.receive_time_monotonic_ns
        message = event.message
        kinds.append(message.kind.value)
        if message.kind in (
            BrokerCaptureEventKind.RECONNECT,
            BrokerCaptureEventKind.PROCESS_RESTART,
        ):
            epoch += 1
        source_id = message.source_message_id
        if source_id is None and message.source_sequence is not None:
            source_id = str(message.source_sequence)
        quote = None
        if message.kind is BrokerCaptureEventKind.QUOTE:
            quote = (
                message.bid_text or repr(message.bid),
                message.ask_text or repr(message.ask),
            )
        output.append(
            _Native(
                event.event_id,
                (
                    None
                    if message.kind is BrokerCaptureEventKind.CLOCK_CORRECTION
                    else message.message_id
                ),
                epoch,
                event.receive_time_utc_ns,
                event.receive_time_monotonic_ns,
                message.symbol,
                message.source_event_time_ns,
                quote,
                source_id,
                message.connection_id,
                message.kind is BrokerCaptureEventKind.HEARTBEAT,
                message.kind
                in (
                    BrokerCaptureEventKind.GAP,
                    BrokerCaptureEventKind.OUTAGE_START,
                ),
                message.kind
                in (
                    BrokerCaptureEventKind.RECONNECT,
                    BrokerCaptureEventKind.PROCESS_RESTART,
                ),
                message.kind is BrokerCaptureEventKind.CLOCK_CORRECTION,
            )
        )
        line = event.to_json().encode("utf-8") + b"\n"
        native_bytes += len(line)
        if native_bytes > MAX_NATIVE_HEALTH_BYTES:
            raise ValueError("native health replay byte bound exceeded")
        lines.append(line)
    offset = 0
    for part in manifest.partitions:
        payload = b"".join(lines[offset : offset + part.event_count])
        if (
            hashlib.sha256(payload).hexdigest() != part.data_artifact.sha256
            or len(payload) != part.data_artifact.size_bytes
        ):
            raise ValueError(
                "legacy partition bytes differ from retained native inventory"
            )
        selected = output[offset : offset + part.event_count]
        if (
            dict(Counter(kinds[offset : offset + part.event_count]))
            != part.event_kind_counts
        ):
            raise ValueError(
                "legacy partition kind counts differ from native records"
            )
        if (
            len(selected) != part.event_count
            or not selected
            or (
                selected[0].utc_ns,
                selected[-1].utc_ns,
                selected[0].monotonic_ns,
                selected[-1].monotonic_ns,
            )
            != (
                part.first_receive_time_utc_ns,
                part.last_receive_time_utc_ns,
                part.first_receive_time_monotonic_ns,
                part.last_receive_time_monotonic_ns,
            )
        ):
            raise ValueError("legacy partition timing/count inventory differs")
        offset += part.event_count
    if offset != len(output) or len(output) != manifest.event_count:
        raise ValueError(
            "legacy health requires complete retained native inventory"
        )
    return (
        output,
        hashlib.sha256(b"".join(lines)).hexdigest(),
        manifest.state is BrokerCaptureSessionState.COMPLETED,
    )


@dataclass(slots=True)
class _Bucket:
    counts: Counter[str] = field(default_factory=Counter)
    source_deltas: list[int] = field(default_factory=list)
    persistence: list[int] = field(default_factory=list)
    queue_maximum: int | None = None
    heartbeat_gap: int | None = None


def _reduce(
    header: BrokerHostHealthHeaderV1,
    observations: Iterable[BrokerHostHealthObservationV1],
    native: list[_Native],
    native_sha: str,
    manifest_id: str,
    native_complete: bool,
) -> BrokerHostHealthAuditV1:
    header = BrokerHostHealthHeaderV1.from_json(header.to_json())
    policy = header.policy
    records = {item.record_id: item for item in native}
    if len(records) != len(native):
        raise ValueError("duplicate native record identities")
    event_records: dict[tuple[int, str], _Native] = {}
    for native_item in native:
        if native_item.event_id is not None:
            event_records.setdefault(
                (native_item.epoch, native_item.event_id), native_item
            )
    buckets: dict[tuple[int, str, int], _Bucket] = {}

    def bucket(epoch: int, symbol: str | None, mono: int) -> _Bucket:
        if mono < header.started_at_monotonic_ns:
            raise ValueError("observation precedes bound capture start")
        if symbol is not None and symbol not in header.symbols:
            raise ValueError("native symbol outside exact selected inventory")
        key = (
            epoch,
            symbol or "",
            (mono - header.started_at_monotonic_ns) // policy.bucket_width_ns,
        )
        if key not in buckets:
            if len(buckets) >= policy.max_buckets:
                raise ValueError("health bucket inventory exceeds frozen bound")
            buckets[key] = _Bucket()
        return buckets[key]

    def sample(target: list[int], value: int) -> None:
        if len(target) >= policy.max_samples_per_bucket:
            raise ValueError(
                "health timing sample inventory exceeds frozen bound"
            )
        target.append(value)

    pending: dict[int, BrokerHostHealthObservationV1] = {}
    completed: set[int] = set()
    persisted: set[str] = set()
    seen_events: set[tuple[int, str]] = set()
    seen_quotes: set[tuple[object, ...]] = set()
    last_quote: dict[tuple[int, str | None], _Native] = {}
    first_source_seen: dict[tuple[int, str | None, int], int] = {}
    heartbeats: dict[int, int] = {}
    queue: BrokerHostHealthObservationV1 | None = None
    last_mono = header.started_at_monotonic_ns
    last_offset = header.started_at_utc_ns - header.started_at_monotonic_ns
    digest = hashlib.sha256()
    count = 0
    ingress_count = 0
    closed = False
    close_reason = Reason.NONE
    overflow_windows: set[tuple[int, int]] = set()

    def overflow(epoch: int, monotonic_ns: int) -> None:
        overflow_windows.add(
            (
                epoch,
                (monotonic_ns - header.started_at_monotonic_ns)
                // policy.bucket_width_ns,
            )
        )

    def queue_slo_breached(counts: Counter[str]) -> bool:
        return counts[
            "queue_saturation_ns"
        ] > policy.max_queue_saturation_ns or (
            policy.max_queue_saturation_ns == 0
            and counts["queue_saturation_events"] > 0
        )

    def queue_interval(end: int) -> None:
        if queue is None or queue.queue_items is None:
            return
        start = queue.monotonic_ns
        while start < end:
            index = (
                start - header.started_at_monotonic_ns
            ) // policy.bucket_width_ns
            stop = min(
                end,
                header.started_at_monotonic_ns
                + (index + 1) * policy.bucket_width_ns,
            )
            entry = bucket(queue.epoch, None, start)
            entry.queue_maximum = max(
                entry.queue_maximum or 0, queue.queue_items
            )
            entry.counts["queue_covered_ns"] += stop - start
            if queue.queue_items == header.queue_capacity:
                entry.counts["queue_saturation_ns"] += stop - start
            start = stop

    for raw in observations:
        if (
            type(raw) is not BrokerHostHealthObservationV1
            or count >= policy.max_observations
        ):
            raise ValueError("invalid/beyond-bound host observation inventory")
        item = BrokerHostHealthObservationV1.from_json(raw.to_json())
        if item.sequence != count or closed or item.monotonic_ns < last_mono:
            raise ValueError("host observation omission/reorder/after-close")
        digest.update(item.to_json().encode("ascii") + b"\n")
        count += 1
        offset = item.utc_ns - item.monotonic_ns
        if abs(offset - last_offset) > policy.max_clock_jump_ns:
            clock_bucket = bucket(item.epoch, None, item.monotonic_ns)
            clock_bucket.counts["clock_jump_count"] += 1
            clock_bucket.counts["host_clock_jump_count"] += 1
        last_offset, last_mono = offset, item.monotonic_ns
        if item.kind is Kind.QUEUE:
            assert item.queue_items is not None
            if item.queue_items > header.queue_capacity:
                raise ValueError("queue exceeds declared native capacity")
            queue_interval(item.monotonic_ns)
            entry = bucket(item.epoch, None, item.monotonic_ns)
            entry.counts["queue_samples"] += 1
            entry.queue_maximum = max(
                entry.queue_maximum or 0, item.queue_items
            )
            if item.queue_items == header.queue_capacity and (
                queue is None or queue.queue_items != header.queue_capacity
            ):
                entry.counts["queue_saturation_events"] += 1
            if item.reason is Reason.QUEUE_OVERFLOW:
                # A drained snapshot cannot erase the host's explicit overflow
                # observation. Its closed reason remains in the hashed journal;
                # do not invent a dropped-event count when none is identifiable.
                overflow(item.epoch, item.monotonic_ns)
            queue = item
            continue
        if item.kind is Kind.CLOSE:
            queue_interval(item.monotonic_ns)
            closed, close_reason = True, item.reason
            for epoch, previous_heartbeat in heartbeats.items():
                entry = bucket(epoch, None, item.monotonic_ns)
                entry.heartbeat_gap = max(
                    entry.heartbeat_gap or 0,
                    item.monotonic_ns - previous_heartbeat,
                )
            continue
        associated = (
            None
            if item.event_id is None
            else event_records.get((item.epoch, item.event_id))
        )
        if associated is not None and associated.epoch != item.epoch:
            raise ValueError("event observation substitutes connection epoch")
        symbol = None if associated is None else associated.symbol
        if item.kind is Kind.INGRESS:
            if item.ingress_sequence != ingress_count:
                raise ValueError(
                    "ingress inventory is not complete and contiguous"
                )
            ingress_count += 1
            assert item.ingress_sequence is not None
            pending[item.ingress_sequence] = item
            entry = bucket(item.epoch, symbol, item.monotonic_ns)
            entry.counts[
                "malformed" if item.event_id is None else "received"
            ] += 1
            continue
        if item.kind is Kind.PERSISTED:
            record = records.get(item.native_record_id or "")
            if (
                record is None
                or record.record_id in persisted
                or record.epoch != item.epoch
                or record.event_id != item.event_id
                or item.monotonic_ns < record.monotonic_ns
            ):
                raise ValueError(
                    "persistence observation lacks exact native record"
                )
            # Every native record, including host control, has one completion.
            if record.record_id != native[len(persisted)].record_id:
                raise ValueError("persistence order differs from native order")
            persisted.add(record.record_id)
            associated = record
            if record.event_id is None:
                entry = bucket(item.epoch, None, record.monotonic_ns)
                entry.counts["reconnect_count"] += record.reconnect
                entry.counts["clock_jump_count"] += record.clock_correction
                entry.counts[
                    "clock_correction_count"
                ] += record.clock_correction
                continue
        assert item.ingress_sequence is not None
        ingress = pending.get(item.ingress_sequence)
        if (
            ingress is None
            or item.ingress_sequence in completed
            or ingress.epoch != item.epoch
            or ingress.event_id != item.event_id
        ):
            raise ValueError(
                "terminal observation does not match exact ingress"
            )
        completed.add(item.ingress_sequence)
        entry = bucket(item.epoch, symbol, ingress.monotonic_ns)
        if item.kind is Kind.REFUSED:
            entry.counts["refused"] += 1
            if item.reason is Reason.QUEUE_OVERFLOW:
                overflow(item.epoch, ingress.monotonic_ns)
            if item.reason is Reason.DUPLICATE_DELIVERY:
                if (
                    item.event_id is None
                    or (item.epoch, item.event_id) not in seen_events
                ):
                    raise ValueError(
                        "duplicate refusal has no prior persisted canonical event"
                    )
                entry.counts["delivery_retries"] += 1
            elif item.event_id is not None:
                entry.counts["known_host_dropped"] += 1
            continue
        assert associated is not None
        if associated.monotonic_ns < ingress.monotonic_ns:
            raise ValueError("native receive timestamp predates host ingress")
        seen_events.add((item.epoch, associated.event_id or ""))
        entry.counts["persisted"] += 1
        sample(entry.persistence, item.monotonic_ns - ingress.monotonic_ns)
        entry.counts["heartbeat_count"] += associated.heartbeat
        entry.counts["gap_count"] += associated.gap
        entry.counts["reconnect_count"] += associated.reconnect
        entry.counts["clock_jump_count"] += associated.clock_correction
        entry.counts["clock_correction_count"] += associated.clock_correction
        entry.counts["healthy_claim_count"] += associated.healthy_claim
        if associated.heartbeat:
            previous_heartbeat = heartbeats.get(
                item.epoch, header.started_at_monotonic_ns
            )
            entry.heartbeat_gap = max(
                entry.heartbeat_gap or 0,
                ingress.monotonic_ns - previous_heartbeat,
            )
            heartbeats[item.epoch] = ingress.monotonic_ns
        if associated.source_ns is None:
            entry.counts["source_clock_missing"] += 1
        else:
            sample(entry.source_deltas, ingress.utc_ns - associated.source_ns)
        if associated.quote is None:
            continue
        key = (item.epoch, associated.symbol)
        previous = last_quote.get(key)
        if previous is not None:
            entry.counts["unchanged_quotes"] += (
                previous.quote == associated.quote
            )
            if (
                previous.source_ns is not None
                and associated.source_ns is not None
            ):
                entry.counts["reordered_source_times"] += (
                    associated.source_ns < previous.source_ns
                )
                if (
                    abs(
                        (associated.source_ns - previous.source_ns)
                        - (associated.monotonic_ns - previous.monotonic_ns)
                    )
                    > policy.max_clock_jump_ns
                ):
                    entry.counts["source_clock_jump_count"] += 1
                    entry.counts["clock_jump_count"] += 1
        if (
            associated.source_ns is not None
            or associated.source_identity is not None
        ):
            quote_key = (
                item.epoch,
                associated.symbol,
                associated.connection_id,
                associated.source_ns,
                associated.source_identity,
                associated.quote,
            )
            entry.counts["exact_quote_duplicates"] += quote_key in seen_quotes
            seen_quotes.add(quote_key)
        if associated.source_ns is not None:
            source_key = (item.epoch, associated.symbol, associated.source_ns)
            first = first_source_seen.setdefault(
                source_key, ingress.monotonic_ns
            )
            # Age of a repeated source instant on the host monotonic clock;
            # never call a raw uncalibrated source/host UTC difference latency.
            entry.counts["stale_quotes"] += (
                ingress.monotonic_ns - first > policy.stale_after_ns
            )
        last_quote[key] = associated
    if set(records) != persisted:
        raise ValueError(
            "native persisted inventory has missing host observations"
        )
    # Incomplete runs retain unresolved ingress as missing evidence, not loss=0.
    complete = (
        closed
        and close_reason is Reason.NONE
        and len(completed) == len(pending)
        and native_complete
    )
    result: list[BrokerHostHealthBucketV1] = []
    failures: set[Failure] = set()
    if overflow_windows:
        failures.add(Failure.QUEUE_SATURATION)
    if not complete:
        failures.add(Failure.INCOMPLETE)
    received = sum(entry.counts["received"] for entry in buckets.values())
    if received < policy.minimum_events:
        failures.add(Failure.NO_EVENTS)
    if policy.require_known_upstream_loss:
        failures.add(Failure.UPSTREAM_UNKNOWN)
    if policy.require_queue_observations and not any(
        entry.counts["queue_samples"] for entry in buckets.values()
    ):
        failures.add(Failure.QUEUE_UNOBSERVED)
    bad_claim_windows: set[tuple[int, int]] = set(overflow_windows)
    for (epoch, _symbol, index), entry in buckets.items():
        c = entry.counts
        if any(
            c[name]
            for name in (
                "known_host_dropped",
                "delivery_retries",
                "exact_quote_duplicates",
                "stale_quotes",
                "reordered_source_times",
                "gap_count",
                "reconnect_count",
                "clock_jump_count",
                "malformed",
                "refused",
            )
        ):
            bad_claim_windows.add((epoch, index))
        if (
            entry.persistence
            and max(entry.persistence) > policy.max_persistence_p95_ns
        ):
            bad_claim_windows.add((epoch, index))
        if (
            policy.max_heartbeat_gap_ns is not None
            and entry.heartbeat_gap is not None
            and entry.heartbeat_gap > policy.max_heartbeat_gap_ns
        ) or queue_slo_breached(c):
            bad_claim_windows.add((epoch, index))
    for (epoch, symbol, index), entry in sorted(buckets.items()):
        c = entry.counts
        c["healthy_claim_discrepancies"] = (
            c["healthy_claim_count"]
            if (epoch, index) in bad_claim_windows
            else 0
        )
        source = summarize_health_times(
            entry.source_deltas, maximum_samples=policy.max_samples_per_bucket
        )
        persistence = summarize_health_times(
            entry.persistence, maximum_samples=policy.max_samples_per_bucket
        )
        dropped = health_rate(c["known_host_dropped"], c["received"])
        duplicates = health_rate(
            c["delivery_retries"] + c["exact_quote_duplicates"], c["received"]
        )
        stale = health_rate(c["stale_quotes"], c["persisted"])
        if (
            dropped.value is not None
            and dropped.value > policy.max_known_drop_rate
        ):
            failures.add(Failure.HOST_DROP)
        if (
            duplicates.value is not None
            and duplicates.value > policy.max_duplicate_rate
        ):
            failures.add(Failure.DUPLICATE)
        if stale.value is not None and stale.value > policy.max_stale_rate:
            failures.add(Failure.STALE)
        if c["reordered_source_times"]:
            failures.add(Failure.REORDER)
        if (
            persistence.p95_ns is not None
            and persistence.p95_ns > policy.max_persistence_p95_ns
        ):
            failures.add(Failure.PERSISTENCE_LAG)
        if queue_slo_breached(c):
            failures.add(Failure.QUEUE_SATURATION)
        for name, reason in (
            ("host_clock_jump_count", Failure.CLOCK_JUMP),
            ("source_clock_jump_count", Failure.CLOCK_JUMP),
            ("healthy_claim_discrepancies", Failure.PLUGIN_CLAIM),
            ("malformed", Failure.MALFORMED),
            ("refused", Failure.MALFORMED),
        ):
            if c[name]:
                failures.add(reason)
        if (
            c["gap_count"] > policy.max_reported_source_gap_events
            or c["reconnect_count"] > policy.max_reconnect_events
        ):
            failures.add(Failure.SOURCE_GAP)
        if c["clock_correction_count"] > policy.max_clock_correction_events:
            failures.add(Failure.CLOCK_JUMP)
        if (
            policy.max_heartbeat_gap_ns is not None
            and entry.heartbeat_gap is not None
            and entry.heartbeat_gap > policy.max_heartbeat_gap_ns
        ):
            failures.add(Failure.HEARTBEAT_GAP)
        little = None
        # A partial last bucket is not a full declared observation window.
        # Neither missing residences nor dropped arrivals may be silently
        # substituted into the stationary-window arithmetic.
        full_window = (
            header.started_at_monotonic_ns
            + (index + 1) * policy.bucket_width_ns
            <= last_mono
        )
        if (
            persistence.mean_ns is not None
            and persistence.count == c["received"] == c["persisted"]
            and full_window
        ):
            little = little_law_diagnostic(
                c["received"],
                policy.bucket_width_ns,
                persistence.mean_ns,
                stationary_window_declared=policy.stationary_window_declared
                and complete,
            )
        result.append(
            BrokerHostHealthBucketV1(
                epoch=epoch,
                symbol=symbol or None,
                bucket=index,
                received=c["received"],
                persisted=c["persisted"],
                malformed=c["malformed"],
                refused=c["refused"],
                known_host_dropped=c["known_host_dropped"],
                delivery_retries=c["delivery_retries"],
                exact_quote_duplicates=c["exact_quote_duplicates"],
                unchanged_quotes=c["unchanged_quotes"],
                stale_quotes=c["stale_quotes"],
                reordered_source_times=c["reordered_source_times"],
                heartbeat_count=c["heartbeat_count"],
                gap_count=c["gap_count"],
                reconnect_count=c["reconnect_count"],
                host_clock_jump_count=c["host_clock_jump_count"],
                source_clock_jump_count=c["source_clock_jump_count"],
                clock_correction_count=c["clock_correction_count"],
                clock_jump_count=c["clock_jump_count"],
                healthy_claim_count=c["healthy_claim_count"],
                healthy_claim_discrepancies=c["healthy_claim_discrepancies"],
                source_clock_missing=c["source_clock_missing"],
                upstream_loss_unknown=True,
                queue_samples=c["queue_samples"],
                queue_maximum=entry.queue_maximum,
                queue_covered_ns=c["queue_covered_ns"],
                queue_saturation_ns=c["queue_saturation_ns"],
                queue_saturation_events=c["queue_saturation_events"],
                max_heartbeat_gap_ns=entry.heartbeat_gap,
                source_to_receive_delta=source,
                receive_to_persist=persistence,
                known_drop_rate=dropped,
                duplicate_rate=duplicates,
                stale_rate=stale,
                little_law_mean_in_system=little,
            )
        )
    if policy.max_heartbeat_gap_ns is not None and not heartbeats:
        failures.add(Failure.HEARTBEAT_GAP)
    insufficient = {
        Failure.NO_EVENTS,
        Failure.INCOMPLETE,
        Failure.QUEUE_UNOBSERVED,
        Failure.UPSTREAM_UNKNOWN,
    }
    state = (
        State.QUALIFIED
        if not failures
        else State.INSUFFICIENT if failures & insufficient else State.DEGRADED
    )
    return BrokerHostHealthAuditV1(
        header,
        manifest_id,
        native_sha,
        digest.hexdigest(),
        count,
        len(native),
        complete,
        state,
        tuple(sorted(failures)),
        tuple(result),
    )


def replay_lifecycle_host_health(
    header: BrokerHostHealthHeaderV1,
    observations: Iterable[BrokerHostHealthObservationV1],
    manifest: BrokerLifecycleManifestV1,
    records: Iterable[BrokerLifecycleRecordV1],
    invocation: BrokerSDKInvocationV1,
    permission_manifest: BrokerPermissionManifestV1,
    permission_context: BrokerPermissionContextV1,
    permission_decision: BrokerPermissionDecisionV1,
    provider_decision: BrokerPolicyDecisionV1,
) -> BrokerHostHealthAuditV1:
    expected = make_lifecycle_health_header(
        manifest.header,
        invocation,
        permission_manifest,
        permission_context,
        permission_decision,
        provider_decision,
        started_at_utc_ns=header.started_at_utc_ns,
        started_at_monotonic_ns=header.started_at_monotonic_ns,
        policy=header.policy,
    )
    if header != expected:
        raise ValueError(
            "lifecycle health header does not bind supplied native evidence"
        )
    native, digest, complete = _lifecycle_records(
        manifest, records, header.policy.max_observations
    )
    return _reduce(
        header, observations, native, digest, manifest.artifact_id, complete
    )


def replay_legacy_host_health(
    header: BrokerHostHealthHeaderV1,
    observations: Iterable[BrokerHostHealthObservationV1],
    manifest: BrokerCaptureSessionManifestV1,
    records: Iterable[BrokerCaptureEventV1],
    provider_decision: BrokerPolicyDecisionV1,
    *,
    provider_request: BrokerLegacyCaptureV1,
) -> BrokerHostHealthAuditV1:
    expected = make_legacy_health_header(
        manifest.session,
        provider_decision,
        provider_request=provider_request,
        symbols=header.symbols,
        queue_capacity=header.queue_capacity,
        policy=header.policy,
    )
    if header != expected:
        raise ValueError(
            "legacy health header does not bind supplied native evidence"
        )
    native, digest, complete = _legacy_records(
        manifest, records, header.policy.max_observations
    )
    return _reduce(
        header, observations, native, digest, manifest.manifest_id, complete
    )
