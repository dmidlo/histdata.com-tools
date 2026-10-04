"""Read-only native intake. Never accept caller-provided quote projections."""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
import hashlib
from pathlib import Path
from typing import Any

from histdatacom.broker_capture.contracts import (
    BrokerCaptureSessionManifestV1,
)
from histdatacom.broker_plugin_lifecycle.contracts import (
    BrokerLifecycleManifestV1,
)
from histdatacom.broker_plugin_policy.bindings import (
    BrokerLegacyCaptureV1,
    BrokerSDKInvocationV1,
    _restore_native,
)
from histdatacom.broker_plugin_provenance.contracts import (
    BrokerProvenanceSealV1,
)

from ._wire import RationalV1
from .capture_contracts import NativeCaptureV1, NativeQuoteV1
from .rights import snapshot_request


@dataclass(frozen=True, slots=True)
class NativeCaptureRefV1:
    """Process-local locator plus independently retained expected evidence.

    A path or a freshly self-sealed capture is not an independent trust anchor.
    The operator retains the seal/health IDs separately from mutable capture IO.
    """

    directory: Path
    manifest: BrokerCaptureSessionManifestV1 | BrokerLifecycleManifestV1
    provider_request: BrokerLegacyCaptureV1 | BrokerSDKInvocationV1
    expected_root: BrokerProvenanceSealV1
    expected_health_id: str


def _snapshot(ref: NativeCaptureRefV1) -> NativeCaptureRefV1:
    if type(ref) is not NativeCaptureRefV1 or not isinstance(
        ref.directory, Path
    ):
        raise ValueError("exact native capture reference required")
    manifest = _restore_native(
        ref.manifest,
        {BrokerCaptureSessionManifestV1, BrokerLifecycleManifestV1},
    )
    request = snapshot_request(ref.provider_request)
    seal = _restore_native(ref.expected_root, {BrokerProvenanceSealV1})
    legacy = type(manifest) is BrokerCaptureSessionManifestV1
    if legacy != (type(request) is BrokerLegacyCaptureV1):
        raise ValueError("capture and invocation native families differ")
    manifest_id = manifest.manifest_id if legacy else manifest.artifact_id
    records = manifest.event_count if legacy else manifest.appended_records
    byte_count = sum(
        part.data_artifact.size_bytes if legacy else part.byte_count
        for part in manifest.partitions
    )
    if (
        records > 8192
        or byte_count > 16 * 1024 * 1024
        or seal.entry_count > 32768
    ):
        raise ValueError("cross-feed native capture admission resource bound")
    if (
        seal.terminal.native_manifest_id != manifest_id
        or seal.terminal.health_audit_id != ref.expected_health_id
        or not seal.terminal.native_complete
        or not seal.terminal.observations_complete
    ):
        raise ValueError("expected seal does not bind complete native evidence")
    if ref.directory.is_symlink():
        raise ValueError("cross-feed native directory cannot be a symlink")
    return NativeCaptureRefV1(
        ref.directory, manifest, request, seal, ref.expected_health_id
    )


def _health(audit: Any, ref: NativeCaptureRefV1) -> None:
    from histdatacom.broker_plugin_health.contracts import (
        BrokerHostHealthAuditV1,
    )

    if (
        type(audit) is not BrokerHostHealthAuditV1
        or audit.artifact_id != ref.expected_health_id
        or not audit.complete_observations
        or audit.observation_count > 32768
    ):
        raise ValueError(
            "complete independently bound host-health audit required"
        )


def _prefix_health(
    directory: Path, audit: Any
) -> dict[str, tuple[tuple[str, ...], str]]:
    """Facts known by each persisted-record frontier; never future buckets.

    Full health replay has already verified journal ordering and attribution.
    This projection does not call an incomplete prefix a qualified audit.
    """
    from histdatacom.broker_plugin_health.storage import _observations

    result = {}
    digest = hashlib.sha256()
    count = 0
    reasons = {"upstream_loss_unidentified"}
    previous_offset = (
        audit.header.started_at_utc_ns - audit.header.started_at_monotonic_ns
    )
    for item in _observations(directory, audit.header.policy.max_observations):
        digest.update(item.to_json().encode("ascii") + b"\n")
        count += 1
        offset = item.utc_ns - item.monotonic_ns
        if (
            abs(offset - previous_offset)
            > audit.header.policy.max_clock_jump_ns
        ):
            reasons.add("clock_discontinuity")
        previous_offset = offset
        reason = item.reason.value
        if reason in (
            "malformed_event",
            "capability_refused",
            "permission_refused",
            "provider_policy_refused",
        ):
            reasons.add("malformed_or_refused_event")
        elif reason == "queue_overflow":
            reasons.add("known_host_drop_rate")
        elif reason == "duplicate_delivery":
            reasons.add("duplicate_rate")
        elif reason not in ("none", "unknown_loss"):
            reasons.add("host_observation:" + reason)
        if item.kind.value == "persisted":
            if item.native_record_id is None or item.native_record_id in result:
                raise ValueError("invalid native persistence health frontier")
            result[item.native_record_id] = (
                tuple(sorted(reasons)),
                item.artifact_id,
            )
    if (
        count != audit.observation_count
        or digest.hexdigest() != audit.observations_sha256
    ):
        raise ValueError(
            "native prefix observations differ from verified audit"
        )
    return result


def _health_fields(
    audit: Any,
    epoch: int,
    symbol: str,
    mono: int,
    prefix: tuple[tuple[str, ...], str],
    native_reasons: set[str],
) -> dict[str, Any]:
    bucket = (
        mono - audit.header.started_at_monotonic_ns
    ) // audit.header.policy.bucket_width_ns
    buckets = tuple(
        b
        for b in audit.buckets
        if b.epoch == epoch
        and b.bucket == bucket
        and b.symbol in (None, symbol)
    )
    return {
        "health_state": "native_prefix_diagnostics",
        "health_reasons": tuple(sorted(set(prefix[0]) | native_reasons)),
        "health_bucket_ids": tuple(sorted(b.artifact_id for b in buckets)),
        "prefix_health_observation_id": prefix[1],
    }


def _result(
    ref: NativeCaptureRefV1,
    audit: Any,
    quotes: list[NativeQuoteV1],
    controls: Any,
    count: int,
    family: str,
) -> NativeCaptureV1:
    return NativeCaptureV1(
        root_id=ref.expected_root.artifact_id,
        manifest_id=ref.expected_root.terminal.native_manifest_id,
        health_audit_id=audit.artifact_id,
        health_state=audit.state.value,
        health_reasons=tuple(sorted(f.value for f in audit.failures)),
        quotes=tuple(quotes),
        control_digest=controls.hexdigest(),
        control_count=count,
        source_family=family,
    )


def _legacy(ref: NativeCaptureRefV1) -> NativeCaptureV1:
    from histdatacom.broker_capture.storage import BrokerCaptureReplaySourceV1
    from histdatacom.broker_plugin_health.runtime_legacy import (
        read_legacy_host_health,
    )
    from histdatacom.broker_plugin_provenance.native import (
        require_legacy_capture_provenance,
    )

    manifest, request = ref.manifest, ref.provider_request
    assert isinstance(manifest, BrokerCaptureSessionManifestV1)
    assert isinstance(request, BrokerLegacyCaptureV1)
    verification = require_legacy_capture_provenance(
        ref.directory,
        manifest,
        provider_request=request,
        expected_root=ref.expected_root,
    )
    if not verification.complete or not verification.anchored:
        raise ValueError("independently anchored complete capture required")
    audit = read_legacy_host_health(
        ref.directory, manifest, provider_request=request
    )
    _health(audit, ref)
    from histdatacom.broker_plugin_health.runtime_legacy import (
        legacy_host_health_directory,
    )

    prefixes = _prefix_health(
        legacy_host_health_directory(ref.directory, manifest.session), audit
    )
    native_reasons: set[str] = set()
    quotes: list[NativeQuoteV1] = []
    controls = hashlib.sha256()
    count = epoch = clock_epoch = 0
    connection: str | None = None
    for event in BrokerCaptureReplaySourceV1(
        ref.directory, manifest, provider_request=request
    ).iter_events():
        message = event.message
        kind = message.kind.value
        if kind in ("reconnect", "process_restart"):
            epoch += 1
            native_reasons.add("source_gap_or_reconnect")
        if kind == "clock_correction":
            native_reasons.add("clock_discontinuity")
        if kind in (
            "clock_correction",
            "connection_open",
            "reconnect",
            "process_restart",
        ):
            clock_epoch += 1
        if (
            message.connection_id is not None
            and message.connection_id != connection
        ):
            connection = message.connection_id
            clock_epoch += 1
        encoded = event.to_json().encode("ascii")
        if kind != "quote":
            controls.update(encoded + b"\n")
            count += 1
            continue
        if len(quotes) >= 2048:
            raise ValueError("cross-feed quote projection bound")
        assert message.bid is not None and message.ask is not None
        if message.price_text_semantics.value == "source_lexeme":
            assert message.bid_text is not None and message.ask_text is not None
            bid, ask = Fraction(message.bid_text), Fraction(message.ask_text)
            basis = "exact_decimal"
        else:
            bid, ask = Fraction.from_float(message.bid), Fraction.from_float(
                message.ask
            )
            basis = "exact_binary64_no_source_decimal_precision"
        assert message.symbol is not None
        quotes.append(
            NativeQuoteV1(
                capture_id=ref.expected_root.artifact_id,
                event_id=event.event_id,
                record_id=event.event_id,
                sequence=event.capture_sequence,
                epoch_id=f"{epoch}:{clock_epoch}",
                symbol=message.symbol,
                source_time_ns=message.source_event_time_ns,
                source_precision_ns=message.source_timestamp_precision_ns,
                source_time_semantics=message.source_timestamp_semantics.value,
                receive_utc_ns=event.receive_time_utc_ns,
                receive_monotonic_ns=event.receive_time_monotonic_ns,
                clock_id="host_utc_and_monotonic",
                bid=RationalV1.from_fraction(bid),
                ask=RationalV1.from_fraction(ask),
                price_basis=basis,
                source_sequence=(
                    None
                    if message.source_sequence is None
                    else str(message.source_sequence)
                ),
                source_message_id=message.source_message_id,
                source_batch_id=message.source_batch_id,
                native_sha256=hashlib.sha256(encoded).hexdigest(),
                **_health_fields(
                    audit,
                    epoch,
                    message.symbol,
                    event.receive_time_monotonic_ns,
                    prefixes[event.event_id],
                    native_reasons,
                ),
            )
        )
    return _result(ref, audit, quotes, controls, count, "legacy_capture_v1")


def _sdk(ref: NativeCaptureRefV1) -> NativeCaptureV1:
    from histdatacom.broker_plugin_capabilities.contracts import (
        BrokerAdmittedEventV1,
    )
    from histdatacom.broker_plugin_health.storage import (
        read_lifecycle_host_health,
    )
    from histdatacom.broker_plugin_lifecycle.storage import (
        replay_broker_lifecycle,
    )
    from histdatacom.broker_plugin_provenance.native import (
        read_lifecycle_capture_provenance,
    )

    manifest, request = ref.manifest, ref.provider_request
    assert isinstance(manifest, BrokerLifecycleManifestV1)
    assert isinstance(request, BrokerSDKInvocationV1)
    verification = read_lifecycle_capture_provenance(
        ref.directory,
        manifest,
        provider_request=request,
        expected_root=ref.expected_root,
    )
    if not verification.complete or not verification.anchored:
        raise ValueError("independently anchored complete capture required")
    records = tuple(
        replay_broker_lifecycle(ref.directory, provider_request=request)
    )
    audit = read_lifecycle_host_health(
        ref.directory.with_name(ref.directory.name + "-host-health"),
        manifest,
        records,
        request,
    )
    _health(audit, ref)
    prefixes = _prefix_health(
        ref.directory.with_name(ref.directory.name + "-host-health"), audit
    )
    native_reasons: set[str] = set()
    quotes: list[NativeQuoteV1] = []
    controls = hashlib.sha256()
    count = clock_epoch = 0
    connection: str | None = None
    for record in records:
        encoded = record.to_json().encode("ascii")
        event = None
        if record.kind == "event":
            event = BrokerAdmittedEventV1.from_json(record.payload_json).event
            if event.connection_id != connection:
                connection = event.connection_id
                clock_epoch += 1
            if event.kind.value == "clock_correction":
                clock_epoch += 1
                native_reasons.add("clock_discontinuity")
            if event.kind.value in ("reconnecting", "gap"):
                native_reasons.add("source_gap_or_reconnect")
        if event is None or event.quote is None:
            controls.update(encoded + b"\n")
            count += 1
            continue
        if len(quotes) >= 2048:
            raise ValueError("cross-feed quote projection bound")
        source, quote = event.source_time, event.quote
        quotes.append(
            NativeQuoteV1(
                capture_id=ref.expected_root.artifact_id,
                event_id=event.artifact_id,
                record_id=record.artifact_id,
                sequence=record.capture_sequence,
                epoch_id=f"{record.epoch}:{clock_epoch}",
                symbol=quote.symbol,
                source_time_ns=None if source is None else source.timestamp_ns,
                source_precision_ns=(
                    None if source is None else source.precision_ns
                ),
                source_time_semantics=(
                    "unavailable" if source is None else source.semantics.value
                ),
                receive_utc_ns=record.receive_utc_ns,
                receive_monotonic_ns=record.receive_monotonic_ns,
                clock_id="host_utc_and_monotonic",
                bid=RationalV1.from_fraction(Fraction(quote.bid)),
                ask=RationalV1.from_fraction(Fraction(quote.ask)),
                price_basis="exact_decimal",
                source_sequence=None,
                source_message_id=None,
                source_batch_id=None,
                native_sha256=hashlib.sha256(encoded).hexdigest(),
                **_health_fields(
                    audit,
                    record.epoch,
                    quote.symbol,
                    record.receive_monotonic_ns,
                    prefixes[record.artifact_id],
                    native_reasons,
                ),
            )
        )
    return _result(ref, audit, quotes, controls, count, "lifecycle_v1")


def admit_capture(ref: NativeCaptureRefV1) -> NativeCaptureV1:
    """Replay both byte/provenance and current provider-rights boundaries."""
    ref = _snapshot(ref)
    if type(ref.manifest) is BrokerCaptureSessionManifestV1:
        return _legacy(ref)
    return _sdk(ref)
