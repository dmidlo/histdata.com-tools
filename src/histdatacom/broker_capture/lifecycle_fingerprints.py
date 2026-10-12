"""Bounded deterministic projection of exact SDK-native capture evidence.

Only actual quote fields are fitted. Host observations are not source/server
timestamps; sessions, source batches, account facts and provider protocols are
never manufactured to pass through the legacy fitter.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from .fingerprint_contracts import (
    BrokerDeliveryCellV1,
    BrokerDeliveryConditionV1,
    BrokerDeliveryMetricV1,
    BrokerDeliverySupportStatus,
)
from .fingerprint_v3 import (
    BrokerDeliveryFingerprintV3,
    BrokerFingerprintLifecycleRootV1,
    BrokerLifecycleFingerprintEligibilityV1,
    BrokerLifecycleFingerprintFitConfigV1,
)

if TYPE_CHECKING:
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1


def _quote_spread(bid: str, ask: str) -> float:
    """Exact bounded SDK decimal lexemes, independent of ambient Decimal state."""
    digits = max(len(text.partition(".")[2]) for text in (bid, ask))
    integers = []
    for text in (bid, ask):
        whole, _, fraction = text.partition(".")
        integers.append(int(whole + fraction.ljust(digits, "0")))
    return float((integers[1] - integers[0]) / 10**digits)


def _bounds(
    manifest: BrokerLifecycleManifestV1,
    policy: BrokerLifecycleFingerprintFitConfigV1,
) -> None:
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )

    from .fingerprints import BrokerDeliveryResourceLimitError

    if type(manifest) is not BrokerLifecycleManifestV1:
        raise ValueError("exact lifecycle manifest required")
    if (
        manifest.appended_records > policy.max_input_records
        or sum(x.byte_count for x in manifest.partitions)
        + (
            manifest.partial_partition.byte_count
            if manifest.partial_partition
            else 0
        )
        > policy.max_input_bytes
    ):
        raise BrokerDeliveryResourceLimitError(
            "native fingerprint input exceeds declared record/byte bounds"
        )


def _source_root(
    directory: Path,
    manifest: BrokerLifecycleManifestV1,
    invocation: BrokerSDKInvocationV1,
    policy: BrokerLifecycleFingerprintFitConfigV1,
    expected: BrokerFingerprintLifecycleRootV1 | None = None,
) -> BrokerFingerprintLifecycleRootV1:
    from histdatacom.broker_plugin_compatibility import (
        read_lifecycle_capture_compatibility,
    )
    from histdatacom.broker_plugin_health.storage import (
        read_lifecycle_host_health,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleCompletion,
    )
    from histdatacom.broker_plugin_lifecycle.storage import (
        inspect_broker_lifecycle,
        replay_broker_lifecycle,
    )
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyOperation,
        require_provider_operation,
    )
    from histdatacom.broker_plugin_provenance.native import (
        read_lifecycle_capture_provenance,
    )

    _bounds(manifest, policy)
    require_provider_operation(invocation, BrokerPolicyOperation.MATERIAL_USE)
    if manifest.completion is not BrokerLifecycleCompletion.COMPLETE:
        raise ValueError(
            "native scientific fit requires a complete finite capture"
        )
    if directory.is_symlink():
        raise ValueError("native scientific source may not be a symlink")
    actual = inspect_broker_lifecycle(directory).manifest
    if actual.to_json() != manifest.to_json():
        raise ValueError("actual native manifest differs from requested source")
    audit = read_lifecycle_host_health(
        directory.with_name(directory.name + "-host-health"),
        manifest,
        replay_broker_lifecycle(
            directory, provider_request=invocation, expected_manifest=manifest
        ),
        invocation,
    )
    proof = read_lifecycle_capture_provenance(
        directory,
        manifest,
        provider_request=invocation,
        expected_root=None if expected is None else expected.seal,
    )
    if (
        expected is None
        and proof.structurally_complete
        and proof.seal is not None
    ):
        # The initial read discovers an expected root, not an external trust
        # grant. Replay again against that exact root before deriving anything.
        proof = read_lifecycle_capture_provenance(
            directory,
            manifest,
            provider_request=invocation,
            expected_root=proof.seal,
        )
    if (
        not proof.complete
        or proof.seal is None
        or (expected is not None and not proof.anchored)
    ):
        raise ValueError(
            "native scientific fit requires complete anchored provenance"
        )
    compatibility = read_lifecycle_capture_compatibility(
        directory,
        manifest,
        provider_request=invocation,
        expected_root=proof.seal,
    )
    result = BrokerFingerprintLifecycleRootV1(
        manifest,
        invocation,
        audit,
        proof.header,
        proof.seal,
        None if compatibility is None else compatibility.to_json(),
    )
    if expected is not None and result.to_json() != expected.to_json():
        raise ValueError(
            "native scientific source changed from its retained root"
        )
    require_provider_operation(invocation, BrokerPolicyOperation.MATERIAL_USE)
    return result


class _Projection:
    def __init__(self, policy: BrokerLifecycleFingerprintFitConfigV1) -> None:
        from .fingerprints import _CellAccumulator

        self.policy = policy
        self.cells: dict[str, _CellAccumulator] = {}
        self.records = 0
        self.bytes = 0
        self.source_time_missing: set[str] = set()
        self.source_time_semantics: dict[str, set[str]] = {}

    def consume(
        self, directory: Path, root: BrokerFingerprintLifecycleRootV1
    ) -> BrokerLifecycleFingerprintEligibilityV1:
        from histdatacom.broker_plugin_capabilities.contracts import (
            BrokerAdmittedEventV1,
        )
        from histdatacom.broker_plugin_lifecycle.storage import (
            replay_broker_lifecycle,
        )
        from histdatacom.broker_plugins.contracts import BrokerEventKind

        from .fingerprints import (
            BrokerDeliveryResourceLimitError,
            _CellAccumulator,
        )

        quote_count = record_count = 0
        start: int | None = None
        end: int | None = None
        previous: dict[tuple[int, str, str], int] = {}
        reasons: set[str] = set()
        for record in replay_broker_lifecycle(
            directory,
            provider_request=root.invocation,
            expected_manifest=root.manifest,
        ):
            self.records += 1
            self.bytes += len(record.to_json().encode("ascii")) + 1
            record_count += 1
            if (
                self.records > self.policy.max_input_records
                or self.bytes > self.policy.max_input_bytes
            ):
                raise BrokerDeliveryResourceLimitError(
                    "native fingerprint aggregate stream bound exceeded"
                )
            if record.kind != "event":
                continue
            event = BrokerAdmittedEventV1.from_json(record.payload_json).event
            if event.kind is not BrokerEventKind.QUOTE:
                continue
            quote = event.quote
            assert quote is not None
            quote_count += 1
            if event.receive_time is None:
                reasons.add("quote_transport_receive_time_unavailable")
            start = (
                record.receive_utc_ns
                if start is None
                else min(start, record.receive_utc_ns)
            )
            end = (
                record.receive_utc_ns
                if end is None
                else max(end, record.receive_utc_ns)
            )
            key = (record.epoch, event.connection_id, quote.symbol)
            last = previous.get(key)
            # Never connect cadence samples across capture/session epochs or
            # connections. Zero-resolution host samples are unavailable, not a
            # fabricated positive interval.
            interval = (
                None if last is None else record.receive_monotonic_ns - last
            )
            if interval is not None and interval < 0:
                reasons.add("host_monotonic_clock_regression")
            previous[key] = record.receive_monotonic_ns
            if len(previous) > self.policy.max_cells * 9:
                raise BrokerDeliveryResourceLimitError(
                    "native cadence state bound exceeded"
                )
            spread = _quote_spread(quote.bid, quote.ask)
            decimals = float(
                max(
                    len(text.partition(".")[2])
                    for text in (quote.bid, quote.ask)
                )
            )
            for dimensions in ({}, {"symbol": quote.symbol}):
                condition = BrokerDeliveryConditionV1(dimensions)
                identifier = condition.condition_id
                if identifier not in self.cells:
                    if len(self.cells) >= self.policy.max_cells:
                        raise BrokerDeliveryResourceLimitError(
                            "native fingerprint cell bound exceeded"
                        )
                    self.cells[identifier] = _CellAccumulator(
                        condition,
                        self.policy.max_samples_per_metric,
                        self.policy.rounding_digits,
                    )
                cell = self.cells[identifier]
                cell.observe_quote()
                cell.add("spread", spread, record.artifact_id, unit="price")
                cell.add(
                    "price_decimal_places",
                    decimals,
                    record.artifact_id,
                    unit="digits",
                )
                if interval is not None and interval > 0:
                    cell.add(
                        "quote_interarrival_ns",
                        float(interval),
                        record.artifact_id,
                        unit="nanoseconds",
                    )
                if event.source_time is None:
                    self.source_time_missing.add(identifier)
                else:
                    self.source_time_semantics.setdefault(
                        identifier, set()
                    ).add(event.source_time.semantics.value)
                    cell.add(
                        "source_timestamp_precision_ns",
                        float(event.source_time.precision_ns),
                        record.artifact_id,
                        unit="nanoseconds",
                    )
        if quote_count < self.policy.min_quote_events:
            reasons.add("insufficient_quote_support")
        return BrokerLifecycleFingerprintEligibilityV1(
            root.manifest.artifact_id,
            self.policy.config_id,
            not reasons,
            tuple(sorted(reasons)),
            record_count,
            quote_count,
            start,
            end,
        )

    def finish(self) -> tuple[BrokerDeliveryCellV1, ...]:
        global_id = BrokerDeliveryConditionV1({}).condition_id
        result = []
        for identifier, cell in sorted(self.cells.items()):
            supported = cell.support_count >= self.policy.min_cell_support
            backoffs = () if not cell.condition.dimensions else (global_id,)
            effective = (
                identifier
                if supported
                else (
                    global_id
                    if backoffs
                    and self.cells[global_id].support_count
                    >= self.policy.min_cell_support
                    else None
                )
            )
            status = (
                BrokerDeliverySupportStatus.SUPPORTED
                if supported
                else (
                    BrokerDeliverySupportStatus.BACKED_OFF
                    if effective
                    else BrokerDeliverySupportStatus.UNSUPPORTED
                )
            )
            metrics = {
                name: acc.to_metric(self.policy.quantiles)
                for name, acc in cell.metrics.items()
            }
            for name, unit, reason in (
                (
                    "source_batch_quote_count",
                    "quotes",
                    "native_contract_has_no_source_batch_semantics",
                ),
                (
                    "source_timestamp_precision_ns",
                    "nanoseconds",
                    "source_time_unavailable_for_one_or_more_quotes",
                ),
                (
                    "quote_interarrival_ns",
                    "nanoseconds",
                    "no_positive_same_connection_host_interval",
                ),
            ):
                mixed = (
                    name == "source_timestamp_precision_ns"
                    and len(self.source_time_semantics.get(identifier, ())) > 1
                )
                if (
                    name not in metrics
                    or (
                        name == "source_timestamp_precision_ns"
                        and identifier in self.source_time_missing
                    )
                    or mixed
                ):
                    if mixed:
                        reason = "mixed_source_time_semantics_not_pooled"
                    metrics[name] = BrokerDeliveryMetricV1(
                        name,
                        "distribution",
                        unit,
                        0,
                        0,
                        None,
                        None,
                        None,
                        limitations=(reason,),
                    )
            result.append(
                BrokerDeliveryCellV1(
                    cell.condition,
                    cell.support_count,
                    status,
                    backoffs,
                    effective,
                    tuple(metrics[name] for name in sorted(metrics)),
                    (
                        ()
                        if supported
                        else (
                            (
                                "insufficient_support_using_parent_condition"
                                if effective
                                else "insufficient_support_no_qualified_backoff"
                            ),
                        )
                    ),
                )
            )
        return tuple(result)


def _project(
    directories: Sequence[Path],
    manifests: Sequence[BrokerLifecycleManifestV1],
    requests: Sequence[BrokerSDKInvocationV1],
    policy: BrokerLifecycleFingerprintFitConfigV1,
    *,
    expected: tuple[BrokerFingerprintLifecycleRootV1, ...] | None = None,
    effective_start_utc_ns: int | None = None,
    effective_end_utc_ns: int | None = None,
    supersedes_fingerprint_id: str | None = None,
) -> BrokerDeliveryFingerprintV3:
    from histdatacom.broker_plugin_policy.bindings import sdk_invocation_binding

    from .fingerprints import BrokerDeliveryResourceLimitError

    if (
        not 1 <= len(manifests) <= policy.max_capture_manifests
        or len(requests) != len(manifests)
        or len(directories) != len(manifests)
    ):
        raise ValueError(
            "bounded exact native source/request inventory required"
        )
    if (
        sum(x.appended_records for x in manifests) > policy.max_input_records
        or sum(p.byte_count for x in manifests for p in x.partitions)
        > policy.max_input_bytes
    ):
        raise BrokerDeliveryResourceLimitError(
            "aggregate native input bounds exceeded"
        )
    if len({sdk_invocation_binding(x).artifact_id for x in requests}) != 1:
        raise ValueError(
            "native fingerprint cannot mix incompatible plugin/configuration identities"
        )
    projection = _Projection(policy)
    roots, decisions = [], []
    for i, (directory, manifest, request) in enumerate(
        zip(directories, manifests, requests)
    ):
        source = _source_root(
            directory,
            manifest,
            request,
            policy,
            None if expected is None else expected[i],
        )
        decision = projection.consume(directory, source)
        if not decision.eligible:
            raise ValueError(
                "native capture ineligible: " + ",".join(decision.reason_codes)
            )
        # Re-read actual bytes/current rights after the projection, anchored to
        # the evidence used by this pass. No cached verification claim survives.
        _source_root(directory, manifest, request, policy, source)
        roots.append(source)
        decisions.append(decision)
    start = min(
        x.first_receive_time_utc_ns
        for x in decisions
        if x.first_receive_time_utc_ns is not None
    )
    end = max(
        x.last_receive_time_utc_ns
        for x in decisions
        if x.last_receive_time_utc_ns is not None
    )
    return BrokerDeliveryFingerprintV3(
        policy,
        tuple(roots),
        tuple(decisions),
        start,
        end,
        start if effective_start_utc_ns is None else effective_start_utc_ns,
        effective_end_utc_ns,
        projection.finish(),
        supersedes_fingerprint_id,
    )


def assess_lifecycle_fingerprint_eligibility(
    directory: str | Path,
    manifest: BrokerLifecycleManifestV1,
    *,
    provider_request: BrokerSDKInvocationV1,
    config: BrokerLifecycleFingerprintFitConfigV1 | None = None,
) -> BrokerLifecycleFingerprintEligibilityV1:
    from histdatacom.broker_plugin_policy import BrokerPolicyError

    policy = config or BrokerLifecycleFingerprintFitConfigV1()
    _bounds(manifest, policy)
    try:
        root = _source_root(Path(directory), manifest, provider_request, policy)
        result = _Projection(policy).consume(Path(directory), root)
        _source_root(Path(directory), manifest, provider_request, policy, root)
        return result
    except BrokerPolicyError:
        raise
    except (OSError, ValueError):
        return BrokerLifecycleFingerprintEligibilityV1(
            manifest.artifact_id,
            policy.config_id,
            False,
            ("native_source_integrity_health_or_provenance_unqualified",),
            0,
            0,
            None,
            None,
        )


def fit_lifecycle_delivery_fingerprint(
    directory: str | Path,
    manifests: Sequence[BrokerLifecycleManifestV1],
    *,
    provider_requests: Sequence[BrokerSDKInvocationV1],
    config: BrokerLifecycleFingerprintFitConfigV1 | None = None,
    effective_start_utc_ns: int | None = None,
    effective_end_utc_ns: int | None = None,
    supersedes: BrokerDeliveryFingerprintV3 | None = None,
) -> BrokerDeliveryFingerprintV3:
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyOperation,
        require_provider_operation,
    )
    from histdatacom.broker_plugin_policy.native_fingerprint_bindings import (
        BrokerLifecycleFingerprintFitV1,
    )

    from .fingerprint_sources import (
        _lifecycle_directory,
        broker_fingerprint_sources,
    )
    from .fingerprint_v2 import require_qualified_broker_fingerprint
    from .fingerprints import BrokerDeliveryResourceLimitError

    policy = config or BrokerLifecycleFingerprintFitConfigV1()
    if type(policy) is not BrokerLifecycleFingerprintFitConfigV1:
        raise ValueError(
            "SDK-native fitting requires an explicit native configuration"
        )
    if len(manifests) > policy.max_capture_manifests:
        raise BrokerDeliveryResourceLimitError(
            "capture manifests exceed max_capture_manifests"
        )
    if len(manifests) != len(provider_requests) or len(
        {x.artifact_id for x in manifests}
    ) != len(manifests):
        raise ValueError("native source/request inventories differ or repeat")
    pairs = sorted(
        zip(manifests, provider_requests), key=lambda pair: pair[0].artifact_id
    )
    ordered = tuple(x[0] for x in pairs)
    requests = tuple(x[1] for x in pairs)
    require_provider_operation(
        BrokerLifecycleFingerprintFitV1(requests, policy),
        BrokerPolicyOperation.DERIVE,
    )
    if supersedes is not None:
        if type(supersedes) is not BrokerDeliveryFingerprintV3:
            raise ValueError(
                "native supersession requires an exact V3 predecessor"
            )
        require_qualified_broker_fingerprint(supersedes)
        require_provider_operation(
            supersedes, BrokerPolicyOperation.MATERIAL_USE
        )
    with broker_fingerprint_sources(directory):
        directories = tuple(
            _lifecycle_directory(x.header.artifact_id) for x in ordered
        )
        result = _project(
            directories,
            ordered,
            requests,
            policy,
            effective_start_utc_ns=effective_start_utc_ns,
            effective_end_utc_ns=effective_end_utc_ns,
            supersedes_fingerprint_id=(
                None if supersedes is None else supersedes.fingerprint_id
            ),
        )
        if supersedes is not None:
            from histdatacom.broker_plugin_policy.bindings import (
                sdk_invocation_binding,
            )

            if (
                result.effective_start_utc_ns
                <= supersedes.effective_start_utc_ns
                or sdk_invocation_binding(result.capture_roots[0].invocation)
                != sdk_invocation_binding(
                    supersedes.capture_roots[0].invocation
                )
            ):
                raise ValueError(
                    "native successor must preserve exact provider/configuration identity and advance effective start"
                )
        require_provider_operation(result, BrokerPolicyOperation.DERIVE)
    return result


def verify_lifecycle_fingerprint_sources(
    fingerprint: BrokerDeliveryFingerprintV3,
) -> None:
    from .fingerprint_sources import _lifecycle_directory

    restored = BrokerDeliveryFingerprintV3.from_json(fingerprint.to_json())
    roots = restored.capture_roots
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyOperation,
        require_provider_operation,
    )

    for root in roots:
        require_provider_operation(
            root.invocation, BrokerPolicyOperation.MATERIAL_USE
        )
    actual = _project(
        tuple(
            _lifecycle_directory(x.manifest.header.artifact_id) for x in roots
        ),
        tuple(x.manifest for x in roots),
        tuple(x.invocation for x in roots),
        restored.fit_config,
        expected=roots,
        effective_start_utc_ns=restored.effective_start_utc_ns,
        effective_end_utc_ns=restored.effective_end_utc_ns,
        supersedes_fingerprint_id=restored.supersedes_fingerprint_id,
    )
    if actual.to_json() != restored.to_json():
        raise ValueError(
            "native fingerprint statistics differ from actual bounded source projection"
        )
