"""Closed native lineage bindings for broker-conditioned derived output.

These process-local wrappers grant no permissions. They reconcile retained
native relationships, not physical source authenticity or unretained generator
execution. The original producer/publication verifier still owns those checks.
No provider IDs supplied without their actual native fingerprint are accepted.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from ._wire import canonical_json, load_json
from .bindings import (
    _ResolvedNative,
    _composite_ref,
    _preflight_native,
    _ref,
    _resolve_native,
    _restore_native,
)
from .contracts import BrokerPolicyDataClass as DataClass
from .contracts import BrokerPolicySubjectV1

if TYPE_CHECKING:
    from histdatacom.synthetic.activity import ReconstructionActivityManifestV1
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.synthetic.broker_transfer import (
        BrokerConditionedProposalV1,
        BrokerProfileSelectionV1,
        BrokerRenderedGroupV1,
        BrokerTransferManifestV1,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    DerivedNative: TypeAlias = (
        BrokerProfileSelectionV1
        | BrokerConditionedProposalV1
        | BrokerTransferManifestV1
        | BrokerRenderedGroupV1
        | ReconstructionProductManifestV1
        | ReconstructionActivityManifestV1
    )


@dataclass(frozen=True, slots=True)
class BrokerSyntheticOutputV1:
    """Expected derived shape, not a retained artifact or computation proof."""

    fingerprint: BrokerDeliveryFingerprintV1


@dataclass(frozen=True, slots=True)
class BrokerDerivedArtifactV1:
    """Actual native artifact and its complete exact fingerprint inventory."""

    fingerprints: tuple[BrokerDeliveryFingerprintV1, ...]
    artifact: DerivedNative


def _selection_parent(selection: Any, fingerprint: Any) -> None:
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryConditionV1,
        BrokerDeliverySupportStatus,
    )
    from histdatacom.synthetic.broker_transfer import (
        BrokerTransferStatus,
        _resolved_metrics,
    )

    if (
        selection.fingerprint_id != fingerprint.fingerprint_id
        or selection.fingerprint_schema_version != fingerprint.schema_version
        or selection.profile_effective_start_utc_ns
        != fingerprint.effective_start_utc_ns
        or selection.profile_effective_end_utc_ns
        != fingerprint.effective_end_utc_ns
        or selection.supersedes_fingerprint_id
        != fingerprint.supersedes_fingerprint_id
    ):
        raise ValueError("derived selection fingerprint parent differs")
    condition = BrokerDeliveryConditionV1(dict(selection.requested_condition))
    if selection.requested_condition_id != condition.condition_id:
        raise ValueError("derived selection requested condition differs")
    # Refusals have no applied metrics. Their reason/optional comparison are
    # retained native diagnostic declarations, not proof of an omitted input.
    if not selection.applied:
        if selection.metrics or selection.metric_condition_ids:
            raise ValueError("refused selection carries applied metrics")
        return
    if selection.selected_at_utc_ns < fingerprint.effective_start_utc_ns or (
        fingerprint.effective_end_utc_ns is not None
        and selection.selected_at_utc_ns >= fingerprint.effective_end_utc_ns
    ):
        raise ValueError("derived selection is outside profile interval")
    cells = {cell.condition.condition_id: cell for cell in fingerprint.cells}
    requested = cells.get(selection.requested_condition_id)
    effective = cells.get(selection.effective_condition_id)
    if (
        requested is None
        or effective is None
        or requested.effective_condition_id != selection.effective_condition_id
        or requested.support_status.value != selection.support_status
        or requested.support_status is BrokerDeliverySupportStatus.UNSUPPORTED
    ):
        raise ValueError("derived selection does not resolve native cells")
    expected_status = (
        BrokerTransferStatus.BACKED_OFF
        if requested.support_status is BrokerDeliverySupportStatus.BACKED_OFF
        else BrokerTransferStatus.APPLIED
    )
    global_cell = next(
        c for c in fingerprint.cells if not c.condition.dimensions
    )
    metrics, sources = _resolved_metrics(effective, global_cell)
    if (
        selection.status is not expected_status
        or dict(selection.metrics) != metrics
        or dict(selection.metric_condition_ids) != sources
    ):
        raise ValueError("derived selection metrics differ from native parent")


def _rendered_relationships(group: Any) -> None:
    from histdatacom.synthetic.broker_transfer import (
        BrokerTransferStatus,
        _content_sha256,
        _streams_content_sha256,
    )
    from histdatacom.synthetic.contracts import SyntheticEventOrigin
    from histdatacom.synthetic.cross_currency import (
        CrossCurrencyValidationStage,
        _streams_content_sha256 as validation_content_sha256,
    )

    manifest = group.manifest
    if manifest.status is BrokerTransferStatus.REFUSED:
        return
    events = [event for stream in group.streams for event in stream.events]
    synthetic = {
        event.event_id: event
        for event in events
        if event.origin is SyntheticEventOrigin.SYNTHETIC
    }
    observed_count = sum(
        event.origin is SyntheticEventOrigin.OBSERVED for event in events
    )
    validation = group.post_broker_validation
    if (
        validation is None
        or validation.run_id != manifest.run_id
        or validation.window_id != manifest.window_id
        or validation.synchronization_unit_id
        != manifest.synchronization_unit_id
        or validation.ensemble_member_id != manifest.ensemble_member_id
        or validation.symbols
        != tuple(sorted(stream.symbol for stream in group.streams))
        or validation.stage is not CrossCurrencyValidationStage.POST_BROKER
        or validation.observed_event_count != observed_count
        or validation.output_content_sha256
        != validation_content_sha256(group.streams)
    ):
        raise ValueError(
            "rendered validation does not bind actual output scope"
        )
    if len({event.event_id for event in events}) != len(events):
        raise ValueError("rendered output event identities are duplicated")
    if (
        _streams_content_sha256(group.streams) != manifest.output_content_sha256
        or len(synthetic) != manifest.synthetic_event_count
        or observed_count != manifest.observed_event_count
        or _content_sha256([row.to_dict() for row in group.event_lineage])
        != manifest.lineage_content_sha256
        or _content_sha256(group.cross_instrument_quality_payload)
        != manifest.cross_instrument_quality_sha256
    ):
        raise ValueError("rendered native content and manifest differ")
    selections = {item.selection_id for item in manifest.selections}
    seen: set[str] = set()
    actions: Counter[str] = Counter()
    for row in group.event_lineage:
        event = synthetic.get(row.output_event_id)
        if (
            event is None
            or row.output_event_id in seen
            or row.selection_id not in selections
            or row.symbol != event.symbol.upper()
            or row.output_event_time_ns != event.event_time_ns
            or event.broker_profile_id != manifest.fingerprint_id
        ):
            raise ValueError("rendered event lineage does not bind output")
        seen.add(row.output_event_id)
        actions.update(row.actions)
    if seen != set(synthetic) or dict(actions) != dict(manifest.action_counts):
        raise ValueError("rendered event/action inventory differs")
    for stream in group.streams:
        if (
            stream.run_id != manifest.run_id
            or stream.ensemble_member_id != manifest.ensemble_member_id
        ):
            raise ValueError("rendered stream belongs to another run/member")


def _native_snapshot(artifact: object) -> tuple[Any, str, str]:
    from histdatacom.synthetic.broker_transfer import (
        BrokerConditionedProposalV1,
        BrokerProfileSelectionV1,
        BrokerRenderedGroupV1,
        BrokerTransferManifestV1,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    if type(artifact) in {BrokerProfileSelectionV1, BrokerTransferManifestV1}:
        _preflight_native(artifact)
        exact = cast(Any, artifact)
        text = canonical_json(exact.to_dict())
        payload = load_json(text)
        if type(payload) is not dict:
            raise ValueError("native derived payload must be an object")
        restored = type(exact).from_dict(payload)
        if canonical_json(restored.to_dict()) != text:
            raise ValueError("native derived canonical roundtrip differs")
        identity = (
            restored.selection_id
            if type(artifact) is BrokerProfileSelectionV1
            else restored.manifest_id
        )
        return restored, text, identity
    types = {
        BrokerConditionedProposalV1: "proposal_id",
        BrokerTransferManifestV1: "manifest_id",
        BrokerRenderedGroupV1: None,
        ReconstructionProductManifestV1: "manifest_id",
    }
    if type(artifact) not in types:
        raise ValueError("unsupported exact broker-derived native artifact")
    restored = _restore_native(artifact, set(types))
    text = cast(str, restored.to_json())
    field = types[type(artifact)]
    identity = (
        _composite_ref("rendered-group", {"native": load_json(text)}).native_id
        if field is None
        else cast(str, getattr(restored, field))
    )
    return restored, text, identity


def resolve_derived_native(native: object) -> _ResolvedNative:
    """Closed dispatcher called only after lightweight SDK dispatch finishes."""
    from .bar_bindings import _resolve_bar_native

    bar = _resolve_bar_native(native)
    if bar is not None:
        return bar
    from .activity_bindings import _resolve_activity_native

    activity = _resolve_activity_native(native)
    if activity is not None:
        return activity
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.synthetic.broker_transfer import (
        BrokerConditionedProposalV1,
        BrokerProfileSelectionV1,
        BrokerRenderedGroupV1,
        BrokerTransferManifestV1,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    if type(native) is BrokerSyntheticOutputV1:
        fingerprint = _restore_native(
            native.fingerprint, {BrokerDeliveryFingerprintV1}
        )
        parent = _resolve_native(fingerprint).subject
        return _ResolvedNative(
            BrokerPolicySubjectV1(
                _composite_ref(
                    "broker-synthetic-output",
                    {"fingerprint": parent.native_ref},
                ),
                parent.bindings,
                tuple(
                    sorted(
                        set(parent.data_classes) | {DataClass.BROKER_SYNTHETIC}
                    )
                ),
            ),
            None,
        )
    if type(native) in {
        ReconstructionProductManifestV1,
        BrokerProfileSelectionV1,
        BrokerConditionedProposalV1,
        BrokerTransferManifestV1,
        BrokerRenderedGroupV1,
    }:
        from .native_inputs import fingerprint_for

        artifact, _, _ = _native_snapshot(native)
        if type(artifact) is ReconstructionProductManifestV1:
            parent_id = artifact.broker_profile_id
        elif type(artifact) is BrokerConditionedProposalV1:
            parent_id = artifact.selection.fingerprint_id
        elif type(artifact) is BrokerRenderedGroupV1:
            parent_id = artifact.manifest.fingerprint_id
        else:
            parent_id = artifact.fingerprint_id
        native = BrokerDerivedArtifactV1(
            (fingerprint_for(parent_id),), artifact
        )
    if type(native) is not BrokerDerivedArtifactV1:
        raise ValueError("unsupported or incomplete native provider subject")
    if (
        type(native.fingerprints) is not tuple
        or not 1 <= len(native.fingerprints) <= 16
    ):
        raise ValueError("derived fingerprints must be a bounded exact tuple")
    _preflight_native(native)
    fingerprints = tuple(
        _restore_native(item, {BrokerDeliveryFingerprintV1})
        for item in native.fingerprints
    )
    parents = {item.fingerprint_id: item for item in fingerprints}
    if len(parents) != len(fingerprints):
        raise ValueError("derived fingerprint inventory has duplicate parents")
    artifact, text, identity = _native_snapshot(native.artifact)
    selections: tuple[Any, ...] = ()
    if type(artifact) is BrokerProfileSelectionV1:
        fingerprint_id = artifact.fingerprint_id
        selections = (artifact,)
    elif type(artifact) is BrokerConditionedProposalV1:
        fingerprint_id = artifact.selection.fingerprint_id
        selections = (artifact.selection,)
        if artifact.conditioned_query is not None and dict(
            artifact.conditioned_query.condition.metrics
        ) != dict(artifact.metrics_after):
            raise ValueError("conditioned proposal metrics differ from query")
    elif type(artifact) in (BrokerTransferManifestV1, BrokerRenderedGroupV1):
        manifest = (
            artifact.manifest
            if type(artifact) is BrokerRenderedGroupV1
            else artifact
        )
        fingerprint_id = manifest.fingerprint_id
        selections = manifest.selections
        if type(artifact) is BrokerRenderedGroupV1:
            _rendered_relationships(artifact)
    else:
        fingerprint_id = artifact.broker_profile_id
        if artifact.quality.broker_fingerprint_id != fingerprint_id:
            raise ValueError("product quality and broker profile differ")
    if set(parents) != {fingerprint_id}:
        raise ValueError(
            "derived fingerprint inventory does not match native parents"
        )
    fingerprint = parents[fingerprint_id]
    for selection in selections:
        _selection_parent(selection, fingerprint)
    parent = _resolve_native(fingerprint).subject
    classes = set(parent.data_classes)
    if type(artifact) is not BrokerProfileSelectionV1:
        classes.add(DataClass.BROKER_SYNTHETIC)
    ref = _composite_ref(
        "broker-derived-artifact",
        {
            "artifact": _ref(type(artifact).__name__, identity, text),
            "fingerprints": [parent.native_ref],
        },
    )
    return _ResolvedNative(
        BrokerPolicySubjectV1(ref, parent.bindings, tuple(sorted(classes))),
        text,
    )
