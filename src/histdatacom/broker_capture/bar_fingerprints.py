"""Delivery fingerprints with separately bound causal bar-state evidence.

The original delivery model still fits verified capture events. Reconstructed
bar states describe products rendered under that exact profile; they are not
claimed to be simultaneous capture observations or independent across widths.
"""

from __future__ import annotations

from collections.abc import Sequence
import math
from pathlib import Path
import re
import statistics
from typing import Any, cast

from histdatacom.broker_capture.contracts import BrokerCaptureSessionManifestV1
from histdatacom.broker_capture.fingerprint_contracts import (
    BrokerDeliveryDriftConfigV1,
    BrokerDeliveryFingerprintV1,
    BrokerDeliveryFitConfigV1,
)
from histdatacom.broker_capture.fingerprints import (
    compare_broker_delivery_fingerprints,
    fit_broker_delivery_fingerprint,
)
from histdatacom.synthetic.bar_features import (
    MAX_BAR_FEATURE_COLLECTION_ITEMS,
    BarFeatureConsumerResultV1,
    BarFeaturePolicyV1,
    BarFeatureSourceV1,
    BarFeatureState,
    BarFeatureValueV1,
    CausalBarSnapshotV1,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bars import STANDARD_DERIVED_BAR_INTERVALS
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    ReconstructionProductManifestV1,
    load_reconstruction_manifest,
)


def fit_broker_delivery_fingerprint_with_bar_state(
    root: str | Path,
    manifests: Sequence[BrokerCaptureSessionManifestV1],
    *,
    source: BarFeatureSourceV1,
    snapshots: Sequence[CausalBarSnapshotV1],
    information_mode: InformationMode,
    config: BrokerDeliveryFitConfigV1 | None = None,
) -> BarFeatureConsumerResultV1:
    """Fit delivery events and summarize unique closed-bar feature support.

    Source product profile ownership must equal the newly fitted fingerprint.
    Snapshots must have one policy. Overlapping widths remain separate cells;
    repeated snapshots cannot increase the support of the same feature bar.
    """
    if not 1 <= len(snapshots) <= MAX_BAR_FEATURE_COLLECTION_ITEMS:
        raise ValueError("fingerprint snapshot support exceeds bounds")
    policies = {item.policy.artifact_id for item in snapshots}
    if len(policies) != 1 or len(
        {item.artifact_id for item in snapshots}
    ) != len(snapshots):
        raise ValueError(
            "fingerprint requires unique snapshots with one exact policy"
        )
    for snapshot in snapshots:
        source.verify_snapshot(snapshot, information_mode=information_mode)
    product = load_reconstruction_manifest(source.reconstruction_manifest_path)
    if not isinstance(product, ReconstructionProductManifestV1):
        raise ValueError(
            "broker bar-state fitting requires a broker-specific source product, not generic delivery"
        )
    fingerprint = fit_broker_delivery_fingerprint(
        root, manifests, config=config
    )
    if product.broker_profile_id != fingerprint.fingerprint_id:
        raise ValueError(
            "bar product was not rendered under this exact delivery fingerprint"
        )
    evidence: dict[tuple[str, str, str, int, str], dict[str, object]] = {}
    for snapshot in snapshots:
        for cell in snapshot.cells:
            for value in cell.values:
                key = (
                    snapshot.symbol,
                    cell.scope.value,
                    cell.interval_code,
                    cell.bar_start_ns,
                    value.name,
                )
                item: dict[str, object] = {
                    "symbol": snapshot.symbol,
                    "scope": cell.scope.value,
                    "interval_code": cell.interval_code,
                    "bar_start_ns": cell.bar_start_ns,
                    "name": value.name,
                    "value": value.value,
                    "state": value.state.value,
                    "source_bar_ids": list(value.source_bar_ids),
                    "available_at_ns": value.available_at_ns,
                }
                if key in evidence and evidence[key] != item:
                    raise ValueError(
                        "fingerprint has conflicting state for one closed-bar feature"
                    )
                evidence[key] = item
                if len(evidence) > MAX_BAR_FEATURE_COLLECTION_ITEMS:
                    raise ValueError(
                        "fingerprint bar evidence exceeds bounded support"
                    )
    ordered_evidence = [evidence[key] for key in sorted(evidence)]
    summaries = _state_summaries(ordered_evidence)
    return BarFeatureConsumerResultV1(
        consumer="broker_fingerprint",
        information_mode=information_mode,
        snapshot_ids=tuple(sorted(item.artifact_id for item in snapshots)),
        policy_ids=tuple(policies),
        result_json=canonical_bar_feature_json(
            {
                "state_schema_version": "histdatacom.causal-broker-bar-state.v1",
                "association": "verified_rendering_profile_not_simultaneous_capture",
                "delivery_fingerprint": fingerprint.to_dict(),
                "source_product_manifest_id": product.manifest_id,
                "feature_policy": snapshots[0].policy.to_dict(),
                "bar_evidence": ordered_evidence,
                "state_summaries": summaries,
            }
        ),
    )


def _state_summaries(
    evidence: list[dict[str, object]],
) -> list[dict[str, object]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for item in evidence:
        key = tuple(
            cast(str, item[name])
            for name in ("symbol", "scope", "interval_code", "name")
        )
        groups.setdefault(cast(tuple[str, str, str, str], key), []).append(item)
    summaries = []
    for group_key, items in sorted(groups.items()):
        values = [
            float(cast(int | float, item["value"]))
            for item in items
            if item["value"] is not None
        ]
        try:
            mean = statistics.mean(values) if values else None
            variance = statistics.pvariance(values) if values else None
        except (OverflowError, ValueError) as exc:
            raise ValueError(
                "bar fingerprint summary is not representable"
            ) from exc
        if (mean is not None and not math.isfinite(mean)) or (
            variance is not None and not math.isfinite(variance)
        ):
            raise ValueError("bar fingerprint summary is not representable")
        summaries.append(
            {
                "symbol": group_key[0],
                "scope": group_key[1],
                "interval_code": group_key[2],
                "name": group_key[3],
                "support_count": len(values),
                "unavailable_count": len(items) - len(values),
                "mean": mean,
                "population_variance": variance,
            }
        )
    return summaries


def _validated_fit(
    artifact: BarFeatureConsumerResultV1,
) -> tuple[BrokerDeliveryFingerprintV1, dict[str, object]]:
    body = artifact.result()
    if set(body) != {
        "state_schema_version",
        "association",
        "delivery_fingerprint",
        "source_product_manifest_id",
        "feature_policy",
        "bar_evidence",
        "state_summaries",
    }:
        raise ValueError("bar fingerprint result schema is missing or unknown")
    if (
        body["state_schema_version"] != "histdatacom.causal-broker-bar-state.v1"
        or body["association"]
        != "verified_rendering_profile_not_simultaneous_capture"
    ):
        raise ValueError("bar fingerprint result has unsupported semantics")
    product_id = body["source_product_manifest_id"]
    if type(product_id) is not str or not re.fullmatch(
        r"reconstruction-manifest(?:-v[23])?:sha256:[a-f0-9]{64}", product_id
    ):
        raise ValueError("bar fingerprint source identity is malformed")
    if not isinstance(body["feature_policy"], dict) or not isinstance(
        body["delivery_fingerprint"], dict
    ):
        raise ValueError(
            "bar fingerprint requires typed policy/fingerprint objects"
        )
    policy = BarFeaturePolicyV1.from_dict(body["feature_policy"])
    if (
        artifact.policy_ids != (policy.artifact_id,)
        or artifact.information_mode is not policy.information_mode
    ):
        raise ValueError("bar fingerprint policy binding differs")
    fingerprint = BrokerDeliveryFingerprintV1.from_dict(
        body["delivery_fingerprint"]
    )
    records = body["bar_evidence"]
    if not isinstance(records, list) or not records:
        raise ValueError("bar fingerprint evidence is not a nonempty array")
    keys = []
    for item in records:
        if not isinstance(item, dict) or set(item) != {
            "symbol",
            "scope",
            "interval_code",
            "bar_start_ns",
            "name",
            "value",
            "state",
            "source_bar_ids",
            "available_at_ns",
        }:
            raise ValueError("bar fingerprint evidence fields differ")
        if type(item["symbol"]) is not str or not re.fullmatch(
            r"[A-Z]{6}", item["symbol"]
        ):
            raise ValueError("bar fingerprint symbol is malformed")
        if (
            item["scope"] not in tuple(scope.value for scope in policy.scopes)
            or item["interval_code"] not in policy.intervals
            or item["name"] not in policy.feature_names
        ):
            raise ValueError("bar fingerprint evidence is outside its policy")
        start = item["bar_start_ns"]
        if (
            type(start) is not int
            or start < 0
            or start % STANDARD_DERIVED_BAR_INTERVALS[item["interval_code"]]
        ):
            raise ValueError("bar fingerprint evidence has unaligned bounds")
        value = BarFeatureValueV1(
            item["name"],
            item["value"],
            BarFeatureState(item["state"]),
            item["source_bar_ids"],
            item["available_at_ns"],
        )
        if (
            policy.information_mode is InformationMode.EX_ANTE_SIMULATION
            and ActivitySliceScope(item["scope"])
            is not ActivitySliceScope.OBSERVED
            and value.value is not None
        ):
            raise ValueError(
                "generated ex-ante fingerprint state is unsupported"
            )
        keys.append(
            (
                item["symbol"],
                item["scope"],
                item["interval_code"],
                start,
                item["name"],
            )
        )
    if keys != sorted(set(keys)):
        raise ValueError("bar fingerprint evidence is duplicate or unordered")
    expected = _state_summaries(records)
    if canonical_bar_feature_json(
        {"summaries": body["state_summaries"]}
    ) != canonical_bar_feature_json({"summaries": expected}):
        raise ValueError(
            "bar fingerprint summaries do not replay from retained evidence"
        )
    return fingerprint, body


def compare_broker_delivery_fingerprints_with_bar_state(
    reference: BarFeatureConsumerResultV1,
    candidate: BarFeatureConsumerResultV1,
    *,
    information_mode: InformationMode,
    config: BrokerDeliveryDriftConfigV1 | None = None,
) -> BarFeatureConsumerResultV1:
    """Compare immutable fitted artifacts; do not reclassify source evidence.

    As with existing fingerprint comparison, this compares supplied fitted
    artifacts, not raw capture authenticity. Availability is never upgraded.
    The exact fitted result identities and policy must accompany the output.
    """
    for item in (reference, candidate):
        if (
            item.consumer != "broker_fingerprint"
            or item.information_mode is not information_mode
        ):
            raise ValueError(
                "bar comparison requires same-mode fitted bar fingerprints"
            )
    if reference.policy_ids != candidate.policy_ids:
        raise ValueError(
            "bar fingerprint comparison requires the same feature policy"
        )
    left_fp, left = _validated_fit(reference)
    right_fp, right = _validated_fit(candidate)
    comparison = compare_broker_delivery_fingerprints(
        left_fp, right_fp, config=config
    )
    indexed = []
    for result in (left, right):
        records = cast(list[dict[str, Any]], result["state_summaries"])
        indexed.append(
            {
                (
                    item["symbol"],
                    item["scope"],
                    item["interval_code"],
                    item["name"],
                ): item
                for item in records
            }
        )
    deltas = []
    for key in sorted(set(indexed[0]) | set(indexed[1])):
        before, after = indexed[0].get(key), indexed[1].get(key)
        a = before["mean"] if before is not None else None
        b = after["mean"] if after is not None else None
        deltas.append(
            {
                "symbol": key[0],
                "scope": key[1],
                "interval_code": key[2],
                "name": key[3],
                "reference": before,
                "candidate": after,
                "mean_delta": None if a is None or b is None else b - a,
            }
        )
    return BarFeatureConsumerResultV1(
        consumer="broker_comparison",
        information_mode=information_mode,
        snapshot_ids=tuple(
            sorted(set(reference.snapshot_ids + candidate.snapshot_ids))
        ),
        policy_ids=reference.policy_ids,
        result_json=canonical_bar_feature_json(
            {
                "reference_result_id": reference.artifact_id,
                "candidate_result_id": candidate.artifact_id,
                "delivery_comparison": comparison.to_dict(),
                "state_comparisons": deltas,
            }
        ),
    )


__all__ = [
    "fit_broker_delivery_fingerprint_with_bar_state",
    "compare_broker_delivery_fingerprints_with_bar_state",
]
