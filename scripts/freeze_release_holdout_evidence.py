#!/usr/bin/env python3
"""Freeze declared release-holdout evidence before candidate fitting."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.synthetic.benchmark_corpus import (
    BenchmarkWindowPartitionV1,
    ReverseDegradationCorpusProfileV1,
    build_reverse_degradation_benchmark_corpus,
    write_reverse_degradation_benchmark_corpus,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.release_holdout import (
    ProtectedReleaseHoldoutWindowV1,
    ReleaseHoldoutAccessPolicyV1,
    ReleaseHoldoutAuditStatus,
    ReleaseHoldoutDevelopmentUnitV1,
    build_protected_release_holdout_manifest,
    write_protected_release_holdout_manifest,
    write_release_holdout_access_policy,
)
from histdatacom.synthetic.release_holdout_evaluation import (
    load_default_release_holdout_evaluation_policy,
    write_release_holdout_evaluation_policy,
)

_DURATION_NS = 600_000_000_000
_DECLARATION_SCHEMA_VERSION_V1 = "histdatacom.release-holdout-declaration.v1"
_DECLARATION_SCHEMA_VERSION_V2 = "histdatacom.release-holdout-declaration.v2"
_DEFAULT_CLAIM_SCOPE = "v2.5-marked-hawkes-release-decision-successor-1"
_DEFAULT_RESOURCE_BOUNDS: Mapping[str, int | float] = {
    "max_source_bytes": 4 * 1024**3,
    "max_runtime_seconds": 3600.0,
    "max_peak_memory_bytes": 2 * 1024**3,
}
_DEFAULT_SPLIT_PERIODS: Mapping[str, str] = {
    "calibration": "202512",
    "validation": "202601",
    "final_holdout": "202606",
}
_DEFAULT_DECLARATIONS: Mapping[str, tuple[tuple[str, str], ...]] = {
    "calibration": (
        ("2025-12-03T00:00:00Z", "asia"),
        ("2025-12-12T07:00:00Z", "london"),
        ("2025-12-10T19:00:00Z", "new_york"),
        ("2025-12-23T16:00:00Z", "overlap_closure"),
    ),
    "validation": (
        ("2026-01-05T00:00:00Z", "asia"),
        ("2026-01-15T07:00:00Z", "london"),
        ("2026-01-28T19:00:00Z", "new_york"),
        ("2026-01-22T16:00:00Z", "overlap_closure"),
    ),
    "final_holdout": (
        # June 1 lacks synchronized triangle support at the month opening.
        ("2026-06-02T00:00:00Z", "asia"),
        ("2026-06-09T08:00:00Z", "london"),
        ("2026-06-17T18:00:00Z", "new_york"),
        ("2026-06-26T16:00:00Z", "overlap_closure"),
    ),
}
_SPLIT_KINDS = ("calibration", "validation", "final_holdout")
_HOLDOUT_AXES = {
    "asia": (
        "ordinary",
        "high_retention_low_infill",
        "exact",
        "low",
    ),
    "london": (
        "ordinary",
        "central_fitted_retention",
        "bounded_nearest",
        "median",
    ),
    "new_york": (
        "event",
        "low_retention_high_infill",
        "exact",
        "high",
    ),
    "overlap_closure": (
        "ordinary",
        "central_fitted_retention",
        "bounded_nearest",
        "median",
    ),
}
_HOLDOUT_AXIS_FIELDS = (
    "event_stratum",
    "observation_scenario_id",
    "alignment_kind",
    "deficit_stratum",
)


def _timestamp_ns(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("window declaration must use an aware timestamp")
    return int(parsed.timestamp()) * 1_000_000_000


def _development_source_cutoff_ns(
    split_periods: Mapping[str, str | Sequence[str]],
) -> int:
    """Return the first month boundary after all development splits."""
    development_periods = tuple(
        str(period)
        for split_kind, raw_periods in split_periods.items()
        if split_kind != "final_holdout"
        for period in (
            (raw_periods,) if isinstance(raw_periods, str) else raw_periods
        )
    )
    if not development_periods:
        raise ValueError("release evidence requires a development split")
    period = max(development_periods)
    if len(period) != 6 or not period.isdigit():
        raise ValueError("development split period must use YYYYMM")
    year, month = int(period[:4]), int(period[4:])
    if month < 1 or month > 12:
        raise ValueError("development split month is invalid")
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1
    return int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp()) * (
        1_000_000_000
    )


def _predeclared_intervals(
    declarations: Mapping[str, Sequence[tuple[str, str]]],
) -> Mapping[str, tuple[tuple[int, int, str], ...]]:
    return {
        split_kind: tuple(
            (
                _timestamp_ns(timestamp),
                _timestamp_ns(timestamp) + _DURATION_NS,
                session,
            )
            for timestamp, session in values
        )
        for split_kind, values in declarations.items()
    }


def _period_for_timestamp_ns(timestamp_ns: int) -> str:
    timestamp = datetime.fromtimestamp(
        timestamp_ns / 1_000_000_000, tz=timezone.utc
    )
    return f"{timestamp.year:04d}{timestamp.month:02d}"


def _validated_period(value: Any, *, field: str) -> str:
    period = str(value)
    if len(period) != 6 or not period.isdigit():
        raise ValueError(f"{field} must use YYYYMM")
    month = int(period[4:])
    if month < 1 or month > 12:
        raise ValueError(f"{field} month is invalid")
    return period


def _validated_holdout_axes(
    value: Any,
) -> Mapping[str, tuple[str, str, str, str]]:
    if not isinstance(value, Mapping) or set(value) != set(_HOLDOUT_AXES):
        raise ValueError("holdout_axes must declare each required session")
    normalized: dict[str, tuple[str, str, str, str]] = {}
    for session in _HOLDOUT_AXES:
        raw = value[session]
        if not isinstance(raw, Mapping) or set(raw) != set(
            _HOLDOUT_AXIS_FIELDS
        ):
            raise ValueError(f"holdout_axes {session} fields differ")
        fields = (
            str(raw["event_stratum"]).strip(),
            str(raw["observation_scenario_id"]).strip(),
            str(raw["alignment_kind"]).strip(),
            str(raw["deficit_stratum"]).strip(),
        )
        if any(not field for field in fields):
            raise ValueError(f"holdout_axes {session} contains an empty value")
        normalized[session] = fields

    for index, field in enumerate(_HOLDOUT_AXIS_FIELDS):
        expected = Counter(values[index] for values in _HOLDOUT_AXES.values())
        observed = Counter(values[index] for values in normalized.values())
        if observed != expected:
            raise ValueError(
                f"holdout_axes {field} coverage differs from established policy"
            )
    return normalized


def _load_declaration(
    path: Path | None,
) -> tuple[
    Mapping[str, str],
    Mapping[str, tuple[tuple[str, str], ...]],
    str,
    Mapping[str, int | float],
    Mapping[str, tuple[str, str, str, str]],
]:
    if path is None:
        return (
            dict(_DEFAULT_SPLIT_PERIODS),
            dict(_DEFAULT_DECLARATIONS),
            _DEFAULT_CLAIM_SCOPE,
            dict(_DEFAULT_RESOURCE_BOUNDS),
            dict(_HOLDOUT_AXES),
        )

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise TypeError("release-holdout declaration must be an object")
    schema_version = str(payload.get("schema_version", ""))
    expected_keys = {
        "schema_version",
        "claim_scope",
        "resource_bounds",
        "split_periods",
        "windows",
    }
    if schema_version == _DECLARATION_SCHEMA_VERSION_V2:
        expected_keys.add("holdout_axes")
    if set(payload) != expected_keys:
        raise ValueError(
            "release-holdout declaration fields differ: "
            f"expected {sorted(expected_keys)}"
        )
    if schema_version not in {
        _DECLARATION_SCHEMA_VERSION_V1,
        _DECLARATION_SCHEMA_VERSION_V2,
    }:
        raise ValueError("release-holdout declaration schema differs")
    claim_scope = str(payload["claim_scope"]).strip()
    if not claim_scope:
        raise ValueError("release-holdout declaration claim_scope is empty")

    resource_values = payload["resource_bounds"]
    resource_keys = {
        "max_source_bytes",
        "max_runtime_seconds",
        "max_peak_memory_bytes",
    }
    if not isinstance(resource_values, Mapping) or set(resource_values) != (
        resource_keys
    ):
        raise ValueError("release-holdout resource_bounds differ")
    if any(isinstance(resource_values[name], bool) for name in resource_keys):
        raise TypeError("release-holdout resource bounds must be numeric")
    resource_bounds: Mapping[str, int | float] = {
        "max_source_bytes": int(resource_values["max_source_bytes"]),
        "max_runtime_seconds": float(resource_values["max_runtime_seconds"]),
        "max_peak_memory_bytes": int(resource_values["max_peak_memory_bytes"]),
    }
    if any(value <= 0 for value in resource_bounds.values()):
        raise ValueError("release-holdout resource bounds must be positive")

    split_values = payload["split_periods"]
    window_values = payload["windows"]
    if not isinstance(split_values, Mapping) or set(split_values) != set(
        _SPLIT_KINDS
    ):
        raise ValueError("release-holdout split_periods differ")
    if not isinstance(window_values, Mapping) or set(window_values) != set(
        _SPLIT_KINDS
    ):
        raise ValueError("release-holdout window splits differ")
    split_periods = {
        split_kind: _validated_period(
            split_values[split_kind], field=f"{split_kind} period"
        )
        for split_kind in _SPLIT_KINDS
    }

    declarations: dict[str, tuple[tuple[str, str], ...]] = {}
    seen_intervals: list[tuple[int, int]] = []
    required_sessions = set(_HOLDOUT_AXES)
    for split_kind in _SPLIT_KINDS:
        raw_windows = window_values[split_kind]
        if not isinstance(raw_windows, Sequence) or isinstance(
            raw_windows, (str, bytes)
        ):
            raise TypeError(f"{split_kind} windows must be an array")
        selected: list[tuple[str, str]] = []
        for index, raw_window in enumerate(raw_windows):
            if not isinstance(raw_window, Mapping) or set(raw_window) != {
                "start_utc",
                "session",
            }:
                raise ValueError(f"{split_kind} window {index} fields differ")
            start_utc = str(raw_window["start_utc"])
            session = str(raw_window["session"])
            start_ns = _timestamp_ns(start_utc)
            if _period_for_timestamp_ns(start_ns) != split_periods[split_kind]:
                raise ValueError(
                    f"{split_kind} window {index} is outside its period"
                )
            interval = (start_ns, start_ns + _DURATION_NS)
            if any(
                interval[0] < prior_end and prior_start < interval[1]
                for prior_start, prior_end in seen_intervals
            ):
                raise ValueError("release-holdout declaration windows overlap")
            seen_intervals.append(interval)
            selected.append((start_utc, session))
        if len(selected) != 4 or {session for _, session in selected} != (
            required_sessions
        ):
            raise ValueError(
                f"{split_kind} must declare each required session exactly once"
            )
        declarations[split_kind] = tuple(selected)
    holdout_axes = (
        _validated_holdout_axes(payload["holdout_axes"])
        if schema_version == _DECLARATION_SCHEMA_VERSION_V2
        else dict(_HOLDOUT_AXES)
    )
    return (
        split_periods,
        declarations,
        claim_scope,
        resource_bounds,
        holdout_axes,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        canonical_contract_json(value).encode("utf-8")
    ).hexdigest()


def _context_event_ids(
    window: BenchmarkWindowPartitionV1,
    *,
    timeline_id: str,
    events: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    selected = tuple(
        sorted(
            str(item["event_id"])
            for item in events
            if window.start_ns <= int(item["event_time_ns"]) < window.end_ns
        )
    )
    if selected:
        return selected
    return (f"{timeline_id}#ordinary:{window.window_id}",)


def _identity_fields(
    window: BenchmarkWindowPartitionV1,
    *,
    source_by_id: Mapping[str, Any],
    timeline_id: str,
    context_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    context_event_ids = _context_event_ids(
        window,
        timeline_id=timeline_id,
        events=context_events,
    )
    partition_ids = tuple(
        sorted(
            f"{source_by_id[item].partition_id}#window:"
            f"{window.start_ns}:{window.end_ns}"
            for item in window.source_partition_ids
        )
    )
    source_hashes = dict(window.symbol_partition_sha256)
    source_signature = _sha256(
        {
            "source_partition_ids": partition_ids,
            "source_hashes": source_hashes,
        }
    )
    motif_signature = _sha256(
        {
            "event_state_counts": dict(window.event_state_counts),
            "symbol_event_counts": dict(window.symbol_event_counts),
            "selection_rule": window.selection_rule,
        }
    )
    context_signature = _sha256(
        {
            "context_event_ids": context_event_ids,
            "context_state": window.context_state,
            "positioning_state": window.positioning_state,
        }
    )
    return {
        "period": window.period,
        "start_ns": window.start_ns,
        "end_ns": window.end_ns,
        "source_partition_ids": partition_ids,
        "source_hashes": source_hashes,
        "source_signature_sha256": source_signature,
        "motif_signature_sha256": motif_signature,
        "context_signature_sha256": context_signature,
        "source_neighbor_sketch": source_signature[:16],
        "motif_neighbor_sketch": motif_signature[:16],
        "context_neighbor_sketch": context_signature[:16],
        "cohesion_group_ids": (f"whole-window:{window.window_id}",),
        "anchor_neighborhood_ids": (f"anchor-neighborhood:{window.window_id}",),
        "context_event_ids": context_event_ids,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument(
        "--source-projection-manifest",
        type=Path,
        help="optional lineage manifest for canonical projected sources",
    )
    parser.add_argument("--feed-epoch-definition", required=True, type=Path)
    parser.add_argument("--observation-campaign", required=True, type=Path)
    parser.add_argument("--market-context-corpus", required=True, type=Path)
    parser.add_argument("--cftc-positioning-corpus", required=True, type=Path)
    parser.add_argument("--selection-dossier", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--frozen-at-utc", required=True)
    parser.add_argument(
        "--declaration-file",
        type=Path,
        help=(
            "strict row-free release-holdout declaration JSON; defaults to "
            "the issue-#512 successor declaration"
        ),
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    split_periods, declarations, claim_scope, resource_bounds, holdout_axes = (
        _load_declaration(args.declaration_file)
    )
    profile = ReverseDegradationCorpusProfileV1(
        split_periods=split_periods,
        synchronized_windows_per_split=4,
        window_duration_seconds=600,
        minimum_events_per_symbol=64,
        max_events_per_symbol=256,
        neighbor_guard_seconds=1800,
        ensemble_member_ids=tuple(
            f"member-{index:02d}" for index in range(1, 9)
        ),
        max_source_bytes=int(resource_bounds["max_source_bytes"]),
        max_runtime_seconds=float(resource_bounds["max_runtime_seconds"]),
        max_peak_memory_bytes=int(resource_bounds["max_peak_memory_bytes"]),
    )
    source_cutoff_ns = _development_source_cutoff_ns(profile.split_periods)
    corpus = build_reverse_degradation_benchmark_corpus(
        args.source_root,
        feed_epoch_definition_path=args.feed_epoch_definition,
        observation_campaign_path=args.observation_campaign,
        market_context_corpus_path=args.market_context_corpus,
        cftc_positioning_corpus_path=args.cftc_positioning_corpus,
        source_projection_manifest_path=args.source_projection_manifest,
        profile=profile,
        predeclared_window_intervals=_predeclared_intervals(declarations),
    )
    if corpus.neighbor_leakage_count != 0:
        raise RuntimeError("predeclared benchmark corpus has split leakage")

    context_payload = json.loads(
        args.market_context_corpus.read_text(encoding="utf-8")
    )
    timeline = context_payload["timeline"]
    timeline_id = str(timeline["timeline_id"])
    context_events = tuple(timeline["events"])
    source_by_id = {item.partition_id: item for item in corpus.sources}
    development: list[ReleaseHoldoutDevelopmentUnitV1] = []
    protected: list[ProtectedReleaseHoldoutWindowV1] = []
    for window in corpus.windows:
        fields = _identity_fields(
            window,
            source_by_id=source_by_id,
            timeline_id=timeline_id,
            context_events=context_events,
        )
        if window.split_kind == "final_holdout":
            event, scenario, alignment, deficit = holdout_axes[window.session]
            if event == "event" and window.context_state.startswith(
                "market_context:none:"
            ):
                raise RuntimeError("predeclared event window lacks context")
            protected.append(
                ProtectedReleaseHoldoutWindowV1(
                    **fields,
                    symbol_event_counts=window.symbol_event_counts,
                    epoch_stratum=window.epoch_label,
                    session_stratum=window.session,
                    event_stratum=event,
                    observation_scenario_id=scenario,
                    alignment_kind=alignment,
                    deficit_stratum=deficit,
                )
            )
        else:
            development.append(
                ReleaseHoldoutDevelopmentUnitV1(
                    **fields,
                    split_role=window.split_kind,
                )
            )

    selection_payload = json.loads(
        args.selection_dossier.read_text(encoding="utf-8")
    )
    selection_id = str(selection_payload["dossier_id"])
    selection_ref = artifact_ref_for_file(
        args.selection_dossier,
        kind="hawkes_product_selection_dossier_v1",
        metadata={"dossier_id": selection_id},
    )
    access_policy = ReleaseHoldoutAccessPolicyV1(
        required_feed_epochs=("technology_epoch_04",)
    )
    manifest = build_protected_release_holdout_manifest(
        access_policy,
        protected,
        development,
        selection_dossier_id=selection_id,
        selection_dossier_ref=selection_ref,
        source_cutoff_ns=source_cutoff_ns,
        claim_scope=claim_scope,
        frozen_at_utc=args.frozen_at_utc,
    )
    if (
        manifest.leakage_audit.status is not ReleaseHoldoutAuditStatus.PASS
        or manifest.coverage_audit.status is not ReleaseHoldoutAuditStatus.PASS
    ):
        raise RuntimeError(
            "release-holdout audits failed: "
            f"leakage={manifest.leakage_audit.status.value} "
            f"findings={list(manifest.leakage_audit.finding_codes)}; "
            f"coverage={manifest.coverage_audit.status.value} "
            f"missing={list(manifest.coverage_audit.missing_strata)}"
        )

    output = args.output_directory.resolve()
    refs = {
        "access_policy": write_release_holdout_access_policy(
            access_policy, output
        ),
        "benchmark_corpus": write_reverse_degradation_benchmark_corpus(
            corpus, output
        ),
        "evaluation_policy": write_release_holdout_evaluation_policy(
            load_default_release_holdout_evaluation_policy(), output
        ),
        "protected_manifest": write_protected_release_holdout_manifest(
            manifest, output
        ),
    }
    print(
        json.dumps(
            {
                "corpus_id": corpus.corpus_id,
                "manifest_id": manifest.manifest_id,
                "access_policy_id": access_policy.policy_id,
                "evaluation_policy_id": (
                    load_default_release_holdout_evaluation_policy().policy_id
                ),
                "window_ids": [item.window_id for item in manifest.windows],
                "artifacts": {
                    name: ref.to_dict() for name, ref in sorted(refs.items())
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
