"""Cache-scale preflight benchmarks for data-quality runs."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shlex
from time import perf_counter
from typing import Any, cast

from histdatacom import __version__ as HISTDATACOM_VERSION
from histdatacom.data_quality.contracts import (
    QualityReport,
    QualityTarget,
    QualityTargetKind,
    QualityStatus,
)
from histdatacom.data_quality.discovery import (
    QualityDiscoveryError,
    discover_quality_targets,
    normalize_quality_check_groups,
)
from histdatacom.data_quality.engine import run_quality_assessment
from histdatacom.data_quality.polars_cache import read_quality_polars_cache
from histdatacom.data_quality.rules import (
    quality_profile_report_metadata,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
)
from histdatacom.fx_enums import (
    Format,
    PAIR_GROUPS,
    Timeframe,
    expand_pair_selection,
    normalize_pair_group,
)
from histdatacom.orchestration.workflows import activity_execution_policy
from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_path,
)
from histdatacom.runtime_contracts import JSONValue

QUALITY_PREFLIGHT_SCHEMA_VERSION = "histdatacom.quality-preflight.v1"
QUALITY_PREFLIGHT_INSPECTION_SCHEMA_VERSION = (
    "histdatacom.quality-preflight-inspection.v1"
)
DEFAULT_QUALITY_PREFLIGHT_SAMPLE_SIZE = 4
DEFAULT_QUALITY_PREFLIGHT_EVIDENCE_MAX_AGE_SECONDS = 24 * 60 * 60
QUALITY_PREFLIGHT_WARN_FRACTION = 0.80
QUALITY_PREFLIGHT_LARGE_CACHE_TARGET_COUNT = 32


@dataclass(frozen=True, slots=True)
class _CacheTarget:
    """One discovered cache target with filesystem metadata."""

    target: QualityTarget
    path: Path
    size_bytes: int


def _utc_now() -> datetime:
    """Return the current UTC timestamp for evidence metadata."""
    return datetime.now(timezone.utc)


def run_cache_quality_preflight(
    root: str | Path,
    *,
    pairs: Iterable[object] | None = None,
    pair_groups: Iterable[object] | None = None,
    formats: Iterable[object] | None = None,
    timeframes: Iterable[object] | None = None,
    quality_check_groups: Iterable[str] | None = None,
    quality_profile: Mapping[str, Any] | None = None,
    sample_size: int = DEFAULT_QUALITY_PREFLIGHT_SAMPLE_SIZE,
    activity_budget_seconds: int | None = None,
    clock: Callable[[], float] = perf_counter,
    utc_now: Callable[[], datetime] = _utc_now,
) -> dict[str, JSONValue]:
    """Benchmark a bounded cache sample and estimate full quality runtime."""
    if sample_size < 1:
        raise ValueError("quality preflight sample size must be positive")

    root_path = Path(root).expanduser()
    check_groups = normalize_quality_check_groups(quality_check_groups)
    selected_groups = _normalize_groups(pair_groups)
    selected_pairs = _selected_pairs(pairs, selected_groups)
    selected_formats = _normalize_formats(formats)
    selected_timeframes = _normalize_timeframes(timeframes)
    budget_seconds = (
        activity_budget_seconds
        if activity_budget_seconds is not None
        else activity_execution_policy(
            "data_quality"
        ).start_to_close_timeout_seconds
    )
    discovery = discover_quality_targets((root_path,))
    all_cache_targets = tuple(
        target
        for target in discovery.targets
        if target.kind is QualityTargetKind.CACHE
    )
    cache_targets = _filtered_cache_targets(
        discovery.targets,
        pairs=selected_pairs,
        formats=selected_formats,
        timeframes=selected_timeframes,
    )
    samples = _select_sample(cache_targets, sample_size)
    benchmark = _benchmark_samples(
        samples,
        check_groups=check_groups,
        quality_profile=quality_profile,
        clock=clock,
    )
    return _payload(
        root_path=root_path,
        check_groups=check_groups,
        selected_groups=selected_groups,
        selected_pairs=selected_pairs,
        selected_formats=selected_formats,
        selected_timeframes=selected_timeframes,
        cache_targets=cache_targets,
        samples=samples,
        sample_size=sample_size,
        generated_at_utc=_utc_timestamp(utc_now()),
        benchmark=benchmark,
        budget_seconds=budget_seconds,
        quality_profile=quality_profile,
        all_cache_targets=all_cache_targets,
    )


def quality_preflight_to_json(payload: Mapping[str, JSONValue]) -> str:
    """Return deterministic JSON for a quality preflight payload."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def write_quality_preflight_report(
    payload: Mapping[str, JSONValue],
    path: str | Path,
) -> Path:
    """Write a publish-safe quality preflight report."""
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(quality_preflight_to_json(payload), encoding="utf-8")
    return output.resolve(strict=False)


def inspect_quality_preflight_evidence(
    root: str | Path,
    evidence_path: str | Path,
    *,
    pairs: Iterable[object] | None = None,
    pair_groups: Iterable[object] | None = None,
    formats: Iterable[object] | None = None,
    timeframes: Iterable[object] | None = None,
    quality_check_groups: Iterable[str] | None = None,
    evidence_max_age_seconds: int = (
        DEFAULT_QUALITY_PREFLIGHT_EVIDENCE_MAX_AGE_SECONDS
    ),
    allow_stale_evidence: bool = False,
    activity_budget_seconds: int | None = None,
    utc_now: Callable[[], datetime] = _utc_now,
) -> dict[str, JSONValue]:
    """Inspect saved preflight evidence against the current target scope."""
    root_path = Path(root).expanduser()
    check_groups = normalize_quality_check_groups(quality_check_groups)
    selected_groups = _normalize_groups(pair_groups)
    selected_pairs = _selected_pairs(pairs, selected_groups)
    selected_formats = _normalize_formats(formats)
    selected_timeframes = _normalize_timeframes(timeframes)
    budget_seconds = (
        activity_budget_seconds
        if activity_budget_seconds is not None
        else activity_execution_policy(
            "data_quality"
        ).start_to_close_timeout_seconds
    )
    discovery = discover_quality_targets((root_path,))
    cache_targets = _filtered_cache_targets(
        discovery.targets,
        pairs=selected_pairs,
        formats=selected_formats,
        timeframes=selected_timeframes,
    )
    cache_inventory = _cache_inventory_payload(cache_targets)
    evidence = _quality_preflight_evidence_state(
        evidence_path,
        root_path=root_path,
        check_groups=check_groups,
        selected_groups=selected_groups,
        selected_pairs=selected_pairs,
        selected_formats=selected_formats,
        selected_timeframes=selected_timeframes,
        expected_target_count=len(cache_targets),
        expected_cache_byte_count=sum(
            target.size_bytes for target in cache_targets
        ),
        expected_cache_inventory=cache_inventory,
        activity_budget_seconds=budget_seconds,
        max_age_seconds=evidence_max_age_seconds,
        allow_stale=allow_stale_evidence,
        now_utc=utc_now(),
    )
    status = _inspection_status(evidence)
    accepted = status == "accepted"
    commands: dict[str, JSONValue] = {
        "quality": _quality_command(
            root=root_path,
            check_groups=check_groups,
            selected_groups=selected_groups,
            selected_pairs=selected_pairs,
            selected_formats=selected_formats,
            selected_timeframes=selected_timeframes,
        ),
        "preflight": _quality_preflight_command(
            root=root_path,
            check_groups=check_groups,
            selected_groups=selected_groups,
            selected_pairs=selected_pairs,
            selected_formats=selected_formats,
            selected_timeframes=selected_timeframes,
        ),
    }
    payload: dict[str, JSONValue] = {
        "schema_version": QUALITY_PREFLIGHT_INSPECTION_SCHEMA_VERSION,
        "operation": "quality-preflight-evidence-inspection",
        "status": status,
        "accepted": accepted,
        "reason": str(evidence.get("reason", "")),
        "root": str(publish_safe_path(str(root_path.resolve(strict=False)))),
        "evidence_path": str(
            publish_safe_path(str(Path(evidence_path).expanduser()))
        ),
        "filters": {
            "checks": list(check_groups),
            "pairs": list(selected_pairs),
            "pair_groups": list(selected_groups),
            "formats": list(selected_formats),
            "timeframes": list(selected_timeframes),
        },
        "target_count": len(cache_targets),
        "cache_byte_count": sum(target.size_bytes for target in cache_targets),
        "cache_inventory": cache_inventory,
        "policy": {
            "activity": "data_quality",
            "activity_budget_seconds": budget_seconds,
            "evidence_max_age_seconds": max(
                int(evidence_max_age_seconds),
                0,
            ),
            "allow_stale_evidence": allow_stale_evidence,
        },
        "evidence": cast(JSONValue, evidence),
        "commands": commands,
        "action": (
            "run full quality battery"
            if accepted
            else "rerun quality preflight for this target scope"
        ),
    }
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload


def load_quality_preflight_evidence(
    path: str | Path,
) -> dict[str, JSONValue]:
    """Load a publish-safe quality preflight evidence report."""
    source = Path(path).expanduser()
    with source.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, Mapping):
        raise QualityDiscoveryError(
            "quality preflight evidence must be a JSON object"
        )
    payload = cast(dict[str, JSONValue], dict(data))
    if payload.get("schema_version") != QUALITY_PREFLIGHT_SCHEMA_VERSION:
        raise QualityDiscoveryError(
            "quality preflight evidence schema is not supported"
        )
    return payload


def quality_run_preflight_warning(
    roots: Iterable[str | Path],
    *,
    pairs: Iterable[object] | None = None,
    pair_groups: Iterable[object] | None = None,
    formats: Iterable[object] | None = None,
    timeframes: Iterable[object] | None = None,
    quality_check_groups: Iterable[str] | None = None,
    evidence_path: str | Path | None = None,
    evidence_max_age_seconds: int = (
        DEFAULT_QUALITY_PREFLIGHT_EVIDENCE_MAX_AGE_SECONDS
    ),
    allow_stale_evidence: bool = False,
    activity_budget_seconds: int | None = None,
    utc_now: Callable[[], datetime] = _utc_now,
    large_target_count: int = QUALITY_PREFLIGHT_LARGE_CACHE_TARGET_COUNT,
) -> dict[str, JSONValue] | None:
    """Return a non-blocking warning for large cache quality runs."""
    root_paths = tuple(Path(root).expanduser() for root in roots)
    if not root_paths:
        return None
    check_groups = normalize_quality_check_groups(quality_check_groups)
    selected_groups = _normalize_groups(pair_groups)
    selected_pairs = _selected_pairs(pairs, selected_groups)
    selected_formats = _normalize_formats(formats)
    selected_timeframes = _normalize_timeframes(timeframes)
    budget_seconds = (
        activity_budget_seconds
        if activity_budget_seconds is not None
        else activity_execution_policy(
            "data_quality"
        ).start_to_close_timeout_seconds
    )
    try:
        discovery = discover_quality_targets(root_paths)
    except QualityDiscoveryError:
        return None
    cache_targets = _filtered_cache_targets(
        discovery.targets,
        pairs=selected_pairs,
        formats=selected_formats,
        timeframes=selected_timeframes,
    )
    if evidence_path and len(root_paths) != 1:
        evidence: dict[str, JSONValue] = {
            "status": "mismatch",
            "reason": "multiple quality roots cannot match one preflight report",
        }
    else:
        evidence = _quality_preflight_evidence_state(
            evidence_path,
            root_path=root_paths[0],
            check_groups=check_groups,
            selected_groups=selected_groups,
            selected_pairs=selected_pairs,
            selected_formats=selected_formats,
            selected_timeframes=selected_timeframes,
            expected_target_count=len(cache_targets),
            expected_cache_byte_count=sum(
                target.size_bytes for target in cache_targets
            ),
            expected_cache_inventory=_cache_inventory_payload(cache_targets),
            activity_budget_seconds=budget_seconds,
            max_age_seconds=evidence_max_age_seconds,
            allow_stale=allow_stale_evidence,
            now_utc=utc_now(),
        )
    if (
        evidence.get("status") == "matched"
        or len(cache_targets) < large_target_count
    ):
        return None

    root_payload: JSONValue
    if len(root_paths) == 1:
        root_payload = str(
            publish_safe_path(str(root_paths[0].resolve(strict=False)))
        )
    else:
        root_payload = [
            str(publish_safe_path(str(root.resolve(strict=False))))
            for root in root_paths
        ]
    preflight_command = _quality_preflight_command(
        root=root_paths[0],
        check_groups=check_groups,
        selected_groups=selected_groups,
        selected_pairs=selected_pairs,
        selected_formats=selected_formats,
        selected_timeframes=selected_timeframes,
    )
    warning: dict[str, JSONValue] = {
        "status": "warn",
        "reason": "large cache-backed quality run has no matching preflight evidence",
        "root": root_payload,
        "target_count": len(cache_targets),
        "large_target_count": large_target_count,
        "evidence": cast(JSONValue, evidence),
        "suggested_preflight_command": preflight_command,
    }
    safe_warning: dict[str, JSONValue] = publish_safe_json_mapping(warning)
    return safe_warning


def format_quality_run_preflight_warning(
    warning: Mapping[str, JSONValue],
) -> str:
    """Return a compact warning before large cache quality runs."""
    evidence = _mapping(warning.get("evidence"))
    lines = [
        "Data quality preflight warning",
        f"status: {warning.get('status', 'warn')}",
        f"reason: {warning.get('reason', '')}",
        (
            "targets: "
            f"{warning.get('target_count', 0)} canonical .data caches "
            f"(large-run threshold "
            f"{warning.get('large_target_count', 0)})"
        ),
    ]
    if evidence.get("status") and evidence.get("status") != "not-provided":
        lines.append(
            "evidence: "
            f"{evidence.get('status')} "
            f"({evidence.get('reason', 'no detail')})"
        )
    command = warning.get("suggested_preflight_command")
    if command:
        lines.append(f"preflight first: {command}")
    lines.append("continuing without prompting")
    return "\n".join(lines)


def format_quality_preflight_evidence_inspection(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return a compact human-readable evidence inspection report."""
    filters = _mapping(payload.get("filters"))
    commands = _mapping(payload.get("commands"))
    evidence = _mapping(payload.get("evidence"))
    lines = [
        "Quality preflight evidence inspection",
        f"status: {payload.get('status', 'unknown')}",
        f"accepted: {'yes' if payload.get('accepted') else 'no'}",
        f"reason: {payload.get('reason', '')}",
        f"evidence: {payload.get('evidence_path', '')}",
        f"root: {payload.get('root', '')}",
        (
            "checks: "
            + ", ".join(str(item) for item in filters.get("checks", []))
        ),
        (
            "targets: "
            f"{payload.get('target_count', 0)} cache files, "
            f"{_format_bytes(_int_value(payload.get('cache_byte_count')))}"
        ),
    ]
    if evidence.get("mismatch_kind"):
        lines.append(f"mismatch: {evidence['mismatch_kind']}")
    if payload.get("accepted"):
        lines.append(f"next: {commands.get('quality', '')}")
    else:
        lines.append(f"next: {commands.get('preflight', '')}")
    lines.append("continuing without prompting")
    return "\n".join(lines)


def format_quality_preflight_console_summary(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return a compact operator summary for cache quality preflight."""
    filters = _mapping(payload.get("filters"))
    sample = _mapping(payload.get("sample"))
    benchmark = _mapping(payload.get("benchmark"))
    estimate = _mapping(payload.get("estimate"))
    budget = _mapping(payload.get("temporal_budget"))
    quality = _mapping(payload.get("sample_quality"))
    quality_summary = _mapping(quality.get("summary"))
    decision = _mapping(payload.get("decision"))
    diagnostics = _mapping(payload.get("diagnostics"))
    lines = [
        "Data quality cache preflight",
        "status: " + str(payload.get("status", "unknown")),
        "decision: " + str(decision.get("label", "unknown")),
        "checks: " + ", ".join(str(item) for item in filters.get("checks", [])),
        (
            "targets: "
            f"{payload.get('target_count', 0)} cache files, "
            f"{_format_bytes(_int_value(payload.get('cache_byte_count')))}"
        ),
        (
            "sample: "
            f"{sample.get('selected_count', 0)}/"
            f"{sample.get('requested_count', 0)} "
            f"{sample.get('strategy', 'unknown')}"
        ),
        (
            "elapsed: "
            f"{float(benchmark.get('elapsed_seconds', 0.0) or 0.0):.3f}s; "
            f"rows/sec: "
            f"{float(benchmark.get('rows_per_second', 0.0) or 0.0):.1f}; "
            f"bytes/sec: "
            f"{float(benchmark.get('bytes_per_second', 0.0) or 0.0):.1f}"
        ),
        (
            "eta: "
            f"{_format_duration(estimate.get('estimated_seconds_min'))} to "
            f"{_format_duration(estimate.get('estimated_seconds_max'))}"
        ),
        (
            "Temporal budget: "
            f"{_format_duration(budget.get('activity_budget_seconds'))} "
            f"({budget.get('status', 'unknown')})"
        ),
    ]
    if quality_summary:
        lines.append(
            "sample quality: "
            f"{quality_summary.get('status', 'unknown')} "
            f"findings={quality_summary.get('finding_count', 0)} "
            f"warnings={quality_summary.get('warning_count', 0)} "
            f"errors={quality_summary.get('error_count', 0)}"
        )
    if decision.get("reason"):
        lines.append(f"reason: {decision['reason']}")
    if decision.get("next_command"):
        lines.append(f"next: {decision['next_command']}")
    if payload.get("report_path"):
        lines.append(f"report: {payload['report_path']}")
    if _int_value(payload.get("target_count")) == 0:
        lines.append("No .data cache targets matched the requested scope.")
        lines.extend(_format_no_target_diagnostics(diagnostics))
    return "\n".join(lines)


def _filtered_cache_targets(
    targets: Iterable[QualityTarget],
    *,
    pairs: tuple[str, ...],
    formats: tuple[str, ...],
    timeframes: tuple[str, ...],
) -> tuple[_CacheTarget, ...]:
    pair_filter = set(pairs)
    format_filter = set(formats)
    timeframe_filter = set(timeframes)
    selected: list[_CacheTarget] = []
    for target in targets:
        if target.kind is not QualityTargetKind.CACHE:
            continue
        if pair_filter and target.symbol.lower() not in pair_filter:
            continue
        if format_filter and target.data_format.lower() not in format_filter:
            continue
        if (
            timeframe_filter
            and target.timeframe.upper() not in timeframe_filter
        ):
            continue
        path = Path(target.path)
        selected.append(
            _CacheTarget(
                target=target,
                path=path,
                size_bytes=_file_size(path),
            )
        )
    return tuple(sorted(selected, key=lambda item: (item.path.as_posix())))


def _select_sample(
    targets: tuple[_CacheTarget, ...],
    sample_size: int,
) -> tuple[_CacheTarget, ...]:
    if len(targets) <= sample_size:
        return tuple(sorted(targets, key=lambda item: item.path.as_posix()))
    ordered = sorted(
        targets, key=lambda item: (item.size_bytes, item.path.as_posix())
    )
    positions = _sample_positions(len(ordered), sample_size)
    return tuple(ordered[position] for position in positions)


def _sample_positions(total_count: int, sample_size: int) -> tuple[int, ...]:
    if total_count <= 0:
        return ()
    if sample_size <= 1:
        return (total_count - 1,)
    return tuple(
        dict.fromkeys(
            round(index * (total_count - 1) / (sample_size - 1))
            for index in range(sample_size)
        )
    )


def _benchmark_samples(
    samples: tuple[_CacheTarget, ...],
    *,
    check_groups: tuple[str, ...],
    quality_profile: Mapping[str, Any] | None,
    clock: Callable[[], float],
) -> dict[str, JSONValue]:
    started = clock()
    sample_payloads = [_sample_payload(sample) for sample in samples]
    report = run_quality_assessment(
        [sample.target for sample in samples],
        quality_rules_for_groups(check_groups, profile=quality_profile),
        run_rules=quality_run_rules_for_groups(
            check_groups,
            profile=quality_profile,
        ),
        metadata={
            "operation": "data-quality-preflight",
            **quality_profile_report_metadata(quality_profile),
        },
    )
    elapsed = max(clock() - started, 0.0)
    sample_bytes = sum(sample.size_bytes for sample in samples)
    sample_rows = sum(
        _int_value(item.get("row_count")) for item in sample_payloads
    )
    return {
        "elapsed_seconds": round(elapsed, 6),
        "sample_cache_bytes": sample_bytes,
        "sample_row_count": sample_rows,
        "rows_per_second": _rate(sample_rows, elapsed),
        "bytes_per_second": _rate(sample_bytes, elapsed),
        "targets": cast(JSONValue, sample_payloads),
        "quality_report": _sample_quality_payload(report),
    }


def _sample_payload(sample: _CacheTarget) -> dict[str, JSONValue]:
    row_count = 0
    error = ""
    cache = read_quality_polars_cache(
        sample.target,
        required_columns=(),
    )
    if cache is None:
        error = "cache could not be read"
    else:
        row_count = int(getattr(cache.frame, "height", 0) or 0)
    payload: dict[str, JSONValue] = {
        "path": str(publish_safe_path(str(sample.path))),
        "size_bytes": sample.size_bytes,
        "row_count": row_count,
        "symbol": sample.target.symbol.lower(),
        "timeframe": sample.target.timeframe,
        "data_format": sample.target.data_format,
        "period": sample.target.period,
    }
    if error:
        payload["error"] = error
    return payload


def _sample_quality_payload(report: QualityReport) -> dict[str, JSONValue]:
    summaries = report.target_summaries
    payload: dict[str, JSONValue] = publish_safe_json_mapping(
        {
            "summary": report.summary().to_dict(),
            "target_status_counts": {
                QualityStatus.CLEAN.value: sum(
                    1
                    for item in summaries
                    if item.status is QualityStatus.CLEAN
                ),
                QualityStatus.WARNING.value: sum(
                    1
                    for item in summaries
                    if item.status is QualityStatus.WARNING
                ),
                QualityStatus.FAILED.value: sum(
                    1
                    for item in summaries
                    if item.status is QualityStatus.FAILED
                ),
            },
        }
    )
    return payload


def _cache_inventory_payload(
    targets: Iterable[_CacheTarget],
) -> dict[str, JSONValue]:
    rows: list[str] = []
    target_items = tuple(targets)
    for item in target_items:
        rows.append(
            "\t".join(
                (
                    item.target.symbol.lower(),
                    item.target.data_format.lower(),
                    item.target.timeframe.upper(),
                    item.target.period,
                    str(item.size_bytes),
                    str(publish_safe_path(str(item.path))),
                )
            )
        )
    material = "\n".join(sorted(rows)).encode("utf-8")
    return {
        "target_count": len(target_items),
        "cache_byte_count": sum(item.size_bytes for item in target_items),
        "fingerprint_algorithm": "sha256",
        "fingerprint": hashlib.sha256(material).hexdigest(),
    }


def _payload(
    *,
    root_path: Path,
    check_groups: tuple[str, ...],
    selected_groups: tuple[str, ...],
    selected_pairs: tuple[str, ...],
    selected_formats: tuple[str, ...],
    selected_timeframes: tuple[str, ...],
    cache_targets: tuple[_CacheTarget, ...],
    samples: tuple[_CacheTarget, ...],
    sample_size: int,
    generated_at_utc: str,
    benchmark: Mapping[str, JSONValue],
    budget_seconds: int,
    quality_profile: Mapping[str, Any] | None,
    all_cache_targets: tuple[QualityTarget, ...],
) -> dict[str, JSONValue]:
    target_bytes = sum(item.size_bytes for item in cache_targets)
    sample_bytes = _int_value(benchmark.get("sample_cache_bytes"))
    sample_rows = _int_value(benchmark.get("sample_row_count"))
    estimated_total_rows = _estimated_total_rows(
        target_bytes=target_bytes,
        sample_bytes=sample_bytes,
        sample_rows=sample_rows,
    )
    estimate = _estimate(
        target_bytes=target_bytes,
        sample_bytes=sample_bytes,
        estimated_total_rows=estimated_total_rows,
        rows_per_second=_float_value(benchmark.get("rows_per_second")),
        bytes_per_second=_float_value(benchmark.get("bytes_per_second")),
    )
    budget = _budget_payload(estimate, budget_seconds)
    diagnostics = _diagnostics_payload(
        root_path=root_path,
        all_cache_targets=all_cache_targets,
        check_groups=check_groups,
        selected_groups=selected_groups,
        selected_pairs=selected_pairs,
        selected_formats=selected_formats,
        selected_timeframes=selected_timeframes,
        matched_cache_count=len(cache_targets),
    )
    payload: dict[str, JSONValue] = {
        "schema_version": QUALITY_PREFLIGHT_SCHEMA_VERSION,
        "operation": "data-quality-cache-preflight",
        "generated_at_utc": generated_at_utc,
        "package": {
            "name": "histdatacom",
            "version": HISTDATACOM_VERSION,
        },
        "status": budget["status"],
        "root": str(publish_safe_path(str(root_path.resolve(strict=False)))),
        "filters": {
            "checks": list(check_groups),
            "pairs": list(selected_pairs),
            "pair_groups": list(selected_groups),
            "formats": list(selected_formats),
            "timeframes": list(selected_timeframes),
        },
        "target_count": len(cache_targets),
        "cache_byte_count": target_bytes,
        "cache_inventory": _cache_inventory_payload(cache_targets),
        "estimated_row_count": estimated_total_rows,
        "sample": {
            "strategy": "size-quantiles",
            "requested_count": sample_size,
            "selected_count": len(samples),
            "selection_positions": list(
                _sample_positions(len(cache_targets), sample_size)
            ),
            "targets": _list_value(benchmark.get("targets")),
        },
        "benchmark": {
            "elapsed_seconds": benchmark["elapsed_seconds"],
            "sample_cache_bytes": sample_bytes,
            "sample_row_count": sample_rows,
            "rows_per_second": benchmark["rows_per_second"],
            "bytes_per_second": benchmark["bytes_per_second"],
        },
        "estimate": estimate,
        "temporal_budget": budget,
        "preflight_policy": _preflight_policy_payload(
            sample_size=sample_size,
            budget_seconds=budget_seconds,
        ),
        "sample_quality": benchmark["quality_report"],
        "quality_profile": quality_profile_report_metadata(quality_profile)[
            "quality_profile"
        ],
        "diagnostics": diagnostics,
    }
    if not cache_targets:
        payload["status"] = "fail"
        payload["temporal_budget"] = {
            **budget,
            "status": "fail",
            "reason": "no cache targets matched the requested scope",
        }
    payload["decision"] = _decision_payload(
        payload,
        root_path=root_path,
        check_groups=check_groups,
        selected_groups=selected_groups,
        selected_pairs=selected_pairs,
        selected_formats=selected_formats,
        selected_timeframes=selected_timeframes,
    )
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload


def _diagnostics_payload(
    *,
    root_path: Path,
    all_cache_targets: tuple[QualityTarget, ...],
    check_groups: tuple[str, ...],
    selected_groups: tuple[str, ...],
    selected_pairs: tuple[str, ...],
    selected_formats: tuple[str, ...],
    selected_timeframes: tuple[str, ...],
    matched_cache_count: int,
) -> dict[str, JSONValue]:
    dimensions = _cache_dimensions(all_cache_targets)
    return {
        "target_root": str(
            publish_safe_path(str(root_path.resolve(strict=False)))
        ),
        "requested_filters": {
            "checks": list(check_groups),
            "pair_groups": list(selected_groups),
            "pairs": list(selected_pairs),
            "formats": list(selected_formats),
            "timeframes": list(selected_timeframes),
        },
        "discovered_cache_dimensions": {
            **dimensions,
            "matching_cache_count": matched_cache_count,
        },
    }


def _cache_dimensions(
    targets: Iterable[QualityTarget],
) -> dict[str, JSONValue]:
    target_items = tuple(targets)
    pairs = sorted(
        {
            target.symbol.lower()
            for target in target_items
            if target.symbol.strip()
        }
    )
    formats = sorted(
        {
            target.data_format.lower()
            for target in target_items
            if target.data_format.strip()
        }
    )
    timeframes = sorted(
        {
            target.timeframe.upper()
            for target in target_items
            if target.timeframe.strip()
        }
    )
    pair_set = set(pairs)
    groups = sorted(
        group
        for group, group_pairs in PAIR_GROUPS.items()
        if pair_set.intersection(group_pairs)
    )
    return cast(
        dict[str, JSONValue],
        {
            "canonical_data_cache_count": len(target_items),
            "pair_groups": groups,
            "pairs": pairs,
            "formats": formats,
            "timeframes": timeframes,
        },
    )


def _decision_payload(
    payload: Mapping[str, JSONValue],
    *,
    root_path: Path,
    check_groups: tuple[str, ...],
    selected_groups: tuple[str, ...],
    selected_pairs: tuple[str, ...],
    selected_formats: tuple[str, ...],
    selected_timeframes: tuple[str, ...],
) -> dict[str, JSONValue]:
    target_count = _int_value(payload.get("target_count"))
    budget = _mapping(payload.get("temporal_budget"))
    status = str(budget.get("status", payload.get("status", "unknown")))
    reason = str(budget.get("reason", "") or "")
    if target_count == 0:
        return {
            "state": "no-targets",
            "label": "no matching targets",
            "action": "adjust target scope",
            "reason": reason or "no cache targets matched the requested scope",
            "next_command": "",
        }
    if status == "pass":
        return {
            "state": "safe",
            "label": "safe to run full quality battery",
            "action": "run full quality battery",
            "reason": reason,
            "next_command": _quality_command(
                root=root_path,
                check_groups=check_groups,
                selected_groups=selected_groups,
                selected_pairs=selected_pairs,
                selected_formats=selected_formats,
                selected_timeframes=selected_timeframes,
            ),
        }
    if status == "warn":
        return {
            "state": "warn",
            "label": "warning; rerun recommended before full battery",
            "action": "review estimate or rerun preflight with larger sample",
            "reason": reason,
            "next_command": _quality_command(
                root=root_path,
                check_groups=check_groups,
                selected_groups=selected_groups,
                selected_pairs=selected_pairs,
                selected_formats=selected_formats,
                selected_timeframes=selected_timeframes,
            ),
        }
    return {
        "state": "fail",
        "label": "do not run full quality battery",
        "action": "reduce scope or adjust runtime budget",
        "reason": reason,
        "next_command": "",
    }


def _quality_preflight_evidence_state(
    evidence_path: str | Path | None,
    *,
    root_path: Path,
    check_groups: tuple[str, ...],
    selected_groups: tuple[str, ...],
    selected_pairs: tuple[str, ...],
    selected_formats: tuple[str, ...],
    selected_timeframes: tuple[str, ...],
    expected_target_count: int,
    expected_cache_byte_count: int,
    expected_cache_inventory: Mapping[str, JSONValue],
    activity_budget_seconds: int,
    max_age_seconds: int,
    allow_stale: bool,
    now_utc: datetime,
) -> dict[str, JSONValue]:
    if not evidence_path:
        return {"status": "not-provided", "reason": "no evidence path supplied"}
    try:
        payload = load_quality_preflight_evidence(evidence_path)
    except (OSError, json.JSONDecodeError, QualityDiscoveryError) as exc:
        return {"status": "unavailable", "reason": str(exc)}

    expected_root = str(publish_safe_path(str(root_path.resolve(strict=False))))
    if payload.get("root") != expected_root:
        return {
            "status": "mismatch",
            "mismatch_kind": "root",
            "reason": "target root differs",
        }
    filters = _mapping(payload.get("filters"))
    expected_filters = {
        "checks": list(check_groups),
        "pairs": list(selected_pairs),
        "pair_groups": list(selected_groups),
        "formats": list(selected_formats),
        "timeframes": list(selected_timeframes),
    }
    for key, expected in expected_filters.items():
        observed = filters.get(key)
        if observed != expected:
            return {
                "status": "mismatch",
                "mismatch_kind": "filter",
                "filter": key,
                "reason": f"{key} filter differs",
            }
    decision = _mapping(payload.get("decision"))
    if decision.get("state") not in {"safe", "warn"}:
        return {
            "status": "not-actionable",
            "reason": "evidence decision is not safe or warn",
        }
    package = _mapping(payload.get("package"))
    if package.get("version") != HISTDATACOM_VERSION:
        return {
            "status": "version-mismatch",
            "reason": "package version differs",
            "expected_version": HISTDATACOM_VERSION,
            "observed_version": str(package.get("version", "") or ""),
        }
    if not allow_stale:
        freshness = _freshness_mismatch(
            payload,
            max_age_seconds=max_age_seconds,
            now_utc=now_utc,
        )
        if freshness is not None:
            return freshness
    policy_reason = _policy_mismatch_reason(
        payload,
        activity_budget_seconds=activity_budget_seconds,
    )
    if policy_reason:
        return {"status": "policy-mismatch", "reason": policy_reason}
    if _int_value(payload.get("target_count")) != expected_target_count:
        return {
            "status": "mismatch",
            "mismatch_kind": "cache-count",
            "reason": "cache target count differs",
        }
    if _int_value(payload.get("cache_byte_count")) != expected_cache_byte_count:
        return {
            "status": "mismatch",
            "mismatch_kind": "cache-byte",
            "reason": "cache inventory bytes differ",
        }
    observed_inventory = _mapping(payload.get("cache_inventory"))
    if observed_inventory.get("fingerprint") != expected_cache_inventory.get(
        "fingerprint"
    ):
        return {
            "status": "mismatch",
            "mismatch_kind": "cache-fingerprint",
            "reason": "cache inventory fingerprint differs",
        }
    return {"status": "matched", "reason": "evidence matches target scope"}


def _inspection_status(evidence: Mapping[str, JSONValue]) -> str:
    status = str(evidence.get("status", "unknown"))
    if status == "matched":
        return "accepted"
    if status == "stale":
        return "stale"
    if status in {"version-mismatch", "policy-mismatch"}:
        return status
    if status == "mismatch":
        kind = str(evidence.get("mismatch_kind", "") or "")
        if kind:
            return f"{kind}-mismatch"
    return status


def _quality_command(
    *,
    root: Path,
    check_groups: tuple[str, ...],
    selected_groups: tuple[str, ...],
    selected_pairs: tuple[str, ...],
    selected_formats: tuple[str, ...],
    selected_timeframes: tuple[str, ...],
) -> str:
    return _command(
        "histdatacom",
        "--quality",
        "--quality-target",
        str(publish_safe_path(str(root.resolve(strict=False)))),
        *_selector_args(
            check_groups=check_groups,
            selected_groups=selected_groups,
            selected_pairs=selected_pairs,
            selected_formats=selected_formats,
            selected_timeframes=selected_timeframes,
        ),
    )


def _quality_preflight_command(
    *,
    root: Path,
    check_groups: tuple[str, ...],
    selected_groups: tuple[str, ...],
    selected_pairs: tuple[str, ...],
    selected_formats: tuple[str, ...],
    selected_timeframes: tuple[str, ...],
) -> str:
    return _command(
        "histdatacom",
        "--quality-preflight",
        "--quality-target",
        str(publish_safe_path(str(root.resolve(strict=False)))),
        *_selector_args(
            check_groups=check_groups,
            selected_groups=selected_groups,
            selected_pairs=selected_pairs,
            selected_formats=selected_formats,
            selected_timeframes=selected_timeframes,
        ),
    )


def _selector_args(
    *,
    check_groups: tuple[str, ...],
    selected_groups: tuple[str, ...],
    selected_pairs: tuple[str, ...],
    selected_formats: tuple[str, ...],
    selected_timeframes: tuple[str, ...],
) -> tuple[str, ...]:
    args: list[str] = []
    if check_groups and check_groups != ("all",):
        args.extend(["--quality-checks", *check_groups])
    if selected_groups:
        args.extend(["--pair-groups", *selected_groups])
    elif selected_pairs:
        args.extend(["-p", *selected_pairs])
    if selected_formats:
        args.extend(["-f", *selected_formats])
    if selected_timeframes:
        args.extend(
            [
                "-t",
                *(_timeframe_cli_value(item) for item in selected_timeframes),
            ]
        )
    return tuple(args)


def _timeframe_cli_value(value: str) -> str:
    key = value.upper()
    if key in Timeframe.__members__:
        return str(Timeframe[key].value)
    return value


def _command(*parts: str) -> str:
    return " ".join(shlex.quote(part) for part in parts if part)


def _format_no_target_diagnostics(
    diagnostics: Mapping[str, JSONValue],
) -> list[str]:
    requested = _mapping(diagnostics.get("requested_filters"))
    dimensions = _mapping(diagnostics.get("discovered_cache_dimensions"))
    if not requested and not dimensions:
        return []
    return [
        "requested filters: "
        f"groups={_join_or_all(requested.get('pair_groups'))}; "
        f"pairs={_join_or_all(requested.get('pairs'))}; "
        f"formats={_join_or_all(requested.get('formats'))}; "
        f"timeframes={_join_or_all(requested.get('timeframes'))}",
        "discovered caches: "
        f"{dimensions.get('canonical_data_cache_count', 0)} canonical .data; "
        f"groups={_join_or_none(dimensions.get('pair_groups'))}; "
        f"pairs={_join_or_none(dimensions.get('pairs'))}; "
        f"formats={_join_or_none(dimensions.get('formats'))}; "
        f"timeframes={_join_or_none(dimensions.get('timeframes'))}",
    ]


def _estimate(
    *,
    target_bytes: int,
    sample_bytes: int,
    estimated_total_rows: int,
    rows_per_second: float,
    bytes_per_second: float,
) -> dict[str, JSONValue]:
    by_rows = (
        estimated_total_rows / rows_per_second if rows_per_second > 0 else None
    )
    by_bytes = target_bytes / bytes_per_second if bytes_per_second > 0 else None
    values = [value for value in (by_rows, by_bytes) if value is not None]
    if not values:
        min_seconds: float | None = None
        max_seconds: float | None = None
    else:
        min_seconds = min(values)
        max_seconds = max(values)
    return {
        "basis": "sample-throughput",
        "sample_coverage_fraction": (
            round(sample_bytes / target_bytes, 6) if target_bytes else 0.0
        ),
        "estimated_seconds_by_rows": _rounded_optional(by_rows),
        "estimated_seconds_by_bytes": _rounded_optional(by_bytes),
        "estimated_seconds_min": _rounded_optional(min_seconds),
        "estimated_seconds_max": _rounded_optional(max_seconds),
    }


def _budget_payload(
    estimate: Mapping[str, JSONValue],
    budget_seconds: int,
) -> dict[str, JSONValue]:
    max_seconds = estimate.get("estimated_seconds_max")
    if not isinstance(max_seconds, (int, float)):
        status = "fail"
        reason = "estimate unavailable"
    elif max_seconds > budget_seconds:
        status = "fail"
        reason = "estimated runtime exceeds Temporal data-quality budget"
    elif max_seconds > budget_seconds * QUALITY_PREFLIGHT_WARN_FRACTION:
        status = "warn"
        reason = "estimated runtime is close to Temporal data-quality budget"
    else:
        status = "pass"
        reason = "estimated runtime is within Temporal data-quality budget"
    return {
        "activity": "data_quality",
        "activity_budget_seconds": budget_seconds,
        "warn_fraction": QUALITY_PREFLIGHT_WARN_FRACTION,
        "status": status,
        "reason": reason,
    }


def _preflight_policy_payload(
    *,
    sample_size: int,
    budget_seconds: int,
) -> dict[str, JSONValue]:
    return {
        "sample_size": sample_size,
        "temporal_budget": {
            "activity": "data_quality",
            "activity_budget_seconds": budget_seconds,
            "warn_fraction": QUALITY_PREFLIGHT_WARN_FRACTION,
        },
        "freshness": {
            "default_max_age_seconds": (
                DEFAULT_QUALITY_PREFLIGHT_EVIDENCE_MAX_AGE_SECONDS
            )
        },
    }


def _policy_mismatch_reason(
    payload: Mapping[str, JSONValue],
    *,
    activity_budget_seconds: int,
) -> str:
    policy = _mapping(payload.get("preflight_policy"))
    if not policy:
        return "preflight policy metadata is missing"
    sample = _mapping(payload.get("sample"))
    policy_sample_size = _int_value(policy.get("sample_size"))
    if policy_sample_size < 1:
        return "preflight sample size policy is invalid"
    if policy_sample_size != _int_value(sample.get("requested_count")):
        return "preflight sample size policy differs from report sample"
    budget = _mapping(policy.get("temporal_budget"))
    if budget.get("activity") != "data_quality":
        return "Temporal budget activity differs"
    if _int_value(budget.get("activity_budget_seconds")) != (
        activity_budget_seconds
    ):
        return "Temporal data_quality budget differs"
    if _float_value(budget.get("warn_fraction")) != (
        QUALITY_PREFLIGHT_WARN_FRACTION
    ):
        return "Temporal budget warning policy differs"
    return ""


def _freshness_mismatch(
    payload: Mapping[str, JSONValue],
    *,
    max_age_seconds: int,
    now_utc: datetime,
) -> dict[str, JSONValue] | None:
    generated_at = _parse_utc_timestamp(payload.get("generated_at_utc"))
    if generated_at is None:
        return {
            "status": "stale",
            "reason": "generated_at_utc is missing or invalid",
        }
    normalized_now = _normalize_utc(now_utc)
    age_seconds = (normalized_now - generated_at).total_seconds()
    if age_seconds < -60:
        return {
            "status": "stale",
            "reason": "generated_at_utc is in the future",
        }
    max_age = max(int(max_age_seconds), 0)
    if age_seconds > max_age:
        return {
            "status": "stale",
            "reason": f"evidence is older than {max_age} seconds",
            "age_seconds": round(age_seconds, 3),
            "max_age_seconds": max_age,
        }
    return None


def _utc_timestamp(value: datetime) -> str:
    return (
        _normalize_utc(value)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _parse_utc_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return _normalize_utc(parsed)


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _selected_pairs(
    pairs: Iterable[object] | None,
    groups: tuple[str, ...],
) -> tuple[str, ...]:
    if not pairs and not groups:
        return ()
    return tuple(
        str(pair).lower() for pair in expand_pair_selection(pairs or (), groups)
    )


def _normalize_groups(groups: Iterable[object] | None) -> tuple[str, ...]:
    return tuple(
        sorted({normalize_pair_group(group) for group in groups or ()})
    )


def _normalize_formats(values: Iterable[object] | None) -> tuple[str, ...]:
    normalized: set[str] = set()
    for value in values or ():
        text = str(value).strip()
        if not text:
            continue
        upper = text.upper()
        if upper in Format.__members__:
            normalized.add(Format[upper].value.lower())
            continue
        normalized.add(text.lower())
    return tuple(sorted(normalized))


def _normalize_timeframes(values: Iterable[object] | None) -> tuple[str, ...]:
    normalized: set[str] = set()
    for value in values or ():
        text = str(value).strip()
        if not text:
            continue
        upper = text.upper()
        if upper in Timeframe.__members__:
            normalized.add(upper)
            continue
        lower = text.lower()
        for member in Timeframe:
            if member.value.lower() == lower:
                normalized.add(member.name)
                break
        else:
            normalized.add(upper)
    return tuple(sorted(normalized))


def _estimated_total_rows(
    *,
    target_bytes: int,
    sample_bytes: int,
    sample_rows: int,
) -> int:
    if not target_bytes or not sample_bytes or not sample_rows:
        return 0
    return round(target_bytes * (sample_rows / sample_bytes))


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _rate(numerator: int, elapsed_seconds: float) -> float:
    if elapsed_seconds <= 0:
        return 0.0
    return round(numerator / elapsed_seconds, 6)


def _rounded_optional(value: float | None) -> float | None:
    return None if value is None else round(value, 6)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_value(value: object) -> list[JSONValue]:
    return list(value) if isinstance(value, list) else []


def _join_or_all(value: object) -> str:
    items = [str(item) for item in value] if isinstance(value, list) else []
    return ", ".join(items) if items else "all"


def _join_or_none(value: object) -> str:
    items = [str(item) for item in value] if isinstance(value, list) else []
    return ", ".join(items) if items else "none"


def _int_value(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return default
    return default


def _float_value(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _format_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{int(size)} {unit}"
    return f"{size:.2f} {unit}"


def _format_duration(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "unknown"
    seconds = float(value)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    if hours < 48:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


__all__ = [
    "DEFAULT_QUALITY_PREFLIGHT_SAMPLE_SIZE",
    "QUALITY_PREFLIGHT_LARGE_CACHE_TARGET_COUNT",
    "QUALITY_PREFLIGHT_INSPECTION_SCHEMA_VERSION",
    "QUALITY_PREFLIGHT_SCHEMA_VERSION",
    "QualityDiscoveryError",
    "format_quality_preflight_evidence_inspection",
    "format_quality_preflight_console_summary",
    "format_quality_run_preflight_warning",
    "inspect_quality_preflight_evidence",
    "load_quality_preflight_evidence",
    "quality_preflight_to_json",
    "quality_run_preflight_warning",
    "run_cache_quality_preflight",
    "write_quality_preflight_report",
]
