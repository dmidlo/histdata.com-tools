"""Sidecar provenance and manifest lineage checks."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import hashlib
from pathlib import Path
import sqlite3
from typing import Any

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualityRunRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.manifest_store import (
    MANIFEST_DB_FILENAME,
    MANIFEST_DIRECTORY,
    ManifestStatusStore,
    STATUS_STORE_REF_KEY,
    STATUS_STORE_REF_KIND,
)
from histdatacom.runtime_contracts import JSONValue, WorkItem, WorkStatus

PROVENANCE_MANIFEST_SCHEMA_VERSION = "histdatacom.provenance-manifest.v1"
PROVENANCE_MANIFEST_RULE_ID = "provenance.manifest.lineage"
PROVENANCE_METADATA_KEY = "provenance_manifest"

_DATA_ARTIFACT_KINDS = frozenset({"zip", "csv", "cache"})
_SOURCE_ARTIFACT_KINDS = frozenset({"zip", "csv"})
_DEFAULT_MAX_FINDINGS = 200
_SHA256_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True, slots=True)
class _StoreRef:
    root: Path
    db_path: Path
    source: str

    def to_metadata(self) -> dict[str, JSONValue]:
        return {
            "store_root": str(self.root),
            "store_path": str(self.db_path),
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class _FindingTemplate:
    severity: QualitySeverity
    code: str
    message: str
    location_path: str = ""
    metadata: Mapping[str, JSONValue] | None = None


@dataclass(slots=True)
class _StoreEvaluation:
    store: _StoreRef
    work_item_count: int = 0
    artifact_count: int = 0
    data_artifact_count: int = 0
    target_count: int = 0
    job_snapshot_count: int = 0
    findings: list[_FindingTemplate] | None = None

    @property
    def finding_count(self) -> int:
        return len(self.findings or [])

    def to_metadata(self) -> dict[str, JSONValue]:
        findings = self.findings or []
        by_code = Counter(finding.code for finding in findings)
        by_severity = Counter(finding.severity.value for finding in findings)
        return {
            **self.store.to_metadata(),
            "work_item_count": self.work_item_count,
            "artifact_count": self.artifact_count,
            "data_artifact_count": self.data_artifact_count,
            "target_count": self.target_count,
            "job_snapshot_count": self.job_snapshot_count,
            "finding_count": len(findings),
            "finding_count_by_code": dict(sorted(by_code.items())),
            "finding_count_by_severity": dict(sorted(by_severity.items())),
        }


@dataclass(frozen=True, slots=True)
class HistDataProvenanceManifestRule:
    """Validate local files against sidecar manifest/status lineage records."""

    explicit: bool = False
    max_findings: int = _DEFAULT_MAX_FINDINGS
    rule_id: str = PROVENANCE_MANIFEST_RULE_ID
    description: str = (
        "Compare discovered files to sidecar manifest/status artifact lineage."
    )

    def evaluate_run(
        self,
        targets: Iterable[QualityTarget],
        *,
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> QualityReport:
        """Return a provenance report fragment for a whole quality run."""
        target_tuple = tuple(targets)
        metadata_map = dict(metadata or {})
        config = _provenance_config(metadata_map)
        stores = _candidate_stores(target_tuple, config, metadata_map)
        if not stores:
            return _unavailable_report(
                targets=target_tuple,
                config=config,
                explicit=self.explicit,
                rule_id=self.rule_id,
            )

        evaluations = tuple(
            _evaluate_store(store_ref, target_tuple) for store_ref in stores
        )
        templates = [
            finding
            for evaluation in evaluations
            for finding in (evaluation.findings or [])
        ]
        limit = max(0, int(self.max_findings))
        limited = templates[:limit] if limit else []
        finding_limit_reached = len(templates) > len(limited)
        if finding_limit_reached:
            limited.append(
                _FindingTemplate(
                    severity=QualitySeverity.WARNING,
                    code="PROVENANCE_FINDING_LIMIT_REACHED",
                    message=(
                        "Provenance findings were truncated; inspect "
                        "metadata counts for the full summary."
                    ),
                    metadata={
                        "finding_count": len(templates),
                        "emitted_finding_count": len(limited),
                        "max_findings": limit,
                    },
                )
            )

        manifest = _provenance_manifest_payload(
            evaluations,
            roots=_string_list(config.get("roots")),
            finding_count=len(templates),
            emitted_finding_count=len(limited),
            finding_limit_reached=finding_limit_reached,
        )
        target = _provenance_target(stores[0], manifest)
        findings = tuple(
            _quality_finding(template, target, self.rule_id)
            for template in limited
        )
        return QualityReport(
            targets=(target,),
            rule_results=(
                QualityRuleResult(
                    rule_id=self.rule_id,
                    target=target,
                    findings=findings,
                ),
            ),
            metadata={PROVENANCE_METADATA_KEY: manifest},
        )


def provenance_quality_run_rules(
    *,
    explicit: bool = False,
) -> tuple[QualityRunRule, ...]:
    """Return provenance manifest run rules in deterministic order."""
    rule: QualityRunRule = HistDataProvenanceManifestRule(explicit=explicit)
    return (rule,)


def provenance_manifest_metadata(
    *,
    roots: Iterable[str] = (),
    store_root: str = "",
    store_path: str = "",
) -> dict[str, JSONValue]:
    """Return quality-run metadata consumed by the provenance rule."""
    payload: dict[str, JSONValue] = {
        "roots": [str(root) for root in roots],
    }
    if store_root:
        payload["store_root"] = str(store_root)
    if store_path:
        payload["store_path"] = str(store_path)
    return payload


def _unavailable_report(
    *,
    targets: tuple[QualityTarget, ...],
    config: Mapping[str, JSONValue],
    explicit: bool,
    rule_id: str,
) -> QualityReport:
    manifest = {
        "schema_version": PROVENANCE_MANIFEST_SCHEMA_VERSION,
        "available": False,
        "status": "unavailable",
        "reason": "no sidecar manifest/status store found",
        "roots": _string_list(config.get("roots")),
        "store_count": 0,
        "target_count": len(targets),
        "finding_count": int(explicit),
        "emitted_finding_count": int(explicit),
        "finding_limit_reached": False,
        "stores": [],
    }
    if not explicit:
        return QualityReport(metadata={PROVENANCE_METADATA_KEY: manifest})

    target = QualityTarget(
        path="",
        kind=QualityTargetKind.DIRECTORY,
        metadata={
            "schema_version": PROVENANCE_MANIFEST_SCHEMA_VERSION,
            "provenance_available": False,
        },
    )
    finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="PROVENANCE_STORE_UNAVAILABLE",
        message=(
            "No sidecar manifest/status store was found; provenance checks "
            "were not applied."
        ),
        rule_id=rule_id,
        target=target,
        metadata={
            "roots": _string_list(config.get("roots")),
            "target_count": len(targets),
        },
    )
    return QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id=rule_id,
                target=target,
                findings=(finding,),
            ),
        ),
        metadata={PROVENANCE_METADATA_KEY: manifest},
    )


def _evaluate_store(
    store_ref: _StoreRef,
    targets: tuple[QualityTarget, ...],
) -> _StoreEvaluation:
    try:
        store = ManifestStatusStore(store_ref.root)
    except (OSError, ValueError, sqlite3.DatabaseError) as err:
        return _StoreEvaluation(
            store=store_ref,
            target_count=len(targets),
            findings=[
                _FindingTemplate(
                    severity=QualitySeverity.ERROR,
                    code="PROVENANCE_STORE_UNREADABLE",
                    message="Sidecar manifest/status store could not be read.",
                    location_path=str(store_ref.db_path),
                    metadata={
                        "store_root": str(store_ref.root),
                        "store_path": str(store_ref.db_path),
                        "error": str(err),
                    },
                )
            ],
        )
    target_by_path = {
        _normalize_path(target.path): target
        for target in targets
        if target.path
    }
    target_paths = {
        path
        for path, target in target_by_path.items()
        if target.kind.value in _DATA_ARTIFACT_KINDS
    }
    artifact_paths: set[str] = set()
    source_artifacts_by_work_id: dict[str, list[Mapping[str, Any]]] = (
        defaultdict(list)
    )
    cache_artifacts_by_work_id: dict[str, list[Mapping[str, Any]]] = (
        defaultdict(list)
    )
    findings: list[_FindingTemplate] = []
    artifact_count = 0
    data_artifact_count = 0
    work_items = store.list_work_items()

    for item in work_items:
        artifacts = tuple(store.list_artifacts(item.work_id))
        artifact_count += len(artifacts)
        history = store.status_history(item.work_id, owner_kind="work_item")
        _append_lifecycle_findings(
            findings,
            work_item=item,
            history=history,
            store_ref=store_ref,
        )
        for artifact in artifacts:
            kind = str(artifact.get("kind", "") or "").lower()
            if kind not in _DATA_ARTIFACT_KINDS:
                continue
            data_artifact_count += 1
            artifact_path = _artifact_path(artifact, root=store_ref.root)
            normalized = _normalize_path(artifact_path)
            artifact_paths.add(normalized)
            target = target_by_path.get(normalized)
            _append_artifact_file_findings(
                findings,
                artifact=artifact,
                artifact_path=artifact_path,
                work_item=item,
                target=target,
                store_ref=store_ref,
            )
            if kind in _SOURCE_ARTIFACT_KINDS:
                source_artifacts_by_work_id[item.work_id].append(artifact)
            elif kind == "cache":
                cache_artifacts_by_work_id[item.work_id].append(artifact)

    for item in work_items:
        _append_stale_cache_findings(
            findings,
            work_item=item,
            sources=tuple(source_artifacts_by_work_id[item.work_id]),
            caches=tuple(cache_artifacts_by_work_id[item.work_id]),
            root=store_ref.root,
        )

    for path in sorted(target_paths.difference(artifact_paths)):
        target = target_by_path[path]
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.WARNING,
                code="PROVENANCE_ORPHAN_TARGET",
                message="Discovered quality target is not referenced by sidecar artifacts.",
                location_path=target.path,
                metadata={
                    **_target_metadata(target),
                    "store_path": str(store_ref.db_path),
                },
            )
        )

    return _StoreEvaluation(
        store=store_ref,
        work_item_count=len(work_items),
        artifact_count=artifact_count,
        data_artifact_count=data_artifact_count,
        target_count=len(targets),
        job_snapshot_count=len(store.list_job_snapshots()),
        findings=findings,
    )


def _append_lifecycle_findings(
    findings: list[_FindingTemplate],
    *,
    work_item: WorkItem,
    history: tuple[Mapping[str, Any], ...],
    store_ref: _StoreRef,
) -> None:
    if not history:
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.WARNING,
                code="PROVENANCE_STATUS_HISTORY_MISSING",
                message="Work item has no sidecar status history.",
                metadata={
                    "work_id": work_item.work_id,
                    "status": work_item.status.value,
                    "store_path": str(store_ref.db_path),
                },
            )
        )
        return

    last_status = WorkStatus.from_value(history[-1].get("status"))
    if last_status != work_item.status:
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.WARNING,
                code="PROVENANCE_STATUS_LIFECYCLE_MISMATCH",
                message="Work item status does not match its latest status event.",
                metadata={
                    "work_id": work_item.work_id,
                    "work_item_status": work_item.status.value,
                    "latest_event_status": last_status.value,
                    "store_path": str(store_ref.db_path),
                },
            )
        )


def _append_artifact_file_findings(
    findings: list[_FindingTemplate],
    *,
    artifact: Mapping[str, Any],
    artifact_path: Path,
    work_item: WorkItem,
    target: QualityTarget | None,
    store_ref: _StoreRef,
) -> None:
    artifact_metadata = _artifact_metadata(artifact)
    artifact_kind = str(artifact.get("kind", "") or "")
    location_path = str(artifact_path)
    if not artifact_path.exists():
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.ERROR,
                code="PROVENANCE_ARTIFACT_MISSING",
                message="Sidecar artifact reference points to a missing file.",
                location_path=location_path,
                metadata={
                    "artifact_kind": artifact_kind,
                    "work_id": work_item.work_id,
                    "store_path": str(store_ref.db_path),
                },
            )
        )
        return

    expected_size = _optional_int(artifact.get("size_bytes"))
    actual_size = artifact_path.stat().st_size
    if expected_size is not None and expected_size != actual_size:
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.ERROR,
                code="PROVENANCE_ARTIFACT_SIZE_MISMATCH",
                message="Sidecar artifact size does not match the file on disk.",
                location_path=location_path,
                metadata={
                    "artifact_kind": artifact_kind,
                    "work_id": work_item.work_id,
                    "expected_size_bytes": expected_size,
                    "actual_size_bytes": actual_size,
                    "store_path": str(store_ref.db_path),
                },
            )
        )

    expected_sha256 = str(artifact.get("sha256", "") or "").strip().lower()
    if expected_sha256:
        actual_sha256 = _file_sha256(artifact_path)
        if actual_sha256 != expected_sha256:
            findings.append(
                _FindingTemplate(
                    severity=QualitySeverity.ERROR,
                    code="PROVENANCE_ARTIFACT_CHECKSUM_MISMATCH",
                    message="Sidecar artifact checksum does not match the file on disk.",
                    location_path=location_path,
                    metadata={
                        "artifact_kind": artifact_kind,
                        "work_id": work_item.work_id,
                        "expected_sha256": expected_sha256,
                        "actual_sha256": actual_sha256,
                        "store_path": str(store_ref.db_path),
                    },
                )
            )

    if target is not None:
        _append_target_dimension_findings(
            findings,
            target=target,
            work_item=work_item,
            artifact_kind=artifact_kind,
            location_path=location_path,
            store_ref=store_ref,
        )

    if artifact_kind.lower() == "cache":
        _append_cache_metadata_findings(
            findings,
            artifact_metadata=artifact_metadata,
            artifact_path=artifact_path,
            work_item=work_item,
            store_ref=store_ref,
        )


def _append_target_dimension_findings(
    findings: list[_FindingTemplate],
    *,
    target: QualityTarget,
    work_item: WorkItem,
    artifact_kind: str,
    location_path: str,
    store_ref: _StoreRef,
) -> None:
    comparisons = (
        ("data_format", target.data_format, work_item.data_format),
        ("timeframe", target.timeframe, work_item.data_timeframe),
        ("symbol", target.symbol, work_item.data_fxpair),
        ("period", target.period, work_item.data_datemonth),
    )
    for field, observed, expected in comparisons:
        if not observed or not expected:
            continue
        if _dimension_value(field, observed) == _dimension_value(
            field, expected
        ):
            continue
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.ERROR,
                code="PROVENANCE_TARGET_METADATA_MISMATCH",
                message="Discovered target metadata differs from its work item.",
                location_path=location_path,
                metadata={
                    "field": field,
                    "artifact_kind": artifact_kind,
                    "work_id": work_item.work_id,
                    "target_value": observed,
                    "work_item_value": expected,
                    "store_path": str(store_ref.db_path),
                },
            )
        )


def _append_cache_metadata_findings(
    findings: list[_FindingTemplate],
    *,
    artifact_metadata: Mapping[str, Any],
    artifact_path: Path,
    work_item: WorkItem,
    store_ref: _StoreRef,
) -> None:
    expected_filename = work_item.cache_filename
    if expected_filename and artifact_path.name != expected_filename:
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.ERROR,
                code="PROVENANCE_CACHE_METADATA_MISMATCH",
                message="Cache artifact filename differs from work item metadata.",
                location_path=str(artifact_path),
                metadata={
                    "field": "cache_filename",
                    "work_id": work_item.work_id,
                    "artifact_value": artifact_path.name,
                    "work_item_value": expected_filename,
                    "store_path": str(store_ref.db_path),
                },
            )
        )

    comparisons = (
        (
            "line_count",
            artifact_metadata.get("line_count"),
            work_item.cache_line_count,
        ),
        ("start", artifact_metadata.get("start"), work_item.cache_start),
        ("end", artifact_metadata.get("end"), work_item.cache_end),
    )
    for field, artifact_value, work_item_value in comparisons:
        if artifact_value in {None, ""} or work_item_value in {None, ""}:
            continue
        if str(artifact_value) == str(work_item_value):
            continue
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.ERROR,
                code="PROVENANCE_CACHE_METADATA_MISMATCH",
                message="Cache artifact metadata differs from work item metadata.",
                location_path=str(artifact_path),
                metadata={
                    "field": field,
                    "work_id": work_item.work_id,
                    "artifact_value": str(artifact_value),
                    "work_item_value": str(work_item_value),
                    "store_path": str(store_ref.db_path),
                },
            )
        )


def _append_stale_cache_findings(
    findings: list[_FindingTemplate],
    *,
    work_item: WorkItem,
    sources: tuple[Mapping[str, Any], ...],
    caches: tuple[Mapping[str, Any], ...],
    root: Path,
) -> None:
    existing_sources = tuple(
        path
        for path in (
            _artifact_path(artifact, root=root) for artifact in sources
        )
        if path.exists()
    )
    if not existing_sources:
        return
    newest_source_mtime = max(path.stat().st_mtime for path in existing_sources)
    newest_source_path = max(
        existing_sources, key=lambda path: path.stat().st_mtime
    )
    for cache in caches:
        cache_path = _artifact_path(cache, root=root)
        if not cache_path.exists():
            continue
        if cache_path.stat().st_mtime >= newest_source_mtime:
            continue
        findings.append(
            _FindingTemplate(
                severity=QualitySeverity.WARNING,
                code="PROVENANCE_CACHE_STALE",
                message="Cache artifact is older than its source ZIP/CSV artifact.",
                location_path=str(cache_path),
                metadata={
                    "work_id": work_item.work_id,
                    "cache_mtime": cache_path.stat().st_mtime,
                    "source_path": str(newest_source_path),
                    "source_mtime": newest_source_mtime,
                },
            )
        )


def _provenance_manifest_payload(
    evaluations: tuple[_StoreEvaluation, ...],
    *,
    roots: list[str],
    finding_count: int,
    emitted_finding_count: int,
    finding_limit_reached: bool,
) -> dict[str, JSONValue]:
    by_code = Counter(
        finding.code
        for evaluation in evaluations
        for finding in (evaluation.findings or [])
    )
    by_severity = Counter(
        finding.severity.value
        for evaluation in evaluations
        for finding in (evaluation.findings or [])
    )
    return {
        "schema_version": PROVENANCE_MANIFEST_SCHEMA_VERSION,
        "available": True,
        "status": "checked",
        "roots": roots,
        "store_count": len(evaluations),
        "target_count": sum(
            evaluation.target_count for evaluation in evaluations
        ),
        "work_item_count": sum(
            evaluation.work_item_count for evaluation in evaluations
        ),
        "artifact_count": sum(
            evaluation.artifact_count for evaluation in evaluations
        ),
        "data_artifact_count": sum(
            evaluation.data_artifact_count for evaluation in evaluations
        ),
        "job_snapshot_count": sum(
            evaluation.job_snapshot_count for evaluation in evaluations
        ),
        "finding_count": finding_count,
        "emitted_finding_count": emitted_finding_count,
        "finding_limit_reached": finding_limit_reached,
        "finding_count_by_code": dict(sorted(by_code.items())),
        "finding_count_by_severity": dict(sorted(by_severity.items())),
        "stores": [evaluation.to_metadata() for evaluation in evaluations],
    }


def _provenance_target(
    store_ref: _StoreRef,
    manifest: Mapping[str, JSONValue],
) -> QualityTarget:
    return QualityTarget(
        path=str(store_ref.db_path),
        kind=QualityTargetKind.DIRECTORY,
        metadata={
            "schema_version": PROVENANCE_MANIFEST_SCHEMA_VERSION,
            "provenance_available": bool(manifest.get("available")),
            "store_root": str(store_ref.root),
            "store_path": str(store_ref.db_path),
        },
    )


def _quality_finding(
    template: _FindingTemplate,
    target: QualityTarget,
    rule_id: str,
) -> QualityFinding:
    return QualityFinding(
        severity=template.severity,
        code=template.code,
        message=template.message,
        rule_id=rule_id,
        target=target,
        location=QualityLocation(path=template.location_path),
        metadata=dict(template.metadata or {}),
    )


def _provenance_config(
    metadata: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    config: dict[str, JSONValue] = {}
    raw = metadata.get(PROVENANCE_METADATA_KEY)
    if isinstance(raw, Mapping):
        config.update(dict(raw))

    coverage = metadata.get("coverage_manifest")
    if isinstance(coverage, Mapping) and "roots" not in config:
        roots = coverage.get("roots")
        if isinstance(roots, list):
            config["roots"] = [str(root) for root in roots]

    status_store_ref = metadata.get(STATUS_STORE_REF_KEY)
    if isinstance(status_store_ref, Mapping):
        _merge_status_store_ref(config, status_store_ref)
    raw_status_store = metadata.get("sidecar_status_store")
    if isinstance(raw_status_store, Mapping):
        _merge_status_store_ref(config, raw_status_store)

    config.setdefault("roots", [])
    return config


def _merge_status_store_ref(
    config: dict[str, JSONValue],
    status_store_ref: Mapping[str, Any],
) -> None:
    kind = str(status_store_ref.get("kind", "") or "")
    if kind and kind != STATUS_STORE_REF_KIND:
        return
    if status_store_ref.get("store_root") and not config.get("store_root"):
        config["store_root"] = str(status_store_ref["store_root"])
    if status_store_ref.get("store_path") and not config.get("store_path"):
        config["store_path"] = str(status_store_ref["store_path"])


def _candidate_stores(
    targets: tuple[QualityTarget, ...],
    config: Mapping[str, JSONValue],
    metadata: Mapping[str, JSONValue],
) -> tuple[_StoreRef, ...]:
    refs: list[_StoreRef] = []
    seen: set[str] = set()

    def add_ref(root: Path, db_path: Path, source: str) -> None:
        if not db_path.exists():
            return
        normalized = _normalize_path(db_path)
        if normalized in seen:
            return
        seen.add(normalized)
        refs.append(_StoreRef(root=root, db_path=db_path, source=source))

    store_path = str(config.get("store_path", "") or "")
    if store_path:
        path = Path(store_path).expanduser()
        add_ref(_root_from_db_path(path), path, "metadata.store_path")

    store_root = str(config.get("store_root", "") or "")
    if store_root:
        root = Path(store_root).expanduser()
        add_ref(
            root, ManifestStatusStore.path_for_root(root), "metadata.store_root"
        )

    for root_value in _string_list(config.get("roots")):
        _add_candidate_root(
            add_ref, Path(root_value).expanduser(), "metadata.roots"
        )

    if not refs:
        for root_value in _string_list(metadata.get("roots")):
            _add_candidate_root(
                add_ref, Path(root_value).expanduser(), "metadata.roots"
            )

    if not refs:
        for target in targets:
            if target.path:
                _add_candidate_root(
                    add_ref,
                    Path(target.path).expanduser(),
                    "target.ancestor",
                )

    return tuple(refs)


def _add_candidate_root(
    add_ref: Any,
    path: Path,
    source: str,
) -> None:
    if path.name == MANIFEST_DB_FILENAME:
        add_ref(_root_from_db_path(path), path, source)
        return

    start = path if path.is_dir() else path.parent
    candidates = (start, *start.parents)
    for candidate in candidates:
        db_path = candidate / MANIFEST_DIRECTORY / MANIFEST_DB_FILENAME
        if db_path.exists():
            add_ref(candidate, db_path, source)
            return


def _root_from_db_path(db_path: Path) -> Path:
    if (
        db_path.name == MANIFEST_DB_FILENAME
        and db_path.parent.name == MANIFEST_DIRECTORY
    ):
        return db_path.parent.parent
    return db_path.parent


def _artifact_path(artifact: Mapping[str, Any], *, root: Path) -> Path:
    path = Path(str(artifact.get("path", "") or "")).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve(strict=False)


def _artifact_metadata(artifact: Mapping[str, Any]) -> dict[str, Any]:
    raw = artifact.get("metadata")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _target_metadata(target: QualityTarget) -> dict[str, JSONValue]:
    return {
        "target_kind": target.kind.value,
        "data_format": target.data_format,
        "timeframe": target.timeframe,
        "symbol": target.symbol,
        "period": target.period,
    }


def _dimension_value(field: str, value: str) -> str:
    value = str(value or "").strip()
    if field in {"timeframe", "symbol"}:
        return value.upper()
    if field == "data_format":
        return value.lower()
    return value


def _normalize_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve(strict=False))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(_SHA256_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _string_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    if value is None or value == "":
        return []
    return [str(value)]


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
