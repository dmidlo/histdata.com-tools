"""Cancellation, cleanup, and resume policy helpers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable

from histdatacom.runtime_contracts import JSONValue, WorkItem, derive_work_id


class PartialArtifactDisposition(str, Enum):
    """How cancellation treats incomplete side-effect artifacts."""

    NONE = "none"
    REMOVE_TEMP = "remove_temp"
    REUSE_COMPLETE = "reuse_complete"
    IDEMPOTENT_EXTERNAL = "idempotent_external"


class ResumeMode(str, Enum):
    """How interrupted work can be started again."""

    REPLAY = "replay"
    REUSE_COMPLETE_ARTIFACT = "reuse_complete_artifact"
    RETRY_IDEMPOTENT = "retry_idempotent"


@dataclass(frozen=True, slots=True)
class OperationResumePolicy:
    """Machine-readable cancellation and resume behavior for one operation."""

    stage: str
    partial_artifact_disposition: PartialArtifactDisposition
    resume_mode: ResumeMode
    cleanup_summary: str
    resume_summary: str
    retry_safe: bool = True
    stops_future_work: bool = True
    partial_artifact_patterns: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible policy metadata."""
        return {
            "stage": self.stage,
            "partial_artifact_disposition": (
                self.partial_artifact_disposition.value
            ),
            "resume_mode": self.resume_mode.value,
            "cleanup_summary": self.cleanup_summary,
            "resume_summary": self.resume_summary,
            "retry_safe": self.retry_safe,
            "stops_future_work": self.stops_future_work,
            "partial_artifact_patterns": list(self.partial_artifact_patterns),
        }


@dataclass(frozen=True, slots=True)
class CleanupResult:
    """Result of one partial-artifact cleanup attempt."""

    path: str
    existed: bool
    removed: bool = False
    error: str = ""

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible cleanup metadata."""
        return {
            "path": self.path,
            "existed": self.existed,
            "removed": self.removed,
            "error": self.error,
        }


OPERATION_RESUME_POLICIES: dict[str, OperationResumePolicy] = {
    "repository_refresh": OperationResumePolicy(
        stage="repository_refresh",
        partial_artifact_disposition=PartialArtifactDisposition.REMOVE_TEMP,
        resume_mode=ResumeMode.REUSE_COMPLETE_ARTIFACT,
        cleanup_summary=(
            "Repository metadata is written through a hidden temp file; "
            "cancellation removes temp files and keeps the last complete repo."
        ),
        resume_summary=(
            "Resume reads the last complete repository file or retries the "
            "refresh if no complete file exists."
        ),
        partial_artifact_patterns=(".repo.*.tmp",),
    ),
    "dataset_plan": OperationResumePolicy(
        stage="dataset_plan",
        partial_artifact_disposition=PartialArtifactDisposition.NONE,
        resume_mode=ResumeMode.REPLAY,
        cleanup_summary="Planning has no filesystem side effects.",
        resume_summary="Resume deterministically replans the same work items.",
    ),
    "validate_urls": OperationResumePolicy(
        stage="validate_urls",
        partial_artifact_disposition=PartialArtifactDisposition.REUSE_COMPLETE,
        resume_mode=ResumeMode.REPLAY,
        cleanup_summary=(
            "Validation writes only complete record metadata artifacts; no "
            "partial data artifact is promoted."
        ),
        resume_summary=(
            "Resume can repeat validation for unfinished URLs and reuse "
            "complete metadata."
        ),
    ),
    "validate_url": OperationResumePolicy(
        stage="validate_url",
        partial_artifact_disposition=PartialArtifactDisposition.REUSE_COMPLETE,
        resume_mode=ResumeMode.REPLAY,
        cleanup_summary=(
            "Validation writes only complete record metadata artifacts; no "
            "partial data artifact is promoted."
        ),
        resume_summary=(
            "Resume can repeat validation for unfinished URLs and reuse "
            "complete metadata."
        ),
    ),
    "download_archives": OperationResumePolicy(
        stage="download_archives",
        partial_artifact_disposition=PartialArtifactDisposition.REMOVE_TEMP,
        resume_mode=ResumeMode.REUSE_COMPLETE_ARTIFACT,
        cleanup_summary=(
            "Archive downloads are written to hidden temp ZIPs and atomically "
            "renamed; cancellation removes temp ZIPs."
        ),
        resume_summary=(
            "Resume reuses complete ZIP/CSV/cache artifacts or redownloads "
            "missing archives."
        ),
        partial_artifact_patterns=("*.zip.*.tmp",),
    ),
    "download_archive": OperationResumePolicy(
        stage="download_archive",
        partial_artifact_disposition=PartialArtifactDisposition.REMOVE_TEMP,
        resume_mode=ResumeMode.REUSE_COMPLETE_ARTIFACT,
        cleanup_summary=(
            "Archive downloads are written to hidden temp ZIPs and atomically "
            "renamed; cancellation removes temp ZIPs."
        ),
        resume_summary=(
            "Resume reuses complete ZIP/CSV/cache artifacts or redownloads "
            "missing archives."
        ),
        partial_artifact_patterns=("*.zip.*.tmp",),
    ),
    "extract_csv": OperationResumePolicy(
        stage="extract_csv",
        partial_artifact_disposition=PartialArtifactDisposition.REMOVE_TEMP,
        resume_mode=ResumeMode.REUSE_COMPLETE_ARTIFACT,
        cleanup_summary=(
            "Extraction writes hidden temp CSV/XLSX files and atomically "
            "renames them; cancellation removes temp extraction files."
        ),
        resume_summary=(
            "Resume reuses complete CSV/cache artifacts or extracts again "
            "from the complete ZIP."
        ),
        partial_artifact_patterns=("*.csv.*.tmp", "*.xlsx.*.tmp"),
    ),
    "build_cache": OperationResumePolicy(
        stage="build_cache",
        partial_artifact_disposition=PartialArtifactDisposition.REMOVE_TEMP,
        resume_mode=ResumeMode.REUSE_COMPLETE_ARTIFACT,
        cleanup_summary=(
            "Cache builds write hidden temp IPC files and atomically rename "
            "them; cancellation removes temp cache files."
        ),
        resume_summary=(
            "Resume reuses a complete cache or rebuilds from the complete "
            "ZIP/CSV source."
        ),
        partial_artifact_patterns=(".data.*.tmp",),
    ),
    "merge_cache": OperationResumePolicy(
        stage="merge_cache",
        partial_artifact_disposition=PartialArtifactDisposition.NONE,
        resume_mode=ResumeMode.REPLAY,
        cleanup_summary=(
            "Merge assembly records bounded metadata and does not promote "
            "partial merged data in orchestration mode."
        ),
        resume_summary=(
            "Resume replays merge assembly from complete cache artifacts."
        ),
    ),
    "import_to_influx": OperationResumePolicy(
        stage="import_to_influx",
        partial_artifact_disposition=(
            PartialArtifactDisposition.IDEMPOTENT_EXTERNAL
        ),
        resume_mode=ResumeMode.RETRY_IDEMPOTENT,
        cleanup_summary=(
            "Influx writes are external batches; local cache/ZIP files remain "
            "until a successful import cleanup decision."
        ),
        resume_summary=(
            "Resume retries bounded line-protocol batches using the cache "
            "path/start/end/batch-size idempotency key."
        ),
    ),
}

WORKFLOW_STAGE_ALIASES = {
    "RepositoryRefreshWorkflow": "repository_refresh",
    "DatasetPlanWorkflow": "dataset_plan",
    "ValidateUrlsWorkflow": "validate_urls",
    "DownloadArchivesWorkflow": "download_archives",
    "ExtractCsvWorkflow": "extract_csv",
    "BuildCacheWorkflow": "build_cache",
    "MergeCacheWorkflow": "merge_cache",
    "ImportWorkflow": "import_to_influx",
}


def operation_resume_policy(stage: str) -> OperationResumePolicy:
    """Return cancellation/resume policy for an operation stage."""
    raw_stage = str(stage or "").strip()
    normalized = WORKFLOW_STAGE_ALIASES.get(raw_stage, raw_stage)
    return OPERATION_RESUME_POLICIES.get(
        normalized,
        OperationResumePolicy(
            stage=normalized or "unknown",
            partial_artifact_disposition=PartialArtifactDisposition.NONE,
            resume_mode=ResumeMode.REPLAY,
            cleanup_summary="No operation-specific cleanup policy is defined.",
            resume_summary="Resume replays the operation from prior inputs.",
        ),
    )


def all_operation_resume_policies() -> tuple[OperationResumePolicy, ...]:
    """Return the documented operation resume policy set."""
    return tuple(OPERATION_RESUME_POLICIES.values())


def operation_resume_metadata(stage: str) -> dict[str, JSONValue]:
    """Return JSON-compatible resume policy metadata for one stage."""
    return operation_resume_policy(stage).to_dict()


def cancellation_metadata(
    stage: str,
    *,
    reason: str = "",
    cleanup_results: Iterable[CleanupResult] = (),
) -> dict[str, JSONValue]:
    """Return JSON-compatible cancellation metadata for status payloads."""
    policy = operation_resume_policy(stage)
    return {
        "cancelled": True,
        "reason": reason,
        "stops_future_work": policy.stops_future_work,
        "cleanup": [result.to_dict() for result in cleanup_results],
        "resume_policy": policy.to_dict(),
    }


def job_cancellation_metadata(reason: str = "") -> dict[str, JSONValue]:
    """Return GUI-ready cancellation semantics for a whole orchestration job."""
    return {
        "cancelled": True,
        "reason": reason,
        "stops_future_work": True,
        "cleanup": "in-flight activities remove hidden temp artifacts",
        "resume_policies": [
            policy.to_dict() for policy in all_operation_resume_policies()
        ],
    }


def deterministic_partial_path(target_path: Path, work_id: str) -> Path:
    """Return the hidden temp path used for atomic artifact writes."""
    suffix = derive_work_id(work_id).removeprefix("work-")
    return target_path.with_name(f".{target_path.name}.{suffix}.tmp")


def partial_artifact_candidates(
    stage: str,
    work_item: WorkItem | None = None,
    *,
    repo_local_path: str | Path = "",
) -> tuple[Path, ...]:
    """Return known partial artifact candidates for cleanup."""
    candidates: set[Path] = set()
    repo_path = Path(repo_local_path) if repo_local_path else None
    if repo_path is not None:
        candidates.update(repo_path.parent.glob(f".{repo_path.name}.*.tmp"))

    if work_item is not None and work_item.data_dir:
        data_dir = Path(work_item.data_dir)
        policy = operation_resume_policy(stage)
        for pattern in policy.partial_artifact_patterns:
            candidates.update(data_dir.glob(pattern))
            if not pattern.startswith("."):
                candidates.update(data_dir.glob(f".{pattern}"))
        candidates.update(_deterministic_work_item_partials(stage, work_item))

    return tuple(sorted(candidates, key=lambda path: str(path)))


def cleanup_partial_artifacts(
    paths: Iterable[Path],
) -> tuple[CleanupResult, ...]:
    """Remove partial artifact candidates and report each action."""
    results: list[CleanupResult] = []
    for path in paths:
        try:
            existed = path.exists()
            if existed:
                path.unlink()
            results.append(
                CleanupResult(
                    path=str(path),
                    existed=existed,
                    removed=existed and not path.exists(),
                )
            )
        except OSError as err:
            results.append(
                CleanupResult(
                    path=str(path),
                    existed=True,
                    removed=False,
                    error=str(err),
                )
            )
    return tuple(results)


def _deterministic_work_item_partials(
    stage: str,
    work_item: WorkItem,
) -> tuple[Path, ...]:
    data_dir = Path(work_item.data_dir)
    urls_or_id = work_item.url or work_item.work_id
    candidates: list[Path] = []
    if stage in {"download_archive", "download_archives"}:
        _append_candidate(
            candidates, data_dir, work_item.zip_filename, urls_or_id
        )
    if stage == "extract_csv":
        _append_candidate(
            candidates, data_dir, work_item.csv_filename, urls_or_id
        )
    if stage == "build_cache":
        _append_candidate(
            candidates,
            data_dir,
            work_item.cache_filename or ".data",
            urls_or_id,
        )
    return tuple(candidates)


def _append_candidate(
    candidates: list[Path],
    data_dir: Path,
    filename: str,
    work_id: str,
) -> None:
    if filename:
        candidates.append(
            deterministic_partial_path(data_dir / filename, work_id)
        )
