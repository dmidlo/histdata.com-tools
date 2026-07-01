"""Rich terminal renderers for orchestration job progress."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from rich import box
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table
from rich.text import Text

from histdatacom.runtime_contracts import ArtifactRef, StatusEvent, WorkStatus
from histdatacom.orchestration.control import (
    JobLifecycle,
    JobProgressSnapshot,
    OrchestrationJobSnapshot,
)

RECENT_EVENT_LIMIT = 6
STAGE_LIMIT = 8
ARTIFACT_LIMIT = 5
COMPONENT_LIMIT = 5
GROUP_LIMIT = 4
DEFAULT_HEALTH_REFRESH_SECONDS = 30.0
OPERATIONAL_HEALTH_METADATA_KEY = "operational_health"
RUNTIME_HEALTH_METADATA_KEY = "runtime_health"
HealthProvider = Callable[
    [OrchestrationJobSnapshot],
    Mapping[str, Any] | None,
]
STAGE_LABELS = {
    "RepositoryRefreshWorkflow": "Repository refresh",
    "DataQualityWorkflow": "Data quality",
    "DatasetPlanWorkflow": "Plan datasets",
    "SymbolTimeframeWorkflow": "Run dataset batches",
    "ValidateUrlsWorkflow": "Validate URLs",
    "DownloadArchivesWorkflow": "Download archives",
    "ExtractCsvWorkflow": "Extract files",
    "BuildCacheWorkflow": "Build Polars caches",
    "MergeCacheWorkflow": "Merge caches",
    "ImportWorkflow": "Import to InfluxDB",
    "repository_refresh": "Repository refresh",
    "data_quality": "Data quality",
    "dataset_plan": "Plan datasets",
    "validate_urls": "Validate URLs",
    "validate_url": "Validate URL",
    "download_archives": "Download archives",
    "download_archive": "Download archive",
    "extract_csv": "Extract files",
    "build_cache": "Build Polars cache",
    "merge_cache": "Merge caches",
    "import_to_influx": "Import to InfluxDB",
    "started": "Started",
    "finished": "Finished",
}


def render_job_progress(
    snapshot: OrchestrationJobSnapshot,
    *,
    console: Console | None = None,
) -> None:
    """Print one Rich progress dashboard for a job snapshot."""
    resolved_console = console or Console()
    resolved_console.print(build_job_progress_renderable(snapshot))


def watch_job_progress(
    fetch_snapshot: Callable[[], OrchestrationJobSnapshot],
    *,
    interval_seconds: float = 1.0,
    console: Console | None = None,
) -> OrchestrationJobSnapshot:
    """Poll a job snapshot and render a live Rich progress dashboard."""
    resolved_console = console or Console()
    snapshot = fetch_snapshot()
    with Live(
        build_job_progress_renderable(snapshot),
        console=resolved_console,
        refresh_per_second=4,
        transient=False,
    ) as live:
        while not is_job_terminal(snapshot):
            time.sleep(max(0.1, interval_seconds))
            snapshot = fetch_snapshot()
            live.update(build_job_progress_renderable(snapshot))
    return snapshot


class LiveJobProgressRenderer:
    """Callback-friendly Rich live renderer for a waited foreground job."""

    def __init__(
        self,
        *,
        console: Console | None = None,
        health_provider: HealthProvider | None = None,
        health_refresh_seconds: float = DEFAULT_HEALTH_REFRESH_SECONDS,
    ) -> None:
        self.console = console or Console()
        self._live: Live | None = None
        self._health_provider = health_provider
        self._health_refresh_seconds = max(1.0, health_refresh_seconds)
        self._last_health_refresh = 0.0
        self._cached_health: dict[str, Any] | None = None

    def __enter__(self) -> "LiveJobProgressRenderer":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def update(self, snapshot: OrchestrationJobSnapshot) -> None:
        """Render the latest snapshot, starting the live display if needed."""
        snapshot = self._snapshot_with_health(snapshot)
        renderable = build_job_progress_renderable(snapshot)
        if self._live is None:
            self._live = Live(
                renderable,
                console=self.console,
                refresh_per_second=4,
                transient=False,
            )
            self._live.start()
            return
        self._live.update(renderable)

    def close(self) -> None:
        """Stop the live display if it has been started."""
        if self._live is None:
            return
        self._live.stop()
        self._live = None

    def _snapshot_with_health(
        self,
        snapshot: OrchestrationJobSnapshot,
    ) -> OrchestrationJobSnapshot:
        if self._health_provider is None:
            return snapshot
        now = time.monotonic()
        if (
            self._cached_health is None
            or now - self._last_health_refresh >= self._health_refresh_seconds
        ):
            self._last_health_refresh = now
            try:
                payload = self._health_provider(snapshot)
            except Exception:  # pragma: no cover - UI fallback only
                payload = {
                    "status": "unavailable",
                    "message": "health unavailable",
                    "runtime": _mapping(
                        snapshot.metadata.get(RUNTIME_HEALTH_METADATA_KEY)
                    ),
                }
            self._cached_health = dict(payload or {})
        metadata = {
            **snapshot.metadata,
            OPERATIONAL_HEALTH_METADATA_KEY: self._cached_health,
        }
        return replace(snapshot, metadata=metadata)


def build_job_progress_renderable(
    snapshot: OrchestrationJobSnapshot,
) -> RenderableType:
    """Build a Rich renderable for one orchestration job snapshot."""
    progress = snapshot.progress
    status = progress.status if progress is not None else snapshot.status
    style = _status_style(status, snapshot.lifecycle)
    body: list[RenderableType] = [
        _summary_table(snapshot),
        _progress_bar(snapshot),
    ]
    operational_health = _operational_health_table(snapshot)
    if operational_health is not None:
        body.append(operational_health)
    if progress is not None and progress.planned_children:
        body.append(_stage_table(progress))
    if progress is not None and progress.events:
        body.append(_events_table(progress.events))
    artifacts = tuple(snapshot.artifacts)
    if progress is not None and progress.artifacts:
        artifacts = progress.artifacts
    if artifacts:
        body.append(_artifacts_table(artifacts))
    return Panel(
        Group(*body),
        title="HistData job progress",
        subtitle=_subtitle(snapshot),
        border_style=style,
        box=box.ROUNDED,
        padding=(1, 2),
    )


def is_job_terminal(snapshot: OrchestrationJobSnapshot) -> bool:
    """Return whether a job snapshot is terminal for watch-mode polling."""
    if snapshot.lifecycle in {
        JobLifecycle.SUCCEEDED,
        JobLifecycle.FAILED,
        JobLifecycle.CANCELLED,
    }:
        return True
    if snapshot.status in {
        WorkStatus.COMPLETED,
        WorkStatus.FAILED,
        WorkStatus.CANCELLED,
    }:
        return True
    progress = snapshot.progress
    return progress is not None and progress.status in {
        WorkStatus.COMPLETED,
        WorkStatus.FAILED,
        WorkStatus.CANCELLED,
    }


def _summary_table(snapshot: OrchestrationJobSnapshot) -> Table:
    progress = snapshot.progress
    status = progress.status if progress is not None else snapshot.status
    current_stage = (
        _stage_label(progress.current_stage if progress is not None else "")
        or "waiting for first progress event"
    )
    table = Table.grid(expand=True, padding=(0, 1))
    table.add_column(style="bold dim", no_wrap=True)
    table.add_column(ratio=1)
    table.add_row("Workflow:", snapshot.workflow_id or snapshot.job_id)
    table.add_row("Request:", snapshot.request_id or "-")
    table.add_row("Lifecycle:", snapshot.lifecycle.value)
    table.add_row(
        "Status:",
        Text(status.value, style=_status_style(status)),
    )
    table.add_row("Stage:", current_stage)
    table.add_row("Updated:", _updated_at(snapshot))
    if progress is not None:
        table.add_row("Started:", progress.started_at_utc or "-")
        table.add_row(
            "Rate:",
            _format_rate(progress.rate_per_second, progress.unit),
        )
        eta = _format_eta(progress)
        if eta:
            table.add_row("ETA:", eta)
        table.add_row("Unit:", progress.unit)
    if progress is not None and progress.last_error:
        table.add_row(
            "Error:",
            Text(progress.last_error, style="red"),
        )
    return table


def _progress_bar(snapshot: OrchestrationJobSnapshot) -> Progress:
    progress = snapshot.progress
    total = progress.total_children if progress is not None else 0
    completed = progress.completed_children if progress is not None else 0
    unit = progress.unit if progress is not None else "items"
    percent = progress.percent_complete if progress is not None else 0.0
    status = progress.status if progress is not None else snapshot.status
    current_stage = progress.current_stage if progress is not None else ""
    description = _stage_label(current_stage) or status.value.lower()
    progress_bar = Progress(
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("{task.fields[percent_text]}"),
        TextColumn("{task.fields[count_text]}"),
        expand=True,
    )
    progress_bar.add_task(
        description,
        total=max(float(total), 1.0),
        completed=min(float(completed), max(float(total), 1.0)),
        percent_text=(f"{percent:5.1f}%" if total > 0 else "waiting"),
        count_text=(
            f"{_format_number(completed)}/{_format_number(total)} {unit}"
            if total > 0
            else f"{_format_number(completed)} {unit}"
        ),
    )
    return progress_bar


def _operational_health_table(
    snapshot: OrchestrationJobSnapshot,
) -> Table | None:
    health = _mapping(snapshot.metadata.get(OPERATIONAL_HEALTH_METADATA_KEY))
    runtime = _mapping(health.get("runtime")) or _mapping(
        snapshot.metadata.get(RUNTIME_HEALTH_METADATA_KEY)
    )
    if not runtime and (
        snapshot.orchestration_state or snapshot.orchestration_message
    ):
        runtime = {
            "state": snapshot.orchestration_state,
            "message": snapshot.orchestration_message,
        }
    summary = _mapping(health.get("summary"))
    cleanup = _mapping(health.get("cleanup"))
    workflows = _mapping(health.get("workflows"))
    disk = _mapping(health.get("disk")) or _mapping(runtime.get("disk"))
    groups = _list_of_mappings(health.get("groups"))
    if not any((runtime, summary, cleanup, workflows, disk, groups)):
        return None

    table = Table(
        title="Operational Health",
        box=box.SIMPLE_HEAD,
        expand=True,
        show_lines=False,
    )
    table.add_column("Signal", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Detail", overflow="fold")

    if runtime:
        runtime_state = str(runtime.get("state", "unknown") or "unknown")
        table.add_row(
            "Runtime",
            Text(runtime_state, style=_health_style(runtime_state)),
            _runtime_detail(runtime),
        )
        component_detail = _component_detail(runtime)
        if component_detail:
            table.add_row(
                "Components",
                _component_status(runtime),
                component_detail,
            )

    if disk:
        disk_state = str(disk.get("state", "unknown") or "unknown")
        table.add_row(
            "Disk",
            Text(disk_state, style=_health_style(disk_state)),
            _disk_detail(disk),
        )

    if summary:
        status = str(health.get("status", "") or "cache-status")
        source_state = _source_state(summary, cleanup)
        table.add_row(
            "Cache",
            Text(status, style=_health_style(status)),
            _cache_detail(summary),
        )
        table.add_row(
            "Sources",
            Text(source_state, style=_health_style(source_state)),
            _source_detail(summary),
        )

    if workflows:
        workflow_state = str(workflows.get("state", "unknown") or "unknown")
        table.add_row(
            "Workflows",
            Text(workflow_state, style=_health_style(workflow_state)),
            _workflow_detail(workflows),
        )

    if groups:
        table.add_row(
            "Groups",
            Text(str(len(groups)), style="cyan"),
            _group_detail(groups),
        )

    return table


def _runtime_detail(runtime: Mapping[str, Any]) -> str:
    message = str(runtime.get("message", "") or "").strip()
    pid_count = _int_value(runtime.get("pid_count"))
    component_count = _int_value(runtime.get("component_count"))
    parts = []
    if message:
        parts.append(message)
    if component_count:
        parts.append(f"{component_count} component(s)")
    if pid_count:
        parts.append(f"{pid_count} PID(s)")
    return "; ".join(parts) or "-"


def _component_status(runtime: Mapping[str, Any]) -> Text:
    components = _mapping(runtime.get("components"))
    states = {
        str(_mapping(component).get("state", "unknown") or "unknown")
        for component in components.values()
    }
    if not components:
        return Text("unknown", style="dim")
    if states <= {"running", "ready"}:
        return Text("running", style="green")
    if "missing" in states or "dead" in states:
        return Text("attention", style="red")
    return Text("mixed", style="yellow")


def _component_detail(runtime: Mapping[str, Any]) -> str:
    components = _mapping(runtime.get("components"))
    if not components:
        return ""
    rows = []
    for name, raw_component in list(sorted(components.items()))[
        :COMPONENT_LIMIT
    ]:
        component = _mapping(raw_component)
        state = str(component.get("state", "unknown") or "unknown")
        pid = _int_value(component.get("pid"))
        pid_text = f" pid {pid}" if pid > 0 else ""
        readiness = str(component.get("readiness_state", "") or "")
        readiness_text = (
            f" readiness {readiness}"
            if readiness and readiness != state
            else ""
        )
        rows.append(f"{name} {state}{pid_text}{readiness_text}")
    remaining = len(components) - len(rows)
    if remaining > 0:
        rows.append(f"+{remaining} more")
    return "; ".join(rows)


def _disk_detail(disk: Mapping[str, Any]) -> str:
    free = _int_value(disk.get("free_bytes"))
    used = _int_value(disk.get("used_bytes"))
    percent = disk.get("percent_used")
    percent_text = (
        f"{float(percent):.1f}% used"
        if isinstance(percent, int | float)
        else "used unknown"
    )
    return (
        f"{_format_size(free)} free, {_format_size(used)} used, "
        f"{percent_text} (POSIX writes)"
    )


def _cache_detail(summary: Mapping[str, Any]) -> str:
    cache_count = _int_value(summary.get("cache_count"))
    cache_size = _int_value(summary.get("cache_size_bytes"))
    symbol_count = _int_value(summary.get("symbol_count"))
    symbols_with_cache = _int_value(summary.get("symbols_with_cache"))
    parts = [f"{cache_count} .data cache(s)", _format_size(cache_size)]
    if symbol_count:
        parts.append(f"{symbols_with_cache}/{symbol_count} symbols cached")
    return "; ".join(parts)


def _source_state(
    summary: Mapping[str, Any],
    cleanup: Mapping[str, Any],
) -> str:
    explicit = str(cleanup.get("state", "") or "")
    if explicit:
        return explicit
    return (
        "clean"
        if _int_value(summary.get("source_artifact_count")) == 0
        else "dirty"
    )


def _source_detail(summary: Mapping[str, Any]) -> str:
    count = _int_value(summary.get("source_artifact_count"))
    size = _int_value(summary.get("source_artifact_size_bytes"))
    return (
        f"{count} transient ZIP/CSV/XLS/XLSX artifact(s), "
        f"{_format_size(size)}"
    )


def _workflow_detail(workflows: Mapping[str, Any]) -> str:
    active = _int_value(workflows.get("active_count"))
    jobs = _int_value(workflows.get("job_count"))
    return f"active={active}, jobs={jobs}"


def _group_detail(groups: list[dict[str, Any]]) -> str:
    rows = []
    for group in groups[:GROUP_LIMIT]:
        name = str(group.get("group", "") or "-")
        status = str(group.get("status", "unknown") or "unknown")
        cached = _int_value(group.get("symbols_with_cache"))
        expected = _int_value(group.get("expected_symbol_count"))
        source_count = _int_value(group.get("source_artifact_count"))
        rows.append(
            f"{name}: {status}, {cached}/{expected} cached, "
            f"sources={source_count}"
        )
    remaining = len(groups) - len(rows)
    if remaining > 0:
        rows.append(f"+{remaining} more")
    return "; ".join(rows)


def _stage_table(progress: JobProgressSnapshot) -> Table:
    table = Table(
        title="Plan",
        box=box.SIMPLE_HEAD,
        expand=True,
        show_lines=False,
    )
    table.add_column("Stage", overflow="fold")
    table.add_column("State", justify="right")
    completed = set(progress.completed_stages)
    rows = list(progress.planned_children[:STAGE_LIMIT])
    for stage in rows:
        if stage in completed:
            state = Text("done", style="green")
        elif stage == progress.current_stage:
            state = Text("active", style="cyan")
        else:
            state = Text("queued", style="dim")
        table.add_row(_stage_label(stage), state)
    remaining = len(progress.planned_children) - len(rows)
    if remaining > 0:
        table.add_row(f"... {remaining} more", Text("queued", style="dim"))
    return table


def _events_table(events: tuple[StatusEvent, ...]) -> Table:
    table = Table(
        title="Recent Events",
        box=box.SIMPLE_HEAD,
        expand=True,
        show_lines=False,
    )
    table.add_column("Time", no_wrap=True)
    table.add_column("Stage", overflow="fold")
    table.add_column("Status", no_wrap=True)
    table.add_column("Message", overflow="fold")
    for event in events[-RECENT_EVENT_LIMIT:]:
        table.add_row(
            _clock_time(event.timestamp_utc),
            _stage_label(event.stage) or "-",
            Text(event.status.value, style=_status_style(event.status)),
            _event_message(event),
        )
    return table


def _artifacts_table(artifacts: tuple[ArtifactRef, ...]) -> Table:
    table = Table(
        title="Artifacts",
        box=box.SIMPLE_HEAD,
        expand=True,
        show_lines=False,
    )
    table.add_column("Kind", no_wrap=True)
    table.add_column("Path", overflow="fold")
    table.add_column("Size", justify="right", no_wrap=True)
    table.add_column("SHA256", no_wrap=True)
    rows = artifacts[:ARTIFACT_LIMIT]
    for artifact in rows:
        table.add_row(
            artifact.kind or "-",
            artifact.path or "-",
            _format_size(artifact.size_bytes),
            artifact.sha256[:12] if artifact.sha256 else "-",
        )
    remaining = len(artifacts) - len(rows)
    if remaining > 0:
        table.add_row("...", f"{remaining} more", "", "")
    return table


def _subtitle(snapshot: OrchestrationJobSnapshot) -> str:
    if snapshot.run_id:
        return f"run {snapshot.run_id}"
    if snapshot.namespace:
        return f"namespace {snapshot.namespace}"
    return "Temporal orchestration"


def _stage_label(stage: str) -> str:
    return STAGE_LABELS.get(stage, stage)


def _event_message(event: StatusEvent) -> str:
    if event.message == f"{event.stage} completed.":
        return f"{_stage_label(event.stage)} completed."
    return event.message or "-"


def _updated_at(snapshot: OrchestrationJobSnapshot) -> str:
    progress = snapshot.progress
    if progress is not None and progress.updated_at_utc:
        return str(progress.updated_at_utc)
    return str(snapshot.updated_at_utc or "-")


def _clock_time(value: str) -> str:
    if "T" not in value:
        return value or "-"
    time_part = value.split("T", 1)[1]
    return time_part.replace("Z", "").split(".", 1)[0]


def _format_rate(rate: float, unit: str) -> str:
    if rate <= 0:
        return "waiting"
    return f"{rate:.2f} {unit}/s"


def _format_eta(progress: JobProgressSnapshot) -> str:
    if progress.rate_per_second <= 0 or progress.total_children <= 0:
        return ""
    remaining = max(
        0.0,
        float(progress.total_children - progress.completed_children),
    )
    if remaining <= 0:
        return "complete"
    return _format_duration(remaining / progress.rate_per_second)


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes, seconds = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def _format_number(value: float | int) -> str:
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:.1f}"


def _format_size(value: int | None) -> str:
    if value is None:
        return "-"
    units = ("B", "KB", "MB", "GB")
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"


def _status_style(
    status: WorkStatus,
    lifecycle: JobLifecycle = JobLifecycle.UNKNOWN,
) -> str:
    if status == WorkStatus.FAILED or lifecycle == JobLifecycle.FAILED:
        return "red"
    if status == WorkStatus.CANCELLED or lifecycle == JobLifecycle.CANCELLED:
        return "yellow"
    if status == WorkStatus.COMPLETED or lifecycle == JobLifecycle.SUCCEEDED:
        return "green"
    if lifecycle in {JobLifecycle.SUBMITTED, JobLifecycle.UNKNOWN}:
        return "blue"
    return "cyan"


def _health_style(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {
        "ok",
        "running",
        "ready",
        "clean",
        "cache-ready",
        "completed",
        "succeeded",
    }:
        return "green"
    if normalized in {
        "warning",
        "dirty",
        "partial-cache",
        "mixed",
        "stop_pending",
        "stopped",
    }:
        return "yellow"
    if normalized in {
        "failed",
        "fail",
        "error",
        "stale",
        "missing",
        "dead",
        "unavailable",
        "attention",
    }:
        return "red"
    return "cyan"


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list | tuple):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
