"""Rich terminal renderers for orchestration job progress."""

from __future__ import annotations

import time
from collections.abc import Callable

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

    def __init__(self, *, console: Console | None = None) -> None:
        self.console = console or Console()
        self._live: Live | None = None

    def __enter__(self) -> "LiveJobProgressRenderer":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def update(self, snapshot: OrchestrationJobSnapshot) -> None:
        """Render the latest snapshot, starting the live display if needed."""
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
        progress.current_stage if progress is not None else ""
    ) or "waiting for first progress event"
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
    description = current_stage or status.value.lower()
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
        table.add_row(stage, state)
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
            event.stage or "-",
            Text(event.status.value, style=_status_style(event.status)),
            event.message or "-",
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
