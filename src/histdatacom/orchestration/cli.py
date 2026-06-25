"""Command-line interface for Temporal orchestration runtime operations."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_jobs_argv,
    configured_runtime_argv,
)
from histdatacom.runtime_contracts import RunRequest
from histdatacom.orchestration.performance import (
    DEFAULT_INFLUX_WORKERS,
    DEFAULT_NETWORK_MULTIPLIER,
    DEFAULT_ORCHESTRATION_WORKERS,
)
from histdatacom.orchestration.client import (
    cancel_job_sync,
    get_job_result_sync,
    inspect_job_status_sync,
    list_job_statuses_sync,
    resume_job_sync,
    resolve_orchestration_worker_config,
    retry_job_sync,
    submit_control_job_sync,
)
from histdatacom.orchestration.control import CONTROL_SCHEMA_VERSION
from histdatacom.orchestration.maintenance import (
    OrchestrationRetentionPolicy,
    run_orchestration_maintenance,
)
from histdatacom.orchestration.queues import (
    DEFAULT_TASK_QUEUE_PREFIX,
    DEFAULT_TEMPORAL_NAMESPACE,
    OrchestrationWorkerConfig,
)
from histdatacom.orchestration.resources import OrchestrationResourceError
from histdatacom.orchestration.runtime import (
    PortAllocationError,
    OrchestrationPaths,
    build_orchestration_runtime_policy,
    default_orchestration_runtime_home,
    default_orchestration_workspace,
)
from histdatacom.orchestration.supervisor import (
    DEFAULT_STARTUP_TIMEOUT_SECONDS,
    DEFAULT_STOP_TIMEOUT_SECONDS,
    OrchestrationStatus,
    OrchestrationSupervisor,
)


def _add_common_args(
    parser: argparse.ArgumentParser,
    *,
    include_defaults: bool,
) -> None:
    """Add common runtime options to a parser or subparser."""
    workspace_default = (
        str(default_orchestration_workspace())
        if include_defaults
        else argparse.SUPPRESS
    )
    runtime_home_default = (
        str(default_orchestration_runtime_home())
        if include_defaults
        else argparse.SUPPRESS
    )
    state_dir_default = None if include_defaults else argparse.SUPPRESS
    json_default = False if include_defaults else argparse.SUPPRESS
    parser.add_argument(
        "--workspace",
        default=workspace_default,
        help="workspace path used to scope orchestration runtime state",
    )
    parser.add_argument(
        "--runtime-home",
        default=runtime_home_default,
        help="base directory for per-workspace orchestration runtime state",
    )
    parser.add_argument(
        "--state-dir",
        default=state_dir_default,
        help="explicit state directory override for tests or manual recovery",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=json_default,
        help="emit machine-readable JSON",
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the orchestration runtime lifecycle argument parser."""
    parser = argparse.ArgumentParser(prog="histdatacom runtime")
    add_config_argument(parser)
    _add_common_args(parser, include_defaults=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser(
        "start", help="start the local orchestration runtime"
    )
    _add_common_args(start, include_defaults=False)
    start.add_argument(
        "--executable",
        help="explicit Temporal executable path for development/testing",
    )
    start.add_argument(
        "--startup-timeout",
        type=float,
        default=DEFAULT_STARTUP_TIMEOUT_SECONDS,
        help="seconds to wait for runtime processes to stay alive",
    )
    _add_worker_fleet_args(start)
    start.add_argument(
        "extra_args",
        nargs=argparse.REMAINDER,
        help="extra arguments appended after 'temporal server start-dev'",
    )

    status = subparsers.add_parser(
        "status", help="show orchestration runtime status"
    )
    _add_common_args(status, include_defaults=False)

    stop = subparsers.add_parser(
        "stop", help="stop the local orchestration runtime"
    )
    _add_common_args(stop, include_defaults=False)
    stop.add_argument(
        "--stop-timeout",
        type=float,
        default=DEFAULT_STOP_TIMEOUT_SECONDS,
        help="seconds to wait for process shutdown",
    )

    restart = subparsers.add_parser(
        "restart", help="restart the orchestration runtime"
    )
    _add_common_args(restart, include_defaults=False)
    restart.add_argument(
        "--executable",
        help="explicit Temporal executable path for development/testing",
    )
    restart.add_argument(
        "--startup-timeout",
        type=float,
        default=DEFAULT_STARTUP_TIMEOUT_SECONDS,
        help="seconds to wait for runtime processes to stay alive",
    )
    restart.add_argument(
        "--stop-timeout",
        type=float,
        default=DEFAULT_STOP_TIMEOUT_SECONDS,
        help="seconds to wait for process shutdown",
    )
    _add_worker_fleet_args(restart)
    restart.add_argument(
        "extra_args",
        nargs=argparse.REMAINDER,
        help="extra arguments appended after 'temporal server start-dev'",
    )

    doctor = subparsers.add_parser(
        "doctor", help="show orchestration runtime diagnostics"
    )
    _add_common_args(doctor, include_defaults=False)

    maintenance = subparsers.add_parser(
        "maintenance",
        aliases=("cleanup",),
        help="prune orchestration runtime logs and status metadata",
    )
    _add_common_args(maintenance, include_defaults=False)
    _add_maintenance_args(maintenance)

    jobs = subparsers.add_parser(
        "jobs",
        help="submit, inspect, and control jobs using orchestration routing",
    )
    _add_common_args(jobs, include_defaults=False)
    _add_jobs_args(jobs)
    return parser


def build_jobs_parser() -> argparse.ArgumentParser:
    """Build the first-class orchestration jobs argument parser."""
    parser = argparse.ArgumentParser(prog="histdatacom jobs")
    add_config_argument(parser)
    _add_common_args(parser, include_defaults=True)
    _add_jobs_args(parser)
    return parser


def _add_jobs_args(parser: argparse.ArgumentParser) -> None:
    """Add orchestration job command arguments to a parser."""
    parser.add_argument(
        "--offline",
        action="store_true",
        help="read persisted local job snapshots without querying Temporal",
    )
    job_subparsers = parser.add_subparsers(
        dest="jobs_command",
        required=True,
    )

    submit = job_subparsers.add_parser(
        "submit",
        help="submit a serialized RunRequest JSON payload",
    )
    submit.add_argument(
        "--request-json",
        required=True,
        help="path to a RunRequest JSON payload, or '-' for stdin",
    )
    submit.add_argument(
        "--start",
        action="store_true",
        help="start the runtime if it is not already running",
    )
    submit.add_argument(
        "--submit-only",
        action="store_true",
        help="return after submission instead of waiting for the result",
    )
    _add_job_command_common_args(submit, include_offline=False)

    list_jobs = job_subparsers.add_parser(
        "list",
        help="list known HistData orchestration jobs",
    )
    list_jobs.add_argument(
        "--query",
        default="",
        help="Temporal workflow list query override",
    )
    list_jobs.add_argument(
        "--limit",
        type=int,
        default=None,
        help="maximum stored jobs to return in offline/fallback mode",
    )
    _add_job_command_common_args(list_jobs, include_offline=True)

    for command, help_text in (
        ("inspect", "inspect one orchestration job"),
        ("progress", "show one job's progress view"),
        ("logs", "show one job's event/log view"),
        ("artifacts", "show one job's artifact view"),
        ("result", "show one job's result payload"),
        ("cancel", "request job cancellation"),
        ("retry", "start a deterministic retry replacement job"),
        ("resume", "start a deterministic resume replacement job"),
    ):
        job_parser = job_subparsers.add_parser(command, help=help_text)
        _add_job_identity_args(job_parser)
        _add_job_command_common_args(job_parser, include_offline=True)
        if command in {"cancel", "retry", "resume"}:
            job_parser.add_argument(
                "--reason",
                default="",
                help="operator-visible reason for the control request",
            )
        if command in {"retry", "resume"}:
            job_parser.add_argument(
                "--recompute-complete",
                action="store_true",
                help=(
                    "mark the replacement run to recompute completed artifacts "
                    "instead of reusing them"
                ),
            )


def _supervisor(args: argparse.Namespace) -> OrchestrationSupervisor:
    """Create a supervisor for CLI arguments."""
    paths = (
        OrchestrationPaths.from_state_dir(args.state_dir)
        if args.state_dir
        else None
    )
    runtime_policy = build_orchestration_runtime_policy(
        workspace=args.workspace,
        runtime_home=args.runtime_home,
        paths=paths,
    )
    return OrchestrationSupervisor(
        runtime_policy=runtime_policy,
        namespace=getattr(args, "namespace", DEFAULT_TEMPORAL_NAMESPACE),
        task_queue_prefix=getattr(
            args,
            "task_queue_prefix",
            DEFAULT_TASK_QUEUE_PREFIX,
        ),
        cpu_utilization=getattr(args, "cpu_utilization", "medium"),
        network_multiplier=getattr(
            args,
            "network_multiplier",
            DEFAULT_NETWORK_MULTIPLIER,
        ),
        orchestration_workers=getattr(
            args,
            "orchestration_workers",
            DEFAULT_ORCHESTRATION_WORKERS,
        ),
        influx_workers=getattr(
            args,
            "influx_workers",
            DEFAULT_INFLUX_WORKERS,
        ),
    )


def _write_payload(payload: dict, *, as_json: bool) -> None:
    """Write a CLI payload."""
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return
    print(f"{payload['state']}: {payload['message']}")  # noqa:T201
    if payload.get("pids"):
        print(f"pids: {payload['pids']}")  # noqa:T201
    if payload.get("components"):
        print(f"components: {payload['components']}")  # noqa:T201
    if payload.get("ports"):
        ports = payload["ports"]
        print(  # noqa:T201
            "ports: "
            f"{ports.get('bind_ip')}:{ports.get('grpc')} "
            f"ui={ports.get('ui')}"
        )
    print(f"state_dir: {payload['state_dir']}")  # noqa:T201


def _status_exit_code(status: OrchestrationStatus) -> int:
    """Return shell exit code for runtime status."""
    return 0 if status.state in {"running", "stopped"} else 1


def _extra_args(args: Sequence[str]) -> tuple[str, ...]:
    """Normalize argparse remainder values for Temporal passthrough args."""
    if args and args[0] == "--":
        return tuple(args[1:])
    return tuple(args)


def _add_worker_fleet_args(parser: argparse.ArgumentParser) -> None:
    """Add worker fleet options for supervised runtime lifecycle commands."""
    parser.add_argument(
        "--namespace",
        default=DEFAULT_TEMPORAL_NAMESPACE,
        help="Temporal namespace used by the local runtime worker fleet",
    )
    parser.add_argument(
        "--task-queue-prefix",
        default=DEFAULT_TASK_QUEUE_PREFIX,
        help="prefix for workspace-scoped Temporal task queues",
    )
    parser.add_argument(
        "--cpu-utilization",
        default="medium",
        help=(
            "CPU policy used to derive runtime worker concurrency "
            "(low, medium, high, or percent 1-200)"
        ),
    )
    parser.add_argument(
        "--network-multiplier",
        type=int,
        default=DEFAULT_NETWORK_MULTIPLIER,
        help="network lane multiplier applied to the CPU worker count",
    )
    parser.add_argument(
        "--orchestration-workers",
        type=int,
        default=DEFAULT_ORCHESTRATION_WORKERS,
        help="max concurrent orchestration lane activities",
    )
    parser.add_argument(
        "--influx-workers",
        type=int,
        default=DEFAULT_INFLUX_WORKERS,
        help="max concurrent Influx lane activities",
    )


def _add_job_identity_args(parser: argparse.ArgumentParser) -> None:
    """Add common workflow identity arguments for job commands."""
    parser.add_argument("workflow_id", help="Temporal workflow/job ID")
    parser.add_argument(
        "--run-id",
        default="",
        help="Temporal run ID when targeting a specific run",
    )


def _add_job_command_common_args(
    parser: argparse.ArgumentParser,
    *,
    include_offline: bool,
) -> None:
    """Accept shared jobs options after a job subcommand."""
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="emit machine-readable JSON",
    )
    if include_offline:
        parser.add_argument(
            "--offline",
            action="store_true",
            default=argparse.SUPPRESS,
            help="read persisted local job snapshots without querying Temporal",
        )


def _add_maintenance_args(parser: argparse.ArgumentParser) -> None:
    """Add retention options for runtime maintenance."""
    defaults = OrchestrationRetentionPolicy()
    parser.add_argument(
        "--allow-running",
        action="store_true",
        help="allow cleanup while the runtime is running",
    )
    parser.add_argument(
        "--max-log-bytes",
        type=_non_negative_int,
        default=defaults.max_log_bytes,
        help="maximum active bytes per log before rotation",
    )
    parser.add_argument(
        "--max-rotated-logs",
        type=_non_negative_int,
        default=defaults.max_rotated_logs,
        help="number of rotated log files to retain per active log",
    )
    parser.add_argument(
        "--max-temporal-sqlite-bytes",
        type=_non_negative_int,
        default=defaults.max_temporal_sqlite_bytes,
        help="warning threshold for Temporal SQLite history bytes",
    )
    parser.add_argument(
        "--max-job-snapshots",
        type=_non_negative_int,
        default=defaults.max_job_snapshots,
        help="maximum durable job snapshots to retain",
    )
    parser.add_argument(
        "--max-status-events-per-owner",
        type=_non_negative_int,
        default=defaults.max_status_events_per_owner,
        help="maximum status events to retain per job or work item",
    )
    parser.add_argument(
        "--max-stage-results-per-work-item",
        type=_non_negative_int,
        default=defaults.max_stage_results_per_work_item,
        help="maximum stage results to retain per work item",
    )
    parser.add_argument(
        "--max-artifacts-per-owner",
        type=_non_negative_int,
        default=defaults.max_artifacts_per_owner,
        help="maximum artifact references to retain per job or work item",
    )
    parser.add_argument(
        "--max-dataset-plans-per-request",
        type=_non_negative_int,
        default=defaults.max_dataset_plans_per_request,
        help="maximum spilled dataset plans to retain per request",
    )


def _non_negative_int(value: str) -> int:
    """Parse an argparse integer that cannot be negative."""
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError(
            "value must be greater than or equal to 0"
        )
    return parsed


def _load_run_request(path: str) -> RunRequest:
    """Load a RunRequest from JSON."""
    payload = sys.stdin.read() if path == "-" else Path(path).read_text()
    return RunRequest.from_dict(json.loads(payload))


def _worker_config(args: argparse.Namespace) -> OrchestrationWorkerConfig:
    """Create an orchestration worker config from CLI arguments."""
    return resolve_orchestration_worker_config(
        supervisor=_supervisor(args),
    )


def _write_control_payload(payload: dict, *, as_json: bool) -> None:
    """Write a local-control API payload for CLI callers."""
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return
    if "jobs" in payload:
        print(f"jobs: {len(payload['jobs'])}")  # noqa:T201
        return
    workflow_id = payload.get("workflow_id") or payload.get("job_id", "")
    lifecycle = payload.get("lifecycle", "")
    status = payload.get("status", "")
    print(f"{workflow_id}: {lifecycle} ({status})")  # noqa:T201


def _retention_policy(args: argparse.Namespace) -> OrchestrationRetentionPolicy:
    """Create an orchestration retention policy from CLI arguments."""
    return OrchestrationRetentionPolicy(
        max_log_bytes=args.max_log_bytes,
        max_rotated_logs=args.max_rotated_logs,
        max_temporal_sqlite_bytes=args.max_temporal_sqlite_bytes,
        max_job_snapshots=args.max_job_snapshots,
        max_status_events_per_owner=args.max_status_events_per_owner,
        max_stage_results_per_work_item=args.max_stage_results_per_work_item,
        max_artifacts_per_owner=args.max_artifacts_per_owner,
        max_dataset_plans_per_request=args.max_dataset_plans_per_request,
    )


def _write_maintenance_payload(payload: dict, *, as_json: bool) -> None:
    """Write a runtime maintenance payload."""
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return
    print(f"{payload['state']}: {payload['message']}")  # noqa:T201
    log_actions = [
        item.get("action", "")
        for item in payload.get("logs", [])
        if item.get("action")
    ]
    rows_deleted = payload.get("status_store", {}).get("rows_deleted", {})
    print(f"logs: {log_actions}")  # noqa:T201
    print(f"rows_deleted: {rows_deleted}")  # noqa:T201
    print(f"state_dir: {payload['paths']['state_dir']}")  # noqa:T201


def _run_jobs_command(args: argparse.Namespace) -> int:
    """Run orchestration job control commands."""
    supervisor = _supervisor(args)
    config = (
        None
        if args.jobs_command == "submit" and args.start
        else _worker_config(args)
    )
    if args.jobs_command == "submit":
        snapshot = submit_control_job_sync(
            _load_run_request(args.request_json),
            config=config,
            supervisor=supervisor,
            start_if_needed=args.start,
            wait_for_result=not args.submit_only,
        )
        _write_control_payload(snapshot.to_dict(), as_json=args.json)
        return 0
    if args.jobs_command == "list":
        jobs = list_job_statuses_sync(
            config=config,
            supervisor=supervisor,
            query=args.query,
            offline=args.offline,
            limit=args.limit,
        )
        _write_control_payload(jobs.to_dict(), as_json=args.json)
        return 0

    identity_kwargs = {
        "run_id": args.run_id,
        "config": config,
        "supervisor": supervisor,
        "offline": args.offline,
    }
    if args.jobs_command == "inspect":
        snapshot = inspect_job_status_sync(args.workflow_id, **identity_kwargs)
        _write_control_payload(snapshot.to_dict(), as_json=args.json)
        return 0
    if args.jobs_command == "progress":
        snapshot = inspect_job_status_sync(args.workflow_id, **identity_kwargs)
        payload = {
            "schema_version": CONTROL_SCHEMA_VERSION,
            "workflow_id": snapshot.workflow_id,
            "progress": (
                snapshot.progress.to_dict()
                if snapshot.progress is not None
                else None
            ),
        }
        _write_control_payload(payload, as_json=args.json)
        return 0
    if args.jobs_command == "logs":
        snapshot = inspect_job_status_sync(args.workflow_id, **identity_kwargs)
        payload = {
            "schema_version": CONTROL_SCHEMA_VERSION,
            "workflow_id": snapshot.workflow_id,
            "logs": [entry.to_dict() for entry in snapshot.logs],
        }
        _write_control_payload(payload, as_json=args.json)
        return 0
    if args.jobs_command == "artifacts":
        snapshot = inspect_job_status_sync(args.workflow_id, **identity_kwargs)
        payload = {
            "schema_version": CONTROL_SCHEMA_VERSION,
            "workflow_id": snapshot.workflow_id,
            "artifacts": [
                artifact.to_dict() for artifact in snapshot.artifacts
            ],
        }
        _write_control_payload(payload, as_json=args.json)
        return 0
    if args.jobs_command == "result":
        snapshot = get_job_result_sync(args.workflow_id, **identity_kwargs)
        payload = {
            "schema_version": CONTROL_SCHEMA_VERSION,
            "workflow_id": snapshot.workflow_id,
            "result": snapshot.result,
        }
        _write_control_payload(payload, as_json=args.json)
        return 0
    if args.jobs_command == "cancel":
        snapshot = cancel_job_sync(
            args.workflow_id,
            reason=args.reason,
            **identity_kwargs,
        )
        _write_control_payload(snapshot.to_dict(), as_json=args.json)
        return 0
    if args.jobs_command == "retry":
        snapshot = retry_job_sync(
            args.workflow_id,
            reason=args.reason,
            reuse_completed_artifacts=not args.recompute_complete,
            **identity_kwargs,
        )
        _write_control_payload(snapshot.to_dict(), as_json=args.json)
        return 0
    if args.jobs_command == "resume":
        snapshot = resume_job_sync(
            args.workflow_id,
            reason=args.reason,
            reuse_completed_artifacts=not args.recompute_complete,
            **identity_kwargs,
        )
        _write_control_payload(snapshot.to_dict(), as_json=args.json)
        return 0
    raise ValueError(f"unsupported jobs command: {args.jobs_command}")


def main(argv: Sequence[str] | None = None) -> int:
    """Run orchestration runtime lifecycle commands."""
    parser = build_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_runtime_argv(raw_argv))
    except CliConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)  # noqa:T201
        return 1

    try:
        supervisor = _supervisor(args)
        if args.command == "start":
            status = supervisor.start(
                executable=args.executable,
                extra_args=_extra_args(args.extra_args),
                startup_timeout=args.startup_timeout,
            )
            _write_payload(status.to_dict(), as_json=args.json)
            return 0
        if args.command == "status":
            status = supervisor.status(repair=False)
            _write_payload(status.to_dict(), as_json=args.json)
            return _status_exit_code(status)
        if args.command == "stop":
            status = supervisor.stop(stop_timeout=args.stop_timeout)
            _write_payload(status.to_dict(), as_json=args.json)
            return _status_exit_code(status)
        if args.command == "restart":
            status = supervisor.restart(
                executable=args.executable,
                extra_args=_extra_args(args.extra_args),
                startup_timeout=args.startup_timeout,
                stop_timeout=args.stop_timeout,
            )
            _write_payload(status.to_dict(), as_json=args.json)
            return 0
        if args.command == "doctor":
            payload = supervisor.doctor()
            if args.json:
                print(
                    json.dumps(payload, indent=2, sort_keys=True)
                )  # noqa:T201
            else:
                status = payload["status"]
                print(f"{status['state']}: {status['message']}")  # noqa:T201
                print(payload["platform"]["message"])  # noqa:T201
                ports = payload["runtime_policy"]["ports"]
                print(  # noqa:T201
                    "ports: "
                    f"{ports['bind_ip']}:{ports['grpc']} ui={ports['ui']}"
                )
                print(f"state_dir: {status['state_dir']}")  # noqa:T201
            return 0
        if args.command in {"maintenance", "cleanup"}:
            status = supervisor.status(repair=False)
            result = run_orchestration_maintenance(
                supervisor.runtime_policy,
                _retention_policy(args),
                orchestration_state=status.state,
                allow_running=args.allow_running,
            )
            _write_maintenance_payload(result.to_dict(), as_json=args.json)
            return 0 if result.state == "completed" else 1
        if args.command == "jobs":
            return _run_jobs_command(args)
        parser.error(f"unsupported runtime command: {args.command}")
    except (
        RuntimeError,
        OrchestrationResourceError,
        PortAllocationError,
        OSError,
    ) as err:
        if args.json:
            print(
                json.dumps(
                    {
                        "state": "error",
                        "message": str(err),
                        "state_dir": str(args.state_dir or ""),
                        "workspace": str(args.workspace),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )  # noqa:T201
        else:
            print(f"error: {err}", file=sys.stderr)  # noqa:T201
        return 1


def jobs_main(argv: Sequence[str] | None = None) -> int:
    """Run first-class orchestration job telemetry commands."""
    parser = build_jobs_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_jobs_argv(raw_argv))
    except CliConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)  # noqa:T201
        return 1

    try:
        return _run_jobs_command(args)
    except (
        RuntimeError,
        OrchestrationResourceError,
        PortAllocationError,
        OSError,
    ) as err:
        if args.json:
            print(
                json.dumps(
                    {
                        "state": "error",
                        "message": str(err),
                        "state_dir": str(args.state_dir or ""),
                        "workspace": str(args.workspace),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )  # noqa:T201
        else:
            print(f"error: {err}", file=sys.stderr)  # noqa:T201
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
