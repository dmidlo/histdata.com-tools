"""Temporal worker construction and orchestration-internal worker CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from inspect import isawaitable
from typing import Any, Mapping, Sequence, cast

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_worker_argv,
)
from histdatacom.orchestration.client import (
    TEMPORAL_EXTRA_HINT,
    TemporalDependencyError,
    connect_temporal_client,
)
from histdatacom.orchestration.queues import (
    DEFAULT_TASK_QUEUE_PREFIX,
    DEFAULT_TEMPORAL_NAMESPACE,
    OrchestrationWorkerConfig,
    TaskQueueLane,
    build_orchestration_worker_config,
)
from histdatacom.orchestration.readiness import write_worker_readiness
from histdatacom.orchestration.runtime import (
    PortAllocationError,
    OrchestrationPaths,
    build_orchestration_runtime_policy,
    default_orchestration_runtime_home,
    default_orchestration_workspace,
)


def build_temporal_worker(
    client: Any,
    *,
    config: OrchestrationWorkerConfig | None = None,
    worker_class: Any | None = None,
    workflows: Sequence[Any] = (),
    activities: Sequence[Any] = (),
    **worker_options: Any,
) -> Any:
    """Build a Temporal worker from centralized orchestration configuration."""
    resolved_config = config or build_orchestration_worker_config()
    temporal_worker_class = worker_class or _load_temporal_worker_class()
    workflow_classes = (
        list(workflows) if workflows else list(default_workflows())
    )
    activity_functions = (
        list(activities) if activities else list(default_activities())
    )
    resolved_worker_options = {
        **resolved_config.worker_options,
        **worker_options,
    }
    if (
        activity_functions
        and "activity_executor" not in resolved_worker_options
    ):
        resolved_worker_options["activity_executor"] = _activity_executor(
            resolved_worker_options
        )
    return temporal_worker_class(
        client,
        task_queue=resolved_config.task_queue,
        workflows=workflow_classes,
        activities=activity_functions,
        **resolved_worker_options,
    )


async def run_temporal_worker(
    *,
    config: OrchestrationWorkerConfig | None = None,
    client: Any | None = None,
    client_class: Any | None = None,
    worker_class: Any | None = None,
    workflows: Sequence[Any] = (),
    activities: Sequence[Any] = (),
    **worker_options: Any,
) -> Any:
    """Connect to Temporal, build the configured worker, and run it."""
    resolved_config = config or build_orchestration_worker_config()
    temporal_client = client or await connect_temporal_client(
        config=resolved_config,
        client_class=client_class,
    )
    worker = build_temporal_worker(
        temporal_client,
        config=resolved_config,
        worker_class=worker_class,
        workflows=workflows,
        activities=activities,
        **worker_options,
    )
    run = getattr(worker, "run", None)
    if run is None:
        raise TypeError("Temporal worker object must define run()")
    run_result = run()
    if isawaitable(run_result):
        run_task = asyncio.ensure_future(run_result)
        await asyncio.sleep(0)
        if run_task.done():
            await run_task
            return worker
        write_worker_readiness(
            resolved_config,
            pid=os.getpid(),
            state="ready",
            message="Worker connected and entering run loop.",
        )
        await run_task
        return worker
    write_worker_readiness(
        resolved_config,
        pid=os.getpid(),
        state="ready",
        message="Worker connected and entering run loop.",
    )
    await _maybe_await(run_result)
    return worker


def build_parser() -> argparse.ArgumentParser:
    """Build the orchestration worker argument parser."""
    parser = argparse.ArgumentParser(prog="histdatacom-orchestration-worker")
    add_config_argument(parser)
    _add_common_args(parser, include_defaults=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    config = subparsers.add_parser(
        "config",
        help="show Temporal worker configuration",
    )
    _add_common_args(config, include_defaults=False)
    _add_worker_args(config)

    run = subparsers.add_parser(
        "run", help="run a Temporal orchestration worker"
    )
    _add_common_args(run, include_defaults=False)
    _add_worker_args(run)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the orchestration worker command-line interface."""
    parser = build_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_worker_argv(raw_argv))
    except CliConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)  # noqa:T201
        return 1

    try:
        config = _config_from_args(args)
        if args.command == "config":
            _write_config(config, as_json=args.json)
            return 0
        if args.command == "run":
            asyncio.run(run_temporal_worker(config=config))
            return 0
        parser.error(f"unsupported worker command: {args.command}")
    except KeyboardInterrupt:
        return 130
    except (
        RuntimeError,
        TemporalDependencyError,
        PortAllocationError,
        OSError,
        ValueError,
    ) as err:
        message = str(err)
        if isinstance(err, TemporalDependencyError):
            message = TEMPORAL_EXTRA_HINT
        if args.json:
            print(  # noqa:T201
                json.dumps(
                    {
                        "state": "error",
                        "message": message,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(f"error: {message}", file=sys.stderr)  # noqa:T201
        return 1


def _add_common_args(
    parser: argparse.ArgumentParser,
    *,
    include_defaults: bool,
) -> None:
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


def _add_worker_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--namespace",
        default=DEFAULT_TEMPORAL_NAMESPACE,
        help="Temporal namespace used by the local orchestration",
    )
    parser.add_argument(
        "--task-queue-prefix",
        default=DEFAULT_TASK_QUEUE_PREFIX,
        help="prefix for workspace-scoped Temporal task queues",
    )
    parser.add_argument(
        "--lane",
        choices=[lane.value for lane in TaskQueueLane],
        default=TaskQueueLane.ORCHESTRATION.value,
        help="worker task queue lane to run",
    )
    parser.add_argument(
        "--cpu-utilization",
        default="medium",
        help=(
            "CPU policy used to derive orchestration worker concurrency "
            "(low, medium, high, or percent 1-200)"
        ),
    )
    parser.add_argument(
        "--network-multiplier",
        type=int,
        default=3,
        help="network lane multiplier applied to the CPU worker count",
    )
    parser.add_argument(
        "--orchestration-workers",
        type=int,
        default=1,
        help="max concurrent orchestration lane activities",
    )
    parser.add_argument(
        "--influx-workers",
        type=int,
        default=1,
        help="max concurrent Influx lane activities",
    )
    parser.add_argument(
        "--max-concurrent-activities",
        type=int,
        default=None,
        help="explicit max concurrent activities override for this lane",
    )


def _config_from_args(args: argparse.Namespace) -> OrchestrationWorkerConfig:
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
    return build_orchestration_worker_config(
        runtime_policy=runtime_policy,
        namespace=args.namespace,
        task_queue_prefix=args.task_queue_prefix,
        lane=args.lane,
        cpu_utilization=args.cpu_utilization,
        network_multiplier=args.network_multiplier,
        orchestration_workers=args.orchestration_workers,
        influx_workers=args.influx_workers,
        concurrency_overrides=(
            {args.lane: args.max_concurrent_activities}
            if args.max_concurrent_activities is not None
            else None
        ),
    )


def _write_config(
    config: OrchestrationWorkerConfig,
    *,
    as_json: bool,
) -> None:
    payload = config.to_dict()
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return
    print(f"namespace: {payload['namespace']}")  # noqa:T201
    print(f"target_host: {payload['target_host']}")  # noqa:T201
    print(f"lane: {payload['lane']}")  # noqa:T201
    print(f"task_queue: {payload['task_queue']}")  # noqa:T201
    print(f"worker_options: {payload['worker_options']}")  # noqa:T201


def _load_temporal_worker_class() -> Any:
    try:
        return getattr(import_module("temporalio.worker"), "Worker")
    except ModuleNotFoundError as err:
        if (err.name or "").split(".")[0] == "temporalio":
            raise TemporalDependencyError(TEMPORAL_EXTRA_HINT) from err
        raise


def _activity_executor(worker_options: Mapping[str, Any]) -> ThreadPoolExecutor:
    """Build the default executor required for synchronous activities."""
    max_workers = int(worker_options.get("max_concurrent_activities", 1) or 1)
    return ThreadPoolExecutor(max_workers=max(1, max_workers))


def default_workflows() -> tuple[Any, ...]:
    """Return default orchestration workflow classes without importing activities."""
    from histdatacom.orchestration.workflows import DEFAULT_WORKFLOWS

    return cast(tuple[Any, ...], DEFAULT_WORKFLOWS)


def default_activities() -> tuple[Any, ...]:
    """Return default orchestration activity callables."""
    from histdatacom.orchestration.activities import (
        default_activities as defaults,
    )

    return cast(tuple[Any, ...], defaults())


async def _maybe_await(value: Any) -> Any:
    if isawaitable(value):
        return await value
    return value


if __name__ == "__main__":
    raise SystemExit(main())
