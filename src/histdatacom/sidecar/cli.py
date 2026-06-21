"""Command-line interface for Temporal sidecar lifecycle operations."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from histdatacom.sidecar.resources import SidecarResourceError
from histdatacom.sidecar.runtime import (
    PortAllocationError,
    SidecarPaths,
    build_sidecar_runtime_policy,
    default_sidecar_runtime_home,
    default_sidecar_workspace,
)
from histdatacom.sidecar.supervisor import (
    DEFAULT_STARTUP_TIMEOUT_SECONDS,
    DEFAULT_STOP_TIMEOUT_SECONDS,
    SidecarStatus,
    SidecarSupervisor,
)


def _add_common_args(
    parser: argparse.ArgumentParser,
    *,
    include_defaults: bool,
) -> None:
    """Add common sidecar options to a parser or subparser."""
    workspace_default = (
        str(default_sidecar_workspace())
        if include_defaults
        else argparse.SUPPRESS
    )
    runtime_home_default = (
        str(default_sidecar_runtime_home())
        if include_defaults
        else argparse.SUPPRESS
    )
    state_dir_default = None if include_defaults else argparse.SUPPRESS
    json_default = False if include_defaults else argparse.SUPPRESS
    parser.add_argument(
        "--workspace",
        default=workspace_default,
        help="workspace path used to scope sidecar runtime state",
    )
    parser.add_argument(
        "--runtime-home",
        default=runtime_home_default,
        help="base directory for per-workspace sidecar runtime state",
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
    """Build the sidecar lifecycle argument parser."""
    parser = argparse.ArgumentParser(prog="histdatacom sidecar")
    _add_common_args(parser, include_defaults=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="start the local sidecar")
    _add_common_args(start, include_defaults=False)
    start.add_argument(
        "--executable",
        help="explicit Temporal executable path for development/testing",
    )
    start.add_argument(
        "--startup-timeout",
        type=float,
        default=DEFAULT_STARTUP_TIMEOUT_SECONDS,
        help="seconds to wait for the sidecar process to stay alive",
    )
    start.add_argument(
        "extra_args",
        nargs=argparse.REMAINDER,
        help="extra arguments appended after 'temporal server start-dev'",
    )

    status = subparsers.add_parser("status", help="show sidecar status")
    _add_common_args(status, include_defaults=False)

    stop = subparsers.add_parser("stop", help="stop the local sidecar")
    _add_common_args(stop, include_defaults=False)
    stop.add_argument(
        "--stop-timeout",
        type=float,
        default=DEFAULT_STOP_TIMEOUT_SECONDS,
        help="seconds to wait for process shutdown",
    )

    restart = subparsers.add_parser("restart", help="restart the sidecar")
    _add_common_args(restart, include_defaults=False)
    restart.add_argument(
        "--executable",
        help="explicit Temporal executable path for development/testing",
    )
    restart.add_argument(
        "--startup-timeout",
        type=float,
        default=DEFAULT_STARTUP_TIMEOUT_SECONDS,
        help="seconds to wait for the sidecar process to stay alive",
    )
    restart.add_argument(
        "--stop-timeout",
        type=float,
        default=DEFAULT_STOP_TIMEOUT_SECONDS,
        help="seconds to wait for process shutdown",
    )
    restart.add_argument(
        "extra_args",
        nargs=argparse.REMAINDER,
        help="extra arguments appended after 'temporal server start-dev'",
    )

    doctor = subparsers.add_parser("doctor", help="show sidecar diagnostics")
    _add_common_args(doctor, include_defaults=False)
    return parser


def _supervisor(args: argparse.Namespace) -> SidecarSupervisor:
    """Create a supervisor for CLI arguments."""
    paths = (
        SidecarPaths.from_state_dir(args.state_dir) if args.state_dir else None
    )
    runtime_policy = build_sidecar_runtime_policy(
        workspace=args.workspace,
        runtime_home=args.runtime_home,
        paths=paths,
    )
    return SidecarSupervisor(runtime_policy=runtime_policy)


def _write_payload(payload: dict, *, as_json: bool) -> None:
    """Write a CLI payload."""
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return
    print(f"{payload['state']}: {payload['message']}")  # noqa:T201
    if payload.get("pids"):
        print(f"pids: {payload['pids']}")  # noqa:T201
    if payload.get("ports"):
        ports = payload["ports"]
        print(  # noqa:T201
            "ports: "
            f"{ports.get('bind_ip')}:{ports.get('grpc')} "
            f"ui={ports.get('ui')}"
        )
    print(f"state_dir: {payload['state_dir']}")  # noqa:T201


def _status_exit_code(status: SidecarStatus) -> int:
    """Return shell exit code for a sidecar status."""
    return 0 if status.state in {"running", "stopped"} else 1


def _extra_args(args: Sequence[str]) -> tuple[str, ...]:
    """Normalize argparse remainder values for Temporal passthrough args."""
    if args and args[0] == "--":
        return tuple(args[1:])
    return tuple(args)


def main(argv: Sequence[str] | None = None) -> int:
    """Run sidecar lifecycle commands."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

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
        parser.error(f"unsupported sidecar command: {args.command}")
    except (
        RuntimeError,
        SidecarResourceError,
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
