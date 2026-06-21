"""Command-line interface for Temporal sidecar lifecycle operations."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from histdatacom.sidecar.resources import SidecarResourceError
from histdatacom.sidecar.supervisor import (
    DEFAULT_STARTUP_TIMEOUT_SECONDS,
    DEFAULT_STOP_TIMEOUT_SECONDS,
    SidecarPaths,
    SidecarStatus,
    SidecarSupervisor,
    default_sidecar_state_dir,
)


def _add_common_args(
    parser: argparse.ArgumentParser,
    *,
    include_defaults: bool,
) -> None:
    """Add common sidecar options to a parser or subparser."""
    state_dir_default = (
        str(default_sidecar_state_dir())
        if include_defaults
        else argparse.SUPPRESS
    )
    json_default = False if include_defaults else argparse.SUPPRESS
    parser.add_argument(
        "--state-dir",
        default=state_dir_default,
        help="directory for sidecar PID, lock, and log files",
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


def _supervisor(state_dir: str) -> SidecarSupervisor:
    """Create a supervisor for CLI arguments."""
    return SidecarSupervisor(SidecarPaths.from_state_dir(Path(state_dir)))


def _write_payload(payload: dict, *, as_json: bool) -> None:
    """Write a CLI payload."""
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return
    print(f"{payload['state']}: {payload['message']}")  # noqa:T201
    if payload.get("pids"):
        print(f"pids: {payload['pids']}")  # noqa:T201
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
    supervisor = _supervisor(args.state_dir)

    try:
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
                print(f"state_dir: {status['state_dir']}")  # noqa:T201
            return 0
        parser.error(f"unsupported sidecar command: {args.command}")
    except (RuntimeError, SidecarResourceError, OSError) as err:
        if args.json:
            print(
                json.dumps(
                    {
                        "state": "error",
                        "message": str(err),
                        "state_dir": str(args.state_dir),
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
