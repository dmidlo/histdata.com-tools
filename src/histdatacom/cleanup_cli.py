"""End-user cleanup commands for local data artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Sequence

from histdatacom.cache_status import (
    CacheRunStatusResult,
    collect_cache_run_status,
)
from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_cleanup_argv,
)
from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.orchestration.runtime import (
    OrchestrationPaths,
    OrchestrationRuntimePolicy,
    build_orchestration_runtime_policy,
)
from histdatacom.orchestration.supervisor import OrchestrationSupervisor
from histdatacom.source_cleanup import (
    SourceCleanupResult,
    cleanup_transient_source_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the cleanup command parser."""
    parser = argparse.ArgumentParser(prog="histdatacom cleanup")
    add_config_argument(parser)
    parser.add_argument(
        "cleanup_command",
        nargs="?",
        default="sources",
        choices=("sources", "transient-sources", "status"),
        help="cleanup operation to run",
    )
    parser.add_argument(
        "--data-directory",
        default="data",
        metavar="PATH",
        help="local data directory to scan",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="delete matched source artifacts; omit for a dry run",
    )
    parser.add_argument(
        "-p",
        "--pairs",
        nargs="+",
        default=(),
        metavar="PAIR",
        help="limit status to one or more symbols",
    )
    parser.add_argument(
        "--pair-groups",
        nargs="+",
        default=(),
        metavar="GROUP",
        help="limit status to shared instrument groups such as majors",
    )
    parser.add_argument(
        "-t",
        "--timeframes",
        nargs="+",
        default=(),
        metavar="TIMEFRAME",
        help="limit status to one or more HistData timeframes",
    )
    parser.add_argument(
        "-f",
        "--formats",
        nargs="+",
        default=(),
        metavar="FORMAT",
        help="limit status to one or more HistData formats",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="workspace path used to scope orchestration runtime state",
    )
    parser.add_argument(
        "--runtime-home",
        default=None,
        help="base directory for per-workspace orchestration runtime state",
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help="explicit runtime state directory override",
    )
    parser.add_argument(
        "--max-jobs",
        default=5,
        type=int,
        help="maximum workflow snapshots to include in status output",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable cleanup payload",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run local cleanup commands."""
    parser = build_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_cleanup_argv(raw_argv))
    except CliConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)  # noqa:T201
        return 1
    if args.cleanup_command == "status":
        try:
            status_result = _collect_status(args)
        except ValueError as exc:
            parser.error(str(exc))
        _write_status(status_result, as_json=args.json)
        return 1 if status_result.errors else 0

    if args.cleanup_command not in {"sources", "transient-sources"}:
        parser.error(f"unsupported cleanup command: {args.cleanup_command}")

    cleanup_result = cleanup_transient_source_artifacts(
        args.data_directory,
        apply=args.apply,
    )
    _write_result(cleanup_result, as_json=args.json)
    return 1 if cleanup_result.errors else 0


def _collect_status(args: argparse.Namespace) -> CacheRunStatusResult:
    runtime_policy = _runtime_policy(args)
    job_snapshots, workflow_store_path = _job_snapshots(runtime_policy)
    return collect_cache_run_status(
        args.data_directory,
        pairs=args.pairs,
        pair_groups=args.pair_groups,
        timeframes=args.timeframes,
        formats=args.formats,
        runtime=_runtime_status(runtime_policy),
        job_snapshots=job_snapshots,
        workflow_store_path=workflow_store_path,
        max_jobs=args.max_jobs,
    )


def _runtime_policy(args: argparse.Namespace) -> OrchestrationRuntimePolicy:
    paths = (
        OrchestrationPaths.from_state_dir(args.state_dir)
        if args.state_dir
        else None
    )
    return build_orchestration_runtime_policy(
        workspace=args.workspace,
        runtime_home=args.runtime_home,
        paths=paths,
    )


def _runtime_status(
    runtime_policy: OrchestrationRuntimePolicy,
) -> dict[str, Any]:
    try:
        status = OrchestrationSupervisor(runtime_policy=runtime_policy).status(
            repair=False
        )
    except Exception as exc:  # pragma: no cover - defensive CLI boundary
        return {
            "state": "unknown",
            "message": str(exc),
        }
    return dict(status.to_dict())


def _job_snapshots(
    runtime_policy: OrchestrationRuntimePolicy,
) -> tuple[tuple[dict[str, Any], ...] | None, str]:
    manifests_dir = getattr(getattr(runtime_policy, "paths"), "manifests_dir")
    store_path = ManifestStatusStore.path_for_root(manifests_dir)
    if not store_path.exists():
        return (), str(store_path)
    try:
        store = ManifestStatusStore(manifests_dir)
        return store.list_job_snapshots(), str(store_path)
    except Exception:  # pragma: no cover - defensive CLI boundary
        return None, str(store_path)


def _write_result(result: SourceCleanupResult, *, as_json: bool) -> None:
    if as_json:
        print(
            json.dumps(result.to_dict(), indent=2, sort_keys=True)
        )  # noqa:T201
        return

    payload = result.to_dict()
    action = "Would delete" if result.dry_run else "Deleted"
    target_count = (
        result.matched_count if result.dry_run else result.deleted_count
    )
    target_size = (
        result.matched_size_bytes
        if result.dry_run
        else result.deleted_size_bytes
    )
    print(  # noqa:T201
        f"{action} {target_count} transient source artifact(s) "
        f"({_format_bytes(target_size)}) under {payload['root']}."
    )
    print(".data cache files are preserved.")  # noqa:T201
    if result.dry_run and result.matched_count:
        print("Re-run with --apply to delete these files.")  # noqa:T201
    if result.errors:
        print(f"errors: {len(result.errors)}", file=sys.stderr)  # noqa:T201


def _write_status(result: CacheRunStatusResult, *, as_json: bool) -> None:
    payload = result.to_dict()
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return

    summary = payload["summary"]
    disk = payload["disk"]
    runtime = payload["runtime"]
    workflows = payload["workflows"]
    print(f"cache status: {payload['status']}")  # noqa:T201
    print(f"root: {payload['root']}")  # noqa:T201
    print(  # noqa:T201
        "caches: "
        f"{summary['cache_count']} .data file(s), "
        f"{_format_bytes(summary['cache_size_bytes'])}; "
        f"sources: {summary['source_artifact_count']} transient artifact(s), "
        f"{_format_bytes(summary['source_artifact_size_bytes'])}"
    )
    print(  # noqa:T201
        "runtime: "
        f"{runtime.get('state', 'unknown')} "
        f"({runtime.get('message', '') or 'no message'})"
    )
    print(  # noqa:T201
        "workflows: "
        f"{workflows.get('state', 'unknown')} "
        f"active={workflows.get('active_count', 0)} "
        f"jobs={workflows.get('job_count', 0)}"
    )
    if disk.get("state") == "ok":
        print(  # noqa:T201
            "disk: "
            f"{_format_bytes(int(disk['free_bytes']))} free, "
            f"{disk['percent_used']}% used at {disk['path']}"
        )
    else:
        print(f"disk: {disk.get('state', 'unknown')}")  # noqa:T201

    for group in payload["groups"]:
        print(  # noqa:T201
            f"group {group['group']}: {group['status']}, "
            f"{group['symbols_with_cache']}/"
            f"{group['expected_symbol_count']} symbols with cache, "
            f"caches={group['cache_count']}, "
            f"sources={group['source_artifact_count']}"
        )

    _write_symbol_status(payload["symbols"])

    for step in payload["next_steps"]:
        print(f"next: {step}")  # noqa:T201
    if result.errors:
        print(f"errors: {len(result.errors)}", file=sys.stderr)  # noqa:T201


def _write_symbol_status(symbols: list[dict[str, Any]]) -> None:
    if not symbols:
        return
    limit = 20
    for symbol in symbols[:limit]:
        raw_timeframes = symbol.get("timeframes")
        timeframes = (
            ",".join(str(item) for item in raw_timeframes)
            if isinstance(raw_timeframes, list)
            else "-"
        )
        print(  # noqa:T201
            f"symbol {symbol['symbol']}: {symbol['status']}, "
            f"caches={symbol['cache_count']}, "
            f"sources={symbol['source_artifact_count']}, "
            f"timeframes={timeframes or '-'}"
        )
    if len(symbols) > limit:
        print(  # noqa:T201
            f"symbols: {len(symbols) - limit} more hidden; use --json "
            "for the full scriptable payload."
        )


def _format_bytes(size_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size_bytes} B"


if __name__ == "__main__":
    raise SystemExit(main())
