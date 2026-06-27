"""End-user cleanup commands for local data artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_cleanup_argv,
)
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
        choices=("sources", "transient-sources"),
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
    if args.cleanup_command not in {"sources", "transient-sources"}:
        parser.error(f"unsupported cleanup command: {args.cleanup_command}")

    result = cleanup_transient_source_artifacts(
        args.data_directory,
        apply=args.apply,
    )
    _write_result(result, as_json=args.json)
    return 1 if result.errors else 0


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
