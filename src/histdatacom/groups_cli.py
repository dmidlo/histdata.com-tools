"""End-user instrument group discovery commands."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_groups_argv,
)
from histdatacom.group_catalog import (
    format_group_catalog,
    format_group_detail,
    list_group_catalog,
    show_group_catalog,
)
from histdatacom.fx_enums import normalize_pair_group, pair_group_names


def build_parser() -> argparse.ArgumentParser:
    """Build the group discovery parser."""
    parser = argparse.ArgumentParser(prog="histdatacom groups")
    add_config_argument(parser)
    subparsers = parser.add_subparsers(dest="groups_command", required=True)

    list_parser = subparsers.add_parser(
        "list",
        help="list supported instrument groups",
    )
    list_parser.add_argument(
        "--triangles",
        action="store_true",
        help="list individual major triangle groups",
    )
    list_parser.add_argument(
        "--all",
        action="store_true",
        help="list broad baskets and individual triangle groups",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable group catalog",
    )

    show_parser = subparsers.add_parser(
        "show",
        help="show one instrument group and its expanded symbols",
    )
    show_parser.add_argument(
        "group",
        type=normalize_pair_group,
        choices=pair_group_names(),
        metavar="GROUP",
        help="instrument group name to inspect",
    )
    show_parser.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable group detail",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run instrument group discovery commands."""
    parser = build_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_groups_argv(raw_argv))
    except CliConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)  # noqa:T201
        return 1

    if args.groups_command == "list":
        payload = list_group_catalog(
            triangles=bool(args.triangles),
            include_all=bool(args.all),
        )
        _write_payload(payload, as_json=bool(args.json), detail=False)
        return 0
    if args.groups_command == "show":
        payload = show_group_catalog(args.group)
        _write_payload(payload, as_json=bool(args.json), detail=True)
        return 0
    parser.error(f"unsupported groups command: {args.groups_command}")


def _write_payload(
    payload: dict[str, Any],
    *,
    as_json: bool,
    detail: bool,
) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return
    if detail:
        print(format_group_detail(payload))  # noqa:T201
    else:
        print(format_group_catalog(payload))  # noqa:T201
