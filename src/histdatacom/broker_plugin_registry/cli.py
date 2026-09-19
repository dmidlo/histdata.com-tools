"""Offline installed inventory; no activation or configuration loading."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import sys
from typing import NoReturn

from .contracts import (
    BrokerPluginInventoryV1,
    BrokerPluginRegistryError,
    BrokerPluginRegistryReason,
)
from .discovery import discover_broker_plugins
from .selection import inspect_broker_plugins, select_broker_plugin
from .storage import write_plugin_inventory


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_SELECTION
        )


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="histdatacom broker-plugins")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON (the default).",
    )
    parser.add_argument(
        "--snapshot", help="Create an immutable local inventory artifact."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser(
        "list", help="List installed declarations without importing plugins."
    )
    for option in ("--json", "--snapshot"):
        if option == "--json":
            listing.add_argument(
                option, action="store_true", default=argparse.SUPPRESS
            )
        else:
            listing.add_argument(option, default=argparse.SUPPRESS)
    for name in ("inspect", "select"):
        command = commands.add_parser(
            name, help="Inspect metadata or select one compatible declaration."
        )
        command.add_argument(
            "--json", action="store_true", default=argparse.SUPPRESS
        )
        command.add_argument("--snapshot", default=argparse.SUPPRESS)
        command.add_argument("--plugin-id")
        command.add_argument("--provider")
        command.add_argument(
            "--version",
            default="",
            help="Plugin SemVer comparators, e.g. >=1.0.0,<2.0.0.",
        )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run metadata-only commands; --config is deliberately unsupported."""
    parser = build_parser()
    inventory: BrokerPluginInventoryV1 | None = None
    try:
        args = parser.parse_args(argv)
        inventory = discover_broker_plugins()
        payload: dict[str, object] = {
            "inventory": inventory.to_dict(),
            "compatible_candidate_ids": [
                candidate.artifact_id
                for candidate in inventory.candidates
                if candidate.registration.supports_sdk(inventory.sdk_version)
            ],
        }
        if args.command == "inspect":
            payload["inspected_declarations"] = [
                candidate.to_dict()
                for candidate in inspect_broker_plugins(
                    inventory,
                    plugin_id=args.plugin_id,
                    provider_id=args.provider,
                    version_constraint=args.version,
                )
            ]
            payload["activation_or_scientific_admission"] = False
        elif args.command == "select":
            selected = select_broker_plugin(
                inventory,
                plugin_id=args.plugin_id,
                provider_id=args.provider,
                version_constraint=args.version,
            )
            payload["selected_declaration"] = selected.to_dict()
            payload["activation_or_scientific_admission"] = False
        if args.snapshot:
            payload["snapshot"] = write_plugin_inventory(
                inventory, args.snapshot
            ).to_dict()
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return 2 if inventory.diagnostics else 0
    except BrokerPluginRegistryError as error:
        failure = error.to_dict()
        if inventory is not None:
            failure["installed_candidates"] = [
                {
                    "candidate_id": candidate.artifact_id,
                    "plugin_id": candidate.registration.plugin_id,
                    "plugin_version": candidate.registration.plugin_version,
                    "distribution_name": candidate.registration.distribution_name,
                    "distribution_version": candidate.registration.distribution_version,
                    "sdk_compatible": candidate.registration.supports_sdk(
                        inventory.sdk_version
                    ),
                }
                for candidate in inventory.candidates
            ]
        print(json.dumps(failure, sort_keys=True), file=sys.stderr)
        return 2
