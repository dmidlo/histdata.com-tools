"""Installed CLI for bounded dataset catalog operations."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import sys
from typing import Any

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetContractError,
    DatasetOrigin,
    DatasetQueryScopeV1,
    read_resolution_receipt,
    write_resolution_receipt,
)
from histdatacom.runtime_contracts import JSONValue

MAX_DATASET_CLI_SEQUENCE_ITEMS = 128
MAX_DATASET_CLI_STRING_LENGTH = 4096
MAX_DATASET_CLI_OUTPUT_BYTES = 1_048_576


def build_parser() -> argparse.ArgumentParser:
    """Return the installed provider-neutral dataset catalog parser."""
    parser = argparse.ArgumentParser(prog="histdatacom datasets")
    parser.add_argument(
        "--catalog",
        required=True,
        help="Path to a versioned local dataset catalog JSON file.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list", help="List logical datasets and versions.")

    describe = subparsers.add_parser(
        "describe", help="Describe a dataset, alias, or immutable version."
    )
    describe.add_argument("reference")

    resolve = subparsers.add_parser(
        "resolve", help="Resolve an alias once to an immutable version."
    )
    resolve.add_argument("reference")
    _add_scope_args(resolve)
    resolve.add_argument(
        "--receipt", help="Optional path for the immutable resolution receipt."
    )

    verify = subparsers.add_parser(
        "verify", help="Resolve and hash-verify a dataset version."
    )
    verify.add_argument("reference")

    replay = subparsers.add_parser(
        "replay",
        help="Replay an existing receipt without re-resolving its alias.",
    )
    replay.add_argument("receipt")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one bounded catalog command with stable failure output."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        catalog = DatasetCatalog.read(args.catalog)
        if args.command == "list":
            payload: JSONValue = {
                "catalog_id": catalog.catalog_id,
                "datasets": list(catalog.list_entries()),
            }
        elif args.command == "describe":
            payload = catalog.describe(args.reference)
        elif args.command == "resolve":
            scope = DatasetQueryScopeV1(
                symbols=tuple(args.symbol),
                periods=tuple(args.period),
                origin=(
                    None if args.origin is None else DatasetOrigin(args.origin)
                ),
                ensemble_member_id=args.ensemble_member,
            )
            receipt = catalog.resolve(args.reference, query_scope=scope)
            if args.receipt:
                write_resolution_receipt(receipt, args.receipt)
            payload = receipt.to_dict()
            if args.receipt:
                payload = {
                    **payload,
                    "receipt_path": str(Path(args.receipt).resolve()),
                }
        elif args.command == "verify":
            payload = catalog.verify(args.reference).to_dict()
        else:
            receipt = read_resolution_receipt(args.receipt)
            replayed = catalog.replay(receipt)
            payload = {
                "resolution": replayed.to_dict(),
                "verification": catalog.verify(replayed).to_dict(),
                "alias_re_resolved": False,
            }
        _emit(payload)
        return 0
    except (DatasetContractError, OSError, TypeError, ValueError) as err:
        code = (
            err.code.value
            if isinstance(err, DatasetContractError)
            else "invalid_dataset_catalog"
        )
        print(
            json.dumps(
                {"status": "failed", "reason_code": code, "message": str(err)},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


def _add_scope_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--period", action="append", default=[])
    parser.add_argument(
        "--origin", choices=tuple(item.value for item in DatasetOrigin)
    )
    parser.add_argument("--ensemble-member")


def _emit(payload: JSONValue) -> None:
    bounded = _bounded_value(payload)
    encoded = json.dumps(bounded, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > MAX_DATASET_CLI_OUTPUT_BYTES:
        raise ValueError("bounded dataset command output exceeds byte limit")
    print(encoded)


def _bounded_value(value: Any) -> JSONValue:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if len(value) <= MAX_DATASET_CLI_STRING_LENGTH:
            return value
        return value[:MAX_DATASET_CLI_STRING_LENGTH] + "..."
    if isinstance(value, Mapping):
        return {
            str(key): _bounded_value(item)
            for key, item in sorted(
                value.items(), key=lambda pair: str(pair[0])
            )
        }
    if isinstance(value, (list, tuple)):
        selected = list(value[:MAX_DATASET_CLI_SEQUENCE_ITEMS])
        result = [_bounded_value(item) for item in selected]
        if len(value) > len(selected):
            result.append(
                {
                    "truncated_item_count": len(value) - len(selected),
                    "bounded_output": True,
                }
            )
        return result
    return _bounded_value(str(value))


__all__ = ["build_parser", "main"]
