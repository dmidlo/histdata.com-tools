"""Command-line entry points for data analytics operations."""

from __future__ import annotations

import argparse
import json
import sys

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_analytics_argv,
)
from histdatacom.data_analytics.feed_regimes import (
    DEFAULT_QUIET_GAP_MS,
    analyze_feed_regimes,
    format_feed_regime_console_summary,
    write_feed_regime_report,
)
from histdatacom.verbosity import configure_logging


def build_parser() -> argparse.ArgumentParser:
    """Build the data analytics argument parser."""
    parser = argparse.ArgumentParser(prog="histdatacom analytics")
    add_config_argument(parser)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbosity",
        action="count",
        default=0,
        help="increase logging verbosity; repeat as -vv or -vvv",
    )
    subparsers = parser.add_subparsers(dest="analytics_command", required=True)
    feed = subparsers.add_parser(
        "feed-regimes",
        help="detect feed technological regimes from local tick data",
    )
    feed.add_argument(
        "--target",
        "--path",
        dest="paths",
        nargs="+",
        required=True,
        metavar="PATH",
        help="local file or directory containing HistData ASCII tick artifacts",
    )
    feed.add_argument(
        "--bucket",
        choices=("month", "year"),
        default="month",
        help="time bucket used before regime segmentation",
    )
    feed.add_argument(
        "--quiet-gap-ms",
        type=int,
        default=DEFAULT_QUIET_GAP_MS,
        metavar="MS",
        help="inter-arrival gap threshold counted as quiet or missing time",
    )
    feed.add_argument(
        "--report",
        default="",
        metavar="PATH",
        help="write the machine-readable analytics report to PATH",
    )
    feed.add_argument(
        "--json",
        action="store_true",
        help="emit the full machine-readable analytics payload",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the data analytics CLI."""
    parser = build_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_analytics_argv(raw_argv))
    except CliConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)  # noqa:T201
        return 1
    configure_logging(args.verbosity)
    if args.analytics_command != "feed-regimes":
        parser.error(f"unsupported analytics command: {args.analytics_command}")

    report = analyze_feed_regimes(
        args.paths,
        bucket=args.bucket,
        quiet_gap_ms=args.quiet_gap_ms,
    )
    artifact = (
        write_feed_regime_report(report, args.report) if args.report else None
    )
    if args.json:
        payload = report.to_dict()
        if artifact is not None:
            payload["report_artifact"] = artifact.to_dict()
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
    else:
        print(  # noqa:T201
            format_feed_regime_console_summary(
                report,
                artifact=artifact,
            )
        )
    return 0
