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
from histdatacom.data_analytics.feed_epochs import (
    FeedEpochFitConfigV1,
    write_feed_epoch_definition,
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
        "--epoch-artifact",
        default="",
        metavar="PATH",
        help="write the compact versioned feed-epoch definition to PATH",
    )
    feed.add_argument(
        "--features",
        nargs="+",
        default=None,
        metavar="NAME",
        help="observation-regime fingerprint features used by the fitter",
    )
    feed.add_argument(
        "--min-evidence-periods",
        type=int,
        default=None,
        metavar="N",
        help="minimum distinct periods required for a full fit",
    )
    feed.add_argument(
        "--min-segment-periods",
        type=int,
        default=None,
        metavar="N",
        help="minimum periods on each side of a boundary",
    )
    feed.add_argument(
        "--min-feature-coverage",
        type=float,
        default=None,
        metavar="RATE",
        help="minimum period coverage required for a feature",
    )
    feed.add_argument(
        "--min-change-score",
        type=float,
        default=None,
        metavar="SCORE",
        help="minimum robust adjacent-period change score",
    )
    feed.add_argument(
        "--min-boundary-support",
        type=float,
        default=None,
        metavar="RATE",
        help="minimum deterministic perturbation support for a boundary",
    )
    feed.add_argument(
        "--boundary-match-tolerance-periods",
        type=int,
        default=None,
        metavar="N",
        help="period tolerance used to match perturbed boundaries",
    )
    feed.add_argument(
        "--max-evidence",
        type=int,
        default=None,
        metavar="N",
        help="maximum canonical symbol-period evidence records",
    )
    feed.add_argument(
        "--max-sensitivity-runs",
        type=int,
        default=None,
        metavar="N",
        help="maximum sampling/missingness/feature-removal fits",
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

    fit_config = _fit_config_from_args(args)
    report = analyze_feed_regimes(
        args.paths,
        bucket=args.bucket,
        quiet_gap_ms=args.quiet_gap_ms,
        fit_config=fit_config,
    )
    artifact = (
        write_feed_regime_report(report, args.report) if args.report else None
    )
    epoch_artifact = (
        write_feed_epoch_definition(
            report.epoch_definition,
            args.epoch_artifact,
        )
        if args.epoch_artifact and report.epoch_definition is not None
        else None
    )
    if args.json:
        payload = report.to_dict()
        if artifact is not None:
            payload["report_artifact"] = artifact.to_dict()
        if epoch_artifact is not None:
            payload["epoch_artifact"] = epoch_artifact.to_dict()
        print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
    else:
        summary = format_feed_regime_console_summary(report, artifact=artifact)
        if epoch_artifact is not None:
            summary += f"\nepoch artifact: {epoch_artifact.path}"
        print(summary)  # noqa:T201
    return 0


def _fit_config_from_args(
    args: argparse.Namespace,
) -> FeedEpochFitConfigV1 | None:
    """Return an explicit fit policy only when the operator configured one."""
    values = {
        "feature_names": tuple(args.features) if args.features else None,
        "min_evidence_periods": args.min_evidence_periods,
        "min_segment_periods": args.min_segment_periods,
        "min_feature_coverage": args.min_feature_coverage,
        "min_change_score": args.min_change_score,
        "min_boundary_support": args.min_boundary_support,
        "boundary_match_tolerance_periods": (
            args.boundary_match_tolerance_periods
        ),
        "max_evidence": args.max_evidence,
        "max_sensitivity_runs": args.max_sensitivity_runs,
    }
    configured = {
        name: value for name, value in values.items() if value is not None
    }
    return FeedEpochFitConfigV1(**configured) if configured else None
