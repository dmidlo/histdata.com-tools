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
from histdatacom.data_analytics.feed_epochs_v2 import (
    FeedEpochFitConfigV2,
    analyze_active_time_feed_epochs,
    read_active_time_feed_epoch_definition,
    write_active_time_feed_epoch_campaign,
)
from histdatacom.market_context import (
    DEFAULT_MARKET_CONTEXT_SOURCES,
    DEFAULT_ONS_QUERIES,
    MarketContextFetchProfileV1,
    build_live_market_context_corpus,
    write_market_context_corpus,
)
from histdatacom.synthetic.observation_calibration import (
    ObservationCalibrationProfileV2,
    calibrate_historical_observation_operators,
    read_feed_epoch_evidence_v2,
    write_observation_calibration_campaign,
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
    active = subparsers.add_parser(
        "feed-epochs-v2",
        help="fit active-time multivariate epochs from ASCII tick caches",
    )
    active.add_argument(
        "--target",
        "--path",
        dest="paths",
        nargs="+",
        required=True,
        metavar="PATH",
        help="local file or directory containing monthly ASCII tick caches",
    )
    active.add_argument(
        "--artifact-dir",
        required=True,
        metavar="PATH",
        help="write definition, evidence, and campaign JSON artifacts",
    )
    active.add_argument(
        "--features",
        nargs="+",
        default=None,
        metavar="NAME",
        help="active-time features included in the multivariate objective",
    )
    active.add_argument("--min-evidence-periods", type=int, default=None)
    active.add_argument("--min-segment-periods", type=int, default=None)
    active.add_argument("--min-feature-coverage", type=float, default=None)
    active.add_argument("--min-symbol-count", type=int, default=None)
    active.add_argument("--penalty-multiplier", type=float, default=None)
    active.add_argument("--robust-clip", type=float, default=None)
    active.add_argument("--min-boundary-support", type=float, default=None)
    active.add_argument(
        "--boundary-match-tolerance-periods", type=int, default=None
    )
    active.add_argument("--active-gap-cap-ms", type=int, default=None)
    active.add_argument("--burst-interval-ms", type=int, default=None)
    active.add_argument("--activity-bin-ms", type=int, default=None)
    active.add_argument("--max-evidence", type=int, default=None)
    active.add_argument("--max-sensitivity-runs", type=int, default=None)
    active.add_argument(
        "--json",
        action="store_true",
        help="emit compact campaign and artifact metadata",
    )
    calibration = subparsers.add_parser(
        "observation-calibrate-v2",
        help="fit and hold out historical observation operators",
    )
    calibration.add_argument(
        "--definition",
        required=True,
        metavar="PATH",
        help="stable feed-epochs-v2 definition artifact",
    )
    calibration.add_argument(
        "--evidence",
        required=True,
        metavar="PATH",
        help="feed-epochs-v2 evidence artifact with local cache paths",
    )
    calibration.add_argument(
        "--artifact-dir",
        required=True,
        metavar="PATH",
        help="write operator, fit evidence, and campaign artifacts",
    )
    calibration.add_argument(
        "--sessions",
        nargs="+",
        default=("asia", "london", "new_york"),
        metavar="NAME",
        help="bounded UTC session windows used for real evaluation",
    )
    calibration.add_argument("--calibration-period", default="")
    calibration.add_argument("--validation-period", default="")
    calibration.add_argument("--final-holdout-period", default="")
    calibration.add_argument("--max-events-per-window", type=int, default=4096)
    calibration.add_argument(
        "--minimum-events-per-window", type=int, default=512
    )
    calibration.add_argument(
        "--max-source-bytes", type=int, default=2 * 1024**3
    )
    calibration.add_argument("--max-runtime-seconds", type=float, default=600.0)
    calibration.add_argument(
        "--max-peak-memory-bytes", type=int, default=2 * 1024**3
    )
    calibration.add_argument(
        "--json",
        action="store_true",
        help="emit compact calibration and artifact metadata",
    )
    context = subparsers.add_parser(
        "market-context-corpus",
        help="build an immutable official-source point-in-time event corpus",
    )
    context.add_argument(
        "--artifact-dir",
        required=True,
        metavar="PATH",
        help="write content-addressed source, timeline, and corpus artifacts",
    )
    context.add_argument("--start-date", required=True, metavar="YYYY-MM-DD")
    context.add_argument("--end-date", required=True, metavar="YYYY-MM-DD")
    context.add_argument(
        "--sources",
        nargs="+",
        choices=DEFAULT_MARKET_CONTEXT_SOURCES,
        default=DEFAULT_MARKET_CONTEXT_SOURCES,
        metavar="NAME",
        help="approved source families to acquire",
    )
    context.add_argument(
        "--ons-query",
        dest="ons_queries",
        nargs="+",
        default=DEFAULT_ONS_QUERIES,
        metavar="TEXT",
        help="bounded ONS release-search queries",
    )
    context.add_argument(
        "--operator-catalog",
        default="",
        metavar="PATH",
        help="optional operator-maintained catalog (default: packaged catalog)",
    )
    context.add_argument("--timeout-seconds", type=float, default=30.0)
    context.add_argument("--max-response-bytes", type=int, default=16 * 1024**2)
    context.add_argument(
        "--max-total-source-bytes", type=int, default=64 * 1024**2
    )
    context.add_argument("--max-ons-pages-per-query", type=int, default=8)
    context.add_argument("--max-events", type=int, default=4096)
    context.add_argument("--max-runtime-seconds", type=float, default=300.0)
    context.add_argument(
        "--json",
        action="store_true",
        help="emit compact corpus and artifact metadata",
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
    if args.analytics_command == "market-context-corpus":
        try:
            build = build_live_market_context_corpus(
                MarketContextFetchProfileV1(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    sources=tuple(args.sources),
                    ons_queries=tuple(args.ons_queries),
                    timeout_seconds=args.timeout_seconds,
                    max_response_bytes=args.max_response_bytes,
                    max_total_source_bytes=args.max_total_source_bytes,
                    max_ons_pages_per_query=args.max_ons_pages_per_query,
                    max_events=args.max_events,
                    max_runtime_seconds=args.max_runtime_seconds,
                ),
                operator_catalog_path=args.operator_catalog or None,
            )
            artifacts = write_market_context_corpus(build, args.artifact_dir)
        except (OSError, ValueError) as exc:
            print(
                f"market-context corpus error: {exc}", file=sys.stderr
            )  # noqa:T201
            return 1
        corpus = build.corpus
        context_payload = {
            "schema_version": corpus.schema_version,
            "corpus_id": corpus.corpus_id,
            "timeline_id": corpus.timeline.timeline_id,
            "event_count": len(corpus.timeline.events),
            "source_count": len(corpus.sources),
            "source_bytes": sum(item.size_bytes for item in corpus.sources),
            "duplicate_event_count": corpus.duplicate_event_count,
            "runtime_seconds": corpus.runtime_seconds,
            "peak_memory_bytes": corpus.peak_memory_bytes,
            "coverage": [item.to_dict() for item in corpus.coverage],
            "artifacts": {
                name: artifact.to_dict()
                for name, artifact in sorted(artifacts.items())
            },
        }
        if args.json:
            print(  # noqa:T201
                json.dumps(context_payload, indent=2, sort_keys=True)
            )
        else:
            print(  # noqa:T201
                "Point-in-time market-context corpus\n"
                f"sources: {len(corpus.sources)}\n"
                f"source bytes: {context_payload['source_bytes']}\n"
                f"events: {len(corpus.timeline.events)}\n"
                f"duplicates removed: {corpus.duplicate_event_count}\n"
                f"timeline: {artifacts['timeline'].path}\n"
                f"corpus: {artifacts['corpus'].path}"
            )
        return 0
    if args.analytics_command == "observation-calibrate-v2":
        definition = read_active_time_feed_epoch_definition(args.definition)
        evidence = read_feed_epoch_evidence_v2(args.evidence)
        split_periods = {
            "calibration": args.calibration_period,
            "validation": args.validation_period,
            "final_holdout": args.final_holdout_period,
        }
        configured_splits = {
            key: value for key, value in split_periods.items() if value
        }
        calibration_campaign = calibrate_historical_observation_operators(
            evidence,
            epoch_definition=definition,
            profile=ObservationCalibrationProfileV2(
                split_periods=configured_splits,
                sessions=tuple(args.sessions),
                max_events_per_window=args.max_events_per_window,
                minimum_events_per_window=args.minimum_events_per_window,
                max_source_bytes=args.max_source_bytes,
                max_runtime_seconds=args.max_runtime_seconds,
                max_peak_memory_bytes=args.max_peak_memory_bytes,
            ),
        )
        artifacts = write_observation_calibration_campaign(
            calibration_campaign, args.artifact_dir
        )
        calibration_payload = {
            "schema_version": calibration_campaign.schema_version,
            "campaign_id": calibration_campaign.campaign_id,
            "operator_id": calibration_campaign.operator.operator_id,
            "readiness_status": calibration_campaign.readiness_status,
            "readiness_reasons": list(calibration_campaign.readiness_reasons),
            "window_count": len(calibration_campaign.windows),
            "runtime_seconds": calibration_campaign.runtime_seconds,
            "peak_memory_bytes": calibration_campaign.peak_memory_bytes,
            "artifacts": {
                name: artifact.to_dict()
                for name, artifact in sorted(artifacts.items())
            },
        }
        if args.json:
            print(  # noqa:T201
                json.dumps(calibration_payload, indent=2, sort_keys=True)
            )
        else:
            print(  # noqa:T201
                "Observation calibration v2\n"
                f"operator: {calibration_campaign.operator.operator_id}\n"
                f"targets: {len(calibration_campaign.targets)}\n"
                f"windows: {len(calibration_campaign.windows)}\n"
                f"readiness: {calibration_campaign.readiness_status}\n"
                f"campaign: {artifacts['campaign'].path}"
            )
        return 0 if calibration_campaign.valid_for_application else 2
    if args.analytics_command == "feed-epochs-v2":
        epoch_campaign = analyze_active_time_feed_epochs(
            args.paths,
            config=_fit_v2_config_from_args(args),
        )
        artifacts = write_active_time_feed_epoch_campaign(
            epoch_campaign, args.artifact_dir
        )
        epoch_payload = epoch_campaign.to_dict(include_evidence=False)
        epoch_payload["artifacts"] = {
            name: artifact.to_dict()
            for name, artifact in sorted(artifacts.items())
        }
        if args.json:
            print(
                json.dumps(epoch_payload, indent=2, sort_keys=True)
            )  # noqa:T201
        else:
            definition = epoch_campaign.definition
            print(  # noqa:T201
                "Active-time feed epoch fit\n"
                f"sources: {epoch_campaign.source_count}\n"
                f"common periods: {definition.period_count}\n"
                f"symbols: {', '.join(definition.symbols)}\n"
                f"boundaries: {len(definition.boundaries)}\n"
                f"stability: {definition.stability.status}\n"
                f"definition: {artifacts['definition'].path}"
            )
        return 0
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
        report_payload = report.to_dict()
        if artifact is not None:
            report_payload["report_artifact"] = artifact.to_dict()
        if epoch_artifact is not None:
            report_payload["epoch_artifact"] = epoch_artifact.to_dict()
        print(json.dumps(report_payload, indent=2, sort_keys=True))  # noqa:T201
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


def _fit_v2_config_from_args(
    args: argparse.Namespace,
) -> FeedEpochFitConfigV2 | None:
    """Return an explicit v2 policy only when the operator configured one."""
    values = {
        "feature_names": tuple(args.features) if args.features else None,
        "min_evidence_periods": args.min_evidence_periods,
        "min_segment_periods": args.min_segment_periods,
        "min_feature_coverage": args.min_feature_coverage,
        "min_symbol_count": args.min_symbol_count,
        "penalty_multiplier": args.penalty_multiplier,
        "robust_clip": args.robust_clip,
        "min_boundary_support": args.min_boundary_support,
        "boundary_match_tolerance_periods": (
            args.boundary_match_tolerance_periods
        ),
        "active_gap_cap_ms": args.active_gap_cap_ms,
        "burst_interval_ms": args.burst_interval_ms,
        "activity_bin_ms": args.activity_bin_ms,
        "max_evidence": args.max_evidence,
        "max_sensitivity_runs": args.max_sensitivity_runs,
    }
    configured = {
        name: value for name, value in values.items() if value is not None
    }
    return FeedEpochFitConfigV2(**configured) if configured else None
