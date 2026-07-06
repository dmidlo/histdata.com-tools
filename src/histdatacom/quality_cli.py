"""End-user quality utility commands."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_quality_argv,
)
from histdatacom.data_quality import QUALITY_CHECK_GROUPS
from histdatacom.data_quality.fingerprint_discovery import (
    fingerprint_schema_discovery,
    format_fingerprint_schema_discovery,
)
from histdatacom.data_quality.preflight import (
    DEFAULT_QUALITY_PREFLIGHT_EVIDENCE_MAX_AGE_SECONDS,
    format_quality_preflight_evidence_inspection,
    inspect_quality_preflight_evidence,
)
from histdatacom.data_quality.profiles import (
    QualityProfileError,
    load_quality_profile_file,
)
from histdatacom.data_quality.remediation_audit import (
    DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT,
    DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
    DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT,
    audit_remediation_catalog_report_paths,
    format_remediation_catalog_audit,
    remediation_catalog_audit_has_warning_error_gaps,
    remediation_catalog_audit_to_json,
)
from histdatacom.fx_enums import (
    Format,
    Pairs,
    Timeframe,
    normalize_pair_group,
    pair_group_names,
)
from histdatacom.verbosity import configure_logging


def build_parser() -> argparse.ArgumentParser:
    """Build the quality utility parser."""
    parser = argparse.ArgumentParser(prog="histdatacom quality")
    add_config_argument(parser)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbosity",
        action="count",
        default=0,
        help="increase logging verbosity; repeat as -vv or -vvv",
    )
    subparsers = parser.add_subparsers(dest="quality_command", required=True)
    evidence = subparsers.add_parser(
        "evidence",
        aliases=("inspect-evidence", "doctor-evidence"),
        help="inspect saved quality preflight evidence",
    )
    evidence.add_argument(
        "--evidence",
        "--quality-preflight-evidence",
        dest="evidence_path",
        required=True,
        metavar="PATH",
        help="saved quality preflight JSON report to inspect",
    )
    evidence.add_argument(
        "--target",
        "--quality-target",
        "--quality-path",
        "--data-directory",
        dest="target_root",
        default="data",
        metavar="PATH",
        help="local cache root to compare against; defaults to data",
    )
    evidence.add_argument(
        "--quality-checks",
        dest="quality_check_groups",
        nargs="+",
        choices=QUALITY_CHECK_GROUPS,
        metavar="GROUP",
        help=(
            "quality check groups used by the target run; defaults to all. "
            "Supported: " + ", ".join(QUALITY_CHECK_GROUPS)
        ),
    )
    evidence.add_argument(
        "-p",
        "--pairs",
        nargs="+",
        choices=Pairs.list_keys(),
        default=(),
        metavar="PAIR",
        help="limit inspection to one or more symbols",
    )
    evidence.add_argument(
        "--pair-groups",
        "--instrument-groups",
        "--symbol-groups",
        dest="pair_groups",
        nargs="+",
        type=normalize_pair_group,
        choices=pair_group_names(),
        default=(),
        metavar="GROUP",
        help="named instrument groups to union with --pairs",
    )
    evidence.add_argument(
        "-f",
        "--formats",
        nargs="+",
        choices=Format.list_values(),
        default=(),
        metavar="FORMAT",
        help="limit inspection to one or more HistData formats",
    )
    evidence.add_argument(
        "-t",
        "--timeframes",
        nargs="+",
        type=lambda value: Timeframe(value).name,  # type: ignore[arg-type]
        choices=Timeframe.list_keys(),
        default=(),
        metavar="TIMEFRAME",
        help="limit inspection to one or more HistData timeframes",
    )
    evidence.add_argument(
        "--quality-preflight-evidence-max-age-seconds",
        dest="evidence_max_age_seconds",
        type=_non_negative_int,
        default=DEFAULT_QUALITY_PREFLIGHT_EVIDENCE_MAX_AGE_SECONDS,
        metavar="SECONDS",
        help=(
            "maximum age for saved evidence; defaults to "
            f"{DEFAULT_QUALITY_PREFLIGHT_EVIDENCE_MAX_AGE_SECONDS}"
        ),
    )
    evidence.add_argument(
        "--quality-preflight-evidence-stale-ok",
        dest="allow_stale_evidence",
        action="store_true",
        help="allow matching evidence even when generated_at_utc is stale",
    )
    evidence.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable inspection payload",
    )
    catalog = subparsers.add_parser(
        "remediation-catalog",
        aliases=("catalog", "remediation-audit"),
        help="audit remediation catalog completeness",
    )
    catalog.add_argument(
        "--report",
        dest="report_paths",
        action="extend",
        nargs="+",
        default=[],
        metavar="PATH",
        help=(
            "saved quality JSON report to include as representative "
            "remediation-coverage evidence; repeat for multiple reports"
        ),
    )
    catalog.add_argument(
        "--code-limit",
        dest="code_limit",
        type=_integer_limit,
        default=DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT,
        metavar="N",
        help=(
            "maximum unmapped finding-code groups to include; use -1 for all"
        ),
    )
    catalog.add_argument(
        "--rule-limit",
        dest="rule_limit",
        type=_integer_limit,
        default=DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
        metavar="N",
        help="maximum rule-id count groups to include; use -1 for all",
    )
    catalog.add_argument(
        "--source-limit",
        dest="source_limit",
        type=_integer_limit,
        default=DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
        metavar="N",
        help=(
            "maximum source examples to include per finding code; "
            "use -1 for all"
        ),
    )
    catalog.add_argument(
        "--target-axis-limit",
        dest="target_axis_limit",
        type=_integer_limit,
        default=DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT,
        metavar="N",
        help=(
            "maximum target-axis samples to keep from report coverage; "
            "use -1 for all"
        ),
    )
    catalog.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable audit payload",
    )
    fingerprint_schema = subparsers.add_parser(
        "fingerprint-schema",
        aliases=("fingerprint-contract", "fingerprint-discovery"),
        help="discover fingerprint schemas, profile knobs, and vocabulary",
    )
    fingerprint_schema.add_argument(
        "--quality-profile",
        dest="quality_profile_path",
        default="",
        metavar="PATH",
        help=(
            "read a quality profile file and reflect effective fingerprint "
            "controls"
        ),
    )
    fingerprint_schema.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable discovery payload",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run quality utility commands."""
    parser = build_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_quality_argv(raw_argv))
    except CliConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)  # noqa:T201
        return 1
    configure_logging(args.verbosity)
    if args.quality_command in {
        "evidence",
        "inspect-evidence",
        "doctor-evidence",
    }:
        payload = inspect_quality_preflight_evidence(
            args.target_root,
            args.evidence_path,
            pairs=args.pairs,
            pair_groups=args.pair_groups,
            formats=args.formats,
            timeframes=args.timeframes,
            quality_check_groups=args.quality_check_groups,
            evidence_max_age_seconds=args.evidence_max_age_seconds,
            allow_stale_evidence=args.allow_stale_evidence,
        )
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        else:
            print(
                format_quality_preflight_evidence_inspection(payload)
            )  # noqa:T201
        return 0 if payload.get("accepted") is True else 1

    if args.quality_command in {
        "remediation-catalog",
        "catalog",
        "remediation-audit",
    }:
        payload = audit_remediation_catalog_report_paths(
            args.report_paths,
            code_limit=args.code_limit,
            rule_limit=args.rule_limit,
            source_limit=args.source_limit,
            target_axis_limit=args.target_axis_limit,
        )
        if args.json:
            print(  # noqa:T201
                remediation_catalog_audit_to_json(payload),
                end="",
            )
        else:
            print(format_remediation_catalog_audit(payload))  # noqa:T201
        return (
            1
            if remediation_catalog_audit_has_warning_error_gaps(payload)
            else 0
        )

    if args.quality_command in {
        "fingerprint-schema",
        "fingerprint-contract",
        "fingerprint-discovery",
    }:
        try:
            profile = (
                load_quality_profile_file(args.quality_profile_path)
                if args.quality_profile_path
                else None
            )
        except QualityProfileError as exc:
            print(f"quality profile error: {exc}", file=sys.stderr)  # noqa:T201
            return 1
        payload = fingerprint_schema_discovery(profile)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        else:
            print(format_fingerprint_schema_discovery(payload))  # noqa:T201
        return 0

    parser.error(f"unsupported quality command: {args.quality_command}")


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not an integer"
        ) from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _integer_limit(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not an integer"
        ) from exc
    if parsed < -1:
        raise argparse.ArgumentTypeError("value must be -1 or greater")
    return parsed
