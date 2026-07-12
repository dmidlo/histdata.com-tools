"""End-user quality utility commands."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from typing import cast

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_quality_argv,
)
from histdatacom.data_quality import QUALITY_CHECK_GROUPS
from histdatacom.data_quality.bounded_payload_contracts import (
    bounded_payload_contract_audit,
    format_bounded_payload_contract_audit,
)
from histdatacom.data_quality.fingerprint_discovery import (
    fingerprint_contract_audit,
    fingerprint_schema_discovery,
    format_fingerprint_contract_audit,
    format_fingerprint_schema_discovery,
)
from histdatacom.data_quality.fingerprint_next_work import (
    DEFAULT_FINGERPRINT_NEXT_WORK_ALTERNATE_LIMIT,
    fingerprint_next_work_recommendation,
    format_fingerprint_next_work,
)
from histdatacom.data_quality.reporting import (
    fingerprint_readiness_risk_summary,
    format_fingerprint_readiness_risk_lines,
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
    load_quality_report,
    remediation_catalog_audit_has_warning_error_gaps,
    remediation_catalog_audit_to_json,
)
from histdatacom.data_quality.repair_plan import (
    DEFAULT_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
    DEFAULT_QUALITY_REPAIR_PLAN_ITEM_LIMIT,
    format_quality_repair_plan,
    quality_repair_plan,
    quality_repair_plan_to_json,
)
from histdatacom.data_quality.synthetic_constraints import (
    DEFAULT_SYNTHETIC_VALIDATION_MISMATCH_LIMIT,
    DEFAULT_SYNTHETIC_VALIDATION_TARGET_LIMIT,
    format_synthetic_validation,
    validate_synthetic_constraint_reports,
)
from histdatacom.fx_enums import (
    Format,
    Pairs,
    Timeframe,
    normalize_pair_group,
    pair_group_names,
)
from histdatacom.publication_safety import publish_safe_path
from histdatacom.runtime_contracts import JSONValue
from histdatacom.verbosity import configure_logging

FINGERPRINT_READINESS_RISK_COMMAND_SCHEMA_VERSION = (
    "histdatacom.fingerprint-readiness-risk-command.v1"
)


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
            "maximum source examples to include per finding code; use -1 for all"
        ),
    )
    catalog.add_argument(
        "--target-axis-limit",
        dest="target_axis_limit",
        type=_integer_limit,
        default=DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT,
        metavar="N",
        help=(
            "maximum target-axis samples to keep from report coverage; use -1 for all"
        ),
    )
    catalog.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable audit payload",
    )
    repair_plan = subparsers.add_parser(
        "repair-plan",
        aliases=("remediation-repair-plan",),
        help="derive a non-mutating repair plan from a saved quality report",
    )
    repair_plan.add_argument(
        "--report",
        dest="report_path",
        required=True,
        metavar="PATH",
        help="saved quality JSON report to translate into a repair plan",
    )
    repair_plan.add_argument(
        "--item-limit",
        type=_non_negative_int,
        default=DEFAULT_QUALITY_REPAIR_PLAN_ITEM_LIMIT,
        metavar="N",
        help=(
            "maximum repair-plan items to include; defaults to "
            f"{DEFAULT_QUALITY_REPAIR_PLAN_ITEM_LIMIT}"
        ),
    )
    repair_plan.add_argument(
        "--evidence-limit",
        type=_non_negative_int,
        default=DEFAULT_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
        metavar="N",
        help=(
            "maximum evidence values per item; defaults to "
            f"{DEFAULT_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT}"
        ),
    )
    repair_plan.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable non-mutating repair plan",
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
            "read a quality profile file and reflect effective fingerprint controls"
        ),
    )
    fingerprint_schema.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable discovery payload",
    )
    fingerprint_schema.add_argument(
        "--verify",
        action="store_true",
        help="emit a data-free fingerprint contract drift audit",
    )
    fingerprint_readiness = subparsers.add_parser(
        "fingerprint-readiness",
        aliases=("fingerprint-risk", "readiness-risk"),
        help="rank fingerprint readiness risks from saved quality reports",
    )
    fingerprint_readiness.add_argument(
        "--report",
        dest="report_paths",
        action="extend",
        nargs="+",
        required=True,
        metavar="PATH",
        help=(
            "saved quality JSON report to rank; repeat or pass multiple "
            "paths to compare reports"
        ),
    )
    fingerprint_readiness.add_argument(
        "--target-limit",
        dest="target_limit",
        type=_integer_limit,
        default=None,
        metavar="N",
        help="maximum ranked targets to include; use -1 for all",
    )
    fingerprint_readiness.add_argument(
        "--section-limit",
        dest="section_limit",
        type=_integer_limit,
        default=None,
        metavar="N",
        help="maximum section risks to include per target; use -1 for all",
    )
    fingerprint_readiness.add_argument(
        "--reason-limit",
        dest="reason_limit",
        type=_integer_limit,
        default=None,
        metavar="N",
        help="maximum reason codes to include; use -1 for all",
    )
    fingerprint_readiness.add_argument(
        "--next-work",
        action="store_true",
        help=(
            "recommend the next fingerprint product work from the saved "
            "report evidence"
        ),
    )
    fingerprint_readiness.add_argument(
        "--alternate-limit",
        dest="alternate_limit",
        type=_integer_limit,
        default=DEFAULT_FINGERPRINT_NEXT_WORK_ALTERNATE_LIMIT,
        metavar="N",
        help=(
            "maximum alternate next-work recommendations to include; "
            "use -1 for all"
        ),
    )
    fingerprint_readiness.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable readiness risk ranking payload",
    )
    synthetic_validate = subparsers.add_parser(
        "synthetic-validate",
        aliases=("synthetic-fingerprint-validate",),
        help="compare candidate and reference synthetic fingerprint constraints",
    )
    synthetic_validate.add_argument(
        "--reference-report",
        required=True,
        metavar="PATH",
        help="saved reference quality report containing fingerprint constraints",
    )
    synthetic_validate.add_argument(
        "--candidate-report",
        required=True,
        metavar="PATH",
        help="saved candidate quality report containing fingerprint constraints",
    )
    synthetic_validate.add_argument(
        "--target-limit",
        type=_integer_limit,
        default=DEFAULT_SYNTHETIC_VALIDATION_TARGET_LIMIT,
        metavar="N",
        help="maximum target comparisons to include; use -1 for all",
    )
    synthetic_validate.add_argument(
        "--mismatch-limit",
        type=_integer_limit,
        default=DEFAULT_SYNTHETIC_VALIDATION_MISMATCH_LIMIT,
        metavar="N",
        help="maximum mismatch details per target; use -1 for all",
    )
    synthetic_validate.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable advisory validation payload",
    )
    bounded_payload = subparsers.add_parser(
        "bounded-payload-contract",
        aliases=("bounded-payload-audit", "report-payload-contract"),
        help="audit bounded quality-report payload metadata semantics",
    )
    bounded_payload.add_argument(
        "--json",
        action="store_true",
        help="emit the machine-readable bounded payload contract audit",
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
        "repair-plan",
        "remediation-repair-plan",
    }:
        try:
            report = load_quality_report(args.report_path)
            repair_payload = quality_repair_plan(
                report,
                report_path=args.report_path,
                item_limit=args.item_limit,
                evidence_limit=args.evidence_limit,
            )
        except (OSError, ValueError, TypeError) as exc:
            print(f"quality report error: {exc}", file=sys.stderr)  # noqa:T201
            return 1
        if args.json:
            print(
                quality_repair_plan_to_json(repair_payload), end=""
            )  # noqa:T201
        else:
            print(format_quality_repair_plan(repair_payload))  # noqa:T201
        return 0

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
        if args.verify:
            audit_payload = fingerprint_contract_audit(profile)
            if args.json:
                print(
                    json.dumps(audit_payload, indent=2, sort_keys=True)
                )  # noqa:T201
            else:
                print(
                    format_fingerprint_contract_audit(audit_payload)
                )  # noqa:T201
            return 1 if audit_payload.get("status") == "fail" else 0

        payload = fingerprint_schema_discovery(profile)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        else:
            print(format_fingerprint_schema_discovery(payload))  # noqa:T201
        return 0

    if args.quality_command in {
        "fingerprint-readiness",
        "fingerprint-risk",
        "readiness-risk",
    }:
        try:
            risk_payload = _fingerprint_readiness_risk_command_payload(
                args.report_paths,
                target_limit=args.target_limit,
                section_limit=args.section_limit,
                reason_limit=args.reason_limit,
                next_work=args.next_work,
                alternate_limit=args.alternate_limit,
            )
        except (OSError, ValueError, TypeError) as exc:
            print(f"quality report error: {exc}", file=sys.stderr)  # noqa:T201
            return 1
        if args.json:
            print(
                json.dumps(risk_payload, indent=2, sort_keys=True)
            )  # noqa:T201
        else:
            print(
                _format_fingerprint_readiness_risk_command(risk_payload)
            )  # noqa:T201
        return 0

    if args.quality_command in {
        "synthetic-validate",
        "synthetic-fingerprint-validate",
    }:
        try:
            reference = load_quality_report(args.reference_report)
            candidate = load_quality_report(args.candidate_report)
            validation = validate_synthetic_constraint_reports(
                reference,
                candidate,
                target_limit=args.target_limit,
                mismatch_limit=args.mismatch_limit,
            )
        except (OSError, ValueError, TypeError) as exc:
            print(f"quality report error: {exc}", file=sys.stderr)  # noqa:T201
            return 1
        if args.json:
            print(json.dumps(validation, indent=2, sort_keys=True))  # noqa:T201
        else:
            print(format_synthetic_validation(validation))  # noqa:T201
        return 0

    if args.quality_command in {
        "bounded-payload-contract",
        "bounded-payload-audit",
        "report-payload-contract",
    }:
        audit_payload = bounded_payload_contract_audit()
        if args.json:
            print(
                json.dumps(audit_payload, indent=2, sort_keys=True)
            )  # noqa:T201
        else:
            print(
                format_bounded_payload_contract_audit(audit_payload)
            )  # noqa:T201
        return 1 if audit_payload.get("status") == "fail" else 0

    parser.error(f"unsupported quality command: {args.quality_command}")


def _fingerprint_readiness_risk_command_payload(
    report_paths: Sequence[str],
    *,
    target_limit: int | None,
    section_limit: int | None,
    reason_limit: int | None,
    next_work: bool = False,
    alternate_limit: int | None = None,
) -> dict[str, JSONValue]:
    reports: list[dict[str, JSONValue]] = []
    loaded_reports = []
    risk_report_count = 0
    for path in report_paths:
        report = load_quality_report(path)
        loaded_reports.append((path, report))
        summary = fingerprint_readiness_risk_summary(
            report,
            target_limit=target_limit,
            section_limit=section_limit,
            reason_limit=reason_limit,
        )
        risk_target_count = _json_int_value(
            summary.get("risk_target_count") if summary else 0
        )
        if risk_target_count > 0:
            risk_report_count += 1
        reports.append(
            {
                "report_path": publish_safe_path(path),
                "summary": summary or {},
            }
        )
    payload: dict[str, JSONValue] = {
        "schema_version": FINGERPRINT_READINESS_RISK_COMMAND_SCHEMA_VERSION,
        "report_count": len(reports),
        "risk_report_count": risk_report_count,
        "reports": cast(JSONValue, reports),
    }
    if next_work:
        payload["next_work"] = fingerprint_next_work_recommendation(
            loaded_reports,
            alternate_limit=alternate_limit,
            target_axis_limit=target_limit,
        )
    return payload


def _format_fingerprint_readiness_risk_command(
    payload: Mapping[str, JSONValue],
) -> str:
    lines = [
        "Fingerprint readiness risk command",
        f"reports: {payload.get('report_count', 0)}",
        f"reports with risks: {payload.get('risk_report_count', 0)}",
    ]
    reports = payload.get("reports")
    if not isinstance(reports, list):
        return "\n".join(lines)
    for item in reports:
        if not isinstance(item, dict):
            continue
        lines.append("")
        lines.append(f"Report: {item.get('report_path', 'unknown')}")
        summary = item.get("summary")
        if isinstance(summary, dict) and summary:
            lines.extend(format_fingerprint_readiness_risk_lines(summary))
        else:
            lines.append("Fingerprint readiness risk")
            lines.append("- no fingerprint readiness data")
    next_work = payload.get("next_work")
    if isinstance(next_work, dict):
        lines.append("")
        lines.append(format_fingerprint_next_work(next_work))
    return "\n".join(lines)


def _json_int_value(value: JSONValue | None) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


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
