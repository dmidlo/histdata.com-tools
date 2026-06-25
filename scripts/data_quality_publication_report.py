"""Sanitize existing data-quality reports and render publishable docs."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from histdatacom.publication_safety import (  # noqa: E402
    publish_safe_json_value,
    publish_safe_path,
)


@dataclass(frozen=True, slots=True)
class ReportRecord:
    """Compact metadata extracted from one data-quality report file."""

    source: str
    issue: str
    schema_version: str
    status: str
    target_count: int
    rule_count: int
    finding_count: int
    info_count: int
    warning_count: int
    error_count: int
    size_bytes: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible record."""
        return {
            "source": self.source,
            "issue": self.issue,
            "schema_version": self.schema_version,
            "status": self.status,
            "target_count": self.target_count,
            "rule_count": self.rule_count,
            "finding_count": self.finding_count,
            "info_count": self.info_count,
            "warning_count": self.warning_count,
            "error_count": self.error_count,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
        }


def main(argv: list[str] | None = None) -> int:
    """Run the report publication workflow."""
    args = _parse_args(argv)
    source = args.source
    records, code_counts, rule_counts = build_publication_records(
        source,
        sanitize_in_place=args.sanitize_in_place,
    )
    index_payload = build_index_payload(
        source=source,
        records=records,
        code_counts=code_counts,
        rule_counts=rule_counts,
        sanitize_in_place=args.sanitize_in_place,
    )
    args.index.parent.mkdir(parents=True, exist_ok=True)
    args.index.write_text(
        json.dumps(index_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        render_executive_report(index_payload),
        encoding="utf-8",
    )
    print(
        f"processed {len(records)} data-quality reports; "
        f"wrote {publish_safe_path(str(args.index))} and "
        f"{publish_safe_path(str(args.output))}"
    )
    return 0


def build_publication_records(
    source: Path,
    *,
    sanitize_in_place: bool,
) -> tuple[list[ReportRecord], Counter[str], Counter[str]]:
    """Read report JSON files and return their compact publication metadata."""
    records: list[ReportRecord] = []
    code_counts: Counter[str] = Counter()
    rule_counts: Counter[str] = Counter()
    for report_path in sorted(source.rglob("*.json")):
        payload = _read_json(report_path)
        safe_payload = publish_safe_json_value(payload)
        if sanitize_in_place and safe_payload != payload:
            report_path.write_text(
                json.dumps(safe_payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        records.append(_record_for(report_path, safe_payload))
        code_counts.update(_finding_codes(safe_payload))
        rule_counts.update(_rule_ids(safe_payload))
    return records, code_counts, rule_counts


def build_index_payload(
    *,
    source: Path,
    records: Iterable[ReportRecord],
    code_counts: Counter[str],
    rule_counts: Counter[str],
    sanitize_in_place: bool,
) -> dict[str, Any]:
    """Return a compact publication index for the report corpus."""
    record_list = list(records)
    totals = {
        "report_count": len(record_list),
        "source_bytes": sum(record.size_bytes for record in record_list),
        "target_count": sum(record.target_count for record in record_list),
        "rule_count": sum(record.rule_count for record in record_list),
        "finding_count": sum(record.finding_count for record in record_list),
        "info_count": sum(record.info_count for record in record_list),
        "warning_count": sum(record.warning_count for record in record_list),
        "error_count": sum(record.error_count for record in record_list),
    }
    status_counts = Counter(record.status for record in record_list)
    issue_counts: dict[str, dict[str, Any]] = {}
    for issue, issue_records in _records_by_issue(record_list).items():
        issue_counts[issue] = _issue_summary(issue_records)
    return {
        "schema_version": "histdatacom.data-quality-publication-index.v1",
        "source": publish_safe_path(str(source)),
        "publication_safety": {
            "raw_local_paths_sanitized": True,
            "sanitize_in_place": sanitize_in_place,
            "full_quality_battery_rerun": False,
        },
        "totals": totals,
        "status_counts": dict(sorted(status_counts.items())),
        "issue_summaries": issue_counts,
        "top_finding_codes": _top_counts(code_counts),
        "top_rule_ids": _top_counts(rule_counts),
        "reports": [record.to_dict() for record in record_list],
    }


def render_executive_report(index_payload: Mapping[str, Any]) -> str:
    """Return a human-readable executive report for publication."""
    totals = _mapping(index_payload.get("totals"))
    safety = _mapping(index_payload.get("publication_safety"))
    lines = [
        "# Data Quality Executive Report",
        "",
        "This report is generated from the latest existing local "
        "data-quality JSON outputs. It does not rerun the full data-quality "
        "battery; it sanitizes the existing evidence set and renders a "
        "publication-safe view for GitHub.",
        "",
        "## Publication posture",
        "",
        "| Control | Value |",
        "| --- | --- |",
        f"| Source | `{index_payload.get('source', '')}` |",
        f"| Raw local paths sanitized | {_yes_no(safety.get('raw_local_paths_sanitized'))} |",
        f"| Existing reports cleaned in place | {_yes_no(safety.get('sanitize_in_place'))} |",
        f"| Full quality battery rerun | {_yes_no(safety.get('full_quality_battery_rerun'))} |",
        "",
        "## Corpus",
        "",
        "| Measure | Count |",
        "| --- | ---: |",
        f"| Report files | {_int(totals.get('report_count')):,} |",
        f"| Source bytes | {_int(totals.get('source_bytes')):,} |",
        f"| Report-counted targets | {_int(totals.get('target_count')):,} |",
        f"| Report-counted findings | {_int(totals.get('finding_count')):,} |",
        f"| Informational findings | {_int(totals.get('info_count')):,} |",
        f"| Warning findings | {_int(totals.get('warning_count')):,} |",
        f"| Error findings | {_int(totals.get('error_count')):,} |",
        "",
        "The report-counted totals intentionally preserve the evidence exactly "
        "as produced by each JSON output. Some reports overlap by issue, "
        "symbol, or campaign batch, so these counts should not be read as a "
        "deduplicated inventory of unique market-data files.",
        "",
    ]
    lines.extend(_issue_section(index_payload))
    lines.extend(_finding_section(index_payload))
    lines.extend(_report_inventory_section(index_payload))
    lines.extend(
        [
            "## Operational interpretation",
            "",
            "The current evidence set is suitable for publication because local "
            "home directories, sidecar workspace locations, temporary "
            "directories, and absolute report paths are converted to relative "
            "project/report paths. The detailed raw report tree remains a "
            "local working artifact; the tracked GitHub surface is this "
            "executive report and the compact JSON index.",
            "",
        ]
    )
    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=PROJECT_ROOT / "data" / ".quality",
        help="Directory containing existing data-quality JSON reports.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=(
            PROJECT_ROOT
            / "docs"
            / "data-quality"
            / "latest-results-executive-report.md"
        ),
        help="Markdown executive report to write.",
    )
    parser.add_argument(
        "--index",
        type=Path,
        default=(
            PROJECT_ROOT
            / "docs"
            / "data-quality"
            / "latest-results-report-index.json"
        ),
        help="Compact publication index JSON to write.",
    )
    parser.add_argument(
        "--sanitize-in-place",
        action="store_true",
        help="Rewrite source JSON files with publish-safe paths.",
    )
    return parser.parse_args(argv)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        msg = f"invalid JSON report: {path}: {exc}"
        raise SystemExit(msg) from exc


def _record_for(path: Path, payload: Any) -> ReportRecord:
    summary = _summary(payload)
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return ReportRecord(
        source=publish_safe_path(str(path)),
        issue=_issue_for(path),
        schema_version=str(_mapping(payload).get("schema_version", "") or ""),
        status=str(summary.get("status", "") or "unknown"),
        target_count=_int(summary.get("target_count")),
        rule_count=_int(summary.get("rule_count")),
        finding_count=_int(summary.get("finding_count")),
        info_count=_int(summary.get("info_count")),
        warning_count=_int(summary.get("warning_count")),
        error_count=_int(summary.get("error_count")),
        size_bytes=path.stat().st_size,
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def _summary(payload: Any) -> dict[str, Any]:
    mapping = _mapping(payload)
    summary = mapping.get("summary")
    if isinstance(summary, Mapping):
        return dict(summary)
    totals = mapping.get("totals")
    if isinstance(totals, Mapping):
        return {
            "status": mapping.get("status", "unknown"),
            "target_count": totals.get("target_count", 0),
            "rule_count": totals.get("rule_count", 0),
            "finding_count": totals.get("finding_count", 0),
            "info_count": totals.get("info_count", 0),
            "warning_count": totals.get("warning_count", 0),
            "error_count": totals.get("error_count", 0),
        }
    return {
        "status": mapping.get("status", "unknown"),
        "target_count": mapping.get("target_count", 0),
        "rule_count": mapping.get("rule_count", 0),
        "finding_count": mapping.get("finding_count", 0),
        "info_count": mapping.get("info_count", 0),
        "warning_count": mapping.get("warning_count", 0),
        "error_count": mapping.get("error_count", 0),
    }


def _finding_codes(payload: Any) -> Iterable[str]:
    for finding in _findings(payload):
        code = str(finding.get("code", "") or "")
        if code:
            yield code


def _rule_ids(payload: Any) -> Iterable[str]:
    for result in _list(_mapping(payload).get("rule_results")):
        rule_id = str(_mapping(result).get("rule_id", "") or "")
        if rule_id:
            yield rule_id


def _findings(payload: Any) -> Iterable[Mapping[str, Any]]:
    for result in _list(_mapping(payload).get("rule_results")):
        for finding in _list(_mapping(result).get("findings")):
            mapping = _mapping(finding)
            if mapping:
                yield mapping


def _issue_for(path: Path) -> str:
    for part in path.parts:
        if part.startswith("issue-") and part[6:].isdigit():
            return part
    return "unscoped"


def _records_by_issue(
    records: Iterable[ReportRecord],
) -> dict[str, list[ReportRecord]]:
    grouped: dict[str, list[ReportRecord]] = defaultdict(list)
    for record in records:
        grouped[record.issue].append(record)
    return dict(sorted(grouped.items()))


def _issue_summary(records: Iterable[ReportRecord]) -> dict[str, Any]:
    record_list = list(records)
    status_counts = Counter(record.status for record in record_list)
    return {
        "report_count": len(record_list),
        "status_counts": dict(sorted(status_counts.items())),
        "target_count": sum(record.target_count for record in record_list),
        "finding_count": sum(record.finding_count for record in record_list),
        "warning_count": sum(record.warning_count for record in record_list),
        "error_count": sum(record.error_count for record in record_list),
    }


def _issue_section(index_payload: Mapping[str, Any]) -> list[str]:
    issue_summaries = _mapping(index_payload.get("issue_summaries"))
    lines = ["## Issue-level inventory", ""]
    if not issue_summaries:
        return lines + ["No issue-scoped reports were found.", ""]
    lines.extend(
        [
            "| Issue | Reports | Statuses | Targets | Findings | Warnings | Errors |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for issue, summary_value in issue_summaries.items():
        summary = _mapping(summary_value)
        statuses = ", ".join(
            f"{key}: {value}"
            for key, value in _mapping(summary.get("status_counts")).items()
        )
        lines.append(
            f"| {issue} | {_int(summary.get('report_count')):,} | "
            f"{statuses or 'none'} | {_int(summary.get('target_count')):,} | "
            f"{_int(summary.get('finding_count')):,} | "
            f"{_int(summary.get('warning_count')):,} | "
            f"{_int(summary.get('error_count')):,} |"
        )
    lines.append("")
    return lines


def _finding_section(index_payload: Mapping[str, Any]) -> list[str]:
    lines = ["## High-signal findings", ""]
    top_codes = _list(index_payload.get("top_finding_codes"))
    top_rules = _list(index_payload.get("top_rule_ids"))
    if top_codes:
        lines.extend(
            ["### Finding codes", "", "| Code | Count |", "| --- | ---: |"]
        )
        for item in top_codes:
            row = _mapping(item)
            lines.append(
                f"| `{row.get('name', '')}` | {_int(row.get('count')):,} |"
            )
        lines.append("")
    if top_rules:
        lines.extend(["### Rule IDs", "", "| Rule | Count |", "| --- | ---: |"])
        for item in top_rules:
            row = _mapping(item)
            lines.append(
                f"| `{row.get('name', '')}` | {_int(row.get('count')):,} |"
            )
        lines.append("")
    if not top_codes and not top_rules:
        lines.extend(["No finding-level rule history was present.", ""])
    return lines


def _report_inventory_section(index_payload: Mapping[str, Any]) -> list[str]:
    reports = _list(index_payload.get("reports"))
    lines = ["## Report inventory", ""]
    if not reports:
        return lines + ["No report files were found.", ""]
    lines.extend(
        [
            "| Source | Status | Targets | Findings | Warnings | Errors |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for report in reports:
        row = _mapping(report)
        lines.append(
            f"| `{row.get('source', '')}` | {row.get('status', '')} | "
            f"{_int(row.get('target_count')):,} | "
            f"{_int(row.get('finding_count')):,} | "
            f"{_int(row.get('warning_count')):,} | "
            f"{_int(row.get('error_count')):,} |"
        )
    lines.append("")
    return lines


def _top_counts(
    counter: Counter[str], *, limit: int = 15
) -> list[dict[str, Any]]:
    return [
        {"name": name, "count": count}
        for name, count in counter.most_common(limit)
    ]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _yes_no(value: Any) -> str:
    return "yes" if bool(value) else "no"


if __name__ == "__main__":
    raise SystemExit(main())
