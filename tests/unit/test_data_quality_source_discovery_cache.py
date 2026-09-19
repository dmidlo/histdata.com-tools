"""Content-validated, bounded reuse of static source-finding discovery."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
import json
import os
from pathlib import Path

import pytest

from histdatacom.data_quality import remediation, remediation_audit as audit
from histdatacom.data_quality import reporting
from histdatacom.data_quality.bounded_payload_contracts import (
    bounded_payload_contract_audit,
    representative_bounded_quality_payload,
    representative_quality_report,
)
from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
)
from histdatacom.data_quality.fingerprint_discovery import (
    fingerprint_contract_audit,
)


@pytest.fixture(autouse=True)
def empty_discovery_cache() -> Iterator[None]:
    """Keep cache tests independent of earlier discovery calls."""
    with audit._SOURCE_FINDINGS_CACHE_LOCK:
        audit._SOURCE_FINDINGS_CACHE.clear()
    yield
    with audit._SOURCE_FINDINGS_CACHE_LOCK:
        audit._SOURCE_FINDINGS_CACHE.clear()


@pytest.fixture
def parse_calls(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Count expensive parsing, without wall-clock timing assertions."""
    calls: list[Path] = []
    original = audit._parse_known_findings

    def parse(
        source: str,
        path: Path,
        root: Path,
        prefixes: tuple[tuple[str, str], ...],
    ) -> tuple[audit.KnownQualityFindingCode, ...]:
        calls.append(path)
        return original(source, path, root, prefixes)

    monkeypatch.setattr(audit, "_parse_known_findings", parse)
    return calls


def _write_source(path: Path, code: str = "CUSTOM_OLD") -> None:
    path.write_text(
        f'finding(code="{code}", rule_id="custom.rule")\n',
        encoding="utf-8",
    )


def test_discovery_reuses_immutable_results_after_fresh_read(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "custom.py"
    _write_source(source)
    first = audit.discover_known_quality_findings(tmp_path)
    second = audit.discover_known_quality_findings(tmp_path)

    assert first == second
    assert first[0] is second[0]
    assert parse_calls == [source]
    with pytest.raises(FrozenInstanceError):
        setattr(first[0], "finding_code", "CONTAMINATED_CODE")

    original = Path.read_text
    reads = []

    def read(path: Path, *args: object, **kwargs: object) -> str:
        reads.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    assert audit.discover_known_quality_findings(tmp_path) == first
    assert reads == [source]
    assert all(
        isinstance(part, bytes) and len(part) == 32
        for key in audit._SOURCE_FINDINGS_CACHE
        for part in key
    )


def test_same_size_restored_mtime_edit_invalidates_discovery(
    tmp_path: Path, parse_calls: list[Path]
) -> None:
    source = tmp_path / "custom.py"
    _write_source(source)
    old_stat = source.stat()
    assert audit.discover_known_quality_findings(tmp_path)[0].finding_code == (
        "CUSTOM_OLD"
    )
    _write_source(source, "CUSTOM_NEW")
    os.utime(source, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
    assert source.stat().st_size == old_stat.st_size
    assert source.stat().st_mtime_ns == old_stat.st_mtime_ns
    assert audit.discover_known_quality_findings(tmp_path)[0].finding_code == (
        "CUSTOM_NEW"
    )
    assert parse_calls == [source, source]


def test_add_remove_and_rename_refresh_membership_and_context(
    tmp_path: Path, parse_calls: list[Path]
) -> None:
    first = tmp_path / "first.py"
    second = tmp_path / "second.py"
    renamed = tmp_path / "calendar.py"
    _write_source(first)
    assert len(audit.discover_known_quality_findings(tmp_path)) == 1
    _write_source(second)
    assert len(audit.discover_known_quality_findings(tmp_path)) == 2
    first.unlink()
    assert len(audit.discover_known_quality_findings(tmp_path)) == 1
    second.rename(renamed)
    finding = audit.discover_known_quality_findings(tmp_path)[0]
    assert finding.source == "data_quality/calendar.py:1"
    assert finding.source_family == "domain/calendar"
    assert parse_calls == [first, second, renamed]


def test_distinct_roots_and_relative_source_labels_do_not_share_entries(
    tmp_path: Path, parse_calls: list[Path]
) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    for root in (left, right):
        _write_source(root / "custom.py")
    assert audit.discover_known_quality_findings(left) == (
        audit.discover_known_quality_findings(right)
    )
    finding = audit._known_findings_from_source(
        left / "custom.py", root=tmp_path
    )[0]
    assert finding.source == "data_quality/left/custom.py:1"
    assert len(parse_calls) == 3


def test_prefix_policy_changes_invalidate_both_attribution_fields(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "custom.py"
    source.write_text('finding(code="CUSTOM_INVALID_INPUT")\n')
    first = audit.discover_known_quality_findings(tmp_path)[0]
    assert first.rule_id == "custom.unresolved"
    assert first.finding_code_prefix == "CUSTOM_INVALID_INPUT"
    monkeypatch.setattr(
        audit, "_FINDING_CODE_RULE_PREFIXES", (("CUSTOM_", "custom.rule"),)
    )
    second = audit.discover_known_quality_findings(tmp_path)[0]
    assert second.rule_id == "custom.rule"
    assert second.finding_code_prefix == "CUSTOM"
    assert len(parse_calls) == 2


def test_policy_snapshot_stays_consistent_during_analysis(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "custom.py"
    source.write_text('finding(code="CUSTOM_INVALID_INPUT")\n')
    original = audit._parse_known_findings

    def parse(
        text: str,
        path: Path,
        root: Path,
        prefixes: tuple[tuple[str, str], ...],
    ) -> tuple[audit.KnownQualityFindingCode, ...]:
        monkeypatch.setattr(
            audit, "_FINDING_CODE_RULE_PREFIXES", (("CUSTOM_", "custom.rule"),)
        )
        return original(text, path, root, prefixes)

    monkeypatch.setattr(audit, "_parse_known_findings", parse)
    first = audit.discover_known_quality_findings(tmp_path)[0]
    assert first.rule_id == "custom.unresolved"
    assert first.finding_code_prefix == "CUSTOM_INVALID_INPUT"
    second = audit.discover_known_quality_findings(tmp_path)[0]
    assert second.rule_id == "custom.rule"
    assert second.finding_code_prefix == "CUSTOM"


def test_read_failure_never_falls_back_to_cached_success(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "custom.py"
    _write_source(source)
    first = audit.discover_known_quality_findings(tmp_path)

    def unreadable(*args: object, **kwargs: object) -> str:
        raise PermissionError("source is unreadable")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_text", unreadable)
        assert audit.discover_known_quality_findings(tmp_path) == ()
    assert audit.discover_known_quality_findings(tmp_path) == first
    assert parse_calls == [source]


def test_syntax_failure_is_not_retained_and_recovery_is_fresh(
    tmp_path: Path, parse_calls: list[Path]
) -> None:
    source = tmp_path / "custom.py"
    _write_source(source)
    audit.discover_known_quality_findings(tmp_path)
    source.write_text("def invalid(\n", encoding="utf-8")
    assert audit.discover_known_quality_findings(tmp_path) == ()
    assert audit.discover_known_quality_findings(tmp_path) == ()
    _write_source(source, "CUSTOM_NEW")
    assert audit.discover_known_quality_findings(tmp_path)[0].finding_code == (
        "CUSTOM_NEW"
    )
    assert len(parse_calls) == 4


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_original_text_decoding_and_newline_semantics(
    tmp_path: Path, newline: str
) -> None:
    source = tmp_path / "custom.py"
    source.write_bytes(
        f'# comment{newline}finding(code="CUSTOM_OLD"){newline}'.encode()
    )
    assert audit.discover_known_quality_findings(tmp_path)[0].source.endswith(
        ":2"
    )
    source.write_bytes(b'\xef\xbb\xbffinding(code="CUSTOM_OLD")\n')
    assert audit.discover_known_quality_findings(tmp_path) == ()
    source.write_bytes(b'# coding: latin-1\n# \xff\nfinding(code="CUSTOM_OLD")')
    with pytest.raises(UnicodeDecodeError):
        audit.discover_known_quality_findings(tmp_path)


def test_escaped_surrogate_literals_keep_uncached_behavior(
    tmp_path: Path, parse_calls: list[Path]
) -> None:
    source = tmp_path / "custom.py"
    source.write_text(r'finding(code="CUSTOM_\ud800", rule_id="custom.rule")')
    for _ in range(2):
        assert audit.discover_known_quality_findings(tmp_path)[
            0
        ].finding_code == ("CUSTOM_\ud800")
    assert parse_calls == [source, source]
    assert not audit._SOURCE_FINDINGS_CACHE


@pytest.mark.parametrize(
    "limit",
    [
        "_SOURCE_FINDINGS_CACHE_SOURCE_BYTE_LIMIT",
        "_SOURCE_FINDINGS_CACHE_ITEM_LIMIT",
        "_SOURCE_FINDINGS_CACHE_TEXT_BYTE_LIMIT",
    ],
)
def test_oversized_inputs_bypass_retention_without_refusal(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
    limit: str,
) -> None:
    source = tmp_path / "custom.py"
    source.write_text('finding(code="CUSTOM_ONE")\nfinding(code="CUSTOM_TWO")')
    monkeypatch.setattr(audit, limit, 1)
    first = audit.discover_known_quality_findings(tmp_path)
    assert len(first) == 2
    assert audit.discover_known_quality_findings(tmp_path) == first
    assert parse_calls == [source, source]
    assert not audit._SOURCE_FINDINGS_CACHE


def test_source_byte_bound_counts_multibyte_text(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "custom.py"
    text = '# éééé\nfinding(code="CUSTOM_ONE")'
    source.write_text(text, encoding="utf-8")
    monkeypatch.setattr(
        audit, "_SOURCE_FINDINGS_CACHE_SOURCE_BYTE_LIMIT", len(text)
    )
    first = audit.discover_known_quality_findings(tmp_path)
    assert audit.discover_known_quality_findings(tmp_path) == first
    assert len(parse_calls) == 2
    assert not audit._SOURCE_FINDINGS_CACHE


def test_entry_budget_evicts_least_recently_used_results(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(audit, "_SOURCE_FINDINGS_CACHE_LIMIT", 2)
    paths = [tmp_path / f"custom_{index}.py" for index in range(3)]
    for path in paths:
        _write_source(path)
    for index in (0, 1, 0, 2, 0, 1):
        assert audit._known_findings_from_source(paths[index], root=tmp_path)
        assert len(audit._SOURCE_FINDINGS_CACHE) <= 2
    assert parse_calls == [paths[0], paths[1], paths[2], paths[1]]


def test_concurrent_lookup_and_eviction_preserve_results_and_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(audit, "_SOURCE_FINDINGS_CACHE_LIMIT", 2)
    paths = [tmp_path / f"custom_{index}.py" for index in range(8)]
    for path in paths:
        _write_source(path)

    def discover(path: Path) -> tuple[audit.KnownQualityFindingCode, ...]:
        return audit._known_findings_from_source(path, root=tmp_path)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = tuple(executor.map(discover, paths * 8))
    assert all(result[0].finding_code == "CUSTOM_OLD" for result in results)
    assert len(audit._SOURCE_FINDINGS_CACHE) <= 2


def test_catalog_and_report_changes_are_live_with_reused_discovery(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_source(tmp_path / "custom.py")
    discover = audit.discover_known_quality_findings
    monkeypatch.setattr(
        audit, "discover_known_quality_findings", lambda: discover(tmp_path)
    )
    first = audit.audit_remediation_catalog()
    assert first["status"] == "needs-remediation-guidance"
    hint = remediation.QualityRemediationHint(
        code="repair_custom", message="repair", action_kind="repair"
    )
    monkeypatch.setattr(
        remediation,
        "REMEDIATION_HINTS_BY_FINDING",
        {
            **remediation.REMEDIATION_HINTS_BY_FINDING,
            ("custom.rule", "CUSTOM_OLD"): hint,
        },
    )
    second = audit.audit_remediation_catalog()
    assert second["status"] == "covered"
    target = QualityTarget(path="custom.csv")
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="custom.rule",
                target=target,
                findings=(
                    QualityFinding(
                        severity=QualitySeverity.ERROR,
                        code="CUSTOM_NEW",
                        message="new gap",
                        rule_id="custom.rule",
                        target=target,
                    ),
                ),
            ),
        ),
    )
    third = audit.audit_remediation_catalog(reports=(report,))
    assert third["status"] == "needs-remediation-guidance"
    assert third["report_coverage"] != second["report_coverage"]
    assert len(parse_calls) == 1


def test_report_audit_and_cli_bytes_match_uncached_cold_and_warm_paths(
    tmp_path: Path,
    parse_calls: list[Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise real renderers/recursion, with a small source-discovery root."""
    _write_source(tmp_path / "custom.py")
    discover = audit.discover_known_quality_findings
    monkeypatch.setattr(
        audit, "discover_known_quality_findings", lambda: discover(tmp_path)
    )
    report = representative_quality_report()

    def encoded_surfaces() -> str:
        return json.dumps(
            {
                "full": reporting.quality_report_to_json(report),
                "bounded": representative_bounded_quality_payload(),
                "cli": reporting.format_quality_console_summary(report),
                "fingerprint_audit": fingerprint_contract_audit(),
                "bounded_audit": bounded_payload_contract_audit(),
                "remediation_audit": audit.audit_remediation_catalog(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    with monkeypatch.context() as patch:
        patch.setattr(audit, "_SOURCE_FINDINGS_CACHE_LIMIT", 0)
        uncached = encoded_surfaces()
    # Three individual renderers (4 scans each), fingerprint (12), bounded
    # audit (4), and remediation (1): this guards the diagnosed repeated path.
    assert len(parse_calls) == 29
    parse_calls.clear()
    cold = encoded_surfaces()
    warm = encoded_surfaces()
    assert uncached == cold == warm
    assert len(parse_calls) == 1
    assert not reporting._FINGERPRINT_REPORT_SURFACE_EVIDENCE_ACTIVE
