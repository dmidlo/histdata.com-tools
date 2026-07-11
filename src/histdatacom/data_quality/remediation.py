"""Shared remediation-hint catalog for data-quality findings."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum

from histdatacom.data_quality.contracts import QualityFinding
from histdatacom.runtime_contracts import JSONValue


class RemediationActionability(str, Enum):
    """Stable remediation actionability and boundary classifications."""

    REMEDIABLE_DEFECT = "remediable_defect"
    POLICY_OR_PROFILE_DECISION = "policy_or_profile_decision"
    UNSUPPORTED_FORMAT_OR_CAPABILITY = "unsupported_format_or_capability"
    EXPECTED_ARTIFACT_OR_CONTEXT = "expected_artifact_or_context"
    NEEDS_RULE_ATTRIBUTION = "needs_rule_attribution"
    NEEDS_DIAGNOSTIC_CONTEXT = "needs_diagnostic_context"
    UNSAFE_TO_AUTOMATE = "unsafe_to_automate"
    INFORMATIONAL_ONLY = "informational_only"


@dataclass(frozen=True, slots=True)
class RemediationActionabilityDecision:
    """One deterministic actionability decision and its stable reason."""

    actionability: RemediationActionability
    reason: str

    def to_payload(self) -> dict[str, JSONValue]:
        """Return the public JSON-compatible decision payload."""
        return {
            "actionability": self.actionability.value,
            "actionability_reason": self.reason,
        }


_POLICY_OR_PROFILE_FINDING_MARKERS = (
    "_POLICY_",
    "_POLICY_MISSING",
    "_PROFILE_",
    "_PROFILE_MISSING",
    "EXPECTED_SESSION_CLOSURE",
    "WEEKEND_ACTIVITY",
)
_EXPECTED_ARTIFACT_OR_CONTEXT_FINDING_MARKERS = (
    "EXPECTED_ARTIFACT_OR_CONTEXT",
    "EXPECTED_CONTEXT",
    "PROVENANCE_MANIFEST_UNAVAILABLE",
)
_NEEDS_DIAGNOSTIC_CONTEXT_FINDING_MARKERS = (
    "DIAGNOSTIC_CONTEXT_MISSING",
    "DIAGNOSTICS_UNAVAILABLE",
    "NEEDS_DIAGNOSTIC_CONTEXT",
)
_UNSAFE_TO_AUTOMATE_FINDING_MARKERS = (
    "UNSAFE_TO_AUTOMATE",
    "DESTRUCTIVE_REPAIR_REQUIRED",
)


def classify_remediation_actionability(
    *,
    rule_id: str,
    finding_code: str,
    severity: str,
    mapped: bool,
    attribution_status: str = "exact",
) -> RemediationActionabilityDecision:
    """Classify remediation actionability without weakening defect coverage."""
    normalized_rule = rule_id.strip().lower()
    normalized_code = finding_code.strip().upper()
    normalized_severity = severity.strip().lower()

    if mapped:
        return RemediationActionabilityDecision(
            RemediationActionability.REMEDIABLE_DEFECT,
            "mapped_remediation_hint",
        )
    if normalized_severity == "info":
        return RemediationActionabilityDecision(
            RemediationActionability.INFORMATIONAL_ONLY,
            "informational_severity",
        )
    if (
        attribution_status == "unresolved"
        or normalized_rule
        in {
            "",
            "unknown",
        }
        or normalized_rule.endswith(".unresolved")
    ):
        return RemediationActionabilityDecision(
            RemediationActionability.NEEDS_RULE_ATTRIBUTION,
            "unresolved_rule_attribution",
        )
    if normalized_rule == "inventory.format_support":
        return RemediationActionabilityDecision(
            RemediationActionability.UNSUPPORTED_FORMAT_OR_CAPABILITY,
            "unsupported_format_rule",
        )
    if "UNSUPPORTED" in normalized_code or "NOT_SUPPORTED" in normalized_code:
        return RemediationActionabilityDecision(
            RemediationActionability.UNSUPPORTED_FORMAT_OR_CAPABILITY,
            "unsupported_capability_finding",
        )
    if any(
        marker in normalized_code
        for marker in _POLICY_OR_PROFILE_FINDING_MARKERS
    ):
        return RemediationActionabilityDecision(
            RemediationActionability.POLICY_OR_PROFILE_DECISION,
            "policy_or_profile_context_required",
        )
    if any(
        marker in normalized_code
        for marker in _EXPECTED_ARTIFACT_OR_CONTEXT_FINDING_MARKERS
    ):
        return RemediationActionabilityDecision(
            RemediationActionability.EXPECTED_ARTIFACT_OR_CONTEXT,
            "expected_artifact_or_context",
        )
    if any(
        marker in normalized_code
        for marker in _NEEDS_DIAGNOSTIC_CONTEXT_FINDING_MARKERS
    ):
        return RemediationActionabilityDecision(
            RemediationActionability.NEEDS_DIAGNOSTIC_CONTEXT,
            "missing_diagnostic_context",
        )
    if any(
        marker in normalized_code
        for marker in _UNSAFE_TO_AUTOMATE_FINDING_MARKERS
    ):
        return RemediationActionabilityDecision(
            RemediationActionability.UNSAFE_TO_AUTOMATE,
            "unsafe_automatic_repair",
        )
    return RemediationActionabilityDecision(
        RemediationActionability.REMEDIABLE_DEFECT,
        "unmapped_warning_or_error",
    )


@dataclass(frozen=True, slots=True)
class QualityRemediationHint:
    """Stable operator guidance for a quality finding or summary flag."""

    code: str
    message: str
    action_kind: str
    rule_id: str = ""
    flag: str = ""
    finding_code: str = ""

    def to_payload(self) -> dict[str, JSONValue]:
        payload: dict[str, JSONValue] = {
            "code": self.code,
            "message": self.message,
            "action_kind": self.action_kind,
        }
        if self.rule_id:
            payload["rule_id"] = self.rule_id
        if self.flag:
            payload["flag"] = self.flag
        if self.finding_code:
            payload["finding_code"] = self.finding_code
        return payload


FINGERPRINT_SERIES_RULE_ID = "fingerprint.series"
ASCII_TIMESTAMP_SEQUENCE_RULE_ID = "time.ascii.sequence"
ZIP_INVENTORY_RULE_ID = "inventory.zip.integrity"

TOPOLOGY_REMEDIATION_HINTS_BY_FLAG: Mapping[str, QualityRemediationHint] = {
    "unavailable_topology": QualityRemediationHint(
        code="verify_fingerprint_source",
        message="rebuild or choose a readable fingerprint source",
        action_kind="rebuild",
        rule_id=FINGERPRINT_SERIES_RULE_ID,
        flag="unavailable_topology",
    ),
    "invalid_timestamps": QualityRemediationHint(
        code="inspect_invalid_timestamp_rows",
        message="inspect invalid timestamp rows",
        action_kind="inspect",
        rule_id=FINGERPRINT_SERIES_RULE_ID,
        flag="invalid_timestamps",
    ),
    "non_monotonic_timestamps": QualityRemediationHint(
        code="repair_timestamp_order",
        message="repair non-monotonic timestamp order",
        action_kind="repair",
        rule_id=FINGERPRINT_SERIES_RULE_ID,
        flag="non_monotonic_timestamps",
    ),
    "duplicate_timestamps": QualityRemediationHint(
        code="inspect_duplicate_timestamp_rows",
        message="inspect duplicate timestamp rows",
        action_kind="inspect",
        rule_id=FINGERPRINT_SERIES_RULE_ID,
        flag="duplicate_timestamps",
    ),
    "suspicious_gaps": QualityRemediationHint(
        code="inspect_gap_boundaries",
        message="inspect largest gap boundaries",
        action_kind="inspect",
        rule_id=FINGERPRINT_SERIES_RULE_ID,
        flag="suspicious_gaps",
    ),
    "weekend_activity": QualityRemediationHint(
        code="verify_weekend_session_policy",
        message="verify weekend-session policy",
        action_kind="verify",
        rule_id=FINGERPRINT_SERIES_RULE_ID,
        flag="weekend_activity",
    ),
}

REMEDIATION_HINTS_BY_FINDING: Mapping[
    tuple[str, str], QualityRemediationHint
] = {
    (
        ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
        "ASCII_TICK_DUPLICATE_ROW",
    ): QualityRemediationHint(
        code="inspect_duplicate_tick_rows",
        message="inspect duplicate tick rows",
        action_kind="inspect",
        rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
        finding_code="ASCII_TICK_DUPLICATE_ROW",
    ),
    (
        ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
        "ASCII_TICK_PRECISION_MISMATCH",
    ): QualityRemediationHint(
        code="repair_tick_timestamp_precision",
        message="repair tick timestamp millisecond precision",
        action_kind="repair",
        rule_id=ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
        finding_code="ASCII_TICK_PRECISION_MISMATCH",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "HISTDATA_ZIP_FILENAME_INVALID",
    ): QualityRemediationHint(
        code="rename_histdata_zip_archive",
        message="rename ZIP archive to the expected HistData filename",
        action_kind="repair",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="HISTDATA_ZIP_FILENAME_INVALID",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "HISTDATA_ZIP_MEMBER_FILENAME_INVALID",
    ): QualityRemediationHint(
        code="rename_histdata_zip_member",
        message="rename ZIP member to the expected HistData member filename",
        action_kind="repair",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="HISTDATA_ZIP_MEMBER_FILENAME_INVALID",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "ZIP_MEMBER_MISSING",
    ): QualityRemediationHint(
        code="restore_expected_zip_member",
        message="restore the expected HistData data member inside the ZIP",
        action_kind="rebuild",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="ZIP_MEMBER_MISSING",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "ZIP_MEMBER_UNEXPECTED",
    ): QualityRemediationHint(
        code="rebuild_expected_zip_member",
        message="rebuild ZIP contents so the expected HistData data member is present",
        action_kind="rebuild",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="ZIP_MEMBER_UNEXPECTED",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "ZIP_EXTRA_MEMBER",
    ): QualityRemediationHint(
        code="inspect_unexpected_zip_members",
        message="inspect unexpected extra ZIP members",
        action_kind="inspect",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="ZIP_EXTRA_MEMBER",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "ZIP_CRC_ERROR",
    ): QualityRemediationHint(
        code="redownload_zip_crc_failure",
        message="redownload or replace the ZIP archive with CRC failures",
        action_kind="rebuild",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="ZIP_CRC_ERROR",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "ZIP_CORRUPT",
    ): QualityRemediationHint(
        code="redownload_corrupt_zip_archive",
        message="redownload or replace the corrupt ZIP archive",
        action_kind="rebuild",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="ZIP_CORRUPT",
    ),
    (
        ZIP_INVENTORY_RULE_ID,
        "ZIP_UNREADABLE",
    ): QualityRemediationHint(
        code="restore_zip_read_access",
        message="restore ZIP archive read access or replace the archive",
        action_kind="repair",
        rule_id=ZIP_INVENTORY_RULE_ID,
        finding_code="ZIP_UNREADABLE",
    ),
}
REMEDIATION_HINTS_BY_FINDING_CODE: Mapping[str, QualityRemediationHint] = {
    hint.finding_code: hint
    for hint in REMEDIATION_HINTS_BY_FINDING.values()
    if hint.finding_code
}


def remediation_hints_for_flags(
    flags: Iterable[str],
) -> tuple[QualityRemediationHint, ...]:
    hints: list[QualityRemediationHint] = []
    seen: set[str] = set()
    for flag in flags:
        if flag in seen:
            continue
        seen.add(flag)
        hint = TOPOLOGY_REMEDIATION_HINTS_BY_FLAG.get(flag)
        if hint is not None:
            hints.append(hint)
    return tuple(hints)


def remediation_hint_payloads_for_flags(
    flags: Iterable[str],
) -> list[JSONValue]:
    return [hint.to_payload() for hint in remediation_hints_for_flags(flags)]


def remediation_hints_for_finding_code(
    finding_code: str,
    *,
    rule_id: str = "",
) -> tuple[QualityRemediationHint, ...]:
    hint = (
        REMEDIATION_HINTS_BY_FINDING.get((rule_id, finding_code))
        if rule_id
        else REMEDIATION_HINTS_BY_FINDING_CODE.get(finding_code)
    )
    if hint is None:
        return ()
    return (hint,)


def remediation_hint_payloads_for_finding(
    finding: QualityFinding,
) -> list[JSONValue]:
    return [
        hint.to_payload()
        for hint in remediation_hints_for_finding_code(
            finding.code,
            rule_id=finding.rule_id,
        )
    ]
