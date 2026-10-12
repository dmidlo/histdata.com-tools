"""Content-addressed ecosystem governance, distinct from rights and trust."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import urlsplit

from histdatacom.broker_plugin_registry.contracts import (
    BrokerPluginCandidateV1,
    semver_key,
)
from histdatacom.broker_plugins.contracts import (
    BROKER_PLUGIN_SUPPORTED_SDK_VERSIONS,
    _ns,
    _semver,
    _text,
)
from histdatacom.broker_plugin_provenance._wire import Artifact as _Artifact

if TYPE_CHECKING:
    from histdatacom.broker_plugin_conformance.retained import (
        RetainedConformanceReport,
    )

MIN_DEPRECATION_NOTICE_DAYS = 180
MIN_DEPRECATION_NOTICE_MINOR_RELEASES = 2
_NOTICE_NS = MIN_DEPRECATION_NOTICE_DAYS * 86_400 * 1_000_000_000


class BrokerSDKUse(str, Enum):
    LIVE = "live_capture"
    REPLAY = "retained_replay"


@dataclass(frozen=True, slots=True)
class BrokerSDKSupportV1(_Artifact):
    """An explicit notice may end live admission, never silently end replay."""

    sdk_version: str
    deprecated_since_host: str | None = None
    announced_at_utc_ns: int | None = None
    live_removed_since_host: str | None = None
    live_removal_not_before_utc_ns: int | None = None
    KIND: ClassVar[str] = "sdk-support"

    def _validate(self) -> None:
        if self.sdk_version not in BROKER_PLUGIN_SUPPORTED_SDK_VERSIONS:
            raise ValueError(
                "SDK generation has no executable host implementation"
            )
        if (self.deprecated_since_host is None) != (
            self.announced_at_utc_ns is None
        ):
            raise ValueError("deprecation requires an exact announcement")
        if (self.live_removed_since_host is None) != (
            self.live_removal_not_before_utc_ns is None
        ):
            raise ValueError("removal requires host and time boundaries")
        if self.deprecated_since_host is not None:
            _semver(self.deprecated_since_host)
            assert self.announced_at_utc_ns is not None
            _ns(self.announced_at_utc_ns, "announcement")
        if self.live_removed_since_host is not None:
            if (
                self.deprecated_since_host is None
                or self.announced_at_utc_ns is None
            ):
                raise ValueError(
                    "removal cannot precede a published deprecation"
                )
            removed = semver_key(self.live_removed_since_host)
            deprecated = semver_key(self.deprecated_since_host)
            if removed <= deprecated or not (
                removed[0] > deprecated[0]
                or removed[0] == deprecated[0]
                and removed[1]
                >= deprecated[1] + MIN_DEPRECATION_NOTICE_MINOR_RELEASES
            ):
                raise ValueError(
                    "removal needs two host minor releases or a new major"
                )
            assert self.live_removal_not_before_utc_ns is not None
            _ns(self.live_removal_not_before_utc_ns, "removal")
            if (
                self.live_removal_not_before_utc_ns
                < self.announced_at_utc_ns + _NOTICE_NS
            ):
                raise ValueError("removal needs at least 180 days notice")


@dataclass(frozen=True, slots=True)
class BrokerSDKCompatibilityPolicyV1(_Artifact):
    versions: tuple[BrokerSDKSupportV1, ...]
    policy_version: str = "1.0.0"
    replay_removal_policy: str = (
        "retained_no_removal_without_golden_corpus_and_registry_successor"
    )
    KIND: ClassVar[str] = "sdk-compatibility-policy"

    def _validate(self) -> None:
        names = tuple(item.sdk_version for item in self.versions)
        if not names or names != tuple(sorted(set(names), key=semver_key)):
            raise ValueError("SDK support must be nonempty, ordered and unique")
        # V1 was first introduced with these two implemented generations.
        # Pin its replay inventory, not a future host's changing default list.
        if names != ("1.0.0", "1.1.0"):
            raise ValueError("V1 policy must retain both SDK generations")
        if self.policy_version != "1.0.0" or self.replay_removal_policy != (
            "retained_no_removal_without_golden_corpus_and_registry_successor"
        ):
            raise ValueError("unsupported compatibility policy semantics")


def broker_sdk_compatibility_policy() -> BrokerSDKCompatibilityPolicyV1:
    """Both implemented generations remain live and replay supported today."""
    return BrokerSDKCompatibilityPolicyV1(
        tuple(
            BrokerSDKSupportV1(version)
            for version in BROKER_PLUGIN_SUPPORTED_SDK_VERSIONS
        )
    )


def _decision_fields(
    candidate: BrokerPluginCandidateV1,
    policy: BrokerSDKCompatibilityPolicyV1,
    host_version: str,
    use: BrokerSDKUse,
    evaluated_at_utc_ns: int,
    requested_sdk_version: str | None,
) -> tuple[tuple[str, ...], str | None, str, tuple[str, ...]]:
    _semver(host_version)
    _ns(evaluated_at_utc_ns, "compatibility time")
    if requested_sdk_version is not None:
        _semver(requested_sdk_version)
    compatible: list[str] = []
    warnings: list[str] = []
    for support in policy.versions:
        if not candidate.registration.supports_sdk(support.sdk_version):
            continue
        deprecated = (
            support.deprecated_since_host is not None
            and support.announced_at_utc_ns is not None
            and semver_key(host_version)
            >= semver_key(support.deprecated_since_host)
            and evaluated_at_utc_ns >= support.announced_at_utc_ns
        )
        removed = (
            support.live_removed_since_host is not None
            and support.live_removal_not_before_utc_ns is not None
            and semver_key(host_version)
            >= semver_key(support.live_removed_since_host)
            and evaluated_at_utc_ns >= support.live_removal_not_before_utc_ns
        )
        if removed and use is BrokerSDKUse.LIVE:
            continue
        compatible.append(support.sdk_version)
        if (
            requested_sdk_version is None
            or support.sdk_version == requested_sdk_version
        ):
            if deprecated:
                warnings.append("sdk_deprecated:" + support.sdk_version)
            if removed:
                warnings.append(
                    "sdk_retained_replay_only:" + support.sdk_version
                )
    selected = (
        requested_sdk_version
        if requested_sdk_version in compatible
        else (
            compatible[-1]
            if requested_sdk_version is None and compatible
            else None
        )
    )
    return (
        tuple(compatible),
        selected,
        (
            "compatible"
            if selected is not None
            else "no_supported_sdk_intersection"
        ),
        tuple(sorted(warnings)),
    )


@dataclass(frozen=True, slots=True)
class BrokerSDKCompatibilityDecisionV1(_Artifact):
    candidate_json: str
    policy: BrokerSDKCompatibilityPolicyV1
    host_version: str
    use: BrokerSDKUse
    evaluated_at_utc_ns: int
    requested_sdk_version: str | None
    compatible_versions: tuple[str, ...]
    selected_sdk_version: str | None
    reason: str
    warnings: tuple[str, ...]
    KIND: ClassVar[str] = "sdk-compatibility-decision"

    def _validate(self) -> None:
        candidate = BrokerPluginCandidateV1.from_json(self.candidate_json)
        if candidate.to_json() != self.candidate_json:
            raise ValueError("candidate must retain exact canonical bytes")
        expected = _decision_fields(
            candidate,
            self.policy,
            self.host_version,
            self.use,
            self.evaluated_at_utc_ns,
            self.requested_sdk_version,
        )
        if expected != (
            self.compatible_versions,
            self.selected_sdk_version,
            self.reason,
            self.warnings,
        ):
            raise ValueError(
                "compatibility decision differs from retained policy"
            )

    def require_admitted(self) -> None:
        if self.selected_sdk_version is None:
            raise ValueError(self.reason)


def negotiate_broker_sdk(
    candidate: BrokerPluginCandidateV1,
    *,
    host_version: str,
    use: BrokerSDKUse,
    evaluated_at_utc_ns: int,
    requested_sdk_version: str | None = None,
    policy: BrokerSDKCompatibilityPolicyV1 | None = None,
) -> BrokerSDKCompatibilityDecisionV1:
    """Intersect an installed declaration with executable generations, not pip success."""
    candidate = BrokerPluginCandidateV1.from_json(candidate.to_json())
    selected_policy = (
        policy if policy is not None else broker_sdk_compatibility_policy()
    )
    fields = _decision_fields(
        candidate,
        selected_policy,
        host_version,
        use,
        evaluated_at_utc_ns,
        requested_sdk_version,
    )
    return BrokerSDKCompatibilityDecisionV1(
        candidate.to_json(),
        selected_policy,
        host_version,
        use,
        evaluated_at_utc_ns,
        requested_sdk_version,
        *fields,
    )


@dataclass(frozen=True, slots=True)
class BrokerConformanceReferenceV1(_Artifact):
    """Exact report bytes as evidence metadata; parsing is not requalification."""

    kit_version: str
    catalog_json: str
    report_json: str
    evidence_ids: tuple[str, ...]
    status: str = "retained_report_not_independently_requalified"
    KIND: ClassVar[str] = "conformance-reference"

    def _validate(self) -> None:
        from histdatacom.broker_plugin_conformance.contracts import (
            BrokerConformanceCatalogV1,
        )

        _semver(self.kit_version)
        catalog = BrokerConformanceCatalogV1.from_json(self.catalog_json)
        report = self.retained_report
        if (
            catalog.to_json() != self.catalog_json
            or catalog.version != self.kit_version
            or report.plan.catalog_id != catalog.artifact_id
        ):
            raise ValueError(
                "conformance kit version differs from exact catalog"
            )
        if self.status != "retained_report_not_independently_requalified":
            raise ValueError("invalid retained conformance evidence")
        expected = tuple(
            sorted(
                set(
                    evidence
                    for result in report.results
                    for evidence in result.evidence_ids
                )
            )
        )
        if self.evidence_ids != expected:
            raise ValueError(
                "conformance evidence inventory differs from exact report"
            )

    @property
    def retained_report(self) -> RetainedConformanceReport:
        from histdatacom.broker_plugin_conformance.contracts import (
            BrokerConformanceCatalogV1,
        )
        from histdatacom.broker_plugin_conformance.retained import (
            read_retained_conformance_report,
        )

        return read_retained_conformance_report(
            self.report_json,
            BrokerConformanceCatalogV1.from_json(self.catalog_json),
        )


def _public_url(value: str) -> None:
    _text(value, "public URL", 512)
    url = urlsplit(value)
    if (
        url.scheme != "https"
        or not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise ValueError("directory URL must be public credential-free HTTPS")


@dataclass(frozen=True, slots=True)
class BrokerPluginDirectoryEntryV1(_Artifact):
    """Community directory metadata, never installation or scientific admission."""

    candidate_json: str
    maintainer: str
    package_url: str
    source_url: str
    status: str
    latest_conformance: BrokerConformanceReferenceV1 | None = None
    nonclaim: str = (
        "discovery_only_no_endorsement_authorization_security_or_scientific_qualification"
    )
    KIND: ClassVar[str] = "directory-entry"

    def _validate(self) -> None:
        candidate = BrokerPluginCandidateV1.from_json(self.candidate_json)
        if candidate.to_json() != self.candidate_json:
            raise ValueError("directory candidate must be exact")
        _text(self.maintainer, "maintainer", 256)
        _public_url(self.package_url)
        _public_url(self.source_url)
        if self.status not in (
            "community",
            "first-party",
            "deprecated",
            "unverified",
        ):
            raise ValueError("unknown directory status")
        if (
            self.nonclaim
            != "discovery_only_no_endorsement_authorization_security_or_scientific_qualification"
        ):
            raise ValueError("directory cannot confer trust")
        if self.latest_conformance is not None:
            report = self.latest_conformance.retained_report
            if report.plan.driver.candidate_id != candidate.artifact_id:
                raise ValueError(
                    "directory evidence belongs to another candidate"
                )
