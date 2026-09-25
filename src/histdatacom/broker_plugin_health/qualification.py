"""Immutable health references for a new native scientific fingerprint fit.

The native V1 fingerprint is unchanged. This separate provenance must be checked
against explicit source captures; reading its JSON alone does not qualify data.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
from typing import TYPE_CHECKING, ClassVar

from ._wire import Artifact
from .contracts import BrokerHostHealthAuditV1
from .storage import _read, _sync

if TYPE_CHECKING:
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureSessionManifestV1,
    )
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerLegacyCaptureV1
    from histdatacom.broker_plugin_policy.health_bindings import (
        BrokerHostHealthEvidenceV1,
    )


@dataclass(frozen=True, slots=True)
class BrokerHostHealthCaptureReferenceV1(Artifact):
    session_id: str
    manifest_id: str
    audit_id: str
    policy_id: str
    KIND: ClassVar[str] = "capture-qualification-reference"

    def _validate(self) -> None:
        for value in (
            self.session_id,
            self.manifest_id,
            self.audit_id,
            self.policy_id,
        ):
            if not re.fullmatch(r"[a-zA-Z0-9_.:-]{1,512}", value):
                raise ValueError("invalid host health qualification reference")
        if not re.fullmatch(
            r"broker-host-health-audit:sha256:[a-f0-9]{64}", self.audit_id
        ) or not re.fullmatch(
            r"broker-host-health-policy:sha256:[a-f0-9]{64}", self.policy_id
        ):
            raise ValueError("wrong host health reference family")


@dataclass(frozen=True, slots=True)
class BrokerHostHealthQualificationV1(Artifact):
    fingerprint_id: str
    native_fingerprint_sha256: str
    captures: tuple[BrokerHostHealthCaptureReferenceV1, ...]
    KIND: ClassVar[str] = "fingerprint-qualification"

    def _validate(self) -> None:
        if not re.fullmatch(r"[a-zA-Z0-9_.:-]{1,512}", self.fingerprint_id):
            raise ValueError("invalid qualified native fingerprint identity")
        if not re.fullmatch(r"[a-f0-9]{64}", self.native_fingerprint_sha256):
            raise ValueError("invalid qualified native fingerprint hash")
        sessions = tuple(item.session_id for item in self.captures)
        if (
            not sessions
            or len(sessions) > 1024
            or sessions != tuple(sorted(set(sessions)))
        ):
            raise ValueError("invalid health qualification capture inventory")


def _current_qualification(
    root: str | Path,
    fingerprint: BrokerDeliveryFingerprintV1,
    manifests: Sequence[BrokerCaptureSessionManifestV1],
    requests: Sequence[BrokerLegacyCaptureV1],
) -> tuple[
    BrokerHostHealthQualificationV1, tuple[BrokerHostHealthEvidenceV1, ...]
]:
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureSessionManifestV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerLegacyCaptureV1
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyOperation
    from histdatacom.broker_plugin_policy.scope import (
        require_provider_operation,
    )
    from histdatacom.broker_plugin_policy.health_bindings import (
        BrokerHostHealthEvidenceV1,
    )
    from .runtime_legacy import require_legacy_capture_health

    if type(fingerprint) is not BrokerDeliveryFingerprintV1:
        raise ValueError("exact native health-qualified fingerprint required")
    fingerprint = BrokerDeliveryFingerprintV1.from_json(fingerprint.to_json())
    require_provider_operation(fingerprint, BrokerPolicyOperation.MATERIAL_USE)
    if len(manifests) != len(requests) or len(manifests) != len(
        fingerprint.capture_evidence
    ):
        raise ValueError(
            "health qualification requires complete native capture inventory"
        )
    if any(
        type(item) is not BrokerCaptureSessionManifestV1 for item in manifests
    ) or any(type(item) is not BrokerLegacyCaptureV1 for item in requests):
        raise ValueError("exact native health qualification inputs required")
    manifest_map = {item.session.session_id: item for item in manifests}
    request_map = {item.session.session_id: item for item in requests}
    evidence_map = {
        item.session_id: item for item in fingerprint.capture_evidence
    }
    if (
        len(manifest_map) != len(manifests)
        or len(request_map) != len(requests)
        or set(manifest_map) != set(evidence_map)
        or set(request_map) != set(evidence_map)
    ):
        raise ValueError(
            "health qualification capture substitution or duplicate"
        )
    references = []
    evidence = []
    for session_id in sorted(manifest_map):
        manifest = manifest_map[session_id]
        if manifest.manifest_id != evidence_map[session_id].manifest_id:
            raise ValueError(
                "health qualification native manifest substitution"
            )
        audit: BrokerHostHealthAuditV1 = require_legacy_capture_health(
            root,
            manifest,
            provider_request=request_map[session_id],
        )
        references.append(
            BrokerHostHealthCaptureReferenceV1(
                session_id,
                manifest.manifest_id,
                audit.artifact_id,
                audit.header.policy.artifact_id,
            )
        )
        evidence.append(
            BrokerHostHealthEvidenceV1(
                request_map[session_id],
                manifest.session,
                audit.header,
                audit,
            )
        )
    qualification = BrokerHostHealthQualificationV1(
        fingerprint.fingerprint_id,
        hashlib.sha256(fingerprint.to_json().encode("utf-8")).hexdigest(),
        tuple(references),
    )
    _fresh_material_use(fingerprint, requests, evidence)
    return qualification, tuple(evidence)


def _fresh_material_use(
    fingerprint: BrokerDeliveryFingerprintV1,
    requests: Sequence[BrokerLegacyCaptureV1],
    evidence: Sequence[BrokerHostHealthEvidenceV1],
) -> None:
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyOperation
    from histdatacom.broker_plugin_policy.scope import (
        require_provider_operation,
    )

    # A later source replay can observe revocation of an earlier source. Final
    # admission checks every actual source plus its health output, not IDs or
    # stored historical decisions alone.
    for request in requests:
        require_provider_operation(request, BrokerPolicyOperation.MATERIAL_USE)
    for audit in evidence:
        require_provider_operation(audit, BrokerPolicyOperation.MATERIAL_USE)
    require_provider_operation(fingerprint, BrokerPolicyOperation.MATERIAL_USE)


def _path(
    root: str | Path, qualification: BrokerHostHealthQualificationV1
) -> Path:
    # The native digest selects a single immutable qualification. A different
    # audit/policy for the same V1 native fingerprint is a conflict, not overwrite.
    name = (
        hashlib.sha256(qualification.fingerprint_id.encode("ascii")).hexdigest()
        + ".json"
    )
    return Path(root).resolve(strict=True) / "host-health-qualifications" / name


def write_broker_health_qualification(
    root: str | Path,
    fingerprint: BrokerDeliveryFingerprintV1,
    manifests: Sequence[BrokerCaptureSessionManifestV1],
    provider_requests: Sequence[BrokerLegacyCaptureV1],
) -> BrokerHostHealthQualificationV1:
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyOperation
    from histdatacom.broker_plugin_policy.scope import (
        require_provider_operation,
    )

    qualification, evidence = _current_qualification(
        root, fingerprint, manifests, provider_requests
    )
    require_provider_operation(fingerprint, BrokerPolicyOperation.RETAIN_LOCAL)
    path = _path(root, qualification)
    path.parent.mkdir(mode=0o700, exist_ok=True)
    if path.parent.is_symlink() or not path.parent.is_dir():
        raise ValueError("host health qualification directory is not ordinary")
    _sync(path.parent.parent)
    _fresh_material_use(fingerprint, provider_requests, evidence)
    require_provider_operation(fingerprint, BrokerPolicyOperation.RETAIN_LOCAL)
    text = qualification.to_json()
    try:
        fd = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
    except FileExistsError:
        if _read(path) != text:
            raise ValueError(
                "immutable health qualification conflict"
            ) from None
    else:
        with os.fdopen(fd, "wb") as stream:
            stream.write(text.encode("ascii"))
            stream.flush()
            os.fsync(stream.fileno())
        _sync(path.parent)
    _fresh_material_use(fingerprint, provider_requests, evidence)
    return qualification


def read_current_broker_health_qualification(
    root: str | Path,
    fingerprint: BrokerDeliveryFingerprintV1,
    manifests: Sequence[BrokerCaptureSessionManifestV1],
    provider_requests: Sequence[BrokerLegacyCaptureV1],
) -> BrokerHostHealthQualificationV1:
    """Recheck actual source health and rights; never trust a stored permit."""
    expected, evidence = _current_qualification(
        root, fingerprint, manifests, provider_requests
    )
    path = _path(root, expected)
    if path.parent.is_symlink():
        raise ValueError("host health qualification directory is not ordinary")
    stored = BrokerHostHealthQualificationV1.from_json(_read(path))
    if stored.to_json() != expected.to_json():
        raise ValueError(
            "host health qualification differs from current source replay"
        )
    _fresh_material_use(fingerprint, provider_requests, evidence)
    return stored
