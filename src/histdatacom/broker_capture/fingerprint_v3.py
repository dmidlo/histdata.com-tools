"""SDK-native scientific identity; no synthetic legacy session or protocol facts.

These bounded contracts retain evidence, not authority. Material use must replay
the actual native sources under current rights and reproduce the projection.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any, cast

from .fingerprint_contracts import BrokerDeliveryCellV1

if TYPE_CHECKING:
    from histdatacom.broker_plugin_health.contracts import (
        BrokerHostHealthAuditV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_provenance.contracts import (
        BrokerProvenanceHeaderV1,
        BrokerProvenanceSealV1,
    )

BROKER_DELIVERY_FINGERPRINT_V3_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-fingerprint.v3"
)
BROKER_FINGERPRINT_LIFECYCLE_ROOT_SCHEMA_VERSION = (
    "histdatacom.broker-fingerprint-lifecycle-root.v1"
)
BROKER_LIFECYCLE_FINGERPRINT_FIT_CONFIG_SCHEMA_VERSION = (
    "histdatacom.broker-lifecycle-fingerprint-fit-config.v1"
)
BROKER_LIFECYCLE_ELIGIBILITY_SCHEMA_VERSION = (
    "histdatacom.broker-lifecycle-fingerprint-eligibility.v1"
)

NATIVE_FINGERPRINT_LIMITATIONS = (
    "statistical_projection_not_semantic_or_empirical_qualification",
    "cadence_uses_host_monotonic_receive_time",
    "support_uses_host_utc_receive_time",
    "source_continuity_and_upstream_loss_not_attested",
    "server_account_and_provider_protocol_not_inferred",
    "source_batch_semantics_unavailable",
    "no_calendar_or_market_context_conditioning",
)


def _canonical(value: object) -> str:
    from histdatacom.broker_plugin_policy._wire import canonical_json

    return canonical_json(value)


def _load(text: str) -> dict[str, Any]:
    from histdatacom.broker_plugin_policy._wire import load_json

    value = load_json(text)
    if type(value) is not dict:
        raise ValueError("native science requires an exact JSON object")
    return cast(dict[str, Any], value)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _keys(value: Mapping[str, Any], cls: type) -> None:
    if type(value) is not dict or set(value) != {f.name for f in fields(cls)}:
        raise ValueError("unknown or missing native scientific fields")
    _canonical(value)


@dataclass(frozen=True, slots=True)
class BrokerLifecycleFingerprintFitConfigV1:
    min_quote_events: int = 3
    min_cell_support: int = 3
    max_capture_manifests: int = 16
    max_input_records: int = 1_000_000
    max_input_bytes: int = 67_108_864
    max_cells: int = 129
    max_samples_per_metric: int = 4096
    rounding_digits: int = 12
    quantiles: tuple[float, ...] = (0.05, 0.25, 0.5, 0.75, 0.95, 0.99)
    schema_version: str = BROKER_LIFECYCLE_FINGERPRINT_FIT_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("min_quote_events", 3, 1_000_000),
            ("min_cell_support", 3, 1_000_000),
            ("max_capture_manifests", 1, 16),
            ("max_input_records", 1, 1_000_000),
            ("max_input_bytes", 1, 1_073_741_824),
            ("max_cells", 2, 129),
            ("max_samples_per_metric", 1, 4096),
            ("rounding_digits", 0, 15),
        ):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("native fit configuration exceeds bounds")
        if (
            self.schema_version
            != BROKER_LIFECYCLE_FINGERPRINT_FIT_CONFIG_SCHEMA_VERSION
            or type(self.quantiles) is not tuple
            or not 1 <= len(self.quantiles) <= 16
            or any(
                type(q) is not float or not math.isfinite(q) or not 0 <= q <= 1
                for q in self.quantiles
            )
            or self.quantiles != tuple(sorted(set(self.quantiles)))
        ):
            raise ValueError("invalid native fit policy")

    def to_dict(self) -> dict[str, Any]:
        return {
            f.name: (
                list(self.quantiles)
                if f.name == "quantiles"
                else getattr(self, f.name)
            )
            for f in fields(self)
        }

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @property
    def config_id(self) -> str:
        return "broker-lifecycle-fingerprint-fit-config:sha256:" + _digest(
            self.to_json()
        )

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any]
    ) -> BrokerLifecycleFingerprintFitConfigV1:
        _keys(value, cls)
        if type(value["quantiles"]) is not list:
            raise ValueError("native quantiles must be a JSON array")
        result = cls(**{**value, "quantiles": tuple(value["quantiles"])})
        if result.to_json() != _canonical(value):
            raise ValueError("noncanonical native fit policy")
        return result

    @classmethod
    def from_json(cls, text: str) -> BrokerLifecycleFingerprintFitConfigV1:
        return cls.from_dict(_load(text))


@dataclass(frozen=True, slots=True)
class BrokerLifecycleFingerprintEligibilityV1:
    manifest_id: str
    config_id: str
    eligible: bool
    reason_codes: tuple[str, ...]
    record_count: int
    quote_count: int
    first_receive_time_utc_ns: int | None
    last_receive_time_utc_ns: int | None
    schema_version: str = BROKER_LIFECYCLE_ELIGIBILITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version != BROKER_LIFECYCLE_ELIGIBILITY_SCHEMA_VERSION
            or type(self.eligible) is not bool
            or self.eligible != (not self.reason_codes)
            or type(self.reason_codes) is not tuple
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or len(self.reason_codes) > 16
            or any(
                type(x) is not str or not 1 <= len(x) <= 128
                for x in self.reason_codes
            )
            or any(
                type(getattr(self, n)) is not str
                or not 1 <= len(getattr(self, n)) <= 512
                for n in ("manifest_id", "config_id")
            )
            or any(
                type(n) is not int or not 0 <= n <= 1_000_000
                for n in (self.record_count, self.quote_count)
            )
            or self.quote_count > self.record_count
        ):
            raise ValueError("invalid native scientific eligibility")
        start, end = (
            self.first_receive_time_utc_ns,
            self.last_receive_time_utc_ns,
        )
        if (start is None) != (end is None) or (
            start is not None
            and (
                type(start) is not int
                or type(end) is not int
                or not 0 <= start <= end < 2**63
            )
        ):
            raise ValueError("invalid native quote support")
        if self.eligible and (self.quote_count < 3 or start is None):
            raise ValueError(
                "eligible native science requires positive timed quote support"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            f.name: (
                list(self.reason_codes)
                if f.name == "reason_codes"
                else getattr(self, f.name)
            )
            for f in fields(self)
        }

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @property
    def decision_id(self) -> str:
        return "broker-lifecycle-fingerprint-eligibility:sha256:" + _digest(
            self.to_json()
        )

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any]
    ) -> BrokerLifecycleFingerprintEligibilityV1:
        _keys(value, cls)
        if type(value["reason_codes"]) is not list:
            raise ValueError("native reasons must be a JSON array")
        return cls(**{**value, "reason_codes": tuple(value["reason_codes"])})


@dataclass(frozen=True, slots=True)
class BrokerFingerprintLifecycleRootV1:
    manifest: BrokerLifecycleManifestV1
    invocation: BrokerSDKInvocationV1
    health_audit: BrokerHostHealthAuditV1
    header: BrokerProvenanceHeaderV1
    seal: BrokerProvenanceSealV1
    compatibility_json: str | None
    schema_version: str = BROKER_FINGERPRINT_LIFECYCLE_ROOT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        from histdatacom.broker_plugin_health.contracts import (
            BrokerHostHealthAuditV1,
            BrokerHostHealthState,
        )
        from histdatacom.broker_plugin_lifecycle.contracts import (
            BrokerLifecycleCompletion,
            BrokerLifecycleManifestV1,
        )
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKInvocationV1,
        )
        from histdatacom.broker_plugin_provenance.contracts import (
            BrokerProvenanceHeaderV1,
            BrokerProvenanceNativeFamily,
            BrokerProvenanceSealV1,
        )

        if (
            self.schema_version
            != BROKER_FINGERPRINT_LIFECYCLE_ROOT_SCHEMA_VERSION
            or type(self.manifest) is not BrokerLifecycleManifestV1
            or type(self.invocation) is not BrokerSDKInvocationV1
            or type(self.health_audit) is not BrokerHostHealthAuditV1
            or type(self.header) is not BrokerProvenanceHeaderV1
            or type(self.seal) is not BrokerProvenanceSealV1
        ):
            raise ValueError("exact SDK-native root contracts required")
        manifest, terminal = self.manifest, self.seal.terminal
        if (
            manifest.completion is not BrokerLifecycleCompletion.COMPLETE
            or manifest.header.plan.to_json() != self.invocation.plan.to_json()
            or self.header.family
            is not BrokerProvenanceNativeFamily.LIFECYCLE_V1
            or self.header.native_header_id != manifest.header.artifact_id
            or self.header.native_header_sha256
            != _digest(manifest.header.to_json())
            or self.seal.header_id != self.header.artifact_id
            or terminal.header_id != self.header.artifact_id
            or terminal.native_manifest_id != manifest.artifact_id
            or terminal.native_manifest_sha256 != _digest(manifest.to_json())
            or not terminal.native_complete
            or not terminal.observations_complete
            or self.health_audit.native_manifest_id != manifest.artifact_id
            or self.health_audit.state is not BrokerHostHealthState.QUALIFIED
            or self.health_audit.failures
            or not self.health_audit.complete_observations
            or terminal.health_audit_id != self.health_audit.artifact_id
            or terminal.health_audit_sha256
            != _digest(self.health_audit.to_json())
        ):
            raise ValueError(
                "native root requires exact complete qualified evidence"
            )
        if self.compatibility_json is not None:
            from histdatacom.broker_plugin_compatibility import (
                BrokerCaptureCompatibilityV1,
            )

            compatibility = BrokerCaptureCompatibilityV1.from_json(
                self.compatibility_json
            )
            declaration = compatibility.declaration
            if (
                compatibility.provenance_header_json != self.header.to_json()
                or compatibility.provenance_seal_json != self.seal.to_json()
                or declaration.invocation_id != self.invocation.artifact_id
                or declaration.invocation_sha256
                != _digest(self.invocation.to_json())
                or compatibility.to_json() != self.compatibility_json
            ):
                raise ValueError(
                    "native compatibility evidence substitutes exact root"
                )
        _canonical(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "manifest": self.manifest.to_dict(),
            "invocation_json": self.invocation.to_json(),
            "health_audit": self.health_audit.to_dict(),
            "header": self.header.to_dict(),
            "seal": self.seal.to_dict(),
            "compatibility_json": self.compatibility_json,
        }

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any]
    ) -> BrokerFingerprintLifecycleRootV1:
        from histdatacom.broker_plugin_health.contracts import (
            BrokerHostHealthAuditV1,
        )
        from histdatacom.broker_plugin_lifecycle.contracts import (
            BrokerLifecycleManifestV1,
        )
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKInvocationV1,
        )
        from histdatacom.broker_plugin_provenance.contracts import (
            BrokerProvenanceHeaderV1,
            BrokerProvenanceSealV1,
        )

        if type(value) is not dict or set(value) != {
            "schema_version",
            "manifest",
            "invocation_json",
            "health_audit",
            "header",
            "seal",
            "compatibility_json",
        }:
            raise ValueError("unknown or missing native root fields")
        result = cls(
            BrokerLifecycleManifestV1.from_dict(value["manifest"]),
            BrokerSDKInvocationV1.from_json(value["invocation_json"]),
            BrokerHostHealthAuditV1.from_dict(value["health_audit"]),
            BrokerProvenanceHeaderV1.from_dict(value["header"]),
            BrokerProvenanceSealV1.from_dict(value["seal"]),
            value["compatibility_json"],
            value["schema_version"],
        )
        if result.to_json() != _canonical(value):
            raise ValueError("native root requires canonical exact evidence")
        return result


@dataclass(frozen=True, slots=True)
class BrokerDeliveryFingerprintV3:
    fit_config: BrokerLifecycleFingerprintFitConfigV1
    capture_roots: tuple[BrokerFingerprintLifecycleRootV1, ...]
    eligibility_decisions: tuple[BrokerLifecycleFingerprintEligibilityV1, ...]
    support_start_utc_ns: int
    support_end_utc_ns: int
    effective_start_utc_ns: int
    effective_end_utc_ns: int | None
    cells: tuple[BrokerDeliveryCellV1, ...]
    supersedes_fingerprint_id: str | None = None
    limitations: tuple[str, ...] = NATIVE_FINGERPRINT_LIMITATIONS
    fingerprint_id: str = ""
    schema_version: str = BROKER_DELIVERY_FINGERPRINT_V3_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version != BROKER_DELIVERY_FINGERPRINT_V3_SCHEMA_VERSION
            or type(self.fit_config)
            is not BrokerLifecycleFingerprintFitConfigV1
            or type(self.capture_roots) is not tuple
            or not 1
            <= len(self.capture_roots)
            <= self.fit_config.max_capture_manifests
            or any(
                type(x) is not BrokerFingerprintLifecycleRootV1
                for x in self.capture_roots
            )
            or type(self.eligibility_decisions) is not tuple
            or any(
                type(x) is not BrokerLifecycleFingerprintEligibilityV1
                for x in self.eligibility_decisions
            )
            or type(self.cells) is not tuple
            or not 1 <= len(self.cells) <= self.fit_config.max_cells
            or any(type(x) is not BrokerDeliveryCellV1 for x in self.cells)
            or self.limitations != NATIVE_FINGERPRINT_LIMITATIONS
            or type(self.fingerprint_id) is not str
        ):
            raise ValueError("exact bounded native fingerprint required")
        roots = tuple(
            BrokerFingerprintLifecycleRootV1.from_dict(x.to_dict())
            for x in self.capture_roots
        )
        ids = tuple(x.manifest.artifact_id for x in roots)
        if ids != tuple(sorted(set(ids))) or ids != tuple(
            x.manifest_id for x in self.eligibility_decisions
        ):
            raise ValueError(
                "native roots and decisions must match unique sorted manifest IDs"
            )
        if any(
            not x.eligible or x.config_id != self.fit_config.config_id
            for x in self.eligibility_decisions
        ):
            raise ValueError(
                "native fingerprint cannot retain ineligible sources"
            )
        for value in (
            self.support_start_utc_ns,
            self.support_end_utc_ns,
            self.effective_start_utc_ns,
        ):
            if type(value) is not int or not 0 <= value < 2**63:
                raise ValueError(
                    "native fingerprint times require bounded integer nanoseconds"
                )
        if (
            self.support_start_utc_ns > self.support_end_utc_ns
            or self.support_start_utc_ns
            != min(
                cast(int, x.first_receive_time_utc_ns)
                for x in self.eligibility_decisions
            )
            or self.support_end_utc_ns
            != max(
                cast(int, x.last_receive_time_utc_ns)
                for x in self.eligibility_decisions
            )
        ):
            raise ValueError(
                "native fingerprint support differs from quote evidence"
            )
        if self.effective_end_utc_ns is not None and (
            type(self.effective_end_utc_ns) is not int
            or not self.effective_start_utc_ns
            < self.effective_end_utc_ns
            < 2**63
        ):
            raise ValueError("invalid native fingerprint effective interval")
        if self.supersedes_fingerprint_id is not None and (
            type(self.supersedes_fingerprint_id) is not str
            or not re.fullmatch(
                r"broker-delivery-fingerprint-v3:sha256:[0-9a-f]{64}",
                self.supersedes_fingerprint_id,
            )
        ):
            raise ValueError("native supersession requires a V3 identity")
        cells = tuple(
            BrokerDeliveryCellV1.from_dict(x.to_dict()) for x in self.cells
        )
        if tuple(x.condition.condition_id for x in cells) != tuple(
            sorted({x.condition.condition_id for x in cells})
        ):
            raise ValueError("native cells must be unique and sorted")
        object.__setattr__(self, "capture_roots", roots)
        object.__setattr__(self, "cells", cells)
        payload = self.to_dict()
        payload.pop("fingerprint_id")
        expected = "broker-delivery-fingerprint-v3:sha256:" + _digest(
            _canonical(payload)
        )
        if self.fingerprint_id and self.fingerprint_id != expected:
            raise ValueError("native fingerprint identity mismatch")
        object.__setattr__(self, "fingerprint_id", expected)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "fingerprint_id": self.fingerprint_id,
            "fit_config": self.fit_config.to_dict(),
            "capture_roots": [x.to_dict() for x in self.capture_roots],
            "eligibility_decisions": [
                x.to_dict() for x in self.eligibility_decisions
            ],
            "support_start_utc_ns": self.support_start_utc_ns,
            "support_end_utc_ns": self.support_end_utc_ns,
            "effective_start_utc_ns": self.effective_start_utc_ns,
            "effective_end_utc_ns": self.effective_end_utc_ns,
            "cells": [x.to_dict() for x in self.cells],
            "supersedes_fingerprint_id": self.supersedes_fingerprint_id,
            "limitations": list(self.limitations),
        }

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BrokerDeliveryFingerprintV3:
        _keys(value, cls)
        if any(
            type(value[n]) is not list
            for n in (
                "capture_roots",
                "eligibility_decisions",
                "cells",
                "limitations",
            )
        ):
            raise ValueError("native fingerprint inventories require arrays")
        result = cls(
            **{
                **value,
                "fit_config": BrokerLifecycleFingerprintFitConfigV1.from_dict(
                    value["fit_config"]
                ),
                "capture_roots": tuple(
                    BrokerFingerprintLifecycleRootV1.from_dict(x)
                    for x in value["capture_roots"]
                ),
                "eligibility_decisions": tuple(
                    BrokerLifecycleFingerprintEligibilityV1.from_dict(x)
                    for x in value["eligibility_decisions"]
                ),
                "cells": tuple(
                    BrokerDeliveryCellV1.from_dict(x) for x in value["cells"]
                ),
                "limitations": tuple(value["limitations"]),
            }
        )
        if result.to_json() != _canonical(value):
            raise ValueError(
                "native fingerprint requires canonical exact bytes"
            )
        return result

    @classmethod
    def from_json(cls, text: str) -> BrokerDeliveryFingerprintV3:
        return cls.from_dict(_load(text))
