"""SDK compatibility and metadata-only ecosystem governance."""

from .contracts import (
    MIN_DEPRECATION_NOTICE_DAYS,
    MIN_DEPRECATION_NOTICE_MINOR_RELEASES,
    BrokerConformanceReferenceV1,
    BrokerPluginDirectoryEntryV1,
    BrokerSDKCompatibilityDecisionV1,
    BrokerSDKCompatibilityPolicyV1,
    BrokerSDKSupportV1,
    BrokerSDKUse,
    broker_sdk_compatibility_policy,
    negotiate_broker_sdk,
)
from .provenance import (
    BrokerCaptureCompatibilityDeclarationV1,
    BrokerCaptureCompatibilityV1,
    read_lifecycle_capture_compatibility,
)

__all__ = [
    "BrokerCaptureCompatibilityDeclarationV1",
    "BrokerCaptureCompatibilityV1",
    "read_lifecycle_capture_compatibility",
    "MIN_DEPRECATION_NOTICE_DAYS",
    "MIN_DEPRECATION_NOTICE_MINOR_RELEASES",
    "BrokerConformanceReferenceV1",
    "BrokerPluginDirectoryEntryV1",
    "BrokerSDKCompatibilityDecisionV1",
    "BrokerSDKCompatibilityPolicyV1",
    "BrokerSDKSupportV1",
    "BrokerSDKUse",
    "broker_sdk_compatibility_policy",
    "negotiate_broker_sdk",
]
