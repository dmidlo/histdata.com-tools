"""Strict public health evidence typing, without runtime source-path imports."""

from collections.abc import Iterable
from pathlib import Path

from histdatacom.broker_plugin_health import (
    BrokerHostHealthAuditV1,
    BrokerHostHealthPolicyV1,
    BrokerHostHealthRateV1,
    health_rate,
)
from histdatacom.broker_plugin_health.storage import read_lifecycle_host_health
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleManifestV1,
    BrokerLifecycleRecordV1,
)
from histdatacom.broker_plugin_policy import BrokerSDKInvocationV1


def rate() -> BrokerHostHealthRateV1:
    return health_rate(25, 10_000)


def restore_actual(
    directory: Path,
    manifest: BrokerLifecycleManifestV1,
    records: Iterable[BrokerLifecycleRecordV1],
    invocation: BrokerSDKInvocationV1,
) -> tuple[BrokerHostHealthAuditV1, BrokerHostHealthPolicyV1]:
    audit = read_lifecycle_host_health(directory, manifest, records, invocation)
    return audit, audit.header.policy
