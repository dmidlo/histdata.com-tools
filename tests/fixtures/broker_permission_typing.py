"""External plugin/resource and host permission APIs; installed-wheel typing."""

from histdatacom.broker_plugin_lifecycle import BrokerLifecycleManifestV1
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionExecutionV1,
    BrokerPermissionResourcesV1,
    broker_permission_scope,
    verify_permission_execution,
)
from histdatacom.broker_plugin_policy import BrokerSDKInvocationV1
from histdatacom.broker_plugins import (
    BrokerHostHTTPResponseV1,
    BrokerHostResourcesV1,
)


def plugin_request(
    resources: BrokerHostResourcesV1,
) -> BrokerHostHTTPResponseV1:
    return resources.request("fixture", "GET", "/quotes")


def host_request(
    authority: BrokerPermissionAuthorityV1,
    resources: BrokerPermissionResourcesV1,
) -> bytes | None:
    public: BrokerHostResourcesV1 = resources
    with broker_permission_scope(authority, resources=public):
        public.put_cache("fixture", "sample", b"synthetic-only")
        return public.get_cache("fixture", "sample")


def historical_association(
    proof: BrokerPermissionExecutionV1,
    invocation: BrokerSDKInvocationV1,
    manifest: BrokerLifecycleManifestV1,
) -> None:
    verify_permission_execution(proof, invocation, manifest)
