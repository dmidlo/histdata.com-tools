"""Explicit generated review inputs for the installed runtime conformance kit.

These known fixture schemas are host-owned test inputs, not queried by invoking
an unreviewed plugin. Nothing here installs an ambient policy scope.
"""

from contextlib import contextmanager

from histdatacom.broker_plugin_policy.bindings import (
    BrokerProviderConfigurationV1,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_policy.contracts import canonical_policy_json
from histdatacom.broker_plugin_policy.scope import provider_policy_scope
from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerEventKind,
)

if __package__:
    from .broker_provider_policy import MutablePolicySource, policy_context
else:
    # The installed-wheel qualifier copies these two generated helper files
    # alongside its script. No checkout/test-package path enters that process.
    from broker_provider_policy import MutablePolicySource, policy_context


def runtime_request(plan, configuration, *, family="lifecycle"):
    if family == "lifecycle":
        fields = (
            BrokerConfigurationFieldV1(
                "mode",
                BrokerConfigurationType.STRING,
                "Offline fixture scenario",
            ),
            BrokerConfigurationFieldV1(
                "credential",
                BrokerConfigurationType.STRING,
                "Ephemeral fixture value",
                required=False,
                secret=True,
            ),
        )
    elif family == "security":
        fields = (
            BrokerConfigurationFieldV1(
                "mode", BrokerConfigurationType.STRING, "Offline scenario"
            ),
            BrokerConfigurationFieldV1(
                "credential",
                BrokerConfigurationType.STRING,
                "Ephemeral credential",
                secret=True,
            ),
            BrokerConfigurationFieldV1(
                "port",
                BrokerConfigurationType.INTEGER,
                "Caller-owned local fixture port",
                required=False,
            ),
        )
    else:
        raise ValueError("unknown generated runtime fixture family")
    schema = BrokerConfigurationSchemaV1(fields)
    public = {
        key: value
        for key, value in configuration.items()
        if key != "credential"
    }
    return BrokerSDKInvocationV1(
        plan,
        BrokerProviderConfigurationV1(
            "generated-runtime-provider",
            "generated-private-profile",
            schema.to_json(),
            canonical_policy_json(public),
            (
                ("credential",)
                if family == "security" or "credential" in configuration
                else ()
            ),
        ),
        BrokerProviderOutputContractV1(
            "sdk-v1",
            tuple(sorted(kind.value for kind in BrokerEventKind)),
            allow_opaque_metadata=True,
            allow_private_account_metadata=True,
        ),
    )


@contextmanager
def runtime_scope(request):
    """Allow only the exact declared generated provider/configuration binding."""
    source = MutablePolicySource(
        policy_context(sdk_invocation_binding(request))
    )
    with provider_policy_scope(source):
        yield source
