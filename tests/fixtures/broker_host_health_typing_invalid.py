"""Expected strict type failures for host-owned health observations."""

from histdatacom.broker_plugin_health import (
    BrokerHostHealthPolicyV1,
    health_rate,
)

BrokerHostHealthPolicyV1(max_duplicate_rate="unbounded")
health_rate("plugin-counter", 100)
