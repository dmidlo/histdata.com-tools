"""Strict installed typing must reject these intentional field/call errors."""

from histdatacom.broker_plugin_lifecycle import BrokerLifecyclePolicyV1

invalid = BrokerLifecyclePolicyV1(queue_items="unbounded")
