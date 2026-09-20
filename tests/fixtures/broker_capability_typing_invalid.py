"""Negative installed typing fixture: this must produce an arg-type error."""

from histdatacom.broker_plugin_capabilities import BrokerCapabilityWorkflowV1

workflow = BrokerCapabilityWorkflowV1(operations=42)
