"""Negative installed-wheel typing fixture; both assignments must be refused.

This file is checked statically, never executed. Accepting either expression
would indicate that public SDK annotations had degraded to untyped Any.
"""

from histdatacom.broker_plugins import BrokerPluginV1, BrokerQuoteV1

invalid_quote = BrokerQuoteV1("EURUSD", 1.2, "1.3")
invalid_plugin: BrokerPluginV1 = object()
