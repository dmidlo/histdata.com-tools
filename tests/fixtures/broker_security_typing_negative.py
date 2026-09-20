"""Deliberately invalid public protocol: strict installed-wheel mypy must fail."""

from histdatacom.broker_plugin_security import BrokerSecretProvider


class InvalidSecrets:
    def resolve(self, handle: str) -> int:
        return 42


provider: BrokerSecretProvider = InvalidSecrets()
