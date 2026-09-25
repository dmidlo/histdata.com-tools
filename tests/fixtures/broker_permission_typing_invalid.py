"""Expected strict type failures at the public resource boundary."""

from histdatacom.broker_plugins import BrokerHostResourcesV1


def invalid(resources: BrokerHostResourcesV1) -> None:
    resources.request("fixture", "GET", "/quotes", body="plaintext")
    resources.put_cache("fixture", "sample", "not-bytes")
    resources.publish_scientific_product("unauthorized")
