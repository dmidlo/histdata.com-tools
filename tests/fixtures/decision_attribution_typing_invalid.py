"""Negative installed consumer: numeric paths are not valid file locators."""

from histdatacom.attribution import (
    AttributionRegistryV1,
    read_attribution_artifact,
)


def invalid_path() -> AttributionRegistryV1:
    return read_attribution_artifact(123, AttributionRegistryV1)
