"""Installed public-API typing fixture; does not import native producers."""

from histdatacom.schema_compatibility import schema_compatibility_registry
from histdatacom.schema_semantics import (
    SemanticProfileV1,
    SemanticRegistryValidationV1,
    exact_unit_conversion,
    semantic_profiles,
    validate_lossless_evidence,
)


def public_metadata() -> tuple[tuple[SemanticProfileV1, ...], str, int]:
    result: SemanticRegistryValidationV1 = validate_lossless_evidence(
        schema_compatibility_registry()
    )
    return (
        semantic_profiles(),
        result.registry_id,
        exact_unit_conversion(1000, 1, 1000),
    )
