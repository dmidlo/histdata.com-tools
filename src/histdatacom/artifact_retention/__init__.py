"""Provenance-aware retention of explicitly managed local artifact stores.

Public contracts and operations are lazy. Importing the package does not open a
store, load a scientific backend, replay a producer or acquire a filesystem lock.
Serialized declarations and content hashes are evidence, not deletion authority.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_CONTRACT_EXPORTS = frozenset(
    {
        "AsciiTickRecipeV1",
        "CacheRegenerationV1",
        "CollectionDecisionV1",
        "CollectionOutcomeV1",
        "CollectionPlanV1",
        "CollectionReceiptV1",
        "CollectionTombstoneV1",
        "DependencyRelation",
        "DependencyV1",
        "ManagedPayloadRefV1",
        "NativeObjectV1",
        "RetentionClass",
        "RetentionHoldV1",
        "RetentionPolicyV1",
        "StoreMarkerV1",
        "StoreSnapshotV1",
        "TransactionCompletionV1",
    }
)
_STORAGE_EXPORTS = frozenset(
    {
        "RetentionStoreError",
        "add_retention_hold",
        "create_retention_store",
        "ingest_ascii_tick_source",
        "inspect_retention_store",
        "plan_artifact_collection",
        "produce_histdata_cache",
        "publish_histdata_cache_catalog",
        "register_retention_root",
    }
)
_LIFECYCLE_EXPORTS = frozenset(
    {
        "AdmissionReceiptV1",
        "CollectionInterruptedEvidenceV1",
        "RootRegistrationV1",
    }
)
_APPLY_EXPORTS = frozenset(
    {"CollectionInterruptedError", "apply_artifact_collection"}
)
__all__ = sorted(
    _CONTRACT_EXPORTS | _LIFECYCLE_EXPORTS | _STORAGE_EXPORTS | _APPLY_EXPORTS
)


def __getattr__(name: str) -> Any:
    """Expose only the closed supported API without eager producer imports."""
    if name in _CONTRACT_EXPORTS:
        module_name = ".contracts"
    elif name in _LIFECYCLE_EXPORTS:
        module_name = ".lifecycle_contracts"
    elif name in _STORAGE_EXPORTS:
        module_name = ".storage"
    elif name in _APPLY_EXPORTS:
        module_name = ".apply"
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
