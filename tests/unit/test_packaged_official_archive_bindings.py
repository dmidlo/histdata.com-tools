"""Cross-artifact identities must move together when the registry changes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import histdatacom.market_context as public

_ARCHIVE_LOADERS: dict[str, Callable[[], Any]] = {
    name: loader
    for name, loader in vars(public).items()
    if name.startswith("load_packaged_")
    and name.endswith("archive_manifest")
    and callable(loader)
}


@pytest.mark.parametrize("loader_name", sorted(_ARCHIVE_LOADERS))
def test_packaged_archive_dependency_identities_are_current(
    loader_name: str,
) -> None:
    """A valid individual receipt must also bind current dependencies."""
    registry = public.load_packaged_official_source_registry()
    manifest = _ARCHIVE_LOADERS[loader_name]()
    payload = manifest.to_dict()

    assert payload["registry_id"] == registry.registry_id
    if "profile_id" in payload:
        profile = public.built_in_united_states_backfill_profile(registry)
        assert payload["profile_id"] == profile.profile_id
    if "decision_archive_manifest_id" in payload:
        decisions = public.load_packaged_ecb_monetary_policy_archive_manifest()
        assert payload["decision_archive_manifest_id"] == decisions.manifest_id


def test_packaged_archive_binding_inventory_is_not_empty() -> None:
    """Discovery cannot silently turn this gate into an empty test set."""
    assert "load_packaged_treasury_mts_archive_manifest" in _ARCHIVE_LOADERS
    assert (
        "load_packaged_insee_industrial_production_archive_manifest"
        in _ARCHIVE_LOADERS
    )
    assert len(_ARCHIVE_LOADERS) >= 41
